# Master Auditor Report — Vibrium Workflow Engine (design plan) — 2026-05-30

## Verdict: NEEDS_FIX (design-level — fixable before coding)

## TL;DR
The plan is structurally sound and reuses the right primitives (`pre_call_gate`, `clevertap_trigger`, SQLite canonical store, orchestrator tick, scheduler dispatch). But it has **6 P0** and **11 P1** design-level holes that will bite in production if you start coding from the plan as written. The single biggest issue: the disposition-wakeup join (`ingest.py` → `workflow_runs.ready_at_ist = now`) is under-specified and unsafe when a customer has multiple open `AWAIT_DISPOSITION` nodes, and worse, `ingest.py` currently writes `decision_log` rows **with a different temporal sequence than the plan assumes** — `ingest.py` writes the decision_log row *before* the FIRE event is reconciled, so any workflow run that just queued a FIRE_VB_CALL can be woken by the **previous attempt's** disposition. The other critical gap is **no documented dedupe key for the workflow→pending_actions queue insert**, which means a tick retry will double-queue calls and the global 3/day cap won't catch it until the second call already fired.

Counts: **P0 = 6**, **P1 = 11**, **P2 = 7**.

## Stack Detected
- **Python:** 3.11+ (matches existing Vibrium codebase)
- **Domain:** Vibrium automation + Ops Console v2 + CleverTap
- **Libraries to be added:** `simpleeval` (not currently in tree — confirmed `ModuleNotFoundError` locally), Drawflow.js (vendored)
- **KB files consulted:** `clevertap.md`, `system-design-patterns.md`, `sqlite3.md` (implied), `fastapi.md` (implied), `regulatory-collections.md` (referenced via `pre_call_gate`)
- **Code files inspected:** `vibrium-automation/scripts/pre_call_gate.py`, `ingest.py`, `clevertap_trigger.py`, `agents/orchestrator.py`, `sqlite_store.py` (schema indexes only)
- **Live lookups:** none (Drawflow / simpleeval evaluated from KB priors + repo grep)

---

## P0 Issues — Must Fix Before Merge

### P0-1: Disposition-wakeup mechanism is racy and ambiguous when a customer has multiple open AWAIT_DISPOSITION nodes
- **Category:** Correctness / State machine
- **Plan section:** "Executor loop → Disposition wakeup"
- **Evidence (plan):**
  > "When `ingest.py` writes a `decision_log` row for a customer that has an open `AWAIT_DISPOSITION`, it sets `ready_at_ist = now()` on the matching run."
- **Why it's wrong:**
  1. **Multiple-run ambiguity.** The plan explicitly allows a customer to be in one ACTIVE run per workflow, but across N workflows there can be N open `AWAIT_DISPOSITION` nodes simultaneously. "The matching run" is not defined. If we wake all of them, every workflow that's waiting attributes the *same* disposition to itself, even though that disposition came from one specific FIRE_VB_CALL — only one workflow actually fired the call that produced this disposition_log row.
  2. **Time-correlation gap.** A `decision_log` row from `ingest.py` is keyed on `(customer_id, comment_id)` — there is **no link to the originating `pending_actions.id` or `workflow_runs.id`**. So you cannot know which workflow's FIRE generated which disposition. Today's Vibrium gets away with this because there's only one journey per customer.
  3. **`ingest.py` writes `decision_log` BEFORE `pending_actions` is appended for any next attempt** (see lines 177-181 in `ingest.py`). So if workflow_run R just queued FIRE_VB_CALL at T-30s and the scheduler fired it at T-25s, when `ingest.py` runs at T-15s and reads collection_comment_data, it sees the disposition from the **PRIOR** attempt (yesterday's, or earlier today's), classifies it, writes `decision_log`, and wakes R. R now branches on a disposition from a call it didn't make.
- **Required fix:** Three changes:
  - Add `pending_action_id` (or originating CRM `comment_id`) to `decision_log` and propagate `run_id` through `pending_actions` → CRM-side metadata → `collection_comment_data` parsing → `decision_log`. Concretely: when the workflow handler queues a `pending_actions` row, write `run_id` and `node_id` into the row; when `scheduler.py` fires it, include a correlation tag in the CT externaltrigger payload (e.g., `tag_group=workflow_run_<id>`); when `ingest.py` parses the resulting comment, propagate the correlation forward to `decision_log`. THEN the wakeup join becomes `decision_log.run_id = workflow_runs.id` — unambiguous.
  - **AND** require that wakeup only fires if `workflow_runs.status = 'WAITING'` **AND** `workflow_runs.current_node_id` is `AWAIT_DISPOSITION` **AND** `decision_log.comment_create_date >= workflow_runs.ready_at_lower_bound`. The lower bound is the timestamp when the workflow *entered* `AWAIT_DISPOSITION`, written by the handler. This blocks waking on stale dispositions from prior attempts.
  - Document this explicitly in the plan under "Executor loop." Right now the one-liner hides three load-bearing decisions.

### P0-2: No dedupe key on workflow → pending_actions inserts → tick retry double-fires
- **Category:** Idempotency
- **Plan section:** "Executor loop → Idempotency"
- **Evidence (plan):**
  > "every handler writes side-effects with deterministic external keys (e.g., `pending_actions` row keyed by `(run_id, node_id, attempt_count)`); re-running a node on retry doesn't double-fire."
- **Why it's wrong:** This is asserted but the schema doesn't enforce it. Current `pending_actions` table (per `sqlite_store.py:231-234`) has indexes on `(status, scheduled_at_ist)`, `customer_id`, `action_class`, `last_attempt_at_ist`, `cohort_name` — **no UNIQUE constraint on `(run_id, node_id, attempt_count)`**. If the executor tick crashes mid-write (handler appended pending_action but didn't commit `workflow_runs.current_node_id` advance), the next tick re-runs the same handler at the same node and a SECOND pending_actions row is queued. The scheduler then fires both. Global 3/day cap catches the third one but the first two still went out.
- **Required fix:**
  1. Add columns `run_id INTEGER, node_id TEXT, attempt_count INTEGER` to `pending_actions` in a migration.
  2. Add `UNIQUE(run_id, node_id, attempt_count) WHERE run_id IS NOT NULL` partial index — SQLite supports this via `CREATE UNIQUE INDEX … WHERE`.
  3. In the FIRE_VB_CALL handler, use `INSERT OR IGNORE` on this dedupe key. The `ingest.py` `INSERT OR IGNORE` pattern (`feedback_decision_resolve` fallback) is the precedent.
  4. **The handler's "advance current_node_id" and "INSERT pending_actions" must be in the SAME SQLite transaction.** The plan says "atomically:" in the tick loop but doesn't pin that transactional boundary across the handler-side-effect — and the existing `SQLiteStore.append` API does not currently nest inside a caller-owned transaction. Either extend `SQLiteStore` to accept an open connection, or do the side-effect first and use the resulting `lastrowid` as the advance-marker (similar to `ingest.py:198`'s `new_row_index` pattern).

### P0-3: Pre-call gate is reused but its **caller** is unchanged — workflow runs that fire calls outside the call window are not gated by the gate the plan thinks they are
- **Category:** Compliance / Correctness
- **Plan section:** "High-level architecture" final paragraph; "Verification" item 7
- **Evidence (plan):**
  > "Compliance (RBI window, daily cap, cooldown, dead_cause) is unchanged and uncircumventable."

  And:
  > "The existing scheduler + pre_call_gate fire it, exactly like cohort calls."
- **Why it's wrong:** **`pre_call_gate.py` does not enforce the 08:00–19:00 IST call window.** Read it (the file in this repo). It enforces:
  1. customer is still in collection_view (overdue not cleared)
  2. paid_today < overdue
  3. daily cap (3/day) + 3h cooldown
  4. manual-retry "bot already connected today" check
  5. dead_cause is checked **elsewhere** (in `dispatcher.py` / `gate.py` agent — `pre_call_gate.check` does NOT touch it)

  The **08:00–19:00 IST window is enforced by `scheduler.py`'s scheduling logic + the cron schedule itself, NOT by `pre_call_gate`**. The plan asserts "RBI window … unchanged and uncircumventable" but doesn't verify which subsystem actually enforces it. If `WorkflowAgent.tick()` is invoked outside the call window and a FIRE_VB_CALL handler synchronously calls `clevertap_trigger.trigger()` (e.g., for an inline-fire optimization later), it bypasses the scheduler entirely. Same risk if any new node (PILL_REROUTE, ASSIGN_AGENT) ever fires a CT campaign directly.
- **Required fix:**
  1. Document explicitly in the plan: **the only path from workflow → CT trigger is via `pending_actions` row → existing `scheduler.run()`**. Forbid synchronous CT fires from any handler. Add an architectural invariant: "no `requests.post` to CT lives in `agents/workflow*` or `scripts/clevertap_profile.py`'s trigger path."
  2. Move the 08:00–19:00 IST check **into `pre_call_gate.check()` itself** (or into a new `pre_call_gate.is_callable_now()` helper called by the scheduler) so it cannot be skipped by a new caller. Today it lives in scheduler-side logic and a new caller (the workflow tick on a 15-min cron during the day) could plausibly call CT directly thinking pre_call_gate "covers it."
  3. Add a test: invoke the workflow tick at 19:30 IST with an ACTIVE FIRE_VB_CALL → assert no pending_actions row gets `last_attempt_at_ist` written within that tick.

### P0-4: `simpleeval` is not in the current dependency set, and CONDITION DSL has no spec for **how scratchpad values are typed**
- **Category:** Library Gotcha / Correctness
- **Plan section:** "CONDITION DSL"
- **Evidence:**
  - `python3 -c "import simpleeval"` → `ModuleNotFoundError: No module named 'simpleeval'` on the Mac dev environment. Adding a new pinned dep is fine; the plan must pin it.
  - The plan says: `expr: "risk < 5 AND wa == 'WA_Available' AND dpd > 1"` — but `simpleeval` parses Python expressions (it uses Python's `ast` module). `AND` / `OR` / `NOT` in uppercase are **not Python boolean operators** (Python uses lowercase `and`/`or`/`not`). The example expression would fail to parse.
  - More importantly: `dpd > 1`. What is `dpd`? CT properties come back as **strings** in GET responses unless the property was uploaded as a number. `COLL_collection_risk_segmentation` is typed as a string-formatted-int in CT today. Doing `risk < 5` against a string raises `TypeError: '<' not supported between instances of 'str' and 'int'`. The plan doesn't say where the type coercion happens.
- **Required fix:**
  1. Pin `simpleeval` in `requirements.txt` with a known-good version (`simpleeval==0.9.13` is current as of audit; verify with `pip install simpleeval==<latest>` before locking).
  2. Decide whether the DSL is **Python expressions** (use `and`/`or`/`not`) or **a custom DSL** (`AND`/`OR`/`NOT`). If the latter, you must preprocess: `expr.replace(" AND ", " and ")` etc. — fragile, prefer Python syntax.
  3. Specify in the plan how scratchpad values are typed when they come from `FETCH_CT_PROPS`: either coerce to int/float/bool at write time based on the property's declared schema, or require the CONDITION to use explicit casts (`int(risk) < 5`). The current plan is silent and will produce a TypeError that the executor will then mark the run as ERROR — i.e., every CONDITION that compares a CT-string to a literal-int will silently corrupt the journey.
  4. Whitelist functions on `SimpleEval.functions`: by default `simpleeval` allows `rand`, `randint`, `int`, `float`, `str`, `bool` — all of which are fine but **`int("CID123")` raises and crashes the executor**. Wrap evaluation in `try/except` that marks run ERROR with a useful message, not a generic stack trace.

### P0-5: Version migration "best-effort node_id remap" is under-specified and will silently corrupt active runs
- **Category:** State machine / Correctness
- **Plan section:** "State and persistence → Versioning rule"
- **Evidence (plan):**
  > "Operators choose at save time whether to 'migrate active runs to new version' (best-effort node_id remap; runs whose current node disappears → ORPHANED for manual review)"
- **Why it's wrong:** What does "remap" mean? If v1 had `node_42 = CONDITION(dpd > 1)` and v2 changes it to `node_42 = CONDITION(dpd > 5)`, do existing runs at `node_42` re-evaluate the *new* condition? If a run is parked at `node_50 = WAIT_UNTIL T+1`, and v2 renames `node_50` → `node_50_v2` but the operator typed it as a fresh node, does the run go ORPHANED or migrate? "Best-effort" is not a contract. Compounding: `node_id` is plan-said to be "unique within version" — so what's the identity of "the same node" across versions? UUIDs? Labels? Hashes of config? Without an answer, you'll either ORPHAN everything (poor UX) or quietly redirect runs onto subtly different logic (worse).
- **Required fix:**
  1. Define `node_id` as a **persistent UUID** that survives across versions (assigned at first save, never reused, never reassigned even on relabel). The graph editor must show it in dev mode.
  2. Define the migration rule precisely:
     - If `current_node_id` exists in the new version AND its `type` is unchanged AND its `config` is unchanged (deep-equality on the JSON) → migrate silently, the run continues.
     - If `current_node_id` exists but `config` changed → require operator to confirm per-run (UI shows "23 runs are parked on a node whose config changed; advance / hold / orphan"). Default: hold (status stays WAITING, version unchanged on the existing runs).
     - If `current_node_id` doesn't exist in new version → ORPHANED (as planned).
  3. Default the toggle to "only new enrollments" (the plan already says this — good — but the UI should not let the operator activate v2 without seeing a count of active runs that *would* migrate under each option).
  4. Add a `version_id` filter to the tick query: a run on v1 is executed by v1's graph, full stop. Hot-swap is not allowed mid-tick.

### P0-6: `enrollment_poller.py` lacks an idempotency contract → polling re-creates runs every tick
- **Category:** Idempotency
- **Plan section:** "CleverTap profile reader (new module)" — last paragraph
- **Evidence (plan):**
  > "a tiny scheduled task … every N minutes scans active workflows whose entry condition references CT props, fetches profiles in bulk, diffs against last-known scratchpad, and creates `workflow_runs` rows for new matches."
- **Why it's wrong:** "Diffs against last-known scratchpad" — last-known where? If we look at the most recent `workflow_runs` for that customer × workflow, we'd miss runs that have already terminated (re-enrollment after a TERMINATE(PAID) is a valid use case — re-enrolling for a different bucket). If we look at the latest non-terminal run, we still need an explicit dedupe key, otherwise a tick at 09:00 that creates a run, followed by a tick at 09:15 that re-reads the same CT property values, creates a duplicate run because the diff says "no change since the run we just made." But that's not the right comparison. The plan glosses over this entirely.
- **Required fix:**
  1. Define a `(workflow_id, customer_id, enrollment_key)` UNIQUE constraint where `enrollment_key` is the operator-configured enrollment trigger key (e.g., date-rounded-to-day for daily enrollment, CT-event-id for event-driven). Without an `enrollment_key` the polling will dedupe forever after the first enrollment, which is also wrong.
  2. State this in the plan: "the operator picks the enrollment key when authoring the workflow; default is `{customer_id}_{YYYY-MM-DD}` so the same customer can re-enroll daily but not multiple times per tick."
  3. Document that the poller is **a separate cron**, not part of the orchestrator tick — the plan does say this (`launchd/com.sahil.vibrium.workflow_enroll.plist`) but doesn't pin the cadence. RBI window applies (don't enroll outside it for journeys that immediately fire). Spec the cadence.

---

## P1 Issues — Should Fix

### P1-1: `clevertap_profile.py` GET response shape assumptions are unverified (echo of KB 2026-05-22 finding)
- **Category:** Library Gotcha (CT)
- **Plan section:** "CleverTap profile reader (new module)"
- **Evidence (KB):** From `~/.claude/auditor_kb/clevertap.md` 2026-05-22 finding: `profile.get("os", "—")` and `profile.get("communicationPreferences")` are likely wrong paths — OS is in `platformInfo[0].os_version`, and WA opt-out is unverified. Yet the plan says "the cred-loading pattern at `ct_generic.py:45-48`" — `ct_generic.py` is in `ops_console/` (v1), not the canonical location, and per memory `feedback_no_proxying_to_v1` you should not be reaching back into v1 for new v2 code.
- **Required fix:**
  1. Before writing `clevertap_profile.py`, do one live `GET /1/profile.json?identity=<known-customer>` and log the *full* JSON. Pin the response shape in a fixture file under `tests/fixtures/ct_profile_response.json`. The 4 properties the plan lists must be confirmed extractable at the documented paths.
  2. Cred-loading: copy the pattern from `clevertap_trigger.py:28-40` (cred cache + mtime invalidation) rather than referencing v1.
  3. Set `timeout=(5, 30)` and honor `Retry-After` on 429 (the CT KB calls this out as a known P1 across the codebase).

### P1-2: Bulk profile fetch concurrency=5 is fine, but no `Retry-After` honor and no backoff strategy specified
- **Category:** Library Gotcha (CT)
- **Plan section:** "CleverTap profile reader (new module)"
- **Evidence (plan):**
  > "bulk_get_profiles(identities: list[str]) -> dict[str, dict] — concurrency=5 (CT KB sustained rate ~10–20 req/s). Returns missing-as-None. Honors Retry-After header on 429."
- **Why it's wrong:** "Honors Retry-After" is asserted; pin the **actual algorithm**: sleep the header's seconds, on subsequent 429 within the same minute halve concurrency, etc. Bulk_get of 1000+ identities serially behind a 429 means the poller stalls for minutes — operationally this is OK but document the timeout-on-poll behaviour so the orchestrator doesn't think the poller hung.
- **Required fix:** Add a section to the plan: "Retry policy on 429 in `clevertap_profile.py`: read `Retry-After` (default 5s if absent), sleep, retry up to 3 times, then surface failure as `{cid: None}` in the bulk map — caller treats None as 'fetch failed, retry on next poll tick' and **does not advance the run**."

### P1-3: `SET_CT_PROP` node uses `POST /1/upload` — must handle `unprocessed[]` per the CT KB hard rule
- **Category:** Library Gotcha (CT)
- **Plan section:** Node model table — `SET_CT_PROP`
- **Evidence (plan):**
  > "SET_CT_PROP | POST `/1/upload` to write a property"
- **Why it's wrong:** The KB rule is binding: `status: "success"` does NOT mean the record was written. The plan does not specify that the handler inspects `unprocessed[]` and routes to the `error` edge when the customer's record failed. Today's `ct_generic.py` does this correctly; the new handler must follow that pattern.
- **Required fix:** Spec the handler: parse `data["unprocessed"]`, match by `identity`, and if our customer is in there → route `error` edge with the error code in the scratchpad (`set_ct_prop_error_code = 524` etc.). Otherwise route `success`. Reuse `_classify_delivery`-style logic from `clevertap_trigger.py:134-157` as the template.

### P1-4: Plan reuses `scheduler.py` but says scheduler "writes back into the run's scratchpad" — this is a new responsibility for the scheduler that's not specified
- **Category:** Interaction / Architecture
- **Plan section:** "Files to add / modify" → `scheduler.py`
- **Evidence (plan):**
  > "scripts/scheduler.py — when a pending_actions row tagged with run_id is fired or errors, write back into the run's scratchpad (so the next tick can branch)"
- **Why it's wrong:** Scheduler today is single-purpose (fire pending_actions). Adding a callback into `workflow_runs` couples it to the new engine. **And** when the scheduler runs without the workflow tables migration applied (e.g., on a server that's not yet on the new code), it'll error. Coupling violates the additive premise stated in the architecture section. There's no need for this coupling: the workflow tick already polls `pending_actions.status` for the run's queued row — it can read FIRED/ERROR on the next tick itself.
- **Required fix:**
  1. Remove the scheduler modification from the plan. Workflow tick reads `pending_actions WHERE run_id = ?` directly to observe fire status.
  2. If you keep the modification, gate it behind a feature flag: scheduler checks `if config.get("workflow_engine_enabled"): ...` and is a no-op otherwise.

### P1-5: No alerting on stalled runs / no metrics surface
- **Category:** Observability (Category I, system-design lens 5)
- **Plan section:** Missing entirely
- **Evidence:** The plan describes `workflow_node_log` (audit trail) but no metrics, no per-run-summary email, no failure alert. The CLAUDE.md release gate says: "State-mutation observability — per-run summary email + failure alert to sahil.miglani@stashfin.com." A workflow engine that creates 100K runs over time WILL accumulate stalls, ORPHANs, ERRORs — without a stalled-run sweeper + alert, you'll discover the problem only when a customer complains.
- **Required fix:** Add to the plan:
  - Daily 09:00 IST cron that emails Sahil a per-workflow summary: `# active, # waiting, # erroring > 24h, # orphaned`.
  - Per-tick log line: `agent=workflow status=X processed=Y actioned=Z` (matches the existing orchestrator log format — see `orchestrator.py:50-51`).
  - Heartbeat: `heartbeat("workflow_agent", "ok" / "down", summary=…)` — the existing pattern in `ingest.py:297-301` and `orchestrator.py:181-183`.
  - Alert email if any run has been at `status=ERROR` for > 24h or `status=WAITING` for > 7 days.

### P1-6: No `--dry-run` mode for the WorkflowAgent itself
- **Category:** Compliance / Safety
- **Plan section:** Missing
- **Evidence:** Existing Vibrium subsystems all have `--dry-run` (see `ingest.py:287`, `orchestrator.py:270`). The plan mentions a per-workflow `shadow_mode` flag (good) but doesn't mention a global `--dry-run` for the WorkflowAgent in the orchestrator flow. CLAUDE.md rule 12: "`--dry-run` flag on every mutating script."
- **Required fix:** Spec a global dry-run that:
  - Reads workflows, ticks all runs, logs all intended actions to `workflow_node_log` with a `dry_run=True` flag
  - Does NOT write `pending_actions` rows, does NOT POST to CT (already shadow_mode), does NOT mutate `workflow_runs.current_node_id` (or writes to a separate `workflow_runs_dryrun` shadow table).
  - Inherits from the orchestrator's existing dry-run plumbing (`agent._dry_run = dry_run`, see `orchestrator.py:47`).

### P1-7: `ASSIGN_AGENT` node has no defined target table / queue
- **Category:** Correctness / Integration
- **Plan section:** Node model table — `ASSIGN_AGENT`
- **Evidence (plan):**
  > "ASSIGN_AGENT | Insert into existing agent-calling queue with reason"
- **Why it's wrong:** "Existing agent-calling queue" — which one? The Vibrium subsystem has no "agent queue" table I can find in `state/vibrium.db` schema (`sqlite_store.py` shows tables: pending_actions, decision_log, escalation_log, agent_events, kill_switch, cohort_input, rtp_pending_review, et al — but no explicit "agent_assignment_queue"). The closest thing is `escalation_log` which feeds into the digest email pipeline. The plan must name the target table.
- **Required fix:** Either:
  - Spec a new `agent_assignments` table with columns `(id, customer_id, reason, assigned_at_ist, assigned_to, resolved_at_ist, run_id)`. Then build the UI to surface and resolve it.
  - OR explicitly write to `escalation_log` with a recognizable reason prefix and reuse the digest pipeline.
  - Pick one in the plan, name the table, and either add it to the migration script or reference the existing one.

### P1-8: `WAIT_UNTIL T+1 08:00 IST` semantics under DST / timezone edge cases not specified
- **Category:** Time-correctness (system-design lens 2)
- **Plan section:** Node model table — `WAIT_UNTIL`
- **Evidence (plan):**
  > "WAIT_UNTIL | Park the run until absolute/relative time | relative: 'T+1 day at 08:00 IST' or absolute: scratchpad['ptp_date']"
- **Why it's wrong:** IST has no DST, so the typical risk is lower, but: (a) `scratchpad["ptp_date"]` — what type? string? epoch? IST or UTC? (b) The tick fires every 15 min — if `ready_at_ist` is `2026-05-31 08:00`, the next tick after that is whenever it lands (could be 08:00, could be 08:14). Does the run "wake up at 08:00" or "wake up at the first tick ≥ 08:00"? In practice the latter, but state it. (c) What if the scheduler is paused (kill switch) at 08:00 and resumed at 11:00? Does the wait expire silently or does the tick re-fire? The plan doesn't say.
- **Required fix:**
  1. Pin: `ready_at_ist` is stored as `YYYY-MM-DD HH:MM:SS` (IST-naive, matches existing Vibrium convention per memory `project_vibrium_event_ts_format_drift`).
  2. Pin: `relative` strings are parsed at handler-execute time using `_now_ist() + timedelta(days=N)` then set to the requested HH:MM.
  3. Pin: `scratchpad["ptp_date"]` is required to be `YYYY-MM-DD` IST; the parser must reject other formats with the run going to ERROR.
  4. Pin: a wait that's overdue at execution time fires immediately on the next tick. Document this so operators know catchup is automatic.

### P1-9: `enrollment_poller` fetches CT props for "active workflows whose entry condition references CT props" — but a typo / config error could enroll 12M customers
- **Category:** Safety / Blast radius
- **Plan section:** "CleverTap profile reader (new module)"
- **Evidence (plan):** No cap on enrollment. A workflow with entry condition `dpd >= 0` matches every customer in CT. The plan doesn't mention an enrollment-rate cap or a daily-new-enrollment cap.
- **Required fix:**
  1. Add a per-workflow `max_new_enrollments_per_day` (default 1000) and a global `max_new_enrollments_per_tick` (default 200).
  2. On ACTIVATE, show the operator a "this would enroll ~N customers in the first 24h" preview (compute by running the entry condition against a sample CT profile pull).
  3. Hard cap: ABORT enrollment_poller if any single tick would create > 5000 runs; require Sahil "go" to override (matches `feedback_manual_vibrium_confirm` pattern).

### P1-10: Drawflow.js is the right call **for the size of the problem**, but no plan for accessibility / keyboard-only operation
- **Category:** Library Gotcha / UX
- **Plan section:** "UI: visual node editor in Ops Console v2"
- **Evidence (plan):**
  > "Graph library: Drawflow — vanilla JS, MIT, ~50 KB, no React/Vue."
- **Why it's right (background):** Drawflow is well-suited for a Jinja/htmx server-rendered console. React Flow is the modern choice but requires React; Litegraph is more for node-based visual programming (overkill); pure htmx wouldn't give you the drag-drop graph UI. Drawflow is the pragmatic pick. Vendor it under `/static/drawflow/` (the plan says this — good).
- **Why a P1 follow-up:** Drawflow has known accessibility gaps — it's a canvas/SVG editor with mouse-only interactions, no ARIA. Sahil's ops console has been generally keyboard-accessible per the design tokens; this regresses that. If a future ops user can't operate it from keyboard, that's an internal-product-quality issue.
- **Required fix:**
  1. Audit Drawflow's keyboard support before commit (verify with `ui-auditor` after vendoring).
  2. Provide a JSON-text-editor fallback view (read/edit the `graph_json` directly) for power users — solves keyboard-only and is trivial to implement.
  3. Confirm the Apple-inspired token system applies (CSS variable overrides for Drawflow's default colors); cite the `console.css` tokens explicitly in `workflow_editor.css`.

### P1-11: No spec for how `BRANCH_ON_DISPOSITION` handles new dispositions added later
- **Category:** Correctness / Evolvability
- **Plan section:** Node model — `BRANCH_ON_DISPOSITION`
- **Evidence (plan):**
  > "(none; uses fixed enum) | PAID, PTP, AGREE_EOD, CALLBACK, PART_PAYMENT, RTP, DISPUTE, NRP, NO_CONNECT, OTHER"
- **Why it's wrong:** The action_class enum in `ingest.py` evolves over time (see imports: `ACTION_NOOP, ACTION_RETRY, ACTION_PTP, ACTION_AGREE_EOD, ACTION_CALLBACK, ACTION_RTP_NEEDS_LLM, ACTION_ESCALATE`). When a new disposition lands in `decision_v2.py`, the workflow engine's enum will be out of date and runs will route to `OTHER` silently. Also: the plan's enum (PAID, PART_PAYMENT, NO_CONNECT, etc.) is **not the action_class names**. action_classes are e.g. `PTP_CALL`, `AGREE_EOD_CALL`, `RETRY` — those are the values in `counts` at `ingest.py:136-137`. The plan is mixing two different namespaces.
- **Required fix:**
  1. Decide: does `BRANCH_ON_DISPOSITION` branch on `decision_log.disposition` (the parsed disposition string), or on `decision_log.action_class` (the classified intent)? They are different things.
  2. Either way: validate at workflow-save-time that all enum values in the branch are members of the *current* canonical list — fail validation if not.
  3. Add a `DEFAULT` edge that catches any disposition not explicitly branched (instead of routing silently to OTHER, which is itself an action_class — confusing).

---

## P2 Issues — Nice to Fix

### P2-1: `workflow_node_log.scratchpad_before` and `scratchpad_after` will balloon — no retention policy
A workflow with 50 nodes × 100K runs = 5M log rows, each with 2 JSON blobs. Spec a 90-day retention sweeper.

### P2-2: `current_node_id` should also store `current_node_type` (denormalized) — query performance
Right now `WHERE status='WAITING' AND ready_at_ist <= now()` requires a join to `workflow_versions.graph_json` to find AWAIT_DISPOSITION runs. Denormalize the type onto `workflow_runs` for fast filtering.

### P2-3: Plan says "100K+ workflow_runs rows, tick every 15 min" but doesn't analyze the tick cost
At 100K rows, `SELECT * FROM workflow_runs WHERE status IN ('ACTIVE','WAITING') AND (ready_at_ist IS NULL OR ready_at_ist <= now)` will scan most of them unless the index covers `(status, ready_at_ist)`. The schema has `idx_runs_ready ON (status, ready_at_ist)` — good. But the executor then loads `workflow_versions.graph_json` per run; cache the parsed graph per `version_id` for the tick.

### P2-4: `PILL_REROUTE` is marked "Future" but is in the table — remove or move to a backlog section

### P2-5: `validation_status` and `validation_errors` are on `workflow_versions` but not on `workflows` — wherever you keep the canonical "is this safe to run" answer, surface it in the UI list view (already implied; just confirm)

### P2-6: Test plan mentions `pytest test_workflow_handlers.py` but doesn't say how to mock `clevertap_trigger.trigger()` — vendor a fixture or a `monkeypatch` recipe in the plan

### P2-7: No explicit mention of how to handle SQLite contention when WorkflowAgent + DispatcherAgent + Ingest all write at once
Existing Vibrium uses SQLite with WAL — the plan should reference this and confirm the new tables are created in the same WAL'd database (`state/vibrium.db`). If you accidentally `attach` a non-WAL'd DB, you'll see "database is locked" under concurrent tick.

---

## Things NOT in the Plan That Should Be

1. **Audit log for workflow-level mutations** — who activated v3, who paused workflow #4, who repaired ORPHANED run #1234. Today's Vibrium has nothing like this; the plan needs a `workflow_admin_log` table.
2. **Operator role / permission model** — the plan implicitly assumes any ops-console authenticated user can activate any workflow. Given the gravity (one bad workflow can call every overdue customer 3 times today), spec a per-workflow `requires_approval` flag and a 2-step activation (draft → approval-requested → approved-and-active). Approval = Sahil clicks "I approve" in chat (matches `feedback_manual_vibrium_confirm`).
3. **Schema migration mechanism** — `scripts/workflow_migrations.py` is listed but no migration framework is specified. The existing tree doesn't have alembic. Pin the approach: idempotent `CREATE TABLE IF NOT EXISTS` + versioned scripts that check a `schema_version` table.
4. **Cohort-name attribution** — `pending_actions` already has a `cohort_name` column for filterability (see `sqlite_store.py:268`). Workflow-engine pending_actions should set `cohort_name = f"workflow:{workflow_id}:v{version_id}"` so existing dashboards (hourly digest, cohort summary email) light up for free.
5. **Stop-everything switch** — the existing `kill_switch` table (orchestrator.py:87-107) halts the dispatcher. Confirm in the plan that the WorkflowAgent also respects it; today the orchestrator returns early on KILL, but only if WorkflowAgent is invoked *via* the orchestrator. If `enrollment_poller.py` is a separate cron, it must also check kill_switch.
6. **Backfill / dry-replay** — operator wants to test "what would v2 do to the 5,197 customers currently on v1?" without firing. Spec a `dry-replay` mode that runs v2's graph against existing scratchpad+history and logs the divergence.
7. **Metrics in `agent_events`** — the existing pattern (orchestrator.py:141-166) emits a JSON summary per tick. Workflow tick should follow suit so the existing observability surface picks it up.

---

## Console Wiring + Release Gate Compliance

Per CLAUDE.md (mandatory):

| Gate | Status |
|---|---|
| Sidebar nav `{{ nav('/workflows', 'Workflows', icon_svg) }}` in `base.html` | Plan ✓ mentions; confirm under Automations section |
| Remove `/workflows*` routes from `STUB_ROUTES` in `app.py` | Not mentioned in plan — add explicitly |
| Apple-token compliance in 3 new templates | Plan ✓ asserts |
| `ui-auditor` PASS on every new template | Plan ✓ |
| `master-auditor` PASS on `workflow.py`, `clevertap_profile.py`, `enrollment_poller.py` | Plan ✓ |
| `/stashfin-qa-backend` PASS | Plan ✓ |
| `/stashfin-qa-ui` PASS | Plan ✓ |

All four release gates are acknowledged. Add to verification list: ensure `agents_health_check.py` knows about the WorkflowAgent (otherwise the pipeline-integrity-auditor skill will flag the new daemon as unmonitored).

---

## Domain Gotchas the Plan Doesn't Address

- **Customer might be in `dead_cause` state** — the plan reuses `pre_call_gate`, but `dead_cause` filtering happens in `dispatcher.py` / `gate.py` (not in `pre_call_gate.check`). The plan should note: every FIRE_VB_CALL still goes through `gate.py`'s dead_cause check via the existing dispatcher; the workflow node does NOT need to query dead_cause itself, but the operator authoring a workflow should be told this so they don't add a redundant `CONDITION dead_cause != true` node.
- **The customer-initiated-signal rule** (`feedback_nach_not_user_signal`) — if any new node tries to compute "best time to call" from `coll_bot_calling` (bot-trigger times, not customer signals), that's wrong. The plan doesn't include such a node, but a future operator might add one — call this out in the validation gate.
- **Indian mobile / E.164 / `.0` float strip** — when `FETCH_CT_PROPS` reads `Phone`, the value may have come in via different upload paths; spec a normalizer.

---

## Assumptions Made

1. The plan's "agent-calling queue" mentioned in `ASSIGN_AGENT` refers to a future table; no such table exists today.
2. `scratchpad_json` is < 1 MB per row (SQLite TEXT can handle larger but indexing breaks down).
3. The 4 new tables go into the same `state/vibrium.db` SQLite file, sharing WAL mode and the existing connection pool.
4. The plan's "VB_Prompt_Doc.docx" rules are exhaustively captured by the 5 risk×WA segments — I have not read the doc, only the plan's transcription of it.
5. Drawflow.js's existing v0.0.59 (current stable) has no known XSS in node-template rendering; confirm at vendor time.
6. The `tag_group` mechanism for cohort attribution still flows through `clevertap_trigger.py` unchanged (the existing payload doesn't include `tag_group` today per the audit at `clevertap.md` line 728 P1-4; if you fix that for ct_generic.py, fix it for the workflow path too at the same time).

---

## What I Did Not Audit

- The actual VB_Prompt_Doc.docx — only the plan's transcription.
- Drawflow.js source code / known CVEs — quick check only.
- A live `GET /1/profile.json` response — needed for P1-1 verification.
- `simpleeval` operator whitelist / `MAX_STRING_LENGTH` / `MAX_POWER` constants — confirm at code-write time.
- The complete schema of `state/vibrium.db` — only inspected indexes for `pending_actions`.
- Whether `agents/gate.py` (a different file than `pre_call_gate.py`) does dead_cause checks — assumed from prior memory.

---

## KB Updates Applied

None during this design audit — this is a pre-code review. After implementation, the master-auditor agent will be re-invoked on `agents/workflow.py`, `clevertap_profile.py`, and `enrollment_poller.py` and findings will land in the KB then.

Candidates for KB inclusion after implementation:
- `~/.claude/auditor_kb/clevertap.md`: pin observed shape of `GET /1/profile.json` once verified.
- `~/.claude/auditor_kb/system-design-patterns.md`: add "workflow-engine versioning" as a new pattern (under canonical-store-vs-mirror) — node_id-as-UUID and version-frozen-at-enrollment as the right answers.

---

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor — this report on the DESIGN PLAN) | NEEDS_FIX — see P0-1 through P0-6 |
| 2 | API / backend QA (/stashfin-qa-backend) | DEFERRED — no code to test yet |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | DEFERRED — no code to wire yet |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | DEFERRED — no UI to test yet |

This audit is gate 1 on the **design**, not the code. Apply the P0 fixes to the plan first; gates 2-4 run after implementation.

---

## The 3 things I would change before writing a single line of code

1. **Fix the disposition-wakeup join.** (P0-1) Add `run_id` propagation through `pending_actions` → CT trigger `tag_group` → `collection_comment_data` parsing → `decision_log`, so `AWAIT_DISPOSITION` wakes on the **specific call that this run made**, not on any disposition for the customer. Without this, multi-workflow customers get wrong-branch routing the moment two workflows fire calls on the same customer the same day.

2. **Define UUID-stable `node_id` and pin the version-migration rules.** (P0-5) Make `node_id` a persistent UUID across versions. Define exactly what "migrate active runs" does at config-changed vs config-unchanged. Default to "hold" (don't auto-migrate config-changed runs); require explicit operator choice. Today's "best-effort remap" wording is the kind of thing that silently corrupts journeys.

3. **Add the dedupe key + transactional advance.** (P0-2) Add `(run_id, node_id, attempt_count)` UNIQUE partial index on `pending_actions` and require the FIRE_VB_CALL handler to use `INSERT OR IGNORE`. Combine with making the "advance current_node_id" and the side-effect-write a single SQLite transaction. Without this, a single tick crash mid-handler doubles a customer's call count and the global 3/day cap is your only backstop — too thin.

Everything else (simpleeval typing, observability, ASSIGN_AGENT target table, ct_profile response shape, enrollment cap, etc.) follows in priority. The three above are the architectural seams; the rest are details that will surface during implementation if the three are right.
