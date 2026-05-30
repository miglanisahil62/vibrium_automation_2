# Vibrium Workflow Engine — Phased Build Plan

> Companion to `~/.claude/plans/this-is-for-vibrium-wiggly-puffin.md` (the approved architecture). This document is the **execution plan**: every phase is scoped, dependency-ordered, audit-gated, and assignable to a separate creator agent.

> **Plan rev:** 1.1 — incorporates master-auditor PASS_WITH_NOTES findings (3 P0s, 7 P1s) on rev 1.0. Audit report at `docs/plan_audit_2026-05-30.md` (master-auditor agent ab5e15ba919f964e3).

## Phase Closure Definition (applies to ALL phases)

A phase is "closed" only when ALL of the following are true:

1. **Code written + tests green** in this repo.
2. **master-auditor PASS or PASS_WITH_NOTES** on the deliverables (critical-surface files reviewed individually, not bundled).
3. **For cross-repo phases (3, 9, 10, 11, 13):** PR merged to main of the cross-repo + deployed to AWS + heartbeat green for ≥ 24h post-deploy + verifiable observable side-effects (e.g., `customer_call_audit` rows appearing for adhoc fires).
4. **Phase artifacts committed** to this repo (`docs/phase_<N>_close.md` summarizing what was built, what was tested, and what was deferred).

Wave N+1 does not start until every phase in Wave N is **closed** per the above definition.

---

## Execution model

- **Each phase** has: (a) clear scope, (b) acceptance criteria, (c) inputs from prior phases, (d) outputs consumed by later phases, (e) dependencies, (f) auditor verdict before closure.
- **Auditor gate per phase:** every phase ends with a `master-auditor` review (and `stashfin-qa-backend` / `stashfin-qa-ui` where applicable). A phase is not "closed" until the auditor returns PASS or PASS_WITH_NOTES.
- **Parallelism:** independent phases run as concurrent creator agents. The dependency graph below pins what can run in parallel.
- **Integration phase:** after independent phases close, an integration phase wires everything into one runnable pipeline + runs end-to-end shadow tests.
- **All code lives in** `/Users/sahil.m/vibrium-workflow/` (this repo). Only minimal surgical changes are made to `vibrium-automation/` (the adhoc system).

---

## Dependency graph

```
        Phase 0 (Repo Foundation)
              │
              ▼
   ┌──────────┼──────────┬──────────┐
   ▼          ▼          ▼          ▼
Phase 1   Phase 2   Phase 3    (these 4 can run in parallel after Phase 0)
Schema    CT       Shared
+ DB      Profile  Audit + Gate
              │          │
              └────┬─────┘
                   ▼
       ┌───────────┼───────────┬──────────┐
       ▼           ▼           ▼          ▼
   Phase 4    Phase 6     Phase 7    Phase 8
   Handlers   Scheduler   Ingest     Enrollment
       │       │           │          │
       └───────┼───────────┴──────────┘
               ▼
           Phase 5 (Executor) + Phase 9 (Orchestrator wraps all daemons)
                                │
                                ▼
                       Phase 10 (Ops Console routes + API)
                                │
                                ▼
                       Phase 11 (Ops Console UI editor)
                                │
                                ▼
                       Phase 12 (Seed workflow + E2E shadow)
                                │
                                ▼
                       Phase 13 (Live cutover)
```

---

# Phase 0a — tag_group Smoke Test (REVISED 2026-05-30 — Sahil directive: no live fires)

**Why this exists:** The entire disposition-wakeup correctness rests on `tag_group` surviving CT externaltrigger → bot → CRM → `collection_comment_data`. We need to confirm this WITHOUT firing live test triggers.

**Scope:** Read-only verification using a known wasim cohort customer (`8968249` from `wasim_ptp_21_may.csv`). No CT externaltrigger fires. No CT property writes. No customer impact.

**Deliverables:**
1. **CT profile GET fixture** — call `GET /1/profile.json?identity=8968249` once; save full JSON to `workflow/tests/fixtures/ct_profile_response.json`. Confirm the 4 target properties (`COLL_collection_risk_segmentation`, `coll_notification_replied`, `coll_bot_calling`, `DPD`) are extractable — record their actual paths in the response. This is the Phase 2 fixture pinned upfront.
2. **Redshift schema check** — `DESCRIBE collection_comment_data` (or `INFORMATION_SCHEMA.COLUMNS` equivalent). Confirm whether `tag_group` is a real column. Record column list in `docs/phase_0a_collection_comment_data_schema.md`.
3. **If `tag_group` column exists:** query for existing non-null values across the last 30 days to learn:
   - Which campaigns currently use `tag_group`.
   - What format/values they use.
   - Whether bot-recorded vs agent-recorded comments both carry it.
   - Save findings to `docs/phase_0a_tag_group_observed_values.md`.
4. **If `tag_group` column does NOT exist:** redesign the disposition-wakeup join. The PHASES.md Phase 7 fallback ("triangulation by `customer_id` + `fired_at_ist` + `comment_create_date`") becomes the **primary** join. Update PHASES.md accordingly.
5. **Decision document** — `docs/phase_0a_decision.md` recording: tag_group available? survival hypothesis (since we didn't fire, this is an inference from existing data)? primary vs fallback join?

**Acceptance:**
- `workflow/tests/fixtures/ct_profile_response.json` committed.
- `docs/phase_0a_collection_comment_data_schema.md` committed with the actual column list.
- `docs/phase_0a_decision.md` committed with the design decision.
- If decision = "tag_group missing or unreliable": PHASES.md updated to reflect primary-join change.

**Dependencies:** none (runs before Phase 0).

**Audit gate:** master-auditor on the decision document.

**Owner:** Can be executed inline (small smoke test) or as a creator agent if Sahil prefers.

---

# Phase 0 — Repo Foundation

**Scope:** Create the project skeleton; pin tool versions; set up tests harness; encode the directory layout.

**Deliverables:**
- Directory tree: `workflow/`, `workflow/agents/`, `workflow/agents/workflow_handlers/`, `workflow/migrations/`, `workflow/launchd/`, `workflow/tests/`, `shared/`, `scripts/`, `state/`, `docs/`.
- `requirements.txt` pinning:
  - `simpleeval==0.9.13`
  - `requests==2.32.3`
  - `python-dateutil==2.9.0`
  - `pytest==8.3.5`, `pytest-mock==3.14.0`
- `setup.py` or `pyproject.toml` declaring the `vibrium_workflow` package.
- `pytest.ini` with `testpaths = workflow/tests` and `addopts = -q --strict-markers`.
- `.gitignore`, `README.md`, `PHASES.md` (this file), `CLAUDE.md` (project-specific instructions for future Claude sessions).
- `config.json.template` and `config.example.json` (cred paths, DB paths, shadow_mode default).
- `state/` is gitignored — DB files live there.

**Acceptance:**
- `python3 -c "import workflow"` succeeds.
- `pytest workflow/tests -q` runs (0 tests, exits 0).
- `pip install -r requirements.txt` resolves cleanly.
- `pyproject.toml` pins `python = ">=3.11,<3.13"` (P2-1 fix).
- External-link shim: `external/vibrium_automation_scripts -> ~/vibrium-automation/scripts` symlink (P1-3 fix: locked to "symlink" approach, not pip-install-e). `scripts/check_external_links.py` exits non-zero with a clear message if the symlink target is missing.
- Drawflow vendor stub created with SOURCES.md placeholder (filled in Phase 11) (P2-3 fix).

**Dependencies:** Phase 0a (the tag_group decision must exist; design may pivot based on its outcome).

**Audit gate:** master-auditor on `requirements.txt` + `pyproject.toml` + directory layout. Verify pinned versions, no security-known-bad libs, no missing dev tooling.

**Owner:** can be created directly (small, deterministic; no creator-agent needed).

---

# Phase 1 — Schema + Migrations

**Scope:** All 11 tables in `state/workflow.db` + the single additive table in `state/vibrium.db`.

**Deliverables:**
- `workflow/migrations/001_init.py` — idempotent `CREATE TABLE IF NOT EXISTS` for all 11 tables in `workflow.db` (workflows, workflow_versions, workflow_runs, wf_pending_actions, wf_decision_log, workflow_node_log, workflow_admin_log, wf_kill_switch, wf_agent_events, agent_assignments, schema_version). Schema matches the architecture plan exactly.
- `workflow/migrations/002_customer_call_audit.py` — `CREATE TABLE IF NOT EXISTS customer_call_audit` on `vibrium.db` (additive only; no ALTER on existing tables).
- `workflow/wf_store.py` — connection helper (`get_workflow_db()`, `get_vibrium_db_audit()`); WAL mode enabled on both; `caller_owned_connection` support for transactional handlers.
- `workflow/migrations/runner.py` — applies pending migrations; reads `schema_version` to skip already-applied.
- `workflow/tests/test_migrations.py`:
  - Empty workflow.db → run 001 → schema matches expected.
  - Run 001 twice → idempotent (no error, no double-apply).
  - Run 002 against a fixture vibrium.db copy → only `customer_call_audit` added.
  - Existing vibrium.db tables byte-identical after 002.

**Acceptance:**
- `python3 -m workflow.migrations.runner` applies all migrations from empty.
- Re-running is a no-op.
- All test cases pass.
- `sqlite3 state/workflow.db ".schema"` produces the expected 11-table schema.

**Dependencies:** Phase 0.

**Audit gate:** master-auditor on `wf_store.py` + the two migration scripts. Verify: idempotency, no `ATTACH DATABASE` from workflow code to vibrium.db for write, indexes correct, CHECK constraints correct.

---

# Phase 2 — CleverTap Profile Module

**Scope:** A standalone HTTP client for reading + writing CT user properties.

**Deliverables:**
- `workflow/clevertap_profile.py`:
  - `get_profile(identity: str) -> dict | None` — GET `/1/profile.json?identity=<id>`; returns parsed `profileData`; returns None on 404. `timeout=(5, 30)`. Honors `Retry-After` on 429.
  - `bulk_get_profiles(identities: list[str], concurrency: int = 5) -> dict[str, dict | None]` — ThreadPoolExecutor; honors Retry-After; halves concurrency on second 429 within 60s; max 3 retries per identity then marks None.
  - `set_profile(identity: str, properties: dict, dry_run: bool = False) -> SetResult` — POST `/1/upload`; parses `unprocessed[]` per CT KB hard rule; returns `SetResult(success: bool, error_code: int | None, raw_response: dict)`.
  - Credential loading: copy pattern from `~/vibrium-automation/scripts/clevertap_trigger.py:28-40` (cred cache + mtime invalidation). Reads `CT_CREDS_FILE` env var, falls back to the existing config path.
  - Module-level session pool (matches `clevertap_trigger.py:22-66` pattern to avoid FD leak under ulimit-1024).
- `workflow/tests/fixtures/ct_profile_response.json` — pinned from a live `GET /1/profile.json` call against a known test customer. Verifies the 4 target properties exist at known paths.
- `workflow/tests/test_clevertap_profile.py`:
  - Mocked 200 response → returns parsed dict.
  - Mocked 404 → returns None.
  - Mocked 429 with Retry-After: 5 → sleeps 5s, retries.
  - Mocked partial unprocessed[] in set_profile → returns success=False with error_code.
  - Bulk fetch with 5 mocked identities → returns 5 entries (some None) — concurrency works.

**Acceptance:**
- Live call (one-time, manual): `python3 -c "from workflow.clevertap_profile import get_profile; print(get_profile('<test_cid>'))"` returns a dict containing the 4 target properties.
- All test cases pass.
- Fixture file committed.

**Dependencies:** Phase 0. (No dependency on Phase 1; independent.)

**Audit gate:** master-auditor on `clevertap_profile.py`. Verify: `Retry-After` honored (not hardcoded backoff), `unprocessed[]` inspected (not just `status:"success"`), `timeout=` on every requests call, no PII logged.

---

# Phase 3 — Shared Audit + Pre-Call Gate Refactor (REVISED per P0-1)

**Scope:** Make `pre_call_gate.py` callable by both adhoc and workflow schedulers WITHOUT regressing the existing batched hot-path. Introduce `customer_call_audit` as the shared single-source-of-truth for the per-customer daily cap.

**Where it lives:** This phase TOUCHES the existing `vibrium-automation` repo (surgical edits) AND adds a new library in `vibrium-workflow`.

**Existing caller inventory (must be migrated, all named):**
- `scheduler.py` (single-customer-at-fire-time path) — calls `check_cap_and_cooldown(customer_id, today_fired_rows)`.
- `morning_catchup.py` (if present — verify in Phase 0) — batched path.
- `manual_trigger.py` — single-customer.
- `cohort_runner.py` — batched eligibility check.
- `pre_call_gate.py:batch_check_*` — batched paths (per pre_call_gate.py:181-231).

**Deliverables in `vibrium-workflow`:**
- `shared/customer_call_audit.py`:
  - `record_fire(customer_id, source, run_id, cohort_name, ct_response_status, ct_error, vibrium_db_path)` — single INSERT.
  - `batch_count_today(customer_ids: list[int], vibrium_db_path: Path) -> dict[int, int]` — bulk read for the batched hot path. ONE query, not N.
  - `batch_last_fire_at(customer_ids: list[int], …) -> dict[int, str]` — for cooldown check on the batched path.

**Deliverables in `vibrium-automation` (surgical patches; PR opened on a branch; FEATURE-FLAG gated):**
- `scripts/pre_call_gate.py`:
  - Add `is_callable_now() -> GateResult` (P0-3 fix from prior audit — RBI window check).
  - Add `customer_daily_cap(customer_id, vibrium_db_path) -> GateResult` — single-customer path.
  - **Keep `check_cap_and_cooldown(customer_id, today_fired_rows)` signature intact** for backward compat. Add internal feature flag `USE_SHARED_AUDIT_CAP` (env var) — when False (default), uses today's `today_fired_rows` path; when True, reads from `customer_call_audit`. Lets us deploy + roll back independently.
  - Add `batch_check_with_shared_audit(customer_ids, …)` — new function using `customer_call_audit.batch_count_today()` for the batched callers.
  - Rework `check()` to call `is_callable_now()` FIRST always, then the (flag-gated) cap.
- `scripts/scheduler.py` — one line after every successful CT trigger: `audit.record_fire(..., source='adhoc')`. Behind the same flag (`USE_SHARED_AUDIT_CAP`) for symmetry — when off, no-op.
- `scripts/ingest.py` — one line: add `WHERE tag_group NOT LIKE 'vbwf:%'` to the collection_comment_data query. **Pre-req:** confirm `tag_group` column exists in `collection_comment_data` (Phase 0a output). If not, the filter form changes.
- `scripts/clevertap_trigger.py` — accept optional `tag_group` kwarg + pin where it lands in the CT payload (the JSON path decided in Phase 0a). Default None — backwards compatible (P2-5 fix).

**Tests in vibrium-workflow:**
- `workflow/tests/test_customer_call_audit.py`:
  - `record_fire('CID123', source='adhoc')` inserts one row.
  - `batch_count_today([CID1, CID2, CID3])` with 2 adhoc + 1 workflow row → correct map.
  - `customer_daily_cap` with 3 fires today → False; with 0 fires → True.
  - Concurrency: 10 parallel writers → no missed rows (WAL test).

**Tests in vibrium-automation (NAMED files, all must be green):**
- `tests/test_pre_call_gate.py` — existing tests + new tests for `is_callable_now()` + new tests for flag-gated cap.
- `tests/test_scheduler.py` — adhoc scheduler test suite, unchanged behavior with flag=False.
- Batched-path runtime regression check: `morning_catchup.py` (or equivalent) — runtime before/after must be within 20% (P0-1 fix).

**Acceptance (HARD gates, all must pass):**
- Flag=False: every existing test in `vibrium-automation` is byte-identical green.
- Flag=True: new tests green; morning-catchup runtime within 20% of baseline.
- Live deployment: flag flipped to True; `customer_call_audit` rows appear within 5 minutes of any adhoc fire; heartbeat green for ≥24h.
- Rollback rehearsal: flip flag back to False mid-day; adhoc behavior reverts immediately; no row drift.

**Dependencies:** Phase 0a (tag_group decision), Phase 0 (repo + external symlink), Phase 1 (`customer_call_audit` migration must apply on vibrium.db before deploy).

**Phase 3 Closure Definition (specific):** Code merged → deployed to AWS → flag flipped to True → heartbeat green for ≥24h → `customer_call_audit` rows confirmed for adhoc fires → rollback rehearsal performed and reverted. ONLY THEN does Wave 2 start.

**Audit gate:** master-auditor on `pre_call_gate.py` (critical-surface) + `customer_call_audit.py` + `clevertap_trigger.py` patch. Specifically audit: (a) signature backward-compat, (b) flag wiring, (c) `tag_group` payload path matches Phase 0a decision, (d) no PII in audit table rows. PASS required before merge.

---

# Phase 4 — Node Handlers (SPLIT into 4a / 4b / 4c per P1-6)

Split into 3 sub-phases so the critical-surface handlers get individual master-auditor passes and Phase 5 (executor) can start as soon as 4a closes.

## Phase 4a — Core Handlers (unblocks Phase 5)

**Scope:** The 6 handlers required for the smallest end-to-end journey: enroll → fetch → condition → fire → await → terminate.

**Deliverables:**
- `workflow/agents/workflow_handlers/types.py` — `NodeResult`, `Run`, `NodeConfig` dataclasses.
- `workflow/agents/workflow_handlers/enroll.py`
- `workflow/agents/workflow_handlers/fetch_ct_props.py` (uses Phase 2; type coercion per architecture rev 3 §4)
- `workflow/agents/workflow_handlers/condition.py` (simpleeval, `MAX_STRING_LENGTH=1024, MAX_POWER=100, functions={}`, names-only access; rejects SyntaxError + NameError with structured error)
- `workflow/agents/workflow_handlers/fire_vb_call.py` (INSERT OR IGNORE into wf_pending_actions; uses Phase 1 dedupe index)
- `workflow/agents/workflow_handlers/await_disposition.py`
- `workflow/agents/workflow_handlers/terminate.py`
- `workflow/agents/workflow_handlers/__init__.py` — REGISTRY dict (partial, 6 entries).
- Tests: one class per handler; happy + 2 edge cases each.

**Acceptance:** all 6 handlers pass tests; registry resolves their types; condition handler rejects malformed exprs cleanly.

**Audit gate:** master-auditor on `fire_vb_call.py`, `condition.py`, `await_disposition.py` INDIVIDUALLY (each is critical-surface). Group review on `enroll.py`, `fetch_ct_props.py`, `terminate.py`.

## Phase 4b — Branching + Scheduling Handlers (parallel with Phase 5)

**Scope:** Handlers needed for the full call_loop subgraph in the seed workflow.

**Deliverables:**
- `workflow/agents/workflow_handlers/switch.py`
- `workflow/agents/workflow_handlers/wait_until.py` (IST-naive `YYYY-MM-DD HH:MM:SS`; reject other formats; late wake = next tick)
- `workflow/agents/workflow_handlers/branch_on_disposition.py` — enum: `NOOP, RETRY, PTP_CALL, AGREE_EOD_CALL, CALLBACK_CALL, RTP_NEEDS_LLM, ESCALATE`; `default` edge mandatory.
- `workflow/agents/workflow_handlers/counter.py`
- Tests + registry updates.

**Audit gate:** master-auditor INDIVIDUALLY on `branch_on_disposition.py` (critical-surface — drives every post-call decision). Group review on the other 3.

## Phase 4c — Side-Effect Handlers (parallel with 4b)

**Scope:** Handlers that write to CT or to external queues.

**Deliverables:**
- `workflow/agents/workflow_handlers/set_ct_prop.py` (uses Phase 2 `set_profile`; parses `unprocessed[]`)
- `workflow/agents/workflow_handlers/assign_agent.py` (INSERT into `agent_assignments`)
- Tests + registry updates.

**Audit gate:** master-auditor INDIVIDUALLY on `set_ct_prop.py` (critical-surface — writes to CT). Group review on `assign_agent.py`.

**Dependencies (all of 4a/4b/4c):** Phase 1 (schema), Phase 2 (CT profile module).

---

# Phase 5 — Workflow Executor (the tick loop)

**Scope:** `WorkflowAgent.tick()` — pulls ready runs, executes one handler per run, advances state transactionally, emits events.

**Deliverables:**
- `workflow/agents/workflow.py`:
  - `WorkflowAgent.tick(dry_run: bool = False) -> AgentResult`.
  - Kill-switch check (reads `wf_kill_switch`).
  - Version cache (per-tick: `{version_id: parsed_graph}`).
  - For each ready run: transactional advance (handler side-effect + current_node_id update + workflow_node_log append in one txn).
  - Error handling: exceptions → run.status = ERROR with traceback in `workflow_node_log.side_effect`.
  - `emit_wf_agent_event("workflow", processed, advanced, errored, orphaned)` per tick.
  - Heartbeat: `heartbeat("workflow_agent", "ok"|"down")`.
  - `--dry-run` flag: logs intended transitions with `dry_run=1`; no DB writes besides the log.
- `workflow/tests/test_executor.py`:
  - 6-node graph: enroll → fetch → condition → wait → fire → await → terminate. Tick repeatedly; assert advancement.
  - Inject failure mid-handler → run.status=ERROR, transaction rolled back.
  - Version pinning: run on v1 stays on v1 even after v2 saved.
  - Dry-run: no writes except `workflow_node_log` rows with `dry_run=1`.

**Acceptance:**
- Synthetic test customer flows through a 6-node graph end-to-end in a unit test.
- Idempotency: kill tick mid-handler → re-tick → no duplicates.
- Heartbeat row appears in `wf_agent_events` after every tick.

**Dependencies:** Phase 1 (schema), Phase 4 (handlers).

**Audit gate:** master-auditor on `workflow.py`. Verify: transactional boundaries, error handling, no orphaned connections, kill-switch respected first thing, dry-run mode complete.

---

# Phase 6 — Workflow Scheduler

**Scope:** Fires `wf_pending_actions` rows; calls pre_call_gate; records audit; invokes `clevertap_trigger.trigger()`.

**Deliverables:**
- `workflow/workflow_scheduler.py`:
  - Reads `wf_pending_actions WHERE status='PENDING' AND scheduled_at_ist <= now`.
  - Marks `FIRING_IN_PROGRESS` before HTTP call.
  - Calls `pre_call_gate.check(customer_id, source='workflow')`.
  - If gate passes: calls `clevertap_trigger.trigger(customer_id, tag_group=f"vbwf:{run_id}:{node_id}:{attempt_count}")` (uses the existing library from vibrium-automation).
  - Records via `customer_call_audit.record_fire(..., source='workflow', run_id=run_id, ...)`.
  - Updates status to FIRED / SUPPRESSED / ERROR.
- `workflow/tests/test_scheduler.py`:
  - Mocked CT trigger + audit; assert fire path.
  - Gate fails → SUPPRESSED.
  - CT error → ERROR; pending row retained.
  - Tag_group format pinned.

**Acceptance:**
- Synthetic queue of 5 rows, all eligible → all fire, audit table has 5 new rows.
- Outside RBI window → 0 fire, all marked SUPPRESSED with reason.

**Dependencies:** Phase 1 (schema), Phase 3 (pre_call_gate + audit lib), and **a sym-installed import path** to `vibrium-automation/scripts/clevertap_trigger.py`.

**Audit gate:** master-auditor on `workflow_scheduler.py` (critical-surface). Plus stashfin-qa-backend if it exposes any HTTP surface.

---

# Phase 7 — Workflow Ingest

**Scope:** Reads `collection_comment_data` filtered by `tag_group LIKE 'vbwf:%'`; parses `vbwf:R:N:K`; populates `wf_decision_log`; wakes matching runs.

**Deliverables:**
- `workflow/workflow_ingest.py`:
  - Watermarked Redshift query (reuses existing `vibrium-automation` Redshift conn helper as library).
  - Parses tag_group → extracts run_id, node_id, attempt_count.
  - Classifies disposition into action_class (reuses `decision_v2.classify` as library — read-only consume of rules.json).
  - INSERTs `wf_decision_log`.
  - Sets `workflow_runs.ready_at_ist = now()` for matching run (only if currently WAITING at AWAIT_DISPOSITION with matching attempt_count and comment_create_date >= entered_node_at_ist).
- `workflow/tests/test_ingest.py`:
  - Mocked Redshift row with `tag_group=vbwf:42:N1:3` → wf_decision_log row written; matching run wakes.
  - Stale disposition (comment_create_date < entered_node_at_ist) → does NOT wake.
  - Two open workflows for same customer; only the one with matching `run_id` wakes.
  - Missing tag_group + fallback: most-recent FIRED row matched if `vbwf:` absent (per the spec's fallback path).

**Acceptance:**
- Synthetic Redshift fixture with 3 rows (1 matching, 1 stale, 1 wrong-cohort) → only matching one wakes.

**Dependencies:** Phase 1 (schema), Phase 6 (scheduler — to know how tag_group is written).

**Audit gate:** master-auditor on `workflow_ingest.py` (critical-surface — it's the disposition-wakeup join). Special attention to the lower-bound guard.

---

# Phase 8 — Enrollment Poller

**Scope:** Polls CT for entry-condition matches and creates new `workflow_runs` rows; idempotent; capped; kill-switch-aware.

**Deliverables:**
- `workflow/enrollment_poller.py`:
  - For each ACTIVE workflow with `enrollment_trigger="ct_property_watcher"`: pull a sample of customer_ids (from a configurable source — `collection_view` SQL query or a CSV), bulk-fetch CT profiles via Phase 2, evaluate entry CONDITION, INSERT OR IGNORE into `workflow_runs` keyed on `(workflow_id, customer_id, enrollment_key)`.
  - Enrollment-key default: `"{customer_id}_{YYYY-MM-DD}"` — operator-configurable per workflow.
  - Caps: `max_new_enrollments_per_day` (workflow config), `max_new_enrollments_per_tick=200` (global).
  - Hard abort if any single tick would create > 5000 — exits non-zero + alert.
  - Respects `wf_kill_switch` first thing.
  - Respects RBI window via `pre_call_gate.is_callable_now()`.
- `workflow/tests/test_enrollment_poller.py`:
  - 10 candidates, 4 match condition → 4 runs created. Re-run → 0 new (idempotent).
  - Per-tick cap=2 → only first 2 created.
  - Outside window → no enrollment.
  - Kill switch active → no enrollment.

**Acceptance:**
- Synthetic profiles fixture + entry condition → expected enrollment count.

**Dependencies:** Phase 1 (schema), Phase 2 (CT profile), Phase 3 (gate).

**Audit gate:** master-auditor on `enrollment_poller.py` (blast-radius critical). Verify the 5000-row hard-abort, the daily cap, and the kill-switch check.

---

# Phase 8.5 — Observability + Alerts (NEW per P1-7)

**Scope:** Alerting + runbook for the workflow system. Heartbeat is in Phase 9; this phase covers what to do when a heartbeat goes red.

**Deliverables:**
- `workflow/alerts.py` — runs every 15 min via its own launchd plist; emits page to sahil.miglani@stashfin.com if:
  - Any daemon heartbeat is `down` for >30 min.
  - Any `workflow_runs.status='ERROR'` for >2h.
  - Any `workflow_runs.status='WAITING'` for >7 days.
  - `wf_kill_switch.action='KILL'` is set without a subsequent `RESUME` for >1h.
  - Any single tick processed > TICK_BATCH_LIMIT × 0.9 rows (queue building up).
- `workflow/launchd/com.sahil.workflow.alerts.plist`.
- `docs/runbook.md` — what to do when each alert fires. Step-by-step: pause via kill_switch, inspect `workflow_node_log`, repair orphan via UI, escalate to Sahil.
- Tests: each failure mode simulated; alert payload generated in dry-run; verify email body shape (no live SMTP).

**Acceptance:**
- 5 simulated failure modes each produce the expected alert payload.
- `docs/runbook.md` is a one-page operator-facing doc.

**Dependencies:** Phase 1 (schema).

**Audit gate:** master-auditor on `alerts.py` + runbook review.

---

# Phase 9 — Workflow Orchestrator + Launchd

**Scope:** Wraps the four daemons (executor tick, scheduler, ingest, enrollment_poller) and the digest emailer into separate processes with their own launchd plists. Registers heartbeats so `pipeline-integrity-auditor` skill picks them up.

**Deliverables:**
- `workflow/workflow_orchestrator.py` — single entrypoint that imports + runs the appropriate daemon based on `--mode` flag (`executor` / `scheduler` / `ingest` / `enrollment` / `digest`).
- 5 launchd plists in `workflow/launchd/`. **Pattern (P1-5 fix):** all interval-based plists use `StartInterval` (300s / 900s) — NOT 132-row `StartCalendarInterval`. The window check is done by the daemon entrypoint itself via `pre_call_gate.is_callable_now()` (and the narrower 08:00–18:00 check for enrollment). Outside-window ticks emit an observable "skipped — outside window" heartbeat rather than silent no-op.
  - `com.sahil.workflow.executor.plist` (`StartInterval=900`, 15 min, runs all day; advances waits + transitions even outside the call window)
  - `com.sahil.workflow.scheduler.plist` (`StartInterval=300`, 5 min; entrypoint guards with `is_callable_now()`, exits no-op outside 08:00–19:00 IST with a "skipped" heartbeat)
  - `com.sahil.workflow.ingest.plist` (`StartInterval=900`, 15 min, runs all day; dispositions arrive whenever)
  - `com.sahil.workflow.enroll.plist` (`StartInterval=1800`, 30 min; entrypoint guards with narrower 08:00–18:00 window)
  - `com.sahil.workflow.digest.plist` (`StartCalendarInterval` daily 09:00 IST — single firing per day, so calendar form is correct here)
- `workflow/workflow_digest.py` — daily summary email: per-workflow active / waiting / erroring >24h / orphaned counts → sahil.miglani@stashfin.com.
- `workflow/health.py` — heartbeat writer to be added to the `agents_health_check.py` registry in vibrium-automation (one-line edit).

**Acceptance:**
- All 5 plists load via `launchctl load` without error.
- Manual run of each `--mode` exits 0 on a fresh state.
- After one cycle, `wf_agent_events` has 4 rows (one per non-digest daemon).

**Dependencies:** Phases 5, 6, 7, 8.

**Audit gate:** master-auditor on `workflow_orchestrator.py` + `workflow_digest.py` + each plist (verify absolute paths, env vars, working directory, redirect of stdout/stderr to logs). pipeline-integrity-auditor verifies the new daemons appear in its registry.

---

# Phase 10 — Ops Console v2 — Routes + API

**Scope:** FastAPI routes and JSON APIs for the workflow management UI. Backend only — no UI templates yet.

**Where it lives:** Edits in `~/ops_console_v2/`.

**Deliverables (in ops_console_v2):**
- `ops_console_v2/services_workflow.py` — wraps `workflow.db` reads (list_workflows, get_workflow, list_runs, get_run_log) + writes (create_workflow, save_version, activate, repair_orphan).
- `ops_console_v2/app.py` — register 9 new routes:
  - `GET /workflows` (list page) — Phase 11 will render
  - `GET /workflows/{id}` (detail page) — Phase 11
  - `GET /workflows/{id}/runs` (runs table) — Phase 11
  - `GET /workflows/{id}/runs/{run_id}` (per-run timeline) — Phase 11
  - `POST /api/workflows` (create draft)
  - `POST /api/workflows/{id}/version` (save graph_json + validate)
  - `POST /api/workflows/{id}/activate` (with confirmation modal payload check)
  - `POST /api/workflows/{id}/preview-enrollment` (sampled projection)
  - `POST /api/workflows/{id}/runs/{run_id}/repair` (advance orphan)
- Remove `/workflows*` from `STUB_ROUTES` in app.py.
- Add sidebar entry `{{ nav('/workflows', 'Workflows', icon_svg) }}` in `templates/base.html`.
- `workflow/validation.py` — server-side validator (used by `/api/workflows/{id}/version`):
  - Exactly one ENROLL node.
  - Every non-terminal node has all required edges.
  - No cycles unless through WAIT_UNTIL.
  - Every CONDITION.expr parses under simpleeval.
  - Every scratchpad key reference has an upstream writer.
  - Every BRANCH_ON_DISPOSITION enum value is canonical.
  - Every node_id is UUID.

**Acceptance:**
- `/stashfin-qa-backend` PASS on all 5 POST routes + 4 GET routes.
- Dry-run / approval gates enforced on `/activate`.
- Invalid graph_json rejected with structured error.

**Dependencies:** Phase 1 (schema), Phase 5 (executor — provides ctx for repair).

**Audit gate:** master-auditor on routes + services + validation. `/stashfin-qa-backend` PASS.

---

# Phase 11 — Ops Console v2 — UI (Drawflow Node Editor)

**Scope:** Visual node editor; runs view; run-timeline view. Apple-token compliant.

**Deliverables (in ops_console_v2):**
- `templates/workflows_list.html`
- `templates/workflow_detail.html` — Drawflow editor on the left, node-config form on the right, JSON-fallback toggle in the toolbar.
- `templates/workflow_runs.html`
- `templates/workflow_run_detail.html`
- `static/drawflow/` (vendored Drawflow v0.0.59 — JS + CSS) + `static/drawflow/SOURCES.md` recording: source URL `https://github.com/jerosoler/Drawflow/releases/tag/0.0.59`, SHA256 of JS + CSS files, vendor date (P2-3 fix).
- `static/workflow_editor.js` — wires Drawflow to the API; per-node-type config form rendering; save/load round-trip; activation modal with bucket-counts.
- `static/workflow_editor.css` — overrides Drawflow defaults with `var(--accent)`, `var(--bg-elevated)`, etc.

**Acceptance:**
- `/stashfin-qa-ui` PASS: sidebar entry renders, both Vibrium and Workflows entries work, editor loads, save round-trips, JSON-fallback toggle works.
- `ui-auditor` PASS on all 4 templates (Apple design system compliance).
- **Accessibility (P1-2 fix):**
  - Tab order across Drawflow nodes is deterministic (left-to-right, top-to-bottom) — verified via Playwright test.
  - Every node-config form field has a `<label for>` or `aria-label`.
  - JSON-fallback view is feature-parity with graph view (save + activate works from JSON alone).
  - ESC closes any open modal; Enter on a focused node opens the config drawer.
  - WCAG-basics check added to `/stashfin-qa-ui` scope.

**Dependencies:** Phase 10.

**Audit gate:** `ui-auditor` PASS, `/stashfin-qa-ui` PASS.

---

# Phase 12 — Seed Workflow + End-to-End Shadow Test

**Scope:** Build `vb_collections_v1` (VB_Prompt_Doc 5 segments) as a graph_json file; activate in shadow_mode; enroll 100 test customers; verify branch distribution matches expectation.

**Deliverables:**
- `seed_workflows/vb_collections_v1.json` — full graph definition (~25 nodes covering high/mid × WA available/unavailable × 4 mid sub-variants + call_loop).
- `seed_workflows/vb_collections_v1.expected_distribution.json` (NEW per P1-1) — the expected per-branch share table, derived from `vb_collections_v1.json` traversed against the test cohort's CT property distribution. Concrete numbers (e.g., `{"high_wa_avail": 0.32, "high_wa_unavail": 0.18, ...}`).
- `scripts/seed_workflow.py` — loads JSON → POSTs to `/api/workflows` → activates in shadow_mode.
- `scripts/e2e_shadow_test.py` — enrolls 100 test customers; ticks executor 50 times; emits per-customer journey path; writes structured `docs/shadow_run.json` (NEW per P2-2) for auditor ingestion; verifies expected branch distribution.
- `docs/shadow_run_2026-XX-XX.md` — narrative summary referencing `shadow_run.json`.

**Acceptance (split into HARD and SOFT per P1-4):**

**Hard gates (any failure = NEEDS_FIX):**
- 100 customers enrolled → after 50 ticks, all in terminal state (or expected WAITING state).
- `live_ct_triggers == 0` (verify via CT dashboard + `clevertap_trigger.py` log).
- `customer_call_audit_rows_for_shadow_cohort == 0`.
- `wf_pending_actions.status == 'SHADOW_FIRED'` for every fire on the shadow cohort.

**Soft gates (deviation reviewed by Sahil, not auto-blocking):**
- Observed each-branch share within ±5 percentage points of expected (per `expected_distribution.json`).
- No branch with expected ≥5% observed at 0%.

**Dependencies:** Phases 1–11.

**Audit gate:** master-auditor on seed JSON + scripts. Final go/no-go review by Sahil before Phase 13.

---

# Phase 13 — Live Cutover

**Scope:** Sahil "go" approval → flip shadow_mode off for a small named cohort → compare outcomes to current cohort-runner-based baseline → promote if match within tolerance.

**Deliverables:**
- Pick a named test cohort (e.g., wasim_X_test — small, well-known group).
- Flip `vb_collections_v1.shadow_mode = 0`.
- Run for 48 hours.
- `docs/live_run_2026-XX-XX.md` — outcome comparison vs adhoc baseline, decision to promote / hold / revert.

**Acceptance (rewritten per P1-1 — workflow-internal criteria, NOT cross-system outcome comparison):**
1. **Zero per-customer cap violations** (verified against `customer_call_audit` — no customer with >3 fires/day across both systems combined).
2. **Disposition wakeup accuracy ≥98%** on a manually-verified sample of 100 awaited dispositions — i.e., every wakeup pointed to the run that actually fired the call.
3. **No workflow_run stuck in `ERROR` for >2h** during the 48h window (alerted via Phase 8.5 alerts).
4. **Net VB call volume within 20%** of what cohort_runner would have produced on the same customers over the same window (volume comparison, not outcome — explicit because the two systems do different things and an outcome match would not be meaningful).
5. Manual audit by Sahil: spot-check 10 customer journeys end-to-end → all routed correctly per the seed workflow graph.

**Dependencies:** Phase 12.

**Audit gate:** Sahil's explicit "go" + master-auditor PASS + pipeline-integrity-auditor PASS on all 5 daemons live.

---

## Parallelism plan (revised per P0-3, P1-6)

**Wave 0 (sequential, pre-coding):** Phase 0a (tag_group spike). 1 creator agent. Decision recorded before Wave 0a.

**Wave 0a (sequential):** Phase 0 (repo foundation). Single small task.

**Wave 1 (after Phase 0 closed):** Phases 1, 2, 3 in parallel — 3 creator agents. **Phase 3 closure includes AWS deploy + 24h heartbeat (per Phase Closure Definition).** Wave 2 does NOT start until Phase 3 is deployed and observable.

**Wave 2 (after Wave 1 fully closed AND deployed):** Phase 4a (core handlers) + Phase 7 (ingest) + Phase 8 (enrollment) in parallel — 3 creator agents. Phase 4a closes first; the others can extend slightly past it.

**Wave 3 (after 4a closes):** Phase 5 (executor), Phase 4b (branching), Phase 4c (side-effects), Phase 6 (scheduler) in parallel — 4 creator agents. (Phase 6 requires Phase 3 deployed + Phase 0 symlink; Phase 5 only requires Phase 4a.)

**Wave 4 (after Wave 3 closes):** Phase 8.5 (alerts), Phase 10 (ops console routes) in parallel — 2 creator agents.

**Wave 5 (after Wave 4 closes):** Phase 9 (orchestrator + launchd), Phase 11 (UI) in parallel — 2 creator agents.

**Wave 6:** Phase 12 (seed + E2E shadow) — single agent.

**Wave 7:** Phase 13 (live cutover) — Sahil-driven, single agent.

Each wave's audit gate (master-auditor + qa + cross-repo Closure Definition where applicable) must PASS before the next wave starts.

---

## Decisions still needed before kickoff

1. **CT credentials** — can the creator agents for Phases 2, 8, 12 use `CT_CREDS_FILE=~/Collections_v3/Clevertap\ campaigns/config_CT_credentials.json`? Or do you want a separate test cred?
2. **Test customer ID** for the Phase 2 live fixture pull — pick one known-good ID.
3. **Initial test cohort** for Phase 13 — name and size.
4. **GitHub remote** — should I create `miglanisahil62/vibrium-workflow` and push, or keep local-only until Wave 5?
5. **Symlinks vs imports** — Phase 6 needs `clevertap_trigger.trigger()` from vibrium-automation. Options: (a) `pip install -e ~/vibrium-automation` so it's a real Python import, (b) symlink the file, (c) copy-paste with a comment pinning the source revision. Recommendation: (a).
