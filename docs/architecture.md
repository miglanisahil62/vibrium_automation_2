# Vibrium Workflow Engine — Node-Based, Live-Editable

*Plan revision 3 — adds arm's-length separation from the existing adhoc Vibrium system per Sahil's directive (2026-05-30). Earlier rev 2 master-auditor findings (P0×6, P1×11) at `this-is-for-vibrium-wiggly-puffin-agent-a8b13751c6b27e898.md`.*

---

## ⚠ Arm's-length separation from adhoc Vibrium (load-bearing decision)

The current Vibrium system (cohort_runner + ingest + decision_v2 + scheduler) handles **adhoc activity** — operator-driven cohorts, disposition-triggered retries, no fixed end date. The new workflow engine handles **planned journeys** — defined entry, defined call cadence, defined exit, rules independent of the adhoc rules.

**These are two separate products that must not overlap.** The plan enforces this with the following invariants:

1. **Separate SQLite database file.** Workflows live in `state/workflow.db`, NOT in `state/vibrium.db`. No `ALTER TABLE` on any existing Vibrium table.
2. **Separate queue.** Workflow calls flow through a new `wf_pending_actions` table inside `workflow.db`. The adhoc system's `pending_actions` (in `vibrium.db`) is untouched.
3. **Separate scheduler.** New `workflow_scheduler.py` fires `wf_pending_actions`. The existing `scheduler.py` continues firing the adhoc `pending_actions` exactly as today. Each is a separate launchd job.
4. **Separate ingest.** New `workflow_ingest.py` reads `collection_comment_data` filtered on the `vbwf:` tag_group prefix, populates `wf_decision_log`. The existing `ingest.py` filters those rows OUT (one-line patch — `WHERE tag_group NOT LIKE 'vbwf:%'` — so adhoc-system disposition processing doesn't double-handle them).
5. **Separate kill switch.** `workflow.db.kill_switch` is independent of `vibrium.db.kill_switch`. Pausing the adhoc system does not pause workflows, and vice versa.
6. **Separate orchestrator process.** New `workflow_orchestrator.py` (its own launchd plist). The existing Vibrium `orchestrator.py` is untouched.
7. **Separate UI surface.** Sidebar gets a new "Workflows" entry under Automations. The existing "Vibrium" entry is untouched. Each links to a different set of routes.
8. **Separate alert pipeline.** Workflow digest email is its own message; Vibrium hourly digest is unchanged.

**What is shared — by design, narrowly:**

| Shared thing | Why | How |
|---|---|---|
| `pre_call_gate.py` (compliance) | RBI 08:00–19:00 IST window + per-customer 3/day cap + dead_cause are CUSTOMER-LEVEL rules, not system-level. If they're siloed per system, two systems together can call a customer 6 times today. | Used as a pure-library import. Cap check reads from a shared `customer_call_audit` table that both systems INSERT into on every fire. |
| `clevertap_trigger.py` / `clevertap_profile.py` | These are stateless HTTP wrappers; duplicating the CT auth/retry/429 logic invites drift. | Library import only. No shared state. |
| CT credentials file | Single auth identity to CT. | Same `CT_CREDS_FILE` env var; both systems load on cold start. |
| `customer_call_audit` table | The only place the per-customer daily cap is computed from. Single source of truth. | Lives in `vibrium.db` (existing canonical DB). Both schedulers INSERT into it. Both call `pre_call_gate.customer_daily_cap(cid)` to read. |
| Redshift `collection_comment_data` | Source of truth for dispositions; cannot be duplicated. | Both ingests read it; filter on `tag_group` to avoid double-handling. |

**What is NOT shared:**
- `pending_actions` queue
- `decision_log`
- `customer_state`
- `cohort_input`
- `kill_switch`
- `escalation_log`
- `agent_events` (workflow gets its own `wf_agent_events`)
- orchestrator process
- scheduler process
- UI pages, sidebar entries, dashboards
- launchd plists
- cron cadences

**Failure isolation guarantee:** if the workflow engine crashes / has a bad migration / has a data-corruption bug, the adhoc Vibrium system keeps running unaffected. If the adhoc system pauses, workflows keep running. The blast radius of either system is bounded by its own DB file + scheduler process.

**One-line summary:** two systems, one compliance library, one shared per-customer audit log. Everything else is independent.

---

## Context

Currently Vibrium fires VB calls via two paths: cohort uploads (`cohort_runner.py`) and disposition-driven retries (`ingest.py` + `decision_v2.py` + `rules.json`). Neither expresses **multi-step journeys** — "fire on T+1; if PTP, fire on PTP date; if dispute, assign to agent; if no payment after 2 attempts, escalate."

The VB_Prompt_Doc.docx encodes exactly that — 5 segments (risk×WA) with different daily call cadences and disposition-driven post-call routing.

Sahil wants a **node-based workflow engine** where:
1. Each node is a typed primitive (fetch CT prop, check condition, wait, fire VB call, branch on disposition, …) and edges route between them.
2. CleverTap user properties (`COLL_collection_risk_segmentation`, `coll_notification_replied`, `coll_bot_calling`, `DPD`) are the data source — updated upstream; we just consume them.
3. **Live editability** — change rules from a UI; running customers either continue on the old version or migrate to the new one.
4. **Pluggable** — add new node types and journeys without code changes.
5. The 5 VB_Prompt_Doc segments are the seed workflow, proving the engine.

Intended outcome: an internal product where Ops can express any collections journey as a flowchart, ship it, and revise it without code deploys.

---

## High-level architecture

```
                    ┌─────────────────── ADHOC SYSTEM (unchanged) ───────────────────┐
                    │  orchestrator.py  →  scheduler.py  →  clevertap_trigger        │
                    │           ↓                ↑                                    │
                    │   ingest.py ← decision_v2 ← rules.json                          │
                    │           ↓                                                     │
                    │   state/vibrium.db: pending_actions, decision_log, …            │
                    └────────────────┬───────────────────────────────────────────────┘
                                     │ shares only:
                                     │  - pre_call_gate (library)
                                     │  - customer_call_audit (table in vibrium.db,
                                     │       both schedulers INSERT into it)
                                     │  - CT creds + HTTP libs
                                     ▼
                    ┌────────────── NEW: WORKFLOW SYSTEM ────────────────────────────┐
                    │  workflow_orchestrator.py  →  WorkflowAgent.tick()              │
                    │                                  ↓                              │
                    │                          handler queues row in →                │
                    │                          state/workflow.db:                     │
                    │                            wf_pending_actions ──→ workflow_scheduler.py
                    │                                                         ↓       │
                    │                                                  CT externaltrigger
                    │                                                         ↓       │
                    │                          ← wf_decision_log ← workflow_ingest.py │
                    │                              (filters tag_group LIKE 'vbwf:%')  │
                    │                                                                 │
                    │  state/workflow.db tables (all new, in own DB file):            │
                    │    workflows, workflow_versions, workflow_runs,                 │
                    │    wf_pending_actions, wf_decision_log, workflow_node_log,      │
                    │    workflow_admin_log, wf_agent_events, wf_kill_switch,         │
                    │    agent_assignments, schema_version                            │
                    └─────────────────────────────────────────────────────────────────┘

                    ┌────────────── Ops Console v2 ──────────────────────────────────┐
                    │  Sidebar (Automations section):                                 │
                    │    "Vibrium" → /vibrium  (existing, unchanged)                  │
                    │    "Workflows" → /workflows  (new section)                      │
                    └─────────────────────────────────────────────────────────────────┘
```

**Invariants (locked):**

1. **Database isolation.** No node handler, no workflow_scheduler line, no workflow_ingest line touches `state/vibrium.db` for write — except via the explicit shared library function that inserts into `customer_call_audit`. SQLite `ATTACH DATABASE` is forbidden in the workflow code path; cross-DB reads only.
2. **The only path from a workflow to a CT call** is `WORKFLOW handler → INSERT wf_pending_actions tagged with run_id → workflow_scheduler.run() picks it up → pre_call_gate.check() gates it → INSERT customer_call_audit → clevertap_trigger.trigger()`. No node handler may call `clevertap_trigger.trigger()` directly.
3. **Static lint rule added** (stashfin_lint): no `requests.post` to CT in any file under `agents/workflow*.py` or `workflow_*.py`; no `sqlite3.connect("vibrium.db")` for write inside workflow code (read is allowed for the audit table only).

---

## P0 fixes — architectural decisions locked

### 1. Run-ID propagation (P0-1 fix) — disposition wakeup is unambiguous

**The problem:** without a link from disposition back to the originating call, a workflow run that just queued FIRE_VB_CALL can get woken by yesterday's disposition.

**The fix — propagate `run_id` end-to-end, but entirely within the workflow system:**

```
WORKFLOW handler
  INSERT wf_pending_actions(run_id=R, node_id=N, attempt_count=K, …)
       │ [own DB: workflow.db]
       ▼
workflow_scheduler.run()
  - calls pre_call_gate.check() (library — RBI window + per-customer cap via customer_call_audit)
  - INSERT customer_call_audit(customer_id, fired_at_ist, source='workflow', run_id, …)
  - fires CT externaltrigger with tag_group = f"vbwf:{R}:{N}:{K}"
       │
       ▼
CT delivers. CRM/bot records comment in collection_comment_data with the tag.
       │
       ▼
workflow_ingest.py (own process, runs alongside but separate from ingest.py)
  - SELECT * FROM collection_comment_data WHERE tag_group LIKE 'vbwf:%' AND ts > watermark
  - parse vbwf:R:N:K → extract run_id, node_id, attempt_count
  - INSERT wf_decision_log(run_id=R, node_id=N, attempt_count=K, action_class, …)
       │ [own DB: workflow.db]
       ▼
WorkflowAgent.tick() wakes run R only if:
   workflow_runs.status = 'WAITING'
   AND workflow_runs.current_node_id = N
   AND wf_decision_log.attempt_count = K
   AND wf_decision_log.comment_create_date >= workflow_runs.entered_node_at_ist
```

**The existing `ingest.py` filters out workflow rows** (one-line change: `WHERE tag_group NOT LIKE 'vbwf:%'`) so adhoc-system disposition processing doesn't double-handle workflow calls.

**Schema (all in `workflow.db`):**
- `wf_pending_actions`: new table — owns `run_id`, `node_id`, `attempt_count` natively (no ALTER on the adhoc system's table).
- `wf_decision_log`: new table — owns same three columns.
- `workflow_runs.entered_node_at_ist`: set on every `current_node_id` change; lower bound for disposition wakeup.

If `tag_group` round-tripping turns out to be lossy on some CRM channels (a real risk per the CT P1-4 KB finding), fall back to: `workflow_ingest.py` matches a disposition to the most recent `wf_pending_actions WHERE status='FIRED' AND last_attempt_at_ist <= comment_create_date AND customer_id = ?`. Acceptable because (a) workflow_scheduler enforces 3h cooldown (own version of the rule, mirrors adhoc) so the candidate set is tiny, and (b) the `comment_create_date >= entered_node_at_ist` guard rejects stale dispositions.

### 2. Dedupe key + transactional advance (P0-2 fix)

**Schema (in `workflow.db`, no impact on the adhoc system):**
```sql
CREATE TABLE wf_pending_actions (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL,
  node_id TEXT NOT NULL,
  attempt_count INTEGER NOT NULL,
  customer_id TEXT NOT NULL,
  scheduled_at_ist TEXT NOT NULL,
  status TEXT CHECK(status IN ('PENDING','FIRING_IN_PROGRESS','FIRED','SUPPRESSED','ERROR','SHADOW_FIRED')),
  attempts INTEGER DEFAULT 0,
  last_attempt_at_ist TEXT,
  last_error TEXT,
  cohort_name TEXT,
  created_at_ist TEXT NOT NULL,
  UNIQUE(run_id, node_id, attempt_count)
);
CREATE INDEX idx_wfpa_ready ON wf_pending_actions(status, scheduled_at_ist);
CREATE INDEX idx_wfpa_customer ON wf_pending_actions(customer_id, status);
```

**Handler contract:** every side-effecting handler (FIRE_VB_CALL, SET_CT_PROP, ASSIGN_AGENT) must:
1. Take an open SQLite connection to `workflow.db` from the executor.
2. Inside one transaction: `INSERT OR IGNORE` into the side-effect table → if `cursor.rowcount == 0`, treat as "already done" and advance the run anyway → update `workflow_runs.current_node_id`, `entered_node_at_ist`, `scratchpad_json` → append `workflow_node_log`.
3. Commit. On any exception: rollback; mark run.status = ERROR; record exception in `workflow_node_log.side_effect`.

`INSERT OR IGNORE` precedent: `ingest.py` already uses this pattern for `decision_resolve` fallback (per memory `project_ops_console_v2`); workflow code adopts the same pattern in its own DB.

### 3. RBI window + per-customer cap moved into the shared gate (P0-3 fix)

**Today:** `pre_call_gate.check()` enforces overdue/paid_today/cap/cooldown using the adhoc system's `pending_actions` table. Window is enforced by `scheduler.py` + cron. Neither is reusable by a separate scheduler.

**Fix:** make `pre_call_gate.py` a pure library that both schedulers (adhoc + workflow) can call:
```python
def is_callable_now() -> GateResult:
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    if now.hour < 8 or now.hour >= 19:
        return GateResult(fire=False, reason=f"outside RBI window {now.strftime('%H:%M')} IST")
    return GateResult(fire=True, reason="")

def customer_daily_cap(customer_id: str, vibrium_db: Path) -> GateResult:
    # Reads customer_call_audit in vibrium.db — the single source of truth across systems.
    # Both schedulers INSERT a row here on every successful fire.
    # If today's count >= MAX_CALLS_PER_DAY (3), refuse.
    ...

def check(customer_id: str, source: str, vibrium_db: Path, workflow_db: Path|None = None) -> GateResult:
    # Composes: is_callable_now → customer_daily_cap → cooldown → dead_cause → collection_view → paid_today.
    # Source-aware: 'adhoc' or 'workflow' (for logging only, not for cap bypass).
    ...
```

**The shared audit table** in `vibrium.db`:
```sql
CREATE TABLE IF NOT EXISTS customer_call_audit (
  id INTEGER PRIMARY KEY,
  customer_id TEXT NOT NULL,
  fired_at_ist TEXT NOT NULL,
  source TEXT NOT NULL CHECK(source IN ('adhoc','workflow')),
  run_id INTEGER,           -- NULL for adhoc; populated for workflow
  cohort_name TEXT,
  ct_response_status TEXT,
  ct_error TEXT
);
CREATE INDEX idx_cca_customer_day ON customer_call_audit(customer_id, fired_at_ist);
```

Both `scheduler.py` (adhoc) and `workflow_scheduler.py` (new) INSERT here on every fire — a one-line library call (`audit.record_fire(...)`). The per-customer daily cap reads from THIS table only, so the two systems cannot together exceed 3 calls/day to one customer.

Add unit tests:
- `pre_call_gate.is_callable_now()` at mocked 19:30 IST → False.
- `pre_call_gate.customer_daily_cap()` with 2 adhoc rows + 1 workflow row today → False (cap=3 reached).
- `pre_call_gate.check()` from workflow_scheduler context at 11:00 IST with a customer who's had 0 calls today → True.

### 4. CONDITION DSL — pin syntax, types, and library version (P0-4 fix)

**Syntax: Python-flavored.** Use lowercase `and / or / not`. Operators: `<, <=, >, >=, ==, !=, in, not in`. Numeric literals, single-quoted strings.

**Example expressions:**
```
risk_segmentation < 5 and wa_status == 'WA_Available' and dpd > 1
bot_calling == 'ai_vb_calling_highv1' and attempts < 2
```

**Library:** `simpleeval==0.9.13` pinned in `vibrium-automation/requirements.txt`. Add to existing pip install loop on AWS deploy.

**Type coercion at fetch time (not at eval time):**
- `FETCH_CT_PROPS` node config now requires a `schema` dict per property: `{"DPD": "int", "risk_segmentation": "int", "wa_status": "str", "bot_calling": "str"}`.
- On fetch, the handler coerces using `int(value)` / `float(value)` / explicit `str(value)` and writes typed values to scratchpad. Coercion failure → run.status = ERROR with `coercion_failed_property = "DPD"` in scratchpad.
- This eliminates the `TypeError: '<' not supported between instances of 'str' and 'int'` class of errors at CONDITION eval time.

**Safe evaluator config:** `SimpleEval(functions={}, names=scratchpad)`. Disable all functions (no `int()`, no `rand()`) — names-only access. `MAX_STRING_LENGTH = 1024`, `MAX_POWER = 100`.

**Save-time validation:** before persisting a workflow version, run `simpleeval.compile(expr)` for every CONDITION node — reject the save on `SyntaxError`. Also verify every referenced scratchpad key was written by an upstream `FETCH_CT_PROPS`, `SET_*`, or built-in (`attempts`, `disposition`).

### 5. UUID-stable node_id + pinned migration rules (P0-5 fix)

**Identity:** every node gets a UUID at creation time, persisted across versions. The graph editor never reassigns it; renaming a node keeps the same UUID; copy-pasting a node mints a new UUID.

**Migration rules (formal):**
| Case | Behavior |
|---|---|
| `current_node_id` exists in new version, `type` unchanged, `config` deep-equals old | **Migrate silently** — run advances to v_new on its current node. |
| `current_node_id` exists, `config` changed (any field) | **Hold** — run stays on v_old. UI surfaces a "23 runs parked on changed-config nodes; choose: advance / hold / orphan" prompt. **Default: hold.** |
| `current_node_id` doesn't exist in new version | **ORPHANED** — `status = 'ORPHANED'`. UI shows orphans separately; operator can repair (advance to a chosen node + scratchpad patch) or terminate. |

**Tick query** is filtered by `version_id`: a run on v1 is executed by v1's graph, full stop. No hot-swap mid-tick.

**UI guardrail:** the "activate v2" button can't be clicked without a modal showing per-bucket counts (`silent: 412, hold: 23, orphan: 4`) under each migration mode. Default selection is "only new enrollments" (zero impact on existing runs).

### 6. Enrollment idempotency + caps (P0-6 fix)

**enrollment_key:** every workflow defines an `enrollment_key_template` (string with `{customer_id}` and date placeholders). Default: `"{customer_id}_{YYYY-MM-DD}"`. Same customer can re-enroll once per day; never twice within one tick.

**Schema:**
```sql
ALTER TABLE workflow_runs ADD COLUMN enrollment_key TEXT;
CREATE UNIQUE INDEX idx_runs_enrollment_key
  ON workflow_runs(workflow_id, customer_id, enrollment_key)
  WHERE enrollment_key IS NOT NULL;
```
Enrollment via `INSERT OR IGNORE` on this key.

**Caps:**
- Per-workflow daily cap: `max_new_enrollments_per_day` (default 1000, operator-tunable in workflow config).
- Per-tick global cap: `max_new_enrollments_per_tick` (default 200).
- Hard abort: any single tick that would enroll > 5000 — `enrollment_poller.py` exits with non-zero and emits an alert email; requires Sahil "go" to override via `--force` flag (matches `feedback_manual_vibrium_confirm`).

**Cadence:** `enrollment_poller.py` runs every 30 min via launchd, only inside call window 08:00–18:00 IST. It calls `pre_call_gate.is_callable_now()` first and exits no-op outside the window. It also respects `kill_switch` exactly like `orchestrator.py:87-107`.

**Preview at ACTIVATE time:** UI runs the entry condition against a sampled batch of CT profiles (e.g., 500 random customer_ids from collection_view) and projects `"this workflow would enroll ~N customers in the first 24h"`. Sahil approves the projection before activation.

---

## Node model

| Node type | Purpose | Config | Edges out |
|---|---|---|---|
| `ENROLL` | Entry point | enrollment trigger config | one |
| `FETCH_CT_PROPS` | GET CT profile; coerces to typed scratchpad | `properties: { "DPD": "int", "risk_segmentation": "int", "wa_status": "str", "bot_calling": "str" }` | `success`, `not_found`, `error` |
| `CONDITION` | Boolean expression over scratchpad (simpleeval, Python syntax) | `expr` | `true`, `false` |
| `SWITCH` | Multi-way branch on a single scratchpad value | `on`, `cases` (numeric ranges or string equality) | one per case + `default` |
| `WAIT_UNTIL` | Park run until wall-clock time | `relative: "T+1 day at 08:00"` or `absolute: scratchpad_key` | one |
| `SET_CT_PROP` | POST /1/upload (inspects `unprocessed[]`) | `properties: { "coll_bot_calling": "ai_vb_calling_highv1" }` | `success`, `error` |
| `FIRE_VB_CALL` | Queue pending_actions row tagged with run_id | (defaults from config.json) | `queued`, `suppressed` |
| `AWAIT_DISPOSITION` | Wait until decision_log row arrives for this run_id+node_id+attempt_count | `timeout: 24h` | `disposition`, `timeout` |
| `BRANCH_ON_DISPOSITION` | Multi-way on `decision_log.action_class` | (uses canonical enum; see below) | one per case + `default` |
| `COUNTER` | Increment named scratchpad counter | `name`, `limit` | `under_limit`, `at_limit` |
| `ASSIGN_AGENT` | Insert into `agent_assignments` (new table) | `reason: "dispute" | "max_attempts" | ...` | one |
| `TERMINATE` | End run | `status: "PAID" | "MAX_ATTEMPTS" | "INELIGIBLE" | "OUT_OF_SCOPE" | ...` | terminal |
| `PILL_REROUTE` | *(backlog — not in v1)* | — | — |

**`BRANCH_ON_DISPOSITION` enum source — pinned:** branches on `decision_log.action_class` from `ingest.py`'s canonical set:
`NOOP, RETRY, PTP_CALL, AGREE_EOD_CALL, CALLBACK_CALL, RTP_NEEDS_LLM, ESCALATE`. Save-time validator rejects branches on values not in this set. A `default` edge is mandatory.

**`ASSIGN_AGENT` target — new table:**
```sql
CREATE TABLE agent_assignments (
  id INTEGER PRIMARY KEY,
  customer_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  source TEXT,                  -- e.g., 'workflow:vb_collections_v1:v3'
  assigned_at_ist TEXT NOT NULL,
  assigned_to TEXT,             -- nullable; team picks up via console
  resolved_at_ist TEXT,
  resolution_note TEXT,
  run_id INTEGER
);
```
Existing escalation_log stays as-is; agent_assignments is a separate queue for non-escalation handoffs (e.g., "no progress after N attempts").

**`WAIT_UNTIL` semantics — pinned (per P1-8 fix):**
- `ready_at_ist` stored as `YYYY-MM-DD HH:MM:SS` IST-naive (matches `project_vibrium_event_ts_format_drift`).
- Relative strings parsed at handler-execute: `_now_ist() + timedelta(days=N)` then set HH:MM.
- Absolute requires `YYYY-MM-DD` IST; other formats → run.status=ERROR.
- Late wakeups fire on the next tick (catchup is automatic).
- Kill-switch pause + resume: waits never expire silently; they re-fire on the first tick after resume.

---

## State and persistence

**New database file** `state/workflow.db` (separate WAL'd SQLite). Plus one shared table added to `state/vibrium.db`.

### `state/workflow.db` (all new, all owned by the workflow system):

```sql
CREATE TABLE workflows (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL,
  status TEXT CHECK(status IN ('DRAFT','ACTIVE','PAUSED','ARCHIVED')),
  active_version_id INTEGER,
  shadow_mode INTEGER NOT NULL DEFAULT 1,
  requires_approval INTEGER NOT NULL DEFAULT 1,
  max_new_enrollments_per_day INTEGER DEFAULT 1000,
  enrollment_key_template TEXT DEFAULT '{customer_id}_{YYYY-MM-DD}',
  created_at_ist TEXT, created_by TEXT
);

CREATE TABLE workflow_versions (
  id INTEGER PRIMARY KEY, workflow_id INTEGER NOT NULL,
  version INTEGER NOT NULL,
  graph_json TEXT NOT NULL,
  validation_status TEXT, validation_errors TEXT,
  approved_at_ist TEXT, approved_by TEXT,
  created_at_ist TEXT, created_by TEXT,
  UNIQUE(workflow_id, version)
);

CREATE TABLE workflow_runs (
  id INTEGER PRIMARY KEY,
  workflow_id INTEGER NOT NULL, version_id INTEGER NOT NULL,
  customer_id TEXT NOT NULL,
  enrollment_key TEXT,
  current_node_id TEXT, current_node_type TEXT,    -- denormalized type (P2-2)
  entered_node_at_ist TEXT,                         -- lower bound for disposition wakeup
  status TEXT CHECK(status IN ('ACTIVE','WAITING','PAUSED','DONE','ERROR','ORPHANED')),
  scratchpad_json TEXT NOT NULL DEFAULT '{}',
  ready_at_ist TEXT,
  enrolled_at_ist TEXT, updated_at_ist TEXT,
  terminated_at_ist TEXT, terminal_status TEXT
);
CREATE INDEX idx_runs_ready    ON workflow_runs(status, ready_at_ist);
CREATE INDEX idx_runs_customer ON workflow_runs(customer_id, status);
CREATE UNIQUE INDEX idx_runs_enrollment_key
  ON workflow_runs(workflow_id, customer_id, enrollment_key)
  WHERE enrollment_key IS NOT NULL;

CREATE TABLE workflow_node_log (    -- append-only audit
  id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL,
  ts_ist TEXT NOT NULL,
  from_node_id TEXT, to_node_id TEXT, edge_label TEXT,
  scratchpad_before TEXT, scratchpad_after TEXT,
  side_effect TEXT,
  dry_run INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE workflow_admin_log (   -- who did what at workflow level
  id INTEGER PRIMARY KEY, ts_ist TEXT NOT NULL,
  workflow_id INTEGER NOT NULL, version_id INTEGER,
  actor TEXT NOT NULL,              -- ops console user
  action TEXT NOT NULL,             -- CREATE | EDIT | ACTIVATE | PAUSE | ARCHIVE | MIGRATE_RUNS | REPAIR_ORPHAN
  detail_json TEXT
);

CREATE TABLE wf_decision_log (      -- workflow's own disposition log
  id INTEGER PRIMARY KEY, ts_ist TEXT NOT NULL,
  comment_id TEXT, customer_id TEXT NOT NULL,
  run_id INTEGER NOT NULL, node_id TEXT NOT NULL, attempt_count INTEGER NOT NULL,
  disposition TEXT, sub_disposition TEXT,
  action_class TEXT,
  comment_create_date TEXT,
  notes TEXT
);
CREATE INDEX idx_wfdl_run ON wf_decision_log(run_id, node_id, attempt_count);

CREATE TABLE wf_kill_switch (
  id INTEGER PRIMARY KEY, ts_ist TEXT NOT NULL,
  action TEXT CHECK(action IN ('KILL','RESUME')),
  reason TEXT, set_by TEXT
);

CREATE TABLE wf_agent_events (
  id INTEGER PRIMARY KEY, ts_ist TEXT NOT NULL,
  agent TEXT NOT NULL, status TEXT, summary_json TEXT
);

CREATE TABLE agent_assignments (    -- target for ASSIGN_AGENT nodes
  id INTEGER PRIMARY KEY,
  customer_id TEXT NOT NULL, reason TEXT NOT NULL,
  source TEXT,                       -- e.g., 'workflow:vb_collections_v1:v3'
  assigned_at_ist TEXT NOT NULL, assigned_to TEXT,
  resolved_at_ist TEXT, resolution_note TEXT,
  run_id INTEGER
);

CREATE TABLE schema_version (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
```

### `state/vibrium.db` (existing, single additive table):

```sql
CREATE TABLE IF NOT EXISTS customer_call_audit (
  id INTEGER PRIMARY KEY,
  customer_id TEXT NOT NULL,
  fired_at_ist TEXT NOT NULL,
  source TEXT NOT NULL CHECK(source IN ('adhoc','workflow')),
  run_id INTEGER,
  cohort_name TEXT,
  ct_response_status TEXT,
  ct_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_cca_customer_day ON customer_call_audit(customer_id, fired_at_ist);
```
This is the **only** modification to `vibrium.db`. The adhoc system's `scheduler.py` gets a one-line `audit.record_fire(...)` call added after every successful fire; the workflow scheduler does the same. Per-customer cap reads from this table.

**Schema migrations:** idempotent `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE` guarded by `schema_version` check. Numbered migration scripts in `scripts/workflow_migrations/001_init.py`, `002_<feature>.py`, etc. No alembic — matches existing tree.

**Explicitly NOT modified:** `pending_actions`, `decision_log`, `customer_state`, `cohort_input`, `kill_switch`, `escalation_log`, `agent_events`. The adhoc system's schema is untouched.

**cohort_name attribution:** `FIRE_VB_CALL` handler sets `wf_pending_actions.cohort_name = f"workflow:{workflow_id}:v{version_id}"` for traceability inside the workflow system. The adhoc cohort digest does NOT see workflow rows (it reads `pending_actions`, not `wf_pending_actions`).

---

## Executor loop

In `vibrium-automation/workflow/agents/workflow.py`, invoked by the **separate** `workflow_orchestrator.py` (its own process, its own launchd plist). The existing `orchestrator.py` does not know about the workflow system.

```python
class WorkflowAgent:
    def tick(self, *, dry_run: bool = False) -> AgentResult:
        if wf_kill_switch_active(workflow_db): return AgentResult(status="paused")

        for run in workflow_db.query("""
            SELECT * FROM workflow_runs
            WHERE status IN ('ACTIVE','WAITING')
              AND (ready_at_ist IS NULL OR ready_at_ist <= ?)
            ORDER BY enrolled_at_ist
            LIMIT ?
        """, (_now_ist_str(), TICK_BATCH_LIMIT)):
            version = version_cache.get(run.version_id)
            node = version.find_node(run.current_node_id)
            handler = REGISTRY[node.type]
            try:
                with workflow_db.transaction() as txn:
                    result = handler.execute(node, run, ctx, txn, dry_run=dry_run)
                    apply_transition(run, result, txn, dry_run=dry_run)
            except Exception as e:
                mark_error(run, e)
        emit_wf_agent_event("workflow", processed=N, advanced=A, ...)
```

**Side-effect rules:**
- `FIRE_VB_CALL` handler inserts a `wf_pending_actions` row inside the same transaction as the `current_node_id` advance. `INSERT OR IGNORE` on the `(run_id, node_id, attempt_count)` UNIQUE.
- `SET_CT_PROP` is the **only** node that calls a live CT API at tick time, via `clevertap_profile.set_profile()`. Parses `unprocessed[]` per the CT KB hard rule.
- `clevertap_trigger.trigger()` is **never** called from a handler. Calls flow `wf_pending_actions → workflow_scheduler.py → clevertap_trigger.trigger() (library)`.
- The workflow's tick reads `wf_pending_actions.status` to observe fire outcomes (no callback from the scheduler back into the workflow — the workflow polls).

**dry-run mode:** `--dry-run` flag on WorkflowAgent inherits from `orchestrator.py:47` pattern. In dry-run: handlers log intended transitions to `workflow_node_log` with `dry_run=1` and do NOT advance `current_node_id`, do NOT insert `pending_actions`, do NOT POST to CT.

**version cache:** `version_id → parsed graph` cached per tick. Invalidated at tick boundary, so a mid-tick edit doesn't affect the in-flight tick (P2-3 fix).

**heartbeat:** `heartbeat("workflow_agent", "ok"|"down", summary=...)` matching `ingest.py:297-301` pattern.

---

## CleverTap profile reader (new module)

`scripts/clevertap_profile.py`:

- `get_profile(identity) -> dict | None` — `GET /1/profile.json?identity=<id>`. Returns parsed `profileData` or None on 404. `timeout=(5, 30)`.
- `bulk_get_profiles(identities) -> dict[str, dict|None]` — concurrency=5; **honors `Retry-After` header** on 429 (sleep header seconds, halve concurrency on second 429 in 60s, max 3 retries, then mark `{cid: None}` and continue — caller treats None as "fetch failed, do not advance the run, retry next tick").
- `set_profile(identity, properties) -> SetResult` — POST `/1/upload`; parses `unprocessed[]`; returns `SetResult(success=bool, error_code=int|None)`. **`status:"success"` alone is NOT trusted** (CT KB hard rule).
- Cred-loading: copy pattern from `clevertap_trigger.py:28-40` (cred cache + mtime invalidation). Reads `CT_CREDS_FILE` env var, falls back to `~/Collections_v3/Clevertap campaigns/config_CT_credentials.json` (matches every other CT script).
- **Response-shape fixture pinned before code:** before writing the module, fetch one live profile, save full JSON to `tests/fixtures/ct_profile_response.json`, confirm the 4 target properties are extractable. KB note added.

---

## UI: visual node editor in Ops Console v2

**Routes (new in `ops_console_v2/app.py`):**

| Route | Method | Purpose |
|---|---|---|
| `/workflows` | GET | List page |
| `/workflows/{id}` | GET | Detail page (Drawflow editor + node config form) |
| `/workflows/{id}/runs` | GET | Runs table with drill-down to `workflow_node_log` |
| `/workflows/{id}/runs/{run_id}` | GET | Per-run timeline |
| `/api/workflows` | POST | Create draft |
| `/api/workflows/{id}/version` | POST | Save graph_json → validate → write workflow_versions row |
| `/api/workflows/{id}/activate` | POST | Two-step: draft → approval-requested → activated. Requires Sahil "go" in chat for the first activation per workflow. Modal shows the migration-bucket counts. |
| `/api/workflows/{id}/preview-enrollment` | POST | Sampled projection: "would enroll ~N customers" |
| `/api/workflows/{id}/runs/{run_id}/repair` | POST | Manually advance an ORPHANED run |

**Remove from `STUB_ROUTES`** in `app.py` (per release-gate checklist).

**Sidebar nav:** `{{ nav('/workflows', 'Workflows', icon_svg) }}` under Automations in `templates/base.html`.

**Graph library: Drawflow.js v0.0.59** (vendored under `/static/drawflow/`). Plus a **JSON-text fallback view** (toggle in the toolbar) so power users + keyboard-only operators can edit `graph_json` directly. ui-auditor PASS required.

**Apple-token compliance:** all new templates use `var(--accent)`, `var(--bg-elevated)`, `var(--separator)`, `var(--label)`, `var(--r-xl)`, etc. Drawflow's CSS overridden via `workflow_editor.css` to match.

**Save-time validation (server-side):**
- Exactly one ENROLL node.
- Every non-terminal node has all required edges populated.
- No cycles unless cycle goes through `WAIT_UNTIL` (else infinite tick spin).
- Every CONDITION.expr parses under simpleeval.
- Every scratchpad key referenced is written by an upstream FETCH/SET/built-in.
- Every BRANCH_ON_DISPOSITION enum value is in the canonical action_class set.
- Every node_id is a valid UUID; no duplicates within a version.

---

## Operator approvals & permissions

- **First activation of a workflow:** `requires_approval=1` by default. Activation modal blocks until Sahil approves in chat (per `feedback_manual_vibrium_confirm`).
- **shadow_mode** on workflow itself: in shadow, FIRE_VB_CALL handler logs the intended fire to `workflow_node_log` but does NOT insert into `pending_actions`. Same pattern as `config.json.shadow_mode`.
- **workflow_admin_log** captures every mutation with actor + timestamp + before/after diff (graph_json hash + bucket counts).
- **kill_switch** (orchestrator.py:87-107) — WorkflowAgent and enrollment_poller both check it before doing anything. Latest KILL → no-op tick.

---

## Observability

- **agent_events emission** every tick (matches `orchestrator.py:141-166`): `{"agent":"workflow", "processed":N, "advanced":A, "errored":E, "orphaned":O, ...}`.
- **Daily 09:00 IST summary email** to sahil.miglani@stashfin.com: per-workflow active / waiting / erroring >24h / orphaned / shadow runs. New launchd plist: `com.sahil.vibrium.workflow_digest.plist`.
- **Alert email** if any run has `status=ERROR > 24h` or `status=WAITING > 7 days`.
- **heartbeat("workflow_agent", ...)** per tick — picked up by pipeline-integrity-auditor skill via existing `agents_health_check.py` (add the new daemon to the registry).
- **Per-tick log line:** `agent=workflow status=ok processed=Y advanced=Z errored=E` — orchestrator captures via existing log format.

---

## Seed workflow: VB_Prompt_Doc

To prove the engine, ship `vb_collections_v1`:

```
ENROLL (CT-property-watcher: poll for coll_bot_calling transitions)
  → FETCH_CT_PROPS [DPD:int, risk_segmentation:int, wa_status:str, bot_calling:str]
  → CONDITION [dpd > 1]                              false → TERMINATE(INELIGIBLE)
                                                     true ↓
  → SWITCH on risk_segmentation
      case 1..4 (High) ──┐
      case 5,6,7 (Mid)   ┤
      default            └─→ TERMINATE(OUT_OF_SCOPE)

      High branch
      → SWITCH on wa_status
          WA_Available   → SET_CT_PROP coll_bot_calling=ai_vb_calling_highv1
                         → WAIT_UNTIL T+1 08:00
                         → call_loop(max=2)
          WA_Unavailable → WAIT_UNTIL today 08:00
                         → call_loop(max=2)

      Mid branch → SWITCH on wa_status → [v1/v2/v3/v4 variants per doc]

  call_loop subgraph (replicated; SUBWORKFLOW node is backlog):
    → FIRE_VB_CALL → AWAIT_DISPOSITION
      BRANCH_ON_DISPOSITION on action_class:
        NOOP                 → TERMINATE(PAID)
        PTP_CALL             → WAIT_UNTIL scratchpad.ptp_date 08:00 → FIRE_VB_CALL …
        AGREE_EOD_CALL       → WAIT_UNTIL today 18:00 → FIRE_VB_CALL …
        CALLBACK_CALL        → WAIT_UNTIL scratchpad.callback_at → FIRE_VB_CALL …
        ESCALATE             → ASSIGN_AGENT(reason='dispute_or_nrp')
        RETRY                → COUNTER attempts++
                                under_limit → WAIT_UNTIL +1 day → FIRE_VB_CALL
                                at_limit    → TERMINATE(MAX_ATTEMPTS)
        default              → ASSIGN_AGENT(reason='unhandled_disposition')
```

The "Low Risk + WA Available" branch (incomplete in the doc) and any future segments are added as new nodes in the UI — no code change.

---

## Files to add / modify

The new code lives under a dedicated `workflow/` subdirectory inside the repo to make the boundary visually obvious in the file tree.

**Add (workflow system — its own subtree):**
- `vibrium-automation/workflow/__init__.py`
- `vibrium-automation/workflow/agents/workflow.py` — WorkflowAgent.tick(), registry
- `vibrium-automation/workflow/agents/workflow_handlers/*.py` — one file per node type
- `vibrium-automation/workflow/workflow_orchestrator.py` — its own process (analog of adhoc `orchestrator.py`)
- `vibrium-automation/workflow/workflow_scheduler.py` — its own scheduler (analog of adhoc `scheduler.py`); fires `wf_pending_actions`
- `vibrium-automation/workflow/workflow_ingest.py` — its own disposition reader (filters `tag_group LIKE 'vbwf:%'`)
- `vibrium-automation/workflow/clevertap_profile.py` — GET + POST /1/upload client (used only by workflow handlers)
- `vibrium-automation/workflow/enrollment_poller.py` — kill-switch-aware, runs in workflow's own cron
- `vibrium-automation/workflow/workflow_digest.py` — daily summary email (workflow only)
- `vibrium-automation/workflow/wf_store.py` — owns the `workflow.db` connection pool
- `vibrium-automation/workflow/migrations/001_init.py` etc.
- `vibrium-automation/workflow/launchd/com.sahil.workflow.orchestrator.plist`
- `vibrium-automation/workflow/launchd/com.sahil.workflow.scheduler.plist`
- `vibrium-automation/workflow/launchd/com.sahil.workflow.ingest.plist`
- `vibrium-automation/workflow/launchd/com.sahil.workflow.enroll.plist`
- `vibrium-automation/workflow/launchd/com.sahil.workflow.digest.plist`
- `vibrium-automation/workflow/tests/test_handlers.py`, `test_executor.py`, `test_scheduler.py`, `test_ingest.py`, `test_migrations.py`
- `vibrium-automation/workflow/tests/fixtures/ct_profile_response.json` (pinned from live call)
- `ops_console_v2/templates/workflows_list.html`, `workflow_detail.html`, `workflow_runs.html`, `workflow_run_detail.html`
- `ops_console_v2/static/drawflow/` (vendored), `ops_console_v2/static/workflow_editor.{js,css}`

**Add (shared infra — single shared library + single shared audit table):**
- `vibrium-automation/scripts/customer_call_audit.py` — small library that both schedulers call: `record_fire(customer_id, source, run_id, …)`. Owns the `customer_call_audit` table in `vibrium.db`.
- One migration that creates `customer_call_audit` in `vibrium.db` (idempotent `CREATE TABLE IF NOT EXISTS`).

**Modify (adhoc system — minimal, surgical):**
- `vibrium-automation/scripts/pre_call_gate.py` — add `is_callable_now()` and rework `customer_daily_cap()` to read from the shared `customer_call_audit` table. Existing callers continue to work; workflow scheduler can now call the same library.
- `vibrium-automation/scripts/scheduler.py` — one new line: `audit.record_fire(customer_id, source='adhoc', …)` after every successful CT trigger.
- `vibrium-automation/scripts/ingest.py` — one new line: `WHERE tag_group NOT LIKE 'vbwf:%'` filter on the collection_comment_data query so adhoc disposition processing skips workflow rows.
- `vibrium-automation/scripts/clevertap_trigger.py` — accept an optional `tag_group` kwarg in the payload (closes the existing P1-4 KB finding too). Backwards compatible: default `None` means no tag.
- `vibrium-automation/requirements.txt` — add `simpleeval==0.9.13`.
- `ops_console_v2/app.py` — register `/workflows` routes; remove from STUB_ROUTES. The existing `/vibrium` route is untouched.
- `ops_console_v2/templates/base.html` — add sidebar entry `{{ nav('/workflows', 'Workflows', icon_svg) }}` under Automations; existing "Vibrium" entry is untouched.
- `vibrium-automation/scripts/agents_health_check.py` — register the new workflow daemons (workflow_orchestrator, workflow_scheduler, workflow_ingest, enrollment_poller) as separately-monitored heartbeats so pipeline-integrity-auditor sees them.

**Explicitly NOT modified (zero touch):**
- `cohort_runner.py`, `decision.py`, `decision_v2.py`, `decision_legacy.py`, `rules.json`, `manual_trigger.py`
- `agents/orchestrator.py`, `agents/decision.py`, `agents/scheduling.py`, `agents/dispatcher.py`, `agents/gate.py`, `agents/timing.py`, `agents/recovery.py`
- `scripts/sqlite_store.py` (no new tables added to vibrium.db beyond the shared audit table — and that migration is in its own module)
- `state/vibrium.db` schema (apart from the additive `customer_call_audit` table)

**Reuse (library-only; no state shared):**
- `clevertap_trigger.trigger()` — called by `workflow_scheduler.py` exactly as the adhoc scheduler does. Pure HTTP wrapper.
- `clevertap_profile.py` — used by FETCH_CT_PROPS and SET_CT_PROP handlers. Pure HTTP wrapper.
- `pre_call_gate.{is_callable_now, customer_daily_cap, check}` — pure functions, no state.

---

## Verification

1. **Unit:** `pytest vibrium-automation/workflow/tests/test_handlers.py` — every handler returns correct NodeResult on happy + edge inputs. CT calls mocked via `monkeypatch`.
2. **Migrations:** `test_migrations.py` runs `001_init.py` against an empty `workflow.db`; confirms idempotency on second run; confirms `customer_call_audit` migration on `vibrium.db` is also idempotent and does not touch any existing table.
3. **Integration:** synthetic `workflow.db` + a 6-node graph (FETCH → CONDITION → WAIT → FIRE → AWAIT → TERMINATE); run `tick()` repeatedly; assert advancement + transactional rollback on injected failure.
4. **Live CT read:** call `clevertap_profile.get_profile(<known_test_customer>)`, confirm 4 target properties round-trip. Save fixture.
5. **Arm's-length proof — adhoc untouched:** snapshot `vibrium.db` schema before and after deploying the workflow system; the diff must show ONLY the new `customer_call_audit` table. Existing tables byte-identical.
6. **Arm's-length proof — failure isolation:** kill `workflow_scheduler.py` mid-tick; run `adhoc_scheduler.py` for an hour; assert adhoc tick runs normally and the adhoc `pending_actions` queue advances normally.
7. **Arm's-length proof — kill-switch independence:** set `wf_kill_switch.action=KILL`; tick both systems; assert adhoc continues firing, workflow does not. Reverse: set adhoc `kill_switch.action=KILL`; assert workflow continues, adhoc does not.
8. **Shared cap enforcement:** seed `customer_call_audit` with 2 adhoc rows + 1 workflow row for customer X today; call `pre_call_gate.check(X, source='workflow')` → False (cap reached). Then call `pre_call_gate.check(Y, source='workflow')` for a different customer → True.
9. **Disposition wakeup correctness:** stage two workflow runs for the same customer in two workflows; fire only one via `workflow_scheduler`; assert only the matching `run_id` wakes in `workflow_ingest`.
10. **Idempotency:** kill the tick mid-handler; re-tick; assert no duplicate `wf_pending_actions` row (UNIQUE index prevents).
11. **RBI window:** `pre_call_gate.is_callable_now()` at mocked 19:30 IST → False; tick the WorkflowAgent at 19:30; assert no FIRE happens.
12. **Versioning:** create v1, enroll 10 runs, edit one node's config, save v2 with "only new enrollments"; assert all 10 runs remain on v1; enroll 5 more, they go to v2.
13. **Console wiring:** `/stashfin-qa-backend` (new routes return 200, dry_run enforced, auth gates active, /vibrium routes still PASS); `/stashfin-qa-ui` (BOTH sidebar entries render — Vibrium and Workflows — neither broken; workflow editor loads, save round-trips, JSON-fallback toggle works).
14. **`ui-auditor` PASS** on all 4 new templates.
15. **`master-auditor` PASS** on `workflow/agents/workflow.py`, `workflow/workflow_scheduler.py`, `workflow/workflow_ingest.py`, `workflow/clevertap_profile.py`, `workflow/enrollment_poller.py`, and the `customer_call_audit` library.
16. **End-to-end shadow:** activate `vb_collections_v1` in shadow_mode; enroll a 100-customer test cohort; check `workflow_node_log` for expected branch distribution; assert zero live CT triggers AND zero `customer_call_audit` rows added for these customers.
17. **End-to-end live:** flip shadow off after Sahil "go"; run on a small named cohort; compare outcomes to current cohort-runner-based baseline; promote when match is within tolerance. Confirm `customer_call_audit` rows appear for both adhoc fires (unchanged) and workflow fires (new).

---

## What's explicitly out of scope for v1

- `SUBWORKFLOW` / nested-workflow node (call_loop is duplicated in v1; refactor in v2).
- `PILL_REROUTE` node (kept as placeholder; not implemented).
- Real-time CT event webhook receiver (polling is sufficient given operational tempo).
- Multi-tenant workflows (single-tenant: Stashfin-only).
- Per-operator role-based permissions (single role: ops admin; gated by Sahil approval).
