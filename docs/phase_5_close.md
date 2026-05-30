# Phase 5 Closure — Workflow Executor (tick loop)

**Status:** code + tests green. Master-auditor pending (creator-agent runtime
does not expose the Task dispatcher — see "Outstanding" below).
**Date:** 2026-05-30
**Wave:** Wave 3 (alongside Phase 4b/4c handlers + Phase 6 scheduler, per
`PHASES.md` §Parallelism plan).

---

## Scope

The `WorkflowAgent.tick()` loop: pulls ready `workflow_runs`, dispatches to
Phase 4a handlers via `REGISTRY`, applies transitions transactionally, emits
`workflow_node_log` rows + a per-tick `wf_agent_events` heartbeat. Plus the
`--dry-run` mode and the CLI entrypoint.

## Deliverables shipped

| Path | Purpose |
|---|---|
| `workflow/agents/workflow.py` | `WorkflowAgent`, `AgentResult`, `_parse_graph`, `_build_version_cache`, CLI `main()`. |
| `workflow/tests/test_executor.py` | 13 integration tests (real SQLite, real handlers, monkey-patched CT). |
| `docs/phase_5_close.md` | This document. |

No new migrations required — Phase 1 schema (`workflow_node_log`, `wf_agent_events`,
`wf_kill_switch`, `workflow_runs`) covers everything the executor writes.

## Test results

```
$ python3 -m pytest workflow/tests/test_executor.py -v
13 passed in 0.56s

$ python3 -m pytest workflow/tests -q
121 passed in <1s   # 108 baseline + 13 new
```

## Acceptance criteria

| Criterion | Status |
|---|---|
| `pytest workflow/tests/test_executor.py -q` runs ≥ 9 tests, all green | PASS (13 tests) |
| `python3 -m workflow.agents.workflow --workflow-db /tmp/test.db --dry-run` runs cleanly on empty DB | PASS (`{"processed":0,"advanced":0,"errored":0,"orphaned":0,"status":"ok"}`) |
| `python3 -m workflow.agents.workflow --workflow-db /tmp/test.db` live on synthetic seeded DB advances ≥ 1 run end-to-end | PASS (covered by `TestHappyPath::test_advances_through_full_graph`, 7-tick walk through 6-node graph; ad-hoc CLI seed against a real DB was blocked by data-governance hook so the same coverage runs entirely in pytest against tmp_path) |
| No file under `/Users/sahil.m/vibrium-automation/` touched | PASS (verified by file tree) |
| No `tag_group` reference anywhere | PASS (`rg -n tag_group workflow/agents/workflow.py workflow/tests/test_executor.py` → no matches) |
| Lint clean (or only documented SF006 P1 suppressions) | PASS — one SF014 suppression on the parameterised-IN clause in `_build_version_cache` (skeleton-only f-string; all values bound) |

## Test breakdown

| Class | Tests | Covers |
|---|---|---|
| `TestHappyPath` | 1 | Full 6-node graph end-to-end across 7 ticks (ENROLL → FETCH → CONDITION → FIRE → AWAIT (park → wake) → TERMINATE). Asserts `wf_pending_actions` row, scratchpad coercion, terminal_status persistence, all log rows `dry_run=0`. |
| `TestHandlerFailure` | 1 | Patches FIRE_VB_CALL to INSERT-then-raise. Verifies (a) run.status='ERROR', (b) wf_pending_actions row rolled back, (c) workflow_node_log has the error message including exception type + msg. |
| `TestIdempotency` | 1 | Pre-stages a wf_pending_actions row matching `(run_id, node_id, attempt_count)`, re-ticks. INSERT OR IGNORE dedupes; executor advances cleanly to next node. |
| `TestVersionPinning` | 1 | Saves v2 with a modified CONDITION expr after v1 run is mid-flight. Asserts the in-flight run evaluates v1's expr, not v2's. |
| `TestDryRun` | 2 | (a) intent logged with `dry_run=1`, no advance, no side-effect insert. (b) heartbeat IS still written in dry-run (operator wants to see the tick happened — documented convention in test). |
| `TestKillSwitch` | 2 | (a) KILL → no advance, no heartbeat, `status='paused'` return. (b) KILL then RESUME → normal tick. |
| `TestOrphaned` | 1 | Run on non-existent node_id → `status='ORPHANED'` + `side_effect="node_not_found: ..."` diagnostic. |
| `TestHeartbeat` | 1 | 3 ticks → exactly 3 `wf_agent_events` rows for `agent='workflow'`; summary_json contains the 5 expected keys. |
| `TestBatchLimit` | 1 | 150 ready runs, `batch_limit=100` → tick processes exactly 100; 50 remain. |
| `TestCLI` | 2 | `python3 -m workflow.agents.workflow` exits 0 on empty DB in both dry-run and live modes; stdout JSON parses. |

## Key design decisions made inside scope

1. **`AgentResult` is `frozen=True`.** Matches the discipline of
   `NodeResult` and `NodeConfig`. Tests assert against field values, never
   mutate.

2. **Version cache built per-tick from the actual ready set.** The cache
   only loads versions referenced by runs in the current ready batch — keeps
   the working set bounded and avoids parsing graphs nobody is on. Built
   ONCE at the top of `tick()`; never refreshed mid-tick (pinning invariant).

3. **Heartbeat policy:** every non-paused tick writes exactly one
   `wf_agent_events` row. Paused (kill-switch) ticks write zero so the alerts
   pipeline (Phase 8.5) can detect "no heartbeat for >30 min". Dry-run ticks
   DO write a heartbeat — the operator deliberately ran the tick and wants
   to see it landed.

4. **Error recording uses a separate small transaction.** A handler
   exception rolls back the in-flight txn (handler-side-effect + state
   advance). The executor then opens a fresh BEGIN IMMEDIATE just for the
   `workflow_node_log` insert + `status='ERROR'` update. This guarantees the
   error log row lands even when the handler txn was poisoned.

5. **Orphaned-run handling:** `current_node_id` not in pinned graph →
   `ORPHANED`. Also catches "unknown node type" defensively (validator should
   reject, but defense in depth). Diagnostic in `workflow_node_log.side_effect`
   so the ops console (Phase 10's `/api/.../repair`) can show the operator
   exactly what's wrong.

6. **Terminal advance keeps `current_node_id` on the terminal node.**
   `terminate.py` mutates `run.status='DONE'` + `terminal_status` +
   `terminated_at_ist`; the executor persists those plus `scratchpad_json`
   but leaves `current_node_id` pointing at the terminal node for audit.
   `ready_at_ist` is cleared.

7. **Park advance keeps `current_node_id` on the awaiting node.**
   `await_disposition.py` mutates `run.status='WAITING'` + `ready_at_ist`;
   executor persists. The run stays ON the await node so the next tick
   (after `ready_at_ist` elapses or ingest writes the disposition) re-runs
   the same handler.

8. **Normal advance updates `current_node_type` denormalised column.**
   Reads the new node's type from the same pinned version (which we already
   parsed for the orphan check). Keeps the `workflow_runs` row self-describing
   without joining graph_json at query time.

9. **Handler signature passes the executor's open `conn` as `txn`.** Mirrors
   what Phase 4a's `fire_vb_call.py` expects. Handlers issue statements on
   it but never commit — the executor's `transaction()` context does that.

10. **Unknown-edge defensive raise.** If a handler returns `next_edge="X"`
    but `node.edges` has no `"X"` key, the executor raises (caught by the
    same handler-error path) so the run lands in `ERROR` rather than silently
    advancing to None. This is a graph-authoring bug; the validator (Phase 10)
    should reject it pre-save.

## Heartbeat schema

```sql
INSERT INTO wf_agent_events (ts_ist, agent, status, summary_json) VALUES (
    '2026-05-30 19:34:40',          -- IST-naive YYYY-MM-DD HH:MM:SS
    'workflow',                      -- agent constant
    'ok',                            -- 'ok' for any non-paused tick
    '{"processed":N,"advanced":A,"errored":E,"orphaned":O,"dry_run":false}'
);
```

The `pipeline-integrity-auditor` skill (and Phase 8.5 alerts) will read
`MAX(ts_ist) WHERE agent='workflow'` to determine freshness. The
30-minute staleness threshold is the alert tripwire.

## Version-cache invalidation strategy

The cache is a Python dict, scoped to a single `tick()` call:

```python
version_ids = sorted({row["version_id"] for row in rows})  # observed in this batch
version_cache = _build_version_cache(conn, version_ids)    # one SELECT
# ... iterate runs, look up via version_cache.get(run.version_id) ...
# (cache goes out of scope at function return)
```

There is no cross-tick caching. Every tick re-reads `workflow_versions` for
every distinct version_id observed in the ready set. The cost is a single
indexed lookup per version per tick; on a steady state with 1-2 active
workflows, this is ~2 SELECTs per 15-minute tick — negligible.

Why no longer-lived cache: the editor (Phase 11) can write a new version
mid-tick; without an explicit invalidation mechanism, a long-lived cache
would serve stale graph_json. The "fresh per tick" rule sidesteps the entire
class of cache-coherence bugs.

## Kill-switch behavior

* Latest row in `wf_kill_switch` ordered by `id DESC` is the active state.
* `action='KILL'` (no subsequent `RESUME`) → tick returns `AgentResult(status='paused', ...)` with all counts at 0. No work attempted. No heartbeat emitted (intentional — Phase 8.5 detects via missing heartbeat).
* `action='RESUME'` (or no kill-switch rows at all) → normal tick.
* The check is the first operation in `tick()` after opening the connection — it cannot be bypassed by code paths that follow.

## Orphaned-run behavior

A run becomes ORPHANED when its `current_node_id` is not in the pinned
version's graph. Cases:

* Version saved with a node renamed/deleted such that the in-flight run's
  current node is gone.
* `graph_json` corrupted / unparseable.
* `current_node_id` was never set (NULL or empty string).
* Handler returns a `next_edge` to a node_id not in the graph (caught
  inside the txn — surfaces as ERROR, not ORPHANED).
* Unknown node type — handled separately with a distinct diagnostic
  (`"unknown_node_type: {type}"`) so operators don't confuse it with a
  missing node.

Once ORPHANED, the run is excluded from future tick queries (status filter
is `IN ('ACTIVE','WAITING')`). The Phase 10 ops console exposes a
`POST /api/workflows/{id}/runs/{run_id}/repair` endpoint to manually advance
the run to a chosen node + patch scratchpad.

## Phase 0a pivot — confirmed compliance

* No `tag_group` field anywhere in `workflow.py` or `test_executor.py`.
* All schemas referenced lowercase (`coll_*`, `dpd`).
* Engine reads CT properties via handlers (FETCH_CT_PROPS); no executor-level
  HTTP egress.

## Outstanding (not blockers for Wave 4 to start)

1. **Master-auditor pass on `workflow/agents/workflow.py`.** The lint hook
   auto-injected the `MASTER-AUDIT-REQUIRED` marker; the creator-agent
   runtime here does NOT expose the `Task` subagent dispatcher so the audit
   could not be run from this agent. Same constraint Phase 4a documented.
   Sahil (or the next session with `Task` access) should dispatch
   `master-auditor` on `workflow/agents/workflow.py` before this phase is
   formally closed per `PHASES.md` §"Phase Closure Definition" item 2.

2. **Lint suppression to review:** one `# stashfin-lint: ignore` on the
   parameterised IN clause in `_build_version_cache`. The interpolated
   portion is the comma-separated `?` skeleton derived from `len(version_ids)`
   only; all values are bound parameters. Standard sqlite idiom — see
   suppression comment for justification.

3. **Phase 4b/4c handlers not yet integrated.** The executor's REGISTRY-
   based dispatch will pick them up automatically once those phases land
   (Wave 3 parallel work). Tests in this phase exercise only the 6 Phase 4a
   handler types.

## What's intentionally NOT here

* No SET_CT_PROP, SWITCH, WAIT_UNTIL, BRANCH_ON_DISPOSITION, COUNTER,
  ASSIGN_AGENT — Phase 4b/4c.
* No live CT egress at executor level (the architecture pins this).
* No call to `clevertap_trigger.trigger()` — that's the workflow_scheduler's
  job (Phase 6).
* No alerts emission — Phase 8.5.
* No launchd plist — Phase 9.
* No ops console wiring — Phase 10/11.
* No edits to `vibrium-automation/` (verified by `git status`).
