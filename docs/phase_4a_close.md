# Phase 4a Closure — Core Node Handlers

**Status:** code written + 30 unit tests green. Master-auditor passes pending
(creator agent does not have the Task-dispatch tool — see "Outstanding" below).
**Date:** 2026-05-30
**Wave:** Wave 2 (alongside Phase 7 ingest + Phase 8 enrollment, per PHASES.md
§Parallelism plan).

---

## Scope

Six handlers + types + registry — the minimum to unblock Phase 5 (executor).

## Deliverables shipped

| Path | Purpose |
|---|---|
| `workflow/agents/workflow_handlers/types.py` | `NodeResult`, `Run`, `NodeConfig` dataclasses. |
| `workflow/agents/workflow_handlers/enroll.py` | Trivial pass-through onto `"next"` edge. |
| `workflow/agents/workflow_handlers/fetch_ct_props.py` | CT profile GET via Phase 2; type coercion at fetch time; routes `success` / `not_found` / `error`. |
| `workflow/agents/workflow_handlers/condition.py` | simpleeval evaluator with `functions={}`, `MAX_STRING_LENGTH=1024`, `MAX_POWER=100`; routes `true` / `false` / `error`. |
| `workflow/agents/workflow_handlers/fire_vb_call.py` | `INSERT OR IGNORE` into `wf_pending_actions` inside the executor-owned txn; `cohort_name = workflow:{wf}:v{ver}`. NO `tag_group` (Phase 0a pivot). |
| `workflow/agents/workflow_handlers/await_disposition.py` | Routes `disposition` (scratchpad has `last_disposition_action_class`) or `timeout` (deadline elapsed), else parks (`status=WAITING`, `ready_at_ist=anchor+timeout_hours`). |
| `workflow/agents/workflow_handlers/terminate.py` | Sets `run.status='DONE'`, `terminal_status`, `terminated_at_ist`. |
| `workflow/agents/workflow_handlers/__init__.py` | `REGISTRY` of 6 entries. |
| `workflow/tests/test_handlers.py` | 30 unit tests (one class per handler + registry sanity). |

## Test results

```
$ python3 -m pytest workflow/tests/test_handlers.py -q
30 passed in 0.21s

$ python3 -m pytest workflow/tests/ -q
108 passed in <1s   # all existing migrations / wf_store / CT tests still green
```

## Acceptance criteria

| Criterion | Status |
|---|---|
| `pytest workflow/tests/test_handlers.py -q` runs ≥ 18 tests, all green | PASS (30 tests) |
| `python3 -c "from workflow.agents.workflow_handlers import REGISTRY; print(list(REGISTRY))"` lists 6 keys | PASS (`['ENROLL', 'FETCH_CT_PROPS', 'CONDITION', 'FIRE_VB_CALL', 'AWAIT_DISPOSITION', 'TERMINATE']`) |
| No file under `/Users/sahil.m/vibrium-automation/` modified | PASS |
| CONDITION rejects `"int(x)"` cleanly to error edge | PASS (`TestCondition::test_functions_disabled_rejects_int_call`) |
| FIRE_VB_CALL second-insert is a no-op (dedupe) | PASS (`TestFireVbCall::test_dedupe_second_insert_no_op`) |

## Decisions made inside scope

1. **`NodeResult.next_edge: str | None`** — None used by both terminal nodes
   (TERMINATE, run.status='DONE') and parks (AWAIT_DISPOSITION,
   run.status='WAITING'). Executor (Phase 5) distinguishes via `run.status`.
   This matches the architecture doc's prose pattern; no new field added.

2. **`Run` is mutable, `NodeResult` and `NodeConfig` are frozen.** Handlers
   mutate `Run` (status, ready_at_ist, terminal_status, terminated_at_ist) —
   the executor persists those mutations at txn commit. This avoids stuffing
   "run mutation patch" into NodeResult, which keeps the result type clean.

3. **Type coercion strictness in fetch_ct_props:** `int` and `float` refuse
   to coerce `bool` (silent `True→1` is a footgun). `bool` coerces only
   from explicit `True/False/0/1/"true"/"false"`. All other coercions raise
   `ValueError` and route to the `error` edge with `coercion_failed_property`
   set.

4. **AWAIT_DISPOSITION anchor:** deadline is computed from
   `run.entered_node_at_ist` (or now, if absent). This makes a kill-switch
   pause+resume idempotent — the deadline doesn't shift.

5. **AWAIT_DISPOSITION does NOT clear `last_disposition_action_class`.**
   The downstream BRANCH_ON_DISPOSITION handler (Phase 4b) reads it on the
   next tick and is responsible for clearing. Documented inline.

6. **`MAX_STRING_LENGTH` and `MAX_POWER` are set at module import time of
   `condition.py`** (simpleeval reads them from its module namespace). This
   pins them for every SimpleEval instance created downstream.

## Phase 0a pivot — confirmed compliance

- ✅ No `tag_group` field anywhere in fire_vb_call.py or the queued row.
- ✅ Disposition wakeup signal is `last_disposition_action_class` in
  scratchpad (set by Phase 7's `workflow_ingest.py`).
- ✅ All property names lowercase (`coll_*`, `dpd`).
- ✅ Engine reads `coll_bot_calling`; no handler writes it.

## Outstanding (not blockers for Phase 5 to start)

1. **Master-auditor passes.** Task instructed to dispatch `master-auditor`
   on `condition.py`, `fire_vb_call.py`, `await_disposition.py` individually
   and a group review on `enroll.py`, `fetch_ct_props.py`, `terminate.py`.
   The creator agent runtime does NOT expose the Task tool, so the dispatch
   could not be done from this agent. Sahil (or the next session with Task
   access) should run those audits before this phase is formally closed per
   PHASES.md §"Phase Closure Definition" item 2.

   The lint hook auto-injected `MASTER-AUDIT-REQUIRED` markers on each
   file as it was written; those reminders are pending action.

2. **Phase 4b (SWITCH / WAIT_UNTIL / BRANCH_ON_DISPOSITION / COUNTER)**
   and **Phase 4c (SET_CT_PROP / ASSIGN_AGENT)** remain out of scope per
   the split mandated in PHASES.md.

## What's intentionally NOT here

- No executor (`workflow/agents/workflow.py`). Phase 5.
- No SET_CT_PROP. Phase 4c.
- No SWITCH / WAIT_UNTIL / COUNTER / BRANCH_ON_DISPOSITION. Phase 4b.
- No live HTTP / no live SQLite writes outside the executor-owned txn.
- No edits to `vibrium-automation/` (verified by file tree).
