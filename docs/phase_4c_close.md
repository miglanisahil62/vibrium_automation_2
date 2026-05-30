# Phase 4c Closure — Side-Effect Handlers

**Status:** code written + 13 unit tests green; full handler suite at 43 tests; full workflow suite at 121 tests. Master-auditor pass pending (this creator agent lacks the Task-dispatch tool — see "Outstanding").
**Date:** 2026-05-30
**Wave:** Wave 3 (alongside Phase 5 executor + Phase 4b branching + Phase 6 scheduler, per PHASES.md §Parallelism plan).

---

## Scope

Two handlers that mutate state outside the workflow DB:

| Handler | What it writes | Where |
|---|---|---|
| `SET_CT_PROP` | CleverTap profile properties | CT `/1/upload` (via Phase 2 `set_profile`) |
| `ASSIGN_AGENT` | One row | `state/workflow.db.agent_assignments` |

## Deliverables shipped

| Path | Purpose |
|---|---|
| `workflow/agents/workflow_handlers/set_ct_prop.py` | Calls Phase 2 `set_profile`; enforces the `coll_bot_calling` guard; routes `success`/`error`; honors `dry_run` from both `ctx` and kwarg. |
| `workflow/agents/workflow_handlers/assign_agent.py` | INSERTs into `agent_assignments` inside the caller-owned txn. No dedupe (deliberate). |
| `workflow/agents/workflow_handlers/__init__.py` | `REGISTRY` extended with the 2 new keys. Now 12 total (4a:6 + 4b:4 + 4c:2). |
| `workflow/tests/test_handlers.py` | 13 new tests across `TestSetCtProp` (7) and `TestAssignAgent` (6); registry-keys-exact updated to 12. |

---

## The `coll_bot_calling` guard — runtime enforcement of the Phase 0a invariant

Per `docs/phase_0a_decision.md §1`:

> `coll_bot_calling` is READ-ONLY for the engine. The granular trigger values (`ai_vb_calling_highv1`, `_midv1`, `_mid_v2/v3/v4`) are set "directly in the journey" by an upstream system. The engine polls CT for transitions to those values and enrolls customers.

`SET_CT_PROP.execute()` enforces this as a runtime guard, not just a code-review check:

```python
_FORBIDDEN_PROPERTIES: frozenset[str] = frozenset({"coll_bot_calling"})

forbidden_hits = sorted(set(properties.keys()) & _FORBIDDEN_PROPERTIES)
if forbidden_hits:
    return NodeResult(
        next_edge="error",
        scratchpad_patch={"set_ct_prop_error": "forbidden_property: ..."},
        ...
    )
```

The guard is checked **before** any HTTP call — so even a malformed `graph_json` that slips past Phase 10's validator cannot cause a CT write of this property. The test `test_coll_bot_calling_guard_fires` swaps `set_profile` for a function that raises and asserts the guard fires without ever invoking it.

Why this matters: the upstream "Journey" system in CleverTap writes `coll_bot_calling`. If the engine also wrote it, we'd race the upstream writer and corrupt enrollment-eligibility state. The cost of getting this wrong is silent dual-enrollment.

---

## Other design choices documented

### SET_CT_PROP

- **Dry-run dual-source**: both `ctx["dry_run"]` and the `dry_run` kwarg are honored. The Phase 5 executor will pass `dry_run=True` as a kwarg in dry-run mode; ad-hoc callers that build a `ctx` can also set it there. Either True suppresses the HTTP call.
- **`set_profile` is called with `dry_run=False`** inside `SET_CT_PROP` (never passed through). The handler's dry-run path short-circuits before reaching `set_profile`. CT's own `dryRun=1` mode is a different concern (payload validation) and is not exposed here — kept simple.
- **`SetResult.success=False` does NOT raise.** It routes to the `error` edge with `set_ct_prop_error_code` in scratchpad so graph authors can branch on specific CT error codes (e.g. 516 for bad phone).
- **HTTP-level catastrophe (3x 429, 5xx, network) DOES raise** out of `set_profile`. The Phase 5 executor traps it and marks `run.status=ERROR`. We deliberately do not swallow it here — distinguishing "CT batch rejected one record" (recoverable, graph-author branches on it) from "CT is on fire" (operator-level concern) is important.

### ASSIGN_AGENT

- **No dedupe by design.** `agent_assignments` has no UNIQUE constraint. Two consecutive ASSIGN_AGENT events on the same `run_id` create two rows. Documented in the module docstring. Rationale: if a graph loops back to ASSIGN_AGENT (operator-driven edit), the ops user reviewing the table needs to see "this customer was re-routed to an agent twice" — not have the second event silently dropped. Compare with `wf_pending_actions` which DOES dedupe (idempotent CT fire), where the trade-off goes the other way because a duplicate fire is more expensive than a duplicate audit row.
- **`source = workflow:{wf_id}:v{ver_id}`** — same format as `FIRE_VB_CALL`'s `cohort_name`. Lets a future joiner correlate agent-assignments with the cohort fires that preceded them.
- **Caller-owned txn**: handler issues ONE INSERT, does NOT commit. The test `test_caller_owned_transaction` verifies this by rolling back and asserting the row vanishes.

---

## Test results

```
$ python3 -m pytest workflow/tests/test_handlers.py -q
43 passed in 0.20s

$ python3 -m pytest workflow/tests/ -q
121 passed in <1s   # all 4a + 4b + 4c + Phase 1/2/3 tests green
```

13 new tests added on top of Phase 4a + 4b. Acceptance ≥9 met.

---

## Acceptance criteria

- [x] `pytest workflow/tests/test_handlers.py -q` runs ≥9 new tests; all green (13 new).
- [x] REGISTRY has the 2 new entries; total = 12 (4a:6 + 4b:4 + 4c:2). 4b had already merged when 4c started.
- [x] `coll_bot_calling` guard explicitly tested (`test_coll_bot_calling_guard_fires`).
- [x] No file under `/Users/sahil.m/vibrium-automation/` touched.
- [x] Lint clean (stashfin_lint runs as PostToolUse hook on every Write/Edit).

---

## REGISTRY merge state

Phase 4b had already merged by the time 4c started: `__init__.py` already had 10 keys (4a:6 + 4b:4) plus imports for `switch`, `wait_until`, `branch_on_disposition`, `counter`. 4c added 2 new imports + 2 new keys; final state is 12 keys with a "Phase 4a / Phase 4b / Phase 4c" comment grouping in the dict. The `test_ten_keys_exact` assertion was renamed to `test_twelve_keys_exact` and extended.

No conflict experienced — the file was modified between my first Read and my first Edit (4b's merge), which the Edit tool surfaced as "file modified since read"; I re-Read and re-applied my diff against the new baseline. Clean.

---

## Outstanding

- **Master-auditor pass on `set_ct_prop.py`** (INDIVIDUAL — critical-surface, writes to CT) and **group review on `assign_agent.py`**. Per PHASES.md §Phase 4c "Audit gate" + the project's home-CLAUDE.md "auto-invoke on critical-surface files" rule, this must run before the phase can be declared shipped. Creator agents do not have the Task-dispatch tool in this environment; the orchestrator (or a follow-up session with Task tool) must invoke it.

## Scope Notes

- `SET_CT_PROP` honors `dry_run` from BOTH `ctx["dry_run"]` and the `dry_run` kwarg. The task spec only mentioned `ctx.get("dry_run")`; I added kwarg support because Phase 5's executor signature passes `dry_run` as a kwarg (matches every other handler's signature). Either True suppresses the HTTP call.
- `ASSIGN_AGENT` honors `dry_run` symmetrically (same dual-source rationale). The task spec did not mention dry-run for ASSIGN_AGENT explicitly; I implemented it for handler-uniformity with FIRE_VB_CALL / SET_CT_PROP. Added `test_dry_run_skips_insert` to cover it.
- Both handlers route to `next_edge="error"` (not exception) on misconfigured `config` (missing `properties` / missing `reason`). The task spec did not enumerate this case explicitly; routing to error matches the Phase 4a pattern for FETCH_CT_PROPS, CONDITION, and SWITCH.
