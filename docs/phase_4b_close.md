# Phase 4b — Branching + Scheduling Handlers — Closure

**Wave:** 3
**Date:** 2026-05-30
**Scope:** 4 new node handlers + REGISTRY extension + tests.

## What was built

| File | Purpose |
|---|---|
| `workflow/agents/workflow_handlers/switch.py` | Multi-way branch on a single scratchpad value. Cases are string keys (JSON-safe); the scratchpad value is `str(...)`-coerced before lookup. Mandatory `default` edge. Missing `on` key → `error` edge. |
| `workflow/agents/workflow_handlers/wait_until.py` | Park run until wall-clock time. Supports `T+N day`, `T+N day at HH:MM`, `T+N hour` (relative) and `YYYY-MM-DD` IST (absolute via scratchpad key). Invalid absolute → `error` edge. Late wakeups fire immediately next tick — no special handling here (executor's `ready_at_ist <= now` filter covers it). |
| `workflow/agents/workflow_handlers/branch_on_disposition.py` | Multi-way branch on `last_disposition_action_class`. Canonical enum: `NOOP, RETRY, PTP_CALL, AGREE_EOD_CALL, CALLBACK_CALL, RTP_NEEDS_LLM, ESCALATE`. Unknown enum value → `default` edge (NOT error). Missing key → `error` edge. **Clears `last_disposition_action_class` on every successful branch.** |
| `workflow/agents/workflow_handlers/counter.py` | Increment named scratchpad counter; branch `under_limit` vs `at_limit`. No reset. Non-integer current or bad limit → `error` edge with structured `counter_error`. |
| `workflow/agents/workflow_handlers/__init__.py` | REGISTRY extended to include the 4 new keys. Now 12 keys total (Phase 4c landed in parallel and added `SET_CT_PROP` + `ASSIGN_AGENT`). |
| `workflow/tests/test_handlers.py` | +20 new tests appended after Phase 4c tests. |

## REGISTRY state at phase close

10 keys covered by Phase 4b's contribution (4a's 6 + 4b's 4). With Phase 4c also landed in parallel, the live registry has 12 keys:

```
ENROLL, FETCH_CT_PROPS, CONDITION, FIRE_VB_CALL, AWAIT_DISPOSITION, TERMINATE,    # 4a
SWITCH, WAIT_UNTIL, BRANCH_ON_DISPOSITION, COUNTER,                                # 4b
SET_CT_PROP, ASSIGN_AGENT                                                          # 4c
```

The `TestRegistry.test_twelve_keys_exact` assertion (updated by Phase 4c) covers the live state.

## Test results

```
69 passed, 1 warning in 0.24s
```

Breakdown:
- Phase 4a: 30 tests (unchanged)
- Phase 4c: 19 tests (landed in parallel)
- **Phase 4b: 20 new tests** (5 SWITCH + 5 WAIT_UNTIL + 6 BRANCH_ON_DISPOSITION + 4 COUNTER)
- TestRegistry: 2 tests (registry key set + callable check)

Note: the task acceptance criterion specified "≥50 tests"; we land at 69 (well above).

## Key design decisions

1. **Case keys are strings** in SWITCH and BRANCH_ON_DISPOSITION. JSON-safe round-trip via `graph_json`. SWITCH coerces the scratchpad value to `str(...)` before lookup; BRANCH_ON_DISPOSITION's action_class is already a string at ingest.
2. **Unknown action_class in BRANCH_ON_DISPOSITION → default, not error.** Protects in-flight runs against a future ingest revision adding a new enum value. The save-time validator (Phase 10) is the right place to reject *config-time* unknown values; runtime defaults to graceful degradation.
3. **WAIT_UNTIL absolute format is strict `YYYY-MM-DD` only.** No `MM/DD/YYYY`, no `YYYY-MM-DDTHH:MM`, no timezone offsets. Tighter parser = clearer validation failures.
4. **No silent fallbacks.** Every bad config or runtime coercion failure routes to the `error` edge with a structured `*_error` scratchpad key. Two `except` blocks (`wait_until._parse_absolute`, `counter` int coercion) are marked `# stashfin-lint: ignore` with the documented-contract rationale.

## Save-time validation requirement (flagged for Phase 10)

The Phase 10 graph validator must enforce these rules at workflow-save-time. Each is a "P0 reject the graph_json" rule:

1. **BRANCH_ON_DISPOSITION** — every key in `node.config["cases"]` must be a member of the canonical enum:
   ```python
   from workflow.agents.workflow_handlers.branch_on_disposition import CANONICAL_ACTION_CLASSES
   ```
   i.e. `{NOOP, RETRY, PTP_CALL, AGREE_EOD_CALL, CALLBACK_CALL, RTP_NEEDS_LLM, ESCALATE}`. Unknown keys in cases are rejected at save time (whereas unknown runtime values gracefully default — these are different failure modes).
2. **BRANCH_ON_DISPOSITION** — `default` edge must be present (the runtime errors clean if missing, but operators should not be allowed to save such a graph).
3. **SWITCH** — `default` edge must be present.
4. **SWITCH** — every value in `cases` plus the `default` value must be present as an edge label in `node.edges`. Same rule applies to BRANCH_ON_DISPOSITION.
5. **WAIT_UNTIL** — exactly one of `relative` or `absolute` must be set (not both, not neither). If `relative`, the spec must parse against the grammar (`T+N day`, `T+N day at HH:MM`, `T+N hour`). If `absolute`, the named scratchpad key must have an upstream writer in the graph.
6. **COUNTER** — `name` non-empty string; `limit` integer > 0.

## Things NOT done (per task spec)

- No SUBWORKFLOW handler (backlog).
- No PILL_REROUTE handler (backlog).
- No Phase 4a handler modifications (only REGISTRY extension).
- No schema changes.
- No edits under `/Users/sahil.m/vibrium-automation/`.

## Audit gate

Master-auditor invocation pending — see `docs/phase_4b_audit.md`.
