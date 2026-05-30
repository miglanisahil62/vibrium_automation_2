# Phase 4a Close-Out Audit — Workflow Handlers — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
Phase 4a (6 handlers + registry + tests) is ready to close. All 30 tests pass against the real Phase 1 schema. The three critical-surface handlers (`condition`, `fire_vb_call`, `await_disposition`) each implement their stated contract correctly: `simpleeval` is functions-disabled with module-level safety caps, `INSERT OR IGNORE` against the schema UNIQUE constraint is genuinely idempotent (verified end-to-end), and the timeout anchor on `entered_node_at_ist` is correct so kill-switch resume cannot extend deadlines. No P0 issues. Three P2s on minor consistency / documentation drift. No `tag_group` writes anywhere in `workflow/`; no edits leaked into `vibrium-automation/`.

## Stack Detected
- **Python:** 3.9 (system) — handlers use only stdlib + `simpleeval`
- **Critical dep:** `simpleeval` (no `__version__` attr; verified at runtime: `FunctionNotDefined` and `NameNotDefined` both subclass `InvalidExpression` ✓)
- **Schema verified:** `workflow/migrations/001_init.py` defines `wf_pending_actions UNIQUE(run_id, node_id, attempt_count)` table-level; `run_id NOT NULL` makes the spec's "partial WHERE run_id IS NOT NULL" effectively redundant — semantically equivalent.
- **Domain:** Vibrium workflow engine (Phase 4a)
- **KB consulted:** `auditor_kb/system-design-patterns.md` (retry/idempotency), `auditor_kb/datetime-timezones.md` (IST naive canon), `feedback_no_hallucination`, `project_vibrium_event_ts_format_drift`
- **Live lookups:** simpleeval exception hierarchy verified locally via runtime introspection

## P0 — None

## P1 — None

## P2 — Nice to Fix

### P2-1: `condition.py` docstring claims `NameError` is in the handled tuple; code lists `ValueError` instead
- **File:** [workflow/agents/workflow_handlers/condition.py](workflow/agents/workflow_handlers/condition.py#L49-L55)
- **Evidence:** Module docstring (line 17-19) lists `NameError`; the tuple `_HANDLED_EXCEPTIONS` (line 49-55) lists `InvalidExpression, SyntaxError, TypeError, ValueError, ZeroDivisionError`. `NameNotDefined` is a subclass of `InvalidExpression` (verified), so behavior is correct — but the docstring is misleading.
- **Why it's wrong:** Doc drift. Future maintainer reading the docstring will wonder where `NameError` handling is. Recommend rewriting the comment to say "all `InvalidExpression` subclasses including `NameNotDefined`, `FunctionNotDefined`, `AttributeDoesNotExist`."
- **Required fix:** Reword the doc; no code change.

### P2-2: `fire_vb_call.py` does not surface `scheduled_at_ist` semantics in dry-run output
- **File:** [workflow/agents/workflow_handlers/fire_vb_call.py](workflow/agents/workflow_handlers/fire_vb_call.py#L72-L82)
- **Evidence:** Dry-run side_effect string includes cid/run_id/node_id/attempt/cohort but not the `scheduled_at_ist` that would have been written.
- **Why it's wrong:** Operators auditing a workflow's dry-run from `workflow_node_log` cannot see the would-be schedule time. Low impact but cheap to add.
- **Required fix:** Include `scheduled_at={now_ist}` in the DRY-RUN string.

### P2-3: `await_disposition.py` clears `last_disposition_action_class` semantics is documented inconsistently
- **File:** [workflow/agents/workflow_handlers/await_disposition.py](workflow/agents/workflow_handlers/await_disposition.py#L94-L114)
- **Evidence:** Module docstring (line 28-32) says "Once routed (disposition OR timeout), the handler clears the `last_disposition_action_class` key." Actual code (line 105-111) does NOT clear it — the comment at line 107-111 explicitly says "we intentionally do NOT clear … BRANCH_ON_DISPOSITION reads it on the next tick." Two doc blocks contradict.
- **Why it's wrong:** Doc-vs-code disagreement. Behavior is correct (don't clear; Phase 4b BRANCH_ON_DISPOSITION owns clearing). The module header is stale relative to the Phase 4b decision.
- **Required fix:** Rewrite module-level NB to match inline comment (clearing is BRANCH_ON_DISPOSITION's job).

## Detailed Findings — Critical 3

### condition.py
1. `SimpleEval(names=run.scratchpad, functions={})` — functions disabled ✓ (line 80)
2. `simpleeval.MAX_STRING_LENGTH = 1024`, `MAX_POWER = 100` at module level ✓ (line 43-44). simpleeval reads from module namespace, so this covers every instance.
3. Catches `InvalidExpression, SyntaxError, TypeError, ValueError, ZeroDivisionError` ✓ — `FunctionNotDefined` and `NameNotDefined` covered by `InvalidExpression` parent (verified at runtime).
4. Test `test_functions_disabled_rejects_int_call` (line 246-255) confirms `int(x)` routes to error ✓
5. Bool coercion via `bool(result)` ✓ (line 97) — Pythonic truthiness, documented in docstring.
6. No PII in error string — error captures `type(exc).__name__` and `exc` only; condition expressions reference scratchpad keys, not values directly. ✓
7. SF006 lint suppression at line 84 with clear justification ✓

### fire_vb_call.py
1. Uses caller-owned `txn` ✓ — no `sqlite3.connect()` call. (line 87 `txn.execute(...)`)
2. SQL matches spec exactly ✓ (line 88-103). Column order matches schema 001 (no auto-default fields touched).
3. `attempt_count = int(run.scratchpad.get("attempts", 0) or 0)` ✓ (line 66) — `or 0` guards None/zero/falsy. Test `test_attempts_from_scratchpad` covers (line 326-334).
4. `cohort_name = f"workflow:{run.workflow_id}:v{run.version_id}"` ✓ (line 68)
5. `cursor.rowcount == 0` branch returns `next_edge="queued"` with dedupe-hit side_effect ✓ (line 105-121). Test `test_dedupe_second_insert_no_op` (line 309-324) exercises real SQLite + real schema + second INSERT → confirms idempotent advance, only 1 row, second side_effect contains "dedupe-hit". ✓
6. No `tag_group` references in payload ✓
7. `_now_ist_str()` uses `datetime.now(ZoneInfo("Asia/Kolkata"))` ✓ (line 45) — naive-IST format matches event-ts canon
8. No PII logging — log emits `cid={int}` only, no phone/email/PAN. ✓

### await_disposition.py
1. Anchor: `_parse_ist_ts(run.entered_node_at_ist) or _now_ist()` (line 118). Deadline = `anchor + timedelta(hours=timeout_hours)`. ✓ — kill-switch pause cannot shift; pure deterministic from `entered_node_at_ist`.
2. Falls back to `_now_ist()` only when `entered_node_at_ist` is None/unparseable (line 118 `or _now_ist()`, line 64-66 returns None). ✓
3. Routing precedence: disposition → timeout → park ✓ (line 94, 124, 138). Test `test_disposition_wins_even_past_deadline` (line 390-399) confirms order.
4. On park: sets `run.status = "WAITING"`, `run.ready_at_ist = deadline_str`, returns `next_edge=None`, also surfaces `ready_at_ist` in NodeResult ✓ (line 139-146) — executor receives deadline via both `run` mutation and `NodeResult.ready_at_ist`.

### Group review (handlers 4-8)
- **REGISTRY** (line 31-38): exactly 6 keys: ENROLL, FETCH_CT_PROPS, CONDITION, FIRE_VB_CALL, AWAIT_DISPOSITION, TERMINATE ✓. Test `test_six_keys_exact` enforces.
- **types.py:** `NodeResult` and `NodeConfig` frozen ✓ (`@dataclass(frozen=True)`); `Run` mutable ✓. Field set matches schema 1:1.
- **fetch_ct_props.py:** Strict coercion (line 47-92) — booleans rejected for int/float (good — prevents `True→1` silent collapse); bool coercion only accepts literal True/False/"true"/"false"/"True"/"False"/0/1 (good). Uses `clevertap_profile.get_profile` returning `record` dict; reads `record["profileData"]` ✓ (line 139). `coercion_failed_property` set on every error edge ✓ (line 120, 156, 172).
- **terminate.py:** Mutates `run.status='DONE'`, `run.terminal_status`, `run.terminated_at_ist`, `run.ready_at_ist=None` ✓ (line 56-59). Defaults empty/whitespace `status` → `'DONE'` ✓.
- **enroll.py:** Trivial pass-through to `next` edge ✓.

### Test suite
`pytest workflow/tests/test_handlers.py -q` → **30 passed** (only urllib3 LibreSSL deprecation warning; unrelated to handlers).

### Cross-cutting
- No file under `/Users/sahil.m/vibrium-automation/` modified for Phase 4a (timestamps cluster around 2026-05-29/30 but are unrelated edits — `run.sh`, conftest, etc., are not handler-touching).
- `tag_group` appears in `workflow/` only in **comments and docstrings** that explicitly document its absence per Phase 0a — and in `test_migrations.py` enforcing its non-existence. No code reference. ✓
- **SF006 lint suppressions:** 2 found — `condition.py:84` and `fetch_ct_props.py:167`. Both have clear justifications citing the documented "error edge by design" contract. Acceptable.

## Assumptions Made
- `simpleeval` package version on Sahil's machine matches Phase 4a's pin (couldn't read version attr). Behavior validated at runtime; treat as verified.
- `wf_store.transaction()` opens `BEGIN IMMEDIATE` as the fire_vb_call docstring claims — not re-verified in this audit (out of scope; Phase 1 close did this).

## What I Did Not Audit
- Executor (Phase 5) — out of scope.
- `workflow_scheduler` (Phase 6) consuming `wf_pending_actions` rows — out of scope.
- Performance of `SimpleEval` construction per call (creates a fresh evaluator each tick); fine for current scale, may want pooling at 10k+ runs/sec.

## KB Updates Applied
None this pass — no new gotchas discovered. simpleeval's exception hierarchy was already implicitly covered.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES ✓ — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no HTTP surface in Phase 4a |
| 3 | Console wiring | N/A — internal engine modules; no UI surface |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Phase 4a is purely an internal engine layer (handler library + registry + unit tests). Ship gate for the workflow engine as a whole binds when Phase 5 (executor) + Phase 6 (scheduler) + ops console wiring land.

## Recommendations
- **Address the 3 P2s opportunistically** (docstring tidy-ups; no behavior change).
- **Phase 4a is shippable as-is.** Proceed to Phase 4b (SWITCH, WAIT_UNTIL, BRANCH_ON_DISPOSITION, COUNTER).
- When Phase 4b's BRANCH_ON_DISPOSITION lands, audit must verify it actually clears `last_disposition_action_class` — the await_disposition module-level doc currently makes the wrong claim and Phase 4b is the load-bearing implementation.
