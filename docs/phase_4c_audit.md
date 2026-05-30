# Phase 4c Close-Out Audit — 2026-05-30

**Scope:** `set_ct_prop.py`, `assign_agent.py`, `__init__.py` (registry), `test_handlers.py` (Phase 4c additions).

## Verdict: PASS

13 Phase 4c tests pass. Registry contains exactly the 12 required keys. Phase 0a runtime invariant is correctly enforced. No P0/P1 findings.

---

## Verification Matrix

### 1. `coll_bot_calling` guard (Phase 0a runtime enforcement) — PASS

- `_FORBIDDEN_PROPERTIES: frozenset[str] = frozenset({"coll_bot_calling"})` at module scope (line 57). Extensible.
- Guard at lines 97–112 fires via `set(properties.keys()) & _FORBIDDEN_PROPERTIES` — set-intersection is correct regardless of dict ordering; uses `sorted()` for stable error messages.
- Guard fires **before** the `clevertap_profile.set_profile` HTTP call (line 141). Verified by `TestSetCtProp.test_coll_bot_calling_guard_fires` which sets `set_profile` to raise `AssertionError`; test passes, proving no HTTP call.
- Returns `next_edge="error"` with `set_ct_prop_error` in scratchpad containing the forbidden key name and a Phase 0a reference. Logs warning with `cid` and offending keys. Side-effect surfaces the keys for the action log.
- Test asserts both `forbidden_property` substring and `coll_bot_calling` literal in the error.

This is the load-bearing item — fully verified. The invariant is now defended at runtime, not just at code-review time.

### 2. `set_ct_prop.py` general correctness — PASS

- Calls `clevertap_profile.set_profile(run.customer_id, properties, dry_run=False)` — relies on Phase 2's `unprocessed[]` inspection per CT KB hard rule (confirmed in `clevertap_profile.py:418-420` docstring).
- Failure path: `result.success=False` → `next_edge="error"`, `set_ct_prop_error_code=<code>` in scratchpad (lines 153–161). Verified by `test_ct_failure_routes_to_error_with_code`.
- Success path: `next_edge="success"`, `last_ct_prop_set_at=<IST-naive ts>` (lines 145–151). Verified.
- Dry-run propagation: BOTH `ctx["dry_run"]` AND `dry_run=` kwarg (lines 114–118) — short-circuits before HTTP, scratchpad still patched. Verified by `test_dry_run_skips_real_ct_call` (ctx path) and `test_dry_run_via_kwarg_also_works` (kwarg path; kwarg test sets `set_profile` to raise).
- Missing/empty `properties` config → `next_edge="error"` (lines 83–92). Verified.
- No PII logged: only `customer_id` (an ID, not a phone/email) and property keys (not values).
- Timestamp uses `datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")` — matches event-ts canon, avoids SF002 (tz-aware then formatted).

Minor observation (P2, drive-by): the handler always passes `dry_run=False` to `set_profile` (line 142) and instead short-circuits via `effective_dry_run`. This is intentional defensive design — skips HTTP entirely rather than depending on CT's `?dryRun=1`. Documented behavior; not a finding.

### 3. `assign_agent.py` correctness — PASS

- Single `INSERT INTO agent_assignments(customer_id, reason, source, assigned_at_ist, run_id)` (lines 109–122). Schema match confirmed against migration 001 (`id`/`customer_id`/`reason`/`source`/`assigned_at_ist`/`assigned_to`/`resolved_at_ist`/`resolution_note`/`run_id`; nullable cols omitted is correct).
- `source = f"workflow:{run.workflow_id}:v{run.version_id}"` (line 82). Verified by `test_source_field_format`.
- `assigned_at_ist` is IST-naive `%Y-%m-%d %H:%M:%S` (line 59). Matches event-ts canon — avoids the Vibrium ts-format drift gotcha.
- Uses caller-owned `txn` — handler issues `txn.execute(...)` only; no `txn.commit()`. Verified by `test_caller_owned_transaction` which rolls back and asserts the row vanished.
- No dedupe: verified by `test_no_dedupe_two_calls_two_rows` (two calls → two rows). Documented in module docstring.
- Missing/empty `reason` → `next_edge="error"`, no INSERT. Verified.
- Dry-run honored via both ctx and kwarg paths.

### 4. REGISTRY at 12 keys — PASS

`python3 -c "..."` output:
```
['ASSIGN_AGENT', 'AWAIT_DISPOSITION', 'BRANCH_ON_DISPOSITION', 'CONDITION', 'COUNTER', 'ENROLL', 'FETCH_CT_PROPS', 'FIRE_VB_CALL', 'SET_CT_PROP', 'SWITCH', 'TERMINATE', 'WAIT_UNTIL']
count: 12
```
Exact match. `TestRegistry.test_twelve_keys_exact` codifies this; `test_all_handlers_callable` confirms each is callable.

### 5. Tests — PASS

`pytest -k "SetCtProp or AssignAgent"` → **13 passed** (7 SetCtProp + 6 AssignAgent). Phase 4b tests (SWITCH/WAIT_UNTIL/BRANCH/COUNTER) are also present in this file and untouched.

### 6. Cross-cutting — PASS

- `git status` confirms no changes under `/Users/sahil.m/vibrium-automation/` (all changes are within `vibrium-workflow/`).
- `tag_group` grep across handlers + tests: only appearance is in `fire_vb_call.py:19-20` as a doc-comment explicitly confirming it is NOT written. No reintroduction.
- Lint clean on all four files (no SF001–SF023 hits; suppression-free).

---

## P0 / P1 / P2

None.

## Assumptions

- `clevertap_profile.set_profile`'s contract (Phase 2) correctly inspects `unprocessed[]` per CT KB. Spot-checked the docstring; not re-audited in this pass.
- Migration 001's `agent_assignments` schema is the canonical source; column nullability is correct (only `customer_id`, `reason`, `assigned_at_ist` NOT NULL).

## What I Did Not Audit

- The full executor (Phase 5) — only verified handler shape is consistent with the documented `execute(node, run, ctx, txn, dry_run)` signature.
- Whether `validator.py` (Phase 10) also lists `coll_bot_calling` in a property allowlist — the runtime guard is sufficient per spec.

## Recommendations

Ready to close Phase 4c. The runtime `coll_bot_calling` guard is the right shape — set-based, fires pre-HTTP, surfaces the offending key in scratchpad + log + side-effect, and is regression-protected by a test that fails loudly if the guard ever gets removed.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no FastAPI surface added |
| 3 | Console wiring | N/A — Phase 4c is engine internals |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Phase 4c is engine-internal (handler implementations + registry). Ship pipeline gates 2–4 do not apply until a workflow consuming these handlers is wired into the ops console.
