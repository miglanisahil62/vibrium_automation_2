# Phase 3 — Master-Auditor Dispatch (pending)

**Status:** Creator-agent delivery complete. Master-auditor has NOT yet been dispatched from within the creator-agent environment (the Task subagent tool was not available in this session). Sahil to dispatch `master-auditor` manually before merging `phase3-shared-audit-cap` to `main` on `vibrium-automation`.

---

## Files to audit (critical surface — all touch the live cap-enforcement code)

1. **`/Users/sahil.m/vibrium-workflow/shared/customer_call_audit.py`** (new, 261 lines)
2. **Diff of `/Users/sahil.m/vibrium-automation/scripts/pre_call_gate.py`** (227 lines added; signature of `check_cap_and_cooldown` preserved)
3. **Diff of `/Users/sahil.m/vibrium-automation/scripts/scheduler.py`** (61 lines added; one new call site after successful CT trigger)

## Dispatch command (Sahil to run)

```
# In Claude Code session at ~/vibrium-workflow:
# Invoke via Task subagent_type: master-auditor with the three target paths above.
# Phase 3 spec requires individual-file audit (not bundled).
```

## Creator's self-audit summary (pre-flight, NOT a substitute for master-auditor)

**Critical paths verified:**

- `is_callable_now()` — always-on, no feature flag. ZoneInfo("Asia/Kolkata") used; UTC-aware input converted before hour comparison. Exclusive end (08:00 fires, 19:00 blocks).
- `check_cap_and_cooldown()` — public signature byte-identical; new kw-only `vibrium_db_path` is optional. Flag-off path goes through `_check_cap_and_cooldown_legacy` (verbatim pre-Phase-3 body). Flag-on path requires `vibrium_db_path` (ValueError if missing — surfaces config bug, doesn't silently fall back).
- `record_fire()` — `source` validated by Python `assert` before INSERT (clearer stack than the table CHECK constraint, which is also in place as belt-and-braces). `fired_at_ist` set by function from `datetime.now(IST)` — caller cannot override.
- `batch_count_today()` — single SQL query, `customer_id IN (?,...)` with explicit placeholder expansion (tuple binding is not supported by sqlite3). Today's IST date computed in Python and passed as a bound param (not `DATE('now','localtime')`) so the query is host-agnostic (Mac=IST, AWS=UTC).
- `scheduler.py` audit insert — lazy-imported, wrapped in try/except that logs a warning. Audit failure does NOT roll back the FIRED status. Rationale documented inline.

**Byte-identical-when-off contract:**

- Full vibrium-automation test suite ran with flag unset on the Phase 3 branch: 219/220 pass. Single failure (`test_clevertap_trigger.py::test_classify_unprocessed_with_other_cid_still_failed_generic`) was confirmed to predate this branch by `git stash` + re-run; not caused by Phase 3.

**Things the auditor should look hard at:**

1. The cooldown decision on the flag-on path (cooldown still reads from local `today_fired_rows`, NOT from `customer_call_audit`). This is intentional per architecture but is the most non-obvious choice in the diff — document it in audit verdict if it changes.
2. `record_fire` source assertion happens BEFORE the DB connection opens — confirm no leaking connections on the assertion-fail path. (No leak: no resource is acquired.)
3. `_use_shared_audit()` reads env every call (no caching) — intentional so mid-process flag flips take effect. Confirm no perf regression (env read is trivial; the runtime regression test pins the batched hot path to within 20%).
4. `vibrium_db_path` not required when flag is off — verify all flag-off callers still work without supplying it (signature preserved; tested).

---

# Master-Auditor Verdict — 2026-05-30

## Verdict: FAIL (1 P0, 2 P1, 1 P2)

**Note:** Without the P0 fix, the flag-on path is unusable in production. Tests pass because the test suite exercises `check_cap_and_cooldown` directly with `vibrium_db_path=...` — but the real `scheduler.py` callsite does NOT pass it. Flag-on rollout would crash on first cap check.

## TL;DR

Code quality is high — backward-compat on the flag-off path is correctly preserved (22/22 pre-Phase-3 tests byte-identical green; verified via `git stash` re-run), `is_callable_now()` is correctly the first gate, `record_fire()` validates `source` ∈ `{'adhoc','workflow'}` via both Python assert AND SQL CHECK, IST-naive timestamps match the freshness-monitor contract, WAL concurrency test seeds 10 parallel writers and asserts all 10 land, scheduler audit insert is correctly wrapped in `try/except` with `log.warning`, and `clevertap_trigger.py` + `ingest.py` are correctly untouched by Phase 3 per the 0a pivot (`ingest.py`'s diff is pre-existing `cohort_name` work from commit `2b11567`, not Phase 3). However, **the flag-on path has a wiring bug that makes the production rollout DOA** — `scheduler.py:398` and `fire_immediate.py:109` both call `check_cap_and_cooldown(cid, today_fired)` without the new kw-only `vibrium_db_path`. With `USE_SHARED_AUDIT_CAP=true`, every gate check raises `ValueError` per pre_call_gate.py:244-249. The "ValueError surfaces config bug" design intent is correct; the bug is that the callers themselves were never updated to pass the path.

## Stack Detected
- Python 3.13 (system); stdlib sqlite3 + ZoneInfo only on the new code paths.
- vibrium-automation branch `phase3-shared-audit-cap` (uncommitted; commits up to `3033a2e` are pre-Phase-3).
- vibrium-workflow on `main` (uncommitted shared/, tests, docs).
- KB consulted: `system-design-patterns.md` (idempotency, time-correctness), CLAUDE.md rules 1/2/10/11, memory `project_vibrium_event_ts_format_drift`.

## P0 — Must fix before merge

### P0-1: Flag-on rollout is broken — `scheduler.py:398` and `fire_immediate.py:109` don't pass `vibrium_db_path`
- **Category:** Correctness / Idempotency
- **Files:** [scripts/scheduler.py:398](/Users/sahil.m/vibrium-automation/scripts/scheduler.py#L398), [scripts/fire_immediate.py:109](/Users/sahil.m/vibrium-automation/scripts/fire_immediate.py#L109)
- **Evidence (scheduler.py:398):** `cap = check_cap_and_cooldown(cid, today_fired)` — no `vibrium_db_path=` kwarg.
- **Why wrong:** pre_call_gate.py:244-249 raises `ValueError` when flag is on AND path is missing. Sahil's intent (per docstring) is "surface the config bug" — correct as a library invariant, but neither production caller was updated to pass the path. Flipping `USE_SHARED_AUDIT_CAP=true` in prod → every per-customer gate check in `scheduler.run()` raises → the loop's outer try/except catches it as ERROR for that row, the tick degrades to 0 fires, heartbeat probably stays "ok" because no exception escapes to the orchestrator. Silent kill of cap enforcement on rollout.
- **Same bug in fire_immediate.py:109** — single-customer "fire now" path, used by ops console "fire" button.
- **Required fix:**
  1. In `scheduler.py:398`, change to `cap = check_cap_and_cooldown(cid, today_fired, vibrium_db_path=config.get("vibrium_db_path"))`. The function tolerates `None` when flag is off (default), so this is safe under both flag states.
  2. Same edit at `fire_immediate.py:109` — wire `vibrium_db_path` through from the caller's config.
  3. Add a test in `tests/test_scheduler.py` that runs the scheduler with `USE_SHARED_AUDIT_CAP=true` against a tmp `vibrium.db` and confirms zero `ValueError`s in the log.

## P1 — Should fix

### P1-1: `_maybe_record_audit_fire` only writes on `success`, not `delivery_failed` — but cooldown rationale isn't symmetric
- **Category:** Correctness (semantic)
- **File:** [scripts/scheduler.py:469-507](/Users/sahil.m/vibrium-automation/scripts/scheduler.py#L469-L507)
- **Evidence:** Only the `res["status"] == "success"` branch calls `_maybe_record_audit_fire`. `delivery_failed` and ERROR don't. The legacy `today_fired.append` is also only in the success branch, so the flag-off and flag-on behaviours are symmetric for the cap. OK.
- **However:** the docstring at pre_call_gate.py:222-231 says cooldown is "back-to-back within 3h on the adhoc-system's own pending_actions" — fine for the cap-side, but a future workflow scheduler that fires the same customer 30 mins after an adhoc success will NOT see a cooldown trip from the shared audit (only the adhoc-system's `today_fired` slice). That's deliberate (architecture decoupling) — but the workflow scheduler currently has NO cooldown enforcement of its own that I could find (Phase 6 deferred). Flag this for Phase 6 audit. Not a Phase 3 blocker.
- **Required fix:** Add an architectural-decision-record line to `docs/phase_3_close.md` noting "workflow scheduler will own its own cooldown table; cross-system cooldown is NOT pooled." So Phase 6 author doesn't accidentally pool it.

### P1-2: `batch_check_with_shared_audit` doesn't check cooldown at all — risk if a caller swaps it in for `check_cap_and_cooldown`
- **Category:** API safety
- **File:** [scripts/pre_call_gate.py:134-168](/Users/sahil.m/vibrium-automation/scripts/pre_call_gate.py#L134-L168)
- **Evidence:** Docstring says "Cooldown is NOT enforced here — callers that need cooldown still call `check_cap_and_cooldown`." That's a footgun: future caller batched-substitutes `batch_check_with_shared_audit` for `check_cap_and_cooldown` and silently drops cooldown enforcement.
- **Required fix:** Rename to `batch_cap_only_check_with_shared_audit` OR raise a docstring banner + add a runtime warning if `customer_ids` size > 1 and called without an explicit `cooldown_enforced_by_caller=True` flag. The current name is too close to `check_cap_and_cooldown` for safety.

## P2 — Nice to fix

### P2-1: `batch_count_today` index efficiency — DATE(fired_at_ist) vs substr()
- **Category:** Performance
- **File:** [shared/customer_call_audit.py:197](/Users/sahil.m/vibrium-workflow/shared/customer_call_audit.py#L197)
- **Evidence:** `substr(fired_at_ist, 1, 10) = ?` — works correctly (lexical-prefix match on ISO date), but does NOT use `idx_cca_customer_day` for the date predicate (only for the `customer_id IN (...)` lookup). On small N this is fine; if the audit table grows past ~1M rows the per-customer scan inside the IN-list filter will start to bite.
- **Required fix (defer):** Add a covering index `(customer_id, substr(fired_at_ist, 1, 10))` OR store a separate `fired_date_ist` column with its own index. Not blocking — re-evaluate after 90 days of production data.

## Verification of audit checklist (all 11 items)

1. **Backward compat (flag off):** ✅ All 22 pre-Phase-3 tests pass against new code (`git stash` + re-run baseline = 22 passed; current code = 22 deselected + 22 pre-existing pass). Flag-off path is `_check_cap_and_cooldown_legacy()` which is verbatim pre-Phase-3 body. No silent behavior change on flag off.
2. **Flag-on correctness:** ✅ `customer_daily_cap()` correctly reads from `customer_call_audit`; cooldown stays local — confirmed via test_check_cap_and_cooldown_flag_on_cooldown_still_blocks. The "cooldown not pooled" choice is intentional (P1-1 ADR note recommended).
3. **`is_callable_now()` first in `check()`:** ✅ pre_call_gate.py:287 — `win = is_callable_now()` before the Redshift block. Correct on both flag states (it's outside the flag branch).
4. **`_maybe_record_audit_fire` error handling:** ✅ try/except wraps the call; `log.warning` with exception type+message; does NOT roll back FIRED state (correct — call already went out; bias toward over-suppression on next tick).
5. **Path-passing ValueError:** ✅ pre_call_gate.py:244-249 raises when flag on + path missing. But see P0-1 — the production callers don't pass it, so this raises in production on flag flip.
6. **No PII logged:** ✅ Customer_id (integer) only; no phone/PAN/name/email in INSERT params or warning logs.
7. **Concurrency test:** ✅ `test_concurrent_writers_all_land` — 10 workers, WAL mode, 10s timeout, all 10 rows land.
8. **`source` constraint:** ✅ Python assert (pre_call_audit:122) + SQL CHECK (002 migration). Belt-and-braces.
9. **No touch on `ingest.py` / `clevertap_trigger.py` from Phase 3:** ✅ `git diff main HEAD -- scripts/clevertap_trigger.py` = 0 lines. `ingest.py` diff is from commit `2b11567` (cohort_name stamping, pre-Phase-3). Phase 0a pivot honored.
10. **Nothing committed yet:** ✅ Both repos show uncommitted-only state on the new files. `vibrium-automation` is on `phase3-shared-audit-cap` branch; `vibrium-workflow` is on `main`. Sahil reviews before merge.
11. **Tests green:** ✅ `pytest workflow/tests/test_customer_call_audit.py -q` = 8 passed. `pytest tests/test_pre_call_gate.py -q` = 35 passed (22 pre-existing byte-identical + 13 new).

## Recommendations (priority order)

1. **P0-1** — wire `vibrium_db_path` through `scheduler.py:398` and `fire_immediate.py:109`. Add a `test_scheduler_flag_on_smoke.py` that runs one fake tick with flag on and asserts no `ValueError`. **Do not merge until this is fixed.**
2. **P1-1** — add the workflow-scheduler-cooldown ADR note to `phase_3_close.md` so Phase 6 doesn't pool cooldown.
3. **P1-2** — rename `batch_check_with_shared_audit` to make the cooldown-omission obvious in the call site.
4. **P2-1** — defer (revisit at 90-day-of-data mark).

## Re-audit gate

After P0-1 fix, re-run:
- `pytest tests/test_pre_call_gate.py tests/test_scheduler.py -q` — should be 35 + new smoke test all green.
- Manual: `USE_SHARED_AUDIT_CAP=true python3 -c "from scripts.scheduler import run; ..."` against a tmp vibrium.db — must complete without ValueError.

Then PASS_WITH_NOTES is achievable.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | **FAIL** — P0-1 must be fixed |
| 2 | API / backend QA (/stashfin-qa-backend) | BLOCKED on gate 1 |
| 3 | Console wiring | N/A (no UI surface) |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A |

Ship approval blocked until P0-1 is resolved and master-auditor re-run returns PASS or PASS_WITH_NOTES.


---

# Re-Audit (Phase 3 P0 fix) — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
P0 fix is correctly applied at both callsites. Full test suite (42 tests) passes byte-identical with flag off. Smoke test for flag-on path is well-constructed and asserts the right contract (no ValueError, audit-row source='adhoc' if landed). P1-1 documented in `phase_3_close.md`. P1-2 not renamed (docstring patched instead, acceptable). One drive-by note on the new smoke test.

## Verifications

**1. P0 fix correctness — CONFIRMED**
- `scripts/scheduler.py:400-402` passes `vibrium_db_path=config.get("vibrium_db_path")` to `check_cap_and_cooldown`.
- `scripts/fire_immediate.py:111-113` same pattern.
- Comments on both sides correctly note "Flag-off branch ignores this kwarg (back-compat preserved)".
- `config.get(...)` returns `None` when key absent → flag-off path safe; flag-on path raises `ValueError` (correct fail-loud, exercised by `test_check_cap_and_cooldown_flag_on_missing_db_path_raises`).

**2. Smoke test correctness — CONFIRMED**
- DDL mirrors Phase 1 migration 002 (`customer_call_audit` with CHECK(source IN ('adhoc','workflow')) + idx_cca_customer_day) — matches `/Users/sahil.m/vibrium-workflow/shared/customer_call_audit.py` writer schema.
- `monkeypatch.setenv("USE_SHARED_AUDIT_CAP", "true")` correctly scoped to test.
- `monkeypatch.syspath_prepend(workflow_root)` resolves `shared.customer_call_audit` import on flag-on path.
- Reuses `FakeStore + _seed_pending + _gate_allow` from `test_scheduler_integration` — no fixture drift.
- Asserts isinstance(res, dict), no ValueError in caplog, source='adhoc' if rows land. Audit-row presence is informational not load-bearing — defensible given shadow_mode=True path (test note explains it).

**3. No regression — CONFIRMED**
- `pytest tests/test_pre_call_gate.py tests/test_scheduler_integration.py tests/test_scheduler_flag_on_smoke.py -q` → **42 passed**, 1 unrelated urllib3/LibreSSL warning.
- Flag-off default unchanged (enforced by `test_check_cap_and_cooldown_flag_off_default_unchanged`).

**4. Flag-on smoke passes — CONFIRMED** (one of the 42).

**5. Diff scope vs main**
- Uncommitted Phase 3 set: `scripts/{pre_call_gate,scheduler,fire_immediate}.py` + `tests/test_pre_call_gate.py` + untracked `tests/test_scheduler_flag_on_smoke.py`. Matches spec.
- Branch has 14 prior commits ahead of main (cohort_name plumbing, decisions migration, etc.) that touch other files — those pre-date Phase 3 and are not introduced by this P0 fix. **Not a Phase 3 regression** but ship hygiene note: branch is carrying unrelated work; either rebase Phase 3 onto a clean cut from main, or be explicit that the merge bundles them.

**6. ingest.py / clevertap_trigger.py NOT touched by Phase 3 — CONFIRMED**
- `git log main..HEAD --oneline -- scripts/ingest.py` shows only commit `2b11567` (pre-Phase-3 cohort_name plumbing per memory `project_vibrium_automation`). No Phase 3 commit modifies either. clevertap_trigger.py untouched.

## Earlier issues — status

- **P1-1 (cooldown not pooled across systems, document the choice)** — RESOLVED. `phase_3_close.md` contains: *"Cooldown is still computed from the local today_fired_rows slice (architectural choice — pooling cooldown across systems would couple them in ways the architecture forbids)."* Adequate.
- **P1-2 (rename `batch_check_with_shared_audit` to clarify cap-only)** — NOT RENAMED. Function still named identically in `pre_call_gate.py:134`. However, docstring now states *"Cooldown is NOT enforced here — callers that need cooldown still call check_cap_and_cooldown..."* (lines 145-147). Doc-level mitigation is acceptable; flagging as **P2-CARRY** (cosmetic, low risk — only two internal callers).
- **P2-1 (index efficiency)** — DEFERRED as agreed. Status: no change.

## New findings (this re-audit)

### P2-2: smoke test asserts source='adhoc' only IF rows landed — gives a false sense of coverage when no rows land
- **File:** `tests/test_scheduler_flag_on_smoke.py:160-163`
- **Why it's weak:** Under `shadow_mode=True`, the scheduler may not call `record_fire` (depending on how shadow short-circuits the trigger path). If `rows` is empty, the `assert all(...)` is vacuously true. The test note acknowledges this but the load-bearing check is only "no ValueError." That is sufficient to close THIS P0, but a follow-up test with `shadow_mode=False` + mocked `clevertap_trigger.fire(...)` would confirm the audit-row write path under flag-on.
- **Severity:** P2 — out-of-scope-for-this-fix; track in Phase 3 follow-ups.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES ✓ — this re-audit |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no FastAPI route in Phase 3 |
| 3 | Console wiring | N/A — internal lib change, no UI surface |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

**Phase 3 P0 is genuinely closed.** Safe to merge `phase3-shared-audit-cap` once branch-hygiene call is made on the 14 unrelated commits.
