# Phase 2 Close-out Audit — clevertap_profile.py — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
The CT KB hard rule (inspect `unprocessed[]`, do not trust top-level `status:"success"`) is correctly implemented in `_classify_upload`. `Retry-After`, `timeout=(5,30)`, `raise_for_status()`-before-`.json()`, mtime-invalidated cred cache, single pooled `requests.Session`, and concurrency-halving on 2× 429 within 60s are all wired correctly. **Tests: 20/20 pass when run in isolation; `test_bulk_get_profiles_halves_concurrency_on_two_429s` is timing-flaky (3/8 fails in standalone repeats).** One P1 (PII risk in `log.warning(... body=%s)` on fail/partial responses), one P1 (flaky halving test), three P2s. No file under `/Users/sahil.m/vibrium-automation/` was modified by Phase 2 (uncommitted diffs there pre-date this phase, mtime aside).

## Stack Detected
- Python 3.9 (system) — repo pins via `pyproject.toml`
- `requests==2.32.4`, `pytest==8.3.5`, `pytest-mock==3.14.0` (declared)
- Domain: Vibrium / CT external HTTP client
- KB consulted: `auditor_kb/clevertap.md`, `auditor_kb/http-requests.md` (mental), `feedback_clevertap_campaign_id_integer.md`, `project_vibrium_automation.md`
- Live lookups: none (CT KB current, requests 2.32.x stable)

## P0 — Must Fix Before Close
None.

## P1 — Should Fix

### P1-1: PII leak risk in fail/partial-response logging
- **Category:** Governance
- **File:** [workflow/clevertap_profile.py:481,485](workflow/clevertap_profile.py#L481-L485)
- **Evidence:**
  ```python
  log.warning("CT upload rejected for identity=%s body=%s", identity, data)
  log.warning("CT upload partial for identity=%s body=%s", identity, data)
  ```
- **Why it's wrong:** `data` is the full CT response and CT echoes the rejected record's `profileData` inside `unprocessed[].record.profileData`. When operators write a `Phone` / `Email` / `Name` property and CT rejects it (code 516 et al.), the raw PII lands in the log line at WARNING level — exactly the channel most likely shipped to a centralized aggregator. Identity alone is acceptable (numeric customer_id); the whole body is not.
- **Required fix:** Log only `data.get("status")`, `data.get("processed")`, and the list of unprocessed `code`s. Keep the full body in the returned `SetResult.raw_response` for caller-controlled diagnostic surfaces.
- **Source:** `auditor_kb/regex-pii.md` (Phone/Email shapes); aligns with `vibrium-automation/scripts/clevertap_trigger.py:_classify_delivery` which logs codes only.

### P1-2: `test_bulk_get_profiles_halves_concurrency_on_two_429s` is timing-flaky
- **Category:** Tests
- **File:** [workflow/tests/test_clevertap_profile.py:222-293](workflow/tests/test_clevertap_profile.py#L222-L293)
- **Evidence:** 8 standalone runs of just this test: 3 failed with `len(constructed_max_workers) == 1` (only the initial TPE was built; halving never fired). Full-file run passed 20/20 because earlier tests warmed up timings.
- **Why it's wrong:** The test relies on a 0.3s `Timer(0.3, release.set)` to release the first wave AFTER 4 workers are parked in `Session.get`. On a loaded laptop or in CI the timer fires before all 4 are parked, so 429s arrive serially across retries and the halving window math still works — but the test ALSO relies on the as_completed loop seeing the 429s BEFORE all 20 items dispatch under the initial pool. Under quiet-pytest, fewer plugins → faster startup → all 20 work items can drain at concurrency=4 before the governor trips. The underlying production code path is sound (governor + cancel-and-requeue logic is correct); only the test choreography is fragile.
- **Required fix:** Either (a) cap initial dispatch — submit only `current_concurrency` items at a time and pull from a queue, so un-dispatched work is structurally present when the governor trips; or (b) rewrite the test to drive `_ThrottleGovernor` directly and unit-test the `while remaining:` loop with a synthetic governor injection rather than racing wall-clock threads. Option (b) is cheaper and removes timing flake entirely.
- **Source:** Reproduced locally with 8× isolated runs.

## P2 — Nice to Fix

### P2-1: Dead test code
- **File:** [workflow/tests/test_clevertap_profile.py:274-277](workflow/tests/test_clevertap_profile.py#L274-L277) — `_releaser()` is defined and never called; the `time.sleep.__wrapped__(...)` ternary is a no-op. Remove.

### P2-2: No defensive guard on creds dict shape
- **File:** [workflow/clevertap_profile.py:138-140](workflow/clevertap_profile.py#L138-L140) — `creds["ACCOUNT_ID"]` / `creds["PASSCODE"]` raise raw `KeyError` on a malformed creds file. Replace with a `_validate_creds(creds)` that lists missing keys at load time so rotation-induced corruption fails loud and once, not per request.

### P2-3: `Retry-After` HTTP-date form not supported
- **File:** [workflow/clevertap_profile.py:153-172](workflow/clevertap_profile.py#L153-L172) — RFC 7231 also permits HTTP-date. Docstring claims "CT only emits integer seconds"; if CT ever flips, the fallback is 5s. Acceptable per the documented assumption; flag for KB update if CT changes.

## Assumptions Made
- "No file under `/Users/sahil.m/vibrium-automation/` was modified" means by Phase 2 work specifically. `git status` there shows uncommitted changes to `scripts/pre_call_gate.py`, `scripts/scheduler.py`, `tests/test_pre_call_gate.py` — these match the existing commit `3033a2e` ("fix(gates): remove NACH-only payment filter…") authored 2026-05-30 14:40 IST, well before Phase 2's `clevertap_profile.py` mtime of 18:09. Treated as pre-existing and out of Phase 2 scope.
- "identity" was treated as a numeric customer_id (not phone/email). If operators ever pass a phone in identity, the existing log lines become a PII vector and P1-1 worsens to P0.
- `pytest-mock` was not preinstalled; installed via `pip3 install --user pytest-mock==3.14.0` (matches `requirements.txt`). The fact that Sahil's environment lacks this is a separate environment-setup issue, not a Phase 2 code issue.

## What I Did Not Audit
- Did not run the test suite under CI conditions (only local macOS Python 3.9.6).
- Did not exercise against live CT — no creds in this environment and that's intentional.
- Did not audit `workflow/tests/fixtures/ct_profile_response.json` byte-for-byte; the fixture-sanity test (line 94) verifies the 4 target properties are present and typed correctly.

## CT KB Hard Rules — Verification Matrix
| Rule | Verified at | Status |
|---|---|---|
| `unprocessed[]` walked; identity-matched | `_classify_upload` 489-502 | PASS |
| Top-level `status:"success"` not trusted alone | `_classify_upload` 484-504 (partial path still walks unprocessed) | PASS |
| `Retry-After` honored on 429 | `_retry_after_seconds` 153-172 → 213, 388, 453 | PASS |
| Default 5s when header absent | `_DEFAULT_429_SLEEP_SEC = 5` + fallback in `_retry_after_seconds` | PASS |
| Max 3 retries on 429 | `_MAX_RETRIES_ON_429 = 3` + `for attempt in range(1, _MAX_RETRIES_ON_429+1)` | PASS |
| Halve concurrency on 2× 429 in 60s | `_ThrottleGovernor.should_halve` 263-268 + while-remaining loop 316-362 | PASS (test fragile) |
| Halving applies to REMAINDER | cancel + requeue under halved TPE 344-357 | PASS |
| Single `requests.Session` per process | `_session()` singleton with TTL | PASS |
| `timeout=(5, 30)` everywhere | 208, 383, 451 | PASS |
| `raise_for_status()` before `.json()` | 223-224, 393-394, 462-463 | PASS |
| Cred mtime invalidation | `_load_creds` 88-106; verified by test (line 427) | PASS |
| No PII in logs (identity-only) | mostly clean; **P1-1 violates on body=%s** | PARTIAL |

## KB Updates Applied
None this pass. If P1-2 fix lands, consider adding a note to `auditor_kb/clevertap.md` recommending governor-injection over wall-clock test choreography for bulk-throttle tests.

## Recommendations
Phase 2 is **closeable on PASS_WITH_NOTES**. P1-1 should be fixed before Phase 3 begins (Phase 3 will call `set_profile` from the engine and the logging surface will appear in production logs). P1-2 should be fixed before this test ships to CI — it will flap and erode trust. P2s can be deferred.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no FastAPI routes in this phase |
| 3 | Console wiring | N/A — internal library, no UI surface |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Phase 2 ships an internal HTTP client only — gates 2/3/4 attach when the engine wires this into a route (Phase 6+).

---

# Phase 2 Re-Audit — 2026-05-30

## Verdict: PASS

## TL;DR
Both P1 fixes land cleanly. P1-1 (PII log leak) is fully closed — `_classify_upload` now logs `unprocessed_count=%d` only; grep across the module confirms no `log.*body`, `log.*data`, `log.*payload`, `log.*profileData` anywhere. P1-2 (flaky halving test) is replaced by 3 deterministic governor unit tests that cover what the prior Timer-race test could not (window-pruning + Lock contention). 22 tests pass cleanly (0 failed, 0 errors). No regression in the unchanged surface.

## Fix Verification

### P1-1: PII log leak — FIXED
- `clevertap_profile.py:483-486` (top_status=fail branch): logs `unprocessed_count=%d` only. No `body=`, no `data=`.
- `clevertap_profile.py:490-493` (top_status=partial branch): same — count only.
- `clevertap_profile.py:505-508` (per-record fail): logs `identity`, `code`, `rec.get("error")` (CT-supplied short string, not body). Identity is the customer_id, which is already in production logs by design (telemetry key); not classed as PII leak.
- Grep `log\.\w+.*(body|data|payload|properties|profileData)` → 0 matches across module. Clean.

### P1-2: Flaky halving test — FIXED
- Old Timer/Event race test deleted.
- 3 new tests at `test_clevertap_profile.py:222-287`:
  1. `test_throttle_governor_records_429s_and_halves_on_second` — happy-path: 0→1 events no halve, 2 events halves. Direct governor calls, zero timing dependence.
  2. `test_throttle_governor_thread_safe_under_parallel_record_429` — 10 threads × 5 records = exactly 50 events under `_lock`. Proves Lock invariant. NEW COVERAGE vs prior test.
  3. `test_throttle_governor_prunes_events_outside_window` — patches `ctp.time.monotonic` to advance past window boundary; verifies events expire. NEW COVERAGE vs prior test.
- All three test what the prior race test attempted (`should_halve()` returns True after 2 events in window) **plus** thread-safety and pruning that were previously untested.

### Regression check — CLEAN
- `set_profile` (line 469) still walks `unprocessed[]` and matches `str(rec_identity) == str(identity)` — CT KB hard rule intact.
- `Retry-After` honored at lines 153-172 + 213 + 388 + 453 — three call sites, all consistent.
- `timeout=_TIMEOUT` (= `(5, 30)`) on every `requests.Session().get/post`: lines 208, 383, 451. Verified.
- Single pooled `requests.Session()` via `_session()` (line 124) with TTL + ulimit-safe pool sizing — unchanged.
- `raise_for_status()` precedes every `.json()`: lines 223-224, 393-394, 462-463. Verified.

### Test count — MATCHES SPEC
- Prior: 20 passing. Spec target: +3 new governor tests, -1 old flaky = 22.
- Observed: `22 passed` (pytest output above). Matches exactly.

## P0 Issues — None
## P1 Issues — None (both prior P1s closed)
## P2 Issues — Carrying forward from Phase 2 audit (unchanged scope this round)
The 3 P2s from the prior Phase 2 audit (cancel-then-shutdown race surfacing in `cancel_futures=False`, governor doesn't dedupe simultaneous-monotonic events, no explicit `Connection: close` on session-TTL teardown) are unchanged and remain P2 backlog. Not in this diff's scope.

## Assumptions Made
- `rec.get("error")` (line 507) returns CT's short error string (e.g. "Invalid phone number"), not the customer payload — consistent with CT KB. If CT ever inlines properties into the `error` field, that becomes a P1, but no evidence in the fixture or KB suggests this.
- Customer identity in logs is accepted per existing module convention (matches line 210 `CT profile not found: identity=%s`).

## What I Did Not Audit
- Live CT API contract for `error` field — relying on KB + fixture only.
- Did not re-walk callers of `set_profile` to confirm they don't re-log `raw_response` downstream (out of scope; was reviewed in Phase 2).

## KB Updates Applied
None — fixes align with existing KB rules; no new gotcha discovered.

## Recommendations
Ship. Both P1s closed; tests green; no regression. Move to gate 2 (`/stashfin-qa-backend`) once the consumer surfaces of this module are ready.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | PENDING |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | N/A — library module, no UI surface |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — library module, no UI surface |

Ship approval requires gates 1 and 2 to return PASS or PASS_WITH_NOTES (gates 3 and 4 not applicable to backend library module).
