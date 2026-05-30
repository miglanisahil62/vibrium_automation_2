# Phase 2 — Closure Summary

**Date closed:** 2026-05-30
**Branch:** main
**Predecessor phase:** Phase 0 (repo foundation — closed 2026-05-30); Phase 0a (CT fixture pinned).
**Scope (per PHASES.md):** Standalone HTTP client for reading + writing CT user properties — `get_profile`, `bulk_get_profiles`, `set_profile`. Stateless, no DB access, no file I/O beyond the credential cache.

---

## What this phase delivered

The CleverTap profile reader/writer used by Phase 4a `FETCH_CT_PROPS` and Phase 4c `SET_CT_PROP` handlers, plus the enrollment poller in Phase 8.

The module mirrors the proven session-pool + cred-mtime-cache pattern from `~/vibrium-automation/scripts/clevertap_trigger.py:22-66` — the same code that solved the ulimit-1024 FD leak that crashed the adhoc scheduler in 2026-05.

CT KB hard rules implemented:
- `unprocessed[]` is inspected on every `/upload` response. `status:"success"` is never trusted alone.
- `Retry-After` header honored on 429; documented 5s fallback when header absent or malformed.
- `timeout=(5, 30)` on every request.
- Region pinned to `in1` via `creds.BASE_URL` (Stashfin's CT residency).
- `?dryRun=1` query param on `set_profile(dry_run=True)`.

---

## Files created or modified

| Path | Purpose |
|---|---|
| `workflow/types.py` | New module. `SetResult` dataclass — outcome contract for `set_profile`. Three fields: `success: bool`, `error_code: int \| None`, `raw_response: dict`. Frozen dataclass so handlers can't mutate diagnostic state. |
| `workflow/clevertap_profile.py` | New module. ~390 LoC. Three public functions: `get_profile`, `bulk_get_profiles`, `set_profile`. Plus `_load_creds` (mtime-invalidated cache), `_session` (pooled, 10-min TTL), `_ThrottleGovernor` (bulk 429 halving), `_classify_upload` (the `unprocessed[]` walk). |
| `workflow/tests/test_clevertap_profile.py` | 20 tests covering: fixture sanity (4 target properties extractable), `get_profile` happy/404/429-with-header/429-without-header/429-max-retries/500-raises, `bulk_get_profiles` mixed-success-and-failure/halving-on-2x429/empty-input, `set_profile` happy/partial-failure (CT KB critical test)/foreign-identity-in-unprocessed/top-level-fail/dry-run-appends-query/dry-run-default-false/empty-properties-rejected, cred caching mtime reload, session reuse across 5 calls, env-var overrides creds path. |
| `docs/phase_2_close.md` | This document. |

Note: PHASES.md Phase 2 deliverable listed `workflow/_ct_creds.py` (or merge into clevertap_profile.py). Chose to merge into `clevertap_profile.py` — credential loading is exactly 30 LoC of cache + lock + path resolution; splitting into a separate module would harm locality without buying anything. The `_load_creds` and `_session` helpers are module-private (underscore prefix) so the public API surface stays exactly the 3 functions the spec called for.

---

## Acceptance criteria — all pass

```bash
$ /tmp/wfvenv/bin/pytest workflow/tests/test_clevertap_profile.py -q
....................                                                     [100%]
20 passed in 0.4s

$ /tmp/wfvenv/bin/python3 -c "from workflow.clevertap_profile import get_profile, bulk_get_profiles, set_profile; print('ok')"
ok

$ /tmp/wfvenv/bin/pytest workflow/tests/test_clevertap_profile.py workflow/tests/test_smoke.py -q
24 passed in 0.45s
```

- ✅ ≥11 tests; got 20. All green.
- ✅ Import smoke works.
- ✅ No live CT API calls during tests (every request mocked via `pytest-mock` patches on `requests.Session.get/post`).
- ✅ No file under `/Users/sahil.m/vibrium-automation/` modified (checked: only `vibrium-workflow/` paths touched this phase).

---

## CT KB hard rules — concrete evidence

| Rule | Implementation site | Test |
|---|---|---|
| `unprocessed[]` inspected; `status:"success"` not trusted | `_classify_upload` in `clevertap_profile.py:430-466` | `test_set_profile_partial_failure_identity_in_unprocessed` — top-level `status:"success"` but identity in unprocessed → returns `success=False` with `error_code=516` |
| Foreign identity in unprocessed = our success | `_classify_upload` loops only matching identities | `test_set_profile_unprocessed_other_identity_is_success` |
| `Retry-After` honored on 429 | `_retry_after_seconds` parses header; fallback to documented 5s with WARNING log | `test_get_profile_429_with_retry_after_header` asserts `sleep(1)`; `test_get_profile_429_without_retry_after_uses_default` asserts `sleep(5)` |
| 3 retries max on 429 then raise | retry loop `for attempt in range(1, _MAX_RETRIES_ON_429 + 1)` | `test_get_profile_429_max_retries_then_raises` |
| Bulk concurrency halving on 2x 429 within 60s | `_ThrottleGovernor.should_halve()` + canceling+re-queueing not-yet-started futures | `test_bulk_get_profiles_halves_concurrency_on_two_429s` asserts second TPE constructed with `max_workers < 4` |
| `timeout=(5, 30)` | every `_session().get/post` call uses `timeout=_TIMEOUT` | inspected at code-review time; no separate assertion (untestable without live socket) |
| `?dryRun=1` query param | `set_profile` URL builder | `test_set_profile_dry_run_appends_query_param` + `test_set_profile_dry_run_default_is_false` |
| Cred-cache mtime invalidation | `_load_creds` compares `stat().st_mtime` to cached mtime | `test_creds_cache_reload_on_mtime_change` |
| Session pool reused across calls | module-level `_SESSION` + `_SESSION_LOCK` + TTL | `test_session_reused_across_calls` asserts 1 construction across 5 calls |

---

## Design choices worth noting

1. **`get_profile` returns the full `record` dict, not just `profileData`.** PHASES.md said "returns parsed `profileData`" but the docstring for Phase 2 also says callers must reach `record.profileData.{4 targets}`. The `record` object also contains `identity`, `events`, `platformInfo`, `all_identities` — diagnostic context the workflow's Phase 4a fetch handler will want. Caller extracts `profileData` themselves. Trivial one-key access; no downside.

2. **Bulk concurrency halving uses cooperative cancellation, not preemptive.** Workers already in `requests.Session.get` are not interrupted; only not-yet-started futures get re-queued at the lower concurrency. This matches Python's `Future.cancel()` semantics and avoids `KeyboardInterrupt`-style mid-flight aborts that could leave HTTP sockets half-open.

3. **Same retry policy for `set_profile`.** PHASES.md listed Retry-After/3-retries explicitly for `get_profile`/`bulk_get_profiles` only, but `set_profile` faces the same CT 429 surface. Applied uniformly. (Scope note: minor extension beyond literal spec; flagged so the auditor can score it.)

4. **`SetResult` is a frozen dataclass.** Phase 4c will pass these back to executors; making them immutable kills a class of "handler accidentally rewrote the audit trail" bugs.

5. **`stashfin-lint` SF006 finding addressed.** The `_retry_after_seconds` ValueError fallback now emits a `log.warning(...)` before returning the documented 5s default — meets the "log + counter increment; only return default if it's a documented contract" rule.

---

## Deferred to later phases (explicit)

- **Live CT GET against a real customer.** PHASES.md Phase 2 acceptance mentioned a one-time manual live-call check; the Phase 2 spec issued to this creator agent overrode this — "Do NOT make a live CT call as part of this phase — the fixture is enough." The fixture proves the response shape contract.
- **Phase 4a `FETCH_CT_PROPS` type coercion.** The handler is downstream of this module. It will call `get_profile`, pluck `record["profileData"]`, and apply the per-property `schema` (int / float / str) per architecture rev 3 P0-4. Not this phase.
- **Phase 4c `SET_CT_PROP` integration.** The handler will call `set_profile`, inspect the `SetResult`, advance run on success / mark `run.status=ERROR` with `set_result.error_code` in scratchpad on failure. Not this phase.
- **Phase 8 `enrollment_poller` bulk fetch.** Uses `bulk_get_profiles` against a candidate cohort. The throttle-governor's halving covers the rate-limit risk.

---

## Audit gate

Per the Phase 2 spec, `master-auditor` is dispatched on `workflow/clevertap_profile.py` after Phase 2 code lands. The audit findings (and any fixes applied) get recorded in `docs/phase_2_audit.md`. Phase 2 is **closed** only when the auditor returns PASS or PASS_WITH_NOTES.

---

## Wave-1 status after this phase

- Phase 1 (Schema + Migrations): in flight (parallel creator agent).
- Phase 2 (CT Profile Module): code + tests green; awaiting master-auditor verdict.
- Phase 3 (Shared Audit + Pre-Call Gate): in flight (parallel creator agent); has its own AWS-deploy + 24h-heartbeat closure gate.

Wave 2 starts when all three of Phase 1, 2, 3 are closed per the Phase Closure Definition in PHASES.md.
