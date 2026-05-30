# Phase 3 Closure — Shared Audit + Pre-Call Gate Refactor

**Status:** Code complete; awaiting cross-repo deploy + 24h heartbeat-green window before full closure per `PHASES.md` §"Phase Closure Definition (applies to ALL phases)" point 3.

**Branches:**
- `vibrium-workflow` — committed on `main`.
- `vibrium-automation` — committed on **`phase3-shared-audit-cap`** (NOT merged to main; Sahil reviews + merges).

---

## What was built

### `vibrium-workflow`

| File | Purpose |
|---|---|
| `shared/customer_call_audit.py` | Shared library: `record_fire`, `batch_count_today`, `batch_last_fire_at`, `customer_daily_cap` over `customer_call_audit` in `vibrium.db`. Stdlib `sqlite3` only. WAL-aware. IST-naive timestamps (`YYYY-MM-DD HH:MM:SS`). |
| `workflow/tests/test_customer_call_audit.py` | 8 tests: insert + format, cap math, source validation, concurrent writers (WAL test), `batch_last_fire_at` MAX semantics, empty-input guards. |

### `vibrium-automation` (branch `phase3-shared-audit-cap`)

| File | Change |
|---|---|
| `scripts/pre_call_gate.py` | Added `is_callable_now()` (always-on RBI 08:00–19:00 IST gate). Added `customer_daily_cap()` + `batch_check_with_shared_audit()` (flag-on convenience wrappers around `shared.customer_call_audit`). Reworked `check()` to call `is_callable_now()` FIRST. Preserved `check_cap_and_cooldown(customer_id, today_fired_rows)` signature byte-for-byte; added optional kw-only `vibrium_db_path` used only on the flag-on path. Legacy body extracted to `_check_cap_and_cooldown_legacy` and is the default when `USE_SHARED_AUDIT_CAP=false`. |
| `scripts/scheduler.py` | Added `_maybe_record_audit_fire()` helper (lazy-imports `shared.customer_call_audit`; no-op when flag off). One new call site after every successful CT trigger. Audit failures log a warning but never kill a tick. |
| `tests/test_pre_call_gate.py` | 13 new tests: 5 for `is_callable_now`, 2 for `customer_daily_cap`, 1 for `batch_check_with_shared_audit`, 4 for flag-on `check_cap_and_cooldown` (including the missing-db-path ValueError and the still-blocks-on-cooldown path), 1 for byte-identical-when-off default, 1 batched-hot-path runtime regression (flag-on must be within 20% of flag-off). |

### What was explicitly NOT touched (per Phase 0a pivot)

- `scripts/ingest.py` — no `tag_group NOT LIKE 'vbwf:%'` filter. `tag_group` column does not exist in `collection_comment_data` (Phase 0a finding).
- `scripts/clevertap_trigger.py` — no `tag_group` kwarg added. The disposition-wakeup join is now `(customer_id, time-bound)` triangulation; no payload tagging needed.

---

## Feature flag

**Env var:** `USE_SHARED_AUDIT_CAP`
**Default:** `false` (read every call; flip is live without restart).
**Truthy values:** `true`, `1`, `yes`, `on` (case-insensitive). Anything else (including unset) is false.

When **off (default):**
- `is_callable_now()` still fires on every `check()` invocation. RBI window enforcement is unconditional.
- `check_cap_and_cooldown()` body is byte-identical to pre-Phase-3 — reads cap + cooldown from the supplied `today_fired_rows` slice.
- `scheduler.py` does NOT call `record_fire()`.

When **on:**
- `check_cap_and_cooldown()` reads the daily cap from `customer_call_audit` in `vibrium.db` (single source of truth across adhoc + workflow). Cooldown is still computed from the local `today_fired_rows` slice (architectural choice — pooling cooldown across systems would couple them in ways the architecture forbids).
- `scheduler.py` inserts one `customer_call_audit` row after every successful CT trigger.
- `config.json.vibrium_db_path` becomes required. Missing → `ValueError` (loud failure surfaces config bugs; safer than silent fallback).

---

## Tests — green in both repos

- `vibrium-workflow`: `pytest workflow/tests/test_customer_call_audit.py` → **8/8 pass**.
- `vibrium-automation`: `pytest tests/test_pre_call_gate.py` → **35/35 pass** (22 pre-existing + 13 new).
- `vibrium-automation` full suite (flag off): 219/220 pass. The single failure (`test_clevertap_trigger.py::test_classify_unprocessed_with_other_cid_still_failed_generic`) is **pre-existing on the base branch `feature/agent-redesign`** — verified by `git stash` before running Phase 3 diff and confirming the same failure on the unchanged tree. Not caused by Phase 3.

---

## Rollback procedure

1. **Mid-day soft revert (preferred):** set `USE_SHARED_AUDIT_CAP=false` (or unset) in the cron environment / launchd plist env block → restart the scheduler cron job → next tick reads the flag fresh and reverts to the pre-Phase-3 code path. Adhoc behaviour is byte-identical to before deploy. No DB changes needed; the audit table can stay populated (it's append-only, harmless when unread).
2. **Hard revert (only if soft revert is insufficient):** `git revert` the merge of `phase3-shared-audit-cap` into `main` on `vibrium-automation`. Push, deploy. The `customer_call_audit` table on `vibrium.db` remains (additive only — no destructive migration).

The byte-identical-when-off contract is the rollback's safety net. It is enforced by the test `test_check_cap_and_cooldown_flag_off_default_unchanged` and the full pre-Phase-3 test suite (22 tests) still passing.

---

## Sahil's checklist to close Phase 3

1. Review the diff on `phase3-shared-audit-cap` (3 files, ~547 lines added).
2. Master-auditor verdict: see `docs/phase_3_audit.md`.
3. Merge `phase3-shared-audit-cap` → `main` on `vibrium-automation`.
4. Push, pull on AWS, restart relevant launchd jobs (do NOT set `USE_SHARED_AUDIT_CAP=true` yet).
5. Confirm 24h heartbeat green with flag still **off** (zero behaviour change).
6. Set `USE_SHARED_AUDIT_CAP=true` in the cron env + restart scheduler.
7. Watch for the first `customer_call_audit` rows to appear within 5 min of the next fire.
8. Heartbeat green for 24h with flag on → Phase 3 fully closed → Wave 2 starts.
9. Rollback rehearsal: flip back to `false` mid-day, confirm no behaviour drift, flip back on.
