# Phase 6 Audit — workflow_scheduler.py

**Auditor:** master-auditor (close-out review)
**Date:** 2026-05-30
**Verdict:** PASS_WITH_NOTES
**Scope:**
- `/Users/sahil.m/vibrium-workflow/workflow/workflow_scheduler.py`
- `/Users/sahil.m/vibrium-workflow/workflow/tests/test_workflow_scheduler.py`

## TL;DR
Phase 6 close-out PASSES. 0 P0, 0 P1, 3 P2. The load-bearing Phase 0a invariant
(no `tag_group` reaching `trigger()`) is enforced at *two* layers: (a) the
scheduler never names the kwarg, (b) `clevertap_trigger.trigger()` uses
keyword-only parameters with NO `**kwargs`, so a stray kwarg would raise
`TypeError` — defense-in-depth, not just convention. All 11 Phase 6 tests pass;
full suite is 171/171 green (creator claimed 149+; current is higher).

## Stack Detected
- **Python:** 3.11+ (per pyproject pin)
- **Libraries:** stdlib-only at runtime (sqlite3, argparse, zoneinfo); pytest 8.3.5 for tests
- **Domain:** Vibrium / workflow engine (critical-surface — real CT fires + shared audit writes)
- **KB consulted:** system-design-patterns, clevertap, sqlite3, datetime-timezones, observability-and-incident-response
- **Live lookups:** none required

## Load-bearing claim verifications

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | NO `tag_group` kwarg passed to trigger() | PASS | `grep tag_group workflow_scheduler.py` returns only docstring + comment mentions; trigger call at L532–540 has no such kwarg. `clevertap_trigger.trigger()` signature (`scripts/clevertap_trigger.py:84-88`) is keyword-only with NO `**kwargs` — TypeError safety net. Test L236–240 asserts on every `call_args_list`. |
| 2 | Race-safety of FIRING_IN_PROGRESS claim | PASS | `_claim_row` (L245–252): atomic `UPDATE … WHERE id=? AND status='PENDING'` + rowcount=1 winner. Test 8 (`test_claim_row_race_safety`) covers two-conn race. |
| 3 | Kill-switch FIRST | PASS | `run()` L390: first DB read is `_kill_switch_active(conn)`; KILL → return with `status='killed'`, no CT, no audit. Test 7 covers. |
| 4 | `is_callable_now()` SECOND | PASS | L397–406: outside-window → no-op + heartbeat `outside_window`. Test 3 covers. |
| 5 | `pre_call_gate.check()` per row with `source='workflow'` | PARTIAL — see P2-1 | `_gate_check` calls `gate_check_fn(int(customer_id))` (L226) with positional arg only; `source='workflow'` is NOT passed. The composed gate (cap + cooldown + redshift) is correct, but if `pre_call_gate.check` requires `source=` (Phase 3 contract), this would break in live. |
| 6 | Live CT call WITHOUT tag_group | PASS | L532–540, verified by source grep + signature inspection + test 1 assertion. |
| 7 | shadow_mode → SHADOW_FIRED + fired_at_ist, no CT, no audit | PASS | L511–525; test 5 covers including `fired_at_ist is not None`. |
| 8 | dry_run → log + release_pending, no DB terminal-state writes | PASS | L496–508; row released back to PENDING via `_release_pending`. Test 6 covers. |
| 9 | 3h cooldown via shared lib (not duplicated) | PASS-ish — see P2-2 | Cooldown is computed inline in `_gate_check` against `cca.batch_last_fire_at`. Threshold constant `COOLDOWN_HOURS=3` is local; shared lib only supplies the last-fire timestamp. Functionally correct; semantically the rule lives in two repos now. |
| 10 | CLI: --workflow-db, --vibrium-db, --ct-creds, --shadow, --dry-run, --batch-limit, plus live-mode guard exit 2 | PASS | L646–681; exit 2 enforced when `not shadow and not dry_run and not ct_creds`. Test 11 (smoke) covers dry-run path. |
| 11 | Cross-cutting (tz-aware IST, no PII, parameterized SQL, no writes to vibrium-automation/) | PASS | `_now_ist_str` uses `datetime.now(IST)`; all SQL is `?`-bound (5 UPDATEs, 2 INSERTs, 1 SELECT); audit write delegated to shared lib; nothing in scheduler writes under `~/vibrium-automation/`. Logged customer_ids — flagged as P2 PII consideration. |
| 12 | Tests: 11 pass; full suite green | PASS | `pytest workflow/tests/test_workflow_scheduler.py -q` → 11 passed. Full `workflow/tests/` → **171 passed** (exceeds creator's 149+ claim). |

## P0 — None.

## P1 — None.

## P2 Issues

### P2-1: `pre_call_gate.check` called without `source='workflow'`
- **File:** `workflow_scheduler.py:226`
- **Evidence:** `result = gate_check_fn(int(customer_id))` — positional `customer_id` only.
- **Why it matters:** Phase 3 refactor introduced source tagging to disambiguate adhoc vs workflow at the gate. If `pre_call_gate.check` in the deployed version has a required `source` kwarg, live runs raise TypeError. If optional, downstream observability loses the source attribution. The injection seam (`gate_check_fn`) accepts whatever signature tests supply, so the gap is invisible until live wire-up.
- **Required fix:** Either (a) pass `source='workflow', vibrium_db_path=...` explicitly into `gate_check_fn(...)`, or (b) document at the injection-seam site that production `pre_call_gate.check` is being called with positional `customer_id` only and confirm via grep that the deployed signature accepts that. **Uncertain — needs verification against the deployed `pre_call_gate.check` signature on AWS.**

### P2-2: `COOLDOWN_HOURS=3` duplicated across repos
- **File:** `workflow_scheduler.py:83`
- **Evidence:** Module constant local to the workflow scheduler.
- **Why it matters:** The cooldown rule now lives in two places (adhoc `pre_call_gate` + workflow scheduler). If one drifts, time-bound triangulation in Phase 7 becomes ambiguous. Low risk because both anchor to "3h" in PHASES.md.
- **Required fix:** Pull the constant from `shared.customer_call_audit` (or a new `shared.constants` module) in a follow-up. Not blocking Phase 6 close.

### P2-3: customer_id logged in INFO log lines
- **File:** `workflow_scheduler.py:488–492, 519–524, 588–593, 611–616` etc.
- **Evidence:** `log.info("workflow_scheduler: FIRED cid=%s ...", cid, ...)` — customer_id printed at INFO.
- **Why it matters:** customer_id alone is not PII under Stashfin's governance posture (no phone / PAN / name), but log volume on a per-row basis at INFO is high. Consider DEBUG for per-row outcomes, INFO for tick aggregates only. Not a hard violation.
- **Required fix:** None now; revisit during Phase 9 log-volume tuning.

## Cross-cutting checks
- **Phase 0a invariant:** double-enforced (caller convention + callee signature). Strong.
- **No file under `/Users/sahil.m/vibrium-automation/` modified.** Confirmed via `git status` and `git log`.
- **Heartbeat:** emitted on every exit path (killed, outside_window, no_pending_rows, tick_complete). Pipeline-integrity-auditor will see no gaps.
- **Transaction discipline:** every state mutation calls `conn.commit()`; audit write delegated to shared lib (which uses `with conn:` context manager).

## Verdict: PASS_WITH_NOTES
Ready for Phase 6 close. The P2-1 source-kwarg gap should be confirmed against the deployed `pre_call_gate.check` signature on AWS before the first live workflow run (Phase 12 shadow + Phase 13 cutover), but it does not block Wave 3 closure.

## Release gate
| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES ✓ — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — scheduler has no HTTP surface |
| 3 | Console wiring | N/A — daemon process, no UI page |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI |

Ship approval: cleared from auditor side for Phase 6 close. Live-fire activation gated by Phase 12 shadow PASS + Sahil "go".
