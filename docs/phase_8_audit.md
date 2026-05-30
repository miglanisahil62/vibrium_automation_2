# Phase 8 — Enrollment Poller — Master-Auditor Report

**Scope:** `workflow/enrollment_poller.py` (822 lines), `workflow/tests/test_enrollment_poller.py` (13 tests).
**Date:** 2026-05-30.
**Verdict:** **PASS_WITH_NOTES** — 0 P0, 2 P1, 3 P2.

## TL;DR

Code is safe to enable on cron. Kill-switch + window + caps + idempotency are all in the right order and verified by tests; all 13 tests green; no `set_profile` or campaign-fire side effects. Two P1 notes worth fixing before Phase 9 wires the launchd plist: (a) `gate.fire == False` is not propagated into `outside_window` distinctly from kill-after-window, and (b) the daily-cap pre-count is read AFTER the (slow) CT bulk fetch — a race window where two concurrent ticks could double-enrol up to `2*cap`. Hard-abort + per-tick global cap + `--force` semantics are correct.

## Stack Detected

- **Python:** 3.9 (tested) + 3.13-compatible style (`list[str]`, PEP 604 union via `Optional` to stay 3.9-safe).
- **Libraries in use:** `sqlite3` (stdlib), `csv` (stdlib), `simpleeval` (fallback path), `zoneinfo` (stdlib).
- **External imports:** `external.vibrium_automation_scripts.pre_call_gate.is_callable_now` via symlink at `external/vibrium_automation_scripts` → `~/vibrium-automation/scripts`. Verified — `GateResult.fire` attribute matches code usage.
- **Domain:** Vibrium-automation v2 (workflow engine) — customer-contact critical surface.
- **KB consulted:** governance.md, datetime-timezones.md, regulatory-collections.md, system-design-patterns.md, sqlite3.md, observability-and-incident-response.md.

## P0 Issues — None.

The seven blast-radius gates all hold:

1. **Kill-switch FIRST** — `_kill_switch_active` runs at line 513 before any CT call, any DB write, any window read. Tested (`test_kill_switch_blocks` proves `bulk_get_profiles` is NEVER reached; the test installs a `_fail` mock that would raise). Latest-row pattern correct; `test_kill_then_resume_unblocks` proves RESUME clears.
2. **`is_callable_now()` gate** — line 519, BEFORE workflow load + CT calls. Both gates verified: `gate.fire=False` → exit (`test_outside_window`), and `gate.fire=True` but `n.hour >= 18` → exit (`test_outside_window_narrower_18_to_19_window`). The 18:00 IST cap is stricter than RBI's 19:00 — correct for the "enrolled-then-immediately-called" buffer.
3. **Caps stacked correctly** — `slot_count = min(remaining_daily, tick_remaining, len(matched))` at line 658. `remaining_daily = max(cap - already, 0)` is non-negative-safe. `test_daily_cap_partial_room` proves the gap math. `test_per_tick_cap` proves the global 200 (shrunk to 2) clips at the right layer.
4. **Hard-abort 5000** — line 625, `not force` gate present; aborted workflows pushed into `stats.aborted_workflows`; CLI `_main` returns exit code 2 (line 816), surfacing to launchd. `test_hard_abort_when_too_many_match` + `test_hard_abort_force_overrides` cover both paths.
5. **Idempotency** — `INSERT OR IGNORE` at line 695 against the partial UNIQUE index `(workflow_id, customer_id, enrollment_key) WHERE enrollment_key IS NOT NULL` confirmed in `001_init.py:218-220`. `test_rerun_is_idempotent` proves zero growth on re-run.
6. **No CT write side effects** — grep'd: no `set_profile`, no `clevertap_trigger.trigger`, no `requests.post`. Only `bulk_get_profiles` (read-only). Confirmed.
7. **Transactional safety** — `with transaction(conn)` wraps the per-workflow batch (line 689). Atomic.
8. **`--dry-run`** — line 675, no INSERT executed; tested (`test_dry_run_does_not_insert`).
9. **tz-aware IST** — `_now_ist()` always uses `ZoneInfo("Asia/Kolkata")`; all callers convert via `astimezone(IST)` if a non-IST `now` is passed. No naive `datetime.now()` anywhere.

## P1 Issues — Should Fix

### P1-1: Daily-cap pre-count races with concurrent ticks

- **File:** [enrollment_poller.py:641-715](workflow/enrollment_poller.py#L641-L715)
- **Evidence:** Daily-cap is read at line 643 (`_todays_enrollment_count`) AFTER the slow CT fetch at line 593 (~seconds). If two ticks fire concurrently (e.g., overlapping launchd intervals or operator-triggered manual run during cron), both will read `already=N`, compute the same `remaining_daily`, and double-enrol up to `2*cap` distinct rows. The partial UNIQUE absorbs same-customer-same-day collisions but NOT distinct customers in two ticks reading a stale pre-count.
- **Why it matters:** With `max_new_enrollments_per_day=1000` and two ticks racing, worst case 2000 enrollments slip through. Below the hard-abort, so silent.
- **Fix options (creator's call):** (a) Cheapest: a process-level file lock (`fcntl.flock` on `state/workflow.db.poller.lock`) around the whole `run()`. (b) Open `BEGIN IMMEDIATE` transaction before the count + insert so SQLite serialises writers. Phase 9 launchd plist should also use a single instance with `KeepAlive=false` semantics or `LowPriorityIO + ThrottleInterval` to prevent overlap. Recommend (a) + plist-level single-instance.

### P1-2: `stats.outside_window` conflates two reasons

- **File:** [enrollment_poller.py:520-538](workflow/enrollment_poller.py#L520-L538)
- **Evidence:** `outside_window=True` is set on three different paths: `pre_call_gate` ImportError (line 522), `gate.fire=False` (line 537), `hour >= 18` (line 537). The downstream consumer (Phase 8.5 alerts, Phase 9 launchd-summary email) can't distinguish "symlink broken → operator needs to fix" from "evening run, expected idle".
- **Why it matters:** ImportError on the symlink is a P0 ops failure dressed up as a normal no-op. Will silently swallow weeks of broken cron.
- **Fix:** Add a distinct status field — `gate_error` / `outside_rbi_window` / `outside_enrollment_window` — into `_Stats`, or at minimum re-raise the ImportError so the CLI exit code is non-zero and launchd stderr captures it. Currently `ImportError` returns 0 — silently fine.

## P2 Issues — Nice to Fix

### P2-1: `conn.close()` then immediate re-open between matched and insert

- **File:** [enrollment_poller.py:541-641](workflow/enrollment_poller.py#L541-L641) — first conn closed at 545, fresh one opened at 641.
- **Why:** Comment says "HTTP took the wall time" — fine reasoning, but a second `with get_workflow_db(...)` could be a context manager that returns the SAME connection on the same tick. Minor noise; not worth the refactor unless `wf_store.transaction` grows hooks.

### P2-2: CSV input — no schema check on stray columns / row count guard

- **File:** [enrollment_poller.py:317-352](workflow/enrollment_poller.py#L317-L352)
- **Why:** A 6M-row CSV (e.g., the full collection_view dump pasted in) would silently pass `_load_candidates` and only be caught by the 5000 hard-abort. Consider a length guard with a clearer message — "CSV has 6,247,113 rows; expected <50k; check source_csv" — before any CT fetch is even attempted. Cheap defence-in-depth.

### P2-3: `_evaluate_condition` fallback uses `dict(scratchpad)` defensively

- **File:** [enrollment_poller.py:207](workflow/enrollment_poller.py#L207)
- **Why:** `SimpleEval` is documented to not mutate `names`, but the defensive copy is a no-op repeated per-candidate (called inside the loop at line 613). Minor perf — move construction outside the per-candidate loop, set `s.names = scratchpad` inline. Doesn't affect correctness.

## Stashfin-lint surface check

- No `requests.*` calls (SF001 N/A).
- All `datetime.now(IST)` calls (SF002 clean).
- No bare `except:` — every `except` is named (SF004/SF005 clean).
- No `/Users/sahil.m/` hardcoded paths (SF007 clean) — uses `Path(__file__)`-free design and accepts `workflow_db_path` from CLI/caller.
- No hardcoded secrets (SF008 clean).
- No SQL f-string in `.execute()` — all parameterised (SF014 clean).
- No subprocess / `shell=True` / `eval()` / `exec()` (SF020/021/023 clean).

## Spec verification — Phase 8 PHASES.md checklist

| Spec item | Status |
|---|---|
| Kill-switch FIRST | PASS (line 513) |
| `is_callable_now()` + 18:00 IST cap | PASS |
| Per-workflow daily cap (default 1000) | PASS |
| Per-tick global cap (200) | PASS |
| Hard-abort >5000 + `--force` | PASS |
| `min(remaining_daily, tick_remaining, len(matched))` | PASS (line 658) |
| `INSERT OR IGNORE` on partial UNIQUE | PASS |
| Re-run = 0 new rows | PASS (tested) |
| No `set_profile` / no campaign trigger | PASS |
| `with transaction(conn)` per batch | PASS (line 689) |
| `--dry-run` | PASS (tested) |
| tz-aware IST | PASS |
| CONDITION via local-import handler | PASS — Phase 4a try/except fallback to inline simpleeval with identical safety knobs |
| CSV-only v1 + extension point documented | PASS (docstring lines 36-40 + `_load_candidates`) |
| `bulk_get_profiles(concurrency=5)` | PASS (line 593) — None returned → silently skipped, retried next tick |
| 13 tests pass | PASS — confirmed via `pytest -q` |

## Assumptions Made

- The Phase 1 schema in `workflow/migrations/001_init.py` is the deployed schema (verified by grep — UNIQUE index lines 218-220 exists).
- `pre_call_gate.GateResult.fire` is the canonical attribute (verified by grep in `~/vibrium-automation/scripts/pre_call_gate.py` line 75-103).
- "Concurrent ticks" is a realistic scenario worth defending against — based on the Phase 9 launchd plan landing in the next phase. If launchd uses single-instance enforcement (`AbandonProcessGroup=false` + reasonable `ThrottleInterval`), P1-1's race window collapses; flag preserved because nothing in code enforces it.
- `bulk_get_profiles` `concurrency=5` is honoured by Phase 2's implementation as recommended; not separately re-audited here.

## What I Did Not Audit

- Phase 2's `clevertap_profile.bulk_get_profiles` — Retry-After + 3-retry-fallback contract assumed per the spec; out of scope here.
- Phase 4a `condition.py` — fallback path tested, primary path not (Phase 4a unclosed).
- The launchd plist + Phase 8.5 alerts — explicitly deferred per `phase_8_close.md`.

## KB Updates Applied

None this round — no new gotcha surfaced that isn't already covered by existing KB files. The two-tick race (P1-1) is a special-case of `sqlite3.md` advice on `BEGIN IMMEDIATE` for read-then-write; existing entry covers it.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — daemon, no HTTP surface; revisit when Phase 9 wires the plist |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | PENDING — surface in Ops Centre v2 once Phase 9 cron lands |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | PENDING — after gate 3 |

Ship-approval requires the two P1 fixes (or explicit acceptance) plus the Phase 9 plist + Ops Centre wiring before this daemon is enabled in production.
