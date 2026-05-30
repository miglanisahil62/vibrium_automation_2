# Phase 8.5 — Observability + Alerts — master-auditor report

**Date:** 2026-05-30
**Scope:** `workflow/alerts.py`, `workflow/tests/test_alerts.py`, `docs/runbook.md`, `docs/phase_8_5_close.md`

## Verdict: PASS_WITH_NOTES

## TL;DR
All 10 tests pass; CLI smoke exits 0; no `smtplib` import anywhere (AST-verified — `alerts.py` only imports stdlib: `argparse, json, logging, sqlite3, sys, dataclasses, datetime, pathlib, typing, zoneinfo`). 5 detectors map cleanly to conditions A–E; cooldown via `alert_state` is idempotent; `--dry-run` correctly does not persist. Two P2 doc-quality items + one P2 daemon-name-contract item. Ship.

## Verification matrix

| Check | Result |
|---|---|
| 1. No real SMTP send / no `smtplib` import | PASS (AST scan) |
| 2. 5 detectors, one function each, named `detect_{a,b,c,d,e}_*` | PASS |
| 3. Cooldown via `alert_state` table, idempotent `CREATE TABLE IF NOT EXISTS`, keyed by condition, 60-min cutoff | PASS (alerts.py:159, 172, 497) |
| 4. `--dry-run`: payload generated, `alert_state` NOT updated | PASS (alerts.py:528, test_dry_run_does_not_persist_cooldown) |
| 5. tz-aware IST (`datetime.now(IST)`); no naive `datetime.now()` | PASS (alerts.py:108) |
| 6. `_parse_ist` failures `log.warning` + counter (not silent) | PASS (alerts.py:151–155; surfaced in stats as `parse_failures`) |
| 7. `python3 -m workflow.alerts --workflow-db /tmp/empty.db --dry-run` exits 0 | PASS (`emitted=0 skipped_cooldown=0 exit=0`) |
| 8. Daemon names documented for Phase 9 to stamp | PASS — see "Daemon name contract" below |
| 9. Runbook has actionable recovery steps per alert | PASS (sections A–E each have Diagnose + numbered Recover) |
| 10. No files under `vibrium-automation/` or `ops_console_v2/` touched in this phase | PASS (changeset confined to `vibrium-workflow/`) |
| `pytest workflow/tests/test_alerts.py -q` | **10 passed** |

## P0 Issues
None.

## P1 Issues
None.

## P2 Issues

### P2-1: Detector E silently skips JSON parse errors
- **File:** `workflow/alerts.py:407-409`
- **Evidence:** `except (TypeError, json.JSONDecodeError): continue` — drops the row without logging.
- **Why:** Phase 9 daemons stamp `summary_json`; if a daemon ever writes a malformed payload (encoding bug, half-written row from a crash), Phase 8.5 will silently under-report queue buildup. Inconsistent with `_parse_ist`'s pattern of `log.warning` + counter.
- **Fix:** `log.warning("alerts: detector E malformed summary_json on agent=%s ts=%s", agent, ts_s)` + bump `_PARSE_FAILURES` (or a sibling counter).

### P2-2: `_PARSE_FAILURES` module global, not thread-/concurrent-run safe
- **File:** `workflow/alerts.py:123, 491`
- **Evidence:** Module-level `list`; `run()` clears it at entry. If two `run()` calls overlap (unlikely under launchd `StartInterval=900` but possible during manual testing), stats are wrong.
- **Fix:** Make it a local in `run()` and thread it through `_parse_ist` as a parameter, or accept the limitation and note it in the docstring.

### P2-3: `wf_kill_switch` lookup uses `ORDER BY id DESC LIMIT 1`, not `ORDER BY ts_ist DESC`
- **File:** `workflow/alerts.py:354`
- **Evidence:** Relies on monotonic `id` matching chronological order. True for SQLite autoincrement on a single-writer, but if a back-fill INSERT ever happens with `ts_ist` in the past, condition D will misfire.
- **Fix:** `ORDER BY ts_ist DESC, id DESC` is defensively correct and free. Drive-by — not load-bearing today.

## Daemon name contract (for Phase 9)

Phase 8.5 hardcodes these `agent` values in `TRACKED_DAEMONS` (alerts.py:54-59) — **Phase 9 launchd entrypoints must stamp the matching string into `wf_agent_events.agent`**:

| Daemon | `agent` value |
|---|---|
| Executor tick | `workflow_executor` |
| Scheduler | `workflow_scheduler` |
| Ingest | `workflow_ingest` |
| Enrollment poller | `workflow_enrollment` |

Notably **NOT tracked** by Phase 8.5 (intentional — the alerts watcher itself shouldn't alert on its own absence, and the digest is daily so a 30-min window is wrong):
- `workflow_alerts` (this module)
- `workflow_digest`

If Phase 9 names diverge (e.g., `executor` instead of `workflow_executor`), condition A fires forever. Cross-check the plist `ProgramArguments` and the `emit_wf_agent_event(...)` call site before activating launchd.

## Assumptions Made
- Phase 1's `wf_agent_events` schema has `agent TEXT`, `ts_ist TEXT`, `summary_json TEXT` — confirmed by test fixture mirror (test_alerts.py:42-48).
- Phase 6's scheduler will stamp `{"processed": N}` and Phase 5's executor will stamp `{"rows_processed": N}` — detector E tolerates either, which is correct hedging.
- `vibrium-automation` and `ops_console_v2` files modified in the last 2h belong to prior closed phases (3, 10), not this changeset — verified against Phase 8.5 close.md scope.

## What I Did Not Audit
- Live launchd plist (Phase 9 deliverable; not in scope).
- Real `wf_agent_events` schema on `state/workflow.db` — relied on the test fixture mirror. If migration 001 diverges from this column set, detector queries will `OperationalError` and (correctly) be tolerated, but condition A would go dark silently except for the DEBUG log line. Worth a Phase 9 integration test.

## KB Updates Applied
None — Phase 8.5 patterns (sqlite cooldown sidecar + payload-only alert watcher + governance-gated SMTP) are already covered in `auditor_kb/governance.md` and `auditor_kb/system-design-patterns.md`.

## Recommendations
1. Address P2-1 (detector E silent JSON skip) — 2-line fix, matches the established `_parse_ist` pattern. Worth doing before Phase 9 wires this into launchd.
2. P2-2 and P2-3 are nice-to-have; defer to a sweep.
3. **Phase 9 owner:** read "Daemon name contract" above and stamp matching `agent` strings.
4. Before authorising real SMTP send (post-Phase-13?), produce a 1-week dry-run log digest to characterise alert noise and tune cooldown / thresholds.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no HTTP surface |
| 3 | Console wiring | N/A — backend daemon only |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI |

Phase 8.5 ships on the master-auditor gate alone (no UI, no API surface). Phase 9 will wire launchd; alerts dashboard UI deferred to a later phase per `phase_8_5_close.md`.
