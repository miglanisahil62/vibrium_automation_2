# Master Auditor Report — Phase 5 close-out (vibrium-workflow executor) — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
13 executor tests pass; full suite 171 / 171 green (well above the >=121 bar). Lint clean on `workflow/agents/workflow.py`, one justified SF014 suppression on the `IN` placeholder skeleton — verified safe (only `?` count interpolated, all values bound). All 11 load-bearing claims hold up under inspection. Two P2s flagged: (a) `_persist_run_advance` re-reads + re-parses `workflow_versions.graph_json` for the type-denorm even though `version_cache` already has it parsed in scope; (b) test fixture uses a real-looking 7-digit customer_id `"8968249"` — harmless but worth a `cid_test_*` scheme for future fixtures.

## Stack Detected
- **Python:** 3.9 (system) — `from __future__ import annotations` used; no 3.10+-only syntax
- **Stdlib only in workflow.py:** `argparse`, `json`, `logging`, `sqlite3`, `sys`, `dataclasses`, `datetime`, `pathlib`, `typing`, `zoneinfo`
- **Domain:** Vibrium-workflow executor (state-machine tick loop, critical-surface)
- **KB files consulted:** `system-design-patterns.md` (retry/idempotency, time-correctness, observability), `sqlite3.md` (transaction semantics, WAL), `regex-pii.md` (test fixture review), `governance.md` (SF014 suppression review)
- **Live lookups performed:** none — stdlib only, KB sufficient

## P0 Issues — Must Fix Before Merge
*(none)*

## P1 Issues — Should Fix
*(none)*

## P2 Issues — Nice to Fix

### P2-1: `_persist_run_advance` re-parses graph instead of using the per-tick cache
- **Category:** Performance / Coherence
- **File:** [workflow/agents/workflow.py](workflow/agents/workflow.py#L604-L615)
- **Evidence:** In `_persist_run_advance` (the ACTIVE-advance branch) the executor runs a fresh `SELECT graph_json FROM workflow_versions WHERE id = ?` and pipes it through `_parse_graph` to discover `new_node_type`. The per-tick `version_cache` already contains this exact parsed map.
- **Why it's wrong:** Functionally fine but (a) wastes one SELECT + one full JSON parse per advance, and (b) violates the stated pinning invariant in spirit: this read is NOT routed through the cache, so a concurrent `workflow_versions` write between the cache build and this read could theoretically be observed. In practice the row IDs are immutable and tests pass, but the design rule "graph_json is read once per tick" is more defensible than "mostly once."
- **KB / source citation:** `system-design-patterns.md` §canonical-query lookup; module docstring §"per-tick version cache (built once at the top of the tick and never mutated mid-tick)."
- **Required fix:** Thread `version_cache` (or just `graph`) into `_persist_run_advance` and look up `to_node_id`'s type from there. Drop the inline SELECT + reparse.

### P2-2: Real-shaped customer_id in test fixtures
- **Category:** Governance (PII shape)
- **File:** [workflow/tests/test_executor.py](workflow/tests/test_executor.py#L202)
- **Evidence:** Default `_seed_run` `customer_id="8968249"` — 7-digit, indistinguishable from a real Stashfin customer_id. The batch-limit test uses safer `cid_0001…cid_0150`.
- **Why it's wrong:** Test fixtures occasionally migrate into demo scripts, screenshots, or copy-paste into prod scratch DBs. A `cid_test_*` namespace eliminates that risk surface. Drive-by; harmless today.
- **KB / source citation:** `regex-pii.md` §customer_id shapes.
- **Required fix:** Switch the default to `customer_id="cid_test_default"` (or similar) in `_seed_run`. Three-line change.

## Verification matrix (the 11 load-bearing claims)

| # | Claim | Verdict | Evidence |
|---|---|---|---|
| 1 | Transactional advance: handler + state advance + log row in one txn; handler raise → rollback → ERROR in separate txn | PASS | `_run_handler` lines 430-495 wrap all three in `with transaction(conn)`. `_mark_errored` opens its own `with transaction(conn)` on lines 660-681 and wraps the inner `_append_node_log` + UPDATE in a defensive `try/except Exception: log.exception` (lines 682-689) — error marking itself cannot crash the tick. |
| 2 | Kill switch FIRST; paused → no work, no heartbeat | PASS | `_kill_switch_active` at line 306 is the first DB read after `try:`. Lines 306-317 short-circuit to `status="paused"` with explicit `# NB: no heartbeat in paused state` comment. `TestKillSwitch::test_kill_blocks_all_advances` asserts `wf_agent_events` count = 0. |
| 3 | Version pinning + parameterised IN clause | PASS | `_build_version_cache` line 179 builds placeholder string from `len(version_ids)` only; values bound on line 182. SF014 suppression on line 180 is correctly scoped and the justification comment is accurate. `TestVersionPinning::test_run_stays_on_v1_after_v2_saved` proves v2 doesn't leak. |
| 4 | Orphan handling: status='ORPHANED', side_effect='node_not_found: <id>', no crash | PASS | `_mark_orphaned` line 706 `diag = reason or f"node_not_found: {run.current_node_id}"`. Wrapped in `try/except Exception: log.exception` (lines 730-731) so even DB-lock at orphan-marking time doesn't crash the tick. `TestOrphaned::test_unknown_node_id_marks_orphaned` covers both halves. |
| 5 | Dry-run: handlers get `dry_run=True`, log rows with `dry_run=1`, no advance, no pending_actions row | PASS | Line 434 passes `dry_run=dry_run` to handler; line 482-485 returns BEFORE `_persist_run_advance` in dry-run; `_append_node_log` line 258 writes `1 if dry_run else 0`. `TestDryRun::test_dry_run_logs_intent_only` asserts current_node_id unchanged + wf_pending_actions count=0. Heartbeat IS written in dry-run (line 376 unconditional); this is a defensible product call documented in `test_dry_run_does_not_write_heartbeat`. |
| 6 | Heartbeat: one row in `wf_agent_events` per non-paused tick | PASS | `_emit_heartbeat` line 734 called once at line 376 outside the per-run loop, only on the non-paused path. `TestHeartbeat::test_one_row_per_tick` asserts 3 ticks → 3 rows + summary_json schema. |
| 7 | TICK_BATCH_LIMIT=100 default; `--batch-limit` tunable | PASS | Line 71 `TICK_BATCH_LIMIT = 100`. CLI arg lines 800-803. `TestBatchLimit::test_caps_per_tick` confirms exactly 100 processed of 150, 50 remain. |
| 8 | CLI runs cleanly on empty DB, exit 0, both modes | PASS | `TestCLI::test_cli_empty_db_dry_run` and `test_cli_empty_db_live_no_runs` both green. JSON line on stdout, exit 0. |
| 9 | tz-aware IST throughout, no `except: pass`, no PII in logs, SQL parameterised | PASS | All `datetime.now()` calls use `ZoneInfo("Asia/Kolkata")`. Every `except` block is named + logged (lines 496, 682, 730, 769, 823) — no bare `except: pass`. `log.exception` on lines 502-505 logs `run_id`, `node_id`, `type` — no customer_id/phone/PAN. SF014 suppression vetted (P2 above is a perf note, not safety). |
| 10 | No file under `/Users/sahil.m/vibrium-automation/` modified | PASS | `git status` in `vibrium-automation`: working tree clean, ahead of origin by 1 commit (pre-existing). |
| 11 | Tests: 13 executor pass, full suite >=121 | PASS | `pytest workflow/tests/test_executor.py -q` → 13 passed in 0.56s. `pytest workflow/tests/ -q` → 171 passed (>121 ✓). |

## Assumptions Made
- Phase 4a handlers (`fire_vb_call`, `terminate`, `await_disposition`, etc.) honor `dry_run=True` themselves — the executor relies on this and I spot-checked `fire_vb_call.py` only. Full handler-side dry-run audit is Phase 4a's audit, not this one.
- The `TestIdempotency` design (pre-stage a pending_actions row, re-tick, expect dedupe) correctly models the architectural invariant. Since handler+state-advance are in one txn, real mid-handler crashes can't leave a half-state — the test's narrative section acknowledges this and the assertion (`INSERT OR IGNORE` dedupes) is still meaningful for out-of-band rows.

## What I Did Not Audit
- Phase 4a handler implementations (out of scope — they have their own audit doc).
- `wf_store.transaction()` semantics — read the file header, did not deep-audit `BEGIN IMMEDIATE` / rollback edges.
- launchd plist, console wiring (Phase 9 / 10 — out of scope).

## KB Updates Applied
*(none — no novel gotcha; the parameterised-IN idiom + the per-tick cache pattern are well-trodden ground)*

## Recommendations
Ready to close Phase 5 once the two P2s are either filed as follow-ups or merged in. The `_persist_run_advance` reparse (P2-1) is the only one with mild design-coherence weight; P2-2 is hygiene.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no HTTP surface in Phase 5 |
| 3 | Console wiring | N/A — Phase 10/11 |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Phase 5 ships as a library/CLI component; gates 2-4 attach later when the executor is wired into launchd (Phase 9) and the console (Phase 10).
