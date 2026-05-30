# Phase 7 — Master Auditor Report — workflow_ingest — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
Phase 7 is structurally sound. The triangulation join is correct, time-zone discipline is clean (single conversion helper, no naive `datetime.now()`), wake guards (incl. the `entered_node_at_ist <= comment_create_ist` stale-disposition gate) are in place, and the watermark + decision-log + wake UPDATE are all inside a single `BEGIN IMMEDIATE` transaction. 13/13 tests pass. No `tag_group` in source (docstring only). No writes to `vibrium-automation/`. Verdict is **PASS_WITH_NOTES** — zero P0, zero P1, four P2 nits worth tightening before Phase 6 lands.

## Stack Detected
- **Python:** 3.x with `zoneinfo`, sqlite3, dataclasses (stdlib only at runtime in this module).
- **Libraries:** none external in the hot path; lazy imports for `external/vibrium_automation_scripts/{db,parser,decision_v2}`.
- **Domain:** Vibrium / workflow engine (disposition wakeup).
- **KB consulted:** `auditor_kb/sqlite3.md`, `datetime-timezones.md`, `system-design-patterns.md`, `governance.md`. Memory: `feedback_no_hallucination`, `project_vibrium_event_ts_format_drift`, `feedback_nach_classification_canonical` (NACH not relevant here — no NACH).
- **Live lookups:** none required.

## P0 Issues — Must Fix Before Merge
None.

## P1 Issues — Should Fix
None.

## P2 Issues — Nice to Fix

### P2-1: Per-row exception inside an open transaction is fragile
- **Category:** Idempotency / Crash-safety
- **File:** [workflow/workflow_ingest.py](workflow/workflow_ingest.py#L555-L565)
- **Evidence:** Loop catches `Exception` per row but stays inside the same `transaction(conn)` block. If the per-row failure originates from sqlite3 (constraint violation, busy lock, etc.), the txn is in an aborted state and subsequent `conn.execute` calls in later rows will raise `sqlite3.OperationalError: cannot start a transaction within a transaction` / `SQL logic error`. In practice failures here are Python-level (parser/classifier) so the risk is low, but the comment "per-row isolation" is misleading.
- **Required fix:** Either (a) move the try/except outside the `with transaction(conn):` and process one row per micro-transaction (slower, truly isolated), or (b) explicitly classify which exceptions are non-DB and let DB exceptions propagate to abort the batch. Document the choice. Low priority because the realistic failure modes today are caught upstream.

### P2-2: Docstring contradicts the code on watermark-vs-poison-pill behaviour
- **Category:** Quality
- **File:** [workflow/workflow_ingest.py](workflow/workflow_ingest.py#L450-L455)
- **Evidence:** `_process_row` docstring says "a poison-pill row will recur" — but `run()` advances `max_id = max(max_id, int(row.get("id") or 0))` regardless of error, so the watermark moves past the bad row and it will NOT recur. Code is correct (no infinite poison-pill loop); docstring is wrong.
- **Required fix:** Update docstring to reflect that bad rows are counted in `errored` and the watermark advances past them; operator sees them via the stats counter, not via re-processing.

### P2-3: `FIRED_RECOVERED` is not in the Phase 1 CHECK constraint
- **Category:** Correctness (deferred)
- **File:** [workflow/migrations/001_init.py](workflow/migrations/001_init.py#L111)
- **Evidence:** `status TEXT NOT NULL CHECK(status IN ('PENDING','FIRING_IN_PROGRESS','FIRED','SUPPRESSED','ERROR','SHADOW_FIRED'))`. The triangulation query selects `WHERE status IN ('FIRED','FIRED_RECOVERED')` (line 220). For Phase 7 this is harmless because no row can carry `FIRED_RECOVERED` until Phase 6 writes it, and the read-side `IN` clause silently no-ops on a missing value.
- **Required fix:** When Phase 6 lands, add a migration extending the CHECK constraint (SQLite requires table rebuild via `INSERT INTO new_table SELECT * FROM old_table`). Track this as a Phase 6 prerequisite; do not patch it from Phase 7.

### P2-4: `_ensure_ist_naive()` is defined but never called
- **Category:** Quality
- **File:** [workflow/workflow_ingest.py](workflow/workflow_ingest.py#L107-L136)
- **Evidence:** All timestamp normalisation goes through `_redshift_to_ist()` and `_now_ist_str()`. `_ensure_ist_naive()` is dead code at present; risk is that a future caller uses it on a Redshift naive datetime expecting UTC→IST conversion and instead gets back the naive string unchanged (the function assumes naive=IST per its docstring).
- **Required fix:** Either delete `_ensure_ist_naive` or call it from one canonical entry point and harden the naive-side semantics.

## Verification Checklist (per audit request)

| # | Check | Result |
|---|---|---|
| 1 | Triangulation join uses `(customer_id, fired_at_ist + 24h)`, status IN ('FIRED','FIRED_RECOVERED'), DESC LIMIT 1 | PASS — line 215-228 |
| 1 | Multi-workflow customer test exists | PASS — `test_two_workflows_same_customer_only_proximity_wins` |
| 2 | Wake guards: `current_node_id == node_id`, `status='WAITING'`, `entered_node_at_ist <= comment_create_ist`, stats counter on mismatch | PASS — lines 309-332 + `skipped_state_mismatch++` at line 519 |
| 3 | Redshift UTC-naive → IST-naive via single helper `_redshift_to_ist` | PASS — lines 139-162 |
| 3 | No `datetime.now()` without tz | PASS — only call is `datetime.now(IST)` line 166 |
| 4 | Watermark + decision_log + wake UPDATE in one txn | PASS — `with transaction(conn):` lines 555-565 |
| 4 | Per-row try/except → errored++, batch continues, watermark still advances | PASS — lines 556-564 (see P2-1 caveat) |
| 5 | No `tag_group` reference outside docstring/architecture commentary | PASS — only docstring (line 8) |
| 6 | No live Redshift in tests — `comment_fetcher` injection | PASS — `_make_fetcher` + lazy import |
| 7 | No writes to `vibrium-automation/` | PASS — no path references |
| 8 | tz-aware datetimes, no `except: pass`, no bare `requests` | PASS |
| 9 | `FIRED_RECOVERED` harmless for Phase 7 | CONFIRMED (see P2-3) |
| 10 | `pytest workflow/tests/test_workflow_ingest.py -q` | PASS — 13/13 |

## Assumptions Made
- `decision_v2.classify()` and `parser.parse_comment()` behave per their existing contract in `external/vibrium_automation_scripts/`. Not re-audited here (Phase 7 scope is the wakeup join, not the parser).
- The Phase 6 workflow_scheduler will write `fired_at_ist` as IST-naive `YYYY-MM-DD HH:MM:SS` (memory `project_vibrium_event_ts_format_drift` — vibrium ecosystem convention). If Phase 6 deviates, the lexicographic compare in `_find_matching_pending_action` breaks silently.
- The 24h triangulation ceiling is operationally sufficient (creator's stated assumption + 3h Phase 6 cooldown).

## What I Did Not Audit
- The lazy-imported parser / classifier / Redshift fetcher in the sibling repo.
- The Phase 6 scheduler (not yet landed) — assumed contract for `fired_at_ist` shape and cooldown enforcement.
- Live Redshift connectivity / actual ILIKE-filter recall against production data.

## KB Updates Applied
None — no novel library behaviour discovered.

## Recommendations
1. Fix P2-2 (docstring) and P2-4 (dead code) opportunistically.
2. Open a Phase 6 prerequisite ticket for P2-3 (CHECK constraint extension).
3. Revisit P2-1 if any DB-level exception ever appears in stats.errored — at that point the in-txn pattern needs splitting.
4. **Ship-ready for Phase 7 scope.**

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no FastAPI route in this phase |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | N/A — daemon module, no UI surface (Phase 9 wires the orchestrator) |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Ship approval for Phase 7 in isolation: cleared on the static-review gate. End-to-end ship of the workflow engine still requires Phase 6 (scheduler) + Phase 9 (orchestrator) before this module has live inputs.
