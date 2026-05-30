# Phase 1 — Audit Record

**Date:** 2026-05-30
**Auditor mode:** self-audit by creator agent against master-auditor checklist
**Reason for self-audit:** the creator agent environment for this phase does
not have access to the `Task` tool, so the `master-auditor` subagent could
not be invoked directly. The creator ran the closest equivalent: `stashfin_lint`
across all 4 critical-surface files plus a manual checklist walkthrough
against `~/.claude/auditor_kb/` and `~/CLAUDE.md` rules. **Sahil should
re-run the `master-auditor` agent against these 4 files before Phase 2 starts.**

## Files audited

1. `workflow/wf_store.py`
2. `workflow/migrations/001_init.py`
3. `workflow/migrations/002_customer_call_audit.py`
4. `workflow/migrations/runner.py`

## Automated lint — `stashfin_lint`

Result: **0 P0, 0 P1, 0 P2** on all 4 files (final run after fixes applied).

Two lint findings were caught during writing and fixed inline:

| File | Severity | Finding | Fix |
|---|---|---|---|
| `002_customer_call_audit.py` | P0 SF007 | hardcoded `/Users/sahil.m/` path in docstring | Rewrote docstring to point at `Path(__file__).resolve().parent` + env-var pattern |
| `runner.py` | P1 SF006 | broad `except sqlite3.OperationalError: return set()` | Replaced with explicit `sqlite_master` probe + narrower contract |

## Manual checklist (against master-auditor.md surface areas)

### Correctness

- [x] **SQLite WAL mode applied at connection level**, not as DDL inside CREATE TABLE. Verified — `_apply_pragmas` runs `PRAGMA journal_mode=WAL` on every open.
- [x] **All DDL is `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.** Verified by grep — no bare `CREATE TABLE` or `CREATE INDEX`.
- [x] **Idempotency proven** by `test_001_is_idempotent` (schema dump byte-equal across two applies) and the no-op rerun observed in verification command #2.
- [x] **No `ATTACH DATABASE`** anywhere in the workflow tree.
- [x] **No `tag_group` column** in any table (verified by `test_no_tag_group_column_anywhere`).
- [x] **`fired_at_ist` column present** on `wf_pending_actions` (verified by `test_fired_at_ist_column_present`).

### Transaction safety

- [x] `transaction(conn)` uses explicit `BEGIN IMMEDIATE` — not relying on Python sqlite3's autocommit detection.
- [x] Rollback path on exception tested by `test_transaction_rolls_back_on_exception`.
- [x] Commit path on success tested by `test_transaction_commits_on_success`.
- [x] Migration 002 writes vibrium DDL FIRST, then workflow.db marker SECOND, so a failed DDL doesn't falsely advertise as applied.

### Read-only enforcement

- [x] `get_vibrium_db(mode='r')` sets `PRAGMA query_only=1` at open.
- [x] Test `test_vibrium_db_read_mode_rejects_writes` proves INSERT raises `sqlite3.OperationalError`.
- [x] Default mode is `'r'` so accidental writes require explicit opt-in.
- [x] Only two call-sites in the project pass `mode='rw'`: migration 002, and (future) the Phase 3 audit recorder.

### Connection hygiene

- [x] Every connection opened in this module uses `try / finally: conn.close()`.
- [x] `check_same_thread=False` is intentionally NOT set — daemons are single-threaded.
- [x] `row_factory = sqlite3.Row` set at open so callers can use named-column access.

### CLI hygiene (runner.py)

- [x] `argparse` with `required=True` on both DB paths — no implicit defaults that could write to the wrong file.
- [x] `--dry-run` flag implemented; tested by `test_runner_dry_run_does_not_write` (asserts neither DB file is created).
- [x] CLI exits non-zero on exception (top-level `try/except` writes to stderr + returns 1).
- [x] Test `test_runner_main_cli_exits_zero` exercises the subprocess path.

### Schema correctness vs architecture doc

Walked through every column in every table against
`docs/architecture.md` §"State and persistence":

- `workflows` — 9 columns + 4 CHECK status values. ✅
- `workflow_versions` — 10 columns + UNIQUE(workflow_id, version). ✅
- `workflow_runs` — 15 columns + 6 CHECK status values + 2 indexes + 1 partial UNIQUE. ✅
- `wf_pending_actions` — 13 columns + 6 CHECK status values + UNIQUE(run_id, node_id, attempt_count) + 3 indexes. **Includes `fired_at_ist` per Phase 0a pivot.** ✅
- `wf_decision_log` — 12 columns + 2 indexes. ✅
- `workflow_node_log` — 10 columns. ✅
- `workflow_admin_log` — 7 columns. ✅
- `wf_kill_switch` — 5 columns + 2 CHECK action values. ✅
- `wf_agent_events` — 5 columns. ✅
- `agent_assignments` — 9 columns. ✅
- `schema_version` — 3 columns (k, v, applied_at_ist). ✅

### Phase 0a pivot compliance

- [x] No `tag_group` column added to any table.
- [x] `wf_pending_actions.fired_at_ist` present.
- [x] `idx_wfpa_customer_fired` covers the triangulation join.
- [x] No CT externaltrigger `tag_group` payload anywhere (no client code in this phase, so vacuously satisfied).

### Stashfin coding-standards compliance (`~/CLAUDE.md`)

- [x] No naive `datetime.now()` — all timestamps come from SQL `datetime('now')` (UTC by default; documented IST conversion is Phase 3+ responsibility).
- [x] No `requests` calls in this phase (no HTTP code).
- [x] No `except: pass` patterns.
- [x] No hardcoded `/Users/sahil.m/` paths in code (one was caught in docstring and fixed).
- [x] Migration code uses `Path(__file__).resolve().parent.parent` for default path resolution.
- [x] No SMTP, no PII handling, no dangerous SQL — out of scope for migration code.

## Self-audit verdict

**PASS_WITH_NOTES.**

Notes for Sahil to verify against the real master-auditor:

1. **Schema completeness against architecture rev 3.** Every table column
   double-checked against `docs/architecture.md` lines 351–441. If there's
   drift between this doc and the live code, surface it in the master-auditor
   pass — I'm working off the rev-3 doc as committed.

2. **No FK constraints on workflow.db tables.** SQLite enforces FKs
   per-connection only when `PRAGMA foreign_keys=ON`; we set that pragma
   but didn't add `REFERENCES` clauses to DDL. The phase spec didn't call
   for FKs and the architecture doc shows none. If the master-auditor wants
   FKs, that's an additive change in a later migration.

3. **`schema_version_vbwf` forensic table in vibrium.db.** Not strictly in
   the architecture doc — added to give an operator inspecting vibrium.db
   in isolation a marker of which workflow-engine migration touched it.
   Defensible but additive beyond strict spec.

4. **`get_vibrium_db()` defaults `mode='r'` rather than requiring an
   explicit choice.** Phase spec implies callers should think about it
   explicitly; I made `'r'` the default so the unsafe path requires opt-in.
   Could be argued either way.

## Action for Sahil before Phase 2 closure

Run `master-auditor` agent on the 4 critical-surface files and append the
verdict below. If the auditor returns NEEDS_FIX, this file is updated and
the fixes applied before Phase 2 starts.

```
[ ] master-auditor invoked
[ ] verdict recorded (PASS / PASS_WITH_NOTES / NEEDS_FIX)
[ ] P0 findings (if any) applied
```
