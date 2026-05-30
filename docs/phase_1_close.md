# Phase 1 — Closure Summary

**Date closed:** 2026-05-30
**Branch:** main
**Predecessor phase:** Phase 0 (repo foundation — closed 2026-05-30)

## What this phase delivered

The complete owned-schema for the workflow engine across two SQLite files,
plus the connection helpers and migration runner that every later phase
depends on. No business logic — just schema, idempotent migrations, and a
typed connection façade that hard-enforces the architecture invariant
"workflow code never writes to vibrium.db outside the audit path."

## Files created or modified

| Path | Purpose |
|---|---|
| `workflow/wf_store.py` | Two-DB connection helper. `get_workflow_db()` opens `state/workflow.db` RW; `get_vibrium_db(mode='r'|'rw')` opens `state/vibrium.db` with `PRAGMA query_only=1` enforcing read-only when `mode='r'`. WAL + foreign_keys pragmas applied at open time. `transaction(conn)` context manager wraps explicit `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK`. |
| `workflow/migrations/001_init.py` | Creates the 11 owned tables in `state/workflow.db` + 8 indexes. Idempotent (every DDL is `IF NOT EXISTS`). Records `schema_version` row on first apply. |
| `workflow/migrations/002_customer_call_audit.py` | Additive-only migration on `state/vibrium.db`. Creates `customer_call_audit` table + `idx_cca_customer_day` index + a forensic `schema_version_vbwf` marker. Also writes a row to `workflow.db.schema_version` so the runner's idempotency check sees it on rerun. |
| `workflow/migrations/runner.py` | Discovers `NNN_*.py` migrations, reads `schema_version` from workflow.db, applies pending ones in order. CLI: `python3 -m workflow.migrations.runner --workflow-db <path> --vibrium-db <path> [--dry-run]`. |
| `workflow/tests/test_migrations.py` | 18 tests (≥10 required) covering schema shape, idempotency, byte-identical preservation of pre-existing vibrium.db tables, query_only enforcement, transaction rollback, dedupe index, partial-UNIQUE enrollment key, CHECK constraints, dry-run no-write, CLI exit codes, the Phase 0a `tag_group`-absent invariant, and the `fired_at_ist` column presence. |
| `docs/phase_1_close.md` | This document. |

## Tables created in `state/workflow.db` (11)

`workflows`, `workflow_versions`, `workflow_runs`, `wf_pending_actions`,
`wf_decision_log`, `workflow_node_log`, `workflow_admin_log`,
`wf_kill_switch`, `wf_agent_events`, `agent_assignments`, `schema_version`.

## Table created in `state/vibrium.db` (1, additive only)

`customer_call_audit` — the shared single-source-of-truth for the
per-customer daily call cap. Both schedulers (adhoc + workflow) will INSERT
here in Phase 3 / Phase 6.

A second forensic table `schema_version_vbwf` is also created in vibrium.db
so an operator inspecting that DB in isolation can see which workflow-engine
migration touched it. This table is for human audit only — the runner's
idempotency check uses `workflow.db.schema_version`.

## Indexes created

In `workflow.db`:
* `idx_runs_ready` — `workflow_runs(status, ready_at_ist)` for tick-loop sweep.
* `idx_runs_customer` — `workflow_runs(customer_id, status)`.
* `idx_runs_enrollment_key` — partial UNIQUE on `(workflow_id, customer_id, enrollment_key) WHERE enrollment_key IS NOT NULL` (P0-6 enrollment idempotency).
* `idx_wfpa_ready` — `wf_pending_actions(status, scheduled_at_ist)` for scheduler sweep.
* `idx_wfpa_customer` — `wf_pending_actions(customer_id, status)`.
* `idx_wfpa_customer_fired` — `wf_pending_actions(customer_id, fired_at_ist)` for the Phase 0a triangulation join.
* `idx_wfdl_run` — `wf_decision_log(run_id, node_id, attempt_count)` for the disposition-wakeup match.
* `idx_wfdl_customer` — `wf_decision_log(customer_id, ts_ist)`.

In `vibrium.db`:
* `idx_cca_customer_day` — `customer_call_audit(customer_id, fired_at_ist)` for daily-cap reads.

## Key design decisions

### Phase 0a pivot reflected in schema

* **`tag_group` is NOT added to any table.** Verified absent in
  `collection_comment_data` per Phase 0a; the wakeup design pivoted to
  `(customer_id, fired_at_ist)` triangulation. A test
  (`test_no_tag_group_column_anywhere`) asserts no table in either DB has a
  column named `tag_group`.
* **`wf_pending_actions.fired_at_ist`** is the column workflow_scheduler
  populates at fire time (Phase 6). It's covered by
  `idx_wfpa_customer_fired` so the Phase 7 ingest join is indexed.

### Idempotency strategy

Every DDL is `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.
The runner additionally tracks applied migrations in `workflow.db.schema_version`
(by `SCHEMA_KEY`, e.g., `'001_init'`, `'002_customer_call_audit'`).
First-run detection is done via `sqlite_master` probe — narrow check on
"is the schema_version table present?" rather than a broad
`except OperationalError`, so genuine SQL errors propagate (SF006 compliance).

Migration 002 writes its applied-marker into BOTH `vibrium.db` (forensic)
AND `workflow.db.schema_version` (runner-canonical). Order is: vibrium DDL
first, workflow.db marker second, so if the actual DDL fails we don't
falsely advertise the migration as applied.

### Read-only enforcement on vibrium.db

`get_vibrium_db(mode='r')` sets `PRAGMA query_only=1` at open time. Test
`test_vibrium_db_read_mode_rejects_writes` proves an INSERT raises
`sqlite3.OperationalError`. The only paths that pass `mode='rw'` are:
1. Migration 002 (this phase).
2. The Phase 3 audit recorder (`shared/customer_call_audit.py`).

Nothing else. Static lint will catch additional `mode='rw'` call-sites in
future code review.

### Transaction semantics

`transaction(conn)` uses explicit `BEGIN IMMEDIATE` rather than relying on
Python sqlite3's autocommit detection. This:
1. Acquires the RESERVED lock at the BEGIN, serializing concurrent writers
   deterministically.
2. Matches the adhoc system's `ingest.py` pattern.
3. Guarantees a single failed handler's rollback doesn't leak partial state.

## Acceptance commands — all pass

```
$ python3 -m workflow.migrations.runner --workflow-db /tmp/wf.db --vibrium-db /tmp/vibrium.db
applying: 001_init
applying: 002_customer_call_audit
applied 2 migration(s)
# exit=0

$ python3 -m workflow.migrations.runner --workflow-db /tmp/wf.db --vibrium-db /tmp/vibrium.db
no pending migrations
# exit=0  (no-op rerun)

$ sqlite3 /tmp/wf.db ".schema" | grep -c "CREATE TABLE"
11

$ pytest workflow/tests/test_migrations.py -q
..................                                                       [100%]
18 passed in 0.34s

$ python3 -m workflow.migrations.runner --workflow-db /tmp/wf2.db --vibrium-db /tmp/vb2.db --dry-run
dry-run: would apply 2 migration(s):
  - 001_init
  - 002_customer_call_audit
# exit=0  (and neither DB file created)
```

`/Users/sahil.m/vibrium-automation/state/vibrium.db` — verified untouched.

## Choices made beyond literal task spec

* **`get_vibrium_db()` accepts `mode='r'` as default** (not `mode='rw'`).
  The phase spec said "mode='rw' only used for the audit-table write path;
  mode='r' used elsewhere" — making `r` the default makes accidental writes
  impossible without an explicit override, which is the safer posture.
* **`transaction(conn)` uses `BEGIN IMMEDIATE`, not `BEGIN DEFERRED`.** The
  phase spec said "transaction context manager that does BEGIN/COMMIT/ROLLBACK
  correctly" — `IMMEDIATE` is the correct choice for write paths because
  it serializes at BEGIN rather than at first write, matching the adhoc
  system's convention.
* **Forensic `schema_version_vbwf` table in vibrium.db.** Not strictly in
  the spec, but lets an operator inspecting `vibrium.db` in isolation see
  which workflow-engine migration touched it. Cheap, additive.
* **8 indexes, not the 5 listed in the architecture doc snippet.** Added
  `idx_wfpa_customer_fired` (Phase 0a triangulation), `idx_wfdl_customer`
  (per-customer disposition lookup), `idx_wfpa_customer` (per-customer
  queue inspection). All justified by Phase 7's query patterns.
* **Test count is 18, not the ≥10 the phase spec requested.** Extra
  coverage: WAL-mode pragma, CHECK constraints on `workflow_runs.status`,
  CLI subprocess exit-code smoke, `tag_group`-absent invariant,
  `fired_at_ist`-present invariant.

## Explicitly NOT done in Phase 1 (deferred)

* No business logic — handlers, schedulers, executors all live in later
  phases.
* No `shared/customer_call_audit.py` write library — Phase 3.
* No connection-pool layer — daemons open their own connections; pool comes
  later if profiling shows it's needed.
* No FK constraints between workflow.db tables — SQLite enforces FKs
  per-connection only when `PRAGMA foreign_keys=ON`; we set that pragma but
  the DDL itself uses no `REFERENCES` clauses. Adding FK enforcement is a
  cleanup item, not a Phase-1 deliverable.

## Audit gate

Master-auditor pass required on `workflow/wf_store.py`,
`workflow/migrations/001_init.py`, `workflow/migrations/002_customer_call_audit.py`,
and `workflow/migrations/runner.py`. Verdict recorded in
`docs/phase_1_audit.md`.
