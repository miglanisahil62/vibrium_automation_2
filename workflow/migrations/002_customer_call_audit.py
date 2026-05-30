"""Migration 002 — additive ``customer_call_audit`` table on ``vibrium.db``.

This is the **only** write this project ever makes to ``state/vibrium.db``.
Everything else (in Phase 3 onwards) is a write through ``shared/customer_call_audit.py``
which is also additive INSERTs to this same table.

Schema source: ``docs/architecture.md`` §"State and persistence" → vibrium.db
section. Single shared audit log for the per-customer daily call cap, used by
both schedulers (adhoc + workflow) as the single source of truth.

Idempotency:
    ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``. Safe to
    re-run. No ``ALTER TABLE`` is ever issued from this migration — the
    architecture invariant is "additive only, no schema changes to existing
    Vibrium tables."

Operator override:
    The path defaults to ``state/vibrium.db`` for local dev but production
    callers MUST pass the explicit path from ``config.json`` (resolved via
    ``Path(__file__).resolve().parent`` + env vars in the runner, never
    hard-coded). The AWS-canonical path is supplied in the server's
    ``config.json``; Mac dev path lives in the user-local config copy.
"""
from __future__ import annotations

from typing import Optional

from workflow.wf_store import PathLike, get_vibrium_db, get_workflow_db, transaction

SCHEMA_KEY = "002_customer_call_audit"
SCHEMA_VERSION = 1


_DDL = [
    """
    CREATE TABLE IF NOT EXISTS customer_call_audit (
        id INTEGER PRIMARY KEY,
        customer_id TEXT NOT NULL,
        fired_at_ist TEXT NOT NULL,
        source TEXT NOT NULL CHECK(source IN ('adhoc','workflow')),
        run_id INTEGER,
        cohort_name TEXT,
        ct_response_status TEXT,
        ct_error TEXT
    )
    """,
    # Daily-cap query: COUNT(*) WHERE customer_id=? AND fired_at_ist >= today_start.
    "CREATE INDEX IF NOT EXISTS idx_cca_customer_day ON customer_call_audit(customer_id, fired_at_ist)",
]


def up(
    workflow_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
    vibrium_db_path: Optional[PathLike] = None,
) -> None:
    """Apply migration 002 to ``vibrium.db``.

    Note the swapped argument: this migration writes to ``vibrium_db_path``,
    NOT ``workflow_db_path``. The runner passes both; we use the second one.

    The function opens the vibrium.db connection with ``mode='rw'`` because
    this is one of the two explicitly-allowed write entry points (the other
    being the runtime audit recorder in Phase 3).
    """
    if vibrium_db_path is None:
        raise ValueError("002_customer_call_audit requires vibrium_db_path")
    if workflow_db_path is None:
        raise ValueError(
            "002_customer_call_audit requires workflow_db_path "
            "(used to record schema_version)"
        )

    # 1) Apply DDL + forensic marker on vibrium.db (the actual target).
    conn = get_vibrium_db(vibrium_db_path, mode="rw")
    try:
        with transaction(conn):
            for stmt in _DDL:
                conn.execute(stmt)
            # Forensic marker IN vibrium.db so an operator looking at that DB
            # in isolation knows which workflow-engine migration touched it.
            # This is for human audit, not for the runner's idempotency check.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version_vbwf (
                    k TEXT PRIMARY KEY,
                    v INTEGER NOT NULL,
                    applied_at_ist TEXT
                )
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO schema_version_vbwf (k, v, applied_at_ist) "
                "VALUES (?, ?, datetime('now'))",
                (SCHEMA_KEY, SCHEMA_VERSION),
            )
    finally:
        conn.close()

    # 2) Record the applied migration in workflow.db's schema_version — this
    #    is the single source of truth the runner consults to decide what to
    #    skip on rerun. Doing this AFTER the vibrium.db write ensures we
    #    don't mark "applied" if the actual DDL failed.
    wf_conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(wf_conn):
            wf_conn.execute(
                "INSERT OR REPLACE INTO schema_version (k, v, applied_at_ist) "
                "VALUES (?, ?, datetime('now'))",
                (SCHEMA_KEY, SCHEMA_VERSION),
            )
    finally:
        wf_conn.close()
