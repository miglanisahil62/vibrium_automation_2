"""Migration 003 — additive ``ct_profile_cache`` table on ``workflow.db``.

WS1 of X-Bucket Vibrium. A NEW dedicated, separately-observable cache table for
the day's CleverTap profile prefetch. Lives in ``workflow.db`` (the FSM's own
DB) — it NEVER touches ``vibrium.db`` / vibrium-automation.

The prefetch job (``workflow/ct_prefetch.py``) UPSERTs one row per
(customer_id, cohort_date); enrollment + the executor's FETCH_CT_PROPS read from
it instead of hitting CT live. Same-day TTL is implicit in ``cohort_date`` — a
new IST day is a new key; old rows are pruned by a daily DELETE.

Idempotency: ``CREATE TABLE/INDEX IF NOT EXISTS``. Safe to re-run.
"""
from __future__ import annotations

from typing import Optional

from workflow.wf_store import PathLike, get_workflow_db, transaction

SCHEMA_KEY = "003_ct_profile_cache"
SCHEMA_VERSION = 1


_DDL = [
    """
    CREATE TABLE IF NOT EXISTS ct_profile_cache (
        customer_id    TEXT NOT NULL,
        cohort_date    TEXT NOT NULL,   -- IST YYYY-MM-DD == TTL / date-stamp
        status         TEXT NOT NULL CHECK(status IN ('found','not_found','error')),
        attempts       INTEGER NOT NULL DEFAULT 0,
        profile_json   TEXT,            -- full CT record dict (profileData nested); NULL unless found
        fetched_at_ist TEXT NOT NULL,
        PRIMARY KEY (customer_id, cohort_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ctcache_date_status ON ct_profile_cache(cohort_date, status)",
]


def up(
    workflow_db_path: Optional[PathLike] = None,
    vibrium_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
) -> None:
    """Apply migration 003 to ``workflow.db`` (cache table + schema_version)."""
    if workflow_db_path is None:
        raise ValueError("003_ct_profile_cache requires workflow_db_path")

    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            for stmt in _DDL:
                conn.execute(stmt)
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (k, v, applied_at_ist) "
                "VALUES (?, ?, datetime('now'))",
                (SCHEMA_KEY, SCHEMA_VERSION),
            )
    finally:
        conn.close()
