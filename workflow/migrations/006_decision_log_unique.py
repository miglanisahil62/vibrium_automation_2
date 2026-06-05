"""Migration 006 — dedup + UNIQUE index on ``wf_decision_log``.

WS3-7 / disposition-fix P1-2. The ingest writes one ``wf_decision_log`` row per
processed disposition. On a watermark replay or two CRM comment rows for the same
call, the unconditional INSERT produced duplicate audit rows. This adds a UNIQUE
index so ``INSERT OR IGNORE`` (set in workflow_ingest) makes the audit exactly-once.

Key = ``(run_id, node_id, attempt_count, COALESCE(comment_id,''))``. The COALESCE
is load-bearing: SQLite treats NULLs as DISTINCT in a UNIQUE, so adhoc dispositions
with a NULL ``comment_id`` would otherwise escape dedup entirely — exactly the rows
we most want to collapse. Building the index over the COALESCE expression makes two
NULL-comment_id rows for the same (run,node,attempt) collide as intended.

Order: dedup existing rows (keep MIN(id) per key) BEFORE creating the UNIQUE index,
or the CREATE fails on current duplicates. Idempotent: the dedup is a no-op once
clean, and the index uses IF NOT EXISTS.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from workflow.wf_store import PathLike, get_workflow_db, transaction

SCHEMA_KEY = "006_decision_log_unique"
SCHEMA_VERSION = 1


def up(
    workflow_db_path: Optional[PathLike] = None,
    vibrium_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
) -> None:
    """Apply migration 006 to ``workflow.db`` (dedup + unique index)."""
    if workflow_db_path is None:
        raise ValueError("006_decision_log_unique requires workflow_db_path")

    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            tbls = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            if "wf_decision_log" not in tbls:
                raise RuntimeError("wf_decision_log missing — run 001_init before 006")

            # 1) Collapse existing duplicates, keeping the earliest row per key.
            #    COALESCE(comment_id,'') so NULL-comment_id rows dedup too.
            conn.execute(
                """
                DELETE FROM wf_decision_log
                WHERE id NOT IN (
                    SELECT MIN(id) FROM wf_decision_log
                    GROUP BY run_id, node_id, attempt_count, COALESCE(comment_id, '')
                )
                """
            )
            # 2) UNIQUE index over the COALESCE expression (matches the dedup key).
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_wfdl_dedup "
                "ON wf_decision_log(run_id, node_id, attempt_count, COALESCE(comment_id, ''))"
            )
            applied_ist = datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (k, v, applied_at_ist) "
                "VALUES (?, ?, ?)",
                (SCHEMA_KEY, SCHEMA_VERSION, applied_ist),
            )
    finally:
        conn.close()
