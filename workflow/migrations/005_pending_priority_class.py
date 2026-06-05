"""Migration 005 — additive ``priority_class`` column on ``wf_pending_actions``.

WS7 reserved-bandwidth scheduling. The scheduler splits its hourly budget into a
RESERVE lane (same-day callback requests + unfulfilled PTP/Agree follow-ups) and
a GENERAL lane (first-attempt breadth), so a high-intent same-day follow-up can
jump the queue without starving first attempts. The FIRE_VB_CALL handler stamps
this column from the run's scratchpad (set by the WAIT_CB / WAIT_PTP / WAIT_EOD
branches) so the scheduler can rank without re-reading the graph.

Values: 'reserve' (callback / unfulfilled-PTP follow-up) | 'general' (default,
NULL-equivalent). NULL is treated as 'general' by the scheduler.

Idempotency: guarded ALTER (PRAGMA table_info check). Safe to re-run.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from workflow.wf_store import PathLike, get_workflow_db, transaction

SCHEMA_KEY = "005_pending_priority_class"
SCHEMA_VERSION = 1

_COLUMN = "priority_class"
_TABLE = "wf_pending_actions"


def up(
    workflow_db_path: Optional[PathLike] = None,
    vibrium_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
) -> None:
    """Apply migration 005 to ``workflow.db`` (add priority_class column)."""
    if workflow_db_path is None:
        raise ValueError("005_pending_priority_class requires workflow_db_path")

    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({_TABLE})")}  # stashfin-lint: ignore  # _TABLE is a module constant ('wf_pending_actions'); identifiers can't be %s-parameterized
            if not cols:
                raise RuntimeError(f"{_TABLE} missing — run 001_init before 005")
            if _COLUMN not in cols:
                conn.execute(f"ALTER TABLE {_TABLE} ADD COLUMN {_COLUMN} TEXT")  # stashfin-lint: ignore  # _TABLE/_COLUMN module constants; DDL identifiers can't be %s-parameterized
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
