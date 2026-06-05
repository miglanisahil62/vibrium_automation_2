"""Migration 004 — additive ``ct_allocation_written_at_ist`` column on
``agent_assignments`` (workflow.db).

WS10 of X-Bucket Vibrium. The exit emailer writes the CleverTap profile property
``coll_agent_allocation = Agent_calling_Recommended`` (per-ID) for customers who
EXHAUSTED their VB call-day budget and are still due (``reason =
'max_attempts_reached'``). The emailer runs every ~2h during the call window, so
it needs an idempotency marker to write each qualified customer's CT property
EXACTLY ONCE — never re-writing on a later pass. This column records when the CT
write succeeded; NULL = not yet written.

Idempotency of the migration itself: ``ALTER TABLE ADD COLUMN`` errors if the
column already exists, so it is guarded by a ``PRAGMA table_info`` check. Safe to
re-run.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from workflow.wf_store import PathLike, get_workflow_db, transaction

SCHEMA_KEY = "004_agent_alloc_ct_written"
SCHEMA_VERSION = 1

_COLUMN = "ct_allocation_written_at_ist"
_TABLE = "agent_assignments"


def up(
    workflow_db_path: Optional[PathLike] = None,
    vibrium_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
) -> None:
    """Apply migration 004 to ``workflow.db`` (add idempotency column)."""
    if workflow_db_path is None:
        raise ValueError("004_agent_alloc_ct_written requires workflow_db_path")

    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({_TABLE})")}  # stashfin-lint: ignore  # _TABLE is a module constant ('agent_assignments'), not user input; identifiers can't be %s-parameterized
            if not cols:
                # agent_assignments is created in 001_init; if it is somehow
                # absent the migration ordering is broken — fail loud rather
                # than silently skipping the WS10 idempotency guard.
                raise RuntimeError(
                    f"{_TABLE} missing — run 001_init before 004"
                )
            if _COLUMN not in cols:
                conn.execute(f"ALTER TABLE {_TABLE} ADD COLUMN {_COLUMN} TEXT")  # stashfin-lint: ignore  # _TABLE/_COLUMN module constants; DDL identifiers can't be %s-parameterized
            # IST stamp — the column is *_ist; SQLite datetime('now') is UTC
            # (off by 5h30m), so compute IST in Python (master-auditor WS10 P2-2).
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
