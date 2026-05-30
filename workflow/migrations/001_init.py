"""Migration 001 — initial schema for ``state/workflow.db``.

Creates the 11 tables that constitute the workflow engine's owned state,
matching ``docs/architecture.md`` §"State and persistence" exactly:

    1.  workflows
    2.  workflow_versions
    3.  workflow_runs
    4.  wf_pending_actions          (fired_at_ist included per Phase 0a pivot)
    5.  wf_decision_log
    6.  workflow_node_log
    7.  workflow_admin_log
    8.  wf_kill_switch
    9.  wf_agent_events
    10. agent_assignments
    11. schema_version

Plus the indexes the architecture document specifies.

This migration is idempotent: every DDL statement is ``CREATE TABLE IF NOT
EXISTS`` / ``CREATE INDEX IF NOT EXISTS``. Re-running is a no-op.

Phase 0a pivot — what is NOT here:
    * No ``tag_group`` column anywhere. Triangulation by
      ``(customer_id, fired_at_ist)`` is the primary disposition-wakeup join
      (see ``docs/phase_0a_decision.md``).

Connection note:
    The runner opens the workflow.db connection (via ``wf_store``) and passes
    the path in. This migration only writes to ``workflow_db_path``;
    ``vibrium_db_path`` is accepted for signature uniformity with 002 but
    ignored here.
"""
from __future__ import annotations

from typing import Optional

from workflow.wf_store import PathLike, get_workflow_db, transaction

# Schema version key stored in the schema_version table once this migration
# completes. The runner uses ``k='001_init'``; see runner.py.
SCHEMA_KEY = "001_init"
SCHEMA_VERSION = 1


_DDL_TABLES = [
    # ---------------------------------------------------------------- workflows
    """
    CREATE TABLE IF NOT EXISTS workflows (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('DRAFT','ACTIVE','PAUSED','ARCHIVED')),
        active_version_id INTEGER,
        shadow_mode INTEGER NOT NULL DEFAULT 1,
        requires_approval INTEGER NOT NULL DEFAULT 1,
        max_new_enrollments_per_day INTEGER DEFAULT 1000,
        enrollment_key_template TEXT DEFAULT '{customer_id}_{YYYY-MM-DD}',
        created_at_ist TEXT,
        created_by TEXT
    )
    """,
    # -------------------------------------------------------- workflow_versions
    """
    CREATE TABLE IF NOT EXISTS workflow_versions (
        id INTEGER PRIMARY KEY,
        workflow_id INTEGER NOT NULL,
        version INTEGER NOT NULL,
        graph_json TEXT NOT NULL,
        validation_status TEXT,
        validation_errors TEXT,
        approved_at_ist TEXT,
        approved_by TEXT,
        created_at_ist TEXT,
        created_by TEXT,
        UNIQUE(workflow_id, version)
    )
    """,
    # ------------------------------------------------------------ workflow_runs
    """
    CREATE TABLE IF NOT EXISTS workflow_runs (
        id INTEGER PRIMARY KEY,
        workflow_id INTEGER NOT NULL,
        version_id INTEGER NOT NULL,
        customer_id TEXT NOT NULL,
        enrollment_key TEXT,
        current_node_id TEXT,
        current_node_type TEXT,
        entered_node_at_ist TEXT,
        status TEXT NOT NULL CHECK(status IN ('ACTIVE','WAITING','PAUSED','DONE','ERROR','ORPHANED')),
        scratchpad_json TEXT NOT NULL DEFAULT '{}',
        ready_at_ist TEXT,
        enrolled_at_ist TEXT,
        updated_at_ist TEXT,
        terminated_at_ist TEXT,
        terminal_status TEXT
    )
    """,
    # ------------------------------------------------------- wf_pending_actions
    # Per Phase 0a pivot: ``fired_at_ist`` is recorded by workflow_scheduler at
    # fire time and is the join key for disposition triangulation. NO
    # ``tag_group`` column here — that column doesn't exist in the upstream
    # ``collection_comment_data`` table either.
    """
    CREATE TABLE IF NOT EXISTS wf_pending_actions (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL,
        node_id TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        customer_id TEXT NOT NULL,
        scheduled_at_ist TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('PENDING','FIRING_IN_PROGRESS','FIRED','SUPPRESSED','ERROR','SHADOW_FIRED')),
        attempts INTEGER NOT NULL DEFAULT 0,
        last_attempt_at_ist TEXT,
        last_error TEXT,
        cohort_name TEXT,
        fired_at_ist TEXT,
        created_at_ist TEXT NOT NULL,
        UNIQUE(run_id, node_id, attempt_count)
    )
    """,
    # ---------------------------------------------------------- wf_decision_log
    """
    CREATE TABLE IF NOT EXISTS wf_decision_log (
        id INTEGER PRIMARY KEY,
        ts_ist TEXT NOT NULL,
        comment_id TEXT,
        customer_id TEXT NOT NULL,
        run_id INTEGER NOT NULL,
        node_id TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        disposition TEXT,
        sub_disposition TEXT,
        action_class TEXT,
        comment_create_date TEXT,
        notes TEXT
    )
    """,
    # --------------------------------------------------------- workflow_node_log
    """
    CREATE TABLE IF NOT EXISTS workflow_node_log (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL,
        ts_ist TEXT NOT NULL,
        from_node_id TEXT,
        to_node_id TEXT,
        edge_label TEXT,
        scratchpad_before TEXT,
        scratchpad_after TEXT,
        side_effect TEXT,
        dry_run INTEGER NOT NULL DEFAULT 0
    )
    """,
    # -------------------------------------------------------- workflow_admin_log
    """
    CREATE TABLE IF NOT EXISTS workflow_admin_log (
        id INTEGER PRIMARY KEY,
        ts_ist TEXT NOT NULL,
        workflow_id INTEGER NOT NULL,
        version_id INTEGER,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        detail_json TEXT
    )
    """,
    # ------------------------------------------------------------ wf_kill_switch
    """
    CREATE TABLE IF NOT EXISTS wf_kill_switch (
        id INTEGER PRIMARY KEY,
        ts_ist TEXT NOT NULL,
        action TEXT NOT NULL CHECK(action IN ('KILL','RESUME')),
        reason TEXT,
        set_by TEXT
    )
    """,
    # ----------------------------------------------------------- wf_agent_events
    """
    CREATE TABLE IF NOT EXISTS wf_agent_events (
        id INTEGER PRIMARY KEY,
        ts_ist TEXT NOT NULL,
        agent TEXT NOT NULL,
        status TEXT,
        summary_json TEXT
    )
    """,
    # --------------------------------------------------------- agent_assignments
    """
    CREATE TABLE IF NOT EXISTS agent_assignments (
        id INTEGER PRIMARY KEY,
        customer_id TEXT NOT NULL,
        reason TEXT NOT NULL,
        source TEXT,
        assigned_at_ist TEXT NOT NULL,
        assigned_to TEXT,
        resolved_at_ist TEXT,
        resolution_note TEXT,
        run_id INTEGER
    )
    """,
    # ------------------------------------------------------------ schema_version
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        k TEXT PRIMARY KEY,
        v INTEGER NOT NULL,
        applied_at_ist TEXT
    )
    """,
]


_DDL_INDEXES = [
    # Disposition wakeup query — runs needing tick by ready time.
    "CREATE INDEX IF NOT EXISTS idx_runs_ready ON workflow_runs(status, ready_at_ist)",
    # Per-customer lookup (e.g., 'is there an open run for this customer?').
    "CREATE INDEX IF NOT EXISTS idx_runs_customer ON workflow_runs(customer_id, status)",
    # Idempotent enrollment guard (P0-6 fix). Partial UNIQUE so old NULL
    # enrollment_key rows don't collide if any sneak in.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_enrollment_key
        ON workflow_runs(workflow_id, customer_id, enrollment_key)
        WHERE enrollment_key IS NOT NULL
    """,
    # workflow_scheduler "ready to fire" sweep.
    "CREATE INDEX IF NOT EXISTS idx_wfpa_ready ON wf_pending_actions(status, scheduled_at_ist)",
    # Per-customer queue inspection.
    "CREATE INDEX IF NOT EXISTS idx_wfpa_customer ON wf_pending_actions(customer_id, status)",
    # Disposition wakeup join (triangulation): scheduler writes fired_at_ist,
    # ingest matches by customer_id + time range.
    "CREATE INDEX IF NOT EXISTS idx_wfpa_customer_fired ON wf_pending_actions(customer_id, fired_at_ist)",
    # wf_decision_log → which run was woken by this disposition.
    "CREATE INDEX IF NOT EXISTS idx_wfdl_run ON wf_decision_log(run_id, node_id, attempt_count)",
    # Per-customer disposition lookup.
    "CREATE INDEX IF NOT EXISTS idx_wfdl_customer ON wf_decision_log(customer_id, ts_ist)",
]


def up(
    workflow_db_path: PathLike,
    vibrium_db_path: Optional[PathLike] = None,  # noqa: ARG001 — signature uniformity
) -> None:
    """Apply migration 001.

    Idempotent: safe to call repeatedly. Records itself in ``schema_version``
    on first apply; subsequent applies overwrite the row with the same data
    (``INSERT OR REPLACE``). The runner is what skips re-application; this
    function tolerates being called either way.

    All DDL is grouped inside one BEGIN IMMEDIATE / COMMIT so a crash mid-way
    leaves the DB in a clean state (SQLite DDL is transactional).
    """
    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            for stmt in _DDL_TABLES:
                conn.execute(stmt)
            for stmt in _DDL_INDEXES:
                conn.execute(stmt)
            # Record the applied migration. INSERT OR REPLACE so a "force
            # re-run" doesn't fail; the runner won't call us if we're already
            # at this version.
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (k, v, applied_at_ist) "
                "VALUES (?, ?, datetime('now'))",
                (SCHEMA_KEY, SCHEMA_VERSION),
            )
    finally:
        conn.close()
