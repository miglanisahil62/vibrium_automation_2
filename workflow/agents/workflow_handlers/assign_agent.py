"""ASSIGN_AGENT handler — INSERT one row into ``agent_assignments``.

Node config shape:
    {
        "reason": "dispute_or_nrp"          # free-text reason string
    }

The handler writes one row to ``agent_assignments`` capturing:
  * ``customer_id``   — run.customer_id
  * ``reason``        — from node.config (free-text; the graph author owns
                        the vocabulary; common values include
                        ``dispute_or_nrp``, ``max_attempts_reached``,
                        ``unhandled_disposition``).
  * ``source``        — ``f"workflow:{workflow_id}:v{version_id}"`` so the
                        assignment is attributable to a specific graph
                        version. Matches the FIRE_VB_CALL cohort_name format.
  * ``assigned_at_ist``— IST-naive ``YYYY-MM-DD HH:MM:SS``.
  * ``run_id``        — for back-reference into ``workflow_runs``.

NO DEDUPE on this table (unlike ``wf_pending_actions``).
    ``agent_assignments`` has no UNIQUE constraint by design. If the same
    run reaches ASSIGN_AGENT twice — which should not happen under a sane
    graph but can under an operator-driven graph edit that loops back to
    the same node — we want BOTH rows in the audit trail. An ops user
    reviewing the table needs to see "this customer was re-routed to an
    agent twice" rather than silently dropping the second event.

Transactional contract (mirrors FIRE_VB_CALL):
    The ``txn`` argument is an open ``workflow.db`` connection inside a
    BEGIN IMMEDIATE transaction (see ``workflow.wf_store.transaction``).
    This handler issues ONE INSERT and does NOT commit — the executor
    (Phase 5) commits both the side-effect and the run-state advance
    atomically.

Dry-run:
    ``ctx.get("dry_run")`` True OR ``dry_run`` kwarg True → log intent,
    skip the INSERT, still return ``next_edge="next"`` with the would-be
    timestamp in scratchpad so downstream nodes can branch on it.

Edges expected from graph_json: ``next``.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.assign_agent")

_IST = ZoneInfo("Asia/Kolkata")


def _now_ist_str() -> str:
    """Return ``YYYY-MM-DD HH:MM:SS`` IST-naive (matches event-ts canon)."""
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: sqlite3.Connection,
    dry_run: bool = False,
) -> NodeResult:
    """INSERT one row into ``agent_assignments``."""
    reason_raw = node.config.get("reason")
    if not isinstance(reason_raw, str) or not reason_raw.strip():
        return NodeResult(
            next_edge="error",
            scratchpad_patch={
                "assign_agent_error": "missing or empty 'reason' config",
            },
            side_effect="ASSIGN_AGENT misconfigured: no reason",
            ready_at_ist=None,
        )
    reason = reason_raw.strip()

    source = f"workflow:{run.workflow_id}:v{run.version_id}"
    now_ist = _now_ist_str()

    # Honor either an explicit dry_run kwarg or a ctx-level dry_run flag.
    ctx_dry_run = False
    if isinstance(ctx, dict):
        ctx_dry_run = bool(ctx.get("dry_run", False))
    effective_dry_run = bool(dry_run) or ctx_dry_run

    if effective_dry_run:
        log.info(
            "ASSIGN_AGENT DRY-RUN cid=%s reason=%s source=%s",
            run.customer_id, reason, source,
        )
        return NodeResult(
            next_edge="next",
            scratchpad_patch={
                "assigned_at": now_ist,
                "assigned_reason": reason,
            },
            side_effect=(
                f"DRY-RUN would INSERT agent_assignments cid={run.customer_id} "
                f"reason={reason} source={source}"
            ),
            ready_at_ist=None,
        )

    txn.execute(
        """
        INSERT INTO agent_assignments
            (customer_id, reason, source, assigned_at_ist, run_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run.customer_id,
            reason,
            source,
            now_ist,
            run.id,
        ),
    )

    return NodeResult(
        next_edge="next",
        scratchpad_patch={
            "assigned_at": now_ist,
            "assigned_reason": reason,
        },
        side_effect=(
            f"agent_assignments cid={run.customer_id} reason={reason}"
        ),
        ready_at_ist=None,
    )
