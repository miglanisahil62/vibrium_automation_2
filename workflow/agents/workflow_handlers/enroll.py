"""ENROLL handler — the entry point of every workflow run.

Trivial by design: the enrollment_poller (Phase 8) already created the
``workflow_runs`` row with ``current_node_id`` pointing at this ENROLL node,
so by the time the executor reaches it the customer is already enrolled.

This handler's only job is to advance onto the "next" edge so the next tick
sees the run on the first real node (typically FETCH_CT_PROPS).

No side effects, no scratchpad mutations, no parking.
"""
from __future__ import annotations

from typing import Any

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Advance onto the ``next`` edge.

    Args are accepted for signature uniformity with the other handlers; none
    are used (ctx/txn/dry_run are no-ops for ENROLL).
    """
    return NodeResult(
        next_edge="next",
        scratchpad_patch={},
        side_effect=None,
        ready_at_ist=None,
    )
