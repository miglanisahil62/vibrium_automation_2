"""TERMINATE handler — end a workflow run.

Node config shape:
    {
        "status": "PAID"      # default "DONE"; one of the architecture's
                              # canonical terminal_status enum-ish values:
                              # PAID, MAX_ATTEMPTS, INELIGIBLE, OUT_OF_SCOPE, etc.
    }

Mutates the run record in place:
    * ``run.status`` = ``'DONE'`` — the run-level lifecycle status (different
      from the journey-level ``terminal_status``).
    * ``run.terminal_status`` = ``node.config['status']`` or ``'DONE'``.
    * ``run.terminated_at_ist`` = now IST.

The executor is responsible for persisting these mutations to SQLite inside
the same transaction as the workflow_node_log append.

Returns ``next_edge=None`` — terminal nodes have no outgoing edges. The
executor distinguishes "terminal" from "parked" (await_disposition also
returns None) by ``run.status``: ``DONE`` means terminal, ``WAITING`` means
parked.

No side effects beyond mutating ``run``.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run


_IST = ZoneInfo("Asia/Kolkata")
_IST_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _now_ist_str() -> str:
    return datetime.now(_IST).strftime(_IST_TS_FMT)


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """End the run."""
    terminal = node.config.get("status", "DONE")
    if not isinstance(terminal, str) or not terminal.strip():
        terminal = "DONE"

    # Mutate the in-memory Run; executor persists.
    run.status = "DONE"
    run.terminal_status = terminal
    run.terminated_at_ist = _now_ist_str()
    run.ready_at_ist = None

    return NodeResult(
        next_edge=None,
        scratchpad_patch={},
        side_effect=f"TERMINATE terminal_status={terminal}",
        ready_at_ist=None,
    )
