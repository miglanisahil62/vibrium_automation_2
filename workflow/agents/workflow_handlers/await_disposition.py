"""AWAIT_DISPOSITION handler — park run until disposition arrives or timeout.

Node config shape:
    {
        "timeout_hours": 24       # default 24
    }

External state coordination (per architecture rev 3 §"Side-effect rules" +
Phase 7 disposition wakeup):
    The disposition arrival is signaled by ``workflow_ingest.py`` (Phase 7)
    which writes ``last_disposition_action_class`` (one of the canonical
    action_class enum values) into the run's scratchpad AND sets
    ``run.ready_at_ist = now()`` so the executor re-ticks the run
    immediately. When this handler then runs, scratchpad already has the
    action_class set → we route to ``disposition``.

    If no disposition has arrived AND ``run.ready_at_ist`` has elapsed past
    the timeout, route to ``timeout``.

    Otherwise: park. Set ``run.status = 'WAITING'``, set
    ``run.ready_at_ist`` to ``entered_node_at_ist + timeout_hours`` (or
    now + timeout_hours if entered_at is unknown), and return
    ``next_edge=None``. The executor reads ``status='WAITING'`` and parks.

Edges: ``disposition``, ``timeout``. ``None`` means "parked" (executor
distinguishes from terminate via ``run.status``).

NB: Once routed (disposition OR timeout), the handler clears the
``last_disposition_action_class`` key so a subsequent re-entry to an
AWAIT_DISPOSITION node doesn't accidentally pick up the same disposition.
The downstream BRANCH_ON_DISPOSITION (Phase 4b) reads from a stable key
written by ingest in the same wakeup.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.await_disposition")

_IST = ZoneInfo("Asia/Kolkata")
_IST_TS_FMT = "%Y-%m-%d %H:%M:%S"
_DEFAULT_TIMEOUT_HOURS = 24


def _now_ist() -> datetime:
    return datetime.now(_IST).replace(tzinfo=None)


def _parse_ist_ts(ts: Optional[str]) -> Optional[datetime]:
    """Parse the canonical IST-naive event-ts. Returns None on any parse
    failure (caller treats as "unknown"). Format drift across vibrium
    consumers is documented in project_vibrium_event_ts_format_drift; this
    handler ONLY accepts the canonical ``YYYY-MM-DD HH:MM:SS`` form.
    """
    if not ts:
        return None
    try:
        return datetime.strptime(ts, _IST_TS_FMT)
    except (TypeError, ValueError):
        log.warning("await_disposition: unparseable timestamp %r — treating as missing", ts)
        return None


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Route by external state, else park.

    Three branches:
        1. ``last_disposition_action_class`` set in scratchpad → ``disposition``.
        2. timeout deadline elapsed → ``timeout``.
        3. otherwise → park (None edge, status=WAITING, ready_at_ist set).
    """
    timeout_hours = node.config.get("timeout_hours", _DEFAULT_TIMEOUT_HOURS)
    try:
        timeout_hours = float(timeout_hours)
        if timeout_hours <= 0:
            raise ValueError(f"timeout_hours must be > 0, got {timeout_hours}")
    except (TypeError, ValueError) as exc:
        log.warning("await_disposition bad timeout_hours: %s; using default %d",
                    exc, _DEFAULT_TIMEOUT_HOURS)
        timeout_hours = _DEFAULT_TIMEOUT_HOURS

    # 1. Disposition arrived?
    if run.scratchpad.get("last_disposition_action_class"):
        # Clear so a future AWAIT_DISPOSITION doesn't see this stale value.
        # The downstream BRANCH_ON_DISPOSITION node consults the canonical
        # ``last_disposition_action_class`` ingest writes per fire; clearing
        # here defends against accidental re-entry on the same run.
        action = run.scratchpad["last_disposition_action_class"]
        # Also restore ACTIVE status; ingest set ready_at_ist=now and may
        # have flipped status='WAITING' → 'ACTIVE' already, but be explicit.
        run.status = "ACTIVE"
        run.ready_at_ist = None
        return NodeResult(
            next_edge="disposition",
            scratchpad_patch={
                # NB: we intentionally do NOT clear last_disposition_action_class
                # here — BRANCH_ON_DISPOSITION (Phase 4b) reads it on the next
                # tick. Clearing happens AFTER BRANCH_ON_DISPOSITION consumes
                # it (responsibility of that handler).
            },
            side_effect=f"AWAIT_DISPOSITION woke on disposition action_class={action}",
            ready_at_ist=None,
        )

    # Compute the timeout deadline. Anchor on entered_node_at_ist so a kill
    # switch pause + resume doesn't shift the deadline forward.
    anchor = _parse_ist_ts(run.entered_node_at_ist) or _now_ist()
    deadline = anchor + timedelta(hours=timeout_hours)
    deadline_str = deadline.strftime(_IST_TS_FMT)
    now = _now_ist()

    # 2. Timeout elapsed?
    if now >= deadline:
        run.status = "ACTIVE"
        run.ready_at_ist = None
        return NodeResult(
            next_edge="timeout",
            scratchpad_patch={},
            side_effect=(
                f"AWAIT_DISPOSITION timeout after {timeout_hours}h "
                f"(anchor={anchor.strftime(_IST_TS_FMT)} deadline={deadline_str})"
            ),
            ready_at_ist=None,
        )

    # 3. Park. Executor reads status=WAITING and ready_at_ist and skips
    # this run until the deadline.
    run.status = "WAITING"
    run.ready_at_ist = deadline_str
    return NodeResult(
        next_edge=None,
        scratchpad_patch={},
        side_effect=f"AWAIT_DISPOSITION parked until {deadline_str}",
        ready_at_ist=deadline_str,
    )
