"""SAME_DAY_GATE handler — decide same-day reattempt vs roll to the next day.

WS3 same-day-retry. Reached on a NO-CONNECT (the BRANCH_ON_DISPOSITION 'retry'
edge, or the AWAIT_DISPOSITION 'timeout' edge after the 90-min vendor SLA). It
decides, in real time, whether to place ANOTHER call to this customer TODAY:

  * retry_today  — there is same-day budget left AND room in the call window for
                   a call ~min_gap_hours from now. Bumps ``attempts_today`` and
                   routes to a same-day WAIT (≥1h gap) → FIRE. Does NOT touch
                   ``day_index`` (a same-day retry must never burn the N-day
                   budget).
  * next_day     — same-day budget spent OR the window is closing. Routes to the
                   day COUNTER → next-day best-hour wait (which resets
                   attempts_today).

Node config:
    {
        "attempts_today_key": "attempts_today",  # default "attempts_today"
        "max_per_day": 3,         # max TOTAL fires/day incl. the day's first call
        "min_gap_hours": 1,       # >=1h gap between same-day calls
        "window_close_hour": 19   # last fire strictly before this IST hour (RBI 19:00)
    }

``attempts_today`` counts same-day RETRIES (0 = none yet; the day's FIRST call
fires directly, not via this gate). So max retries = ``max_per_day - 1`` and the
total calls/day = 1 initial + retries <= max_per_day. The scheduler's
``customer_call_audit`` 3/day cap + ``pre_call_gate`` 08:00-19:00 window are the
HARD backstops; this gate is the intent layer (so it can never *exceed* them,
only decline earlier).

Edges: ``retry_today``, ``next_day``, ``error``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.same_day_gate")

_IST = ZoneInfo("Asia/Kolkata")


def _now_ist() -> datetime:
    return datetime.now(_IST).replace(tzinfo=None)


def _error(msg: str, side: str) -> NodeResult:
    return NodeResult(
        next_edge="error",
        scratchpad_patch={"same_day_gate_error": msg},
        side_effect=side,
        ready_at_ist=None,
    )


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    key = node.config.get("attempts_today_key", "attempts_today")
    try:
        max_per_day = int(node.config.get("max_per_day", 3))
        min_gap_hours = int(node.config.get("min_gap_hours", 1))
        window_close_hour = int(node.config.get("window_close_hour", 19))
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: bad config routes to the 'error' edge, never a silent default that could over-call.
        return _error("bad_config", "SAME_DAY_GATE bad numeric config")
    if max_per_day < 1 or min_gap_hours < 1 or not (1 <= window_close_hour <= 24):
        return _error("bad_config", "SAME_DAY_GATE config out of range")

    try:
        attempts_today = int(run.scratchpad.get(key, 0) or 0)
    except (TypeError, ValueError):
        log.warning("SAME_DAY_GATE non-int %s=%r — treating as 0",
                    key, run.scratchpad.get(key))
        attempts_today = 0
    if attempts_today < 0:
        attempts_today = 0

    now = _now_ist()
    # The next same-day call would fire ~min_gap_hours from now (after the
    # same-day WAIT). It must land strictly before window_close_hour on the SAME
    # calendar day, or there's no room today.
    candidate = now + timedelta(hours=min_gap_hours)
    room_today = (candidate.date() == now.date()
                  and candidate.hour < window_close_hour)

    # max retries = max_per_day - 1 (the day's first call isn't a retry).
    has_budget = attempts_today < (max_per_day - 1)

    if has_budget and room_today:
        return NodeResult(
            next_edge="retry_today",
            # WS7/2c defense-in-depth (P2-2): a no-connect reattempt is NOT a
            # reserve follow-up — reset priority_class to 'general' here so a
            # stale 'reserve' (from a prior PTP/Agree fire that then no-connected)
            # can never leak into the retry fire, independent of the downstream
            # WAIT node's reset.
            scratchpad_patch={key: attempts_today + 1, "priority_class": "general"},
            side_effect=(
                f"SAME_DAY_GATE retry_today: retries {attempts_today}→"
                f"{attempts_today + 1} (max {max_per_day - 1}), "
                f"next fire ~{candidate.strftime('%H:%M')}"
            ),
            ready_at_ist=None,
        )

    reason = ("budget spent" if not has_budget
              else f"no room today (next ~{candidate.strftime('%H:%M')} >= {window_close_hour}:00)")
    return NodeResult(
        next_edge="next_day",
        scratchpad_patch={"priority_class": "general"},  # P2-2: no-connect → general
        side_effect=f"SAME_DAY_GATE next_day: {reason}",
        ready_at_ist=None,
    )
