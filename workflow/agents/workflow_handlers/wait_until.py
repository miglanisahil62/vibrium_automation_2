"""WAIT_UNTIL handler — park run until a wall-clock time.

Node config shape (one of two forms):

    {"relative": "T+1 day at 08:00"}
    {"relative": "T+2 day"}              # implicit 00:00
    {"relative": "T+3 hour"}              # current minute/second preserved
    {"absolute": "scheduled_date"}        # scratchpad key holding YYYY-MM-DD IST

Time format:
  * All computations are IST-naive (``YYYY-MM-DD HH:MM:SS``) — matches
    ``await_disposition.py`` and the canonical workflow-event-ts contract.
  * Anchor for relative offsets is ``datetime.now(IST)`` at handler-execution
    time (the executor's tick clock).
  * Absolute dates accept ONLY ``YYYY-MM-DD``. Any other format → ``error``
    edge with ``wait_error="invalid_date_format"``. Time of day for absolute
    is implicit 00:00:00 IST (operator intent: "park until that calendar
    date"). Future extension: ``"YYYY-MM-DD HH:MM"`` — out of scope here.

Late-wakeup behavior:
  * If the computed ``ready_at_ist`` is already in the past, we still set
    it and park. The executor's ``ready_at_ist <= now`` filter ensures the
    run fires immediately on the next tick. No special handling here.

Edges: ``next``, ``error``.

Park semantics: ``status='WAITING'``, ``ready_at_ist`` set, ``next_edge="next"``.
The executor parks if ``ready_at_ist > now`` and otherwise advances along the
``next`` edge.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.wait_until")

_IST = ZoneInfo("Asia/Kolkata")
_IST_TS_FMT = "%Y-%m-%d %H:%M:%S"
_DATE_FMT = "%Y-%m-%d"

# Relative format grammar (case-sensitive, single-spaces only):
#   "T+<N> day"               → +N days, midnight
#   "T+<N> day at HH:MM"      → +N days at HH:MM
#   "T+<N> hour"              → +N hours from now
# We deliberately keep the grammar tight; add new forms by adding new regexes,
# not by loosening the parser.
_RE_DAY = re.compile(r"^T\+(\d+)\s+day$")
_RE_DAY_AT = re.compile(r"^T\+(\d+)\s+day\s+at\s+(\d{1,2}):(\d{2})$")
_RE_HOUR = re.compile(r"^T\+(\d+)\s+hour$")


def _now_ist() -> datetime:
    return datetime.now(_IST).replace(tzinfo=None)


def _parse_relative(spec: str, now: datetime) -> Optional[datetime]:
    """Parse one of the supported ``T+...`` forms. Returns None on no match.

    Note: returns None for *any* unparseable string. The caller maps that to
    the ``error`` edge. We do NOT try to coerce malformed input — silent
    drift on park time is worse than a clear validation failure.
    """
    spec = spec.strip()

    m = _RE_DAY_AT.match(spec)
    if m:
        n = int(m.group(1))
        hh = int(m.group(2))
        mm = int(m.group(3))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        target_date = (now + timedelta(days=n)).date()
        return datetime(target_date.year, target_date.month, target_date.day, hh, mm, 0)

    m = _RE_DAY.match(spec)
    if m:
        n = int(m.group(1))
        target_date = (now + timedelta(days=n)).date()
        return datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)

    m = _RE_HOUR.match(spec)
    if m:
        n = int(m.group(1))
        return now + timedelta(hours=n)

    return None


def _parse_absolute(value: Any) -> Optional[datetime]:
    """Parse strict ``YYYY-MM-DD``. Returns None on any deviation."""
    if not isinstance(value, str):
        return None
    try:
        d = datetime.strptime(value.strip(), _DATE_FMT)
    except ValueError:  # stashfin-lint: ignore  # documented contract: parser returns None to signal "unparseable"; caller routes to the 'error' edge with wait_error='invalid_date_format' and records to audit log. Not a silent fallback to a default value.
        return None
    return d


def _error(msg: str, side: str) -> NodeResult:
    return NodeResult(
        next_edge="error",
        scratchpad_patch={"wait_error": msg},
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
    """Compute park deadline; set ``run.ready_at_ist``; return ``next``."""
    relative = node.config.get("relative")
    absolute_key = node.config.get("absolute")

    if relative and absolute_key:
        return _error(
            "both 'relative' and 'absolute' set; pick one",
            "WAIT_UNTIL misconfigured: ambiguous spec",
        )

    now = _now_ist()
    target: Optional[datetime] = None

    if relative:
        if not isinstance(relative, str):
            return _error(
                "invalid_relative_format",
                f"WAIT_UNTIL relative not a string: {type(relative).__name__}",
            )
        target = _parse_relative(relative, now)
        if target is None:
            return _error(
                "invalid_relative_format",
                f"WAIT_UNTIL unparseable relative spec: {relative!r}",
            )
    elif absolute_key:
        if not isinstance(absolute_key, str) or not absolute_key:
            return _error(
                "invalid_absolute_key",
                "WAIT_UNTIL absolute key must be a non-empty string",
            )
        raw = run.scratchpad.get(absolute_key)
        if raw is None:
            return _error(
                f"missing scratchpad key: {absolute_key}",
                f"WAIT_UNTIL missing scratchpad key {absolute_key!r}",
            )
        target = _parse_absolute(raw)
        if target is None:
            return _error(
                "invalid_date_format",
                f"WAIT_UNTIL absolute date not YYYY-MM-DD: {raw!r}",
            )
    else:
        return _error(
            "missing 'relative' or 'absolute' config",
            "WAIT_UNTIL misconfigured: no spec",
        )

    ready_at = target.strftime(_IST_TS_FMT)
    late = target <= now

    if not late:
        # Deadline is in the future — park the run.
        run.status = "WAITING"
        run.ready_at_ist = ready_at
        side = f"WAIT_UNTIL parked until {ready_at}"
        return NodeResult(
            next_edge="next",
            scratchpad_patch={},
            side_effect=side,
            ready_at_ist=ready_at,
        )

    # Deadline already passed — advance immediately.
    # The run was loaded from DB as status='WAITING'; _persist_run_advance
    # branches on run.status: WAITING=park, ACTIVE=advance. We MUST explicitly
    # set ACTIVE here so the executor follows next_edge to FIRE_VB_CALL.
    # Not setting it (leaving WAITING from the load) would re-park every tick.
    run.status = "ACTIVE"
    run.ready_at_ist = None
    side = f"WAIT_UNTIL deadline {ready_at} already passed — advancing immediately"
    return NodeResult(
        next_edge="next",
        scratchpad_patch={},
        side_effect=side,
        ready_at_ist=None,
    )
