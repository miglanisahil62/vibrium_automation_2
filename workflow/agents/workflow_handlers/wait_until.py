"""WAIT_UNTIL handler — park run until a wall-clock time.

Node config shape (one of two forms):

    {"relative": "T+1 day at 08:00"}
    {"relative": "T+2 day"}              # implicit 00:00
    {"relative": "T+3 hour"}              # current minute/second preserved
    {"absolute": "scheduled_date"}        # scratchpad key holding YYYY-MM-DD IST

Time format:
  * All computations are IST-naive (``YYYY-MM-DD HH:MM:SS``) — matches
    ``await_disposition.py`` and the canonical workflow-event-ts contract.
  * Anchor for relative/rotate offsets is the run's ``entered_node_at_ist``
    (when the run entered THIS node), falling back to ``datetime.now(IST)`` when
    that is absent. It is deliberately NOT ``now``: a pure forward offset (e.g.
    "T+1 hour") anchored to ``now`` is a moving target — the handler only re-runs
    once the prior deadline is reached, at which point ``now+offset`` is again in
    the future, ``late`` is never true, and the run re-parks forever (a treadmill
    that never returns to FIRE). Anchoring to entry time fixes the deadline so it
    actually expires. The late/early comparison still uses real ``now``.
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

# Population fallback best-hours (mirrors call_timing's default) — used only when
# a run has no usable best_hours in scratchpad. All within the RBI 08:00-18:59
# window so a rotated park can never aim outside it.
_DEFAULT_HOURS = (10, 13, 16)

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


def _parse_ist(ts: Optional[str]) -> Optional[datetime]:
    """Parse an IST-naive ``YYYY-MM-DD HH:MM:SS`` timestamp. None on any failure.

    Used to anchor relative/rotate offsets to the run's node-entry time. Returns
    None (not a default) so the caller can fall back to ``now`` explicitly.
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    try:
        return datetime.strptime(ts.strip()[:19], _IST_TS_FMT)
    except ValueError:  # stashfin-lint: ignore  # documented contract: returns None to signal "unparseable"; caller (execute) falls back to now explicitly. Not a silent default.
        return None


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


def _rotate_target(node: NodeConfig, run: Run, now: datetime) -> "tuple[Optional[datetime], Optional[str]]":
    """WS4 best-hour rotation. Park the day's call at the customer's rotating
    best hour: ``best_hours[day_index % len(best_hours)]`` on the day
    ``today + day_offset``.

    Config: ``{"rotate_day_offset": int, "rotate_hours_key": str,
               "rotate_index_key": str}``.

    Returns ``(target_datetime, None)`` on success or ``(None, error_msg)``.
    Robust to a missing/empty/garbage best_hours list (falls back to the
    population default) and a non-int day_index (treats as 0) — a parked call
    must never crash on soft scratchpad state. The chosen hour is clamped into
    [8,18] (the pre_call_gate is the hard RBI enforcer; this is belt-and-braces
    so rotation can't aim a park outside the window).
    """
    try:
        day_offset = int(node.config.get("rotate_day_offset", 0))
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: bad config returns (None, err) → caller routes to the 'error' edge with wait_error, not a silent value default.
        return None, "invalid_rotate_day_offset"
    if day_offset < 0:
        return None, "invalid_rotate_day_offset"

    hours_key = node.config.get("rotate_hours_key", "best_hours")
    index_key = node.config.get("rotate_index_key", "day_index")

    raw_hours = run.scratchpad.get(hours_key)
    hours: list[int] = []
    if isinstance(raw_hours, (list, tuple)):
        for h in raw_hours:
            try:
                hi = int(h)
            except (TypeError, ValueError):
                continue
            if 8 <= hi <= 18:
                hours.append(hi)
    if not hours:
        hours = list(_DEFAULT_HOURS)  # population fallback

    try:
        day_index = int(run.scratchpad.get(index_key, 0) or 0)
    except (TypeError, ValueError):
        # Documented soft-state contract: a non-int day_index must not crash a
        # parked call. Log so a genuinely corrupt scratchpad is still visible.
        log.warning("WAIT_UNTIL rotate: non-int %s=%r in run scratchpad — using 0",
                    index_key, run.scratchpad.get(index_key))
        day_index = 0
    if day_index < 0:
        day_index = 0

    hour = hours[day_index % len(hours)]
    hour = max(8, min(18, hour))  # clamp into RBI window (defensive)
    target_date = (now + timedelta(days=day_offset)).date()
    return datetime(target_date.year, target_date.month, target_date.day, hour, 0, 0), None


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
    is_rotate = "rotate_hours_key" in node.config or "rotate_day_offset" in node.config

    if sum(bool(x) for x in (relative, absolute_key, is_rotate)) > 1:
        return _error(
            "more than one of 'relative'/'absolute'/'rotate_*' set; pick one",
            "WAIT_UNTIL misconfigured: ambiguous spec",
        )

    now = _now_ist()
    # Anchor relative/rotate offsets to when the run ENTERED this node, not to
    # `now`. A forward offset anchored to `now` is a moving target: the handler
    # only re-runs once the prior deadline is reached, so now+offset is again in
    # the future, `late` is never true, and the run re-parks forever (treadmill —
    # never returns to FIRE). entry+offset is fixed and goes past after `offset`
    # elapses → the run advances. Fall back to `now` if entry time is absent
    # (first-ever evaluation has entry==now anyway). `late` below still uses now.
    anchor = _parse_ist(run.entered_node_at_ist) or now
    target: Optional[datetime] = None

    # WS4: optional reset keys applied on the exit edge (e.g. attempts_today=0 on
    # a day rollover). Parsed up front so both park + advance paths emit it.
    reset_patch: dict = {}
    raw_reset = node.config.get("reset_keys")
    if isinstance(raw_reset, dict):
        reset_patch = dict(raw_reset)

    if is_rotate:
        target, rot_err = _rotate_target(node, run, anchor)
        if rot_err is not None:
            return _error(rot_err, f"WAIT_UNTIL rotate error: {rot_err}")
    elif relative:
        if not isinstance(relative, str):
            return _error(
                "invalid_relative_format",
                f"WAIT_UNTIL relative not a string: {type(relative).__name__}",
            )
        target = _parse_relative(relative, anchor)
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
            "missing 'relative'/'absolute'/'rotate_*' config",
            "WAIT_UNTIL misconfigured: no spec",
        )

    ready_at = target.strftime(_IST_TS_FMT)
    late = target <= now

    if not late:
        # Deadline is in the future — park the run. reset_patch (e.g.
        # attempts_today=0 on a day rollover) is applied on the park edge so the
        # reset lands when the next day's wait begins, regardless of late/early.
        run.status = "WAITING"
        run.ready_at_ist = ready_at
        side = f"WAIT_UNTIL parked until {ready_at}"
        return NodeResult(
            next_edge="next",
            scratchpad_patch=dict(reset_patch),
            side_effect=side,
            ready_at_ist=ready_at,
        )

    # Deadline already passed — advance immediately (best-hour already gone today
    # → "call ASAP in the remaining window", per WS4). The run was loaded as
    # status='WAITING'; _persist_run_advance branches on run.status: WAITING=park,
    # ACTIVE=advance. We MUST set ACTIVE so the executor follows next_edge to FIRE.
    run.status = "ACTIVE"
    run.ready_at_ist = None
    side = f"WAIT_UNTIL deadline {ready_at} already passed — advancing immediately"
    return NodeResult(
        next_edge="next",
        scratchpad_patch=dict(reset_patch),
        side_effect=side,
        ready_at_ist=None,
    )
