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
import os
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.await_disposition")

_IST = ZoneInfo("Asia/Kolkata")
_IST_TS_FMT = "%Y-%m-%d %H:%M:%S"
_DEFAULT_TIMEOUT_HOURS = 24


def _max_await_hours() -> float:
    """Hard upper bound on how long a run may sit at AWAIT being re-parked. A
    call that NEVER fires (queue starvation, stuck PENDING) must not park
    forever (master-auditor WS3-2b P0-1 second half) — past this bound the run
    escalates via the 'timeout' edge so an operator/agent picks it up. Defaults
    to ~one full call window; env-tunable."""
    try:
        return max(2.0, float(os.environ.get("WF_MAX_AWAIT_HOURS", "12")))
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: malformed env → safe 12h default (bounded, not unbounded)
        return 12.0


def _vendor_sla_minutes() -> int:
    """WS3 P0-3: how long after a call FIRES the bot's disposition reliably
    lands. Owner-confirmed: the disposition webhook is immediate once the call
    fires; the only lag is the 750/hr queue BEFORE firing. So 90 min after
    ``fired_at_ist`` is a conservative upper bound for "disposition should have
    arrived by now". Env-tunable; clamped to a sane floor."""
    try:
        v = int(os.environ.get("WF_VENDOR_DISPOSITION_SLA_MIN", "90"))
        return v if v >= 5 else 90
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: a malformed env var falls back to the safe 90-min default (the conservative direction — re-park rather than a false no-connect).
        return 90


def _current_action(txn: Any, run_id: int) -> "tuple[Optional[str], Optional[datetime]]":
    """Return (status, fired_at) of the MOST-RECENT wf_pending_actions row for
    this run — i.e. the call THIS AWAIT is waiting on (the latest FIRE that led
    here; max id). Returns:
      * ('FIRED'/'FIRED_RECOVERED', <fired_at datetime>) — the call went out.
      * ('SUPPRESSED'/'ERROR', None)                     — the scheduler refused
        the fire (cooldown / daily-cap / paid / window / CT error). The call will
        NEVER happen for this queued row → the run must escalate, not park.
      * ('PENDING'/'FIRING_IN_PROGRESS', None)           — still queued under the
        750/hr cap, not fired yet → re-park.
      * (None, None) — no row / txn None / query failed → treat as queued
        (conservative re-park, never a false no-connect)."""
    if txn is None:
        return (None, None)
    try:
        row = txn.execute(
            "SELECT status, fired_at_ist FROM wf_pending_actions "
            "WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (int(run_id),),
        ).fetchone()
    except Exception as exc:  # noqa: BLE001 — query failure → conservative re-park
        log.warning("await_disposition action-lookup failed run=%s (%s) — re-park", run_id, exc)
        return (None, None)
    if not row:
        return (None, None)
    fired = _parse_ist_ts(row[1]) if row[1] else None
    return (row[0], fired)


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

    # 2. Timeout elapsed? With the short (60-min) AWAIT, a timeout is a
    # RE-EVALUATE, not a give-up (WS3 P0-3). The bot disposition webhook is
    # immediate ONCE the call fires; the lag is the 750/hr queue BEFORE firing.
    # Classify via the MOST-RECENT wf_pending_actions row for this run:
    #   FIRED, ≥SLA(90m) silent   → genuine no-connect → 'timeout' edge
    #   SUPPRESSED / ERROR        → the queued fire was REFUSED (cooldown / cap /
    #                               paid / window / CT error) and will never
    #                               happen → escalate via 'timeout' (P1-1), never
    #                               park forever waiting on a call that won't come
    #   total wait ≥ MAX_AWAIT    → never-fired/stuck (queue starvation) → escalate
    #                               via 'timeout' so an operator/agent picks it up
    #                               (P0-1 second half — bounded re-park)
    #   FIRED <SLA, or PENDING    → disposition in-flight / still queued → re-park
    # On a re-park the run MUST stay current_node_type='AWAIT_DISPOSITION' so a
    # late disposition still wakes it via _wake_run.
    if now >= deadline:
        status, fired_at = _current_action(txn, run.id)
        sla_min = _vendor_sla_minutes()
        total_waited = now - anchor
        max_await = timedelta(hours=_max_await_hours())

        genuine_no_connect = (
            status in ("FIRED", "FIRED_RECOVERED")
            and fired_at is not None
            and (now - fired_at) >= timedelta(minutes=sla_min)
        )
        refused = status in ("SUPPRESSED", "ERROR")
        starved = total_waited >= max_await

        if genuine_no_connect or refused or starved:
            run.status = "ACTIVE"
            run.ready_at_ist = None
            if genuine_no_connect:
                why = (f"genuine no-connect (fired {fired_at.strftime(_IST_TS_FMT)} "
                       f"+ {sla_min}m SLA elapsed, silent)")
            elif refused:
                why = f"queued fire was {status} (refused — will not happen); escalate"
            else:
                why = (f"bounded re-park exhausted (waited {total_waited} >= "
                       f"{max_await}, status={status}); escalate")
            return NodeResult(
                next_edge="timeout",
                scratchpad_patch={},
                side_effect=f"AWAIT_DISPOSITION → timeout: {why}",
                ready_at_ist=None,
            )

        # Still in-flight (FIRED <SLA) or still queued (PENDING/None) and within
        # the MAX_AWAIT bound → re-park for another timeout window.
        requeue_deadline = (now + timedelta(hours=timeout_hours)).strftime(_IST_TS_FMT)
        run.status = "WAITING"
        run.ready_at_ist = requeue_deadline
        if status in ("FIRED", "FIRED_RECOVERED") and fired_at is not None:
            reason = f"fired {int((now - fired_at).total_seconds() // 60)}m ago < {sla_min}m SLA"
        else:
            reason = f"not fired yet (status={status or 'queued'})"
        return NodeResult(
            next_edge=None,
            scratchpad_patch={},
            side_effect=(
                f"AWAIT_DISPOSITION re-park ({reason}) until {requeue_deadline}"
            ),
            ready_at_ist=requeue_deadline,
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
