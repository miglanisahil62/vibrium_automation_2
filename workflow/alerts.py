"""Phase 8.5 alert watcher. Reads ``state/workflow.db``; emits alerts on
5 conditions. NEVER sends real email — payload generation only.

Real SMTP send is intentionally gated behind a config flag + Sahil approval
per ``feedback_smtp_governance_block`` (the bare word ``smtplib`` in any
bash/script context is hard-blocked by guard.py without explicit
``I AUTHORIZE THIS SEND`` from the operator).

The five conditions:

  A. DAEMON_DOWN     — Any daemon heartbeat in ``wf_agent_events`` is older
                       than 30 min (or never seen).
  B. RUN_ERROR       — Any ``workflow_runs.status='ERROR'`` for >2h.
  C. RUN_WAITING     — Any ``workflow_runs.status='WAITING'`` for >7 days
                       (likely a stuck WAIT_UNTIL).
  D. KILL_SWITCH     — Latest ``wf_kill_switch.action='KILL'`` with no
                       subsequent ``RESUME`` for >1h.
  E. QUEUE_BUILDUP   — Any single tick in the last hour processed more than
                       ``TICK_BATCH_LIMIT * 0.9`` rows (signal that the
                       executor / scheduler can't keep up).

Cooldown: each condition has a 60-minute cooldown — same condition within
that window is suppressed (counted as ``alerts_skipped_cooldown``). Cooldown
state lives in a sidecar table ``alert_state`` on the same workflow.db.

Usage::

    python3 -m workflow.alerts --workflow-db state/workflow.db --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------- constants

ALERT_COOLDOWN_MINUTES = 60  # don't re-fire same alert within this window
ALERT_EMAIL_TO = "sahil.miglani@stashfin.com"
ALERT_EMAIL_FROM = "vibrium-workflow@stashfin.com"

# Tracked daemons — must match the ``agent`` values written by Phase 9 daemons
# to ``wf_agent_events``. Source of truth: PHASES.md Phase 9 deliverables.
TRACKED_DAEMONS: tuple[str, ...] = (
    "workflow_executor",
    "workflow_scheduler",
    "workflow_ingest",
    "workflow_enrollment",
)

DAEMON_DOWN_THRESHOLD_MIN = 30
RUN_ERROR_THRESHOLD_HOURS = 2
RUN_WAITING_THRESHOLD_DAYS = 7
KILL_SWITCH_THRESHOLD_HOURS = 1
# Per PHASES.md Phase 8.5: queue building up = tick processed > TICK_BATCH_LIMIT * 0.9.
# Default TICK_BATCH_LIMIT=100 in workflow.agents.workflow, so threshold = 90 rows.
TICK_BATCH_LIMIT = 100
QUEUE_BUILDUP_ROW_THRESHOLD = int(TICK_BATCH_LIMIT * 0.9)
QUEUE_BUILDUP_LOOKBACK_MIN = 60


# ---------------------------------------------------------------- dataclass


@dataclass
class Alert:
    """Single alert payload. ``details`` is structured data for the cooldown
    lookup and for downstream consumers (e.g., a future SMTP/Slack send).

    We intentionally do NOT include any ``send()`` method here — the goal of
    Phase 8.5 is to produce the payload, log it, and stop. Real SMTP is
    governance-gated and lives in a separate (post-approval) module.
    """

    condition: str  # "A_DAEMON_DOWN" | "B_RUN_ERROR" | "C_RUN_WAITING" | "D_KILL_SWITCH" | "E_QUEUE_BUILDUP"
    severity: str  # "P0" | "P1" | "P2"
    subject: str
    body: str
    details: dict = field(default_factory=dict)

    def as_email_payload(self) -> dict:
        """Shape: ``{to, from, subject, body, severity, condition, details}``.
        Consumed by tests + any future SMTP module."""
        return {
            "to": ALERT_EMAIL_TO,
            "from": ALERT_EMAIL_FROM,
            "subject": self.subject,
            "body": self.body,
            "severity": self.severity,
            "condition": self.condition,
            "details": self.details,
        }


# ---------------------------------------------------------------- helpers


def _now_ist() -> datetime:
    return datetime.now(IST)


def _ist_str(dt: datetime) -> str:
    """IST-naive string (matches the rest of the workflow code base — see
    ``project_vibrium_event_ts_format_drift``)."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(IST).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Counter for observability — bumped each time _parse_ist sees something it
# can't parse. Surfaced in run() stats for downstream monitoring. (SF006: we
# don't want a true silent default — operators need to see drift.)
_PARSE_FAILURES: list[str] = []


def _parse_ist(s: str | None) -> datetime | None:
    """Parse the IST-naive timestamp shape used by workflow.db. Returns
    aware datetime in IST, or None if blank/unparseable.

    Contract: None is a documented return value (blank cells, NULL columns,
    or genuinely malformed rows). Unparseable non-blank inputs are logged at
    WARNING and recorded in ``_PARSE_FAILURES`` so the next ``run()`` stats
    surface them instead of being silently dropped.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # Tolerate both "%Y-%m-%d %H:%M:%S" and ISO "T" separators.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    # Final fallback: dateutil-style ISO with microseconds.
    if "T" in s or " " in s:
        try:
            return datetime.fromisoformat(s).replace(tzinfo=IST)
        except ValueError as exc:
            log.warning("alerts: _parse_ist could not parse %r (%s)", s, exc)
            _PARSE_FAILURES.append(s)
            return None
    log.warning("alerts: _parse_ist could not parse %r (no known format matched)", s)
    _PARSE_FAILURES.append(s)
    return None


def ensure_alert_state_table(conn: sqlite3.Connection) -> None:
    """Create the cooldown sidecar table. Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_state (
          condition TEXT PRIMARY KEY,
          last_fired_at_ist TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _load_last_fired(conn: sqlite3.Connection) -> dict[str, datetime]:
    out: dict[str, datetime] = {}
    cur = conn.execute("SELECT condition, last_fired_at_ist FROM alert_state")
    for cond, ts in cur.fetchall():
        parsed = _parse_ist(ts)
        if parsed is not None:
            out[cond] = parsed
    return out


def _mark_fired(conn: sqlite3.Connection, condition: str, now: datetime) -> None:
    conn.execute(
        "INSERT INTO alert_state(condition, last_fired_at_ist) VALUES(?, ?) "
        "ON CONFLICT(condition) DO UPDATE SET last_fired_at_ist=excluded.last_fired_at_ist",
        (condition, _ist_str(now)),
    )
    conn.commit()


# ---------------------------------------------------------------- detectors
#
# Each detector is a pure read against workflow.db. Returns 0 or 1 Alert
# (we collapse multi-row findings into one alert with a count + sample list
# in ``details``, otherwise an outage produces a flood).


def detect_a_daemon_down(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """Any tracked daemon's most recent heartbeat is older than 30 min, OR
    has never been seen at all."""
    cutoff = now - timedelta(minutes=DAEMON_DOWN_THRESHOLD_MIN)
    down: list[dict] = []
    for daemon in TRACKED_DAEMONS:
        cur = conn.execute(
            "SELECT ts_ist FROM wf_agent_events WHERE agent=? ORDER BY ts_ist DESC LIMIT 1",
            (daemon,),
        )
        row = cur.fetchone()
        if row is None:
            down.append({"daemon": daemon, "last_seen_at_ist": None, "minutes_since": None})
            continue
        last_ts = _parse_ist(row[0])
        if last_ts is None or last_ts < cutoff:
            mins = None if last_ts is None else int((now - last_ts).total_seconds() // 60)
            down.append({"daemon": daemon, "last_seen_at_ist": row[0], "minutes_since": mins})
    if not down:
        return []
    daemons = ", ".join(d["daemon"] for d in down)
    body_lines = [
        f"{len(down)} workflow daemon(s) have not heartbeated in the last "
        f"{DAEMON_DOWN_THRESHOLD_MIN} minutes:",
        "",
    ]
    for d in down:
        if d["last_seen_at_ist"] is None:
            body_lines.append(f"  - {d['daemon']}: NEVER SEEN")
        else:
            body_lines.append(
                f"  - {d['daemon']}: last seen {d['last_seen_at_ist']} "
                f"({d['minutes_since']} min ago)"
            )
    body_lines += ["", "See docs/runbook.md section A for recovery steps."]
    return [
        Alert(
            condition="A_DAEMON_DOWN",
            severity="P0",
            subject=f"[P0] vibrium-workflow daemon down: {daemons}",
            body="\n".join(body_lines),
            details={"down": down},
        )
    ]


def detect_b_run_error(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """Any ``workflow_runs.status='ERROR'`` whose ``updated_at_ist`` is older
    than 2 hours."""
    cutoff = now - timedelta(hours=RUN_ERROR_THRESHOLD_HOURS)
    cutoff_s = _ist_str(cutoff)
    cur = conn.execute(
        "SELECT id, workflow_id, customer_id, current_node_id, updated_at_ist "
        "FROM workflow_runs "
        "WHERE status='ERROR' AND (updated_at_ist IS NULL OR updated_at_ist <= ?) "
        "ORDER BY updated_at_ist ASC LIMIT 25",
        (cutoff_s,),
    )
    rows = [
        {
            "run_id": r[0],
            "workflow_id": r[1],
            "customer_id": r[2],
            "current_node_id": r[3],
            "updated_at_ist": r[4],
        }
        for r in cur.fetchall()
    ]
    if not rows:
        return []
    cur2 = conn.execute(
        "SELECT COUNT(*) FROM workflow_runs WHERE status='ERROR' AND "
        "(updated_at_ist IS NULL OR updated_at_ist <= ?)",
        (cutoff_s,),
    )
    total = int(cur2.fetchone()[0])
    body_lines = [
        f"{total} workflow_runs stuck in status=ERROR for more than "
        f"{RUN_ERROR_THRESHOLD_HOURS}h. Showing first {len(rows)}:",
        "",
    ]
    for r in rows:
        body_lines.append(
            f"  - run_id={r['run_id']} wf={r['workflow_id']} cust={r['customer_id']} "
            f"node={r['current_node_id']} updated={r['updated_at_ist']}"
        )
    body_lines += ["", "See docs/runbook.md section B for recovery steps."]
    return [
        Alert(
            condition="B_RUN_ERROR",
            severity="P1",
            subject=f"[P1] vibrium-workflow {total} run(s) in ERROR >2h",
            body="\n".join(body_lines),
            details={"total": total, "sample": rows},
        )
    ]


def detect_c_run_waiting(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """Any ``workflow_runs.status='WAITING'`` whose ``updated_at_ist`` is
    older than 7 days. Usually a stuck WAIT_UNTIL or a disposition that
    never arrived."""
    cutoff = now - timedelta(days=RUN_WAITING_THRESHOLD_DAYS)
    cutoff_s = _ist_str(cutoff)
    cur = conn.execute(
        "SELECT id, workflow_id, customer_id, current_node_id, updated_at_ist "
        "FROM workflow_runs "
        "WHERE status='WAITING' AND (updated_at_ist IS NULL OR updated_at_ist <= ?) "
        "ORDER BY updated_at_ist ASC LIMIT 25",
        (cutoff_s,),
    )
    rows = [
        {
            "run_id": r[0],
            "workflow_id": r[1],
            "customer_id": r[2],
            "current_node_id": r[3],
            "updated_at_ist": r[4],
        }
        for r in cur.fetchall()
    ]
    if not rows:
        return []
    cur2 = conn.execute(
        "SELECT COUNT(*) FROM workflow_runs WHERE status='WAITING' AND "
        "(updated_at_ist IS NULL OR updated_at_ist <= ?)",
        (cutoff_s,),
    )
    total = int(cur2.fetchone()[0])
    body_lines = [
        f"{total} workflow_runs stuck in status=WAITING for more than "
        f"{RUN_WAITING_THRESHOLD_DAYS} days. Showing first {len(rows)}:",
        "",
    ]
    for r in rows:
        body_lines.append(
            f"  - run_id={r['run_id']} wf={r['workflow_id']} cust={r['customer_id']} "
            f"node={r['current_node_id']} updated={r['updated_at_ist']}"
        )
    body_lines += ["", "See docs/runbook.md section C for recovery steps."]
    return [
        Alert(
            condition="C_RUN_WAITING",
            severity="P2",
            subject=f"[P2] vibrium-workflow {total} run(s) WAITING >7d",
            body="\n".join(body_lines),
            details={"total": total, "sample": rows},
        )
    ]


def detect_d_kill_switch(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """Latest ``wf_kill_switch`` row is ``action='KILL'`` (no subsequent
    RESUME) older than 1 hour."""
    cutoff = now - timedelta(hours=KILL_SWITCH_THRESHOLD_HOURS)
    cur = conn.execute(
        "SELECT ts_ist, action, reason, set_by FROM wf_kill_switch ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        return []
    ts_s, action, reason, set_by = row
    if action != "KILL":
        return []
    ts = _parse_ist(ts_s)
    if ts is None or ts > cutoff:
        return []  # KILL is recent — give operator time to resolve
    minutes = int((now - ts).total_seconds() // 60)
    body = (
        f"wf_kill_switch action=KILL has been active for {minutes} minutes "
        f"(>1h threshold) with no RESUME.\n\n"
        f"  set_at  : {ts_s}\n"
        f"  set_by  : {set_by}\n"
        f"  reason  : {reason}\n\n"
        f"While KILL is active no scheduler fires happen. Confirm intent "
        f"or insert a RESUME row. See docs/runbook.md section D."
    )
    return [
        Alert(
            condition="D_KILL_SWITCH",
            severity="P1",
            subject=f"[P1] vibrium-workflow KILL switch active {minutes}min",
            body=body,
            details={
                "set_at_ist": ts_s,
                "set_by": set_by,
                "reason": reason,
                "minutes_active": minutes,
            },
        )
    ]


def detect_e_queue_buildup(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """Any single tick within the last hour processed more than
    ``TICK_BATCH_LIMIT * 0.9`` rows. Implementation reads
    ``wf_agent_events.summary_json`` looking for either ``rows_processed`` or
    ``processed`` keys (Phase 9 daemons stamp one of these)."""
    cutoff = now - timedelta(minutes=QUEUE_BUILDUP_LOOKBACK_MIN)
    cutoff_s = _ist_str(cutoff)
    cur = conn.execute(
        "SELECT ts_ist, agent, summary_json FROM wf_agent_events "
        "WHERE ts_ist >= ? AND summary_json IS NOT NULL "
        "ORDER BY ts_ist DESC LIMIT 200",
        (cutoff_s,),
    )
    hot: list[dict] = []
    for ts_s, agent, summary_s in cur.fetchall():
        try:
            summary = json.loads(summary_s) if summary_s else {}
        except (TypeError, json.JSONDecodeError):
            continue
        # Tolerate either key name; Phase 9 daemons may stamp 'rows_processed'
        # (executor) or 'processed' (scheduler). Both are valid signal.
        rows = summary.get("rows_processed")
        if rows is None:
            rows = summary.get("processed")
        if not isinstance(rows, (int, float)):
            continue
        if rows > QUEUE_BUILDUP_ROW_THRESHOLD:
            hot.append(
                {
                    "ts_ist": ts_s,
                    "agent": agent,
                    "rows_processed": int(rows),
                }
            )
    if not hot:
        return []
    body_lines = [
        f"{len(hot)} tick(s) in the last {QUEUE_BUILDUP_LOOKBACK_MIN}min "
        f"processed more than {QUEUE_BUILDUP_ROW_THRESHOLD} rows "
        f"(TICK_BATCH_LIMIT={TICK_BATCH_LIMIT}, threshold = 90%).",
        "",
        "Sample:",
    ]
    for h in hot[:10]:
        body_lines.append(
            f"  - {h['ts_ist']} {h['agent']}: {h['rows_processed']} rows"
        )
    body_lines += [
        "",
        "Queue is building up — likely cause is upstream (CT, ingest, "
        "disposition stream) running hot, or a daemon stalled.",
        "See docs/runbook.md section E for recovery steps.",
    ]
    return [
        Alert(
            condition="E_QUEUE_BUILDUP",
            severity="P1",
            subject=f"[P1] vibrium-workflow queue building up ({len(hot)} hot tick(s))",
            body="\n".join(body_lines),
            details={"hot_ticks": hot},
        )
    ]


_DETECTORS = (
    detect_a_daemon_down,
    detect_b_run_error,
    detect_c_run_waiting,
    detect_d_kill_switch,
    detect_e_queue_buildup,
)


# ---------------------------------------------------------------- orchestration


def run(workflow_db_path: str | Path, dry_run: bool = False) -> dict:
    """Read workflow.db; run all 5 detectors; de-dupe against cooldown;
    return stats.

    Args:
        workflow_db_path: path to ``state/workflow.db``.
        dry_run: when True, payloads are still produced and logged but the
            ``alert_state`` cooldown table is NOT updated. Used by tests and
            by the operator running ``--dry-run`` from the CLI.

    Returns:
        dict with keys ``alerts_emitted``, ``alerts_skipped_cooldown``,
        ``alerts``  (list of dict payloads).
    """
    path = Path(workflow_db_path)
    if not path.exists():
        log.warning("alerts: workflow_db_path=%s does not exist; no alerts emitted", path)
        return {
            "alerts_emitted": 0,
            "alerts_skipped_cooldown": 0,
            "alerts": [],
            "parse_failures": 0,
        }

    _PARSE_FAILURES.clear()
    now = _now_ist()
    conn = sqlite3.connect(str(path))
    try:
        ensure_alert_state_table(conn)
        last_fired = _load_last_fired(conn)
        cooldown_cutoff = now - timedelta(minutes=ALERT_COOLDOWN_MINUTES)

        emitted: list[Alert] = []
        skipped = 0
        for detector in _DETECTORS:
            try:
                found = detector(conn, now)
            except sqlite3.OperationalError as exc:
                # Tolerate a brand-new workflow.db that has only the alert_state
                # table — i.e., schema 001 not yet applied. The test suite
                # exercises this path.
                log.debug("alerts: detector %s skipped (%s)", detector.__name__, exc)
                continue
            for alert in found:
                last = last_fired.get(alert.condition)
                if last is not None and last >= cooldown_cutoff:
                    skipped += 1
                    log.info(
                        "alerts: condition=%s suppressed by cooldown "
                        "(last_fired=%s)",
                        alert.condition,
                        _ist_str(last),
                    )
                    continue
                emitted.append(alert)
                log.warning(
                    "alerts: condition=%s severity=%s subject=%r",
                    alert.condition,
                    alert.severity,
                    alert.subject,
                )
                if not dry_run:
                    _mark_fired(conn, alert.condition, now)
                    last_fired[alert.condition] = now

        return {
            "alerts_emitted": len(emitted),
            "alerts_skipped_cooldown": skipped,
            "alerts": [a.as_email_payload() for a in emitted],
            "parse_failures": len(_PARSE_FAILURES),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- CLI


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workflow.alerts",
        description="Phase 8.5 alert watcher. Reads state/workflow.db; emits "
        "alert payloads (no SMTP).",
    )
    p.add_argument(
        "--workflow-db",
        required=True,
        help="Path to state/workflow.db",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not update the alert_state cooldown table; otherwise "
        "behaviour is identical (payloads are always log-only in Phase 8.5).",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return p


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stats = run(args.workflow_db, dry_run=args.dry_run)
    log.info(
        "alerts: emitted=%d skipped_cooldown=%d dry_run=%s",
        stats["alerts_emitted"],
        stats["alerts_skipped_cooldown"],
        args.dry_run,
    )
    # Always exit 0 — alert generation is observation, not failure.
    return 0


if __name__ == "__main__":
    sys.exit(main())
