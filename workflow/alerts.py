"""Alert watcher. Reads ``state/workflow.db``; emits alerts on 6 conditions and
— when ``WF_ALERTS_SEND=1`` (set in the server cron wrapper) — EMAILS them to
the owner via ``_send_alert_email``. Without that env flag (tests/dev) it is
payload-only, so the suite never sends.

SMTP from a deployed cron script is allowed under ``feedback_smtp_governance_block``
(the guard only blocks ad-hoc Bash ``smtplib``); the ``WF_ALERTS_SEND`` gate is
what keeps dev/test runs silent.

The six conditions:

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
  F. MORNING_HEALTH  — Unattended-ops net: after the morning window, today's
                       cohort was prefetched but 0 enrolled, or enrolled but
                       0 real-fired, or nothing prefetched at all (P0). A
                       partial schema disarming the net emits a P1.

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
import os
import smtplib
import sqlite3
import sys
from dataclasses import dataclass, field
from email.mime.text import MIMEText
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
# detect_f morning-health thresholds (IST). Fetch+prefetch finish by ~08:30
# (fetch 07:30, fallback 08:15, prefetch 07:32) and have NO later retry, so a
# 0-prefetched day is terminal and checkable at 09:00. Enrollment/firing have
# scheduled catch-ups (enrollment 08:30/10:00/12:00, scheduler */5 from 08:00),
# so enrolled==0 / fired==0 must NOT alert until those have had time to land —
# checked at 11:00 to avoid crying wolf before the pipeline's own retries run.
MORNING_FETCH_CHECK_HOUR = 9
MORNING_PIPELINE_CHECK_HOUR = 11
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
    lookup and for the email body/subject. The payload is delivered by the
    module-level ``_send_alert_email`` (gated by ``WF_ALERTS_SEND``); the
    dataclass itself stays send-free so it remains trivially testable.
    """

    condition: str  # A_DAEMON_DOWN|B_RUN_ERROR|C_RUN_WAITING|D_KILL_SWITCH|E_QUEUE_BUILDUP|F_MORNING_HEALTH|F_MORNING_SCHEMA
    severity: str  # "P0" | "P1" | "P2"
    subject: str
    body: str
    details: dict = field(default_factory=dict)

    def as_email_payload(self) -> dict:
        """Shape: ``{to, from, subject, body, severity, condition, details}``.
        Consumed by tests + ``_send_alert_email``."""
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


def detect_f_morning_health(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """WS12 — the unattended-ops safety net. After the morning window, catch a
    silently-broken morning: a cohort was prefetched but 0 enrolled, OR enrolled
    but 0 fired, OR nothing prefetched at all. These are LOGICAL failures the jobs
    exit 0 on (so no DAEMON_DOWN / no 'down' heartbeat) — without this detector
    they are invisible until someone reads the funnel email. P0 because it means
    today's collections calls are NOT going out and the owner must intervene.

    Before the check hours the morning is still in progress → no alert. A bare
    DB (no workflow schema yet) is skipped, but a PARTIAL schema (the net's
    tables missing while the rest exist) emits a distinct P1 rather than
    silently disarming the watchdog."""
    if now.hour < MORNING_FETCH_CHECK_HOUR:
        return []

    # Schema guard: a fresh/empty DB → skip (no false alarm). But if the DB has
    # workflow tables yet the morning-health tables are gone, the net is
    # disarmed — be LOUD about that instead of vanishing (P1-2).
    needed = ("ct_profile_cache", "workflow_runs", "wf_pending_actions")
    present = [
        t for t in needed
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
        ).fetchone() is not None
    ]
    if not present:
        return []  # bare DB — matches run()'s fresh-DB tolerance
    if len(present) < len(needed):
        missing = [t for t in needed if t not in present]
        return [Alert(
            condition="F_MORNING_SCHEMA",
            severity="P1",
            subject="[P1] VB morning-health detector DISARMED — missing tables",
            body=("detect_f_morning_health expected tables "
                  f"{list(needed)} but {missing} are absent on a DB that has "
                  f"{present}. The unattended morning-health net cannot run "
                  "until the schema is repaired (migration not applied?)."),
            details={"missing": missing, "present": present},
        )]

    today = now.strftime("%Y-%m-%d")
    prefetched = conn.execute(
        "SELECT COUNT(*) FROM ct_profile_cache WHERE cohort_date=? AND status='found'",
        (today,),
    ).fetchone()[0]
    enrolled = conn.execute(
        "SELECT COUNT(*) FROM workflow_runs WHERE substr(enrolled_at_ist,1,10)=?",
        (today,),
    ).fetchone()[0]
    # Only REAL fires count — SHADOW_FIRED also stamps fired_at_ist, so without
    # this filter the net reads healthy in shadow mode / on a silent shadow
    # fallback, exactly when it must not (P1-1).
    fired = conn.execute(
        "SELECT COUNT(*) FROM wf_pending_actions "
        "WHERE substr(fired_at_ist,1,10)=? AND status='FIRED'",
        (today,),
    ).fetchone()[0]

    problem = None
    # Fetch/prefetch are done (no later retry) → 0-prefetched is terminal at 09:00.
    if prefetched == 0:
        problem = ("no cohort prefetched today — the 07:30 fetch and/or 07:32 "
                   "prefetch produced nothing (Redshift down? collection_view empty? "
                   "CT fetch failed?). NO calls will go out today.")
    # Enrollment/firing have scheduled catch-ups — only escalate after they've run.
    elif now.hour >= MORNING_PIPELINE_CHECK_HOUR:
        if enrolled == 0:
            problem = (f"{prefetched} customers prefetched but 0 ENROLLED by "
                       f"{now.strftime('%H:%M')} IST — the enrollment poller failed "
                       "or matched nobody (cron TZ? lock? segment mismatch?). NO calls today.")
        elif fired == 0:
            problem = (f"{enrolled} enrolled but 0 real FIRED by {now.strftime('%H:%M')} "
                       "IST — the scheduler is not firing (kill-switch? gate? CT creds? "
                       "shadow latch?). Calls are stuck.")
    if problem is None:
        return []
    body = (
        f"VB Collections morning-health FAILURE ({today}):\n\n"
        f"  prefetched(found) = {prefetched}\n"
        f"  enrolled today    = {enrolled}\n"
        f"  fired today       = {fired}\n\n"
        f"{problem}\n\n"
        "Self-cure + the retry crons (enrollment 08:30/10:00/12:00, fetch "
        "fallback 08:15, executor/scheduler */5) auto-retry — but this has NOT "
        "recovered by the check time. Manual check needed: ssh the server, see "
        "logs/{fetch_dpd1,prefetch,enrollment,scheduler}_<date>.log and "
        "docs/runbook.md.")
    return [Alert(
        condition="F_MORNING_HEALTH",
        severity="P0",
        subject=f"[P0] VB Collections morning DID NOT FIRE — {today}",
        body=body,
        details={"prefetched": prefetched, "enrolled": enrolled, "fired": fired},
    )]


# detect_g capacity-saturation: a large unfired PENDING backlog late in the
# window means the 750/hr cap can't clear today's queue (excess rolls forward).
CAPACITY_CHECK_HOUR = 16
CAPACITY_BACKLOG_THRESHOLD = int(os.environ.get("WF_CAPACITY_ALERT_BACKLOG", "1500"))


def detect_g_capacity_saturation(conn: sqlite3.Connection, now: datetime) -> list[Alert]:
    """P2 throughput signal (not a failure — excess calls roll to tomorrow, not
    lost). Late in the call window, a large unfired PENDING backlog means the
    1-30 base has outgrown the daily capacity (cap 750/hr). Recurring daily =
    raise WF_HOURLY_CALL_CAP or widen the window. Only after CAPACITY_CHECK_HOUR."""
    if now.hour < CAPACITY_CHECK_HOUR:
        return []
    # No schema guard here (unlike detect_f): a missing wf_pending_actions is
    # caught by run()'s OperationalError skip. That silent-skip is acceptable for
    # this P2 advisory because the SAME table absence already trips detect_f's
    # F_MORNING_SCHEMA P1 — the partial-schema blind spot is covered by a sibling.
    pending = conn.execute(
        "SELECT COUNT(*) FROM wf_pending_actions WHERE status='PENDING'"
    ).fetchone()[0]
    if pending < CAPACITY_BACKLOG_THRESHOLD:
        return []
    hours_left = max(0, 19 - now.hour)
    return [Alert(
        condition="G_CAPACITY_SATURATION",
        severity="P2",
        subject=f"[P2] VB Collections capacity — {pending} calls unfired, ~{hours_left}h left",
        body=(f"{pending} calls are queued (PENDING) at {now.strftime('%H:%M')} IST "
              f"with ~{hours_left}h of call window left (cap 750/hr → ~{hours_left * 750} "
              "more possible today). The excess rolls forward to tomorrow on each "
              "customer's call-day (NOT lost). If this recurs daily, the 1-30 base "
              "has outgrown the daily call capacity — raise WF_HOURLY_CALL_CAP or "
              "widen the window."),
        details={"pending": pending, "hours_left": hours_left},
    )]


_HEARTBEAT_LIB = os.environ.get("WF_HEARTBEAT_LIB", "/home/ubuntu/heartbeat_lib.py")


def _emit_pipeline_health(failed: bool, summary: str) -> None:
    """Push a consolidated pipeline-HEALTH heartbeat through the EXISTING ops
    webhook (heartbeat_lib → OPS_WEBHOOK_URL, an off-box Apps Script sheet) —
    the external layer reusing infra already in place, no new account. The
    per-job run.sh heartbeats only say 'the job ran' (always exit 0 for alerts),
    so they'd show all-green even on a 0-call morning; this emits status='down'
    whenever a P0 is active so the external sheet reflects TRUE health, and the
    heartbeat ceasing entirely signals total-server-death. No-op if the lib is
    absent (dev/tests). Best-effort — never raises (heartbeat_lib already
    swallows webhook errors + falls back to a local file)."""
    if not os.path.exists(_HEARTBEAT_LIB):
        return
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("_wf_hb_lib", _HEARTBEAT_LIB)
        if spec is None or spec.loader is None:
            return
        hb = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hb)
        hb.heartbeat("wf_pipeline_health", "down" if failed else "ok",
                     summary=(summary or "all clear")[:200])
    except Exception as exc:  # noqa: BLE001 — external heartbeat is best-effort
        log.warning("pipeline-health heartbeat failed: %s", exc)


def _ping_healthcheck(failed: bool) -> None:
    """EXTERNAL dead-man's-switch. Pings ``WF_HEALTHCHECK_URL`` (e.g. a
    healthchecks.io check) on every non-dry-run alerts pass. If the pings STOP —
    the whole server died, cron was wiped, the box lost power — the external
    service alerts the owner. That is the ONE failure mode the on-box watchdog
    structurally cannot self-report (a dead box can't email). ``/fail`` also
    signals an active P0 so the external check doubles as a failure channel if
    gmail delivery itself breaks. No-op if the URL is unset (dev/tests).
    Best-effort: never raises."""
    url = os.environ.get("WF_HEALTHCHECK_URL", "").strip()
    if not url:
        return
    ping = url.rstrip("/") + ("/fail" if failed else "")
    try:
        import urllib.request
        with urllib.request.urlopen(ping, timeout=10) as resp:  # noqa: S310 — owner-configured URL
            resp.read()
        log.info("healthcheck pinged: %s", ping)
    except Exception as exc:  # noqa: BLE001 — liveness ping is best-effort, must never crash the watchdog
        log.warning("healthcheck ping failed (%s): %s", ping, exc)


_DETECTORS = (
    detect_a_daemon_down,
    detect_b_run_error,
    detect_c_run_waiting,
    detect_d_kill_switch,
    detect_e_queue_buildup,
    detect_f_morning_health,
    detect_g_capacity_saturation,
)


# ---------------------------------------------------------------- orchestration


_GMAIL_CONFIG = os.environ.get("WF_GMAIL_CONFIG", "/home/ubuntu/loan_closure/config_gmail.json")


def _send_alert_email(payload: dict) -> bool:
    """Actually EMAIL an alert to the owner. alerts.py was log-only ("Phase 8.5
    no SMTP") — which means an unattended failure was invisible. This sends it.
    Gated by WF_ALERTS_SEND=1 (off in tests/dev; on in the server cron) so the
    test suite never emails. Best-effort: a send failure is logged, never raises
    (the alert is still logged + cooldown-marked regardless). Reuses the same
    gmail config as the other emailers (governance: smtplib in a deployed cron
    script is allowed; the guard only blocks ad-hoc Bash)."""
    if os.environ.get("WF_ALERTS_SEND", "0") != "1":
        return False
    try:
        with open(_GMAIL_CONFIG) as f:
            gcfg = json.load(f)
        sender = gcfg["user"]
        msg = MIMEText(payload["body"], _charset="utf-8")
        msg["Subject"] = payload["subject"]
        msg["From"] = f"VB Collections Alerts <{sender}>"
        msg["To"] = payload["to"]
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as server:
            server.starttls()
            server.login(sender, gcfg["password"])
            server.sendmail(sender, [payload["to"]], msg.as_string())
        log.warning("alert EMAILED: %s → %s", payload["subject"], payload["to"])
        return True
    except Exception as exc:  # noqa: BLE001 — alert delivery is best-effort; never crash the watchdog
        log.error("alert email FAILED (%s): %s — alert still logged", payload["subject"], exc)
        return False


def run(workflow_db_path: str | Path, dry_run: bool = False) -> dict:
    """Read workflow.db; run all 6 detectors; de-dupe against cooldown;
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
            "alerts_emailed": 0,
            "alerts_active_p0": False,
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
        emailed = 0
        # Active P0 = emitted OR cooldown-suppressed-this-run. Drives the
        # healthcheck /fail signal so it doesn't flap back to OK while a P0
        # condition persists (just suppressed by its 60-min cooldown).
        active_p0 = False
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
                if alert.severity == "P0":
                    active_p0 = True
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
                    # WS12 unattended-ops: actually deliver the alert. Gated by
                    # WF_ALERTS_SEND=1 (server cron only). Best-effort — a send
                    # failure never raises; the alert is already logged +
                    # cooldown-marked, and the cooldown prevents re-email spam.
                    if _send_alert_email(alert.as_email_payload()):
                        emailed += 1

        # External dead-man's-switch: prove the watchdog is alive (and signal a
        # persisting P0). If these pings stop, the box itself is dead and the
        # external service alerts — the one thing on-box monitoring can't do.
        if not dry_run:
            health_summary = "; ".join(a.subject for a in emitted) if emitted else "all clear"
            _ping_healthcheck(active_p0)
            _emit_pipeline_health(active_p0, health_summary)

        return {
            "alerts_emitted": len(emitted),
            "alerts_skipped_cooldown": skipped,
            "alerts_emailed": emailed,
            "alerts_active_p0": active_p0,
            "alerts": [a.as_email_payload() for a in emitted],
            "parse_failures": len(_PARSE_FAILURES),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- CLI


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workflow.alerts",
        description="Alert watcher. Reads state/workflow.db; emits alerts and, "
        "when WF_ALERTS_SEND=1, EMAILS them to the owner.",
    )
    p.add_argument(
        "--workflow-db",
        required=True,
        help="Path to state/workflow.db",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not update the alert_state cooldown table AND do not email; "
        "alerts are only logged. (Note: a non-dry-run with WF_ALERTS_SEND=1 "
        "sends real email.)",
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
