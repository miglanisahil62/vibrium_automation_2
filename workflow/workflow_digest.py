"""Phase 9 — daily summary digest for the workflow system.

Generates an operator-facing summary of the previous 24h:
  * per-workflow: active / waiting / done / erroring >24h / orphaned counts
  * per-daemon: tick count / error count / last-seen heartbeat
  * shadow-mode runs (counted separately so live + shadow don't conflate)

Payload-only. NEVER calls smtplib — the email body is built and returned
in the result dict; real SMTP send is gated behind a config flag + Sahil
approval per the SMTP governance rule (CLAUDE.md feedback_smtp_governance_block).

Cron via launchd: ``com.sahil.workflow.digest.plist`` — daily at 09:00 IST.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger(__name__)

DIGEST_TO = "sahil.miglani@stashfin.com"
DIGEST_FROM = "vibrium-workflow@stashfin.com"

# Stuck-in-ERROR threshold (matches Phase 8.5's detector B).
ERROR_STUCK_HOURS = 24
# Stuck-in-WAITING threshold (matches Phase 8.5's detector C).
WAITING_STUCK_DAYS = 7


def _now_ist() -> datetime:
    return datetime.now(IST)


def _now_ist_str() -> str:
    return _now_ist().strftime("%Y-%m-%d %H:%M:%S")


def _per_workflow_stats(cn: sqlite3.Connection) -> list[dict[str, Any]]:
    """For each ACTIVE/PAUSED workflow, count runs by status."""
    rows = cn.execute(
        "SELECT w.id, w.name, w.status, w.active_version_id "
        "FROM workflows w WHERE w.status IN ('ACTIVE','PAUSED') "
        "ORDER BY w.id"
    ).fetchall()
    now = _now_ist()
    error_cutoff = (now - timedelta(hours=ERROR_STUCK_HOURS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    waiting_cutoff = (now - timedelta(days=WAITING_STUCK_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    out: list[dict[str, Any]] = []
    for wf_id, name, status, active_v in rows:
        # NOTE: SQLite doesn't enforce uniqueness of status values across
        # tables here; we do simple COUNTs per category.
        counts = {
            row[0]: row[1]
            for row in cn.execute(
                "SELECT status, COUNT(*) FROM workflow_runs "
                "WHERE workflow_id=? GROUP BY status",
                (wf_id,),
            ).fetchall()
        }
        erroring_stuck = cn.execute(
            "SELECT COUNT(*) FROM workflow_runs "
            "WHERE workflow_id=? AND status='ERROR' "
            "AND COALESCE(updated_at_ist, entered_node_at_ist) < ?",
            (wf_id, error_cutoff),
        ).fetchone()[0]
        waiting_stuck = cn.execute(
            "SELECT COUNT(*) FROM workflow_runs "
            "WHERE workflow_id=? AND status='WAITING' "
            "AND COALESCE(entered_node_at_ist, updated_at_ist) < ?",
            (wf_id, waiting_cutoff),
        ).fetchone()[0]
        out.append({
            "workflow_id": wf_id,
            "name": name,
            "status": status,
            "active_version_id": active_v,
            "counts": counts,
            "erroring_gt_24h": erroring_stuck,
            "waiting_gt_7d": waiting_stuck,
        })
    return out


def _per_daemon_stats(cn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Per-daemon ticks/errors over the last 24h + last-seen heartbeat."""
    cutoff = (_now_ist() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    agents = [r[0] for r in cn.execute(
        "SELECT DISTINCT agent FROM wf_agent_events WHERE ts_ist >= ? "
        "ORDER BY agent",
        (cutoff,),
    ).fetchall()]

    out: list[dict[str, Any]] = []
    for agent in agents:
        ticks = cn.execute(
            "SELECT COUNT(*) FROM wf_agent_events "
            "WHERE agent=? AND ts_ist >= ?",
            (agent, cutoff),
        ).fetchone()[0]
        downs = cn.execute(
            "SELECT COUNT(*) FROM wf_agent_events "
            "WHERE agent=? AND status='down' AND ts_ist >= ?",
            (agent, cutoff),
        ).fetchone()[0]
        last_seen = cn.execute(
            "SELECT MAX(ts_ist) FROM wf_agent_events WHERE agent=?",
            (agent,),
        ).fetchone()[0]
        out.append({
            "agent": agent,
            "ticks_24h": ticks,
            "downs_24h": downs,
            "last_seen_ist": last_seen,
        })
    return out


def _shadow_run_count(cn: sqlite3.Connection) -> int:
    """Count workflow_runs that landed shadow-fired pending_actions in 24h."""
    cutoff = (_now_ist() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    return cn.execute(
        "SELECT COUNT(DISTINCT run_id) FROM wf_pending_actions "
        "WHERE status='SHADOW_FIRED' AND last_attempt_at_ist >= ?",
        (cutoff,),
    ).fetchone()[0]


def _build_email_body(
    workflow_stats: list[dict[str, Any]],
    daemon_stats: list[dict[str, Any]],
    shadow_runs: int,
    generated_at_ist: str,
) -> str:
    """Render the operator-facing summary as plain text. No HTML — Sahil's
    pattern across the tree is plain-text email."""
    lines = [
        f"Vibrium Workflow — daily digest",
        f"Generated: {generated_at_ist} IST",
        "",
        f"=== Per-workflow ({len(workflow_stats)} active/paused) ===",
    ]
    if not workflow_stats:
        lines.append("(no active workflows)")
    for wf in workflow_stats:
        counts_str = ", ".join(f"{k}={v}" for k, v in sorted(wf["counts"].items()))
        lines.append(
            f"  [{wf['workflow_id']}] {wf['name']} ({wf['status']}, "
            f"v{wf['active_version_id']}): {counts_str or '(no runs)'}"
        )
        if wf["erroring_gt_24h"]:
            lines.append(f"    ⚠ {wf['erroring_gt_24h']} ERROR > 24h")
        if wf["waiting_gt_7d"]:
            lines.append(f"    ⚠ {wf['waiting_gt_7d']} WAITING > 7d")
    lines.append("")
    lines.append(f"=== Per-daemon (24h) ===")
    if not daemon_stats:
        lines.append("(no heartbeats in last 24h — system may be paused or down)")
    for d in daemon_stats:
        marker = " ⚠" if d["downs_24h"] else ""
        lines.append(
            f"  {d['agent']}: {d['ticks_24h']} ticks, "
            f"{d['downs_24h']} downs, last_seen={d['last_seen_ist']}{marker}"
        )
    lines.append("")
    lines.append(f"=== Shadow runs (24h) ===")
    lines.append(f"  {shadow_runs} runs with SHADOW_FIRED pending_actions")
    lines.append("")
    lines.append("Detail: query workflow.db directly or see /workflows in ops console.")
    return "\n".join(lines)


def run(
    *,
    workflow_db_path: Path,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Build and return the digest email payload. No SMTP send.

    Returns:
        dict with keys: to, from, subject, body, generated_at_ist,
        workflow_count, daemon_count, shadow_runs, dry_run.
    """
    cn = sqlite3.connect(str(workflow_db_path))
    try:
        wf_stats = _per_workflow_stats(cn)
        d_stats = _per_daemon_stats(cn)
        shadow = _shadow_run_count(cn)
    finally:
        cn.close()

    generated = _now_ist_str()
    body = _build_email_body(wf_stats, d_stats, shadow, generated)
    subject = f"Vibrium Workflow daily digest — {generated[:10]}"

    payload = {
        "to": DIGEST_TO,
        "from": DIGEST_FROM,
        "subject": subject,
        "body": body,
        "generated_at_ist": generated,
        "workflow_count": len(wf_stats),
        "daemon_count": len(d_stats),
        "shadow_runs": shadow,
        "dry_run": dry_run,
    }

    if dry_run:
        log.info("digest dry_run — payload built but not sent")
    else:
        # SMTP-send is gated behind feedback_smtp_governance_block — the
        # daemon builds the payload; an out-of-band cron + sendmail will
        # deliver it once Sahil authorises. Stamping a log line is enough
        # for now.
        log.info(
            "digest payload built (smtp send gated): subject=%r body_len=%d",
            subject, len(body),
        )
    return payload


def _main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Phase 9 daily digest builder.")
    parser.add_argument("--workflow-db", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        result = run(workflow_db_path=args.workflow_db, dry_run=args.dry_run)
        print(json.dumps({k: v for k, v in result.items() if k != "body"},
                         indent=2))
        return 0
    except sqlite3.Error as exc:
        log.exception("digest sqlite error: %s", exc)
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(_main())
