#!/usr/bin/env python3
"""Email today's VB bot-assigned customers to the collections team.

Triggered each time a customer is routed to ASSIGN_AGENT — or run as a
scheduled job (e.g. every 2 hours during the call window) to batch-report
all assignments since the last send.

Sends to: sahil.miglani@stashfin.com, ishita.goyal@stashfin.com
Attaches: CSV of today's agent_assignments rows with run context
Subject:  [VB Bot] N customers assigned to agent — <date> [<reason breakdown>]

Uses the same gmail config as the adhoc emailer:
    /home/ubuntu/loan_closure/config_gmail.json

CLI:
    python3 scripts/agent_assignment_emailer.py
    python3 scripts/agent_assignment_emailer.py --dry-run      # print; do not send
    python3 scripts/agent_assignment_emailer.py --since 08:00  # only rows from 08:00 IST today
    python3 scripts/agent_assignment_emailer.py --all-time     # ignore date filter (for backfill)

Cron entry (every 2 hours during call window, AWS UTC):
    0 4,6,8,10,12 * * *  /home/ubuntu/vibrium-workflow/run.sh agent_assignment_emailer
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import smtplib
import sqlite3
import sys
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(Path(__file__).stem)

RECIPIENTS = [
    "sahil.miglani@stashfin.com",
    "ishita.goyal@stashfin.com",
]

GMAIL_CONFIG = "/home/ubuntu/loan_closure/config_gmail.json"

REASON_LABELS = {
    "max_attempts_reached":     "Max attempts",
    "dispute_or_nrp":           "Dispute / NRP",
    "disposition_timeout_24h":  "24h timeout",
    "rtp_needs_review":         "RTP review",
    "unhandled_disposition":    "Unhandled",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--gmail-config", default=GMAIL_CONFIG)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print report; do not send email.")
    ap.add_argument("--since", default=None,
                    help="Only rows assigned at or after HH:MM IST today (e.g. 08:00).")
    ap.add_argument("--all-time", action="store_true",
                    help="Include all rows regardless of date (backfill / testing).")
    return ap.parse_args()


def _fetch_assignments(
    db_path: str,
    since_ist: "str | None",
    all_time: bool,
) -> list[dict]:
    """Pull agent_assignments joined to workflow_runs for context."""
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row

        today = datetime.now(IST).strftime("%Y-%m-%d")
        if all_time:
            date_filter = "1=1"
            params: list = []
        elif since_ist:
            cutoff = f"{today} {since_ist}"
            date_filter = "aa.assigned_at_ist >= ?"
            params = [cutoff]
        else:
            date_filter = "substr(aa.assigned_at_ist, 1, 10) = ?"
            params = [today]

        sql = (
            "SELECT "
            "  aa.id, aa.customer_id, aa.reason, aa.assigned_at_ist, aa.run_id, "
            "  wr.terminal_status, wr.enrolled_at_ist, wr.terminated_at_ist, "
            "  json_extract(wr.scratchpad_json, '$.coll_bot_calling') AS bot_calling, "
            "  json_extract(wr.scratchpad_json, '$.coll_collection_risk_segmentation') AS risk_seg, "
            "  json_extract(wr.scratchpad_json, '$.coll_notification_replied') AS wa_status, "
            "  json_extract(wr.scratchpad_json, '$.dpd') AS dpd, "
            "  json_extract(wr.scratchpad_json, '$.attempts') AS attempts "
            "FROM agent_assignments aa "
            "LEFT JOIN workflow_runs wr ON wr.id = aa.run_id "
            "WHERE " + date_filter +
            " ORDER BY aa.assigned_at_ist DESC"
        )
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        if conn is not None:
            conn.close()


def _reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def _build_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    fieldnames = [
        "customer_id", "assigned_at_ist", "reason_label", "reason_code",
        "dpd", "risk_seg", "wa_status", "bot_calling",
        "attempts_before_assign", "terminal_status",
        "enrolled_at_ist", "terminated_at_ist", "run_id",
    ]
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({
            "customer_id":          r.get("customer_id", ""),
            "assigned_at_ist":      r.get("assigned_at_ist", ""),
            "reason_label":         _reason_label(r.get("reason", "")),
            "reason_code":          r.get("reason", ""),
            "dpd":                  r.get("dpd", ""),
            "risk_seg":             r.get("risk_seg", ""),
            "wa_status":            r.get("wa_status", ""),
            "bot_calling":          r.get("bot_calling", ""),
            "attempts_before_assign": r.get("attempts", 0) or 0,
            "terminal_status":      r.get("terminal_status", ""),
            "enrolled_at_ist":      r.get("enrolled_at_ist", ""),
            "terminated_at_ist":    r.get("terminated_at_ist", ""),
            "run_id":               r.get("run_id", ""),
        })
    return buf.getvalue().encode("utf-8")


def _build_subject(rows: list[dict], date_str: str) -> str:
    n = len(rows)
    if n == 0:
        return f"[VB Bot] No new agent assignments — {date_str}"
    # Reason breakdown for subject clarity
    counts: dict[str, int] = {}
    for r in rows:
        lbl = _reason_label(r.get("reason", "other"))
        counts[lbl] = counts.get(lbl, 0) + 1
    breakdown = " | ".join(f"{lbl}: {cnt}" for lbl, cnt in sorted(counts.items(), key=lambda x: -x[1]))
    return f"[VB Bot] {n} customer{'s' if n != 1 else ''} assigned to agent — {date_str} ({breakdown})"


def _build_html(rows: list[dict], date_str: str) -> str:
    n = len(rows)
    if n == 0:
        return (
            f"<p>No customers were assigned to agent on {date_str}.</p>"
            "<p style='color:#888;font-size:12px'>This email was sent by the VB Collections workflow.</p>"
        )

    reason_counts: dict[str, int] = {}
    for r in rows:
        lbl = _reason_label(r.get("reason", "other"))
        reason_counts[lbl] = reason_counts.get(lbl, 0) + 1

    summary_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{lbl}</td>"
        f"<td style='padding:4px 8px;text-align:center'><b>{cnt}</b></td></tr>"
        for lbl, cnt in sorted(reason_counts.items(), key=lambda x: -x[1])
    )
    summary = (
        "<h3 style='margin-bottom:6px'>Summary</h3>"
        "<table border='1' cellpadding='2' cellspacing='0' "
        "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px'>"
        "<tr style='background:#f0f0f0'><th style='padding:4px 8px'>Reason</th>"
        "<th style='padding:4px 8px'>Count</th></tr>"
        + summary_rows + "</table>"
    )

    detail_rows = "".join(
        "<tr>"
        f"<td style='padding:4px 6px'>{r.get('customer_id','')}</td>"
        f"<td style='padding:4px 6px'>{r.get('assigned_at_ist','')}</td>"
        f"<td style='padding:4px 6px'>{_reason_label(r.get('reason',''))}</td>"
        f"<td style='padding:4px 6px;text-align:center'>{r.get('dpd') or '—'}</td>"
        f"<td style='padding:4px 6px;text-align:center'>{r.get('risk_seg') or '—'}</td>"
        f"<td style='padding:4px 6px;text-align:center'>{r.get('attempts') or 0}</td>"
        f"<td style='padding:4px 6px;font-size:11px;color:#555'>{r.get('bot_calling') or '—'}</td>"
        "</tr>"
        for r in rows
    )
    detail = (
        "<h3 style='margin-bottom:6px;margin-top:16px'>Customer detail</h3>"
        "<p style='font-size:12px;color:#888'>Full data in attached CSV.</p>"
        "<table border='1' cellpadding='2' cellspacing='0' "
        "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:12px'>"
        "<thead><tr style='background:#f0f0f0'>"
        "<th style='padding:4px 6px'>Customer ID</th>"
        "<th style='padding:4px 6px'>Assigned at (IST)</th>"
        "<th style='padding:4px 6px'>Reason</th>"
        "<th style='padding:4px 6px'>DPD</th>"
        "<th style='padding:4px 6px'>Risk seg</th>"
        "<th style='padding:4px 6px'>Calls made</th>"
        "<th style='padding:4px 6px'>Bot calling value</th>"
        "</tr></thead><tbody>"
        + detail_rows
        + "</tbody></table>"
    )

    return (
        f"<p style='font-size:14px'>The VB Collections bot assigned <b>{n} customer"
        f"{'s' if n!=1 else ''}</b> to human agent on <b>{date_str}</b>.</p>"
        + summary + detail +
        "<p style='margin-top:16px;font-size:11px;color:#aaa'>"
        "Source: VB Collections v2 workflow (workflow_id=1) | "
        "Reply-to: sahil.miglani@stashfin.com</p>"
    )


def _send_email(
    gmail_config_path: str,
    subject: str,
    html_body: str,
    csv_bytes: bytes,
    csv_filename: str,
) -> None:
    with open(gmail_config_path) as f:
        gcfg = json.load(f)
    sender = gcfg["user"]
    msg = MIMEMultipart()
    msg["From"] = f"VB Collections Bot <{sender}>"
    msg["To"] = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{csv_filename}"')
    msg.attach(part)

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
        server.starttls()
        server.login(sender, gcfg["password"])
        server.sendmail(sender, RECIPIENTS, msg.as_string())
    log.info("sent: %r → %s", subject, RECIPIENTS)


def main() -> None:
    args = parse_args()
    now = datetime.now(IST)
    date_str = now.strftime("%Y-%m-%d")
    csv_filename = f"vb_agent_assignments_{date_str}.csv"

    log.info("fetching assignments since=%s all_time=%s", args.since, args.all_time)
    rows = _fetch_assignments(args.workflow_db, args.since, args.all_time)
    log.info("%d assignment rows found", len(rows))

    subject = _build_subject(rows, date_str)
    html_body = _build_html(rows, date_str)
    csv_bytes = _build_csv(rows)

    if args.dry_run:
        log.info("DRY-RUN — subject: %s", subject)
        log.info("DRY-RUN — %d rows, %d bytes CSV", len(rows), len(csv_bytes))
        print(f"\nSubject: {subject}")
        print(f"To: {', '.join(RECIPIENTS)}")
        print(f"CSV rows: {len(rows)}")
        if rows:
            print("\nFirst row sample:")
            for k, v in list(rows[0].items())[:8]:
                print(f"  {k}: {v}")
        return

    if len(rows) == 0:
        log.info("no assignments today — skipping email")
        return

    _send_email(args.gmail_config, subject, html_body, csv_bytes, csv_filename)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("agent_assignment_emailer failed: %s", exc)
        sys.exit(1)
