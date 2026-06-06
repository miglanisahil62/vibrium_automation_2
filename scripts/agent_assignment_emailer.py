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
import html
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
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
IST = ZoneInfo("Asia/Kolkata")

# WS10: the CT profile property + value written for exhausted-still-due
# customers, and the ONLY assignment reason that qualifies for it. Disputes,
# timeouts and review-exits are DIFFERENT handoffs and must NOT be flagged for
# agent calling — the CRITICAL GUARD from the plan ("NEVER set it for customers
# who never qualified"). Only a customer who completed their full VB call-day
# budget and is still due (reason='max_attempts_reached') is agent-recommended.
CT_ALLOCATION_PROPERTY = "coll_agent_allocation"
CT_ALLOCATION_VALUE = "Agent_calling_Recommended"
CT_QUALIFYING_REASON = "max_attempts_reached"

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
    "loop_error":               "Loop error (engine)",
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
    ap.add_argument("--ct-creds", default=None,
                    help="CleverTap creds JSON path (default: CT_CREDS_FILE env "
                         "→ ~/Collections_v3/Clevertap campaigns/config_CT_credentials.json).")
    ap.add_argument("--no-ct-write", action="store_true",
                    help="Skip the coll_agent_allocation CT write (email only).")
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

        # calls_made = real connect-attempts that WENT OUT for this run
        # (FIRED/SHADOW_FIRED/FIRED_RECOVERED), NOT scratchpad 'attempts' — v8
        # counts fires in 'fire_seq', so reading 'attempts' wrongly showed 0 for
        # every v8 assignment. calls_refused = attempts the gate REFUSED
        # (SUPPRESSED/ERROR: DND, daily cap, 08:00-19:00 window). A customer the
        # bot could never reach (all-refused) still legitimately reaches
        # max_attempts_reached with calls_made=0 — and is the HIGHEST-signal
        # handoff, so we keep + FLAG it, never drop it (master-auditor P0).
        sql = (
            "SELECT "
            "  aa.id, aa.customer_id, aa.reason, aa.assigned_at_ist, aa.run_id, "
            "  wr.terminal_status, wr.enrolled_at_ist, wr.terminated_at_ist, "
            "  json_extract(wr.scratchpad_json, '$.coll_bot_calling') AS bot_calling, "
            "  json_extract(wr.scratchpad_json, '$.coll_collection_risk_segmentation') AS risk_seg, "
            "  json_extract(wr.scratchpad_json, '$.coll_notification_replied') AS wa_status, "
            "  json_extract(wr.scratchpad_json, '$.dpd') AS dpd, "
            "  json_extract(wr.scratchpad_json, '$.attempts') AS attempts, "
            "  (SELECT COUNT(*) FROM wf_pending_actions p WHERE p.run_id = aa.run_id "
            "     AND p.status IN ('FIRED','SHADOW_FIRED','FIRED_RECOVERED')) AS calls_made, "
            "  (SELECT COUNT(*) FROM wf_pending_actions p WHERE p.run_id = aa.run_id "
            "     AND p.status IN ('SUPPRESSED','ERROR')) AS calls_refused "
            "FROM agent_assignments aa "
            "LEFT JOIN workflow_runs wr ON wr.id = aa.run_id "
            "WHERE " + date_filter +
            " ORDER BY aa.assigned_at_ist DESC"
        )
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        # ONE guard: drop only the legacy v3/v4 'disposition_timeout_24h' reason.
        # v8 RETIRED it (the 60-min re-evaluate re-queues a no-disposition call
        # instead of escalating), so it's dead going forward and today's batch
        # are customers v8 is already re-calling. v8's real reasons —
        # dispute_or_nrp / rtp_needs_review / max_attempts_reached — are all kept,
        # INCLUDING calls_made=0 (all-refused) ones, which are flagged below.
        legacy_dead = {"disposition_timeout_24h"}
        kept = [r for r in rows if r.get("reason") not in legacy_dead]
        drop_legacy = len(rows) - len(kept)
        never_reached = sum(1 for r in kept if (r.get("calls_made") or 0) == 0)
        if drop_legacy or never_reached:
            log.warning(
                "agent_assignment_emailer: %d assignment(s) → kept %d "
                "(of which %d never-reached/all-refused — flagged, NOT dropped); "
                "dropped %d legacy disposition_timeout_24h (v8 re-calls those)",
                len(rows), len(kept), never_reached, drop_legacy,
            )
        return kept
    finally:
        if conn is not None:
            conn.close()


def _reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def _calls_cell(r: dict) -> str:
    """'Calls made' cell. A 0 here is NOT 'never tried' — it means every fire
    was gate-refused (DND / daily cap / outside 08:00-19:00), i.e. the bot could
    not reach them. Flag it so the agent knows it's a high-priority unreached
    customer, not a no-op."""
    made = r.get("calls_made") or 0
    if made > 0:
        return str(made)
    refused = r.get("calls_refused") or 0
    return f"0 ⚠ unreached ({refused} refused)" if refused else "0 ⚠ unreached"


def _build_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    fieldnames = [
        "customer_id", "assigned_at_ist", "reason_label", "reason_code",
        "dpd", "risk_seg", "wa_status", "bot_calling",
        "calls_made", "calls_refused", "attempts_before_assign", "terminal_status",
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
            "calls_made":           r.get("calls_made", 0) or 0,
            "calls_refused":        r.get("calls_refused", 0) or 0,
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
        f"<td style='padding:4px 6px;text-align:center'>{_calls_cell(r)}</td>"
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


def _write_ct_allocation(
    db_path: str,
    *,
    dry_run: bool,
    creds_path: "str | None",
) -> dict:
    """WS10 — write ``coll_agent_allocation = Agent_calling_Recommended`` to CT
    for each EXHAUSTED-still-due customer.

    Scope (the CRITICAL GUARD): only ``agent_assignments`` rows with
    ``reason = 'max_attempts_reached'`` AND ``ct_allocation_written_at_ist IS
    NULL`` — i.e. customers who actually completed their VB call-day budget and
    are still due, and whom we have NOT already flagged. ``max_attempts_reached``
    is a CLEAN terminal: engine faults in the call loop route to a SEPARATE
    ``loop_error`` ASSIGN node, so an error-routed customer can never inherit the
    auto-recommend flag (see generate_vb_collections_v2 ASSIGN_LOOP_ERR). This is
    a targeted per-ID profile write (never a segment/broad write), sourced from
    the authoritative table, so a customer who never qualified can never be
    touched.

    Delivery is **at-least-once, idempotent at CT** (NOT strictly exactly-once):
    a successful CT write stamps ``ct_allocation_written_at_ist`` so the
    every-2h cron normally never re-writes. The one residual re-write window is a
    crash AFTER CT persists but BEFORE the per-customer commit lands — the next
    pass would re-write that one customer. That is harmless: writing the same
    scalar property value again is a CT no-op (master-auditor WS10 P1-2). A
    pre-write reservation would only trade this for the worse "stamped-but-not-
    written" failure, so we accept at-least-once.

    ``dry_run`` validates the CT payload (CT ``?dryRun=1``) and writes NEITHER CT
    nor the DB marker. A per-ID CT failure is logged + counted and left unstamped
    (retried next pass) — it never aborts the batch or the email.

    Returns {qualified, attempted, written, failed, skipped_dupe}.
    """
    from workflow import clevertap_profile

    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        # Unwritten qualified rows, any date — the NULL marker is the idempotency
        # guard, and scanning all-unwritten self-heals a day the job missed.
        # DISTINCT customer_id: agent_assignments has no UNIQUE constraint, so a
        # customer with two assignment rows must be written (and stamped) once.
        rows = conn.execute(
            "SELECT id, customer_id FROM agent_assignments "
            "WHERE reason = ? AND ct_allocation_written_at_ist IS NULL "
            "ORDER BY id",
            (CT_QUALIFYING_REASON,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        # Column absent → migration 004 not applied. Loud, but non-fatal to the
        # email (the handoff CSV still goes out); surface so it gets fixed.
        log.error("ct_allocation write skipped — DB not migrated (%s)", exc)
        if conn is not None:
            conn.close()
        return {"qualified": 0, "attempted": 0, "written": 0,
                "failed": 0, "skipped_dupe": 0, "error": str(exc)}

    # Collapse to one write per customer_id; remember every row id to stamp.
    ids_by_cid: dict = {}
    for r in rows:
        ids_by_cid.setdefault(str(r["customer_id"]), []).append(r["id"])
    qualified = len(ids_by_cid)
    skipped_dupe = len(rows) - qualified

    stats = {"qualified": qualified, "attempted": 0, "written": 0,
             "failed": 0, "skipped_dupe": skipped_dupe}
    if qualified == 0:
        conn.close()
        log.info("ct_allocation: 0 unwritten qualified (reason=%s) customers",
                 CT_QUALIFYING_REASON)
        return stats

    log.info("ct_allocation: %d qualified customer(s) to flag %s=%s (dry_run=%s)",
             qualified, CT_ALLOCATION_PROPERTY, CT_ALLOCATION_VALUE, dry_run)

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    for cid, row_ids in ids_by_cid.items():
        stats["attempted"] += 1
        try:
            res = clevertap_profile.set_profile(
                cid,
                {CT_ALLOCATION_PROPERTY: CT_ALLOCATION_VALUE},
                dry_run=dry_run,
                creds_path=Path(creds_path) if creds_path else None,
            )
        except Exception as exc:  # noqa: BLE001 — one bad id must not abort the batch
            stats["failed"] += 1
            log.warning("ct_allocation FAILED cid=%s (%s) — left unstamped, "
                        "will retry next pass", cid, exc)
            continue
        if not res.success:
            stats["failed"] += 1
            log.warning("ct_allocation rejected cid=%s code=%s — left unstamped",
                        cid, res.error_code)
            continue
        stats["written"] += 1
        if not dry_run:
            # Stamp every assignment row for this customer so a duplicate row
            # can't re-trigger the write on a later pass.
            conn.executemany(
                "UPDATE agent_assignments SET ct_allocation_written_at_ist = ? "
                "WHERE id = ?",
                [(now_ist, rid) for rid in row_ids],
            )
            conn.commit()

    conn.close()
    log.info("ct_allocation done: %s", stats)
    return stats


def _ct_status_html(ct_stats: "dict | None") -> str:
    """Small CT-write status block for the email (WS10 P1-3 — visibility)."""
    if not ct_stats:
        return ""
    failed = ct_stats.get("failed", 0)
    err = ct_stats.get("error")
    written = ct_stats.get("written", 0)
    if err:
        return (
            "<div style='margin-top:16px;padding:12px 16px;background:#f8d7da;"
            "border:2px solid #dc3545;border-radius:4px;color:#721c24'>"
            f"<b>🚨 CT allocation write skipped — {html.escape(str(err))}</b>"
            "<p style='margin:6px 0 0;font-size:12px'>Qualified customers were "
            "NOT flagged <code>coll_agent_allocation</code> this run. Likely "
            "migration 004 not applied. Fix + re-run.</p></div>"
        )
    color, border = ("#721c24", "#dc3545") if failed else ("#155724", "#28a745")
    head = ("🚨 CT allocation — some writes FAILED" if failed
            else "✓ CT allocation written")
    return (
        f"<div style='margin-top:16px;padding:10px 14px;background:#f4f8f4;"
        f"border-left:4px solid {border};border-radius:3px;color:{color};font-size:12px'>"
        f"<b>{head}</b><br>"
        f"coll_agent_allocation=Agent_calling_Recommended · "
        f"qualified={ct_stats.get('qualified', 0)} · written={written} · "
        f"failed={failed} · dup-rows-skipped={ct_stats.get('skipped_dupe', 0)}"
        + ("<br><span style='font-size:11px'>Failed IDs are left un-flagged and "
           "retried on the next pass.</span>" if failed else "")
        + "</div>"
    )


def main() -> None:
    args = parse_args()
    now = datetime.now(IST)
    date_str = now.strftime("%Y-%m-%d")
    csv_filename = f"vb_agent_assignments_{date_str}.csv"

    log.info("fetching assignments since=%s all_time=%s", args.since, args.all_time)
    rows = _fetch_assignments(args.workflow_db, args.since, args.all_time)
    log.info("%d assignment rows found", len(rows))

    # WS10 — flag exhausted-still-due customers for agent calling in CleverTap.
    # Runs BEFORE the email and INDEPENDENTLY of the email's date/since window:
    # it has its own unwritten-qualified scan + idempotency marker, so it fires
    # even on a pass where there are no NEW rows to email. --no-ct-write opts out.
    ct_stats = None
    if not args.no_ct_write:
        ct_stats = _write_ct_allocation(
            args.workflow_db, dry_run=args.dry_run, creds_path=args.ct_creds,
        )

    # WS10 P1-3 — a CT-write rejection must never be invisible. If any per-ID
    # write failed (or the table wasn't migrated), log it at ERROR (so it shows
    # in the failure-marker log tail) and surface it in the email body.
    ct_failed = bool(ct_stats and (ct_stats.get("failed") or ct_stats.get("error")))
    if ct_failed:
        log.error("ct_allocation had failures this pass: %s — qualified customers "
                  "left UN-flagged, will retry next pass", ct_stats)

    subject = _build_subject(rows, date_str)
    html_body = _build_html(rows, date_str) + _ct_status_html(ct_stats)
    csv_bytes = _build_csv(rows)

    if args.dry_run:
        log.info("DRY-RUN — subject: %s", subject)
        log.info("DRY-RUN — %d rows, %d bytes CSV", len(rows), len(csv_bytes))
        print(f"\nSubject: {subject}")
        print(f"To: {', '.join(RECIPIENTS)}")
        print(f"CSV rows: {len(rows)}")
        print(f"CT allocation (dry-run): {ct_stats}")
        if rows:
            print("\nFirst row sample:")
            for k, v in list(rows[0].items())[:8]:
                print(f"  {k}: {v}")
        return

    if len(rows) == 0:
        # No new handoff rows to email. If the CT write flagged earlier-unwritten
        # qualifiers OR hit failures, send a short status email so the CT-write
        # half is never silent; otherwise skip (nothing to report).
        wrote = ct_stats.get("written") if ct_stats else 0
        if ct_failed or wrote:
            _send_email(args.gmail_config,
                        f"[VB Bot] No new agent assignments — {date_str} "
                        f"(CT: {wrote} flagged, {ct_stats.get('failed', 0)} failed)",
                        _build_html(rows, date_str) + _ct_status_html(ct_stats),
                        csv_bytes, csv_filename)
        else:
            log.info("no assignments to email + no CT activity — skipping email")
        return

    _send_email(args.gmail_config, subject, html_body, csv_bytes, csv_filename)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("agent_assignment_emailer failed: %s", exc)
        sys.exit(1)
