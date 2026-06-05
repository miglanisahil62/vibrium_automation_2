#!/usr/bin/env python3
"""Daily morning enrollment funnel email for VB Collections v2.

Runs at ~09:00 IST — AFTER the 07:35 + 08:30 enrollment passes have completed,
so the full ageing-1-30 cohort is enrolled before the funnel is computed. (It
used to run at 07:45, which on a slow/late morning fired before enrollment
finished and mailed a misleading "0 enrolled" funnel; the 0-enrolled health
guard below now flags that case loudly regardless.) Sends a segment-by-segment
funnel of today's cohort to the collections team.

Funnel layers:
  1. Candidates fetched from collection_view (ageing 1-30) → from dated CSV
  2. Enrolled → workflow_runs created today
  3. Entry filter exits (INELIGIBLE / FETCH_FAILED / OUT_OF_SCOPE)
  4. Per-segment counts with calls/timing

Sends to: sahil.miglani@stashfin.com, ishita.goyal@stashfin.com

CLI:
    python3 scripts/enrollment_funnel_emailer.py
    python3 scripts/enrollment_funnel_emailer.py --dry-run
    python3 scripts/enrollment_funnel_emailer.py --date 2026-06-01

Cron (09:00 IST = 03:30 UTC):
    30 3 * * *  /home/ubuntu/vibrium-workflow/run.sh enrollment_funnel_emailer
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import logging
import re
import smtplib
import sqlite3
import sys
from datetime import datetime
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


def _load_segment_meta() -> list[dict]:
    """Derive SEGMENT_META directly from the generator — single source of truth.

    Extracts coll_bot_calling values from each segment's match expression so
    this file never drifts from the live segment registry. Adding a new segment
    to generate_vb_collections_v2.py is automatically reflected here on the
    next run.
    """
    seed_dir = str(_REPO / "seed_workflows")
    if seed_dir not in sys.path:
        sys.path.insert(0, seed_dir)
    try:
        from generate_vb_collections_v2 import SEGMENTS  # type: ignore[import]
    except ImportError as exc:
        log.warning("could not import SEGMENTS from generator: %s — using empty list", exc)
        return []

    _BOT_CALLING_PAT = re.compile(r"coll_bot_calling\s*==\s*['\"]([^'\"]+)['\"]")
    meta = []
    for seg in SEGMENTS:
        m = _BOT_CALLING_PAT.search(seg.get("match", ""))
        meta.append({
            "name":        seg["name"],
            "bot_calling": m.group(1) if m else None,
            "calls":       seg["total_calls"],
            "entry":       f"T+{seg['entry_offset_days']}",
        })
    return meta


SEGMENT_META = _load_segment_meta()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--csv-dir",
                    default=str(_REPO / "state" / "enrollment"))
    ap.add_argument("--gmail-config", default=GMAIL_CONFIG)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--date", default=None,
                    help="Override date (YYYY-MM-DD); default today IST.")
    return ap.parse_args()


def _today(override: "str | None") -> str:
    if override:
        datetime.strptime(override, "%Y-%m-%d")   # validate
        return override
    return datetime.now(IST).strftime("%Y-%m-%d")


def _csv_candidate_count(csv_dir: str, date_str: str) -> "int | None":
    """Return number of customer_ids in today's dated CSV, or None if missing."""
    p = Path(csv_dir) / f"dpd1_candidates_{date_str}.csv"
    if not p.exists():
        return None
    try:
        with open(p, newline="") as f:
            rows = list(csv.reader(f))
        return max(0, len(rows) - 1)   # subtract header
    except OSError as exc:
        log.warning("could not read candidate CSV %s: %s", p, exc)
        return None


def _classify_segment(bot_calling: "str | None",
                       risk_seg: "int | str | None",
                       wa_status: "str | None") -> str:
    """Apply the same first-match-wins logic as the graph classifier."""
    for seg in SEGMENT_META:
        if seg["bot_calling"] is not None:
            if bot_calling == seg["bot_calling"]:
                return seg["name"]
        else:
            # high_nowa fallback: risk < 5 and WA_Unavailable
            try:
                rs = int(risk_seg) if risk_seg is not None else None
            except (ValueError, TypeError):
                rs = None
            if rs is not None and rs < 5 and wa_status == "WA_Unavailable":
                return seg["name"]
    return "unclassified"


def _fetch_funnel(db_path: str, date_str: str) -> dict:
    """Query workflow_runs for today's enrollment breakdown."""
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row

        # All runs enrolled today — include customer_id for OOS alert
        rows = conn.execute(
            "SELECT customer_id, terminal_status, "
            "  json_extract(scratchpad_json, '$.coll_bot_calling')                  AS bot_calling, "
            "  json_extract(scratchpad_json, '$.coll_collection_risk_segmentation') AS risk_seg, "
            "  json_extract(scratchpad_json, '$.coll_notification_replied')         AS wa_status "
            "FROM workflow_runs "
            "WHERE substr(enrolled_at_ist, 1, 10) = ?",
            (date_str,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        if conn is not None:
            conn.close()


def _build_funnel(rows: list[dict],
                  csv_count: "int | None",
                  date_str: str) -> dict:
    """Aggregate rows into funnel layers."""
    total_enrolled = len(rows)

    # Entry filter exits
    exits = {
        "FETCH_FAILED": 0,
        "INELIGIBLE":   0,
        "OUT_OF_SCOPE": 0,
        "ENTRY_WAIT_ERROR": 0,
    }
    for r in rows:
        ts = r.get("terminal_status") or ""
        if ts in exits:
            exits[ts] += 1

    # Segment counts — only runs that are NOT in an exit status
    seg_counts: dict[str, int] = {s["name"]: 0 for s in SEGMENT_META}
    seg_counts["unclassified"] = 0
    for r in rows:
        ts = r.get("terminal_status") or ""
        if ts in exits:
            continue
        seg = _classify_segment(
            r.get("bot_calling"),
            r.get("risk_seg"),
            r.get("wa_status"),
        )
        seg_counts[seg] = seg_counts.get(seg, 0) + 1

    in_segments = sum(seg_counts[s["name"]] for s in SEGMENT_META)

    # OUT_OF_SCOPE detail — group by unrecognised coll_bot_calling value
    oos_detail: dict[str, int] = {}
    for r in rows:
        if (r.get("terminal_status") or "") == "OUT_OF_SCOPE":
            val = r.get("bot_calling") or "(no coll_bot_calling set)"
            oos_detail[val] = oos_detail.get(val, 0) + 1

    # Silent-failure guard (plan: "no more silent 0-enrolled mornings"). If the
    # fetch produced candidates but NOTHING enrolled, the cohort isn't ready —
    # enrollment failed, hasn't run yet, or the funnel fired too early. Flag it
    # LOUDLY in the subject + a banner rather than mailing a calm "0 enrolled".
    health_warning = bool(csv_count and csv_count > 0 and total_enrolled == 0)

    return {
        "date": date_str,
        "csv_count": csv_count,
        "total_enrolled": total_enrolled,
        "exits": exits,
        "seg_counts": seg_counts,
        "oos_detail": oos_detail,
        "in_segments": in_segments,
        "health_warning": health_warning,
    }


def _build_subject(f: dict) -> str:
    n = f["total_enrolled"]
    segs = f["in_segments"]
    if f.get("health_warning"):
        # Loud, unmissable subject so a 0-enrolled morning can't slip by.
        return (
            f"🚨 [VB Collections] 0 ENROLLED — {f['date']} "
            f"({f['csv_count']} candidates fetched, NONE enrolled — check pipeline)"
        )
    oos = f["exits"].get("OUT_OF_SCOPE", 0)
    oos_flag = f" ⚠ {oos} OUT_OF_SCOPE" if oos else ""
    return (
        f"[VB Collections] Enrollment funnel — {f['date']} "
        f"({n} enrolled, {segs} in segments{oos_flag})"
    )


def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "—"
    return f"{num/denom*100:.0f}%"


def _row(label: str, count: "int | None", denom: int = 0,
         indent: int = 0, bold: bool = False, color: str = "") -> str:
    pad = "&nbsp;" * (indent * 4)
    cnt_str = str(count) if count is not None else "—"
    pct_str = f"<span style='color:#888;font-size:11px'>&nbsp;{_pct(count or 0, denom)}</span>" if denom else ""
    style = f"color:{color};" if color else ""
    if bold:
        return (
            f"<tr><td style='padding:4px 10px;{style}'>{pad}<b>{label}</b></td>"
            f"<td style='padding:4px 10px;text-align:right;{style}'><b>{cnt_str}</b>{pct_str}</td></tr>"
        )
    return (
        f"<tr><td style='padding:4px 10px;{style}'>{pad}{label}</td>"
        f"<td style='padding:4px 10px;text-align:right;{style}'>{cnt_str}{pct_str}</td></tr>"
    )


def _build_html(f: dict) -> str:
    csv_c = f["csv_count"]
    enrolled = f["total_enrolled"]
    exits = f["exits"]
    seg_counts = f["seg_counts"]
    date_str = f["date"]

    table_rows = ""

    # Layer 1 — fetch
    if csv_c is not None:
        table_rows += _row("Fetched from collection_view (ageing 1-30)", csv_c, bold=True)
    else:
        table_rows += _row("Fetched from collection_view (ageing 1-30)", None, bold=True)
        table_rows += _row("⚠ CSV not found — fetch may have failed", None,
                           indent=1, color="#c00")

    # Layer 2 — enrolled
    table_rows += _row("Enrolled in workflow", enrolled, denom=csv_c or 0, bold=True)

    # Layer 3 — entry filter exits
    table_rows += _row("── Entry filter exits ──", None)
    fetch_fail = exits["FETCH_FAILED"]
    ineligible = exits["INELIGIBLE"]
    oos = exits["OUT_OF_SCOPE"]
    wait_err = exits["ENTRY_WAIT_ERROR"]
    if fetch_fail:
        table_rows += _row("CT fetch failed", fetch_fail,
                           denom=enrolled, indent=1, color="#c00")
    if ineligible:
        table_rows += _row("Ineligible (DPD cured by call time)", ineligible,
                           denom=enrolled, indent=1, color="#888")
    if oos:
        table_rows += _row("Out of scope (no segment matched)", oos,
                           denom=enrolled, indent=1, color="#888")
    if wait_err:
        table_rows += _row("Entry wait error", wait_err,
                           denom=enrolled, indent=1, color="#c00")
    if not any([fetch_fail, ineligible, oos, wait_err]):
        table_rows += _row("None — all enrolled customers routed to a segment", 0,
                           indent=1, color="#2a9d2a")

    # Layer 4 — per-segment
    table_rows += _row("── Segment routing ──", None)
    for seg in SEGMENT_META:
        sname = seg["name"]
        cnt = seg_counts.get(sname, 0)
        label = (
            f"{sname}"
            f"<span style='color:#aaa;font-size:11px'>"
            f"&nbsp;&nbsp;{seg['calls']} call{'s' if seg['calls']!=1 else ''}"
            f" · {seg['entry']}</span>"
        )
        table_rows += _row(label, cnt, denom=enrolled, indent=1)

    if seg_counts.get("unclassified", 0):
        table_rows += _row("unclassified ⚠", seg_counts["unclassified"],
                           indent=1, color="#c00")

    table_rows += _row("Total in segments", f["in_segments"],
                       denom=enrolled, bold=True, color="#1a6e1a")

    table = (
        "<table border='1' cellpadding='0' cellspacing='0' "
        "style='border-collapse:collapse;font-family:Arial,sans-serif;"
        "font-size:13px;min-width:400px'>"
        + table_rows + "</table>"
    )

    # OUT_OF_SCOPE detail block — shown only when there are OOS customers
    oos_section = ""
    oos_detail = f.get("oos_detail", {})
    if oos_detail:
        detail_rows = "".join(
            f"<tr>"
            f"<td style='padding:4px 8px;font-family:monospace'>{html.escape(str(val))}</td>"
            f"<td style='padding:4px 8px;text-align:center'>{cnt}</td>"
            f"</tr>"
            for val, cnt in sorted(oos_detail.items(), key=lambda x: -x[1])
        )
        oos_section = (
            "<div style='margin-top:18px;padding:12px 16px;"
            "background:#fff3cd;border:1px solid #f0ad4e;border-radius:4px'>"
            "<b style='color:#856404'>⚠ OUT_OF_SCOPE customers — action needed</b>"
            "<p style='margin:6px 0 8px;font-size:12px;color:#555'>"
            "These customers had a <code>coll_bot_calling</code> value that "
            "didn't match any segment, or had no value set. "
            "Check CT to make sure the right property value is being applied.</p>"
            "<table border='1' cellpadding='0' cellspacing='0' "
            "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:12px'>"
            "<thead><tr style='background:#ffeeba'>"
            "<th style='padding:4px 8px'>coll_bot_calling value</th>"
            "<th style='padding:4px 8px'>Customers</th>"
            "</tr></thead><tbody>" + detail_rows + "</tbody></table>"
            "</div>"
        )

    banner = ""
    if f.get("health_warning"):
        banner = (
            "<div style='margin:0 0 16px;padding:14px 18px;background:#f8d7da;"
            "border:2px solid #dc3545;border-radius:4px'>"
            "<b style='color:#721c24;font-size:15px'>🚨 PIPELINE WARNING — 0 enrolled</b>"
            f"<p style='margin:6px 0 0;font-size:13px;color:#721c24'>"
            f"{f['csv_count']} candidates were fetched from collection_view but "
            "<b>NONE enrolled</b>. The cohort is not ready — enrollment likely "
            "failed, has not run yet, or this funnel fired before it completed. "
            "Check the enrollment poller + executor on the server.</p></div>"
        )
    return (
        f"<h3 style='margin-bottom:8px'>VB Collections — Enrollment Funnel</h3>"
        f"<p style='color:#555;font-size:13px;margin-top:0'>Date: <b>{date_str}</b></p>"
        + banner + table + oos_section +
        "<p style='margin-top:14px;font-size:11px;color:#bbb'>"
        "Generated by VB Collections v2 workflow · "
        "Segments defined in seed_workflows/generate_vb_collections_v2.py</p>"
    )


def _send_email(gmail_config_path: str, subject: str, html_body: str) -> None:
    with open(gmail_config_path) as f:
        gcfg = json.load(f)
    sender = gcfg["user"]
    msg = MIMEMultipart()
    msg["From"] = f"VB Collections Bot <{sender}>"
    msg["To"] = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
        server.starttls()
        server.login(sender, gcfg["password"])
        server.sendmail(sender, RECIPIENTS, msg.as_string())
    log.info("sent: %r → %s", subject, RECIPIENTS)


def main() -> None:
    args = parse_args()
    date_str = _today(args.date)

    csv_count = _csv_candidate_count(args.csv_dir, date_str)
    log.info("csv_count=%s date=%s", csv_count, date_str)

    rows = _fetch_funnel(args.workflow_db, date_str)
    log.info("workflow_runs enrolled today: %d", len(rows))

    funnel = _build_funnel(rows, csv_count, date_str)
    subject = _build_subject(funnel)
    html_body = _build_html(funnel)

    if args.dry_run:
        log.info("DRY-RUN — subject: %s", subject)
        print(f"\nSubject : {subject}")
        print(f"To      : {', '.join(RECIPIENTS)}")
        print(f"\nFunnel  :")
        print(f"  CSV candidates : {csv_count}")
        print(f"  Enrolled       : {funnel['total_enrolled']}")
        for k, v in funnel["exits"].items():
            if v:
                print(f"  {k:20s}: {v}")
        for seg in SEGMENT_META:
            cnt = funnel["seg_counts"].get(seg["name"], 0)
            print(f"  {seg['name']:20s}: {cnt}  ({seg['calls']}c {seg['entry']})")
        return

    _send_email(args.gmail_config, subject, html_body)


if __name__ == "__main__":
    _args = None
    try:
        _args = parse_args()
        main()
    except Exception as exc:
        log.exception("enrollment_funnel_emailer failed: %s", exc)
        # Best-effort failure alert — do not let alerting failure mask the original.
        try:
            _cfg = (_args.gmail_config if _args else None) or GMAIL_CONFIG
            _send_email(
                _cfg,
                f"[FAIL] VB Enrollment funnel emailer crashed — {datetime.now(IST).strftime('%Y-%m-%d')}",
                f"<p><b>Error:</b> {exc}</p><p>Check logs on the AWS server.</p>",
            )
        except Exception:  # stashfin-lint: ignore — alert failure must never mask original
            pass
        sys.exit(1)
