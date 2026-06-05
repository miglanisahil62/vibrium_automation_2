#!/usr/bin/env python3
"""WS9 — WhatsApp parity for the vibrium-workflow X-Bucket pipeline.

When the WORKFLOW voice bot gets a PTP / Agree-to-pay disposition, fire the
WhatsApp "overdue reminder" CleverTap campaign to reinforce the promise — exactly
like the adhoc vibrium-automation dispatcher does for adhoc calls.

Shared-cap design (NO double-send): candidates come from the WORKFLOW's
``wf_decision_log`` (workflow.db), but the per-customer caps + the fire ledger
are the ADHOC system's ``whatsapp_log`` in vibrium.db — the SAME table the adhoc
dispatcher uses. So a customer in both systems shares one cap ledger and can
never be double-WhatsApp'd, today (adhoc dispatcher idle) or if it revives.
Workflow fires are tagged ``trigger_origin = 'WF_PTP' / 'WF_AGREE'`` so they stay
distinguishable in the shared log (separate observability, shared caps).

Caps (read from vibrium-automation/config.json `whatsapp`, the live source):
  * per_customer_daily_cap  (1)  — no WA to this cid today (any origin)
  * per_customer_weekly_cap (3)  — <3 WA in last 7d
  * daily_cap_global    (2000)   — global fires/day ceiling
  * fire_hour_min/max  (8/19)    — RBI window 08:00 <= IST hour < 19:00
  * WA_Available required         — run scratchpad coll_notification_replied == 'WA_Available'
Idempotent: the 1/day cap (a logged row for the cid today) blocks re-fire across
ticks. Cross-DB: two read-only/own connections, no ATTACH (wf_store convention).

CLI:
    python3 scripts/workflow_whatsapp.py
    python3 scripts/workflow_whatsapp.py --dry-run     # evaluate + log nothing, fire nothing
Env: WF_WA_SHADOW=1 → evaluate + log shadow rows, no real CT fire.

Cron (every 30 min in window; AWS UTC):
    */30 2-13 * * *  /home/ubuntu/vibrium-workflow/run.sh whatsapp
Heartbeat job_id: wf_whatsapp
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(Path(__file__).stem)

_WORKFLOW_DB = os.environ.get("WF_WORKFLOW_DB", str(_REPO / "state" / "workflow.db"))
_VIBRIUM_DB = os.environ.get("WF_VIBRIUM_DB", "/home/ubuntu/vibrium-automation/state/vibrium.db")
_ADHOC_CONFIG = os.environ.get("WF_ADHOC_CONFIG", "/home/ubuntu/vibrium-automation/config.json")

# action_class → WhatsApp trigger origin tag (only promise dispositions).
_ORIGIN = {"PTP_CALL": "WF_PTP", "AGREE_EOD_CALL": "WF_AGREE"}


def _now() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


def _load_wa_config(path: str) -> dict:
    with open(path) as f:
        cfg = json.load(f)
    wa = dict(cfg.get("whatsapp", {}))
    ct = dict(cfg.get("clevertap", {}))
    wa["_bot_id"] = ct.get("bot_id")
    wa["_ct_cred_path"] = ct.get("credentials_path")
    if not wa.get("campaign_id") or not wa["_bot_id"] or not wa["_ct_cred_path"]:
        raise ValueError(f"incomplete WA/clevertap config in {path}: "
                         f"campaign_id/bot_id/credentials_path required")
    return wa


def _candidates(wf_db: str) -> dict:
    """{customer_id(str): origin} for today's workflow PTP/Agree dispositions
    whose run is WA_Available. Latest disposition per customer wins."""
    conn = sqlite3.connect(f"file:{wf_db}?mode=ro", uri=True)
    try:
        today = _now().strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT d.customer_id, d.action_class, "
            "  json_extract(r.scratchpad_json, '$.coll_notification_replied') AS wa "
            "FROM wf_decision_log d "
            "LEFT JOIN workflow_runs r ON r.id = d.run_id "
            "WHERE substr(d.ts_ist,1,10) = ? AND d.action_class IN ('PTP_CALL','AGREE_EOD_CALL') "
            "ORDER BY d.id ASC",
            (today,),
        ).fetchall()
    finally:
        conn.close()
    out: dict = {}
    skipped_no_wa = 0
    for cid, ac, wa in rows:
        if wa != "WA_Available":
            skipped_no_wa += 1
            continue
        out[str(cid)] = _ORIGIN.get(ac, "WF_PTP")  # latest wins (ORDER BY id ASC)
    log.info("candidates: %d WA_Available promise-makers today (%d skipped: not WA_Available)",
             len(out), skipped_no_wa)
    return out


def _cap_state(vib_db: str, cids: list) -> tuple:
    """From the SHARED whatsapp_log: (set of cids fired today, {cid: 7d count},
    global fires today). Read-only."""
    conn = sqlite3.connect(f"file:{vib_db}?mode=ro", uri=True)
    try:
        today = _now().strftime("%Y-%m-%d")
        wk = (_now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        fired_today = set(str(r[0]) for r in conn.execute(
            "SELECT customer_id FROM whatsapp_log WHERE substr(ts_ist,1,10)=? "
            "AND status IN ('success','shadow')", (today,)))
        # Global cap counts success+shadow (parity with adhoc CAP_COUNT_OUTCOMES) —
        # otherwise the 2000/day ceiling is inert in shadow mode (the cutover
        # phase), giving false "volume is bounded" confidence (master-auditor P0-1).
        global_today = conn.execute(
            "SELECT COUNT(*) FROM whatsapp_log WHERE substr(ts_ist,1,10)=? "
            "AND status IN ('success','shadow')", (today,)).fetchone()[0]
        wk_counts: dict = {}
        for r in conn.execute(
            "SELECT customer_id, COUNT(*) FROM whatsapp_log WHERE ts_ist >= ? "
            "AND status IN ('success','shadow') GROUP BY customer_id", (wk,)):
            wk_counts[str(r[0])] = r[1]
    finally:
        conn.close()
    return fired_today, wk_counts, global_today


def _log_fire(vib_db: str, cid: str, origin: str, campaign_id, res: dict, shadow: bool) -> None:
    """Append one row to the SHARED whatsapp_log (vibrium.db) so the cap ledger
    is shared with adhoc. _row_index is AUTOINCREMENT (omit it — parity with the
    adhoc writer). campaign_id stored as str (TEXT column, parity with adhoc).
    shadow_mode populated ('yes'/'no') for the RBI audit trail (P1-1)."""
    conn = sqlite3.connect(vib_db, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        is_shadow = shadow or res.get("status") == "shadow"
        with conn:
            conn.execute(
                "INSERT INTO whatsapp_log (ts_ist, customer_id, campaign_id, "
                "trigger_origin, status, http_status, response_body, shadow_mode) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (_now().strftime("%Y-%m-%d %H:%M:%S"), cid, str(campaign_id), origin,
                 res.get("status", "error"), str(res.get("http_status", "")),
                 json.dumps(res.get("body") or {}, default=str)[:1000],
                 "yes" if is_shadow else "no"),
            )
    finally:
        conn.close()


_WA_LOCK_PATH = os.environ.get(
    "WF_WA_LOCK", "/home/ubuntu/vibrium-automation/state/vibrium_wa_dispatcher.lock")


def run(*, dry_run: bool) -> dict:
    # P1-2: share the ADHOC dispatcher's lockfile so a concurrent adhoc run +
    # this workflow run can't both read-then-write whatsapp_log and double-send
    # the same customer. Same lock path = genuinely shared mutex across systems.
    import fcntl
    # P1-1: open 0o664 (group-writable) like the adhoc _open_lockfile, so a
    # cron-as-ubuntu run and a manual sudo run can share the SAME lock file. A
    # permission/OS error → clean skip (return), never a crash to heartbeat-down.
    try:
        lock_fd = os.fdopen(os.open(_WA_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o664), "r+")
    except OSError as exc:
        log.error("WA lockfile unavailable (%s): %s — skip tick", _WA_LOCK_PATH, exc)
        return {"candidates": 0, "fired": 0, "skipped_reason": "lockfile_unavailable"}
    try:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.warning("WA lock held (%s) — another dispatcher running; skip tick", _WA_LOCK_PATH)
            return {"candidates": 0, "fired": 0, "skipped_reason": "concurrent_invocation"}
        try:
            return _run_locked(dry_run=dry_run)
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    finally:
        lock_fd.close()


def _run_locked(*, dry_run: bool) -> dict:
    from external.vibrium_automation_scripts.clevertap_trigger import trigger as ct_trigger  # type: ignore

    shadow = os.environ.get("WF_WA_SHADOW", "0") == "1"
    wa = _load_wa_config(_ADHOC_CONFIG)
    campaign_id = int(wa["campaign_id"])
    daily_global = int(wa.get("daily_cap_global", 2000))
    weekly_cap = int(wa.get("per_customer_weekly_cap", 3))
    hmin = int(wa.get("fire_hour_min", 8))
    hmax = int(wa.get("fire_hour_max", 19))
    stats = {"candidates": 0, "fired": 0, "shadow": 0, "dry_run_skipped": 0,
             "skipped_cap": 0, "skipped_window": 0, "failed": 0,
             "dry_run": dry_run, "shadow_mode": shadow}

    hour = _now().hour
    cand = _candidates(_WORKFLOW_DB)
    stats["candidates"] = len(cand)
    if not cand:
        log.info("no WA candidates; done %s", stats)
        return stats

    # HARD window guard (RBI): never fire outside [hmin, hmax).
    if not (hmin <= hour < hmax):
        stats["skipped_window"] = len(cand)
        log.info("outside WA window (hour=%d not in [%d,%d)) — skipping all %d", hour, hmin, hmax, len(cand))
        return stats

    fired_today, wk_counts, global_today = _cap_state(_VIBRIUM_DB, list(cand))
    sent_this_run = 0  # rows logged this run (success OR shadow) — feed the global cap
    for cid, origin in cand.items():
        if global_today + sent_this_run >= daily_global:
            log.warning("global daily WA cap %d reached — stopping", daily_global)
            break
        if cid in fired_today:
            stats["skipped_cap"] += 1
            continue
        if wk_counts.get(cid, 0) >= weekly_cap:
            stats["skipped_cap"] += 1
            continue
        if dry_run:
            stats["dry_run_skipped"] += 1   # P2-1: dry-run logs nothing (distinct from shadow)
            continue
        try:
            res = ct_trigger(
                customer_id=int(cid),
                action_class=f"WHATSAPP_{origin}",
                ct_cred_path=wa["_ct_cred_path"],
                campaign_id=campaign_id,
                bot_id=wa["_bot_id"],
                contact_type=wa.get("contact_type", "whatsapp"),
                shadow_mode=shadow,
                extra_props={},
            )
        except Exception as exc:  # noqa: BLE001 — one bad id must not abort the batch
            stats["failed"] += 1
            log.warning("WA fire FAILED cid=%s (%s)", cid, exc)
            continue
        ok = res.get("status") in ("success", "shadow")
        _log_fire(_VIBRIUM_DB, cid, origin, campaign_id, res, shadow)
        if ok:
            sent_this_run += 1          # a row was logged → counts toward global cap
            fired_today.add(cid)        # per-customer 1/day, this run
        if res.get("status") == "shadow":
            stats["shadow"] += 1
        elif ok:
            stats["fired"] += 1
        else:
            stats["failed"] += 1
            log.warning("WA rejected cid=%s status=%s", cid, res.get("status"))
    log.info("workflow_whatsapp done: %s", stats)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit (heartbeat down)
        log.exception("workflow_whatsapp FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
