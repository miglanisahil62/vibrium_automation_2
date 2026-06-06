#!/usr/bin/env python3
"""One-off back-fill: re-enrol blank-label customers already terminated
OUT_OF_SCOPE (on a pre-one_time version) into the LIVE v9 graph so they get the
one-time catch-all call. Their original run is terminal, so the standard poller
(all-time spell-dedup) will never re-enrol them — this uses a DISTINCT
enrollment_key suffix '_onetime' to bypass the dedup surgically.

Idempotent + safe:
  * candidates = workflow_runs terminal_status='OUT_OF_SCOPE' on version_id <
    the live active version, enrolled within --since-days, for the workflow.
  * INSERT-TIME exclusions (mandatory, not gate-deferred): agent-allocated
    (bot_exclusion_ids.csv), any customer with a live ACTIVE/WAITING run, and
    any customer already _onetime-enrolled (re-run safe).
  * pinned to the live active_version_id at the ENROLL node, status ACTIVE,
    seeded scratchpad — flows through v9 normally → classifies one_time_catchall
    → ONE low-priority opportunistic call (paid/window/cap re-checked at fire).
  * --dry-run default-off; --max-ids hard cap aborts on an unexpectedly large set.

Run TOMORROW MORNING (not end-of-day): a one_time low row needs an open window +
spare capacity, else it re-parks then terminates ONE_TIME_NO_OUTCOME without a call.

CLI:
    python3 scripts/reenroll_one_time.py --dry-run
    python3 scripts/reenroll_one_time.py --max-ids 12000
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from workflow.wf_store import get_workflow_db, transaction        # noqa: E402
from workflow.enrollment_poller import (                          # noqa: E402
    _find_enroll_node_id, _seed_best_hours,
)

IST = ZoneInfo("Asia/Kolkata")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(Path(__file__).stem)

WORKFLOW_NAME = "VB Collections v2"
EXCLUSION_FILE = str(_REPO / "state" / "bot_exclusion_ids.csv")


def _load_exclusion(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    out: set[str] = set()
    for line in p.read_text().splitlines():
        s = line.strip()
        if not s or s.lower() in ("customer_id", "customer id"):
            continue
        if s.endswith(".0"):
            s = s[:-2]
        out.add(s)
    return out


def run(workflow_db: str, *, dry_run: bool, since_days: int, max_ids: int) -> dict:
    now = datetime.now(IST)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    since = (now - timedelta(days=since_days)).strftime("%Y-%m-%d")
    excluded = _load_exclusion(EXCLUSION_FILE)
    conn = get_workflow_db(workflow_db)
    try:
        # Deterministic ACTIVE-workflow resolve (the DB may hold several same-named
        # DRAFT/older rows; never rely on implicit row order — master-auditor P1-1).
        wf_rows = conn.execute(
            "SELECT id, active_version_id FROM workflows "
            "WHERE name=? AND status='ACTIVE' AND active_version_id IS NOT NULL",
            (WORKFLOW_NAME,),
        ).fetchall()
        if len(wf_rows) != 1:
            log.error("expected exactly 1 ACTIVE workflow named %r, found %d — aborting",
                      WORKFLOW_NAME, len(wf_rows))
            return {"error": "ambiguous_workflow", "found": len(wf_rows)}
        wf_id, active_vid = wf_rows[0]["id"], wf_rows[0]["active_version_id"]
        enroll_node = _find_enroll_node_id(conn, active_vid)
        if enroll_node is None:
            log.error("no ENROLL node in active version %s", active_vid)
            return {"error": "no_enroll_node"}

        # GRAPH-READINESS GUARD (master-auditor P0-1): the back-fill is pointless —
        # and burns the _onetime idempotency key — unless the ACTIVE graph actually
        # contains the one_time catch-all. If it doesn't, re-enrolled blank-label
        # customers just re-terminate OUT_OF_SCOPE with zero calls. Hard-abort.
        gj_row = conn.execute(
            "SELECT graph_json FROM workflow_versions WHERE id=?", (active_vid,)).fetchone()
        graph_json_str = gj_row["graph_json"] if gj_row else ""
        if "one_time_catchall" not in graph_json_str and "ONE_TIME_NO_OUTCOME" not in graph_json_str:
            log.error("active version %s has NO one_time_catchall node — re-seed + "
                      "go-live the one_time graph first; aborting", active_vid)
            return {"error": "no_one_time_node", "active_version_id": active_vid}

        # Candidates: OUT_OF_SCOPE on a PRE-active version, enrolled within window.
        cand = conn.execute(
            "SELECT DISTINCT customer_id FROM workflow_runs "
            "WHERE workflow_id=? AND terminal_status='OUT_OF_SCOPE' "
            "AND version_id < ? AND substr(enrolled_at_ist,1,10) >= ?",
            (wf_id, active_vid, since),
        ).fetchall()
        cand_ids = [str(r["customer_id"]) for r in cand]

        # Live ACTIVE/WAITING runs (any version) → don't double-enrol.
        live = {str(r["customer_id"]) for r in conn.execute(
            "SELECT DISTINCT customer_id FROM workflow_runs WHERE status IN ('ACTIVE','WAITING')"
        ).fetchall()}
        # Already _onetime-enrolled → re-run idempotency.
        already = {str(r["customer_id"]) for r in conn.execute(
            "SELECT DISTINCT customer_id FROM workflow_runs WHERE enrollment_key LIKE '%\\_onetime' ESCAPE '\\'"
        ).fetchall()}

        targets = [c for c in cand_ids
                   if c not in excluded and c not in live and c not in already]
        log.info("candidates=%d | excl agent-alloc=%d live=%d already=%d → targets=%d",
                 len(cand_ids), len(cand_ids) - len([c for c in cand_ids if c not in excluded]),
                 len(live & set(cand_ids)), len(already & set(cand_ids)), len(targets))

        if max_ids and len(targets) > max_ids:
            log.error("targets %d > --max-ids %d — aborting (set looks too large)",
                      len(targets), max_ids)
            return {"error": "above_max_ids", "targets": len(targets), "max_ids": max_ids}
        if not targets:
            return {"targets": 0, "inserted": 0}
        if dry_run:
            log.info("DRY-RUN — would re-enrol %d customers on version %s (one_time)",
                     len(targets), active_vid)
            return {"targets": len(targets), "inserted": 0, "dry_run": True}

        inserted = 0
        with transaction(conn):
            for cid in targets:
                ek = f"{cid}_{now.strftime('%Y-%m-%d')}_onetime"
                seed = json.dumps({"best_hours": _seed_best_hours(cid),
                                   "day_index": 0, "attempts_today": 0, "fire_seq": 0})
                cur = conn.execute(
                    "INSERT OR IGNORE INTO workflow_runs ("
                    " workflow_id, version_id, customer_id, enrollment_key,"
                    " current_node_id, current_node_type, entered_node_at_ist,"
                    " status, scratchpad_json, enrolled_at_ist, updated_at_ist"
                    ") VALUES (?, ?, ?, ?, ?, 'ENROLL', ?, 'ACTIVE', ?, ?, ?)",
                    (wf_id, active_vid, cid, ek, enroll_node, now_str, seed, now_str, now_str),
                )
                inserted += int(cur.rowcount == 1)
        log.info("APPLIED: re-enrolled %d / %d targets on version %s (one_time)",
                 inserted, len(targets), active_vid)
        return {"targets": len(targets), "inserted": inserted}
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--since-days", type=int, default=2,
                    help="only back-fill OUT_OF_SCOPE runs enrolled within N days")
    ap.add_argument("--max-ids", type=int, default=8000,
                    help="abort if more than N targets (guards a runaway set; expected ~6k)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        summary = run(args.workflow_db, dry_run=args.dry_run,
                      since_days=args.since_days, max_ids=args.max_ids)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit
        log.exception("reenroll_one_time FAILED: %s", exc)
        sys.exit(1)
    log.info("done: %s", summary)
    if summary.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
