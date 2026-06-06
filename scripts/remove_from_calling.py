#!/usr/bin/env python3
"""Remove agent-allocated customers from the VB bot calling pipeline.

Cases already handed to human agents must NOT be bot-called (double-contact).
This terminates their LIVE runs (ACTIVE/WAITING) so the bot stops calling them,
and suppresses any queued PENDING fire. The DURABLE re-enrollment block is
separate — enrollment_poller reads the same exclusion file and skips those ids,
so a terminated customer can't simply re-enroll on the next spell.

Idempotent: only touches ACTIVE/WAITING runs + PENDING actions, so re-running
once they're terminated/suppressed is a no-op. --dry-run reports counts and
writes nothing (atomic rollback).

Exclusion source: state/bot_exclusion_ids.csv (one customer_id per line; a
'customer_id'/'customer id' header line is tolerated).

CLI:
    python3 scripts/remove_from_calling.py --dry-run
    python3 scripts/remove_from_calling.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from workflow.wf_store import get_workflow_db, transaction  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(Path(__file__).stem)

DEFAULT_EXCLUSION = str(_REPO / "state" / "bot_exclusion_ids.csv")
TERMINAL_STATUS = "AGENT_ALLOCATED_MANUAL"


class _Rollback(Exception):
    """Dry-run control-flow: roll back the (zero) writes after counting."""


def load_exclusion_ids(path: str) -> set[str]:
    ids: set[str] = set()
    normalized = 0
    with open(path) as f:
        for line in f:
            s = line.strip()
            if not s or s.lower() in ("customer_id", "customer id"):
                continue
            # Strip float-stored ".0" (Excel/pandas export) — workflow_runs
            # stores clean integer-strings, so "61049164.0" would silently miss
            # the customer_id match and leave them in the calling pool.
            if s.endswith(".0"):
                s = s[:-2]
                normalized += 1
            ids.add(s)
    if normalized:
        log.info("normalized %d float-stored ('.0') ids", normalized)
    return ids


def remove(workflow_db: str, exclusion_file: str, *, dry_run: bool, min_ids: int = 0) -> dict:
    ids = load_exclusion_ids(exclusion_file)
    log.info("loaded %d exclusion ids from %s", len(ids), exclusion_file)
    if not ids:
        log.error("no ids loaded from %s — aborting", exclusion_file)
        return {"error": "no_ids"}
    if min_ids and len(ids) < min_ids:
        # Guard against a truncated/half-written input silently under-removing.
        log.error("loaded %d ids < --min-ids %d — aborting (input looks truncated)",
                  len(ids), min_ids)
        return {"error": "below_min_ids", "loaded": len(ids), "min_ids": min_ids}
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_workflow_db(workflow_db)
    n_runs = n_pend = 0
    try:
        with transaction(conn):
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS _excl (cid TEXT PRIMARY KEY)")
            conn.execute("DELETE FROM _excl")
            conn.executemany("INSERT OR IGNORE INTO _excl(cid) VALUES (?)",
                             [(i,) for i in ids])
            n_runs = conn.execute(
                "SELECT COUNT(*) FROM workflow_runs WHERE status IN ('ACTIVE','WAITING') "
                "AND customer_id IN (SELECT cid FROM _excl)"
            ).fetchone()[0]
            # Suppress not just PENDING but ERROR + FIRING_IN_PROGRESS too:
            # self_cure (30-min cron) flips those back to PENDING with no
            # customer filter, which would re-fire to an agent-allocated
            # customer. (The scheduler's run-status gate is the durable backstop;
            # this is defense-in-depth.)
            n_pend = conn.execute(
                "SELECT COUNT(*) FROM wf_pending_actions "
                "WHERE status IN ('PENDING','ERROR','FIRING_IN_PROGRESS') "
                "AND customer_id IN (SELECT cid FROM _excl)"
            ).fetchone()[0]
            log.info("scope: ids=%d | ACTIVE/WAITING runs to terminate=%d | "
                     "PENDING fires to suppress=%d", len(ids), n_runs, n_pend)
            if dry_run:
                log.info("DRY-RUN — no writes (rolling back)")
                raise _Rollback()
            conn.execute(
                "UPDATE workflow_runs SET status='DONE', terminal_status=?, "
                "terminated_at_ist=?, updated_at_ist=? "
                "WHERE status IN ('ACTIVE','WAITING') "
                "AND customer_id IN (SELECT cid FROM _excl)",
                (TERMINAL_STATUS, now, now),
            )
            conn.execute(
                "UPDATE wf_pending_actions SET status='SUPPRESSED', last_error=? "
                "WHERE status IN ('PENDING','ERROR','FIRING_IN_PROGRESS') "
                "AND customer_id IN (SELECT cid FROM _excl)",
                ("agent_allocated_manual",),
            )
        log.info("APPLIED: terminated=%d runs, suppressed=%d pending (ids=%d)",
                 n_runs, n_pend, len(ids))
        return {"runs_terminated": n_runs, "pending_suppressed": n_pend, "ids": len(ids)}
    except _Rollback:  # stashfin-lint: ignore  # intentional dry-run rollback; counts already gathered
        return {"runs_to_terminate": n_runs, "pending_to_suppress": n_pend,
                "ids": len(ids), "dry_run": True}
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--exclusion-file", default=DEFAULT_EXCLUSION)
    ap.add_argument("--min-ids", type=int, default=0,
                    help="abort if fewer than N ids load (guards a truncated input)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        summary = remove(args.workflow_db, args.exclusion_file,
                         dry_run=args.dry_run, min_ids=args.min_ids)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit
        log.exception("remove_from_calling FAILED: %s", exc)
        sys.exit(1)
    log.info("done: %s", summary)
    if summary.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
