#!/usr/bin/env python3
"""WS14 — server-side self-cure for the vibrium-workflow X-Bucket pipeline.

Runs every ~30 min during the call window (cron). Detects + auto-recovers the
transient stuck states that would otherwise silently strand runs, so the
pipeline keeps moving WITHOUT manual intervention. Every recovery is idempotent:
the downstream pre_call_gate (paid + RBI window) + customer_call_audit (3/day) +
the FIRE UNIQUE(run_id,node_id,attempt_count) dedupe make a re-fire impossible to
double-dial, so re-queueing a stuck row can never over-contact a customer.

What it cures (all bounded, all logged):
  1. ERROR wf_pending_actions  → PENDING (retryable scheduler/FIRE error). attempts
     zeroed so the retry isn't pre-counted against the daily cap (the daily cap
     reads customer_call_audit, not attempts, so this can't defeat the 3/day cap).
  2. FIRING_IN_PROGRESS stuck > STUCK_FIRING_MIN  → PENDING (the scheduler claimed
     the row then crashed mid-fire; the claim is stale). Guarded by age so an
     in-flight fire is never yanked (claim→FIRED is seconds; cutoff is 30 min).
  3. REPORT-ONLY: ACTIVE runs whose entered_node_at_ist is older than
     STUCK_ACTIVE_HOURS (mid-walk orphans) — surfaced in the summary + heartbeat
     for visibility; not auto-mutated (needs a human/executor STALE_REQUEUED look).

Deliberately does NOT touch WAITING runs: the executor already picks up any
WAITING run with ready_at_ist <= now on its next tick (so a past-ready run is
never stranded), and auto-nudging a FUTURE-parked run would prematurely fire a
legitimate next-day / PTP / callback park (a contact-safety hazard). Re-aiming a
genuinely-orphaned future park is the manual ``unpark_waiting_runs.py`` tool's
job, not an unattended auto-cure (master-auditor WS14 P2-1).

Exit code reflects real success ($RC drives the run.sh heartbeat). --dry-run
reports counts and writes nothing.

CLI:
    python3 scripts/self_cure.py
    python3 scripts/self_cure.py --dry-run

Cron (every 30 min, 08:00-19:00 IST window; AWS server is UTC):
    */30 2-13 * * *  /home/ubuntu/vibrium-workflow/run.sh self_cure
Heartbeat job_id: wf_self_cure
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
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

# Tunables (env-overridable). Conservative defaults so we never yank live work.
STUCK_FIRING_MIN = int(os.environ.get("WF_CURE_STUCK_FIRING_MIN", "30"))
STALE_WAIT_MIN = int(os.environ.get("WF_CURE_STALE_WAIT_MIN", "30"))
STUCK_ACTIVE_HOURS = int(os.environ.get("WF_CURE_STUCK_ACTIVE_HOURS", "6"))
# Safety cap: never auto-mutate more than this many rows of one class in a run
# (a huge count means something systemic — surface it, don't mass-mutate blindly).
MAX_CURE_ROWS = int(os.environ.get("WF_CURE_MAX_ROWS", "20000"))


class _DryRunRollback(Exception):
    """Raised inside the transaction on --dry-run to roll back the (zero) writes
    after the counts are gathered. Pure control flow, not an error."""


def _now() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def cure(workflow_db: str, *, dry_run: bool) -> dict:
    now = _now()
    firing_cutoff = _ts(now - timedelta(minutes=STUCK_FIRING_MIN))
    active_cutoff = _ts(now - timedelta(hours=STUCK_ACTIVE_HOURS))

    stats = {"error_reset": 0, "firing_reset": 0, "stuck_active_reported": 0,
             "dry_run": dry_run, "aborted": []}
    aborted: set = set()  # class labels skipped by the MAX_CURE_ROWS guard
    conn = None
    try:
        # Shared helper → WAL + busy_timeout + foreign_keys (P2-3); transaction()
        # issues BEGIN IMMEDIATE so the counts + UPDATEs are one atomic snapshot.
        conn = get_workflow_db(workflow_db)

        with transaction(conn):
            # 1. ERROR pending-actions → PENDING
            n_err = conn.execute(
                "SELECT COUNT(*) FROM wf_pending_actions WHERE status='ERROR'"
            ).fetchone()[0]
            stats["error_reset"] = n_err

            # 2. FIRING_IN_PROGRESS stuck older than the cutoff (claimed at
            #    last_attempt_at_ist; fall back to created_at_ist if null).
            n_firing = conn.execute(
                "SELECT COUNT(*) FROM wf_pending_actions WHERE status='FIRING_IN_PROGRESS' "
                "AND COALESCE(last_attempt_at_ist, created_at_ist) <= ?",
                (firing_cutoff,),
            ).fetchone()[0]
            stats["firing_reset"] = n_firing

            # 3. Report-only: stuck ACTIVE runs (mid-walk orphans).
            n_active = conn.execute(
                "SELECT COUNT(*) FROM workflow_runs WHERE status='ACTIVE' "
                "AND entered_node_at_ist IS NOT NULL AND entered_node_at_ist <= ?",
                (active_cutoff,),
            ).fetchone()[0]
            stats["stuck_active_reported"] = n_active

            # Systemic-anomaly guard: if an auto-cure class exceeds MAX_CURE_ROWS,
            # do NOT mass-mutate — surface it loudly + skip that class (set
            # membership, not substring — P2-2).
            for label, n in (("error_reset", n_err), ("firing_reset", n_firing)):
                if n > MAX_CURE_ROWS:
                    aborted.add(label)
                    stats["aborted"].append(f"{label}={n}>{MAX_CURE_ROWS}")
                    log.error("self_cure: %s count %d exceeds MAX_CURE_ROWS %d — "
                              "NOT auto-curing this class (systemic issue?)",
                              label, n, MAX_CURE_ROWS)

            log.info("self_cure scan: %s", stats)
            if dry_run:
                log.info("DRY-RUN — no writes (rolling back)")
                raise _DryRunRollback()

            if n_err and "error_reset" not in aborted:
                conn.execute(
                    "UPDATE wf_pending_actions SET status='PENDING', last_error=NULL, "
                    "attempts=0 WHERE status='ERROR'"
                )
            if n_firing and "firing_reset" not in aborted:
                conn.execute(
                    "UPDATE wf_pending_actions SET status='PENDING' "
                    "WHERE status='FIRING_IN_PROGRESS' "
                    "AND COALESCE(last_attempt_at_ist, created_at_ist) <= ?",
                    (firing_cutoff,),
                )
        log.info("self_cure applied: error_reset=%d firing_reset=%d (active_reported=%d)",
                 n_err, n_firing, stats["stuck_active_reported"])
        return stats
    except _DryRunRollback:  # stashfin-lint: ignore  # intentional dry-run rollback control-flow; stats already populated from the read counts, no error swallowed
        return stats
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        summary = cure(args.workflow_db, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit so the heartbeat goes 'down'
        log.exception("self_cure FAILED: %s", exc)
        sys.exit(1)
    # Stuck-active runs are report-only but a large count is worth a non-zero-ish
    # signal in the log (heartbeat stays ok — they're not auto-curable here).
    if summary.get("aborted"):
        log.error("self_cure completed WITH aborted classes: %s", summary["aborted"])
    log.info("self_cure done: %s", summary)


if __name__ == "__main__":
    main()
