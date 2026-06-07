"""Reset stuck ``workflow_runs`` in status='ERROR' back to ACTIVE for re-processing.

WHEN TO USE
    The ``B_RUN_ERROR`` (P1) alert fires when one or more runs sit in
    status='ERROR' for >2h. The common cause on this engine is a transient
    ``sqlite3.OperationalError: database is locked`` — the node handler's
    write-back lost a race with a concurrent daemon and the run was parked in
    ERROR. The parked node is almost always WAIT_UNTIL or CONDITION (idempotent
    re-evaluation), NOT the call-firing node — so re-activating the run cannot
    duplicate a customer call. The executor re-runs the run's current node on
    its next tick.

LOCK-ONLY BY DEFAULT
    By default this resets ONLY runs whose latest ``workflow_node_log.side_effect``
    looks transient (``database is locked`` / ``OperationalError`` / ``timeout``).
    A LOGIC-bug run reset to ACTIVE just re-errors on the next tick and would be
    reset again on the next run of this script — a churn loop that also keeps
    bumping ``updated_at_ist`` and so permanently masks the run's true age from
    the B_RUN_ERROR alert. If you have diagnosed (runbook section B) that the
    non-transient errors are also safe to retry, pass ``--all-errors``.

NOT THE SAME AS ``reset_pending_errors.py``
    That script targets ``wf_pending_actions`` (the per-action dispatch queue).
    This one targets ``workflow_runs`` (the per-customer run state machine).
    Different tables; both can hold ERROR rows independently.

CLI:
    python3 scripts/reset_error_runs.py --dry-run      # list runs + side_effect, change nothing
    python3 scripts/reset_error_runs.py                # reset transient (lock/timeout) ERROR runs
    python3 scripts/reset_error_runs.py --all-errors   # reset EVERY ERROR run (use after diagnosis)
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")

# Latest side_effect substrings that mark a retry-safe transient failure.
_TRANSIENT_MARKERS = ("database is locked", "operationalerror", "timeout")

# id + customer + node-type + latest side_effect for every ERROR run.
_SELECT_ERROR_RUNS = """
    SELECT r.id, r.customer_id, r.current_node_type, substr(r.updated_at_ist,1,19),
           (SELECT l.side_effect FROM workflow_node_log l
            WHERE l.run_id = r.id ORDER BY l.id DESC LIMIT 1) AS last_side_effect
    FROM workflow_runs r
    WHERE r.status='ERROR'
    ORDER BY r.updated_at_ist
"""


def _is_transient(side_effect: str | None) -> bool:
    if not side_effect:
        return False  # no log → cannot confirm transient → leave for explicit --all-errors
    se = side_effect.lower()
    return any(m in se for m in _TRANSIENT_MARKERS)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--all-errors", action="store_true",
        help="reset EVERY ERROR run, not just transient lock/timeout ones — "
             "only after diagnosing per runbook section B (logic-bug runs will "
             "just re-error and churn the alert).",
    )
    args = ap.parse_args()

    db_path = Path(args.workflow_db)
    if not db_path.exists():
        print(f"[ERROR] workflow db not found: {db_path}")
        return 2

    # IST-naive "%Y-%m-%d %H:%M:%S" — the engine stores all *_at_ist columns in
    # this shape; the AWS host runs UTC so a naive now() would mislabel the row.
    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    conn = None
    try:
        # Generous busy timeout (== sqlite busy_timeout): this script exists
        # *because* the DB contends, so the UPDATE must not lose the same lock
        # race it is healing. isolation_level=None → we drive transactions
        # explicitly with BEGIN IMMEDIATE so the read+write is atomic against the
        # executor's own BEGIN IMMEDIATE tick (no read-then-write race window).
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.isolation_level = None

        if args.dry_run:
            rows = conn.execute(_SELECT_ERROR_RUNS).fetchall()
            _report(rows, args.all_errors)
            print("[DRY-RUN] no changes made")
            return 0

        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(_SELECT_ERROR_RUNS).fetchall()
        targets = [r for r in rows if args.all_errors or _is_transient(r[4])]
        _report(rows, args.all_errors)
        if not targets:
            conn.execute("COMMIT")
            print("[OK] no eligible ERROR runs to reset")
            return 0

        ids = [r[0] for r in targets]
        # Only "?" placeholders are interpolated into the SQL text — never a
        # value. All values (now_str + ids) are bound parameters below. This is
        # the standard safe pattern for a variable-length IN (...) clause.
        placeholders = ",".join("?" * len(ids))
        # NULL ready_at_ist: the executor gates ACTIVE pickup on
        # (ready_at_ist IS NULL OR ready_at_ist <= now). A stale FUTURE park
        # deadline left on a re-activated run would strand it invisibly (ACTIVE
        # but never selected) AND drop it off the B_RUN_ERROR alert. This matches
        # the engine's own contract: every ACTIVE advance NULLs ready_at_ist.
        update_sql = (  # stashfin-lint: ignore — only "?" placeholders interpolated; values are bound params
            "UPDATE workflow_runs SET status='ACTIVE', ready_at_ist=NULL, updated_at_ist=? "
            f"WHERE id IN ({placeholders}) AND status='ERROR'"
        )
        cur = conn.execute(update_sql, (now_str, *ids))
        conn.execute("COMMIT")

        print(f"[OK] reset {cur.rowcount} run(s) ERROR -> ACTIVE (ready_at_ist nulled) at {now_str} IST")
        if cur.rowcount != len(ids):
            # Authoritative rowcount diverged from the in-txn target set — a
            # concurrent operator tool transitioned a row. Surface, don't hide.
            print(f"[WARN] expected to reset {len(ids)} but updated {cur.rowcount} "
                  f"— concurrent operator/tooling race? re-run --dry-run to confirm state")
            return 1
        return 0
    except Exception:
        if conn is not None:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error as rb_err:
                # Best-effort rollback; do not let a rollback failure mask the
                # original exception (re-raised below) — but surface it.
                print(f"[WARN] rollback after error failed: {rb_err}")
        raise
    finally:
        if conn is not None:
            conn.close()


def _report(rows: list, all_errors: bool) -> None:
    print(f"[INFO] ERROR runs found: {len(rows)}")
    for rid, cust, ntype, updated, se in rows:
        transient = _is_transient(se)
        will = "RESET" if (all_errors or transient) else "SKIP (non-transient; needs --all-errors)"
        se_short = (se or "(no node log)").replace("\n", " ")[:80]
        print(f"  run id={rid} cust={cust} node_type={ntype} updated={updated} "
              f"transient={transient} → {will}\n      last_side_effect: {se_short}")


if __name__ == "__main__":
    raise SystemExit(main())
