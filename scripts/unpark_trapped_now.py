"""Unpark WAITING runs whose ``ready_at_ist`` is in the FUTURE → set it to now,
so the executor re-evaluates them on the next tick instead of waiting out the
park.

WHEN TO USE
    After deploying a WAIT_UNTIL fix that releases runs which were wrongly parked
    (e.g. the relative-offset treadmill fixed 2026-06-08): the trapped runs carry
    a near-future ``ready_at_ist`` and would otherwise only drain as each park
    naturally elapses. This pulls them all to "evaluate now". With the fix in
    place, the WAIT_UNTIL handler then correctly ADVANCES the genuinely-overdue
    runs (entry+offset already past) and RE-PARKS the legitimately-future ones
    (entry+offset still future) — so running this is safe either way.

DIFFERENCE FROM ``unpark_waiting_runs.py``
    That script only targets runs parked for a future DATE (``substr(ready_at_ist
    ,1,10) > today``) — the T+1→T+0 segment-change case. This one targets any
    future ``ready_at_ist`` (including later TODAY), which is what the +1h
    same-day-retry treadmill produces.

CLI:
    python3 scripts/unpark_trapped_now.py --dry-run                 # count only
    python3 scripts/unpark_trapped_now.py                           # all WAITING future-parked
    python3 scripts/unpark_trapped_now.py --node-type WAIT_UNTIL    # restrict to a node type
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")

# Literal SQL (no runtime interpolation). Two static variants per operation —
# with and without the optional current_node_type filter. All values are bound
# parameters; nothing is string-formatted into the SQL.
_COUNT_SQL = (
    "SELECT COUNT(*) FROM workflow_runs WHERE status='WAITING' AND ready_at_ist > ?"
)
_COUNT_SQL_NODE = (
    "SELECT COUNT(*) FROM workflow_runs WHERE status='WAITING' AND ready_at_ist > ? "
    "AND current_node_type = ?"
)
_UPDATE_SQL = (
    "UPDATE workflow_runs SET ready_at_ist = ?, updated_at_ist = ? "
    "WHERE status='WAITING' AND ready_at_ist > ?"
)
_UPDATE_SQL_NODE = (
    "UPDATE workflow_runs SET ready_at_ist = ?, updated_at_ist = ? "
    "WHERE status='WAITING' AND ready_at_ist > ? AND current_node_type = ?"
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--node-type", default=None,
                    help="restrict to a current_node_type (e.g. WAIT_UNTIL); default = any")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.workflow_db)
    if not db_path.exists():
        print(f"[ERROR] workflow db not found: {db_path}")
        return 2

    # IST-naive "%Y-%m-%d %H:%M:%S" — matches the engine's *_at_ist column shape;
    # the AWS host runs UTC so a naive now() would mislabel the row.
    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    if args.node_type:
        count_sql, count_params = _COUNT_SQL_NODE, (now_str, args.node_type)
        update_sql, update_params = _UPDATE_SQL_NODE, (now_str, now_str, now_str, args.node_type)
    else:
        count_sql, count_params = _COUNT_SQL, (now_str,)
        update_sql, update_params = _UPDATE_SQL, (now_str, now_str, now_str)
    suffix = f" (node_type={args.node_type})" if args.node_type else ""

    conn = None
    try:
        # busy_timeout via connect timeout; explicit BEGIN IMMEDIATE so the
        # count+update read-modify-write is atomic against the executor's own
        # BEGIN IMMEDIATE tick (no read-then-write race window).
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.isolation_level = None

        if args.dry_run:
            n = conn.execute(count_sql, count_params).fetchone()[0]
            print(f"[DRY-RUN] would unpark {n} future-parked WAITING run(s){suffix} [db={db_path}]")
            return 0

        conn.execute("BEGIN IMMEDIATE")
        n = conn.execute(count_sql, count_params).fetchone()[0]
        # Set updated_at_ist too, so freshly-serviced runs are not re-flagged stale
        # by the C_RUN_WAITING alert detector. Do NOT null ready_at_ist — we set it
        # to now so the executor picks the run up; the WAIT_UNTIL handler then
        # decides advance-vs-repark from entered_node_at_ist.
        cur = conn.execute(update_sql, update_params)
        conn.execute("COMMIT")

        print(f"[OK] unparked {cur.rowcount} run(s) → ready_at_ist={now_str} IST{suffix} [db={db_path}]")
        if cur.rowcount != n:
            print(f"[WARN] pre-count {n} != updated {cur.rowcount} — concurrent executor/operator race")
            return 1
        return 0
    except Exception:
        if conn is not None:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error as rb_err:
                print(f"[WARN] rollback after error failed: {rb_err}")
        raise
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
