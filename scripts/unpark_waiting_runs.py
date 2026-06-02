"""Reset ready_at_ist for WAITING runs to now so they fire immediately.

Used when segments change from T+1 to T+0 and already-parked runs need
to be advanced without waiting overnight.

CLI:
    python3 scripts/unpark_waiting_runs.py              # move all future-parked runs to now
    python3 scripts/unpark_waiting_runs.py --dry-run    # show count, no changes
"""
from __future__ import annotations
import argparse, sqlite3, sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now(IST).strftime("%Y-%m-%d")

    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)
        n = conn.execute(
            "SELECT COUNT(*) FROM workflow_runs "
            "WHERE status='WAITING' AND substr(ready_at_ist,1,10) > ?",
            (today,),
        ).fetchone()[0]
        print(f"[INFO] runs parked for future dates: {n}")
        if args.dry_run:
            print("[DRY-RUN] no changes made")
            return
        # Include updated_at_ist so the Phase 8.5 alert detector (C_RUN_WAITING)
        # does not treat these freshly-serviced runs as stale. Use cursor.rowcount
        # as the authoritative count (pre-flight SELECT can race with the executor).
        cur = conn.execute(
            "UPDATE workflow_runs SET ready_at_ist=?, updated_at_ist=? "
            "WHERE status='WAITING' AND substr(ready_at_ist,1,10) > ?",
            (now_str, now_str, today),
        )
        conn.commit()
        print(f"[OK] reset {cur.rowcount} runs → ready_at_ist={now_str}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
