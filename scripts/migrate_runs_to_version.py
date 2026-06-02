"""Migrate WAIT_UNTIL WAITING runs from one graph version to another.

Used when a graph version is updated (e.g. T+1 → T+0 entry timing) and
already-enrolled runs are pinned to the old version. Migrates only WAIT_UNTIL
WAITING runs — runs that are mid-call-loop (AWAIT_DISPOSITION etc.) are left
on their original version to avoid breaking in-flight state.

CLI:
    python3 scripts/migrate_runs_to_version.py --to-version 2
    python3 scripts/migrate_runs_to_version.py --to-version 2 --dry-run
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
    ap.add_argument("--to-version", type=int, required=True)
    ap.add_argument("--from-version", type=int, default=None,
                    help="Only migrate runs on this version (default: all except to-version)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)

        if args.from_version is not None:
            n = conn.execute(
                "SELECT COUNT(*) FROM workflow_runs "
                "WHERE status='WAITING' AND current_node_type='WAIT_UNTIL' "
                "AND version_id=? AND version_id != ?",
                (args.from_version, args.to_version),
            ).fetchone()[0]
        else:
            n = conn.execute(
                "SELECT COUNT(*) FROM workflow_runs "
                "WHERE status='WAITING' AND current_node_type='WAIT_UNTIL' "
                "AND version_id != ?",
                (args.to_version,),
            ).fetchone()[0]
        print(f"[INFO] WAIT_UNTIL WAITING runs to migrate: {n}")

        if args.dry_run:
            print("[DRY-RUN] no changes")
            return

        if args.from_version is not None:
            cur = conn.execute(
                "UPDATE workflow_runs "
                "SET version_id=?, ready_at_ist=?, updated_at_ist=? "
                "WHERE status='WAITING' AND current_node_type='WAIT_UNTIL' "
                "AND version_id=? AND version_id != ?",
                (args.to_version, now_str, now_str, args.from_version, args.to_version),
            )
        else:
            cur = conn.execute(
                "UPDATE workflow_runs "
                "SET version_id=?, ready_at_ist=?, updated_at_ist=? "
                "WHERE status='WAITING' AND current_node_type='WAIT_UNTIL' "
                "AND version_id != ?",
                (args.to_version, now_str, now_str, args.to_version),
            )
        conn.commit()
        print(f"[OK] migrated {cur.rowcount} runs → version_id={args.to_version}, ready_at_ist={now_str}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
