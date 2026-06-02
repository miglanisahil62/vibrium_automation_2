"""Reset ERROR rows in wf_pending_actions back to PENDING for retry."""
import sqlite3, sys, argparse
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)
        n = conn.execute("SELECT COUNT(*) FROM wf_pending_actions WHERE status='ERROR'").fetchone()[0]
        print(f"ERROR rows to reset: {n}")
        if args.dry_run:
            print("DRY-RUN — no changes")
            return
        stmt = "UPDATE wf_pending_actions SET status='PENDING', last_error=NULL WHERE status='ERROR'"
        conn.execute(stmt)
        conn.commit()
        print(f"Reset {n} rows → PENDING")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
