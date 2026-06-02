#!/usr/bin/env python3
"""Set max_new_enrollments_per_day on a workflow. One-shot admin tool.

Usage:
    python3 scripts/set_enrollment_cap.py --cap 50000
    python3 scripts/set_enrollment_cap.py --cap 50000 --workflow-id 1
"""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--workflow-id", type=int, default=1)
    ap.add_argument("--cap", type=int, required=True)
    args = ap.parse_args()

    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)
        old = conn.execute(
            "SELECT max_new_enrollments_per_day FROM workflows WHERE id=?",
            (args.workflow_id,)
        ).fetchone()
        if old is None:
            print(f"[FAIL] workflow id={args.workflow_id} not found", file=sys.stderr)
            raise SystemExit(1)
        conn.execute(
            "UPDATE workflows SET max_new_enrollments_per_day=? WHERE id=?",
            (args.cap, args.workflow_id),
        )
        conn.commit()
        new = conn.execute(
            "SELECT max_new_enrollments_per_day FROM workflows WHERE id=?",
            (args.workflow_id,)
        ).fetchone()[0]
        print(f"[OK] workflow_id={args.workflow_id} enrollment cap: {old[0]} → {new}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
