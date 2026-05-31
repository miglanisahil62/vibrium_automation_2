#!/usr/bin/env python3
"""Flip the workflow from shadow mode to live.

Requires explicit authorization in chat before running.
Sets workflows.shadow_mode = 0 for the named workflow.

CLI:
    python3 scripts/go_live.py [--workflow-db state/workflow.db] [--name "VB Collections v2"]
    python3 scripts/go_live.py --dry-run   # show current state only; no writes
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
IST = ZoneInfo("Asia/Kolkata")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--name", default="VB Collections v2")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show current state only; do not write.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, name, status, shadow_mode, active_version_id "
            "FROM workflows WHERE name=? AND status!='ARCHIVED' ORDER BY id DESC LIMIT 1",
            (args.name,),
        ).fetchone()
        if row is None:
            print(f"[FAIL] no active workflow named {args.name!r}", file=sys.stderr)
            raise SystemExit(1)
        print(f"[INFO] current state: {dict(row)}")
        if row["shadow_mode"] == 0:
            print("[INFO] shadow_mode already 0 — nothing to do.")
            return
        if args.dry_run:
            print("[DRY-RUN] would set shadow_mode=0; no write.")
            return
        conn.execute(
            "UPDATE workflows SET shadow_mode=0 WHERE id=?",
            (int(row["id"]),),
        )
        conn.commit()
        after = conn.execute(
            "SELECT id, name, status, shadow_mode FROM workflows WHERE id=?",
            (int(row["id"]),),
        ).fetchone()
        print(f"[OK] {now} — LIVE: {dict(after)}")
        print("[OK] Calls will fire from the next 08:00–19:00 IST window.")
        audit = _REPO / "state" / "go_live_audit.log"
        audit.parent.mkdir(parents=True, exist_ok=True)
        with open(audit, "a") as f:
            f.write(f"{now} — shadow_mode flipped to LIVE: {dict(after)}\n")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
