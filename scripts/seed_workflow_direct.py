#!/usr/bin/env python3
"""Seed a workflow directly into state/workflow.db — no ops console / API.

The console-based ``seed_workflow.py`` POSTs to http://127.0.0.1:8550. On the
AWS server no console runs, so this script performs the same
create → save-version → activate flow as direct DB writes against the engine's
own SQLite DB. It is the canonical server-side seeding path.

Idempotent: an existing non-ARCHIVED workflow of the same name is reused; a new
version row is appended and made active. ``shadow_mode`` is set to 1 (safe) on
first create and left untouched on re-seed — going live is a separate, explicit
console/DB flip, never a side effect of seeding.

CLI:
    python3 scripts/seed_workflow_direct.py
    python3 scripts/seed_workflow_direct.py --workflow-db state/workflow.db \
        --graph seed_workflows/vb_collections_v2.json --name "VB Collections v2"
    python3 scripts/seed_workflow_direct.py --dry-run   # validate + report; no writes
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

IST = ZoneInfo("Asia/Kolkata")


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--workflow-db", default=str(_REPO / "state" / "workflow.db"))
    ap.add_argument("--graph", default=str(_REPO / "seed_workflows" / "vb_collections_v2.json"))
    ap.add_argument("--name", default="VB Collections v2")
    ap.add_argument("--dry-run", action="store_true",
                    help="Validate the graph and report intent; write nothing.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    graph_path = Path(args.graph)
    if not graph_path.exists():
        print(f"[FAIL] graph not found: {graph_path}", file=sys.stderr)
        raise SystemExit(1)
    graph = json.loads(graph_path.read_text())

    # Validate against the engine validator before touching the DB.
    from workflow.validation import validate_graph
    res = validate_graph(graph)
    valid = res.valid if hasattr(res, "valid") else res
    errors = getattr(res, "errors", []) if hasattr(res, "errors") else []
    if not valid:
        print(f"[FAIL] graph invalid: {errors}", file=sys.stderr)
        raise SystemExit(1)
    n_nodes = len(graph.get("nodes", []))
    print(f"[INFO] graph valid: {n_nodes} nodes, name={args.name!r}, db={args.workflow_db}")

    if args.dry_run:
        print("[DRY-RUN] would create/append+activate; no writes made.")
        return

    graph_json = json.dumps(graph)
    ts = _now_ist()
    conn = None
    try:
        conn = sqlite3.connect(args.workflow_db, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")

        existing = conn.execute(
            "SELECT id, shadow_mode FROM workflows WHERE name=? AND status!='ARCHIVED' "
            "ORDER BY id DESC LIMIT 1",
            (args.name,),
        ).fetchone()

        if existing is None:
            cur = conn.execute(
                "INSERT INTO workflows "
                "(name, status, active_version_id, shadow_mode, requires_approval, "
                " max_new_enrollments_per_day, created_at_ist, created_by) "
                "VALUES (?, 'ACTIVE', NULL, 1, 1, 1000, ?, 'server-seed')",
                (args.name, ts),
            )
            workflow_id = int(cur.lastrowid)
            print(f"[OK] created workflow id={workflow_id} (shadow_mode=1)")
        else:
            workflow_id = int(existing["id"])
            print(f"[OK] reusing workflow id={workflow_id} (shadow_mode={existing['shadow_mode']})")

        next_version = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM workflow_versions WHERE workflow_id=?",
            (workflow_id,),
        ).fetchone()["v"]

        # validation_status MUST be uppercase 'VALID' to match the console's
        # canonical contract (services_workflow.activate() rejects anything
        # else). Both writers target the same state/workflow.db.
        cur = conn.execute(
            "INSERT INTO workflow_versions "
            "(workflow_id, version, graph_json, validation_status, "
            " approved_at_ist, approved_by, created_at_ist, created_by) "
            "VALUES (?, ?, ?, 'VALID', ?, 'server-seed', ?, 'server-seed')",
            (workflow_id, int(next_version), graph_json, ts, ts),
        )
        version_id = int(cur.lastrowid)

        # Repoint active_version_id only. Do NOT force status='ACTIVE' here —
        # the first-create INSERT already set ACTIVE, and on a re-seed we must
        # preserve an operator's deliberate PAUSED state (un-pausing would
        # silently restart enrollment). Re-seed is status-neutral.
        conn.execute(
            "UPDATE workflows SET active_version_id=? WHERE id=?",
            (version_id, workflow_id),
        )
        conn.commit()

        row = conn.execute(
            "SELECT id, name, status, active_version_id, shadow_mode FROM workflows WHERE id=?",
            (workflow_id,),
        ).fetchone()
        print(f"[OK] activated: workflow_id={row['id']} name={row['name']!r} "
              f"status={row['status']} active_version_id={row['active_version_id']} "
              f"version={next_version} shadow_mode={row['shadow_mode']}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
