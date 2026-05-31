"""Seed vb_collections_v1 workflow into the ops console via API.

Usage (from vibrium-workflow repo root):
    python scripts/seed_workflow.py [--dry-run] [--console-url URL] [--approver NAME]

Steps:
    1. POST /api/workflows          — create DRAFT named "VB Collections v1"
    2. POST /api/workflows/{id}/version — upload graph_json from seed file
    3. POST /api/workflows/{id}/activate — flip to ACTIVE (shadow_mode stays 1)

Shadow mode is ON by default (enforced server-side). The workflow will fire
SHADOW_FIRED rows in wf_pending_actions — no live CT triggers until an operator
manually flips shadow_mode=0 via the console.

Idempotency: if a workflow named "VB Collections v1" already exists (status ≠
ARCHIVED), the script skips creation and re-uses it. It always saves a new
version.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

_SEED_GRAPH = Path(__file__).resolve().parent.parent / "seed_workflows" / "vb_collections_v1.json"
_DEFAULT_URL = "http://127.0.0.1:8550"
_WORKFLOW_NAME = "VB Collections v1"


def _die(msg: str) -> None:
    print(f"[FAIL] {msg}", file=sys.stderr)
    raise SystemExit(1)


def _post(session: requests.Session, url: str, body: dict, *, dry_run: bool, label: str) -> dict:
    if dry_run:
        print(f"[DRY-RUN] POST {url}  body={json.dumps(body)[:200]}")
        return {"ok": True, "_dry_run": True}
    r = session.post(url, json=body, timeout=(5, 30))
    try:
        data = r.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        _die(f"{label}: non-JSON response {r.status_code}: {r.text[:300]}")
    if not r.ok:
        _die(f"{label}: HTTP {r.status_code} — {data}")
    return data  # type: ignore[return-value]  # _die raises; unreachable on error


def _find_existing(session: requests.Session, base: str) -> "int | None":
    r = session.get(f"{base}/api/workflows", timeout=(5, 30))
    if not r.ok:
        _die(
            f"_find_existing: GET /api/workflows returned {r.status_code} — "
            "aborting to avoid duplicate create. Is the console running?"
        )
    data = r.json()
    for wf in data.get("workflows", []):
        if wf.get("name") == _WORKFLOW_NAME and wf.get("status") != "ARCHIVED":
            return int(wf["id"])
    return None


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Seed vb_collections_v1 workflow via API")
    p.add_argument("--dry-run", action="store_true", help="Log actions without firing HTTP")
    p.add_argument("--console-url", default=_DEFAULT_URL, help=f"Ops console base URL (default: {_DEFAULT_URL})")
    p.add_argument("--approver", default="seed-script", help="X-Workflow-Approver value (default: seed-script)")
    args = p.parse_args(argv)

    base = args.console_url.rstrip("/")
    dry_run: bool = args.dry_run

    if not _SEED_GRAPH.exists():
        _die(f"Seed graph not found: {_SEED_GRAPH}. Run: python seed_workflows/generate_vb_collections_v1.py")

    graph = json.loads(_SEED_GRAPH.read_text())
    nodes = graph.get("nodes")
    if nodes is None:
        _die(f"Seed JSON at {_SEED_GRAPH} has no 'nodes' key — wrong file?")
    print(f"[INFO] Loaded seed graph: {len(nodes)} nodes from {_SEED_GRAPH.name}")

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["X-Workflow-Approver"] = args.approver

    # ── Step 1: Create or reuse workflow ────────────────────────────────────
    if not dry_run:
        existing_id = _find_existing(session, base)
    else:
        existing_id = None

    if existing_id:
        print(f"[INFO] Workflow '{_WORKFLOW_NAME}' already exists: id={existing_id} — reusing")
        workflow_id = existing_id
    else:
        data = _post(
            session, f"{base}/api/workflows",
            {"name": _WORKFLOW_NAME, "created_by": "seed-script"},
            dry_run=dry_run, label="create workflow",
        )
        if dry_run:
            workflow_id = 0
        else:
            wf = data.get("workflow") or {}
            workflow_id = wf.get("id")
            if workflow_id is None:
                _die(f"create workflow: unexpected response shape — {data}")
            print(f"[OK] Created workflow id={workflow_id} name='{_WORKFLOW_NAME}'")

    # ── Step 2: Save version with the seed graph ─────────────────────────────
    save_data = _post(
        session, f"{base}/api/workflows/{workflow_id}/version",
        {"graph_json": graph, "created_by": "seed-script"},
        dry_run=dry_run, label="save version",
    )
    if dry_run:
        version_id = 0
    else:
        ver = save_data.get("version") or {}
        version_id = ver.get("id")
        if version_id is None:
            _die(f"save version: unexpected response shape — {save_data}")
        errors = save_data.get("validation_errors") or []
        if errors:
            _die(f"Validation errors: {errors}")
        print(f"[OK] Saved version id={version_id} validation_status={ver.get('validation_status')}")

    # ── Step 3: Activate ────────────────────────────────────────────────────
    act_data = _post(
        session, f"{base}/api/workflows/{workflow_id}/activate",
        {"version_id": version_id},
        dry_run=dry_run, label="activate",
    )
    if not dry_run:
        print(f"[OK] Activated workflow id={workflow_id} version={version_id} — shadow_mode=1 (no live CT fires)")
        print(f"     View at: {base}/workflows/{workflow_id}")
    else:
        print("[DRY-RUN] Would activate. Done.")

    print(f"\n[SUMMARY] workflow_id={workflow_id}  version_id={version_id}  shadow_mode=ON")
    print("           Run scripts/e2e_shadow_test.py to verify shadow gate hard rules.")


if __name__ == "__main__":
    main()
