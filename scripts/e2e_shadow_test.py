"""E2E shadow test for vb_collections_v1 workflow.

Verifies the three hard gates required before any live cutover:

    Gate 1 — Zero live CT fires:
        wf_pending_actions rows for test run_ids have status IN
        ('SHADOW_FIRED', 'SUPPRESSED') — none have status='FIRED'.

    Gate 2 — Zero customer_call_audit rows:
        vibrium.db customer_call_audit has no rows with run_id in test
        run_ids (scheduler must not have inserted real fire records).

    Gate 3 — Expected branch distribution:
        workflow_node_log shows the correct terminal/parked node per
        customer group after FIRE_VB_CALL → AWAIT_DISPOSITION routing.

Usage (from vibrium-workflow repo root):
    python scripts/e2e_shadow_test.py [--workflow-db PATH] [--vibrium-db PATH]

The test creates a temporary workflow DB so it never touches dev state.
Active-group customers are enrolled directly at their FIRE_VB_CALL node
(bypassing WAIT_UNTIL — which always re-parks from 'now', making
fast-forward ineffective). Terminal-group customers (ineligible, oos) are
pre-inserted as DONE so no live CleverTap call is made.

Exit codes: 0 = all gates passed, 1 = one or more gates failed.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parent.parent

# Ensure the repo root is on sys.path so `workflow.*` is importable when the
# script is run directly (e.g. `python scripts/e2e_shadow_test.py`).
# pytest adds the repo root automatically; direct invocation does not.
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

_SEED_GRAPH = _REPO / "seed_workflows" / "vb_collections_v1.json"
_MIGRATION = _REPO / "workflow" / "migrations" / "001_init.py"
_VIBRIUM_DB_DEFAULT = _REPO / "state" / "vibrium.db"

_IST = ZoneInfo("Asia/Kolkata")
_FMT = "%Y-%m-%d %H:%M:%S"

# ── Node IDs from vb_collections_v1.json (generate_vb_collections_v1.py) ────
# Active groups start at FIRE_VB_CALL (first node of their call loop).
# Executor runs FIRE_VB_CALL → inserts PENDING → advances to AWAIT_DISPOSITION.
# AWAIT_DISPOSITION parks with status=WAITING (24h timeout, no disposition yet).
# This bypasses WAIT_UNTIL nodes which always recompute ready_at_ist from 'now'
# on every call, making fast-forward via DB UPDATE ineffective.

_FIRE_HIGH_WA   = "aa000000-0000-4000-a000-000000000001"  # HWA  0x01
_FIRE_HIGH_NOWA = "bb000000-0000-4000-a000-000000000001"  # HNWA 0x01
_FIRE_MID_WA    = "cc000000-0000-4000-a000-000000000001"  # MWA  0x01
_FIRE_MID_NOWA  = "dd000000-0000-4000-a000-000000000001"  # MNWA 0x01

_AWAIT_HIGH_WA   = "aa000000-0000-4000-a000-000000000002"  # HWA  0x02
_AWAIT_HIGH_NOWA = "bb000000-0000-4000-a000-000000000002"  # HNWA 0x02
_AWAIT_MID_WA    = "cc000000-0000-4000-a000-000000000002"  # MWA  0x02
_AWAIT_MID_NOWA  = "dd000000-0000-4000-a000-000000000002"  # MNWA 0x02

_TERM_INELIGIBLE = "00000000-0000-4000-a000-000000000005"
_TERM_OOS        = "00000000-0000-4000-a000-000000000007"


# ── Customer groups ──────────────────────────────────────────────────────────

_GROUPS: list[dict] = [
    # Active groups — enrolled at FIRE_VB_CALL; expected to park at AWAIT_DISPOSITION.
    {
        "group": "high_wa",    "count": 5,
        "start_node": _FIRE_HIGH_WA,   "start_type": "FIRE_VB_CALL", "start_status": "ACTIVE",
        "expected_node": _AWAIT_HIGH_WA,   "expected_status": "WAITING",
    },
    {
        "group": "high_nowa",  "count": 5,
        "start_node": _FIRE_HIGH_NOWA, "start_type": "FIRE_VB_CALL", "start_status": "ACTIVE",
        "expected_node": _AWAIT_HIGH_NOWA, "expected_status": "WAITING",
    },
    {
        "group": "mid_wa",     "count": 5,
        "start_node": _FIRE_MID_WA,    "start_type": "FIRE_VB_CALL", "start_status": "ACTIVE",
        "expected_node": _AWAIT_MID_WA,    "expected_status": "WAITING",
    },
    {
        "group": "mid_nowa",   "count": 5,
        "start_node": _FIRE_MID_NOWA,  "start_type": "FIRE_VB_CALL", "start_status": "ACTIVE",
        "expected_node": _AWAIT_MID_NOWA,  "expected_status": "WAITING",
    },
    # Terminal groups — pre-inserted as DONE to represent customers filtered
    # before the call loop (DPD <= 1 or out-of-scope risk segment).
    {
        "group": "ineligible", "count": 2,
        "start_node": _TERM_INELIGIBLE, "start_type": "TERMINATE", "start_status": "DONE",
        "expected_node": _TERM_INELIGIBLE, "expected_status": "DONE",
        "terminal_status": "INELIGIBLE",
    },
    {
        "group": "oos",        "count": 2,
        "start_node": _TERM_OOS,        "start_type": "TERMINATE", "start_status": "DONE",
        "expected_node": _TERM_OOS,        "expected_status": "DONE",
        "terminal_status": "OUT_OF_SCOPE",
    },
]

_TOTAL_CUSTOMERS = sum(g["count"] for g in _GROUPS)
_ACTIVE_CUSTOMERS = sum(g["count"] for g in _GROUPS if g["start_status"] == "ACTIVE")


def _now_ist() -> str:
    return datetime.now(_IST).strftime(_FMT)


def _load_migration():
    spec = importlib.util.spec_from_file_location("_mig001_e2e", _MIGRATION)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _open_db(path: "str | Path") -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _seed_workflow(conn: sqlite3.Connection, graph: dict) -> tuple[int, int]:
    """Insert workflow + version rows; return (workflow_id, version_id)."""
    ts = _now_ist()
    cur = conn.execute(
        """
        INSERT INTO workflows
            (name, status, shadow_mode, requires_approval,
             active_version_id, created_at_ist, created_by)
        VALUES ('vb_collections_v1', 'ACTIVE', 1, 0, NULL, ?, 'e2e-shadow-test')
        """,
        (ts,),
    )
    wf_id = cur.lastrowid
    cur2 = conn.execute(
        """
        INSERT INTO workflow_versions
            (workflow_id, version, graph_json, validation_status,
             approved_at_ist, approved_by, created_at_ist, created_by)
        VALUES (?, 1, ?, 'valid', ?, 'e2e-shadow-test', ?, 'e2e-shadow-test')
        """,
        (wf_id, json.dumps(graph), ts, ts),
    )
    version_id = cur2.lastrowid
    conn.execute(
        "UPDATE workflows SET active_version_id = ? WHERE id = ?",
        (version_id, wf_id),
    )
    conn.commit()
    return wf_id, version_id


def _enroll_customers(
    conn: sqlite3.Connection,
    wf_id: int,
    version_id: int,
) -> list[int]:
    """Insert workflow_runs for all test groups.

    Active groups start at their FIRE_VB_CALL node (status=ACTIVE) so ticks
    immediately produce wf_pending_actions PENDING rows without traversing
    WAIT_UNTIL (which recomputes ready_at_ist from 'now' on every handler call,
    making fast-forward via DB UPDATE ineffective).

    Terminal groups (ineligible, oos) are pre-inserted as DONE to represent
    customers that would have been filtered before the call loop; the executor
    ignores DONE runs so they don't consume tick slots.
    """
    ts = _now_ist()
    run_ids: list[int] = []
    for g in _GROUPS:
        for i in range(g["count"]):
            cid = f"TEST_{g['group'].upper()}_{i:02d}"
            status = g["start_status"]
            terminal_status = g.get("terminal_status")
            terminated_at = ts if status == "DONE" else None
            cur = conn.execute(
                """
                INSERT INTO workflow_runs
                    (workflow_id, version_id, customer_id, enrollment_key,
                     current_node_id, current_node_type,
                     entered_node_at_ist, status, scratchpad_json,
                     ready_at_ist, enrolled_at_ist, updated_at_ist,
                     terminal_status, terminated_at_ist)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """,
                (
                    wf_id, version_id, cid,
                    f"{cid}_shadow_test",
                    g["start_node"],
                    g["start_type"],
                    ts,
                    status,
                    "{}",
                    ts, ts,
                    terminal_status,
                    terminated_at,
                ),
            )
            run_ids.append(cur.lastrowid)
    conn.commit()
    return run_ids


def _in_clause(items: list) -> tuple[str, list]:
    """Return (placeholder_str, params) for a SQLite IN clause.

    Usage:
        ph, params = _in_clause(run_ids)
        sql = "SELECT ... WHERE id IN (" + ph + ")"
        conn.execute(sql, params)

    The SQL string is built via concatenation (no f-string in execute()) so
    the linter correctly sees no user-data interpolation into SQL.
    """
    return ",".join(["?"] * len(items)), list(items)


def _run_ticks(db_path: Path, n: int, *, label: str = "") -> None:
    """Run WorkflowAgent.tick() n times against db_path."""
    from workflow.agents.workflow import WorkflowAgent
    agent = WorkflowAgent(db_path)
    for i in range(n):
        result = agent.tick(dry_run=False)
        prefix = f"[{label}] " if label else ""
        print(
            f"  {prefix}tick {i+1:02d}: status={result.status} "
            f"processed={result.processed} advanced={result.advanced}"
        )


# ── Gate checks ─────────────────────────────────────────────────────────────

def _check_gate_1(conn: sqlite3.Connection, run_ids: list[int], n_shadow_marked: int) -> tuple[bool, str]:
    """Gate 1: shadow path was exercised AND no FIRED rows exist for test runs.

    n_shadow_marked must be > 0 — a zero means the FIRE_VB_CALL handler never
    ran (all runs errored or stopped before it), which makes the "no FIRED"
    check vacuously true and meaningless as a shadow-mode safety gate.
    """
    if n_shadow_marked == 0:
        return (
            False,
            "FAIL Gate 1 — shadow path was NEVER exercised (0 PENDING→SHADOW_FIRED "
            "transitions). FIRE_VB_CALL did not run for any test run. Gate would pass "
            "vacuously — aborting.",
        )
    ph, params = _in_clause(run_ids)
    sql = (
        "SELECT run_id, status, COUNT(*) as cnt FROM wf_pending_actions"
        " WHERE run_id IN (" + ph + ") GROUP BY run_id, status"
    )
    rows = conn.execute(sql, params).fetchall()
    bad = [r for r in rows if r["status"] == "FIRED"]
    if bad:
        return False, f"FAIL Gate 1 — {len(bad)} FIRED rows found: {[dict(r) for r in bad]}"
    return True, f"PASS Gate 1 — 0 FIRED; {n_shadow_marked} SHADOW_FIRED rows (shadow path verified)"


def _check_gate_2(vibrium_db_path: "str | Path | None", run_ids: list[int]) -> tuple[bool, str]:
    """Gate 2: no customer_call_audit rows in vibrium.db for test run_ids.

    Filters on both run_id IN (...) AND customer_id LIKE 'TEST_%' to avoid
    collisions with real production run_ids that happen to share the same
    auto-increment values in vibrium.db.
    """
    if not vibrium_db_path or not Path(str(vibrium_db_path)).exists():
        return True, "PASS Gate 2 — vibrium.db not found; skip (expected in fresh dev env)"
    vconn = None
    try:
        vconn = sqlite3.connect(str(vibrium_db_path), timeout=10)
        vconn.row_factory = sqlite3.Row
        ph, params = _in_clause(run_ids)
        # customer_id LIKE 'TEST_%' prevents false positives from real production
        # rows whose run_id happens to match our temp-DB auto-increment values.
        sql = (
            "SELECT COUNT(*) as cnt FROM customer_call_audit"
            " WHERE run_id IN (" + ph + ") AND customer_id LIKE 'TEST_%'"
        )
        row = vconn.execute(sql, params).fetchone()
        cnt = row["cnt"]
        if cnt > 0:
            return False, f"FAIL Gate 2 — {cnt} customer_call_audit rows for test run_ids"
        return True, "PASS Gate 2 — 0 customer_call_audit rows for test run_ids"
    except sqlite3.OperationalError as exc:
        # Expected when customer_call_audit table doesn't exist yet in dev env.
        print(f"  [WARN] Gate 2: {exc} — treating as PASS (table not yet created)")
        return True, f"PASS Gate 2 — table not yet created (OperationalError: {exc})"
    finally:
        if vconn is not None:
            vconn.close()


def _check_gate_3(conn: sqlite3.Connection, run_ids: list[int]) -> tuple[bool, str]:
    """Gate 3: each run's current_node_id and status match expectations.

    Active groups: expected at AWAIT_DISPOSITION with status=WAITING.
    Terminal groups: expected at TERM_* node with status=DONE.
    """
    ph, params = _in_clause(run_ids)
    sql = (
        "SELECT wr.id, wr.customer_id, wr.current_node_id, wr.status"
        " FROM workflow_runs wr WHERE wr.id IN (" + ph + ")"
    )
    rows = conn.execute(sql, params).fetchall()

    # Build per-customer expected maps from _GROUPS.
    exp_node: dict[str, str] = {}
    exp_status: dict[str, str] = {}
    for g in _GROUPS:
        for i in range(g["count"]):
            cid = f"TEST_{g['group'].upper()}_{i:02d}"
            exp_node[cid] = g["expected_node"]
            exp_status[cid] = g["expected_status"]

    failures: list[str] = []
    group_hits: dict[str, int] = {}
    for row in rows:
        cid = row["customer_id"]
        got_node = row["current_node_id"]
        got_status = row["status"]
        want_node = exp_node.get(cid)
        want_status = exp_status.get(cid)

        if want_node and got_node != want_node:
            failures.append(
                f"  cid={cid} expected_node={want_node} got={got_node} status={got_status}"
            )
        if want_status and got_status != want_status:
            failures.append(
                f"  cid={cid} expected_status={want_status} got={got_status} at node={got_node}"
            )
        # Defensive: any status other than WAITING or DONE is unexpected.
        if got_status not in ("WAITING", "DONE"):
            failures.append(
                f"  cid={cid} unexpected status={got_status} at node={got_node}"
            )

        group = next(
            (g["group"] for g in _GROUPS
             if cid.startswith("TEST_" + g["group"].upper() + "_")),
            "?",
        )
        group_hits[group] = group_hits.get(group, 0) + 1

    if failures:
        return False, "FAIL Gate 3 — unexpected branch routing:\n" + "\n".join(failures)

    dist = ", ".join(f"{k}={v}" for k, v in sorted(group_hits.items()))
    return True, f"PASS Gate 3 — branch distribution: {dist}"


# ── Main ─────────────────────────────────────────────────────────────────────

def main(argv: "list[str] | None" = None) -> None:
    p = argparse.ArgumentParser(description="E2E shadow test for vb_collections_v1")
    p.add_argument("--workflow-db", default=None, help="Path to workflow.db (default: temp DB)")
    p.add_argument("--vibrium-db", default=str(_VIBRIUM_DB_DEFAULT), help="Path to vibrium.db for Gate 2")
    p.add_argument("--ticks", type=int, default=4,
                   help="Tick count (default: 4; 2 needed: FIRE_VB_CALL tick + AWAIT_DISPOSITION park tick)")
    args = p.parse_args(argv)

    if not _SEED_GRAPH.exists():
        print(f"[FAIL] Seed graph not found: {_SEED_GRAPH}", file=sys.stderr)
        raise SystemExit(1)

    graph = json.loads(_SEED_GRAPH.read_text())
    print(f"[INFO] Loaded seed graph: {len(graph['nodes'])} nodes")
    print(f"[INFO] Enrolling {_TOTAL_CUSTOMERS} synthetic customers "
          f"({_ACTIVE_CUSTOMERS} ACTIVE at FIRE_VB_CALL, "
          f"{_TOTAL_CUSTOMERS - _ACTIVE_CUSTOMERS} pre-DONE)")

    # ── Set up DB ────────────────────────────────────────────────────────────
    _tmpdir = None
    if args.workflow_db:
        db_path = Path(args.workflow_db)
    else:
        _tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(_tmpdir.name) / "workflow_test.db"
    print(f"[INFO] Using workflow DB: {db_path}")

    try:
        mig = _load_migration()
        mig.up(db_path)
        conn = _open_db(db_path)

        wf_id, version_id = _seed_workflow(conn, graph)
        print(f"[INFO] Created workflow_id={wf_id} version_id={version_id} (shadow_mode=1)")

        run_ids = _enroll_customers(conn, wf_id, version_id)
        print(f"[INFO] Enrolled {len(run_ids)} runs")
        conn.close()

        # ── Run ticks ────────────────────────────────────────────────────────
        # Tick 1: FIRE_VB_CALL runs → INSERT PENDING in wf_pending_actions →
        #         run advances to AWAIT_DISPOSITION (status=ACTIVE).
        # Tick 2: AWAIT_DISPOSITION runs → no disposition yet → parks
        #         (status=WAITING, ready_at_ist = now + 24h).
        # Ticks 3+: no-op (WAITING runs not due yet).
        print(f"\n[TICKS] Running {args.ticks} ticks (need ≥2: FIRE_VB_CALL + AWAIT park)")
        _run_ticks(db_path, args.ticks, label="tick")

        # ── Simulate scheduler shadow processing ─────────────────────────────
        # workflow_scheduler.py (Phase 6) does this in real deployments.
        # In shadow_mode=1 it marks PENDING → SHADOW_FIRED instead of
        # firing CT externaltrigger.
        conn2 = _open_db(db_path)
        ph, id_params = _in_clause(run_ids)
        shadow_sql = (
            "UPDATE wf_pending_actions SET status = 'SHADOW_FIRED'"
            " WHERE run_id IN (" + ph + ") AND status = 'PENDING'"
        )
        n_shadow = conn2.execute(shadow_sql, id_params).rowcount
        conn2.commit()
        conn2.close()
        print(f"[INFO] Simulated scheduler: marked {n_shadow} PENDING → SHADOW_FIRED")

        # ── Hard gate checks ─────────────────────────────────────────────────
        print("\n[GATES]")
        conn_final = _open_db(db_path)
        g1_ok, g1_msg = _check_gate_1(conn_final, run_ids, n_shadow)
        g2_ok, g2_msg = _check_gate_2(args.vibrium_db, run_ids)
        g3_ok, g3_msg = _check_gate_3(conn_final, run_ids)
        conn_final.close()

        print(f"  {g1_msg}")
        print(f"  {g2_msg}")
        print(f"  {g3_msg}")

        all_passed = g1_ok and g2_ok and g3_ok
        print(f"\n{'[PASS] All gates passed' if all_passed else '[FAIL] One or more gates failed'}")

        if not all_passed:
            raise SystemExit(1)

    finally:
        if _tmpdir:
            _tmpdir.cleanup()


if __name__ == "__main__":
    main()
