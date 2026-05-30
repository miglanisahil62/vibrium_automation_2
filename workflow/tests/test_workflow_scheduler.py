"""Phase 6 tests — workflow_scheduler.

Strategy: real SQLite tmp_path DBs (migrations applied), mocked
``trigger_fn`` + ``gate_check_fn`` + ``is_callable_now_fn`` + ``record_fire_fn``
via the run()-level injection seams. No Redshift. No CT HTTP. No
``vibrium-automation/`` files written.

What we prove:

    1. Happy path — 5 PENDING rows, all eligible → all 5 FIRED, audit hit 5×,
       trigger() called 5×, and trigger() never receives a ``tag_group``
       kwarg (Phase 0a invariant).
    2. Gate fails (pre_call_gate.check returns fire=False) → SUPPRESSED, no
       trigger call, no audit write.
    3. Outside RBI window — no rows touched; status='outside_window'.
    4. CT trigger returns ``status='error'`` → row marked ERROR with
       last_error populated; row NOT lost (still queryable).
    5. shadow_mode=True → SHADOW_FIRED + fired_at_ist populated; no
       trigger, no audit.
    6. dry_run=True → no DB writes besides logging intent; row stays
       PENDING.
    7. Kill switch active → 0 fire; status='killed'.
    8. Race-safety — simulate two ticks racing on same row; only the
       winner fires.
    9. Daily-cap gate — pre-seed customer_call_audit with 3 same-day rows;
       row gets SUPPRESSED with cap reason.
    10. Cooldown gate — pre-seed customer_call_audit with one fire 1h ago;
        row gets SUPPRESSED with cooldown reason.

Helpers below build everything from scratch in tmp_path. Each test is
independent.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

# Make ``shared`` and ``workflow`` importable regardless of pytest invocation
# dir. Phase 3 test file uses the same pattern.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from workflow import workflow_scheduler as wfs  # noqa: E402
from workflow.migrations import runner as migration_runner  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")


# ----------------------------------------------------------------- fixtures


@dataclass
class _GateResult:
    """Shape-match of ``pre_call_gate.GateResult`` for the injection seam."""

    fire: bool
    reason: str = ""


@pytest.fixture
def dbs(tmp_path):
    """Apply migrations against fresh workflow.db + vibrium.db and return paths."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))
    return wf, vb


def _now_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _past_str(minutes_ago: int) -> str:
    return (datetime.now(IST) - timedelta(minutes=minutes_ago)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _insert_pending(
    wf_db_path: Path,
    *,
    n: int = 1,
    customer_id_start: int = 9000000,
    run_id_start: int = 1,
    cohort_name: str = "test_cohort",
    scheduled_in_minutes: int = -5,
):
    """Insert ``n`` PENDING rows with sequential customer/run IDs."""
    conn = sqlite3.connect(str(wf_db_path))
    try:
        sched = (
            datetime.now(IST) + timedelta(minutes=scheduled_in_minutes)
        ).strftime("%Y-%m-%d %H:%M:%S")
        now = _now_str()
        rows = []
        for i in range(n):
            cid = customer_id_start + i
            rid = run_id_start + i
            rows.append((rid, "node_a", 1, str(cid), sched, "PENDING", cohort_name, now))
        conn.executemany(
            """
            INSERT INTO wf_pending_actions
              (run_id, node_id, attempt_count, customer_id, scheduled_at_ist,
               status, cohort_name, created_at_ist)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM wf_pending_actions ORDER BY id"
            ).fetchall()
        ]
        return ids
    finally:
        conn.close()


def _row(wf_db_path: Path, row_id: int) -> dict:
    conn = sqlite3.connect(str(wf_db_path))
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(
            "SELECT * FROM wf_pending_actions WHERE id=?", (row_id,)
        ).fetchone()
        return dict(r) if r else {}
    finally:
        conn.close()


def _all_rows(wf_db_path: Path) -> list[dict]:
    conn = sqlite3.connect(str(wf_db_path))
    conn.row_factory = sqlite3.Row
    try:
        rs = conn.execute(
            "SELECT * FROM wf_pending_actions ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rs]
    finally:
        conn.close()


def _set_kill(wf_db_path: Path):
    conn = sqlite3.connect(str(wf_db_path))
    try:
        conn.execute(
            "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
            "VALUES (?, 'KILL', 'test', 'pytest')",
            (_now_str(),),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_audit(
    vb_db_path: Path,
    customer_id: int,
    *,
    fired_at_ist: str,
    source: str = "adhoc",
):
    """Insert a customer_call_audit row directly (bypassing record_fire)."""
    conn = sqlite3.connect(str(vb_db_path))
    try:
        conn.execute(
            """
            INSERT INTO customer_call_audit
                (customer_id, fired_at_ist, source, ct_response_status)
            VALUES (?, ?, ?, 'success')
            """,
            (str(customer_id), fired_at_ist, source),
        )
        conn.commit()
    finally:
        conn.close()


def _ok_window():
    return MagicMock(return_value=_GateResult(fire=True, reason="within window"))


def _ok_gate():
    return MagicMock(return_value=_GateResult(fire=True, reason="ok"))


def _ok_trigger():
    return MagicMock(return_value={"status": "success", "http_status": 200, "body": {}})


# --------------------------------------------------------------- 1. happy path


def test_happy_path_five_rows_all_fire(dbs):
    wf, vb = dbs
    _insert_pending(wf, n=5)
    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["status"] == "ok"
    assert stats["fired"] == 5
    assert stats["suppressed"] == 0
    assert stats["errored"] == 0

    # Audit hit 5×, trigger called 5×.
    assert audit.call_count == 5
    assert trigger.call_count == 5

    # All rows FIRED + fired_at_ist populated.
    for r in _all_rows(wf):
        assert r["status"] == "FIRED"
        assert r["fired_at_ist"] is not None

    # Phase 0a invariant: trigger() was NEVER called with tag_group kwarg.
    for call in trigger.call_args_list:
        args, kwargs = call
        assert "tag_group" not in kwargs, (
            f"trigger() received tag_group kwarg (Phase 0a violation): {kwargs}"
        )


# --------------------------------------------------------------- 2. gate fails


def test_gate_fails_suppresses_no_trigger(dbs):
    wf, vb = dbs
    _insert_pending(wf, n=3)
    trigger = _ok_trigger()
    audit = MagicMock()

    deny = MagicMock(return_value=_GateResult(fire=False, reason="not overdue"))

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=deny,
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["fired"] == 0
    assert stats["suppressed"] == 3
    assert trigger.call_count == 0
    assert audit.call_count == 0
    for r in _all_rows(wf):
        assert r["status"] == "SUPPRESSED"
        assert "not overdue" in (r["last_error"] or "")


# --------------------------------------------------------------- 3. outside window


def test_outside_window_no_processing(dbs):
    wf, vb = dbs
    _insert_pending(wf, n=3)
    trigger = _ok_trigger()
    audit = MagicMock()

    closed = MagicMock(return_value=_GateResult(fire=False, reason="outside RBI"))

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=closed,
        record_fire_fn=audit,
    )

    assert stats["status"] == "outside_window"
    assert stats["processed"] == 0
    assert trigger.call_count == 0
    assert audit.call_count == 0
    for r in _all_rows(wf):
        assert r["status"] == "PENDING"


# --------------------------------------------------------------- 4. CT error


def test_ct_error_marks_error_row_not_lost(dbs):
    wf, vb = dbs
    [row_id] = _insert_pending(wf, n=1)
    err_trigger = MagicMock(
        return_value={"status": "error", "http_status": 500, "body": "boom"}
    )
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=err_trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["errored"] == 1
    assert stats["fired"] == 0
    assert audit.call_count == 0

    r = _row(wf, row_id)
    assert r["status"] == "ERROR"
    assert r["last_error"] is not None
    assert "error" in r["last_error"]
    assert "boom" in r["last_error"]


# --------------------------------------------------------------- 5. shadow mode


def test_shadow_mode_marks_shadow_fired_no_ct(dbs):
    wf, vb = dbs
    [row_id] = _insert_pending(wf, n=1)
    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        shadow_mode=True,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["shadow_fired"] == 1
    assert stats["fired"] == 0
    assert trigger.call_count == 0
    assert audit.call_count == 0

    r = _row(wf, row_id)
    assert r["status"] == "SHADOW_FIRED"
    assert r["fired_at_ist"] is not None  # triangulation join needs this


# --------------------------------------------------------------- 6. dry_run


def test_dry_run_no_writes_beyond_log(dbs):
    wf, vb = dbs
    [row_id] = _insert_pending(wf, n=1)
    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        dry_run=True,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["would_fire"] == 1
    assert stats["fired"] == 0
    assert trigger.call_count == 0
    assert audit.call_count == 0

    r = _row(wf, row_id)
    # Row was claimed (FIRING_IN_PROGRESS) then released back to PENDING.
    assert r["status"] == "PENDING"
    # fired_at_ist must NOT be populated in dry-run.
    assert r["fired_at_ist"] is None


# --------------------------------------------------------------- 7. kill switch


def test_kill_switch_blocks_all_fires(dbs):
    wf, vb = dbs
    _insert_pending(wf, n=3)
    _set_kill(wf)
    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["status"] == "killed"
    assert stats["fired"] == 0
    assert trigger.call_count == 0
    assert audit.call_count == 0
    for r in _all_rows(wf):
        assert r["status"] == "PENDING"


# --------------------------------------------------------------- 8. race safety


def test_claim_row_race_safety(dbs):
    """Simulate two ticks racing for the same PENDING row.

    Proves the ``UPDATE WHERE status='PENDING'`` claim is atomic — the
    second caller sees rowcount=0 and skips the row.
    """
    wf, _vb = dbs
    [row_id] = _insert_pending(wf, n=1)

    conn1 = sqlite3.connect(str(wf))
    conn2 = sqlite3.connect(str(wf))
    try:
        # Tick 1 wins the claim.
        won1 = wfs._claim_row(conn1, row_id)
        # Tick 2 sees the row as already FIRING_IN_PROGRESS — must lose.
        won2 = wfs._claim_row(conn2, row_id)
        assert won1 is True
        assert won2 is False
    finally:
        conn1.close()
        conn2.close()

    r = _row(wf, row_id)
    assert r["status"] == "FIRING_IN_PROGRESS"


# --------------------------------------------------------------- 9. daily cap


def test_daily_cap_three_fires_suppresses(dbs):
    """Pre-seed 3 same-day audit rows → gate suppresses row 4."""
    wf, vb = dbs
    [row_id] = _insert_pending(wf, n=1, customer_id_start=8000001)
    today = _now_str()
    # 3 prior fires today across both systems.
    for _ in range(3):
        _seed_audit(vb, 8000001, fired_at_ist=today, source="adhoc")

    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["suppressed"] == 1
    assert stats["fired"] == 0
    assert trigger.call_count == 0

    r = _row(wf, row_id)
    assert r["status"] == "SUPPRESSED"
    assert "cap" in (r["last_error"] or "").lower()


# --------------------------------------------------------------- 10. cooldown


def test_cooldown_one_hour_ago_suppresses(dbs):
    """Pre-seed an audit row 60 min ago → cooldown gate blocks (3h rule)."""
    wf, vb = dbs
    [row_id] = _insert_pending(wf, n=1, customer_id_start=8000002)
    _seed_audit(vb, 8000002, fired_at_ist=_past_str(60), source="workflow")

    trigger = _ok_trigger()
    audit = MagicMock()

    stats = wfs.run(
        workflow_db_path=wf,
        vibrium_db_path=vb,
        trigger_fn=trigger,
        gate_check_fn=_ok_gate(),
        is_callable_now_fn=_ok_window(),
        record_fire_fn=audit,
    )

    assert stats["suppressed"] == 1
    assert trigger.call_count == 0

    r = _row(wf, row_id)
    assert r["status"] == "SUPPRESSED"
    assert "cooldown" in (r["last_error"] or "").lower()


# --------------------------------------------------------------- 11. CLI smoke


def test_cli_dry_run_empty_db(tmp_path):
    """CLI runs cleanly on empty DBs and exits 0 (dry-run)."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    # Monkey-patch the lazy-imports to mocks so this doesn't try to load
    # the external/vibrium_automation_scripts module (which assumes Redshift
    # creds + a CT credentials file).
    saved = (wfs._default_trigger, wfs._default_gate_check, wfs._default_is_callable_now)
    wfs._default_trigger = lambda: _ok_trigger()
    wfs._default_gate_check = lambda: _ok_gate()
    wfs._default_is_callable_now = lambda: _ok_window()
    try:
        rc = wfs.main([
            "--workflow-db", str(wf),
            "--vibrium-db", str(vb),
            "--dry-run",
        ])
    finally:
        wfs._default_trigger, wfs._default_gate_check, wfs._default_is_callable_now = saved
    assert rc == 0
