"""Tests for workflow/enrollment_poller.py.

No live CT calls — `bulk_get_profiles` is monkeypatched. No live SMTP, no
real `pre_call_gate` import — `_is_callable_now` is mocked so the test
doesn't depend on wall-clock time.

Covers the six DoD scenarios:

  1. 10 candidates, 4 match → 4 runs inserted.
  2. Re-run → 0 new (enrollment_key dedupe).
  3. Per-tick cap = 2 → only first 2 created.
  4. Outside window (mocked gate.fire=False) → no enrollment.
  5. Kill switch active → no enrollment.
  6. Hard-abort > 5000 candidates → exits non-zero, no enrollment.
  7. --dry-run prints, doesn't insert.

Schema is materialised via the real migration runner so any drift in
001_init.py shows up here.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from workflow import enrollment_poller as ep
from workflow.migrations import runner as mig_runner


IST = ZoneInfo("Asia/Kolkata")
WORKFLOW_NAME = "test_wf"


# ----------------------------------------------------------------- Fixtures


@dataclass
class _FakeGate:
    fire: bool
    reason: str = "within RBI call window"


def _seed_workflow(
    workflow_db_path: Path,
    *,
    source_csv: Path,
    condition_expr: str,
    daily_cap: int = 1000,
    enroll_key_template: str = "{customer_id}_{YYYY-MM-DD}",
) -> int:
    """Create an ACTIVE workflow with one ENROLL node carrying the given config.

    Returns the workflow_id.
    """
    enroll_cfg = {
        "source_csv": str(source_csv),
        "condition_expr": condition_expr,
    }
    graph = {
        "nodes": [
            {
                "id": "n_enroll",
                "type": "ENROLL",
                "config": enroll_cfg,
            },
            {
                "id": "n_terminate",
                "type": "TERMINATE",
                "config": {"status": "DONE"},
            },
        ]
    }

    conn = sqlite3.connect(str(workflow_db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO workflows (
              name, status, active_version_id, shadow_mode, requires_approval,
              max_new_enrollments_per_day, enrollment_key_template,
              created_at_ist, created_by
            ) VALUES (?, 'ACTIVE', NULL, 0, 0, ?, ?, datetime('now'), 'test')
            """,
            (WORKFLOW_NAME, daily_cap, enroll_key_template),
        )
        wf_id = cur.lastrowid
        cur.execute(
            """
            INSERT INTO workflow_versions (
              workflow_id, version, graph_json, validation_status,
              created_at_ist, created_by
            ) VALUES (?, 1, ?, 'OK', datetime('now'), 'test')
            """,
            (wf_id, json.dumps(graph)),
        )
        version_id = cur.lastrowid
        cur.execute(
            "UPDATE workflows SET active_version_id = ? WHERE id = ?",
            (version_id, wf_id),
        )
        conn.commit()
        return int(wf_id)
    finally:
        conn.close()


def _make_csv(tmp_path: Path, cids: list[str]) -> Path:
    p = tmp_path / "candidates.csv"
    lines = ["customer_id\n"] + [f"{c}\n" for c in cids]
    p.write_text("".join(lines))
    return p


def _make_profile(*, dpd: int, risk: int, wa: str = "WA_Available") -> dict[str, Any]:
    return {
        "profileData": {
            "dpd": dpd,
            "coll_collection_risk_segmentation": risk,
            "coll_notification_replied": wa,
            "coll_bot_calling": "ai_vb_calling_midv1",
        }
    }


@pytest.fixture
def workflow_db(tmp_path: Path) -> Path:
    db = tmp_path / "workflow.db"
    vib = tmp_path / "vibrium.db"
    mig_runner.run(
        workflow_db_path=str(db),
        vibrium_db_path=str(vib),
        dry_run=False,
    )
    return db


@pytest.fixture(autouse=True)
def _force_in_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default: pretend we're inside the RBI window + before 18:00 IST."""
    monkeypatch.setattr(
        ep, "_is_callable_now", lambda: _FakeGate(fire=True),
    )


@pytest.fixture
def fixed_now() -> datetime:
    """A deterministic IST timestamp safely inside the enrollment window."""
    return datetime(2026, 5, 30, 10, 30, 0, tzinfo=IST)


# -------------------------------------------------------- 1. Happy path (4/10)


def test_four_of_ten_match(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    cids = [f"c{i:02d}" for i in range(10)]
    csv_p = _make_csv(tmp_path, cids)
    # 4 of 10 satisfy dpd > 1 AND risk_segmentation < 5.
    profiles = {}
    for i, cid in enumerate(cids):
        if i < 4:
            profiles[cid] = _make_profile(dpd=10, risk=2)
        else:
            profiles[cid] = _make_profile(dpd=0, risk=8)
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )

    wf_id = _seed_workflow(
        workflow_db,
        source_csv=csv_p,
        condition_expr="dpd > 1 and risk_segmentation < 5",
    )

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["enrolled_total"] == 4
    assert stats["matched_total"] == 4
    assert stats["candidates_total"] == 10
    assert stats["per_workflow"][wf_id]["enrolled"] == 4

    # Confirm rows landed in workflow_runs with correct enrollment_key shape.
    conn = sqlite3.connect(str(workflow_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT customer_id, enrollment_key, status FROM workflow_runs"
    ).fetchall()
    conn.close()
    assert len(rows) == 4
    today = fixed_now.strftime("%Y-%m-%d")
    for r in rows:
        assert r["status"] == "ACTIVE"
        assert r["enrollment_key"] == f"{r['customer_id']}_{today}"


# ---------------------------------------------------- 2. Idempotency (re-run)


def test_rerun_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    cids = [f"c{i:02d}" for i in range(4)]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )

    _seed_workflow(
        workflow_db, source_csv=csv_p, condition_expr="dpd > 1",
    )

    first = ep.run(workflow_db, now=fixed_now)
    assert first["enrolled_total"] == 4

    second = ep.run(workflow_db, now=fixed_now)
    # The partial UNIQUE index on enrollment_key absorbs all 4 reattempts.
    assert second["enrolled_total"] == 0
    # And total rows didn't grow.
    conn = sqlite3.connect(str(workflow_db))
    n = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
    conn.close()
    assert n == 4


# -------------------------------------------------------- 3. Per-tick cap


def test_per_tick_cap(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    cids = [f"c{i:02d}" for i in range(5)]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )
    # Shrink per-tick cap to 2.
    monkeypatch.setattr(ep, "MAX_NEW_ENROLLMENTS_PER_TICK", 2)

    _seed_workflow(
        workflow_db, source_csv=csv_p, condition_expr="dpd > 1",
    )

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["enrolled_total"] == 2
    assert stats["matched_total"] == 5  # all matched; cap just clipped insert


# ------------------------------------------------------- 4. Outside window


def test_outside_window(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    csv_p = _make_csv(tmp_path, ["c01"])
    monkeypatch.setattr(
        ep, "_is_callable_now",
        lambda: _FakeGate(fire=False, reason="outside RBI call window 21:00 IST"),
    )
    # bulk_get_profiles should NOT be called — assert that.
    def _fail(*a, **kw):  # noqa: ARG001
        raise AssertionError("bulk_get_profiles must not be called outside window")
    monkeypatch.setattr(ep, "bulk_get_profiles", _fail)

    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")
    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["outside_window"] is True
    assert stats["enrolled_total"] == 0


def test_outside_window_narrower_18_to_19_window(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
):
    """RBI lets us call until 19:00 but enrollment is capped at 18:00.

    Run with `now` at 18:30 IST — gate.fire is True (still inside RBI window)
    but enrollment_poller refuses because of the narrower 18:00 cap.
    """
    csv_p = _make_csv(tmp_path, ["c01"])
    monkeypatch.setattr(ep, "_is_callable_now", lambda: _FakeGate(fire=True))
    def _fail(*a, **kw):  # noqa: ARG001
        raise AssertionError("bulk_get_profiles must not be called past 18:00")
    monkeypatch.setattr(ep, "bulk_get_profiles", _fail)

    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")
    late = datetime(2026, 5, 30, 18, 30, tzinfo=IST)
    stats = ep.run(workflow_db, now=late)
    assert stats["outside_window"] is True


# ------------------------------------------------------- 5. Kill switch


def test_kill_switch_blocks(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    csv_p = _make_csv(tmp_path, ["c01"])
    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")

    # Set KILL switch — latest row wins.
    conn = sqlite3.connect(str(workflow_db))
    conn.execute(
        "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
        "VALUES (datetime('now'), 'KILL', 'test', 'test')"
    )
    conn.commit()
    conn.close()

    def _fail(*a, **kw):  # noqa: ARG001
        raise AssertionError("bulk_get_profiles must not be called under KILL")
    monkeypatch.setattr(ep, "bulk_get_profiles", _fail)

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["killed"] is True
    assert stats["enrolled_total"] == 0


def test_kill_then_resume_unblocks(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    """RESUME after KILL means latest=RESUME → poller runs normally."""
    csv_p = _make_csv(tmp_path, ["c01", "c02"])
    profiles = {c: _make_profile(dpd=10, risk=2) for c in ["c01", "c02"]}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )
    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")

    conn = sqlite3.connect(str(workflow_db))
    conn.execute(
        "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
        "VALUES (datetime('now'), 'KILL', 'first', 'test')"
    )
    conn.execute(
        "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
        "VALUES (datetime('now'), 'RESUME', 'cleared', 'test')"
    )
    conn.commit()
    conn.close()

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["killed"] is False
    assert stats["enrolled_total"] == 2


# ------------------------------------------------------- 6. Hard-abort > 5000


def test_hard_abort_when_too_many_match(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    monkeypatch.setattr(ep, "PER_WORKFLOW_HARD_ABORT", 3)
    cids = [f"c{i:02d}" for i in range(5)]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )

    wf_id = _seed_workflow(
        workflow_db, source_csv=csv_p, condition_expr="dpd > 1",
    )

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["enrolled_total"] == 0
    assert wf_id in stats["aborted_workflows"]
    assert stats["per_workflow"][wf_id]["hard_aborted"] == 5

    # Confirm no rows landed.
    conn = sqlite3.connect(str(workflow_db))
    n = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
    conn.close()
    assert n == 0


def test_hard_abort_force_overrides(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    monkeypatch.setattr(ep, "PER_WORKFLOW_HARD_ABORT", 3)
    cids = [f"c{i:02d}" for i in range(5)]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )

    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")
    stats = ep.run(workflow_db, now=fixed_now, force=True)
    assert stats["enrolled_total"] == 5
    assert stats["aborted_workflows"] == []


# ------------------------------------------------------- 7. Dry-run


def test_dry_run_does_not_insert(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    cids = [f"c{i:02d}" for i in range(3)]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )
    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")

    stats = ep.run(workflow_db, now=fixed_now, dry_run=True)
    assert stats["matched_total"] == 3
    assert stats["enrolled_total"] == 0
    assert stats["dry_run"] is True
    # Confirm no rows landed.
    conn = sqlite3.connect(str(workflow_db))
    n = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
    conn.close()
    assert n == 0


# ------------------------------------------------------- 8. Per-day cap


def test_daily_cap_partial_room(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    """With daily_cap=2 and 1 row already enrolled today, only 1 more should land."""
    cids = ["c01", "c02", "c03"]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {cid: _make_profile(dpd=10, risk=2) for cid in cids}
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )
    wf_id = _seed_workflow(
        workflow_db, source_csv=csv_p,
        condition_expr="dpd > 1", daily_cap=2,
    )
    # Pre-insert one "already enrolled today" row so the gap is 1.
    today_str = fixed_now.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(str(workflow_db))
    conn.execute(
        """
        INSERT INTO workflow_runs (
          workflow_id, version_id, customer_id, enrollment_key,
          current_node_id, current_node_type, entered_node_at_ist, status,
          scratchpad_json, enrolled_at_ist, updated_at_ist
        ) VALUES (?, 1, 'preexisting', 'preexisting_yesterday', 'n_enroll',
                  'ENROLL', ?, 'ACTIVE', '{}', ?, ?)
        """,
        (wf_id, today_str, today_str, today_str),
    )
    conn.commit()
    conn.close()

    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["enrolled_total"] == 1


# ------------------------------------------------------- 9. No active workflows


def test_no_workflows_clean_exit(
    workflow_db: Path,
    fixed_now: datetime,
):
    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["workflows_seen"] == 0
    assert stats["enrolled_total"] == 0


# ------------------------------------------------------- 10. Condition undefined-name


def test_condition_undefined_name_skips_candidate(
    monkeypatch: pytest.MonkeyPatch,
    workflow_db: Path,
    tmp_path: Path,
    fixed_now: datetime,
):
    """An undefined name in CONDITION must not crash the whole tick.

    The candidate is logged + skipped; other candidates continue.
    """
    cids = ["c01", "c02"]
    csv_p = _make_csv(tmp_path, cids)
    # c01 has dpd set; c02 does not.
    profiles = {
        "c01": _make_profile(dpd=10, risk=2),
        "c02": {"profileData": {}},   # no dpd
    }
    monkeypatch.setattr(
        ep, "bulk_get_profiles", lambda ids, **kw: dict(profiles),
    )
    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr="dpd > 1")
    stats = ep.run(workflow_db, now=fixed_now)
    assert stats["enrolled_total"] == 1
    assert stats["matched_total"] == 1
