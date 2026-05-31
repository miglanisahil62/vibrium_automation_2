"""Tests for the enrollment pre-window prep slot (ENROLLMENT_PREP_START_HOUR).

The prep slot lets enrollment run before the 08:00 RBI calling window so the
morning fetch -> CT -> tick chain finishes in time. It must:
  * stay DISABLED by default (prep_start=8 reproduces legacy behaviour),
  * when enabled (prep_start=7), allow enrollment at 07:xx even though
    gate.fire is False (pre-08:00),
  * never enroll before the prep slot opens (06:xx stays blocked),
  * only ever create ACTIVE runs — enrollment never fires a call (the
    scheduler's own window gate is the firing guard, untouched here).

No live CT / SMTP / pre_call_gate: bulk_get_profiles + _is_callable_now are
monkeypatched. Reuses the helpers from test_enrollment_poller.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from workflow import enrollment_poller as ep
from workflow.migrations import runner as mig_runner
from workflow.tests.test_enrollment_poller import (
    _FakeGate,
    _make_csv,
    _make_profile,
    _seed_workflow,
)

IST = ZoneInfo("Asia/Kolkata")
_EXPR = "dpd > 1 and risk_segmentation < 5"


@pytest.fixture
def workflow_db(tmp_path: Path) -> Path:
    db = tmp_path / "workflow.db"
    vib = tmp_path / "vibrium.db"
    mig_runner.run(workflow_db_path=str(db), vibrium_db_path=str(vib), dry_run=False)
    return db


def _run_at(monkeypatch, workflow_db, tmp_path, *, prep_start, hour, gate_fire):
    """Run one enrollment tick at a given hour with a given prep_start.

    Returns (stats, ct_was_called).
    """
    cids = ["c01", "c02"]
    csv_p = _make_csv(tmp_path, cids)
    profiles = {c: _make_profile(dpd=10, risk=2) for c in cids}

    called = {"ct": False}

    def _bulk(ids, **kw):
        called["ct"] = True
        return dict(profiles)

    monkeypatch.setattr(ep, "bulk_get_profiles", _bulk)
    monkeypatch.setattr(
        ep, "_is_callable_now", lambda: _FakeGate(fire=gate_fire, reason="test")
    )
    monkeypatch.setattr(ep, "ENROLLMENT_PREP_START_HOUR", prep_start)

    _seed_workflow(workflow_db, source_csv=csv_p, condition_expr=_EXPR)

    now = datetime(2026, 5, 30, hour, 5, 0, tzinfo=IST)
    stats = ep.run(workflow_db, now=now)
    return stats, called["ct"]


def test_prep_disabled_blocks_pre_window(monkeypatch, workflow_db, tmp_path):
    # Default prep_start=8: 07:05 with gate.fire=False (pre-08:00) → blocked.
    stats, ct_called = _run_at(
        monkeypatch, workflow_db, tmp_path, prep_start=8, hour=7, gate_fire=False
    )
    assert stats["outside_window"] is True
    assert ct_called is False
    assert stats["enrolled_total"] == 0


def test_prep_enabled_allows_pre_window(monkeypatch, workflow_db, tmp_path):
    # prep_start=7: 07:05 with gate.fire=False → enrollment proceeds, runs ACTIVE.
    stats, ct_called = _run_at(
        monkeypatch, workflow_db, tmp_path, prep_start=7, hour=7, gate_fire=False
    )
    assert stats["outside_window"] is False
    assert ct_called is True
    assert stats["enrolled_total"] == 2

    conn = sqlite3.connect(str(workflow_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT status FROM workflow_runs").fetchall()
    conn.close()
    assert len(rows) == 2
    # Enrollment never fires — every run is ACTIVE at the ENROLL node.
    assert all(r["status"] == "ACTIVE" for r in rows)


def test_prep_enabled_still_blocks_too_early(monkeypatch, workflow_db, tmp_path):
    # prep_start=7: 06:05 is before the prep slot → still blocked.
    stats, ct_called = _run_at(
        monkeypatch, workflow_db, tmp_path, prep_start=7, hour=6, gate_fire=False
    )
    assert stats["outside_window"] is True
    assert ct_called is False
    assert stats["enrolled_total"] == 0


def test_in_window_unaffected_by_prep_setting(monkeypatch, workflow_db, tmp_path):
    # Normal 10:xx with gate.fire=True enrolls regardless of prep_start.
    stats, ct_called = _run_at(
        monkeypatch, workflow_db, tmp_path, prep_start=8, hour=10, gate_fire=True
    )
    assert stats["outside_window"] is False
    assert ct_called is True
    assert stats["enrolled_total"] == 2
