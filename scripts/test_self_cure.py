"""Tests for WS14 self_cure.py — the server-side stuck-state recovery."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import scripts.self_cure as sc  # noqa: E402
from workflow.migrations import runner as mig_runner  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")


def _ts(minutes_ago=0, hours_ago=0):
    return (datetime.now(IST).replace(tzinfo=None)
            - timedelta(minutes=minutes_ago, hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "workflow.db"
    mig_runner.run(str(p), str(tmp_path / "vibrium.db"))
    return str(p)


_ATTEMPT_SEQ = [0]


def _ins_action(db, status, last_attempt=None, created=None):
    _ATTEMPT_SEQ[0] += 1  # unique attempt_count → satisfies UNIQUE(run,node,attempt)
    c = sqlite3.connect(db)
    with c:
        c.execute(
            "INSERT INTO wf_pending_actions (run_id, node_id, attempt_count, customer_id, "
            "scheduled_at_ist, status, created_at_ist, last_attempt_at_ist) "
            "VALUES (1,'n',?,'c',?,?,?,?)",
            (_ATTEMPT_SEQ[0], created or _ts(), status, created or _ts(), last_attempt),
        )
    c.close()


def _ins_run(db, status, node_type, ready_at=None, entered=None):
    c = sqlite3.connect(db)
    with c:
        c.execute(
            "INSERT INTO workflow_runs (workflow_id, version_id, customer_id, "
            "current_node_id, current_node_type, status, ready_at_ist, "
            "entered_node_at_ist, enrolled_at_ist) "
            "VALUES (1,1,'c','nid',?,?,?,?,?)",
            (node_type, status, ready_at, entered, _ts()),
        )
    c.close()


def _count(db, sql, args=()):
    c = sqlite3.connect(db)
    n = c.execute(sql, args).fetchone()[0]
    c.close()
    return n


def test_error_pending_reset_to_pending(db):
    _ins_action(db, "ERROR")
    _ins_action(db, "ERROR")
    sc.cure(db, dry_run=False)
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='ERROR'") == 0
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='PENDING'") == 2


def test_stuck_firing_reset_but_fresh_firing_left(db):
    _ins_action(db, "FIRING_IN_PROGRESS", last_attempt=_ts(minutes_ago=45))  # stuck
    _ins_action(db, "FIRING_IN_PROGRESS", last_attempt=_ts(minutes_ago=5))   # in-flight
    sc.cure(db, dry_run=False)
    # stuck → PENDING; fresh one stays FIRING_IN_PROGRESS
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='FIRING_IN_PROGRESS'") == 1
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='PENDING'") == 1


def test_waiting_runs_are_not_touched(db):
    # WS14 P2-1: self_cure deliberately does NOT mutate WAITING runs (the
    # executor handles past-ready; future parks are legitimate). A stale
    # WAIT_UNTIL is left exactly as-is.
    stale = _ts(minutes_ago=45)
    _ins_run(db, "WAITING", "WAIT_UNTIL", ready_at=stale)
    sc.cure(db, dry_run=False)
    assert "stale_wait_nudged" not in sc.cure(db, dry_run=True)  # key removed
    # ready_at untouched (still the 45m-old value)
    assert _count(db,
        "SELECT COUNT(*) FROM workflow_runs WHERE current_node_type='WAIT_UNTIL' "
        "AND ready_at_ist = ?", (stale,)) == 1


def test_stuck_active_is_report_only(db):
    _ins_run(db, "ACTIVE", "COUNTER", entered=_ts(hours_ago=8))
    stats = sc.cure(db, dry_run=False)
    assert stats["stuck_active_reported"] == 1
    # report-only: the ACTIVE run is NOT mutated
    assert _count(db, "SELECT COUNT(*) FROM workflow_runs WHERE status='ACTIVE'") == 1


def test_dry_run_writes_nothing(db):
    _ins_action(db, "ERROR")
    sc.cure(db, dry_run=True)
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='ERROR'") == 1


def test_max_cure_rows_guard_aborts_class(db, monkeypatch):
    monkeypatch.setattr(sc, "MAX_CURE_ROWS", 1)
    _ins_action(db, "ERROR")
    _ins_action(db, "ERROR")   # 2 > cap of 1 → abort error_reset class
    stats = sc.cure(db, dry_run=False)
    assert any("error_reset" in a for a in stats["aborted"])
    # not mutated (class aborted)
    assert _count(db, "SELECT COUNT(*) FROM wf_pending_actions WHERE status='ERROR'") == 2
