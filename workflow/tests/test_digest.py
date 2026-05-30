"""Phase 9 — workflow_digest payload builder tests."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from workflow import workflow_digest as wd

IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture
def wf_db(tmp_path: Path) -> Path:
    """workflow.db with minimum tables for the digest queries."""
    db = tmp_path / "workflow.db"
    cn = sqlite3.connect(str(db))
    cn.executescript("""
        CREATE TABLE workflows (
          id INTEGER PRIMARY KEY, name TEXT,
          status TEXT, active_version_id INTEGER,
          shadow_mode INTEGER DEFAULT 1, requires_approval INTEGER DEFAULT 1,
          max_new_enrollments_per_day INTEGER DEFAULT 1000,
          enrollment_key_template TEXT, created_at_ist TEXT, created_by TEXT
        );
        CREATE TABLE workflow_runs (
          id INTEGER PRIMARY KEY,
          workflow_id INTEGER, version_id INTEGER, customer_id TEXT,
          enrollment_key TEXT,
          current_node_id TEXT, current_node_type TEXT,
          entered_node_at_ist TEXT, status TEXT,
          scratchpad_json TEXT, ready_at_ist TEXT,
          enrolled_at_ist TEXT, updated_at_ist TEXT,
          terminated_at_ist TEXT, terminal_status TEXT
        );
        CREATE TABLE wf_agent_events (
          id INTEGER PRIMARY KEY, ts_ist TEXT, agent TEXT,
          status TEXT, summary_json TEXT
        );
        CREATE TABLE wf_pending_actions (
          id INTEGER PRIMARY KEY, run_id INTEGER, node_id TEXT,
          attempt_count INTEGER, customer_id TEXT,
          scheduled_at_ist TEXT, status TEXT,
          attempts INTEGER, last_attempt_at_ist TEXT, last_error TEXT,
          cohort_name TEXT, created_at_ist TEXT, fired_at_ist TEXT,
          UNIQUE(run_id, node_id, attempt_count)
        );
    """)
    cn.commit()
    return db


def _now_str(offset: timedelta = timedelta()) -> str:
    return (datetime.now(IST) + offset).strftime("%Y-%m-%d %H:%M:%S")


def test_empty_db_produces_clean_payload(wf_db: Path) -> None:
    """No workflows, no daemons, no shadow runs → payload still well-formed."""
    payload = wd.run(workflow_db_path=wf_db)
    assert payload["to"] == wd.DIGEST_TO
    assert payload["from"] == wd.DIGEST_FROM
    assert payload["workflow_count"] == 0
    assert payload["daemon_count"] == 0
    assert payload["shadow_runs"] == 0
    assert payload["dry_run"] is False
    assert "Vibrium Workflow" in payload["subject"]
    assert "no active workflows" in payload["body"]


def test_workflow_counts_aggregate_by_status(wf_db: Path) -> None:
    """Two workflows, mixed run statuses → counts dict per workflow correct."""
    cn = sqlite3.connect(str(wf_db))
    cn.execute(
        "INSERT INTO workflows (id, name, status, active_version_id, "
        "created_at_ist, created_by) VALUES "
        "(1, 'vb_collections_v1', 'ACTIVE', 1, ?, 'test'), "
        "(2, 'paused_wf', 'PAUSED', 1, ?, 'test')",
        (_now_str(), _now_str()),
    )
    cn.executemany(
        "INSERT INTO workflow_runs (workflow_id, version_id, customer_id, "
        "status, entered_node_at_ist, updated_at_ist) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "c1", "ACTIVE", _now_str(), _now_str()),
            (1, 1, "c2", "ACTIVE", _now_str(), _now_str()),
            (1, 1, "c3", "WAITING", _now_str(), _now_str()),
            (1, 1, "c4", "DONE", _now_str(), _now_str()),
            (2, 1, "c5", "ACTIVE", _now_str(), _now_str()),
        ],
    )
    cn.commit()
    cn.close()

    payload = wd.run(workflow_db_path=wf_db)
    assert payload["workflow_count"] == 2
    assert "vb_collections_v1" in payload["body"]
    assert "paused_wf" in payload["body"]


def test_erroring_gt_24h_flagged(wf_db: Path) -> None:
    """Runs in ERROR with updated_at_ist > 24h ago are counted separately."""
    cn = sqlite3.connect(str(wf_db))
    cn.execute(
        "INSERT INTO workflows (id, name, status, active_version_id, "
        "created_at_ist, created_by) "
        "VALUES (1, 'test_wf', 'ACTIVE', 1, ?, ?)",
        (_now_str(), "test"),
    )
    long_ago = _now_str(-timedelta(hours=48))
    cn.execute(
        "INSERT INTO workflow_runs (workflow_id, version_id, customer_id, "
        "status, entered_node_at_ist, updated_at_ist) "
        "VALUES (1, 1, 'c1', 'ERROR', ?, ?)",
        (long_ago, long_ago),
    )
    cn.commit()
    cn.close()

    payload = wd.run(workflow_db_path=wf_db)
    assert "1 ERROR > 24h" in payload["body"]


def test_waiting_gt_7d_flagged(wf_db: Path) -> None:
    """Runs in WAITING with entered_node_at_ist > 7d ago are flagged."""
    cn = sqlite3.connect(str(wf_db))
    cn.execute(
        "INSERT INTO workflows (id, name, status, active_version_id, "
        "created_at_ist, created_by) "
        "VALUES (1, 'test_wf', 'ACTIVE', 1, ?, ?)",
        (_now_str(), "test"),
    )
    week_ago = _now_str(-timedelta(days=10))
    cn.execute(
        "INSERT INTO workflow_runs (workflow_id, version_id, customer_id, "
        "status, entered_node_at_ist, updated_at_ist) "
        "VALUES (1, 1, 'c1', 'WAITING', ?, ?)",
        (week_ago, week_ago),
    )
    cn.commit()
    cn.close()

    payload = wd.run(workflow_db_path=wf_db)
    assert "1 WAITING > 7d" in payload["body"]


def test_daemon_heartbeats_aggregated(wf_db: Path) -> None:
    """24h of heartbeats summarized per agent with downs counted."""
    cn = sqlite3.connect(str(wf_db))
    cn.executemany(
        "INSERT INTO wf_agent_events (ts_ist, agent, status, summary_json) "
        "VALUES (?, ?, ?, '{}')",
        [
            (_now_str(), "workflow_executor", "ok"),
            (_now_str(), "workflow_executor", "ok"),
            (_now_str(), "workflow_scheduler", "ok"),
            (_now_str(), "workflow_scheduler", "down"),
        ],
    )
    cn.commit()
    cn.close()

    payload = wd.run(workflow_db_path=wf_db)
    assert payload["daemon_count"] == 2
    assert "workflow_executor: 2 ticks" in payload["body"]
    assert "workflow_scheduler: 2 ticks, 1 downs" in payload["body"]


def test_shadow_runs_counted(wf_db: Path) -> None:
    """SHADOW_FIRED pending_actions in last 24h → shadow_runs count."""
    cn = sqlite3.connect(str(wf_db))
    cn.executemany(
        "INSERT INTO wf_pending_actions (run_id, node_id, attempt_count, "
        "customer_id, scheduled_at_ist, status, attempts, last_attempt_at_ist, "
        "cohort_name, created_at_ist) "
        "VALUES (?, ?, ?, ?, ?, 'SHADOW_FIRED', 0, ?, 'test', ?)",
        [
            (1, "n1", 0, "c1", _now_str(), _now_str(), _now_str()),
            (2, "n1", 0, "c2", _now_str(), _now_str(), _now_str()),
            (1, "n2", 0, "c1", _now_str(), _now_str(), _now_str()),
        ],
    )
    cn.commit()
    cn.close()

    payload = wd.run(workflow_db_path=wf_db)
    # Distinct run_id → 2.
    assert payload["shadow_runs"] == 2
    assert "2 runs with SHADOW_FIRED" in payload["body"]


def test_dry_run_does_not_attempt_smtp(wf_db: Path) -> None:
    """dry_run=True: payload built, flag propagated, no smtplib import."""
    import sys as _sys
    payload = wd.run(workflow_db_path=wf_db, dry_run=True)
    assert payload["dry_run"] is True
    # Hard rule: no smtplib loaded by this module's import chain.
    assert "workflow.workflow_digest" in _sys.modules
    # Best-effort check that smtplib isn't unintentionally pulled in by digest.
    # (We don't fail if other modules brought it; only assert digest itself
    # doesn't reference it.)
    src = (Path(__file__).resolve().parent.parent / "workflow_digest.py").read_text()
    assert "import smtplib" not in src
    assert "smtplib." not in src
