"""Phase 8.5 alert watcher tests.

Each of the 5 conditions is simulated by inserting fixture rows into a fresh
tmp_path SQLite DB. We assert (a) the right alert fires, (b) the cooldown
suppresses a second fire within 60 min, (c) dry_run leaves alert_state
untouched, and (d) email-payload shape is well-formed.

We never call SMTP — Phase 8.5 is payload-only.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from workflow import alerts
from workflow.alerts import IST, Alert, run


# ---------------------------------------------------------------- helpers


def _ist_str(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(IST).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _make_db(tmp_path: Path) -> Path:
    """Create a workflow.db with only the tables the alert watcher reads.
    We deliberately mirror the column subset rather than depending on
    migration 001 to keep the test fast and self-contained."""
    db = tmp_path / "workflow.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript(
            """
            CREATE TABLE wf_agent_events (
                id INTEGER PRIMARY KEY,
                ts_ist TEXT NOT NULL,
                agent TEXT NOT NULL,
                status TEXT,
                summary_json TEXT
            );
            CREATE TABLE workflow_runs (
                id INTEGER PRIMARY KEY,
                workflow_id INTEGER NOT NULL,
                version_id INTEGER NOT NULL DEFAULT 1,
                customer_id TEXT NOT NULL,
                current_node_id TEXT,
                status TEXT NOT NULL,
                updated_at_ist TEXT
            );
            CREATE TABLE wf_kill_switch (
                id INTEGER PRIMARY KEY,
                ts_ist TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                set_by TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db


def _seed_fresh_heartbeats(db: Path, now: datetime) -> None:
    """Stamp recent heartbeats for all 4 tracked daemons so detector A is
    silent unless the test explicitly removes/ages them."""
    conn = sqlite3.connect(str(db))
    try:
        for daemon in alerts.TRACKED_DAEMONS:
            conn.execute(
                "INSERT INTO wf_agent_events(ts_ist, agent, status) VALUES (?, ?, 'ok')",
                (_ist_str(now - timedelta(minutes=2)), daemon),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------- tests
# 1. Condition A — daemon down


def test_a_daemon_down_fires_when_heartbeat_stale(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    # Three fresh, one stale (45 min ago, > 30 min threshold).
    conn = sqlite3.connect(str(db))
    try:
        for daemon in alerts.TRACKED_DAEMONS[:3]:
            conn.execute(
                "INSERT INTO wf_agent_events(ts_ist, agent, status) VALUES (?, ?, 'ok')",
                (_ist_str(now - timedelta(minutes=2)), daemon),
            )
        # 4th daemon is stale.
        conn.execute(
            "INSERT INTO wf_agent_events(ts_ist, agent, status) VALUES (?, ?, 'ok')",
            (_ist_str(now - timedelta(minutes=45)), alerts.TRACKED_DAEMONS[3]),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conditions = [a["condition"] for a in stats["alerts"]]
    assert "A_DAEMON_DOWN" in conditions
    a = next(a for a in stats["alerts"] if a["condition"] == "A_DAEMON_DOWN")
    assert a["severity"] == "P0"
    assert alerts.TRACKED_DAEMONS[3] in a["subject"]


# 2. Condition B — run stuck in ERROR


def test_b_run_error_fires_after_2h(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO workflow_runs(workflow_id, customer_id, current_node_id, "
            "status, updated_at_ist) VALUES (1, 'cust_001', 'node_x', 'ERROR', ?)",
            (_ist_str(now - timedelta(hours=3)),),
        )
        # A fresh ERROR row should NOT count (only 30 min old).
        conn.execute(
            "INSERT INTO workflow_runs(workflow_id, customer_id, current_node_id, "
            "status, updated_at_ist) VALUES (1, 'cust_002', 'node_y', 'ERROR', ?)",
            (_ist_str(now - timedelta(minutes=30)),),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conds = [a["condition"] for a in stats["alerts"]]
    assert "B_RUN_ERROR" in conds
    b = next(a for a in stats["alerts"] if a["condition"] == "B_RUN_ERROR")
    assert b["details"]["total"] == 1
    assert b["details"]["sample"][0]["customer_id"] == "cust_001"


# 3. Condition C — run stuck in WAITING


def test_c_run_waiting_fires_after_7d(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO workflow_runs(workflow_id, customer_id, current_node_id, "
            "status, updated_at_ist) VALUES (2, 'cust_w1', 'wait_n', 'WAITING', ?)",
            (_ist_str(now - timedelta(days=10)),),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conds = [a["condition"] for a in stats["alerts"]]
    assert "C_RUN_WAITING" in conds


# 4. Condition D — kill switch active >1h


def test_d_kill_switch_fires_when_active(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) "
            "VALUES (?, 'KILL', 'bot fire issue', 'sahil')",
            (_ist_str(now - timedelta(hours=3)),),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conds = [a["condition"] for a in stats["alerts"]]
    assert "D_KILL_SWITCH" in conds
    d = next(a for a in stats["alerts"] if a["condition"] == "D_KILL_SWITCH")
    assert d["details"]["minutes_active"] >= 60


def test_d_kill_switch_silent_after_resume(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) "
            "VALUES (?, 'KILL', 'test', 'sahil')",
            (_ist_str(now - timedelta(hours=3)),),
        )
        conn.execute(
            "INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) "
            "VALUES (?, 'RESUME', 'resolved', 'sahil')",
            (_ist_str(now - timedelta(hours=2)),),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conds = [a["condition"] for a in stats["alerts"]]
    assert "D_KILL_SWITCH" not in conds


# 5. Condition E — queue building up


def test_e_queue_buildup_fires(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        # Hot tick: 95 rows > threshold (90).
        conn.execute(
            "INSERT INTO wf_agent_events(ts_ist, agent, status, summary_json) "
            "VALUES (?, 'workflow_executor', 'ok', ?)",
            (
                _ist_str(now - timedelta(minutes=5)),
                json.dumps({"rows_processed": 95}),
            ),
        )
        # Cool tick: 10 rows.
        conn.execute(
            "INSERT INTO wf_agent_events(ts_ist, agent, status, summary_json) "
            "VALUES (?, 'workflow_scheduler', 'ok', ?)",
            (
                _ist_str(now - timedelta(minutes=5)),
                json.dumps({"processed": 10}),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    stats = run(db, dry_run=True)
    conds = [a["condition"] for a in stats["alerts"]]
    assert "E_QUEUE_BUILDUP" in conds
    e = next(a for a in stats["alerts"] if a["condition"] == "E_QUEUE_BUILDUP")
    assert len(e["details"]["hot_ticks"]) == 1
    assert e["details"]["hot_ticks"][0]["rows_processed"] == 95


# 6. Cooldown — same condition twice in 60 min → only first fires


def test_cooldown_suppresses_second_fire(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) "
            "VALUES (?, 'KILL', 'test', 'sahil')",
            (_ist_str(now - timedelta(hours=3)),),
        )
        conn.commit()
    finally:
        conn.close()

    # First fire — real (not dry-run), so alert_state is updated.
    first = run(db, dry_run=False)
    assert any(a["condition"] == "D_KILL_SWITCH" for a in first["alerts"])
    assert first["alerts_skipped_cooldown"] == 0

    # Second fire same minute — suppressed by cooldown.
    second = run(db, dry_run=False)
    assert not any(a["condition"] == "D_KILL_SWITCH" for a in second["alerts"])
    assert second["alerts_skipped_cooldown"] >= 1


# 7. dry_run does NOT write to alert_state


def test_dry_run_does_not_persist_cooldown(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    now = datetime.now(IST)
    _seed_fresh_heartbeats(db, now)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) "
            "VALUES (?, 'KILL', 'test', 'sahil')",
            (_ist_str(now - timedelta(hours=3)),),
        )
        conn.commit()
    finally:
        conn.close()

    stats1 = run(db, dry_run=True)
    stats2 = run(db, dry_run=True)
    # Both fire — dry_run never marks the cooldown row.
    assert any(a["condition"] == "D_KILL_SWITCH" for a in stats1["alerts"])
    assert any(a["condition"] == "D_KILL_SWITCH" for a in stats2["alerts"])

    # Verify alert_state row is absent.
    conn = sqlite3.connect(str(db))
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM alert_state WHERE condition='D_KILL_SWITCH'"
        )
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


# 8. Email payload shape — to/from/subject/body non-empty strings


def test_email_payload_shape() -> None:
    a = Alert(
        condition="A_DAEMON_DOWN",
        severity="P0",
        subject="test subject",
        body="test body\nline 2",
        details={"x": 1},
    )
    payload = a.as_email_payload()
    for field in ("to", "from", "subject", "body", "severity", "condition"):
        assert field in payload
        if field in ("to", "from", "subject", "body", "severity", "condition"):
            assert isinstance(payload[field], str) and payload[field]
    assert payload["to"] == alerts.ALERT_EMAIL_TO
    assert payload["from"] == alerts.ALERT_EMAIL_FROM
    assert payload["details"] == {"x": 1}


# 9. Empty DB — no alerts (smoke)


def test_empty_db_emits_nothing(tmp_path: Path) -> None:
    # Path exists but no schema → detector queries raise OperationalError
    # and are tolerated. Condition A is special: it queries wf_agent_events
    # which doesn't exist either, so it's skipped. Stats should be all-zero.
    db = tmp_path / "empty.db"
    sqlite3.connect(str(db)).close()  # creates empty file
    stats = run(db, dry_run=True)
    assert stats["alerts_emitted"] == 0
    assert stats["alerts_skipped_cooldown"] == 0
    assert stats["alerts"] == []
