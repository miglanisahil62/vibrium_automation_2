"""Alert watcher tests.

Each of the 6 conditions (A–F) is simulated by inserting fixture rows into a
fresh tmp_path SQLite DB. We assert (a) the right alert fires, (b) the cooldown
suppresses a second fire within 60 min, (c) dry_run leaves alert_state
untouched, (d) email-payload shape is well-formed, (e) detect_f morning-health
fires/stays-silent across the funnel + check-hour cases, and (f)
_send_alert_email is gate-respecting and crash-safe.

SMTP is exercised only through _send_alert_email with WF_ALERTS_SEND unset (or a
missing config) — no real email is ever sent by the suite.
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
                updated_at_ist TEXT,
                enrolled_at_ist TEXT
            );
            CREATE TABLE wf_kill_switch (
                id INTEGER PRIMARY KEY,
                ts_ist TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                set_by TEXT
            );
            CREATE TABLE ct_profile_cache (
                customer_id TEXT NOT NULL,
                cohort_date TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (customer_id, cohort_date)
            );
            CREATE TABLE wf_pending_actions (
                id INTEGER PRIMARY KEY,
                customer_id TEXT NOT NULL,
                status TEXT NOT NULL,
                fired_at_ist TEXT
            );
            """
        )
        # Seed a HEALTHY morning funnel (prefetched>0, enrolled>0, real-fired>0)
        # so detect_f_morning_health stays silent by default — the A–E tests
        # don't want the morning-health net firing on top of their assertions.
        # The detect_f tests below override this with unhealthy states.
        today = datetime.now(IST).strftime("%Y-%m-%d")
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO ct_profile_cache(customer_id, cohort_date, status) "
            "VALUES ('seed-cust', ?, 'found')", (today,))
        conn.execute(
            "INSERT INTO workflow_runs(workflow_id, customer_id, status, enrolled_at_ist) "
            "VALUES (1, 'seed-cust', 'ACTIVE', ?)", (ts,))
        conn.execute(
            "INSERT INTO wf_pending_actions(customer_id, status, fired_at_ist) "
            "VALUES ('seed-cust', 'FIRED', ?)", (ts,))
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


# ---------------------------------------------------------------- detect_f (morning health)
# Called directly with a controlled `now` so they're deterministic regardless of
# the wall-clock hour the suite runs at.


def _set_funnel(db: Path, *, prefetched=0, enrolled=0, fired=0, shadow_fired=0,
                today: str | None = None) -> None:
    """Reset the morning-funnel tables to exact counts for the cohort day."""
    today = today or datetime.now(IST).strftime("%Y-%m-%d")
    ts = f"{today} 10:00:00"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("DELETE FROM ct_profile_cache")
        conn.execute("DELETE FROM workflow_runs")
        conn.execute("DELETE FROM wf_pending_actions")
        for i in range(prefetched):
            conn.execute("INSERT INTO ct_profile_cache(customer_id,cohort_date,status) "
                         "VALUES (?,?,'found')", (f"c{i}", today))
        for i in range(enrolled):
            conn.execute("INSERT INTO workflow_runs(workflow_id,customer_id,status,enrolled_at_ist) "
                         "VALUES (1,?,'ACTIVE',?)", (f"c{i}", ts))
        for i in range(fired):
            conn.execute("INSERT INTO wf_pending_actions(customer_id,status,fired_at_ist) "
                         "VALUES (?,'FIRED',?)", (f"c{i}", ts))
        for i in range(shadow_fired):
            conn.execute("INSERT INTO wf_pending_actions(customer_id,status,fired_at_ist) "
                         "VALUES (?,'SHADOW_FIRED',?)", (f"s{i}", ts))
        conn.commit()
    finally:
        conn.close()


def _run_detect_f(db: Path, now: datetime) -> list:
    conn = sqlite3.connect(str(db))
    try:
        return alerts.detect_f_morning_health(conn, now)
    finally:
        conn.close()


def _today_at(hour: int) -> datetime:
    return datetime.now(IST).replace(hour=hour, minute=0, second=0, microsecond=0)


def test_f_prefetch_zero_fires_p0_at_9(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=0, enrolled=0, fired=0)
    out = _run_detect_f(db, _today_at(9))
    assert any(a.condition == "F_MORNING_HEALTH" and a.severity == "P0" for a in out)


def test_f_enrolled_zero_fires_after_11(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=100, enrolled=0, fired=0)
    out = _run_detect_f(db, _today_at(11))
    assert any(a.condition == "F_MORNING_HEALTH" for a in out)


def test_f_enrolled_zero_silent_before_11(tmp_path: Path) -> None:
    # 09:00–10:59 the enrollment catch-ups (08:30/10:00) may still be landing.
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=100, enrolled=0, fired=0)
    assert _run_detect_f(db, _today_at(10)) == []


def test_f_shadow_fired_not_counted_as_fired(tmp_path: Path) -> None:
    # P1-1 regression: SHADOW_FIRED stamps fired_at_ist but is NOT a real call.
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=100, enrolled=100, fired=0, shadow_fired=50)
    out = _run_detect_f(db, _today_at(12))
    assert any(a.condition == "F_MORNING_HEALTH" for a in out), \
        "0 REAL fires (50 shadow) must still trip the net"


def test_f_healthy_funnel_silent(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=100, enrolled=98, fired=40)
    assert _run_detect_f(db, _today_at(12)) == []


def test_f_dormant_before_9(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    _set_funnel(db, prefetched=0, enrolled=0, fired=0)
    assert _run_detect_f(db, _today_at(8)) == []


def test_f_partial_schema_emits_p1(tmp_path: Path) -> None:
    # Funnel tables dropped while the rest of the schema exists → net disarmed
    # → must be LOUD (P1), not silently skipped (P1-2).
    db = _make_db(tmp_path)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("DROP TABLE ct_profile_cache")
        conn.execute("DROP TABLE wf_pending_actions")
        conn.commit()
    finally:
        conn.close()
    out = _run_detect_f(db, _today_at(11))
    assert any(a.condition == "F_MORNING_SCHEMA" and a.severity == "P1" for a in out)


def test_f_bare_db_skips(tmp_path: Path) -> None:
    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()
    assert _run_detect_f(db, _today_at(11)) == []


# ---------------------------------------------------------------- _send_alert_email


def test_send_alert_email_gate_off_returns_false(monkeypatch) -> None:
    # WF_ALERTS_SEND unset → no-op, never touches SMTP.
    monkeypatch.delenv("WF_ALERTS_SEND", raising=False)
    assert alerts._send_alert_email(
        {"to": "x@y.com", "subject": "s", "body": "b"}) is False


def test_send_alert_email_missing_config_no_raise(monkeypatch) -> None:
    # Gate on but config file missing → caught, returns False, never raises.
    monkeypatch.setenv("WF_ALERTS_SEND", "1")
    monkeypatch.setattr(alerts, "_GMAIL_CONFIG", "/nonexistent/cfg.json")
    assert alerts._send_alert_email(
        {"to": "x@y.com", "subject": "s", "body": "b"}) is False
