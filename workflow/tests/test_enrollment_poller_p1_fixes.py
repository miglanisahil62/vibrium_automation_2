"""Phase 9 — tests for Phase 8's two deferred P1 fixes.

P1-1: Daily-cap race — file lock around `run()` prevents two overlapping ticks.
P1-2: ImportError → exit 3 (not silent exit 0 as before).
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

from workflow import enrollment_poller as ep


# --------------------------------------------------------------------- fixtures


@pytest.fixture
def wf_db(tmp_path: Path) -> Path:
    """Workflow.db with the minimum tables enrollment_poller needs."""
    db = tmp_path / "workflow.db"
    cn = sqlite3.connect(str(db))
    cn.executescript("""
        CREATE TABLE workflows (
          id INTEGER PRIMARY KEY, name TEXT, status TEXT,
          active_version_id INTEGER,
          shadow_mode INTEGER DEFAULT 1,
          requires_approval INTEGER DEFAULT 1,
          max_new_enrollments_per_day INTEGER DEFAULT 1000,
          enrollment_key_template TEXT DEFAULT '{customer_id}_{YYYY-MM-DD}',
          created_at_ist TEXT, created_by TEXT
        );
        CREATE TABLE workflow_versions (
          id INTEGER PRIMARY KEY, workflow_id INTEGER, version INTEGER,
          graph_json TEXT, validation_status TEXT, validation_errors TEXT,
          approved_at_ist TEXT, approved_by TEXT,
          created_at_ist TEXT, created_by TEXT,
          UNIQUE(workflow_id, version)
        );
        CREATE TABLE workflow_runs (
          id INTEGER PRIMARY KEY,
          workflow_id INTEGER, version_id INTEGER,
          customer_id TEXT, enrollment_key TEXT,
          current_node_id TEXT, current_node_type TEXT,
          entered_node_at_ist TEXT, status TEXT,
          scratchpad_json TEXT, ready_at_ist TEXT,
          enrolled_at_ist TEXT, updated_at_ist TEXT,
          terminated_at_ist TEXT, terminal_status TEXT
        );
        CREATE UNIQUE INDEX idx_runs_enrollment_key
          ON workflow_runs(workflow_id, customer_id, enrollment_key)
          WHERE enrollment_key IS NOT NULL;
        CREATE TABLE wf_kill_switch (
          id INTEGER PRIMARY KEY, ts_ist TEXT, action TEXT,
          reason TEXT, set_by TEXT
        );
    """)
    cn.commit()
    cn.close()
    return db


# --------------------------------------------------------------------- P1-1


def test_p1_1_file_lock_acquire_release(tmp_path: Path) -> None:
    """Lock acquires + releases cleanly. Lock file remains as sentinel."""
    lock_path = tmp_path / "locks" / "enrollment.lock"
    with ep._acquire_lock(lock_path):
        assert lock_path.exists()
    assert lock_path.exists()  # file persists; fd closed; flock released


def test_p1_1_lock_contention_raises(tmp_path: Path) -> None:
    """Second concurrent acquire on the same lock-file raises LockContended."""
    lock_path = tmp_path / "locks" / "enrollment.lock"
    with ep._acquire_lock(lock_path):
        # While still holding, attempt second acquire from same process.
        # On macOS / Linux fcntl, same-process double-lock is allowed (advisory
        # locks are per-fd, not per-process). So this test fires the second
        # acquire from a different OS-level file descriptor by re-running
        # _acquire_lock — which opens a fresh fd.
        with pytest.raises(ep.LockContended) as excinfo:
            with ep._acquire_lock(lock_path):
                pass  # pragma: no cover
        assert str(lock_path) in str(excinfo.value)


def test_p1_1_lock_breadcrumb_written(tmp_path: Path) -> None:
    """The breadcrumb (pid + start ts) is written to the lock file so
    `tail -f` reveals the holder."""
    lock_path = tmp_path / "locks" / "enrollment.lock"
    with ep._acquire_lock(lock_path):
        content = lock_path.read_text()
        assert "pid=" in content
        assert "start=" in content


def test_p1_1_run_under_contention_raises_lockcontended(
    wf_db: Path, tmp_path: Path
) -> None:
    """A second concurrent `run()` raises LockContended (the daemon-level
    surface — what launchd sees via the CLI _main wrapper)."""
    lock_path = tmp_path / "locks" / "enroll.lock"

    # Hold the lock from a side thread for the duration.
    holding = threading.Event()
    release = threading.Event()

    def _hold_lock():
        with ep._acquire_lock(lock_path):
            holding.set()
            release.wait(timeout=5.0)

    t = threading.Thread(target=_hold_lock, daemon=True)
    t.start()
    try:
        assert holding.wait(timeout=2.0), "side thread failed to acquire lock"

        # Real run() with the same lock_file → LockContended.
        with pytest.raises(ep.LockContended):
            ep.run(workflow_db_path=wf_db, lock_file=lock_path)
    finally:
        release.set()
        t.join(timeout=2.0)


def test_p1_1_cli_lock_contention_returns_0(
    wf_db: Path, tmp_path: Path, monkeypatch
) -> None:
    """LockContended at the CLI top-level → exit 0 (benign overlap)."""
    lock_path = tmp_path / "locks" / "enroll.lock"

    holding = threading.Event()
    release = threading.Event()

    def _hold_lock():
        with ep._acquire_lock(lock_path):
            holding.set()
            release.wait(timeout=5.0)

    t = threading.Thread(target=_hold_lock, daemon=True)
    t.start()
    try:
        assert holding.wait(timeout=2.0)
        rc = ep._main([
            "--workflow-db", str(wf_db),
            "--lock-file", str(lock_path),
        ])
        # Contention → log + exit 0 (next tick at +30 min catches up).
        assert rc == 0
    finally:
        release.set()
        t.join(timeout=2.0)


# --------------------------------------------------------------------- P1-2


def test_p1_2_importerror_propagates_from_run(
    wf_db: Path, monkeypatch
) -> None:
    """ImportError from `_is_callable_now()` (broken pre_call_gate symlink)
    bubbles up from run() rather than being silently swallowed."""

    def fake_is_callable_now():
        raise ImportError("synthetic: pre_call_gate symlink broken")

    monkeypatch.setattr(ep, "_is_callable_now", fake_is_callable_now)

    # Seed one ACTIVE workflow so we get past the kill-switch / no-workflows
    # short-circuits and into the is_callable_now() call.
    cn = sqlite3.connect(str(wf_db))
    cn.execute(
        "INSERT INTO workflows (name, status, active_version_id, "
        "created_at_ist, created_by) "
        "VALUES (?, 'ACTIVE', 1, ?, ?)",
        ("test_wf", "2026-05-30 09:00:00", "test"),
    )
    cn.commit()
    cn.close()

    with pytest.raises(ImportError) as excinfo:
        ep.run(workflow_db_path=wf_db)
    assert "synthetic" in str(excinfo.value)


def test_p1_2_cli_importerror_returns_3(
    wf_db: Path, monkeypatch
) -> None:
    """ImportError at the CLI top-level → exit 3 (not 0, not 1).

    Phase 8.5 detector A catches via missing-heartbeat within 30 min;
    the non-zero exit additionally surfaces in launchd stderr.
    """

    def fake_is_callable_now():
        raise ImportError("synthetic: pre_call_gate symlink broken")

    monkeypatch.setattr(ep, "_is_callable_now", fake_is_callable_now)

    cn = sqlite3.connect(str(wf_db))
    cn.execute(
        "INSERT INTO workflows (name, status, active_version_id, "
        "created_at_ist, created_by) "
        "VALUES (?, 'ACTIVE', 1, ?, ?)",
        ("test_wf", "2026-05-30 09:00:00", "test"),
    )
    cn.commit()
    cn.close()

    rc = ep._main(["--workflow-db", str(wf_db)])
    assert rc == 3
