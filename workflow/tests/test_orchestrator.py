"""Phase 9 — workflow_orchestrator dispatcher tests.

Covers:
  * Each --mode dispatches to the correct daemon callable.
  * Heartbeat stamping: 'started' + ('ok' | 'down').
  * Daemon name contract (Phase 8.5 audit): exactly the 6 strings.
  * Exception in daemon → exit 1 + 'down' heartbeat with traceback tail.
  * ImportError → exit 3 + 'down' heartbeat with error_type='ImportError'.
  * Invalid --mode → exit 2.
  * Missing required CLI args → exit 2.
  * Each mode's extra kwargs threaded through correctly.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from workflow import workflow_orchestrator as orch


# --------------------------------------------------------------------- fixtures


@pytest.fixture
def wf_db(tmp_path: Path) -> Path:
    """Tiny workflow.db with just the wf_agent_events table the orchestrator
    needs. Migrations are tested separately in test_migrations.py."""
    db = tmp_path / "workflow.db"
    cn = sqlite3.connect(str(db))
    cn.execute(
        "CREATE TABLE wf_agent_events ("
        "  id INTEGER PRIMARY KEY,"
        "  ts_ist TEXT NOT NULL,"
        "  agent TEXT NOT NULL,"
        "  status TEXT,"
        "  summary_json TEXT"
        ")"
    )
    cn.commit()
    cn.close()
    return db


def _read_heartbeats(db: Path, agent: str | None = None) -> list[dict]:
    cn = sqlite3.connect(str(db))
    if agent is None:
        rows = cn.execute(
            "SELECT ts_ist, agent, status, summary_json FROM wf_agent_events "
            "ORDER BY id"
        ).fetchall()
    else:
        rows = cn.execute(
            "SELECT ts_ist, agent, status, summary_json FROM wf_agent_events "
            "WHERE agent=? ORDER BY id",
            (agent,),
        ).fetchall()
    cn.close()
    return [
        {"ts_ist": r[0], "agent": r[1], "status": r[2],
         "summary": json.loads(r[3] or "{}")}
        for r in rows
    ]


# --------------------------------------------------------------------- tests


def test_daemon_name_contract_six_keys() -> None:
    """The Phase 8.5 audit pins these 6 agent strings. Test guards drift."""
    expected = {
        "workflow_executor",
        "workflow_scheduler",
        "workflow_ingest",
        "workflow_enrollment",
        "workflow_alerts",
        "workflow_digest",
    }
    actual = {triple[0] for triple in orch._MODE_REGISTRY.values()}
    assert actual == expected, (
        f"agent-name drift detected. Phase 8.5 detector A will misfire.\n"
        f"  expected: {sorted(expected)}\n"
        f"  actual:   {sorted(actual)}"
    )


def test_invalid_mode_systemexit() -> None:
    """argparse rejects bad --mode with SystemExit(2)."""
    with pytest.raises(SystemExit) as excinfo:
        orch._main(["--mode", "bogus", "--workflow-db", "/tmp/x"])
    assert excinfo.value.code == 2


def test_scheduler_mode_missing_vibrium_db_returns_2(wf_db: Path) -> None:
    rc = orch._main([
        "--mode", "scheduler", "--workflow-db", str(wf_db),
    ])
    assert rc == 2


def test_dispatch_happy_path_stamps_started_and_ok(
    wf_db: Path, monkeypatch
) -> None:
    """A mode that returns cleanly produces exactly two heartbeats:
    started + ok."""

    def fake_run(*, workflow_db_path, dry_run=False, **kwargs):
        return {"processed": 5, "advanced": 4, "errored": 0}

    # Patch the alerts module to use our fake_run (alerts is the simplest mode).
    import workflow.alerts as alerts_mod
    monkeypatch.setattr(alerts_mod, "run", fake_run)

    rc = orch._dispatch("alerts", wf_db, {"dry_run": True})
    assert rc == 0

    hb = _read_heartbeats(wf_db, agent="workflow_alerts")
    assert len(hb) == 2
    assert hb[0]["status"] == "started"
    assert hb[1]["status"] == "ok"
    assert hb[1]["summary"]["result"]["processed"] == 5


def test_dispatch_exception_returns_1_and_stamps_down(
    wf_db: Path, monkeypatch
) -> None:
    """Exception inside the daemon → exit 1 + 'down' heartbeat with type+msg."""

    def fake_run(*, workflow_db_path, dry_run=False, **kwargs):
        raise RuntimeError("synthetic crash")

    import workflow.alerts as alerts_mod
    monkeypatch.setattr(alerts_mod, "run", fake_run)

    rc = orch._dispatch("alerts", wf_db, {"dry_run": True})
    assert rc == 1

    hb = _read_heartbeats(wf_db, agent="workflow_alerts")
    assert len(hb) == 2
    assert hb[0]["status"] == "started"
    assert hb[1]["status"] == "down"
    assert hb[1]["summary"]["error_type"] == "RuntimeError"
    assert "synthetic crash" in hb[1]["summary"]["error_msg"]
    # Traceback tail captured for forensics.
    assert "traceback_tail" in hb[1]["summary"]
    assert isinstance(hb[1]["summary"]["traceback_tail"], list)


def test_dispatch_importerror_returns_3(wf_db: Path, monkeypatch) -> None:
    """Phase 8 P1-2 + Phase 9 contract: ImportError → exit 3 (not 1).

    Used by launchd to distinguish ops-failure (broken symlink → page operator)
    from generic crash (likely transient).
    """
    # Force the importlib.import_module call to raise ImportError.
    import importlib as importlib_mod

    def fake_import(name):
        raise ImportError(f"synthetic: no module named {name!r}")

    monkeypatch.setattr(orch, "importlib", _Mod(import_module=fake_import))

    rc = orch._dispatch("alerts", wf_db, {"dry_run": True})
    assert rc == 3

    hb = _read_heartbeats(wf_db, agent="workflow_alerts")
    assert hb[-1]["status"] == "down"
    assert hb[-1]["summary"]["error_type"] == "ImportError"


def test_emit_heartbeat_swallows_sqlite_errors(monkeypatch, tmp_path) -> None:
    """If the workflow.db is unreachable, _emit_heartbeat MUST NOT raise —
    the daemon's own work is more important than the heartbeat."""
    bogus_db = tmp_path / "this" / "does" / "not" / "exist.db"
    # Should not raise. Returns None.
    result = orch._emit_heartbeat(bogus_db, "workflow_alerts", "ok", {})
    assert result is None


def test_each_mode_threads_correct_kwargs(wf_db: Path, monkeypatch) -> None:
    """The CLI builds an `extra_kwargs` dict that's threaded into the daemon's
    run(...) call. This test asserts the per-mode mapping."""
    received_kwargs: dict[str, dict] = {}

    def make_capture(name):
        def fake_run(**kwargs):
            received_kwargs[name] = kwargs
            return {"ok": True}
        return fake_run

    import workflow.alerts as alerts_mod
    import workflow.workflow_ingest as ingest_mod
    import workflow.enrollment_poller as ep_mod
    import workflow.workflow_digest as digest_mod

    monkeypatch.setattr(alerts_mod, "run", make_capture("alerts"))
    monkeypatch.setattr(ingest_mod, "run", make_capture("ingest"))
    monkeypatch.setattr(ep_mod, "run", make_capture("enrollment"))
    monkeypatch.setattr(digest_mod, "run", make_capture("digest"))

    # alerts: dry_run=True
    assert orch._dispatch("alerts", wf_db, {"dry_run": True}) == 0
    assert received_kwargs["alerts"]["dry_run"] is True

    # ingest: no extras
    assert orch._dispatch("ingest", wf_db, {"dry_run": False}) == 0
    assert received_kwargs["ingest"]["dry_run"] is False

    # enrollment: force=True
    assert orch._dispatch("enrollment", wf_db,
                          {"dry_run": False, "force": True}) == 0
    assert received_kwargs["enrollment"]["force"] is True

    # digest: just workflow_db
    assert orch._dispatch("digest", wf_db, {"dry_run": False}) == 0


# Helper for the monkeypatched importlib.

class _Mod:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
