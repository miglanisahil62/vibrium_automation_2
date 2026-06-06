"""Tests for WS10 — the coll_agent_allocation CT write in agent_assignment_emailer.

No live CleverTap: clevertap_profile.set_profile is monkeypatched. Covers the
CRITICAL GUARD (only reason='max_attempts_reached' is flagged), idempotency
(the written marker prevents re-writes), dry-run (no CT, no DB stamp), and
the dedupe-by-customer_id behaviour.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import scripts.agent_assignment_emailer as em  # noqa: E402
from workflow import clevertap_profile  # noqa: E402


class _FakeSetResult:
    def __init__(self, success=True, error_code=None):
        self.success = success
        self.error_code = error_code
        self.raw_response = {}


def _make_db(tmp_path: Path, rows: list[tuple]) -> str:
    """rows = list of (customer_id, reason). Builds agent_assignments with the
    WS10 column and inserts the rows."""
    db = tmp_path / "wf.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE agent_assignments ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id TEXT, reason TEXT,"
        " source TEXT, assigned_at_ist TEXT, assigned_to TEXT,"
        " resolved_at_ist TEXT, resolution_note TEXT, run_id INTEGER,"
        " ct_allocation_written_at_ist TEXT)"
    )
    conn.executemany(
        "INSERT INTO agent_assignments (customer_id, reason) VALUES (?, ?)", rows
    )
    conn.commit()
    conn.close()
    return str(db)


def _written(db: str) -> set:
    conn = sqlite3.connect(db)
    out = {
        r[0]
        for r in conn.execute(
            "SELECT customer_id FROM agent_assignments "
            "WHERE ct_allocation_written_at_ist IS NOT NULL"
        )
    }
    conn.close()
    return out


def test_only_max_attempts_reached_is_flagged(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        clevertap_profile, "set_profile",
        lambda cid, props, **kw: calls.append((cid, props)) or _FakeSetResult(),
    )
    db = _make_db(tmp_path, [
        ("100", "max_attempts_reached"),
        ("200", "dispute_or_nrp"),          # must NOT be flagged
        ("300", "disposition_timeout_24h"),  # must NOT be flagged
        ("400", "max_attempts_reached"),
    ])
    stats = em._write_ct_allocation(db, dry_run=False, creds_path=None)
    assert stats["qualified"] == 2
    assert stats["written"] == 2
    # CT called only for the two qualified customers, with the right property.
    assert sorted(c[0] for c in calls) == ["100", "400"]
    assert all(c[1] == {em.CT_ALLOCATION_PROPERTY: em.CT_ALLOCATION_VALUE} for c in calls)
    assert _written(db) == {"100", "400"}


def test_idempotent_second_pass_writes_nothing(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        clevertap_profile, "set_profile",
        lambda cid, props, **kw: calls.append(cid) or _FakeSetResult(),
    )
    db = _make_db(tmp_path, [("100", "max_attempts_reached")])
    em._write_ct_allocation(db, dry_run=False, creds_path=None)
    second = em._write_ct_allocation(db, dry_run=False, creds_path=None)
    assert second["qualified"] == 0
    assert second["written"] == 0
    assert calls == ["100"]  # exactly one CT call across both passes


def test_dry_run_writes_no_ct_marker(tmp_path, monkeypatch):
    monkeypatch.setattr(
        clevertap_profile, "set_profile",
        lambda cid, props, **kw: _FakeSetResult(),
    )
    db = _make_db(tmp_path, [("100", "max_attempts_reached")])
    stats = em._write_ct_allocation(db, dry_run=True, creds_path=None)
    assert stats["written"] == 1      # CT dry-run "succeeds"
    assert _written(db) == set()      # but the DB marker is NOT stamped


def test_duplicate_customer_rows_stamped_once(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        clevertap_profile, "set_profile",
        lambda cid, props, **kw: calls.append(cid) or _FakeSetResult(),
    )
    # Same customer, two assignment rows (no UNIQUE constraint on the table).
    db = _make_db(tmp_path, [
        ("100", "max_attempts_reached"),
        ("100", "max_attempts_reached"),
    ])
    stats = em._write_ct_allocation(db, dry_run=False, creds_path=None)
    assert stats["qualified"] == 1
    assert stats["skipped_dupe"] == 1
    assert calls == ["100"]  # one CT call
    # BOTH rows stamped, so a later pass re-writes nothing.
    conn = sqlite3.connect(db)
    n_unstamped = conn.execute(
        "SELECT COUNT(*) FROM agent_assignments "
        "WHERE ct_allocation_written_at_ist IS NULL"
    ).fetchone()[0]
    conn.close()
    assert n_unstamped == 0


def test_ct_failure_leaves_unstamped_for_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(
        clevertap_profile, "set_profile",
        lambda cid, props, **kw: _FakeSetResult(success=False, error_code=429),
    )
    db = _make_db(tmp_path, [("100", "max_attempts_reached")])
    stats = em._write_ct_allocation(db, dry_run=False, creds_path=None)
    assert stats["failed"] == 1
    assert stats["written"] == 0
    assert _written(db) == set()  # unstamped → retried next pass


def test_missing_column_is_nonfatal(tmp_path, monkeypatch):
    # Pre-migration DB (no ct_allocation_written_at_ist column) → logged, not raised.
    db = tmp_path / "old.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE agent_assignments (id INTEGER PRIMARY KEY, customer_id TEXT, reason TEXT)"
    )
    conn.execute("INSERT INTO agent_assignments (customer_id, reason) VALUES ('1','max_attempts_reached')")
    conn.commit()
    conn.close()
    stats = em._write_ct_allocation(str(db), dry_run=False, creds_path=None)
    assert "error" in stats
    assert stats["written"] == 0


# ---------------------------------------------------------------- _fetch_assignments
# calls_made/calls_refused computation + legacy-reason filter. The P0 regression:
# an all-refused (bot-couldn't-reach) max_attempts_reached customer has
# calls_made=0 but MUST still reach an agent.

from datetime import datetime  # noqa: E402


def _make_full_db(tmp_path: Path, specs: list[dict]) -> str:
    db = tmp_path / "wf_full.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE agent_assignments (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " customer_id TEXT, reason TEXT, source TEXT, assigned_at_ist TEXT,"
        " assigned_to TEXT, resolved_at_ist TEXT, resolution_note TEXT,"
        " run_id INTEGER, ct_allocation_written_at_ist TEXT)"
    )
    conn.execute(
        "CREATE TABLE workflow_runs (id INTEGER PRIMARY KEY, scratchpad_json TEXT,"
        " terminal_status TEXT, enrolled_at_ist TEXT, terminated_at_ist TEXT)"
    )
    conn.execute(
        "CREATE TABLE wf_pending_actions (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " run_id INTEGER, status TEXT)"
    )
    today = datetime.now(em.IST).strftime("%Y-%m-%d %H:%M:%S")
    for s in specs:
        conn.execute(
            "INSERT INTO agent_assignments (customer_id, reason, run_id, assigned_at_ist)"
            " VALUES (?,?,?,?)", (s["customer_id"], s["reason"], s["run_id"], today))
        conn.execute("INSERT INTO workflow_runs (id, scratchpad_json) VALUES (?, '{}')",
                     (s["run_id"],))
        for _ in range(s.get("fired", 0)):
            conn.execute("INSERT INTO wf_pending_actions (run_id, status) VALUES (?, 'FIRED')",
                         (s["run_id"],))
        for _ in range(s.get("suppressed", 0)):
            conn.execute("INSERT INTO wf_pending_actions (run_id, status) VALUES (?, 'SUPPRESSED')",
                         (s["run_id"],))
    conn.commit()
    conn.close()
    return str(db)


def test_fetch_calls_made_counts_real_fires(tmp_path):
    db = _make_full_db(tmp_path, [
        {"customer_id": "1", "reason": "rtp_needs_review", "run_id": 10, "fired": 1}])
    rows = em._fetch_assignments(db, None, all_time=True)
    assert len(rows) == 1 and rows[0]["calls_made"] == 1


def test_fetch_keeps_all_refused_max_attempts(tmp_path):
    # P0 regression: all attempts gate-refused (DND/cap/window) → calls_made=0
    # but the customer MUST still be handed to an agent (bot couldn't reach them).
    db = _make_full_db(tmp_path, [
        {"customer_id": "2", "reason": "max_attempts_reached", "run_id": 11,
         "fired": 0, "suppressed": 3}])
    rows = em._fetch_assignments(db, None, all_time=True)
    assert len(rows) == 1
    assert rows[0]["calls_made"] == 0
    assert rows[0]["calls_refused"] == 3


def test_fetch_drops_legacy_timeout_even_if_fired(tmp_path):
    db = _make_full_db(tmp_path, [
        {"customer_id": "3", "reason": "disposition_timeout_24h", "run_id": 12, "fired": 2}])
    assert em._fetch_assignments(db, None, all_time=True) == []


def test_calls_cell_flags_unreached():
    assert em._calls_cell({"calls_made": 2}) == "2"
    assert "unreached" in em._calls_cell({"calls_made": 0, "calls_refused": 3})
    assert "3 refused" in em._calls_cell({"calls_made": 0, "calls_refused": 3})
