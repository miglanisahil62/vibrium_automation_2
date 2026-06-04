"""Tests for workflow/ct_prefetch.py (WS1).

No live CT — `clevertap_profile.bulk_fetch_status` and the candidate loader are
monkeypatched. Exercises the UPSERT (attempts increment, NULL profile_json on
non-found), resumability complement, coverage gate, and table-absent tolerance.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from workflow import ct_prefetch as cp

_CACHE_DDL = (
    "CREATE TABLE ct_profile_cache ("
    " customer_id TEXT NOT NULL, cohort_date TEXT NOT NULL, status TEXT NOT NULL,"
    " attempts INTEGER NOT NULL DEFAULT 0, profile_json TEXT, fetched_at_ist TEXT NOT NULL,"
    " PRIMARY KEY (customer_id, cohort_date))"
)
_REC = {"record": {"profileData": {"coll_bot_calling": "ai_vb_calling_midv2"}}}


@pytest.fixture
def wf_db(tmp_path: Path) -> Path:
    p = tmp_path / "workflow.db"
    cn = sqlite3.connect(str(p))
    cn.execute(_CACHE_DDL)
    cn.commit()
    cn.close()
    return p


@pytest.fixture
def wf_db_no_table(tmp_path: Path) -> Path:
    p = tmp_path / "workflow_empty.db"
    cn = sqlite3.connect(str(p))
    cn.execute("CREATE TABLE _placeholder (x INTEGER)")
    cn.commit()
    cn.close()
    return p


def _read_row(wf_db: Path, cid: str, cohort_date: str):
    cn = sqlite3.connect(str(wf_db))
    try:
        return cn.execute(
            "SELECT status, attempts, profile_json FROM ct_profile_cache "
            "WHERE customer_id=? AND cohort_date=?",
            (cid, cohort_date),
        ).fetchone()
    finally:
        cn.close()


def test_upsert_insert_then_conflict_increments_and_nulls(wf_db: Path) -> None:
    d = "2026-06-05"
    cp._upsert(wf_db, d, {"c1": ("found", _REC)})
    status, attempts, pj = _read_row(wf_db, "c1", d)
    assert status == "found" and attempts == 1 and pj is not None
    assert json.loads(pj) == _REC

    # Conflict: same key, now not_found → attempts bumps, profile_json nulled.
    cp._upsert(wf_db, d, {"c1": ("not_found", None)})
    status, attempts, pj = _read_row(wf_db, "c1", d)
    assert status == "not_found" and attempts == 2 and pj is None


def test_read_cached_classifies(wf_db: Path) -> None:
    d = "2026-06-05"
    cp._upsert(wf_db, d, {
        "a": ("found", _REC),
        "b": ("not_found", None),
        "c": ("error", None),
    })
    got = cp.read_cached(wf_db, d, ["a", "b", "c", "missing"])
    assert got["a"][0] == "found" and got["a"][1] == _REC
    assert got["b"] == ("not_found", None)
    assert got["c"] == ("error", None)
    assert "missing" not in got  # cache miss → absent


def test_read_cached_table_absent_returns_empty(wf_db_no_table: Path) -> None:
    assert cp.read_cached(wf_db_no_table, "2026-06-05", ["a", "b"]) == {}


def test_prefetch_resumability_and_coverage(monkeypatch, wf_db: Path) -> None:
    d = "2026-06-05"
    # Pre-seed: a already found (terminal), e already error w/ attempts under cap.
    cp._upsert(wf_db, d, {"a": ("found", _REC)})
    cp._upsert(wf_db, d, {"e": ("error", None)})

    candidates = ["a", "b", "e"]
    monkeypatch.setattr(cp, "_load_candidate_ids", lambda cd: list(candidates))

    fetched_args = {}

    def _fake_bulk(ids, **kw):
        fetched_args["ids"] = list(ids)
        # b is new → found; e (retry) → found now.
        return {i: ("found", _REC) for i in ids}

    monkeypatch.setattr(cp.ctp, "bulk_fetch_status", _fake_bulk)
    monkeypatch.setattr(cp, "_MIN_COVERAGE", 0.80)

    stats = cp.prefetch(wf_db, cohort_date=d)
    # 'a' was terminal-found → NOT re-fetched; 'b' (new) + 'e' (error<cap) re-fetched.
    assert set(fetched_args["ids"]) == {"b", "e"}
    assert stats["requested"] == 3
    assert stats["found"] == 3            # a + b + e all found now
    assert stats["coverage_pct"] == 1.0


def test_prefetch_coverage_gate_raises(monkeypatch, wf_db: Path) -> None:
    d = "2026-06-05"
    monkeypatch.setattr(cp, "_load_candidate_ids", lambda cd: ["x", "y", "z", "w"])
    # Only 1 of 4 resolves → coverage 0.25 < 0.80 → raises.
    monkeypatch.setattr(
        cp.ctp, "bulk_fetch_status",
        lambda ids, **kw: {i: (("found", _REC) if i == "x" else ("error", None)) for i in ids},
    )
    monkeypatch.setattr(cp, "_MIN_COVERAGE", 0.80)
    with pytest.raises(RuntimeError):
        cp.prefetch(wf_db, cohort_date=d)


def test_prefetch_dry_run_no_writes(monkeypatch, wf_db: Path) -> None:
    d = "2026-06-05"
    monkeypatch.setattr(cp, "_load_candidate_ids", lambda cd: ["p", "q"])
    called = {"n": 0}

    def _fake_bulk(ids, **kw):
        called["n"] += 1
        return {i: ("found", _REC) for i in ids}

    monkeypatch.setattr(cp.ctp, "bulk_fetch_status", _fake_bulk)
    stats = cp.prefetch(wf_db, cohort_date=d, dry_run=True)
    assert stats.get("dry_run") is True
    assert called["n"] == 0                       # no fetch in dry-run
    assert cp.read_cached(wf_db, d, ["p", "q"]) == {}   # no writes


def test_prefetch_zero_candidates(monkeypatch, wf_db: Path) -> None:
    monkeypatch.setattr(cp, "_load_candidate_ids", lambda cd: [])
    stats = cp.prefetch(wf_db, cohort_date="2026-06-05")
    assert stats["requested"] == 0 and stats["coverage_pct"] == 0.0
