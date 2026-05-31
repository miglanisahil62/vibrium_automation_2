"""Tests for scripts/fetch_dpd1_candidates.py.

No live Redshift — `_fetch_candidates` is monkeypatched. Covers id
normalisation, the zero-rows-is-failure contract, --allow-empty, atomic-write
cleanup, and CLI validation of --date / --limit.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import fetch_dpd1_candidates as fd  # noqa: E402


# ----------------------------------------------------------- _normalize_ids


def test_normalize_strips_float_dot_zero():
    assert fd._normalize_ids([12345.0, "678", 90], None) == ["12345", "678", "90"]


def test_normalize_drops_none_and_blank():
    assert fd._normalize_ids([None, "", "  ", "42"], None) == ["42"]


def test_normalize_dedups_preserving_order():
    # 100 and "100" and 100.0 collapse to one; order preserved.
    assert fd._normalize_ids([100, "100", 100.0, 200], None) == ["100", "200"]


def test_normalize_limit_caps():
    assert fd._normalize_ids(["1", "2", "3", "4"], 2) == ["1", "2"]


def test_normalize_limit_zero_is_ignored():
    # limit < 1 is meaningless and must NOT truncate (CLI rejects 0 anyway).
    assert fd._normalize_ids(["1", "2"], 0) == ["1", "2"]


# ----------------------------------------------------------- do_work paths


def _args(**kw):
    base = dict(dry_run=False, limit=None, allow_empty=False, date="2026-05-30")
    base.update(kw)
    return argparse.Namespace(**base)


def test_zero_rows_raises_without_allow_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(fd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(fd, "_fetch_candidates", lambda limit: [])
    with pytest.raises(RuntimeError):
        fd.do_work(_args())
    # No file written.
    assert list(tmp_path.glob("*.csv")) == []


def test_allow_empty_writes_header_only(monkeypatch, tmp_path):
    monkeypatch.setattr(fd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(fd, "_fetch_candidates", lambda limit: [])
    summary = fd.do_work(_args(allow_empty=True))
    assert summary["n_written"] == 0
    out = tmp_path / "dpd1_candidates_2026-05-30.csv"
    assert out.exists()
    assert out.read_text().strip() == "customer_id"


def test_happy_path_writes_dated_csv(monkeypatch, tmp_path):
    monkeypatch.setattr(fd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(fd, "_fetch_candidates", lambda limit: ["11", "22", "33"])
    summary = fd.do_work(_args())
    assert summary["n_written"] == 3
    out = tmp_path / "dpd1_candidates_2026-05-30.csv"
    lines = out.read_text().splitlines()
    assert lines == ["customer_id", "11", "22", "33"]


def test_dry_run_writes_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr(fd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(fd, "_fetch_candidates", lambda limit: ["1", "2"])
    summary = fd.do_work(_args(dry_run=True))
    assert summary["dry_run"] is True
    assert list(tmp_path.glob("*.csv")) == []


# ----------------------------------------------------------- atomic write


def test_atomic_write_cleans_tmp_on_failure(monkeypatch, tmp_path):
    out = tmp_path / "out.csv"

    class _Boom:
        def writerow(self, *_a):
            raise OSError("disk full")

    monkeypatch.setattr(fd.csv, "writer", lambda *_a, **_k: _Boom())
    with pytest.raises(OSError):
        fd._write_csv_atomic(out, ["1", "2"])
    # No leftover .tmp files, no partial output.
    assert list(tmp_path.glob("*.tmp")) == []
    assert not out.exists()


# ----------------------------------------------------------- date handling


def test_today_ist_validates_override():
    assert fd._today_ist_str("2026-05-30") == "2026-05-30"


def test_today_ist_rejects_malformed():
    with pytest.raises(ValueError):
        fd._today_ist_str("30-05-2026")
