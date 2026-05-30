"""Tests for ``shared/customer_call_audit.py``.

Every test runs against a tmp_path SQLite file — production
``state/vibrium.db`` is never touched.

Each test calls migration ``002_customer_call_audit.py:up`` against the
tmp_path file to materialise the schema, exactly as Phase 1 does in
production.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

# Migration import — uses the runner-style filename with a digit prefix, so
# the standard import path doesn't work. Load via importlib.
import importlib.util

# Make ``shared`` importable for tests run from repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared import customer_call_audit as cca  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")


def _load_migration(name: str):
    """Import a digit-prefixed migration module via importlib."""
    path = _REPO_ROOT / "workflow" / "migrations" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"migration_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _load_migration_002():
    return _load_migration("002_customer_call_audit")


@pytest.fixture
def vibrium_db(tmp_path: Path) -> Path:
    """Create a tmp_path ``vibrium.db`` with migration 002 applied.

    The migration creates ``customer_call_audit`` + ``idx_cca_customer_day``
    + a ``schema_version_vbwf`` marker row. We touch no production DB.
    """
    db_path = tmp_path / "vibrium.db"
    workflow_db_path = tmp_path / "workflow.db"
    # Migration 002 requires the file to exist before opening (wf_store does
    # not create the parent for vibrium.db by design — we create it explicitly).
    db_path.touch()
    # Run migration 001 first to create workflow.db + schema_version table.
    _load_migration("001_init").up(
        workflow_db_path=workflow_db_path, vibrium_db_path=db_path,
    )
    # Then 002 which inserts a marker row into schema_version on workflow.db
    # and creates customer_call_audit on vibrium.db.
    _load_migration_002().up(
        workflow_db_path=workflow_db_path, vibrium_db_path=db_path,
    )
    return db_path


def _ist_now_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# record_fire — basic insert
# ---------------------------------------------------------------------------
def test_record_fire_inserts_one_row_with_ist_naive_timestamp(vibrium_db: Path):
    """record_fire writes one row; fired_at_ist matches ``YYYY-MM-DD HH:MM:SS``
    (IST-naive — no tz suffix). This format is what the freshness monitor
    parses; drift here breaks the freshness monitor."""
    cca.record_fire(
        customer_id=123,
        source="adhoc",
        cohort_name="test_cohort",
        ct_response_status="success",
        vibrium_db_path=vibrium_db,
    )
    conn = sqlite3.connect(str(vibrium_db))
    try:
        rows = conn.execute(
            "SELECT customer_id, fired_at_ist, source, cohort_name, "
            "ct_response_status, run_id, ct_error FROM customer_call_audit"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1
    cid, fired_at_ist, source, cohort, ct_status, run_id, ct_err = rows[0]
    assert cid == "123"
    assert source == "adhoc"
    assert cohort == "test_cohort"
    assert ct_status == "success"
    assert run_id is None
    assert ct_err is None

    # Format: YYYY-MM-DD HH:MM:SS — no T, no offset, no microseconds.
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", fired_at_ist), (
        f"fired_at_ist {fired_at_ist!r} does not match the IST-naive "
        f"YYYY-MM-DD HH:MM:SS contract"
    )

    # The recorded date matches today's IST date (the only thing we can
    # assert without flaking around midnight — and a midnight-crossing
    # flake would be a real bug we'd want to see).
    today_ist = datetime.now(IST).strftime("%Y-%m-%d")
    assert fired_at_ist.startswith(today_ist)


# ---------------------------------------------------------------------------
# batch_count_today + customer_daily_cap — cap math
# ---------------------------------------------------------------------------
def test_cap_reached_at_3_fires_today(vibrium_db: Path):
    """2 adhoc + 1 workflow rows for the same customer today → cap=3 reached."""
    cid = 999001
    cca.record_fire(customer_id=cid, source="adhoc", vibrium_db_path=vibrium_db)
    cca.record_fire(customer_id=cid, source="adhoc", vibrium_db_path=vibrium_db)
    cca.record_fire(
        customer_id=cid, source="workflow", run_id=42, vibrium_db_path=vibrium_db
    )

    counts = cca.batch_count_today([cid], vibrium_db_path=vibrium_db)
    assert counts == {cid: 3}

    cap = cca.customer_daily_cap(cid, vibrium_db_path=vibrium_db, limit=3)
    assert cap.fire is False
    assert "3/3" in cap.reason
    assert "customer_call_audit" in cap.reason


def test_zero_fires_returns_zero_and_allows_fire(vibrium_db: Path):
    """Customer with zero history → batch_count_today returns 0 (NOT missing)
    and customer_daily_cap returns fire=True. The 'zero-as-explicit-zero,
    never missing' contract is what callers depend on."""
    cid = 999002
    counts = cca.batch_count_today([cid], vibrium_db_path=vibrium_db)
    assert counts == {cid: 0}

    cap = cca.customer_daily_cap(cid, vibrium_db_path=vibrium_db, limit=3)
    assert cap.fire is True
    assert "0/3" in cap.reason


def test_batch_count_today_mixes_known_and_unknown(vibrium_db: Path):
    """Mix of customers — some with fires, some without — returns a complete
    map. No customer is missing from the output."""
    cca.record_fire(customer_id=100, source="adhoc", vibrium_db_path=vibrium_db)
    cca.record_fire(customer_id=100, source="adhoc", vibrium_db_path=vibrium_db)
    cca.record_fire(customer_id=200, source="workflow", run_id=7, vibrium_db_path=vibrium_db)

    out = cca.batch_count_today([100, 200, 300], vibrium_db_path=vibrium_db)
    assert out == {100: 2, 200: 1, 300: 0}


# ---------------------------------------------------------------------------
# Source validation
# ---------------------------------------------------------------------------
def test_record_fire_rejects_bogus_source(vibrium_db: Path):
    """Only 'adhoc' / 'workflow' are accepted. Anything else raises
    AssertionError before the INSERT even attempts (the table's CHECK
    constraint would also reject, but the Python assert gives a cleaner
    stack to the caller)."""
    with pytest.raises(AssertionError, match="source must be 'adhoc' or 'workflow'"):
        cca.record_fire(customer_id=111, source="bogus", vibrium_db_path=vibrium_db)

    # Ensure nothing was written.
    conn = sqlite3.connect(str(vibrium_db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM customer_call_audit").fetchone()[0]
    finally:
        conn.close()
    assert n == 0


# ---------------------------------------------------------------------------
# Concurrency — WAL mode test
# ---------------------------------------------------------------------------
def test_concurrent_writers_all_land(vibrium_db: Path):
    """10 parallel writers via ThreadPoolExecutor → all 10 rows present.

    WAL mode + ``timeout=10s`` on each connection means concurrent writers
    serialise at the WAL lock rather than racing or erroring. If any writer
    drops a row (e.g., ``database is locked`` after the timeout), the count
    < 10 makes the bug visible.

    Each thread opens its own connection (record_fire does this internally)
    — that's the contract that lets this work under concurrency."""

    def fire(i: int) -> None:
        cca.record_fire(
            customer_id=500_000 + i,
            source="workflow",
            run_id=i,
            cohort_name=f"thread_{i}",
            vibrium_db_path=vibrium_db,
        )

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fire, i) for i in range(10)]
        # Surface any exception from a worker — re-raises here.
        for f in as_completed(futures):
            f.result()

    conn = sqlite3.connect(str(vibrium_db))
    try:
        rows = conn.execute(
            "SELECT customer_id FROM customer_call_audit "
            "WHERE customer_id LIKE '500%' ORDER BY customer_id"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 10
    # Every thread should have a row keyed by its unique cid.
    got = sorted(int(r[0]) for r in rows)
    assert got == [500_000 + i for i in range(10)]


# ---------------------------------------------------------------------------
# batch_last_fire_at — most-recent timestamp per customer
# ---------------------------------------------------------------------------
def test_batch_last_fire_at_returns_most_recent_per_customer(vibrium_db: Path):
    """Multiple fires per customer → batch_last_fire_at returns the latest
    fired_at_ist for each. Customer with no fires gets None."""
    # Insert two rows for cid 700 with explicit timestamps so we can pin the
    # MAX. record_fire uses now() so we go straight to SQL to set times.
    conn = sqlite3.connect(str(vibrium_db))
    try:
        with conn:
            conn.execute(
                "INSERT INTO customer_call_audit "
                "(customer_id, fired_at_ist, source) VALUES (?, ?, ?)",
                ("700", "2026-05-30 09:15:00", "adhoc"),
            )
            conn.execute(
                "INSERT INTO customer_call_audit "
                "(customer_id, fired_at_ist, source) VALUES (?, ?, ?)",
                ("700", "2026-05-30 14:40:00", "workflow"),
            )
            conn.execute(
                "INSERT INTO customer_call_audit "
                "(customer_id, fired_at_ist, source) VALUES (?, ?, ?)",
                ("701", "2026-05-30 11:00:00", "adhoc"),
            )
    finally:
        conn.close()

    out = cca.batch_last_fire_at([700, 701, 702], vibrium_db_path=vibrium_db)
    assert out[700] == "2026-05-30 14:40:00"
    assert out[701] == "2026-05-30 11:00:00"
    assert out[702] is None


def test_batch_count_today_empty_input_returns_empty_dict(vibrium_db: Path):
    """Empty input → empty dict, no DB query (early-return guard)."""
    out = cca.batch_count_today([], vibrium_db_path=vibrium_db)
    assert out == {}
    out2 = cca.batch_last_fire_at([], vibrium_db_path=vibrium_db)
    assert out2 == {}
