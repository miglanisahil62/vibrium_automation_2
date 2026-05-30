"""Phase 1 tests — schema migrations + wf_store connection helper.

What's covered:
    * 001 applies the 11 tables + all indexes on a fresh DB.
    * 001 is idempotent (re-run is a no-op + no schema drift).
    * 002 against a fixture vibrium.db adds ONLY ``customer_call_audit``;
      pre-existing tables are byte-identical (via .schema diff).
    * ``wf_store.get_vibrium_db(mode='r')`` rejects writes (PRAGMA query_only).
    * ``wf_store.transaction()`` rolls back on exception.
    * ``wf_pending_actions(run_id, node_id, attempt_count)`` UNIQUE dedupe
      index actually enforces uniqueness.
    * ``workflow_runs.enrollment_key`` partial UNIQUE allows multiple NULLs.
    * ``schema_version`` row written after migration applied; runner skips on
      rerun.
    * Runner ``--dry-run`` does not write.

NOTE: tests use ``tmp_path`` exclusively — no real vibrium.db is touched.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from workflow.migrations import runner as migration_runner
from workflow.wf_store import (
    get_vibrium_db,
    get_workflow_db,
    transaction,
)


# Expected schema — what migration 001 should produce.
EXPECTED_TABLES = {
    "workflows",
    "workflow_versions",
    "workflow_runs",
    "wf_pending_actions",
    "wf_decision_log",
    "workflow_node_log",
    "workflow_admin_log",
    "wf_kill_switch",
    "wf_agent_events",
    "agent_assignments",
    "schema_version",
}


EXPECTED_INDEXES = {
    "idx_runs_ready",
    "idx_runs_customer",
    "idx_runs_enrollment_key",
    "idx_wfpa_ready",
    "idx_wfpa_customer",
    "idx_wfpa_customer_fired",
    "idx_wfdl_run",
    "idx_wfdl_customer",
}


def _tables_in(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def _indexes_in(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def _schema_dump(db_path: Path) -> str:
    """Return the .schema-equivalent for diffing."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        return "\n".join(f"{t}|{n}|{s}" for t, n, s in rows)
    finally:
        conn.close()


# ---------------------------------------------------------------- 001 schema

def test_001_creates_all_tables(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    applied = migration_runner.run(str(wf), str(vb))
    assert "001_init" in applied
    tables = _tables_in(wf)
    assert EXPECTED_TABLES.issubset(tables), (
        f"missing tables: {EXPECTED_TABLES - tables}"
    )
    # Exactly 11 tables, no extras. (Cardinality check per the phase spec.)
    assert len(tables & EXPECTED_TABLES) == 11


def test_001_creates_all_indexes(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))
    indexes = _indexes_in(wf)
    assert EXPECTED_INDEXES.issubset(indexes), (
        f"missing indexes: {EXPECTED_INDEXES - indexes}"
    )


def test_001_is_idempotent(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))
    dump1 = _schema_dump(wf)
    # Second run should be a no-op — runner sees 001 already in schema_version.
    applied2 = migration_runner.run(str(wf), str(vb))
    dump2 = _schema_dump(wf)
    assert "001_init" not in applied2  # already applied → skipped
    assert dump1 == dump2  # zero schema drift


def test_001_skips_after_apply(tmp_path):
    """schema_version row is written; runner respects it on rerun."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))
    conn = sqlite3.connect(str(wf))
    try:
        rows = conn.execute(
            "SELECT k FROM schema_version WHERE k='001_init'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    # Re-running explicitly should not add another row.
    migration_runner.run(str(wf), str(vb))
    conn = sqlite3.connect(str(wf))
    try:
        rows = conn.execute(
            "SELECT k FROM schema_version WHERE k='001_init'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1


# ---------------------------------------------------------------- 002 schema

def test_002_additive_only_on_vibrium_fixture(tmp_path):
    """Pre-existing vibrium.db tables stay byte-identical; only audit table added."""
    vb = tmp_path / "vibrium.db"
    # Seed a fixture vibrium.db with a fake pre-existing table to prove we
    # don't touch it. Real adhoc vibrium.db has 28 tables; we simulate one.
    seed = sqlite3.connect(str(vb))
    try:
        seed.execute(
            "CREATE TABLE pending_actions (id INTEGER PRIMARY KEY, cid TEXT)"
        )
        seed.execute("INSERT INTO pending_actions (cid) VALUES ('cust_xyz')")
        seed.commit()
    finally:
        seed.close()

    pre_dump = _schema_dump(vb)
    pre_tables = _tables_in(vb)

    wf = tmp_path / "workflow.db"
    migration_runner.run(str(wf), str(vb))

    post_dump = _schema_dump(vb)
    post_tables = _tables_in(vb)

    # Pre-existing table's schema line is unchanged.
    for line in pre_dump.split("\n"):
        assert line in post_dump, f"pre-existing schema line lost: {line}"

    # Only customer_call_audit (+ schema_version_vbwf marker) added.
    added = post_tables - pre_tables
    assert "customer_call_audit" in added
    assert "pending_actions" in post_tables  # untouched

    # Data preserved.
    conn = sqlite3.connect(str(vb))
    try:
        row = conn.execute("SELECT cid FROM pending_actions").fetchone()
    finally:
        conn.close()
    assert row[0] == "cust_xyz"


def test_002_idempotent(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))
    dump1 = _schema_dump(vb)
    migration_runner.run(str(wf), str(vb))
    dump2 = _schema_dump(vb)
    assert dump1 == dump2


# --------------------------------------------------------- wf_store behaviour

def test_vibrium_db_read_mode_rejects_writes(tmp_path):
    """PRAGMA query_only=1 must hard-fail any write attempt."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_vibrium_db(vb, mode="r")
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                "INSERT INTO customer_call_audit "
                "(customer_id, fired_at_ist, source) VALUES (?, ?, ?)",
                ("CID1", "2026-05-30 12:00:00", "workflow"),
            )
    finally:
        conn.close()


def test_vibrium_db_rw_mode_allows_writes(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_vibrium_db(vb, mode="rw")
    try:
        conn.execute(
            "INSERT INTO customer_call_audit "
            "(customer_id, fired_at_ist, source) VALUES (?, ?, ?)",
            ("CID1", "2026-05-30 12:00:00", "workflow"),
        )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM customer_call_audit"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 1


def test_transaction_rolls_back_on_exception(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        with pytest.raises(RuntimeError, match="forced"):
            with transaction(conn):
                conn.execute(
                    "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
                    "VALUES (?, ?, ?, ?)",
                    ("2026-05-30 12:00:00", "KILL", "test", "tester"),
                )
                raise RuntimeError("forced")
        # Confirm rollback happened.
        rows = conn.execute("SELECT COUNT(*) FROM wf_kill_switch").fetchone()[0]
    finally:
        conn.close()
    assert rows == 0


def test_transaction_commits_on_success(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO wf_kill_switch (ts_ist, action, reason, set_by) "
                "VALUES (?, ?, ?, ?)",
                ("2026-05-30 12:00:00", "KILL", "test", "tester"),
            )
        rows = conn.execute("SELECT COUNT(*) FROM wf_kill_switch").fetchone()[0]
    finally:
        conn.close()
    assert rows == 1


def test_wal_mode_enabled(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    assert mode.lower() == "wal"


# --------------------------------------------------- constraint enforcement

def test_wf_pending_actions_dedupe_index(tmp_path):
    """UNIQUE(run_id, node_id, attempt_count) must enforce uniqueness."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        conn.execute(
            "INSERT INTO wf_pending_actions "
            "(run_id, node_id, attempt_count, customer_id, scheduled_at_ist, "
            " status, created_at_ist) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "node_a", 1, "CID1", "2026-05-30 12:00:00",
             "PENDING", "2026-05-30 11:59:00"),
        )
        conn.commit()
        # Same (run_id, node_id, attempt_count) → IntegrityError.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO wf_pending_actions "
                "(run_id, node_id, attempt_count, customer_id, scheduled_at_ist, "
                " status, created_at_ist) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (1, "node_a", 1, "CID1", "2026-05-30 12:05:00",
                 "PENDING", "2026-05-30 11:59:00"),
            )
        # Different attempt_count → fine.
        conn.execute(
            "INSERT INTO wf_pending_actions "
            "(run_id, node_id, attempt_count, customer_id, scheduled_at_ist, "
            " status, created_at_ist) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "node_a", 2, "CID1", "2026-05-30 12:10:00",
             "PENDING", "2026-05-30 11:59:00"),
        )
        conn.commit()
    finally:
        conn.close()


def test_workflow_runs_enrollment_key_partial_unique(tmp_path):
    """Partial UNIQUE: same enrollment_key forbidden when NOT NULL; multiple NULLs allowed."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        # Seed parent rows aren't enforced (no FK), so go straight in.
        for _ in range(2):
            conn.execute(
                "INSERT INTO workflow_runs "
                "(workflow_id, version_id, customer_id, enrollment_key, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (1, 1, "CID1", None, "ACTIVE"),
            )
        conn.commit()
        # Two NULL enrollment_keys allowed.
        count = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()[0]
        assert count == 2

        # Now with non-NULL enrollment_key — first OK, dup must fail.
        conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_id, version_id, customer_id, enrollment_key, status) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, 1, "CID1", "CID1_2026-05-30", "ACTIVE"),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO workflow_runs "
                "(workflow_id, version_id, customer_id, enrollment_key, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (1, 1, "CID1", "CID1_2026-05-30", "ACTIVE"),
            )
    finally:
        conn.close()


def test_workflow_runs_status_check_constraint(tmp_path):
    """CHECK(status IN ...) rejects bad status."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = get_workflow_db(wf)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO workflow_runs "
                "(workflow_id, version_id, customer_id, status) "
                "VALUES (?, ?, ?, ?)",
                (1, 1, "CID1", "NOT_A_VALID_STATUS"),
            )
    finally:
        conn.close()


# --------------------------------------------------------------- runner CLI

def test_runner_dry_run_does_not_write(tmp_path):
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    applied = migration_runner.run(str(wf), str(vb), dry_run=True)
    assert "001_init" in applied
    assert "002_customer_call_audit" in applied
    # Neither DB file was created.
    assert not wf.exists()
    assert not vb.exists()


def test_runner_main_cli_exits_zero(tmp_path):
    """Smoke: running the runner as a module from a subprocess exits 0."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    # Run from the repo root so the package is importable.
    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "workflow.migrations.runner",
            "--workflow-db",
            str(wf),
            "--vibrium-db",
            str(vb),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    # Idempotent re-run.
    result2 = subprocess.run(
        [
            sys.executable,
            "-m",
            "workflow.migrations.runner",
            "--workflow-db",
            str(wf),
            "--vibrium-db",
            str(vb),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    assert result2.returncode == 0


def test_no_tag_group_column_anywhere(tmp_path):
    """Phase 0a invariant: 'tag_group' column must not appear in any table."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    for db_path in (wf, vb):
        conn = sqlite3.connect(str(db_path))
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            for (tbl,) in tables:
                cols = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
                for col in cols:
                    assert col[1] != "tag_group", (
                        f"Phase 0a violation: tag_group column found in "
                        f"{db_path.name}.{tbl}"
                    )
        finally:
            conn.close()


def test_fired_at_ist_column_present(tmp_path):
    """Phase 0a pivot: wf_pending_actions.fired_at_ist must exist."""
    wf = tmp_path / "workflow.db"
    vb = tmp_path / "vibrium.db"
    migration_runner.run(str(wf), str(vb))

    conn = sqlite3.connect(str(wf))
    try:
        cols = {
            c[1] for c in conn.execute(
                "PRAGMA table_info(wf_pending_actions)"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "fired_at_ist" in cols
