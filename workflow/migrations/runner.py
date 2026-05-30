"""Migration runner — applies pending migrations in numbered order.

Discovery:
    Every sibling module matching ``NNN_*.py`` (where NNN is a 3-digit
    sequence) is treated as a migration. Each must expose a callable
    ``up(workflow_db_path, vibrium_db_path)`` and module-level constants
    ``SCHEMA_KEY`` (str) and ``SCHEMA_VERSION`` (int).

State:
    The ``schema_version`` table inside ``workflow.db`` is the single source
    of truth for "which migrations have been applied." Migration 001 creates
    that table; the runner depends on it.

    For migration 001 specifically, the table won't exist yet on first run —
    the runner handles this by treating "table missing" as "no migrations
    applied" and applying 001 unconditionally.

CLI:
    python3 -m workflow.migrations.runner \\
        --workflow-db state/workflow.db \\
        --vibrium-db state/vibrium.db \\
        [--dry-run]

    --dry-run prints what would run and exits 0 without writing.
"""
from __future__ import annotations

import argparse
import importlib
import re
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple

# Module-name shape — 3-digit prefix + underscore + identifier + .py.
_MIGRATION_RE = re.compile(r"^(\d{3})_([a-z][a-z0-9_]*)\.py$")


def _discover() -> List[str]:
    """Return migration module names sorted by NNN prefix.

    Looks in this package's directory. Excludes ``__init__.py`` and
    ``runner.py`` itself.
    """
    here = Path(__file__).resolve().parent
    names: List[Tuple[str, str]] = []
    for p in here.iterdir():
        if not p.is_file():
            continue
        m = _MIGRATION_RE.match(p.name)
        if not m:
            continue
        # Strip .py to get the module name.
        names.append((m.group(1), p.stem))
    names.sort(key=lambda x: x[0])
    return [n for _, n in names]


def _applied_keys(workflow_db_path: str) -> set[str]:
    """Read ``schema_version`` from workflow.db; return set of applied keys.

    Returns empty set if the DB or the table does not exist — first-run state.
    """
    if not Path(workflow_db_path).exists():
        return set()
    conn = sqlite3.connect(workflow_db_path)
    try:
        # Cheap probe: is the schema_version table present? sqlite_master is
        # always present on a non-empty DB, so this distinguishes "table not
        # created yet" (a documented pre-001 state) from "actual SQL error"
        # (which we want to propagate, not swallow per SF006).
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ).fetchone()
        if row is None:
            # Documented contract: empty applied-set means pre-001 / fresh DB.
            return set()
        rows = conn.execute("SELECT k FROM schema_version").fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def run(
    workflow_db_path: str,
    vibrium_db_path: str,
    *,
    dry_run: bool = False,
) -> List[str]:
    """Apply all pending migrations. Returns the list of migration keys applied.

    Idempotent: migrations already recorded in ``schema_version`` are skipped.
    On ``dry_run=True``, prints intended actions and returns the list that
    WOULD have been applied (no DB writes).
    """
    discovered = _discover()
    applied_before = _applied_keys(workflow_db_path)

    to_apply: List[str] = []
    for mod_name in discovered:
        # Import the module to read its SCHEMA_KEY (the recorded identity)
        # rather than parsing the filename. This way a future migration can
        # change file name without orphaning its schema_version row.
        full = f"workflow.migrations.{mod_name}"
        mod = importlib.import_module(full)
        key = getattr(mod, "SCHEMA_KEY", None)
        if key is None:
            raise RuntimeError(
                f"migration {mod_name!r} is missing SCHEMA_KEY constant"
            )
        if key in applied_before:
            continue
        to_apply.append(mod_name)

    if dry_run:
        if not to_apply:
            print("dry-run: no pending migrations")
        else:
            print(f"dry-run: would apply {len(to_apply)} migration(s):")
            for m in to_apply:
                print(f"  - {m}")
        return to_apply

    applied_now: List[str] = []
    for mod_name in to_apply:
        full = f"workflow.migrations.{mod_name}"
        mod = importlib.import_module(full)
        up_fn = getattr(mod, "up", None)
        if up_fn is None or not callable(up_fn):
            raise RuntimeError(
                f"migration {mod_name!r} is missing callable up(...)"
            )
        print(f"applying: {mod_name}")
        up_fn(
            workflow_db_path=workflow_db_path,
            vibrium_db_path=vibrium_db_path,
        )
        applied_now.append(mod_name)

    if not applied_now:
        print("no pending migrations")
    else:
        print(f"applied {len(applied_now)} migration(s)")
    return applied_now


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow.migrations.runner",
        description="Apply pending vibrium-workflow schema migrations.",
    )
    parser.add_argument(
        "--workflow-db",
        required=True,
        help="Absolute path to state/workflow.db.",
    )
    parser.add_argument(
        "--vibrium-db",
        required=True,
        help="Absolute path to state/vibrium.db (target of migration 002).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended actions and exit without writing.",
    )
    args = parser.parse_args(argv)

    try:
        run(
            workflow_db_path=args.workflow_db,
            vibrium_db_path=args.vibrium_db,
            dry_run=args.dry_run,
        )
    except Exception as e:  # noqa: BLE001 — top-level CLI
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
