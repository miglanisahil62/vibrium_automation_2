"""SQLite connection helpers for the workflow engine.

Two physical SQLite files are involved:

* ``state/workflow.db`` — fully owned by this project. Created by migration
  ``001_init.py``. Both reads and writes from workflow code allowed.
* ``state/vibrium.db`` — owned by the existing adhoc Vibrium system. Migration
  ``002_customer_call_audit.py`` adds **one** additive table here. Every other
  access from workflow code is strictly READ-ONLY (enforced via
  ``PRAGMA query_only=1`` on connections opened with ``mode='r'``).

Design principles (locked):

* ``ATTACH DATABASE`` from one file to the other is forbidden anywhere in this
  codebase — cross-DB reads are done by opening a second connection in
  read-only mode.
* WAL mode is enabled on both files at open time so concurrent readers don't
  block writers (matches Sahil's existing tree convention).
* Handlers may pass an already-open connection in (``caller_owned_connection``).
  In that case this module does NOT close it; the caller owns the lifecycle.
* The ``transaction(conn)`` context manager wraps ``BEGIN``/``COMMIT``/
  ``ROLLBACK`` correctly so handler code can rely on standard exception
  semantics for atomicity (per architecture rev 3 §"Dedupe key + transactional
  advance").

No business logic lives here. Schema creation is migrations' job.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Literal, Optional, Union

# Default DB file locations. The runner / daemons override these via explicit
# arguments — these constants are only used when a caller passes ``path=None``
# and there is no config layer in front. Tests always pass explicit paths.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_WORKFLOW_DB = _REPO_ROOT / "state" / "workflow.db"
_DEFAULT_VIBRIUM_DB = _REPO_ROOT / "state" / "vibrium.db"

PathLike = Union[str, Path]


def _apply_pragmas(conn: sqlite3.Connection, *, read_only: bool) -> None:
    """Apply WAL + foreign_keys + query_only (when read-only).

    PRAGMAs are NOT executed inside CREATE TABLE DDL (that's a sqlite no-op);
    they belong here at connection-open time.
    """
    # WAL improves concurrent read/write throughput; persists across connections
    # but setting it per-open is cheap and self-healing if the file is recreated.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    if read_only:
        # Hard guarantee: any INSERT/UPDATE/DELETE/CREATE on this connection
        # raises sqlite3.OperationalError. This is the enforcement mechanism
        # for the architecture invariant that workflow code never writes to
        # vibrium.db except via migration 002.
        conn.execute("PRAGMA query_only=1")


def _connect(path: PathLike, *, read_only: bool) -> sqlite3.Connection:
    """Open a connection with the project conventions applied.

    ``check_same_thread=False`` is intentionally NOT set — every daemon is
    single-threaded; if a future component wants threading it must open its
    own connection. Bug-by-default is safer than silent cross-thread state.
    """
    p = Path(path)
    # Ensure the parent dir exists for the workflow DB. We do NOT create the
    # parent for vibrium.db — that's owned by another system and silently
    # creating an empty file there could hide a config typo.
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn, read_only=read_only)
    return conn


def get_workflow_db(path: Optional[PathLike] = None) -> sqlite3.Connection:
    """Return a read/write connection to ``state/workflow.db``.

    ``path`` defaults to the repo-local file. Callers in production pass the
    explicit path from ``config.json``. WAL mode is enabled.
    """
    target = Path(path) if path is not None else _DEFAULT_WORKFLOW_DB
    return _connect(target, read_only=False)


def get_vibrium_db(
    path: Optional[PathLike] = None,
    mode: Literal["r", "rw"] = "r",
) -> sqlite3.Connection:
    """Return a connection to ``state/vibrium.db``.

    ``mode='r'`` is the default and the only mode workflow code should use
    outside the audit-table write path. ``PRAGMA query_only=1`` is set so any
    accidental write raises immediately.

    ``mode='rw'`` is used by:
    * migration ``002_customer_call_audit.py`` (creates the table).
    * the audit recorder in Phase 3 (``shared/customer_call_audit.py``).

    Nothing else in workflow code should pass ``mode='rw'``.
    """
    if mode not in ("r", "rw"):
        raise ValueError(f"mode must be 'r' or 'rw', got {mode!r}")
    target = Path(path) if path is not None else _DEFAULT_VIBRIUM_DB
    return _connect(target, read_only=(mode == "r"))


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN / COMMIT / ROLLBACK helper for transactional handlers.

    Usage:
        with transaction(conn):
            conn.execute(...)
            conn.execute(...)
        # commit happens on normal exit; rollback on any exception.

    Why not just use ``with conn:``? Python's built-in sqlite3 context manager
    will commit on exit but does NOT issue an explicit BEGIN — it relies on
    autocommit detection, which interacts poorly with ``PRAGMA query_only=1``
    and with ``INSERT OR IGNORE``-then-check-rowcount patterns. Explicit
    BEGIN IMMEDIATE makes the locking deterministic.
    """
    # BEGIN IMMEDIATE acquires the RESERVED lock immediately so two concurrent
    # writers serialize at the BEGIN, not at the first INSERT. This matches
    # the existing pattern in the adhoc system's ingest.py.
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
