"""Shared customer_call_audit library.

Single source of truth for the per-customer daily VB-call cap across both
schedulers (adhoc and workflow). Lives in ``state/vibrium.db`` so the existing
adhoc system can keep ``vibrium.db`` as its canonical state file.

Schema (created by migration ``workflow/migrations/002_customer_call_audit.py``):

    customer_call_audit (
        id, customer_id, fired_at_ist, source CHECK IN ('adhoc','workflow'),
        run_id, cohort_name, ct_response_status, ct_error
    )
    idx_cca_customer_day(customer_id, fired_at_ist)

All public functions in this module:

* Use stdlib ``sqlite3`` only (NO SQLAlchemy — per Phase 3 spec).
* Open a fresh connection per call. The audit DB is on a hot enforcement path
  but per-call open is cheap on a WAL-mode SQLite file and avoids cross-thread
  / cross-process connection sharing gotchas.
* Use IST-naive ``YYYY-MM-DD HH:MM:SS`` timestamps (matches
  ``project_vibrium_event_ts_format_drift`` — the freshness monitor parses
  this format and only this format).
* Treat the table as append-only. No UPDATE / DELETE entry points.

The cap reader functions (``batch_count_today``, ``customer_daily_cap``) join
on a DATE comparison against today's IST date. The index
``idx_cca_customer_day`` makes the WHERE clause sargable.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
MAX_CALLS_PER_DAY_DEFAULT = 3

PathLike = Union[str, Path]


@dataclass
class GateResult:
    """Matches ``vibrium-automation/scripts/pre_call_gate.GateResult`` shape so
    callers can use either module interchangeably on the cap-check path."""

    fire: bool
    reason: str


def _now_ist_str() -> str:
    """Return current IST as ``YYYY-MM-DD HH:MM:SS`` (naive, no offset)."""
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _today_ist_date() -> str:
    """Return today's IST date as ``YYYY-MM-DD``."""
    return datetime.now(IST).strftime("%Y-%m-%d")


def _open(vibrium_db_path: PathLike, *, write: bool) -> sqlite3.Connection:
    """Open a fresh sqlite3 connection on ``vibrium.db``.

    WAL is enabled at connection-open time (it persists in the file header but
    setting it per-open is a cheap self-heal if the file was recreated).
    ``timeout`` is set to 10s so concurrent writers wait on the WAL lock
    instead of immediately raising ``database is locked``.
    """
    p = Path(vibrium_db_path)
    if not p.exists():
        raise FileNotFoundError(
            f"customer_call_audit: vibrium.db not found at {p}. "
            f"Run migration workflow/migrations/002_customer_call_audit.py first."
        )
    conn = sqlite3.connect(str(p), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    if not write:
        conn.execute("PRAGMA query_only=1")
    return conn


# ---------------------------------------------------------------------------
# Writer: record_fire
# ---------------------------------------------------------------------------
def record_fire(
    customer_id: int,
    source: str,
    *,
    run_id: Optional[int] = None,
    cohort_name: Optional[str] = None,
    ct_response_status: Optional[str] = None,
    ct_error: Optional[str] = None,
    vibrium_db_path: PathLike,
) -> None:
    """Insert one audit row.

    Called by:
      * adhoc ``scheduler.py`` immediately after a successful CT trigger
        (``source='adhoc'``).
      * workflow ``workflow_scheduler.py`` immediately after a successful CT
        trigger (``source='workflow'`` + populated ``run_id``).

    Hard constraints:
      * ``source`` MUST be one of ``'adhoc'`` / ``'workflow'`` — assertion at
        function entry. The CHECK constraint on the table would also reject
        but raising in Python gives the caller a clearer stack.
      * ``customer_id`` is stored as TEXT (matches the schema) — caller can
        pass int or str; we coerce to str.
      * ``fired_at_ist`` is ALWAYS set by this function from IST clock; the
        caller cannot override (prevents clock drift / spoofing).

    No return value. Caller relies on exception propagation if the insert
    fails (e.g., DB locked beyond timeout). The adhoc scheduler wraps the
    call in its own try/except so a transient audit failure doesn't kill a
    tick (with a metric for visibility).
    """
    assert source in ("adhoc", "workflow"), (
        f"record_fire: source must be 'adhoc' or 'workflow', got {source!r}"
    )

    fired_at = _now_ist_str()
    conn = _open(vibrium_db_path, write=True)
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO customer_call_audit
                    (customer_id, fired_at_ist, source, run_id, cohort_name,
                     ct_response_status, ct_error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(customer_id),
                    fired_at,
                    source,
                    run_id,
                    cohort_name,
                    ct_response_status,
                    ct_error,
                ),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Readers: batch_count_today, batch_last_fire_at, customer_daily_cap
# ---------------------------------------------------------------------------
def _ids_to_str_tuple(customer_ids: list[int]) -> tuple[str, ...]:
    """Coerce caller-supplied IDs to a tuple of stringified IDs.

    The audit table stores customer_id as TEXT (matches the adhoc-system
    convention). Comparing TEXT to int via sqlite3 silently affinity-coerces
    in many cases but not all — explicit str-tuple removes the ambiguity.
    """
    return tuple(str(c) for c in customer_ids)


def batch_count_today(
    customer_ids: list[int],
    *,
    vibrium_db_path: PathLike,
) -> dict[int, int]:
    """Return ``{customer_id: count_of_fires_today}`` for every input id.

    ONE query, regardless of input size (uses ``WHERE customer_id IN (...)``).
    Customers in the input with zero fires today are present in the output
    map with value 0 — caller never has to ``.get(cid, 0)``.

    The key in the returned dict is the customer_id coerced to the same type
    as the input (we return ``int(cid)`` for the integer-typed input contract
    documented in the function signature).

    Today is defined as ``DATE(fired_at_ist) = DATE('now','localtime')`` —
    BUT sqlite3's ``localtime`` is process-local, which on AWS is UTC and on
    Mac dev is IST. To make the query host-agnostic, we compute today's IST
    date in Python and pass it as a bound parameter.
    """
    if not customer_ids:
        return {}

    ids_str = _ids_to_str_tuple(customer_ids)
    today = _today_ist_date()

    # Build placeholders dynamically — sqlite3 does NOT support tuple
    # parameters for IN clauses, so we expand here.
    placeholders = ",".join("?" * len(ids_str))
    sql = f"""
        SELECT customer_id, COUNT(*) AS n
        FROM customer_call_audit
        WHERE customer_id IN ({placeholders})
          AND substr(fired_at_ist, 1, 10) = ?
        GROUP BY customer_id
    """

    conn = _open(vibrium_db_path, write=False)
    try:
        rows = conn.execute(sql, (*ids_str, today)).fetchall()
    finally:
        conn.close()

    counts_by_str: dict[str, int] = {r["customer_id"]: int(r["n"]) for r in rows}
    # Restore output keys to int (matches the input contract — list[int]).
    out: dict[int, int] = {}
    for cid in customer_ids:
        out[int(cid)] = counts_by_str.get(str(cid), 0)
    return out


def count_fires_since(
    cutoff_ist: str,
    *,
    vibrium_db_path: PathLike,
) -> int:
    """Count ALL VB calls fired at/after ``cutoff_ist`` across BOTH sources.

    This is the cross-system rolling-window count: ``source='adhoc'`` and
    ``source='workflow'`` rows are counted together, because both drive the
    same bot vendor and a per-pipeline count would under-report the true
    combined call rate.

    ``cutoff_ist`` must be naive IST ``YYYY-MM-DD HH:MM:SS`` (the format every
    ``fired_at_ist`` uses). String ``>=`` comparison is order-preserving on
    that zero-padded format, so it is a correct time-window filter.
    """
    conn = _open(vibrium_db_path, write=False)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM customer_call_audit WHERE fired_at_ist >= ?",
            (cutoff_ist,),
        ).fetchone()
    finally:
        conn.close()
    return int(row["n"] or 0) if row else 0


def batch_last_fire_at(
    customer_ids: list[int],
    *,
    vibrium_db_path: PathLike,
) -> dict[int, Optional[str]]:
    """Return ``{customer_id: most_recent_fired_at_ist_or_None}``.

    Used by the batched cooldown check. ISO timestamp string is returned
    verbatim; caller parses it as needed. Customers with zero fires get
    ``None``.

    ONE query. Uses ``MAX(fired_at_ist)`` which is correct because the
    timestamps are stored as ISO-style strings (lexical sort matches temporal
    sort for that format).
    """
    if not customer_ids:
        return {}

    ids_str = _ids_to_str_tuple(customer_ids)
    placeholders = ",".join("?" * len(ids_str))
    sql = f"""
        SELECT customer_id, MAX(fired_at_ist) AS last_fire
        FROM customer_call_audit
        WHERE customer_id IN ({placeholders})
        GROUP BY customer_id
    """

    conn = _open(vibrium_db_path, write=False)
    try:
        rows = conn.execute(sql, ids_str).fetchall()
    finally:
        conn.close()

    last_by_str: dict[str, str] = {r["customer_id"]: r["last_fire"] for r in rows}
    out: dict[int, Optional[str]] = {}
    for cid in customer_ids:
        out[int(cid)] = last_by_str.get(str(cid))
    return out


def customer_daily_cap(
    customer_id: int,
    *,
    vibrium_db_path: PathLike,
    limit: int = MAX_CALLS_PER_DAY_DEFAULT,
) -> GateResult:
    """Single-customer convenience: returns a ``GateResult`` for the cap.

    ``fire=False`` when today's count >= ``limit``; else ``fire=True``. The
    cooldown rule (3h since last fire) is NOT enforced here — that lives in
    the existing ``pre_call_gate.check_cap_and_cooldown`` path. This function
    is purely the daily-cap check.

    Implemented as a 1-element call to ``batch_count_today`` so the SQL path
    is shared.
    """
    counts = batch_count_today([int(customer_id)], vibrium_db_path=vibrium_db_path)
    n = counts[int(customer_id)]
    if n >= limit:
        return GateResult(
            fire=False,
            reason=f"daily cap reached ({n}/{limit} via customer_call_audit)",
        )
    return GateResult(
        fire=True,
        reason=f"under cap ({n}/{limit} via customer_call_audit)",
    )
