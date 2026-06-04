"""CT profile PREFETCH + cache (WS1 of X-Bucket Vibrium).

Decouples the rate-limited CleverTap profile fetch from the time-critical
enrollment window. A dedicated `ct_prefetch` run mode fetches the day's
candidate profiles (off the critical path, generous timeout) into the
`ct_profile_cache` table in workflow.db; the enrollment poller reads that cache
(cache-first, with a live fallback for misses) instead of hammering CT live on
the time-critical path.

(The executor's FETCH_CT_PROPS still uses the now-robust, token-bucketed
`clevertap_profile.get_profile`; wiring it cache-first is a deferred follow-up
in the larger redesign — it is the lower-frequency caller and the live client is
no longer a 429-storm risk.)

Design (per the audited plan):
  - Cache table lives in workflow.db (migration 003) — never touches vibrium.db.
  - Keyed (customer_id, cohort_date); same-day TTL implicit in cohort_date.
  - RESUMABLE: re-runs fetch only the complement (missing + retryable `error`
    rows under a per-day attempt cap). `found`/`not_found` are terminal-for-day.
  - SINGLE-THREADED PERSIST: the fetch pool returns a map; all UPSERTs happen on
    the main thread (sqlite connections aren't shareable across threads).
  - Coverage is an ALERT, never an enrollment blocker — enrollment proceeds on
    whatever the cache holds (cache is an optimization). `resolve_for_enrollment`
    additionally live-fetches misses (default on) so a cold/partial cache never
    zeroes out a morning.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from workflow import clevertap_profile as ctp
from workflow.wf_store import PathLike, get_workflow_db, transaction

log = logging.getLogger("workflow.ct_prefetch")

_IST = ZoneInfo("Asia/Kolkata")
_REPO_ROOT = Path(__file__).resolve().parent.parent

# Tunables (env-overridable).
_MIN_COVERAGE = float(os.environ.get("CT_PREFETCH_MIN_COVERAGE", "0.80"))
_ERROR_RETRY_CAP = int(os.environ.get("CT_PREFETCH_ERROR_RETRY_CAP", "3"))
_PREFETCH_CONCURRENCY = int(os.environ.get("CT_FETCH_MAX_CONCURRENCY", "8"))


def _today_ist() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d")


def _now_ist_str() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


def _candidates_csv_path(cohort_date: str) -> Path:
    """Resolve the day's DPD candidate CSV — same dir the fetch script writes to
    and the enrollment poller reads from."""
    csv_dir = os.environ.get(
        "WF_ENROLLMENT_CSV_DIR", str(_REPO_ROOT / "state" / "enrollment")
    )
    return Path(csv_dir) / f"dpd1_candidates_{cohort_date}.csv"


def _load_candidate_ids(cohort_date: str) -> list[str]:
    """Read candidate customer_ids from the day's CSV (reuses the poller's
    parser so the contract stays identical)."""
    from workflow.enrollment_poller import _load_candidates  # local import avoids cycle
    path = _candidates_csv_path(cohort_date)
    return _load_candidates(path)


# --------------------------------------------------------------------------
# Cache reads
# --------------------------------------------------------------------------

def read_cached(
    workflow_db_path: PathLike,
    cohort_date: str,
    cids: list[str],
) -> dict[str, tuple[str, dict[str, Any] | None]]:
    """Return `{cid: (status, record)}` for the cids present in the cache for
    `cohort_date`. Missing cids are simply absent from the returned dict."""
    if not cids:
        return {}
    conn = get_workflow_db(workflow_db_path)
    out: dict[str, tuple[str, dict[str, Any] | None]] = {}
    try:
        # Read the whole day's rows (single param) and filter cids in Python —
        # avoids the SQLite 999-variable IN-clause limit entirely.
        want = set(str(c) for c in cids)
        try:
            rows = conn.execute(
                "SELECT customer_id, status, profile_json FROM ct_profile_cache "
                "WHERE cohort_date = ?",
                (cohort_date,),
            ).fetchall()
        except sqlite3.OperationalError as exc:
            # Documented contract: cache table absent (migration 003 not applied
            # / fresh DB) → treat as a full cache miss so callers fall back to a
            # live fetch. Logged (not silent) so an unexpectedly-missing table in
            # production is visible.
            log.warning("ct_profile_cache unavailable (%s) — full cache miss for %s",
                        exc, cohort_date)
            return {}
        for cid, status, pj in rows:
            cid = str(cid)
            if cid not in want:
                continue
            record = None
            if status == "found" and pj:
                try:
                    record = json.loads(pj)
                except (ValueError, TypeError):
                    status, record = "error", None
            out[cid] = (status, record)
    finally:
        conn.close()
    return out


# --------------------------------------------------------------------------
# Prefetch (the run mode)
# --------------------------------------------------------------------------

def _existing_status(
    conn,
    cohort_date: str,
) -> dict[str, tuple[str, int]]:
    """Return `{cid: (status, attempts)}` already cached for the day."""
    rows = conn.execute(
        "SELECT customer_id, status, attempts FROM ct_profile_cache WHERE cohort_date = ?",
        (cohort_date,),
    ).fetchall()
    return {str(c): (s, int(a)) for c, s, a in rows}


def _upsert(
    workflow_db_path: PathLike,
    cohort_date: str,
    fetched: dict[str, tuple[str, dict[str, Any] | None]],
) -> None:
    """Single-threaded UPSERT of fetch results into ct_profile_cache."""
    if not fetched:
        return
    now = _now_ist_str()
    conn = get_workflow_db(workflow_db_path)
    try:
        with transaction(conn):
            for cid, (status, record) in fetched.items():
                pj = json.dumps(record) if (status == "found" and record is not None) else None
                conn.execute(
                    """
                    INSERT INTO ct_profile_cache
                        (customer_id, cohort_date, status, attempts, profile_json, fetched_at_ist)
                    VALUES (?, ?, ?, 1, ?, ?)
                    ON CONFLICT(customer_id, cohort_date) DO UPDATE SET
                        status = excluded.status,
                        attempts = ct_profile_cache.attempts + 1,
                        profile_json = excluded.profile_json,
                        fetched_at_ist = excluded.fetched_at_ist
                    """,
                    (str(cid), cohort_date, status, pj, now),
                )
    finally:
        conn.close()


def prefetch(
    workflow_db_path: PathLike,
    *,
    dry_run: bool = False,
    cohort_date: Optional[str] = None,
    vibrium_db_path: PathLike | None = None,  # noqa: ARG001 — dispatch uniformity
) -> dict[str, Any]:
    """Fetch the day's candidate profiles into the cache (resumable).

    Signature matches the orchestrator dispatch contract:
    `fn(workflow_db_path=..., dry_run=..., **extra)`.

    Returns a stats dict (also stamped into the heartbeat summary). Exits the
    process non-zero (raises) ONLY when coverage < CT_PREFETCH_MIN_COVERAGE, as
    an operator signal — it never blocks enrollment (enrollment has its own
    cache-miss live fallback)."""
    cohort_date = cohort_date or _today_ist()
    try:
        cids = _load_candidate_ids(cohort_date)
    except FileNotFoundError as exc:
        log.error("prefetch: candidate CSV missing for %s: %s", cohort_date, exc)
        return {"cohort_date": cohort_date, "requested": 0, "error": "csv_missing",
                "coverage_pct": 0.0}
    except ValueError as exc:
        log.error("prefetch: candidate CSV malformed for %s: %s", cohort_date, exc)
        return {"cohort_date": cohort_date, "requested": 0, "error": "csv_malformed",
                "coverage_pct": 0.0}

    # Dedupe, preserve order.
    seen: set[str] = set()
    uniq: list[str] = []
    for c in cids:
        c = str(c).strip()
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)
    requested = len(uniq)
    if requested == 0:
        log.warning("prefetch: 0 candidates for %s", cohort_date)
        return {"cohort_date": cohort_date, "requested": 0, "coverage_pct": 0.0}

    conn = get_workflow_db(workflow_db_path)
    try:
        existing = _existing_status(conn, cohort_date)
    finally:
        conn.close()

    # Complement: fetch cids that are uncached, OR cached 'error' under the
    # per-day attempt cap. 'found'/'not_found' are terminal for the day.
    to_fetch: list[str] = []
    already_terminal = 0
    for cid in uniq:
        ent = existing.get(cid)
        if ent is None:
            to_fetch.append(cid)
        elif ent[0] == "error" and ent[1] < _ERROR_RETRY_CAP:
            to_fetch.append(cid)
        else:
            already_terminal += 1

    if dry_run:
        log.info("[dry-run] prefetch %s: requested=%d already=%d to_fetch=%d",
                 cohort_date, requested, already_terminal, len(to_fetch))
        return {"cohort_date": cohort_date, "requested": requested,
                "already_cached": already_terminal, "to_fetch": len(to_fetch),
                "dry_run": True}

    fetched: dict[str, tuple[str, dict[str, Any] | None]] = {}
    if to_fetch:
        log.info("prefetch %s: fetching %d/%d (already=%d)",
                 cohort_date, len(to_fetch), requested, already_terminal)
        fetched = ctp.bulk_fetch_status(to_fetch, concurrency=_PREFETCH_CONCURRENCY)
        _upsert(workflow_db_path, cohort_date, fetched)

    # Recompute coverage over the full requested set.
    conn = get_workflow_db(workflow_db_path)
    try:
        final = _existing_status(conn, cohort_date)
    finally:
        conn.close()
    found = sum(1 for cid in uniq if final.get(cid, ("", 0))[0] == "found")
    not_found = sum(1 for cid in uniq if final.get(cid, ("", 0))[0] == "not_found")
    errors = sum(1 for cid in uniq if final.get(cid, ("", 0))[0] == "error")
    coverage = (found + not_found) / requested if requested else 0.0

    stats = {
        "cohort_date": cohort_date,
        "requested": requested,
        "already_cached": already_terminal,
        "fetched_this_run": len(fetched),
        "found": found,
        "not_found": not_found,
        "errors": errors,
        "coverage_pct": round(coverage, 4),
    }
    log.info("prefetch %s done: %s", cohort_date, stats)

    if coverage < _MIN_COVERAGE:
        # Operator signal — NOT an enrollment blocker (enrollment has live
        # fallback). Raising makes the run-wrapper stamp a 'down' heartbeat.
        raise RuntimeError(
            f"prefetch coverage {coverage:.2%} < {_MIN_COVERAGE:.0%} "
            f"(found={found} not_found={not_found} errors={errors} requested={requested})"
        )
    return stats


# Orchestrator entry point name expected by the registry ("prefetch").
run = prefetch
