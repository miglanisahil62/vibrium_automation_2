"""workflow_ingest — Phase 7.

Reads vibrium-tagged rows from Redshift ``sttash_website_live.collection_comment_data``,
parses dispositions, classifies them, and wakes the matching workflow run via
**time-bound triangulation** on ``(customer_id, fired_at_ist + 24h window)``.

Phase 0a pivot (docs/phase_0a_decision.md):
    The original architecture relied on a ``tag_group`` payload propagating
    through CleverTap → CRM → ``collection_comment_data``. Schema introspection
    showed that column does not exist; metadata is encoded as free-text inside
    the ``comment`` column. The disposition-wakeup join is therefore the
    primary triangulation: match each comment to the most-recent ``FIRED`` /
    ``FIRED_RECOVERED`` row in ``wf_pending_actions`` for that customer
    whose ``fired_at_ist`` falls within the 24h window ending at
    ``comment_create_date``. The Phase 6 workflow_scheduler enforces a 3h
    per-customer cooldown, making ambiguity vanishingly small.

Boundaries — what this module does NOT do:
    * Does not fire CT triggers (that is Phase 6 — workflow_scheduler).
    * Does not advance workflow state machine (that is Phase 5 — WorkflowAgent
      tick); we only flip ``ready_at_ist`` so the executor picks it up.
    * Does not modify the adhoc ingest.py — non-matching comments are simply
      ignored. The two ingest systems claim comments by consulting different
      reference tables (``pending_actions`` vs ``wf_pending_actions``).

Watermark:
    The highest processed ``collection_comment_data.id`` is persisted in
    workflow.db's ``schema_version`` table under key ``wf_ingest_watermark``
    (reusing the Phase 1 table to avoid a second migration just for state).
    The runner skips this key — it does not look like a migration identifier.

Fetcher injection:
    ``run()`` accepts an optional ``comment_fetcher`` callable. Tests pass a
    fixture-replay function; production passes ``None`` and the module wires
    up the Redshift connection via ``external/vibrium_automation_scripts/db.py``.

I/O discipline:
    Pure Python + SQLite for the wakeup join. The Redshift fetch is the only
    non-deterministic boundary; isolate it behind ``comment_fetcher`` so the
    test suite never hits Redshift.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

from workflow.wf_store import PathLike, get_workflow_db, transaction

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

log = logging.getLogger("workflow_ingest")

# Watermark key inside workflow.db.schema_version. Chosen so it cannot collide
# with a 3-digit migration identifier (which always start with NNN_).
_WATERMARK_KEY = "wf_ingest_watermark"

# Default call-window for decision_v2.classify(). The action_class is the only
# thing we consume from the Decision — these times only affect the (unused)
# scheduled_at_ist field.
_DEFAULT_WINDOW_START = time(8, 0)
_DEFAULT_WINDOW_END = time(19, 0)
_DEFAULT_AGREE_FIRE_AT = time(18, 30)


# --------------------------------------------------------------- data shapes


@dataclass
class IngestStats:
    """Per-run counters. ``to_dict()`` shape is what the CLI prints."""

    fetched: int = 0
    matched: int = 0
    waked: int = 0
    unmatched: int = 0
    skipped_non_vibrium: int = 0
    skipped_state_mismatch: int = 0
    errored: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "fetched": self.fetched,
            "matched": self.matched,
            "waked": self.waked,
            "unmatched": self.unmatched,
            "skipped_non_vibrium": self.skipped_non_vibrium,
            "skipped_state_mismatch": self.skipped_state_mismatch,
            "errored": self.errored,
        }


CommentRow = dict[str, Any]
CommentFetcher = Callable[[int, datetime], Iterable[CommentRow]]


# --------------------------------------------------------------- time helpers


def _ensure_ist_naive(value: Any) -> Optional[str]:
    """Normalize a timestamp to IST-naive ``YYYY-MM-DD HH:MM:SS``.

    Accepts:
        * naive ``datetime`` (assumed IST per Phase 6 contract);
        * aware ``datetime`` (converted to IST);
        * ISO-8601 string (with or without tz);
        * ``None`` → ``None``.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            log.warning("could not parse timestamp %r — treating as none", value)
            return None
    else:
        log.warning("unexpected timestamp type %s — treating as none", type(value))
        return None

    # Redshift ``create_date`` is conventionally UTC-naive. Treat naive as UTC
    # if the caller provided a hint, otherwise assume IST. Tests pass aware
    # datetimes explicitly — naive datetimes from Redshift go through
    # ``_redshift_to_ist`` which handles the UTC→IST conversion.
    if dt.tzinfo is None:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def _redshift_to_ist(value: Any) -> Optional[str]:
    """Convert Redshift ``create_date`` (UTC-naive) → IST-naive string.

    Redshift returns naive datetimes that semantically represent UTC. Phase 6
    writes ``fired_at_ist`` as IST-naive strings. To compare them we promote
    the Redshift naive UTC → aware UTC → IST → naive IST string.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            log.warning("unparseable create_date %r", value)
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    else:
        return None
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def _now_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------- watermark


def _get_watermark(conn) -> int:
    """Read the highest processed ``collection_comment_data.id``.

    Returns 0 if no watermark stored yet (cold start).
    """
    row = conn.execute(
        "SELECT v FROM schema_version WHERE k = ?", (_WATERMARK_KEY,)
    ).fetchone()
    if row is None:
        return 0
    return int(row["v"])


def _set_watermark(conn, value: int) -> None:
    """Persist the new high-water mark inside the caller's transaction."""
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (k, v, applied_at_ist) "
        "VALUES (?, ?, datetime('now'))",
        (_WATERMARK_KEY, int(value)),
    )


# --------------------------------------------------------------- triangulation


def _find_matching_pending_action(
    conn, *, customer_id: str, comment_create_ist: str
) -> Optional[dict]:
    """Triangulation join — Phase 0a primary path.

    Picks the most-recent ``FIRED`` / ``FIRED_RECOVERED`` row for this
    customer whose ``fired_at_ist`` is within 24 hours preceding the comment.

    Returns the row as a dict, or ``None`` if no candidate exists.
    """
    # 24h window: fired_at_ist must be in [comment - 24h, comment].
    # Both sides as IST-naive strings — SQLite lexicographic comparison is
    # correct for the ``YYYY-MM-DD HH:MM:SS`` format.
    window_start = (
        datetime.strptime(comment_create_ist, "%Y-%m-%d %H:%M:%S")
        - timedelta(hours=24)
    ).strftime("%Y-%m-%d %H:%M:%S")

    row = conn.execute(
        """
        SELECT id, run_id, node_id, attempt_count, fired_at_ist, customer_id
        FROM wf_pending_actions
        WHERE customer_id = ?
          AND status IN ('FIRED', 'FIRED_RECOVERED')
          AND fired_at_ist IS NOT NULL
          AND fired_at_ist <= ?
          AND fired_at_ist >= ?
        ORDER BY fired_at_ist DESC
        LIMIT 1
        """,
        (str(customer_id), comment_create_ist, window_start),
    ).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------- wake


def _insert_decision_log(
    conn,
    *,
    customer_id: str,
    comment_id: str,
    run_id: int,
    node_id: str,
    attempt_count: int,
    disposition: Optional[str],
    sub_disposition: Optional[str],
    action_class: str,
    comment_create_date: str,
    notes: str,
) -> None:
    """Append a wf_decision_log row."""
    conn.execute(
        """
        INSERT INTO wf_decision_log (
            ts_ist, comment_id, customer_id, run_id, node_id, attempt_count,
            disposition, sub_disposition, action_class, comment_create_date, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _now_ist_str(),
            str(comment_id) if comment_id else None,
            str(customer_id),
            int(run_id),
            str(node_id),
            int(attempt_count),
            disposition,
            sub_disposition,
            action_class,
            comment_create_date,
            notes,
        ),
    )


def _wake_run(
    conn,
    *,
    run_id: int,
    node_id: str,
    comment_create_ist: str,
    action_class: str,
) -> bool:
    """Wake the matched run, but only if its state is consistent.

    Guards (all must hold):
        * workflow_runs.id == run_id exists
        * workflow_runs.current_node_id == node_id  (still parked here)
        * workflow_runs.status == 'WAITING'
        * workflow_runs.entered_node_at_ist <= comment_create_ist  (no
          time-travelling — comment must not predate the node entry).

    Side-effects (one UPDATE):
        * ready_at_ist := now()
        * scratchpad_json gets ``last_disposition_action_class`` merged in
          (JSON object).

    Returns True iff the wakeup UPDATE was executed.
    """
    run = conn.execute(
        """
        SELECT id, current_node_id, status, entered_node_at_ist, scratchpad_json
        FROM workflow_runs
        WHERE id = ?
        """,
        (int(run_id),),
    ).fetchone()

    if run is None:
        log.info("wake skipped: run %d not found", run_id)
        return False
    if run["current_node_id"] != node_id:
        log.info(
            "wake skipped: state mismatch run=%d current_node=%r vs matched_node=%r",
            run_id, run["current_node_id"], node_id,
        )
        return False
    if run["status"] != "WAITING":
        log.info(
            "wake skipped: status=%r (need WAITING) run=%d",
            run["status"], run_id,
        )
        return False
    if run["entered_node_at_ist"] is None:
        log.info("wake skipped: entered_node_at_ist NULL run=%d", run_id)
        return False
    if run["entered_node_at_ist"] > comment_create_ist:
        # Stale disposition — predates the node entry. This is the primary
        # safety net per architecture rev 3 §1 ("lower bound for disposition
        # wakeup").
        log.info(
            "wake skipped: stale disposition run=%d entered_at=%s comment_at=%s",
            run_id, run["entered_node_at_ist"], comment_create_ist,
        )
        return False

    # Merge the action_class into scratchpad without losing other keys.
    try:
        scratchpad = json.loads(run["scratchpad_json"] or "{}")
        if not isinstance(scratchpad, dict):
            scratchpad = {}
    except (json.JSONDecodeError, TypeError):
        log.warning("bad scratchpad_json on run %d — resetting to {}", run_id)
        scratchpad = {}
    scratchpad["last_disposition_action_class"] = action_class

    conn.execute(
        """
        UPDATE workflow_runs
        SET ready_at_ist = datetime('now'),
            scratchpad_json = ?,
            updated_at_ist = datetime('now')
        WHERE id = ?
        """,
        (json.dumps(scratchpad), int(run_id)),
    )
    return True


# --------------------------------------------------------------- fetcher


def _redshift_comment_fetcher(
    watermark: int, since: datetime
) -> Iterable[CommentRow]:
    """Default fetcher — pulls from Redshift via the sibling repo's helper.

    Lazy import: keeps the test suite from needing Redshift credentials.
    """
    # Lazy import — the symlink may legitimately be absent in some test envs.
    from external.vibrium_automation_scripts.db import redshift, query  # type: ignore

    # Pin the lower bound to whichever is later: the persisted watermark or
    # ``since``. The watermark is the canonical guard against re-processing;
    # ``since`` exists so an operator can replay a window manually.
    since_dt = since if since.tzinfo else since.replace(tzinfo=IST)
    with redshift() as cn:
        df = query(
            cn,
            """
            SELECT id, customer_id, comment_id, create_date, comment
            FROM sttash_website_live.collection_comment_data
            WHERE id > %(wm)s
              AND create_date >= %(since)s
              AND comment ILIKE %(pat)s
              AND add_user_id IS NULL
            ORDER BY id ASC
            LIMIT 5000
            """,
            params={
                "wm": int(watermark),
                "since": since_dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S"),
                "pat": "%merchant_name : vibrium%",
            },
            rationale=(
                "workflow_ingest: pull new vibrium-tagged comments since "
                "watermark to triangulate against wf_pending_actions"
            ),
        )
    # Normalize to plain dicts so the rest of the pipeline doesn't depend on
    # pandas at runtime (tests use dicts directly).
    for _, row in df.iterrows():
        yield {
            "id": int(row["id"]),
            "customer_id": str(row["customer_id"]) if row["customer_id"] is not None else "",
            "comment_id": str(row["comment_id"] or ""),
            "create_date": row["create_date"],
            "comment": row["comment"],
        }


# --------------------------------------------------------------- main loop


def _classify_row(
    parsed: dict, create_date_ist_str: str
) -> tuple[str, str]:
    """Run decision_v2.classify(); return (action_class, notes).

    decision_v2 needs an aware IST datetime for ``create_date_ist`` — we
    reconstruct one from the IST-naive string. The scheduled_at_ist on the
    returned Decision is ignored; only ``action_class`` matters for the wake.
    """
    from external.vibrium_automation_scripts.decision_v2 import classify  # type: ignore

    create_dt = datetime.strptime(create_date_ist_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=IST
    )
    decision = classify(
        parsed,
        create_dt,
        _DEFAULT_WINDOW_START,
        _DEFAULT_WINDOW_END,
        _DEFAULT_AGREE_FIRE_AT,
    )
    return decision.action_class, decision.notes or ""


def _process_row(
    conn,
    *,
    row: CommentRow,
    stats: IngestStats,
) -> None:
    """Process one collection_comment_data row inside the caller's txn.

    Strategy:
        1. Parse the comment via the adhoc parser (handles VIBRIUM_TAG guard).
        2. Look up a matching wf_pending_actions row via triangulation.
        3. If matched → classify, insert wf_decision_log, conditionally wake.
        4. If unmatched → ignore (likely an adhoc-system fire).

    All exceptions other than the parser/classifier returning a clean result
    are surfaced via ``stats.errored`` and logged but do NOT abort the run —
    one bad row should not block the watermark advance for the others (the
    caller commits per-batch and a poison-pill row will recur, but the
    operator gets a heartbeat).
    """
    # Lazy import so the symlink absence in a CI scratch box doesn't break
    # module import (Phase 0 scripts/check_external_links.py guards prod).
    from external.vibrium_automation_scripts.parser import parse_comment  # type: ignore

    comment_create_ist = _redshift_to_ist(row.get("create_date"))
    if comment_create_ist is None:
        log.warning("row id=%s: missing/unparseable create_date — skipping", row.get("id"))
        stats.errored += 1
        return

    parsed = parse_comment(row.get("comment"))
    if not parsed.get("is_vibrium"):
        # Not a vibrium row — the ILIKE filter at fetch time means we
        # shouldn't usually see these, but the parser's check is authoritative.
        stats.skipped_non_vibrium += 1
        return

    customer_id = str(row.get("customer_id") or "")
    if not customer_id:
        log.warning("row id=%s: missing customer_id — skipping", row.get("id"))
        stats.errored += 1
        return

    match = _find_matching_pending_action(
        conn,
        customer_id=customer_id,
        comment_create_ist=comment_create_ist,
    )
    if match is None:
        # No workflow_scheduler fire in the 24h window → likely an adhoc
        # fire (or the customer was never enrolled in any workflow). Ignore
        # cleanly — the adhoc system's ingest.py owns this row.
        stats.unmatched += 1
        return

    stats.matched += 1

    action_class, notes = _classify_row(parsed, comment_create_ist)

    _insert_decision_log(
        conn,
        customer_id=customer_id,
        comment_id=str(row.get("comment_id") or ""),
        run_id=int(match["run_id"]),
        node_id=str(match["node_id"]),
        attempt_count=int(match["attempt_count"]),
        disposition=parsed.get("disposition"),
        sub_disposition=parsed.get("sub_disposition"),
        action_class=action_class,
        comment_create_date=comment_create_ist,
        notes=notes,
    )

    waked = _wake_run(
        conn,
        run_id=int(match["run_id"]),
        node_id=str(match["node_id"]),
        comment_create_ist=comment_create_ist,
        action_class=action_class,
    )
    if waked:
        stats.waked += 1
    else:
        stats.skipped_state_mismatch += 1


def run(
    workflow_db_path: PathLike,
    *,
    since: Optional[datetime] = None,
    dry_run: bool = False,
    comment_fetcher: Optional[CommentFetcher] = None,
    **_ignored: object,
) -> dict[str, int]:
    """Entrypoint — process new vibrium comments and wake matching runs.

    Signature matches the Phase 9 orchestrator's uniform ``run(**kwargs)``
    dispatch contract. The orchestrator passes ``workflow_db_path`` +
    ``dry_run``; everything else has a sensible default.

    Args:
        workflow_db_path: absolute path to ``state/workflow.db``.
        since: minimum ``create_date`` to consider on cold start (UTC or IST).
            The persisted watermark always takes precedence once set. Defaults
            to 24 hours ago if not supplied — adequate for steady-state cron
            invocations where the watermark has long since taken over.
        dry_run: reserved — currently inert (per-row inserts already gated by
            the IngestStats counters; no destructive side-effects to skip
            beyond the wake_at update which is the whole point of ingest).
            Accepted for orchestrator-uniform contract.
        comment_fetcher: testing hook. Defaults to the Redshift fetcher.
        **_ignored: orchestrator may thread other daemons' kwargs through
            generic plumbing; accept and discard rather than TypeError.

    Returns:
        A stats dict — see ``IngestStats.to_dict()``.
    """
    fetcher = comment_fetcher or _redshift_comment_fetcher
    stats = IngestStats()

    if since is None:
        # Fallback for orchestrator-driven invocations where no --since is
        # threaded in. 24h is well past the workflow's 3h cooldown window;
        # the persisted watermark filters out already-processed rows.
        since = datetime.now(IST) - timedelta(hours=24)

    conn = get_workflow_db(workflow_db_path)
    try:
        watermark = _get_watermark(conn)
        log.info(
            "watermark=%d since=%s dry_run=%s",
            watermark, since.isoformat(), dry_run,
        )

        rows = list(fetcher(watermark, since))
        stats.fetched = len(rows)
        log.info("fetched %d candidate rows", stats.fetched)
        if not rows:
            return stats.to_dict()

        max_id = watermark
        # One transaction per batch — keeps the watermark + wakeups atomic.
        with transaction(conn):
            for row in rows:
                try:
                    _process_row(conn, row=row, stats=stats)
                except Exception as exc:  # noqa: BLE001 — per-row isolation
                    log.exception(
                        "row id=%s: unhandled error: %s", row.get("id"), exc
                    )
                    stats.errored += 1
                max_id = max(max_id, int(row.get("id") or 0))
            _set_watermark(conn, max_id)

        log.info(
            "done. fetched=%d matched=%d waked=%d unmatched=%d max_id=%d",
            stats.fetched, stats.matched, stats.waked, stats.unmatched, max_id,
        )
    finally:
        conn.close()

    return stats.to_dict()


# --------------------------------------------------------------- CLI


def _parse_since(s: str) -> datetime:
    """Parse the ``--since`` CLI value (ISO-8601, with or without tz)."""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"bad --since {s!r}: {e}") from e
    if dt.tzinfo is None:
        # Treat naive CLI inputs as IST (matches the rest of the system).
        dt = dt.replace(tzinfo=IST)
    return dt


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint — `python3 -m workflow.workflow_ingest ...`."""
    ap = argparse.ArgumentParser(
        prog="workflow.workflow_ingest",
        description=(
            "Read new vibrium-tagged comments from Redshift, triangulate "
            "against wf_pending_actions, and wake the matching workflow run."
        ),
    )
    ap.add_argument(
        "--workflow-db",
        required=True,
        help="Absolute path to state/workflow.db.",
    )
    ap.add_argument(
        "--since",
        required=True,
        type=_parse_since,
        help="Minimum create_date (ISO-8601). Watermark wins once set.",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        result = run(
            workflow_db_path=args.workflow_db,
            since=args.since,
        )
    except Exception as e:  # noqa: BLE001 — top-level CLI
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
