"""workflow_scheduler — Phase 6.

Fires ``wf_pending_actions`` rows by:

1. Atomically claiming a PENDING row (race-safe via
   ``UPDATE WHERE status='PENDING'`` + rowcount=1 winner check).
2. Composing the pre-call gate over the customer:
   * RBI 08:00–19:00 IST window — ``pre_call_gate.is_callable_now()``
     (also evaluated once at top of ``run()`` for short-circuit).
   * Cross-system daily cap (3/day) — ``customer_call_audit.customer_daily_cap()``
     reads ``vibrium.db.customer_call_audit`` and counts adhoc + workflow
     fires together. This is the canonical per-customer cap (Phase 3).
   * 3h cooldown since most-recent fire ANY source — read via
     ``customer_call_audit.batch_last_fire_at()`` and threshold-compared.
   * Customer overdue + paid_today gate via ``pre_call_gate.check()``
     (Redshift round-trip; lives behind a fetcher injection so tests stay
     offline).
3. Branching on mode:
   * ``shadow_mode=True``: mark ``SHADOW_FIRED`` + set ``fired_at_ist`` so
     the Phase 7 triangulation join can still match in test runs. Does NOT
     call CT. Does NOT record audit.
   * ``dry_run=True``: log intent; do NOT update status, do NOT call CT, do
     NOT record audit. The PENDING row is released back to PENDING.
   * Live: call ``clevertap_trigger.trigger()`` (without ``tag_group`` —
     Phase 0a invariant: that column does not exist in
     ``collection_comment_data``; disposition wakeup is time-bound
     triangulation instead). Mark FIRED + ``fired_at_ist`` + record audit.
4. CT delivery failures / network errors mark ERROR with ``last_error``
   populated. The row is NOT lost; an operator can decide to retry.

Imports + library boundaries (locked):

* ``from external.vibrium_automation_scripts.pre_call_gate import …`` —
  RBI window + check() (Redshift round-trip). Same library the adhoc
  scheduler uses; we never duplicate the rules.
* ``from external.vibrium_automation_scripts.clevertap_trigger import trigger`` —
  same HTTP wrapper the adhoc scheduler uses (pooled session, FD-leak-safe).
  Called WITHOUT ``tag_group`` kwarg per Phase 0a.
* ``from shared.customer_call_audit import …`` — single source of truth for
  the per-customer daily cap + last-fire timestamp across both systems.
* ``from workflow.wf_store import …`` — SQLite connection helpers.

Imports we intentionally avoid:

* No ``import ingest`` / ``decision_v2`` / ``cohort_runner`` from
  vibrium-automation — those are adhoc-system control paths.
* No direct ``requests`` to CleverTap — only via ``clevertap_trigger``.

Exit codes:

* 0 — tick completed normally (including the kill-switch / outside-window
  short-circuits, which are observable in heartbeat).
* non-zero — top-level unhandled exception (fatal to the daemon).
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from shared import customer_call_audit as cca
from workflow.wf_store import PathLike, get_workflow_db

IST = ZoneInfo("Asia/Kolkata")

log = logging.getLogger("workflow_scheduler")

# How many wf_pending_actions rows we attempt per tick. Matches the architecture
# doc's TICK_BATCH_LIMIT for the executor; the scheduler uses the same default
# so the call volume per tick is bounded.
DEFAULT_BATCH_LIMIT = 100

# Cooldown between fires for a single customer (any source). Mirrors the adhoc
# COOLDOWN_HOURS=3 — keeping them equal makes the Phase 7 time-bound
# triangulation unambiguous (at most one fire per customer per 3h across
# both systems).
COOLDOWN_HOURS = 3

# Daily cap (matches MAX_CALLS_PER_DAY in pre_call_gate). Stored here as a
# module constant so the test suite can monkeypatch it down to 1 without
# touching shared/customer_call_audit (which has its own default).
MAX_CALLS_PER_DAY = 3

# Default CT externaltrigger args for workflow-driven fires. These are the
# Vibrium production values; overrideable via the run() kwargs so future
# workflows can target different campaigns / bots without code changes.
_DEFAULT_CAMPAIGN_ID = 1779376735
_DEFAULT_BOT_ID = "ai_vb_calling_default"
_DEFAULT_CONTACT_TYPE = "voice"


# ---------------------------------------------------------------------- types


def _now_ist_str() -> str:
    """Return current IST as ``YYYY-MM-DD HH:MM:SS`` (naive, no offset).

    Matches the format every other timestamp column in workflow.db /
    vibrium.db / customer_call_audit uses (see
    ``project_vibrium_event_ts_format_drift``: the freshness monitor parses
    this and only this format).
    """
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _parse_ist(ts: str) -> Optional[datetime]:
    """Parse an IST-naive ``YYYY-MM-DD HH:MM:SS`` back to an aware datetime.

    Returns None on any parse failure so callers can treat malformed values
    as "no recent fire" rather than crashing the tick. This is a documented
    contract: ``customer_call_audit.fired_at_ist`` is freshly written by our
    own code in the canonical format, so a parse failure indicates either
    schema drift or hand-edited data — both warrant a log line but not a
    crash that takes down the whole tick.
    """
    if not ts:
        return None
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError) as e:
        log.warning(
            "workflow_scheduler: _parse_ist failed for ts=%r: %s — "
            "treating as 'no recent fire'",
            ts,
            e,
        )
        return None
    return dt.replace(tzinfo=IST)


# ------------------------------------------------------- kill-switch + window


def _kill_switch_active(conn: sqlite3.Connection) -> bool:
    """Return True if the most recent ``wf_kill_switch`` row is KILL.

    Returns False when the table is empty (default = running). Matches the
    architecture doc's "latest KILL → no-op tick" semantics.
    """
    row = conn.execute(
        "SELECT action FROM wf_kill_switch ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return False
    return str(row["action"]).upper() == "KILL"


def _emit_heartbeat(conn: sqlite3.Connection, status: str, summary: dict) -> None:
    """Append one ``wf_agent_events`` row.

    Heartbeat row is the single observable side-effect of every tick — even
    a no-op (kill-switch / outside-window) emits one so the pipeline-integrity
    auditor never sees a silent gap.
    """
    try:
        conn.execute(
            "INSERT INTO wf_agent_events (ts_ist, agent, status, summary_json) "
            "VALUES (?, ?, ?, ?)",
            (_now_ist_str(), "workflow_scheduler", status, json.dumps(summary)),
        )
        conn.commit()
    except sqlite3.Error as e:
        # Heartbeat failures are non-fatal — log and continue. The daemon's
        # exit code is the durable signal; missing one heartbeat row is
        # cosmetic.
        log.warning("workflow_scheduler: heartbeat write failed: %s", e)


# --------------------------------------------------------- gate composition


def _gate_check(
    customer_id: int,
    *,
    vibrium_db_path: PathLike,
    gate_check_fn: Callable[[int], Any],
    last_fire_lookup: dict[int, Optional[str]],
    daily_cap_limit: int,
    cooldown_hours: int,
    now_ist: datetime,
) -> tuple[bool, str]:
    """Compose the per-customer gate.

    Order is deliberate:

    1. Daily cap (cheap SQLite read on local vibrium.db; always run).
    2. 3h cooldown vs ``customer_call_audit.batch_last_fire_at()`` (already
       fetched in bulk for the whole batch).
    3. ``pre_call_gate.check(customer_id)`` — RBI window (we already verified
       at tick start) + collection_view + paid_today (Redshift round-trip).

    The Redshift hop is last so we short-circuit cheap checks first.

    ``gate_check_fn`` is the injection seam: production passes
    ``pre_call_gate.check``; tests pass a mock so no Redshift is touched.
    Returns ``(fire, reason)``.
    """
    cap = cca.customer_daily_cap(
        int(customer_id),
        vibrium_db_path=vibrium_db_path,
        limit=daily_cap_limit,
    )
    if not cap.fire:
        return False, cap.reason

    last_fire_ts = last_fire_lookup.get(int(customer_id))
    if last_fire_ts:
        last_fire = _parse_ist(last_fire_ts)
        if last_fire is not None:
            elapsed = now_ist - last_fire
            if elapsed < timedelta(hours=cooldown_hours):
                return (
                    False,
                    f"cooldown — last fire at {last_fire_ts} "
                    f"(<{cooldown_hours}h ago, elapsed={elapsed})",
                )

    # Redshift-backed check — RBI window + collection_view + paid_today.
    # Tests inject a stub here so the suite stays hermetic.
    result = gate_check_fn(int(customer_id))
    return bool(result.fire), str(result.reason)


# ------------------------------------------------------------- claim + update


def _claim_row(conn: sqlite3.Connection, row_id: int) -> bool:
    """Atomically transition status=PENDING → FIRING_IN_PROGRESS.

    Returns True if we won the race (rowcount=1), False otherwise.
    No-op when the row is already claimed by another tick — the loser
    simply skips it.

    Implementation note: a single UPDATE with status='PENDING' in the
    WHERE clause is sufficient for race-safety because SQLite serializes
    writes via the RESERVED→PENDING→EXCLUSIVE lock chain; both ticks
    cannot succeed.
    """
    cur = conn.execute(
        "UPDATE wf_pending_actions "
        "SET status='FIRING_IN_PROGRESS', last_attempt_at_ist=? "
        "WHERE id=? AND status='PENDING'",
        (_now_ist_str(), row_id),
    )
    conn.commit()
    return cur.rowcount == 1


_ALLOWED_STATUSES = frozenset(
    {"PENDING", "FIRING_IN_PROGRESS", "FIRED", "SUPPRESSED", "ERROR", "SHADOW_FIRED"}
)


def _mark_status(
    conn: sqlite3.Connection,
    row_id: int,
    *,
    status: str,
    fired_at_ist: Optional[str] = None,
    last_error: Optional[str] = None,
    increment_attempts: bool = True,
) -> None:
    """Update a wf_pending_actions row's terminal status.

    Uses a fixed-shape UPDATE statement (no dynamic SQL string-build) — each
    column is bound positionally with ``?`` placeholders and the SQL text is
    the same on every call. ``COALESCE(?, fired_at_ist)`` lets the caller
    pass ``None`` to mean "do not change this column". ``status`` is
    validated against a closed allow-list before reaching the bind step,
    backstopping the CHECK constraint already in the schema.

    ``increment_attempts`` is True by default — every status transition
    counts as an attempt for operator visibility (SUPPRESSED-by-gate
    included).
    """
    if status not in _ALLOWED_STATUSES:
        raise ValueError(
            f"_mark_status: status must be one of {sorted(_ALLOWED_STATUSES)}, "
            f"got {status!r}"
        )
    attempts_delta = 1 if increment_attempts else 0
    conn.execute(
        """
        UPDATE wf_pending_actions
           SET status = ?,
               fired_at_ist = COALESCE(?, fired_at_ist),
               last_error = COALESCE(?, last_error),
               attempts = attempts + ?,
               last_attempt_at_ist = ?
         WHERE id = ?
        """,
        (
            status,
            fired_at_ist,
            last_error,
            attempts_delta,
            _now_ist_str(),
            row_id,
        ),
    )
    conn.commit()


def _release_pending(conn: sqlite3.Connection, row_id: int) -> None:
    """Revert a FIRING_IN_PROGRESS row to PENDING.

    Used only by the dry-run path: we claim the row to evaluate the gate
    against it, then put it back so the next non-dry-run tick can fire it.
    """
    conn.execute(
        "UPDATE wf_pending_actions SET status='PENDING' WHERE id=?",
        (row_id,),
    )
    conn.commit()


# ------------------------------------------------------------------- runner


def _default_trigger():
    """Lazy-import ``clevertap_trigger.trigger``.

    Lazy so tests that monkeypatch via the ``trigger_fn`` injection seam
    don't have to satisfy the import at module-load time (the external
    symlink may not be present in CI). Production always uses the real one.
    """
    from external.vibrium_automation_scripts.clevertap_trigger import trigger
    return trigger


def _default_gate_check():
    """Lazy-import ``pre_call_gate.check``."""
    from external.vibrium_automation_scripts.pre_call_gate import check
    return check


def _default_is_callable_now():
    """Lazy-import ``pre_call_gate.is_callable_now``."""
    from external.vibrium_automation_scripts.pre_call_gate import is_callable_now
    return is_callable_now


def run(
    *,
    workflow_db_path: PathLike,
    vibrium_db_path: PathLike,
    ct_creds_path: Optional[PathLike] = None,
    shadow_mode: bool = False,
    dry_run: bool = False,
    batch_limit: int = DEFAULT_BATCH_LIMIT,
    campaign_id: int = _DEFAULT_CAMPAIGN_ID,
    bot_id: str = _DEFAULT_BOT_ID,
    contact_type: str = _DEFAULT_CONTACT_TYPE,
    # Injection seams for the test suite. Production callers leave these None.
    trigger_fn: Optional[Callable[..., dict]] = None,
    gate_check_fn: Optional[Callable[[int], Any]] = None,
    is_callable_now_fn: Optional[Callable[[], Any]] = None,
    record_fire_fn: Optional[Callable[..., None]] = None,
) -> dict:
    """Run one tick of the workflow_scheduler.

    Returns a stats dict: ``{processed, fired, suppressed, errored,
    shadow_fired, would_fire, status}``. ``status`` is one of
    ``'ok'``, ``'killed'``, ``'outside_window'``.
    """
    trigger_fn = trigger_fn or _default_trigger()
    gate_check_fn = gate_check_fn or _default_gate_check()
    is_callable_now_fn = is_callable_now_fn or _default_is_callable_now()
    record_fire_fn = record_fire_fn or cca.record_fire

    stats: dict[str, Any] = {
        "processed": 0,
        "fired": 0,
        "suppressed": 0,
        "errored": 0,
        "shadow_fired": 0,
        "would_fire": 0,  # dry-run only
        "status": "ok",
    }

    conn = get_workflow_db(workflow_db_path)
    try:
        # --- 1. Kill switch FIRST -----------------------------------------
        if _kill_switch_active(conn):
            stats["status"] = "killed"
            _emit_heartbeat(conn, "skipped", {"reason": "kill_switch_active", **stats})
            log.info("workflow_scheduler: kill-switch active — no-op tick")
            return stats

        # --- 2. RBI window check ------------------------------------------
        window = is_callable_now_fn()
        if not window.fire:
            stats["status"] = "outside_window"
            _emit_heartbeat(
                conn,
                "skipped",
                {"reason": f"outside_window: {window.reason}", **stats},
            )
            log.info("workflow_scheduler: outside RBI window — %s", window.reason)
            return stats

        # --- 3. Claim batch of PENDING rows -------------------------------
        now_str = _now_ist_str()
        rows = conn.execute(
            """
            SELECT id, run_id, node_id, attempt_count, customer_id,
                   scheduled_at_ist, status, cohort_name
            FROM wf_pending_actions
            WHERE status='PENDING'
              AND scheduled_at_ist <= ?
            ORDER BY scheduled_at_ist
            LIMIT ?
            """,
            (now_str, int(batch_limit)),
        ).fetchall()

        if not rows:
            _emit_heartbeat(conn, "ok", {"reason": "no_pending_rows", **stats})
            return stats

        # Bulk pre-fetch last-fire timestamps for cooldown gate (one query
        # for the whole batch, not N).
        cids_int = [int(r["customer_id"]) for r in rows]
        last_fire_lookup = cca.batch_last_fire_at(
            cids_int, vibrium_db_path=vibrium_db_path
        )
        now_ist_dt = datetime.now(IST)

        # --- 4. Process each row ------------------------------------------
        for row in rows:
            row_id = int(row["id"])
            cid = int(row["customer_id"])
            run_id = int(row["run_id"])
            cohort_name = row["cohort_name"]

            # 4a. Atomic claim (race-safe vs concurrent ticks)
            if not _claim_row(conn, row_id):
                log.debug(
                    "workflow_scheduler: row id=%d lost claim race — skipping",
                    row_id,
                )
                continue

            stats["processed"] += 1

            # 4b. Gate check
            try:
                fire, reason = _gate_check(
                    cid,
                    vibrium_db_path=vibrium_db_path,
                    gate_check_fn=gate_check_fn,
                    last_fire_lookup=last_fire_lookup,
                    daily_cap_limit=MAX_CALLS_PER_DAY,
                    cooldown_hours=COOLDOWN_HOURS,
                    now_ist=now_ist_dt,
                )
            except Exception as e:  # noqa: BLE001 — gate failure is per-row
                log.warning(
                    "workflow_scheduler: gate raised for cid=%s row=%d: %s",
                    cid,
                    row_id,
                    e,
                )
                _mark_status(
                    conn,
                    row_id,
                    status="ERROR",
                    last_error=f"gate raised: {type(e).__name__}: {e}",
                )
                stats["errored"] += 1
                continue

            if not fire:
                _mark_status(
                    conn,
                    row_id,
                    status="SUPPRESSED",
                    last_error=f"gate: {reason}",
                )
                stats["suppressed"] += 1
                log.info(
                    "workflow_scheduler: SUPPRESSED cid=%s row=%d reason=%s",
                    cid,
                    row_id,
                    reason,
                )
                continue

            # 4c. dry-run — log intent, release row, no side effects
            if dry_run:
                log.info(
                    "workflow_scheduler: DRY-RUN would fire cid=%s row=%d "
                    "run_id=%d node_id=%s attempt=%s",
                    cid,
                    row_id,
                    run_id,
                    row["node_id"],
                    row["attempt_count"],
                )
                _release_pending(conn, row_id)
                stats["would_fire"] += 1
                continue

            # 4d. shadow_mode — mark SHADOW_FIRED with fired_at_ist, no CT/audit
            if shadow_mode:
                _mark_status(
                    conn,
                    row_id,
                    status="SHADOW_FIRED",
                    fired_at_ist=_now_ist_str(),
                )
                stats["shadow_fired"] += 1
                log.info(
                    "workflow_scheduler: SHADOW_FIRED cid=%s row=%d run_id=%d",
                    cid,
                    row_id,
                    run_id,
                )
                continue

            # 4e. Live — call CT, mark FIRED on success, record audit
            #
            # NOTE: do NOT pass ``tag_group`` to trigger() — Phase 0a invariant.
            # Disposition wakeup is time-bound triangulation in Phase 7.
            try:
                result = trigger_fn(
                    cid,
                    "workflow",  # action_class — informational only; CT does not branch on this
                    ct_cred_path=str(ct_creds_path) if ct_creds_path else None,
                    campaign_id=int(campaign_id),
                    bot_id=str(bot_id),
                    contact_type=str(contact_type),
                    shadow_mode=False,
                )
            except Exception as e:  # noqa: BLE001 — CT call failure is per-row
                log.warning(
                    "workflow_scheduler: trigger raised for cid=%s row=%d: %s",
                    cid,
                    row_id,
                    e,
                )
                _mark_status(
                    conn,
                    row_id,
                    status="ERROR",
                    last_error=f"trigger raised: {type(e).__name__}: {e}",
                )
                stats["errored"] += 1
                continue

            status_str = str((result or {}).get("status", "error"))
            if status_str == "success":
                fired_at = _now_ist_str()
                _mark_status(
                    conn,
                    row_id,
                    status="FIRED",
                    fired_at_ist=fired_at,
                )
                # Audit write — single source of truth for the cross-system
                # daily cap. Failures here are logged but never kill the tick;
                # losing one audit row is observable + correctable, losing the
                # tick is not.
                try:
                    record_fire_fn(
                        customer_id=cid,
                        source="workflow",
                        run_id=run_id,
                        cohort_name=cohort_name,
                        ct_response_status="success",
                        vibrium_db_path=vibrium_db_path,
                    )
                except Exception as e:  # noqa: BLE001 — audit is observability
                    log.warning(
                        "workflow_scheduler: audit record_fire failed for "
                        "cid=%s row=%d: %s",
                        cid,
                        row_id,
                        e,
                    )
                stats["fired"] += 1
                log.info(
                    "workflow_scheduler: FIRED cid=%s row=%d run_id=%d",
                    cid,
                    row_id,
                    run_id,
                )
            else:
                # delivery_failed or error from CT — preserve the body in
                # last_error so the operator can decide what to do.
                body = (result or {}).get("body")
                err_msg = (
                    f"ct {status_str}: "
                    f"http={(result or {}).get('http_status')} body={body}"
                )
                # Hard cap the error string so we don't bloat the row on a
                # giant HTML error page.
                _mark_status(
                    conn,
                    row_id,
                    status="ERROR",
                    last_error=err_msg[:1024],
                )
                stats["errored"] += 1
                log.warning(
                    "workflow_scheduler: ERROR cid=%s row=%d status=%s",
                    cid,
                    row_id,
                    status_str,
                )

        _emit_heartbeat(conn, "ok", {"reason": "tick_complete", **stats})
        return stats
    finally:
        try:
            conn.close()
        except sqlite3.Error as e:
            # Close-on-finally errors are non-fatal (the OS reclaims the FD on
            # process exit) but observability matters — log so a recurring
            # close failure surfaces in the heartbeat trail.
            log.warning("workflow_scheduler: connection close failed: %s", e)


# ----------------------------------------------------------------------- CLI


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint — wraps ``run()`` for launchd.

    Example (paths come from ``config.json`` in production; supply your own):
        python3 -m workflow.workflow_scheduler \\
            --workflow-db state/workflow.db \\
            --vibrium-db state/vibrium.db \\
            [--shadow] [--dry-run] [--batch-limit 100]
    """
    parser = argparse.ArgumentParser(
        prog="workflow.workflow_scheduler",
        description="Fire wf_pending_actions rows via CleverTap externaltrigger.",
    )
    parser.add_argument("--workflow-db", required=True, help="Path to state/workflow.db")
    parser.add_argument("--vibrium-db", required=True, help="Path to state/vibrium.db")
    parser.add_argument(
        "--ct-creds",
        default=None,
        help="Path to CT credentials JSON. If omitted, the trigger() default is used.",
    )
    parser.add_argument("--shadow", action="store_true", help="Mark SHADOW_FIRED, do not call CT.")
    parser.add_argument("--dry-run", action="store_true", help="Log intent only, no DB or CT writes.")
    parser.add_argument(
        "--batch-limit",
        type=int,
        default=DEFAULT_BATCH_LIMIT,
        help=f"Max rows per tick (default {DEFAULT_BATCH_LIMIT}).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Config-error guard: in live mode (no --shadow / --dry-run) we MUST
    # have a CT creds path; passing None into trigger() would surface as a
    # cryptic TypeError. Surface the misconfiguration here with a clear exit.
    if not args.shadow and not args.dry_run and not args.ct_creds:
        log.error(
            "workflow_scheduler: live mode requires --ct-creds; "
            "pass --shadow or --dry-run for non-firing runs."
        )
        return 2

    try:
        stats = run(
            workflow_db_path=args.workflow_db,
            vibrium_db_path=args.vibrium_db,
            ct_creds_path=args.ct_creds,
            shadow_mode=args.shadow,
            dry_run=args.dry_run,
            batch_limit=args.batch_limit,
        )
    except Exception as e:  # noqa: BLE001 — top-level CLI guard
        log.exception("workflow_scheduler: fatal error: %s", e)
        return 1

    print(json.dumps(stats, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
