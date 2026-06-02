"""Enrollment poller — Wave-2 Phase 8 daemon.

A standalone, kill-switch-aware daemon that polls CleverTap for entry-condition
matches and creates new ``workflow_runs`` rows. Idempotent via the partial
UNIQUE index on ``(workflow_id, customer_id, enrollment_key)`` from Phase 1.

Hard guarantees:

  1. **READ-ONLY against CT.** This module NEVER fires a CT externaltrigger
     campaign and NEVER calls ``set_profile`` — it only reads profiles via
     Phase 2's ``bulk_get_profiles``. CT writes belong to the scheduler
     (Phase 6) and the SET_CT_PROP handler (Phase 4c).

  2. **Kill-switch first.** Latest row of ``wf_kill_switch.action='KILL'``
     short-circuits the run with zero side effects. Pattern lifted from
     ``~/vibrium-automation/agents/orchestrator.py`` `_check_kill_switch`.

  3. **RBI window with a narrower 08:00-18:00 IST cap.** Enrollment is one
     hour stricter than the calling window (08:00-19:00) so we don't enroll
     customers at 18:30 IST who would then immediately need calling outside
     the window. The always-on check is
     ``pre_call_gate.is_callable_now()``; this module additionally enforces
     ``now.hour < 18``.

  4. **Triple-cap on enrollment volume:**
       - Per-workflow daily cap (``workflows.max_new_enrollments_per_day``,
         default 1000) — pre-counted from today's existing rows; only the
         remaining gap is enrolled this tick.
       - Per-tick global cap (200) — applied across all workflows in this tick.
       - Per-workflow hard-abort threshold (5000) — if a single workflow
         would create more than this in one tick we exit non-zero with a loud
         log and zero side effects, unless ``--force`` is passed. The alert
         email path is Phase 8.5; v1 logs at ERROR level so the launchd
         stderr capture surfaces the failure.

  5. **CSV-input only for v1.** Each workflow's
     ``enrollment_trigger_config`` JSON contains ``{"source_csv": "/abs/path"}``;
     the CSV has a header row with ``customer_id`` as one column. Later
     phases can plug in a SQL-pull mode by extending
     ``_load_candidates``. Out of scope for Phase 8.

Idempotency comes from the partial UNIQUE index on workflow_runs — the same
customer can re-enroll at most once per (enrollment_key) value. The default
template ``{customer_id}_{YYYY-MM-DD}`` therefore allows one re-enrollment
per day.
"""
from __future__ import annotations

import argparse
import csv
import errno
import fcntl
import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional, Union
from zoneinfo import ZoneInfo

from workflow.clevertap_profile import bulk_get_profiles
from workflow.wf_store import PathLike, get_workflow_db, transaction

# Defer the simpleeval / external import to call-sites — keeps the module
# importable in environments where simpleeval isn't yet on the path (Phase 4a
# hasn't closed; the local fallback below is used).

log = logging.getLogger("workflow.enrollment_poller")

IST = ZoneInfo("Asia/Kolkata")


# ------------------------------------------------------------------ Tunables

# Per-tick global cap. After this many enrollments have been inserted across
# all workflows in this tick, the poller stops processing any further workflow.
MAX_NEW_ENROLLMENTS_PER_TICK = 200

# Per-workflow hard-abort threshold. If, after evaluating CT profiles, the
# *number of matched candidates* (before any cap is applied) for a single
# workflow exceeds this value, the poller treats it as a mis-configured
# entry condition and refuses to enrol any of them. Operator runs with
# ``--force`` to override after manual review.
PER_WORKFLOW_HARD_ABORT = 5000

# Narrower enrollment window: end at 18:00 IST (one hour stricter than the
# 19:00 RBI calling window).
ENROLLMENT_WINDOW_END_HOUR = 18

# Hour the RBI calling window opens (08:00 IST). Used only to bound the
# enrollment "prep window" below — the canonical calling gate stays
# pre_call_gate.is_callable_now().
RBI_CALL_START_HOUR = 8

# Enrollment may begin BEFORE the 08:00 calling window opens, in a pre-window
# "prep slot" [ENROLLMENT_PREP_START_HOUR, 08:00). Why this is safe — the exact
# invariant: enrollment writes only ``workflow_runs`` rows (status='ACTIVE' at
# the ENROLL node). A call can only fire from a ``wf_pending_actions`` row, and
# none exists until the executor advances the run through the graph. Even then,
# the scheduler independently gates every fire on ``is_callable_now()`` (08:00
# RBI window) + a per-customer ``pre_call_gate.check()``. So enrolling at 07:xx
# cannot produce a pre-08:00 call — it just front-loads run creation + the
# CleverTap profile fetch so the same-day (T+0) cohort is ready the instant the
# window opens. Default 8 == no prep window (legacy behaviour: enrollment starts
# with the RBI window). Set env WF_ENROLLMENT_PREP_START_HOUR=7 to enable 07:00.
ENROLLMENT_PREP_START_HOUR = int(os.environ.get("WF_ENROLLMENT_PREP_START_HOUR", "8"))

# CT bulk-fetch concurrency. Phase 2 module defaults to 5; we pin it here for
# clarity and to make it easy to tune from one place if a future workflow
# fans out very wide.
CT_BULK_CONCURRENCY = 5

# Repo root — used to derive the default lock-file path. Two levels up from
# this file (workflow/enrollment_poller.py → workflow/ → repo root).
_REPO_ROOT = Path(__file__).resolve().parent.parent

# Default lock-file path for the daily-cap race fix (Phase 8 P1-1). Operators
# can override via the CLI `--lock-file` flag if state/ is somewhere else.
DEFAULT_LOCK_FILE = _REPO_ROOT / "state" / "locks" / "enrollment_poller.lock"


# ------------------------------------------------------------------- Helpers


def _now_ist() -> datetime:
    """Tz-aware IST clock — CLAUDE.md rule #1."""
    return datetime.now(IST)


def _today_str(now: Optional[datetime] = None) -> str:
    """`YYYY-MM-DD` in IST. Used in the default enrollment-key template."""
    n = now if now is not None else _now_ist()
    if n.tzinfo is not None and n.tzinfo != IST:
        n = n.astimezone(IST)
    return n.strftime("%Y-%m-%d")


def _now_ist_str(now: Optional[datetime] = None) -> str:
    """`YYYY-MM-DD HH:MM:SS` IST-naive — matches event-ts drift rule."""
    n = now if now is not None else _now_ist()
    if n.tzinfo is not None and n.tzinfo != IST:
        n = n.astimezone(IST)
    return n.strftime("%Y-%m-%d %H:%M:%S")


def _is_callable_now():
    """Resolve `is_callable_now` from the symlinked external scripts.

    Local-imported so a missing/broken symlink raises at run-time, not at
    module-import time. Callers must handle ImportError.
    """
    # External import via the symlinked package (Phase 0 deliverable).
    from external.vibrium_automation_scripts.pre_call_gate import (  # type: ignore[import-not-found]
        is_callable_now,
    )
    return is_callable_now()


# ------------------------------------------------------------------ Kill switch


# ------------------------------------------------------------------ File lock


class LockContended(RuntimeError):
    """Raised when another `enrollment_poller` instance holds the lock.

    Distinct from generic OSError so the CLI can return a specific exit code
    and so tests can assert on the contention path without ambiguity.
    """


@contextmanager
def _acquire_lock(lock_path: PathLike) -> Iterator[int]:
    """Non-blocking exclusive flock on a sidecar lock file.

    Fixes Phase 8 P1-1: two overlapping ticks would each pre-count the daily
    cap before the (slow) CT bulk fetch, race past the cap, and double-enrol.
    With a process-level flock, the second tick exits cleanly with
    ``LockContended`` instead of running.

    Yields the open file descriptor so the caller can write a debug breadcrumb
    if needed; releases the lock on context exit (flock is released on fd
    close — we let the OS handle that path too).
    """
    p = Path(lock_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Open with O_CREAT so the lock file is auto-created on first run; we
    # never truncate or unlink — the file is a sentinel, content is debug-only.
    fd = os.open(str(p), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            # EWOULDBLOCK / EAGAIN → another instance holds the lock.
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise LockContended(
                    f"enrollment_poller lock {p} held by another instance"
                ) from exc
            raise
        # Best-effort breadcrumb: stamp pid + tick start so an operator
        # tail-f'ing the file sees who's holding it.
        try:
            os.ftruncate(fd, 0)
            os.write(
                fd,
                f"pid={os.getpid()} start={_now_ist_str()}\n".encode("utf-8"),
            )
        except OSError as exc:
            # Breadcrumb is non-essential; never let it crash the daemon.
            # Log + continue — operator can investigate disk/perm issues
            # via the lock file path next time without losing the tick.
            log.warning("enrollment_poller: lock breadcrumb write failed: %s", exc)
        yield fd
    finally:
        # flock releases on close; explicit unlock for clarity.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError as exc:
            # Already-closed / never-held — non-fatal; the os.close below
            # will release the lock unconditionally. Log so a recurring
            # pattern surfaces on the launchd stderr.
            log.warning("enrollment_poller: flock LOCK_UN failed: %s", exc)
        os.close(fd)


def _kill_switch_active(workflow_db_path: PathLike) -> bool:
    """Latest-row KILL detection on `wf_kill_switch`.

    Mirrors the agent orchestrator pattern: the LATEST row decides — earlier
    KILL/RESUME entries are history. Returns False on empty table or any
    error (loud-log, fail-safe-forward).
    """
    conn = get_workflow_db(workflow_db_path)
    try:
        row = conn.execute(
            "SELECT action, reason, set_by FROM wf_kill_switch "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return False
        if str(row["action"]).upper() == "KILL":
            log.warning(
                "wf_kill_switch active — aborting enrollment tick. reason=%s set_by=%s",
                row["reason"], row["set_by"],
            )
            return True
    except Exception as exc:  # noqa: BLE001 — top-level guard, log and continue
        log.error("kill_switch check failed: %s", exc)
    finally:
        conn.close()
    return False


# ------------------------------------------------------------------ CONDITION

class _ConditionError(RuntimeError):
    """Raised when an entry condition cannot be evaluated for a candidate."""


def _evaluate_condition(expr: str, scratchpad: dict[str, Any]) -> bool:
    """Evaluate the entry CONDITION expression against a CT-derived scratchpad.

    Attempts a local-import of the Phase 4a `workflow.agents.workflow_handlers.condition`
    module first — when that's available it's the canonical evaluator (saves
    drift). Falls back to an inline simpleeval-based evaluator with the same
    safety knobs (`functions={}`, `names=scratchpad`, `MAX_STRING_LENGTH=1024`,
    `MAX_POWER=100`) so Phase 8 can ship and audit cleanly without waiting
    for Phase 4a to close.

    Returns True / False. Raises `_ConditionError` on a syntax error or on
    a name referenced in the expression that isn't in the scratchpad — both
    are operator-misconfiguration signals that should fail loud, not
    silently enroll-everyone / enroll-no-one.
    """
    try:
        from workflow.agents.workflow_handlers.condition import (  # type: ignore[import-not-found]
            evaluate as _phase4a_evaluate,
        )
    except ImportError:
        _phase4a_evaluate = None  # type: ignore[assignment]

    if _phase4a_evaluate is not None:
        try:
            return bool(_phase4a_evaluate(expr, scratchpad))
        except Exception as exc:  # noqa: BLE001 — re-raise with structured signal
            raise _ConditionError(f"condition handler rejected expr: {exc}") from exc

    # ------ Fallback: inline simpleeval with the Phase 4a safety profile -----
    try:
        from simpleeval import SimpleEval, NameNotDefined, InvalidExpression
    except ImportError as exc:
        raise _ConditionError(
            "simpleeval is not installed; cannot evaluate condition. "
            "Run `pip install -r requirements.txt`."
        ) from exc

    s = SimpleEval(functions={}, names=dict(scratchpad))
    s.MAX_STRING_LENGTH = 1024  # type: ignore[attr-defined]
    s.MAX_POWER = 100  # type: ignore[attr-defined]
    try:
        result = s.eval(expr)
    except NameNotDefined as exc:
        raise _ConditionError(f"undefined name in expr {expr!r}: {exc}") from exc
    except (SyntaxError, InvalidExpression) as exc:
        raise _ConditionError(f"invalid expr {expr!r}: {exc}") from exc
    return bool(result)


# ------------------------------------------------------------------ Loading


@dataclass
class _Workflow:
    """In-memory view of an ACTIVE workflow row.

    Only the columns this poller needs are materialised; the executor (Phase 5)
    re-reads the full graph from `workflow_versions` when it ticks a run.
    """
    id: int
    name: str
    active_version_id: int
    enrollment_trigger_config: dict[str, Any]
    max_new_enrollments_per_day: int
    enrollment_key_template: str


def _load_active_workflows(conn) -> list[_Workflow]:
    """Read ACTIVE workflows with a published active_version_id.

    Schema source: `workflow/migrations/001_init.py`.

    ``enrollment_trigger_config`` is NOT a column on the workflows table in
    the v1 schema — it lives inside ``workflow_versions.graph_json`` (on the
    ENROLL node's config). For Phase 8, the spec asks us to read this off
    the workflow row. We parse the active version's graph_json and extract
    the ENROLL node's config.
    """
    rows = conn.execute(
        """
        SELECT w.id, w.name, w.active_version_id,
               w.max_new_enrollments_per_day, w.enrollment_key_template,
               v.graph_json
        FROM workflows w
        JOIN workflow_versions v ON v.id = w.active_version_id
        WHERE w.status = 'ACTIVE'
          AND w.active_version_id IS NOT NULL
        ORDER BY w.id
        """
    ).fetchall()

    out: list[_Workflow] = []
    for r in rows:
        try:
            graph = json.loads(r["graph_json"]) if r["graph_json"] else {}
        except json.JSONDecodeError as exc:
            log.error(
                "workflow id=%s graph_json malformed; skipping. err=%s",
                r["id"], exc,
            )
            continue
        enroll_cfg = _extract_enroll_config(graph)
        if enroll_cfg is None:
            log.warning(
                "workflow id=%s has no ENROLL node config; skipping",
                r["id"],
            )
            continue
        out.append(_Workflow(
            id=int(r["id"]),
            name=str(r["name"]),
            active_version_id=int(r["active_version_id"]),
            enrollment_trigger_config=enroll_cfg,
            max_new_enrollments_per_day=int(
                r["max_new_enrollments_per_day"] or 1000
            ),
            enrollment_key_template=str(
                r["enrollment_key_template"] or "{customer_id}_{YYYY-MM-DD}"
            ),
        ))
    return out


def _extract_enroll_config(graph: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Find the ENROLL node in a graph_json document and return its config.

    The graph format follows Phase 0 / Phase 4a conventions: a list of nodes
    keyed by `type`, each carrying a `config` dict. We accept both
    `{"nodes": [...]}` and a top-level list for robustness — these formats
    have churned during the design phase and the v1 lock isn't until Phase 11.
    """
    nodes: list[dict[str, Any]]
    if isinstance(graph, dict) and "nodes" in graph:
        nodes = graph.get("nodes") or []  # type: ignore[assignment]
    elif isinstance(graph, list):
        nodes = graph
    else:
        return None
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if str(n.get("type", "")).upper() == "ENROLL":
            cfg = n.get("config")
            return cfg if isinstance(cfg, dict) else {}
    return None


def _load_candidates(source_csv: PathLike) -> list[str]:
    """Read candidate customer_ids from a CSV.

    The CSV must have a header row containing a `customer_id` column (case-
    insensitive). Other columns are ignored. Duplicates are preserved in
    input order — the dedupe-by-enrollment-key index does the right thing
    at insert time.

    Raises FileNotFoundError if the file is missing — operator config bug;
    fail loud rather than enrol zero customers silently.
    """
    p = Path(source_csv)
    if not p.exists():
        raise FileNotFoundError(f"enrollment source_csv not found: {p}")
    cids: list[str] = []
    with open(p, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"CSV {p} has no header row")
        # Tolerate `customer_id` in any case.
        key = next(
            (k for k in reader.fieldnames if k.strip().lower() == "customer_id"),
            None,
        )
        if key is None:
            raise ValueError(
                f"CSV {p} missing a 'customer_id' column; got {reader.fieldnames}"
            )
        for row in reader:
            raw = row.get(key)
            # Falsy-filter trap (memory `feedback_falsy_filter_bug`): None
            # collapse only, all other falsy values pass through.
            v = ("" if raw is None else str(raw)).strip()
            if v:
                cids.append(v)
    return cids


# ------------------------------------------------------------------ Scratchpad


def _profile_to_scratchpad(record: dict[str, Any]) -> dict[str, Any]:
    """Project a CT `record` into the scratchpad shape that CONDITION expects.

    Pulls the 4 workflow-relevant properties from `profileData` and coerces
    them where the doc vocabulary requires int. Missing properties are
    omitted (they evaluate as undefined → CONDITION raises, which is the
    correct behavior — operator must filter for property presence
    explicitly).

    Phase 0a confirmed CT property names are lowercase. We tolerate uppercase
    too (legacy doc vocabulary) in case operator wrote `DPD` in the CT side.
    """
    pd = record.get("profileData") or {}
    if not isinstance(pd, dict):
        return {}

    def _g(*aliases: str) -> Any:
        for a in aliases:
            if a in pd:
                return pd[a]
        return None

    out: dict[str, Any] = {}
    risk = _g("coll_collection_risk_segmentation", "COLL_collection_risk_segmentation")
    if risk is not None:
        try:
            out["risk_segmentation"] = int(risk)
        except (TypeError, ValueError):
            # Leave it out — CONDITION raises NameNotDefined / type errors,
            # which the caller logs and skips this candidate.
            log.warning("non-int risk_segmentation: %r", risk)

    dpd_v = _g("dpd", "DPD")
    if dpd_v is not None:
        try:
            out["dpd"] = int(dpd_v)
        except (TypeError, ValueError):
            log.warning("non-int dpd: %r", dpd_v)

    wa = _g("coll_notification_replied")
    if wa is not None:
        out["wa_status"] = str(wa)

    bot = _g("coll_bot_calling")
    if bot is not None:
        out["bot_calling"] = str(bot)

    return out


# ------------------------------------------------------------------ Enrollment-key


def _format_enrollment_key(template: str, customer_id: str, now: datetime) -> str:
    """Render the operator-configurable enrollment_key template.

    Recognised placeholders:
      - {customer_id}
      - {YYYY-MM-DD}     IST date
      - {YYYY}, {MM}, {DD}

    Unknown placeholders are passed through verbatim — they show up in
    `workflow_runs.enrollment_key`, which makes the misconfiguration
    immediately visible to operators.
    """
    if now.tzinfo is not None and now.tzinfo != IST:
        now = now.astimezone(IST)
    return (
        template
        .replace("{customer_id}", str(customer_id))
        .replace("{YYYY-MM-DD}", now.strftime("%Y-%m-%d"))
        .replace("{YYYY}", now.strftime("%Y"))
        .replace("{MM}", now.strftime("%m"))
        .replace("{DD}", now.strftime("%d"))
    )


# ------------------------------------------------------------------ Stats


@dataclass
class _Stats:
    killed: bool = False
    # Phase 8 P1-2 fix: split the prior single `outside_window` bool into
    # three distinct reasons so Phase 8.5 alerts / Phase 9 launchd-summary
    # email can distinguish "symlink broken → operator must fix" from
    # "evening run, expected idle".
    outside_window: bool = False  # legacy aggregate (True if ANY of the below)
    outside_rbi_window: bool = False  # gate.fire == False, RBI 08:00-19:00
    outside_enrollment_window: bool = False  # hour >= 18 (narrower cap)
    workflows_seen: int = 0
    candidates_total: int = 0
    matched_total: int = 0
    enrolled_total: int = 0
    aborted_workflows: list[int] = field(default_factory=list)
    per_workflow: dict[int, dict[str, int]] = field(default_factory=dict)
    dry_run: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "killed": self.killed,
            "outside_window": self.outside_window,
            "outside_rbi_window": self.outside_rbi_window,
            "outside_enrollment_window": self.outside_enrollment_window,
            "workflows_seen": self.workflows_seen,
            "candidates_total": self.candidates_total,
            "matched_total": self.matched_total,
            "enrolled_total": self.enrolled_total,
            "aborted_workflows": list(self.aborted_workflows),
            "per_workflow": dict(self.per_workflow),
            "dry_run": self.dry_run,
        }


# ------------------------------------------------------------------ Today-count


def _todays_enrollment_count(conn, workflow_id: int, today: str) -> int:
    """Number of rows in workflow_runs created today for this workflow.

    `today` is an IST `YYYY-MM-DD` string. We count by `enrolled_at_ist`
    prefix-matching — cheap, sargable on a string column, and avoids
    timezone-conversion-in-SQL gotchas.
    """
    row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM workflow_runs
        WHERE workflow_id = ?
          AND enrolled_at_ist IS NOT NULL
          AND enrolled_at_ist LIKE ?
        """,
        (workflow_id, f"{today}%"),
    ).fetchone()
    return int(row["n"] or 0)


# ------------------------------------------------------------------ Main


def run(
    workflow_db_path: PathLike,
    *,
    force: bool = False,
    dry_run: bool = False,
    now: Optional[datetime] = None,
    lock_file: Optional[PathLike] = None,
) -> dict[str, Any]:
    """Single enrollment-poller tick.

    Returns a stats dict (see `_Stats.as_dict`). Exits early with
    `{'killed': True}` when the kill-switch is active, or
    `{'outside_window': True}` when called outside the enrollment window.

    `force=True` bypasses the per-workflow 5000-row hard abort. Use only
    after manual review.

    `dry_run=True` does NOT insert; logs the would-be enrollments instead.

    `now` is a test seam — production callers pass None.

    `lock_file` — Phase 8 P1-1 fix. Non-blocking flock around the body
    prevents two overlapping ticks from racing past the daily-cap pre-count.
    Defaults to ``state/locks/enrollment_poller.lock`` at repo root.
    Pass ``False``-y / explicit "" to disable (tests only).

    Raises:
        LockContended: another instance holds the lock; CLI converts to
            exit code 0 with a "another instance running" log so launchd
            doesn't escalate this benign condition.
        ImportError: the ``pre_call_gate`` symlink is broken / external
            package missing. CLI converts to exit code 3 so launchd stderr
            captures the ops failure and Phase 8.5 alerts can wake the
            operator (the prior swallow-and-return-0 silently rotted).
    """
    stats = _Stats(dry_run=dry_run)

    lock_path = (
        Path(lock_file) if lock_file else DEFAULT_LOCK_FILE
    )

    with _acquire_lock(lock_path):
        return _run_locked(
            workflow_db_path=workflow_db_path,
            force=force,
            dry_run=dry_run,
            now=now,
            stats=stats,
        )


def _run_locked(
    *,
    workflow_db_path: PathLike,
    force: bool,
    dry_run: bool,
    now: Optional[datetime],
    stats: "_Stats",
) -> dict[str, Any]:
    """Body of `run()` invoked under the file lock. See `run()` for docs."""
    # Gate 1 — kill switch. Read using a short-lived connection so we don't
    # hold the workflow.db open while we make the (slow) CT HTTP calls.
    if _kill_switch_active(workflow_db_path):
        stats.killed = True
        return stats.as_dict()

    # Gate 2 — RBI window + the narrower 18:00 enrollment cap.
    # Phase 8 P1-2 fix: ImportError is an ops failure (broken symlink) that
    # MUST surface as a non-zero exit so launchd stderr / Phase 8.5 alerts
    # catch it. Previously it was conflated with the legitimate
    # "outside window" no-op. We now re-raise; the CLI converts it to
    # exit code 3 and the function caller sees an unambiguous signal.
    gate = _is_callable_now()

    n = now if now is not None else _now_ist()
    if n.tzinfo is not None and n.tzinfo != IST:
        n = n.astimezone(IST)

    # `gate.fire` is False outside 08:00-19:00. Enrollment is additionally
    # allowed in a pre-window prep slot [ENROLLMENT_PREP_START_HOUR, 08:00) so
    # the morning fetch -> CT -> tick chain finishes before the calling window
    # opens. No call can fire in that slot — the scheduler enforces the 08:00
    # gate independently. We still suppress 18:00+ for enrollment below.
    prep_ok = ENROLLMENT_PREP_START_HOUR <= n.hour < RBI_CALL_START_HOUR
    if not gate.fire and not prep_ok:
        log.info(
            "outside enrollment window (gate.fire=False, hour=%s, prep_start=%s): %s",
            n.hour, ENROLLMENT_PREP_START_HOUR, getattr(gate, "reason", ""),
        )
        stats.outside_window = True
        stats.outside_rbi_window = True
        return stats.as_dict()
    if prep_ok and not gate.fire:
        log.info(
            "enrollment prep window active (hour=%s, prep_start=%s) — creating "
            "parked runs ahead of the 08:00 calling window; scheduler still "
            "gates all firing.",
            n.hour, ENROLLMENT_PREP_START_HOUR,
        )
    if n.hour >= ENROLLMENT_WINDOW_END_HOUR:
        log.info(
            "outside enrollment window (hour=%s >= %d, narrower than RBI 19:00)",
            n.hour, ENROLLMENT_WINDOW_END_HOUR,
        )
        stats.outside_window = True
        stats.outside_enrollment_window = True
        return stats.as_dict()

    # Gate 3 — load active workflows.
    conn = get_workflow_db(workflow_db_path)
    try:
        workflows = _load_active_workflows(conn)
    finally:
        conn.close()

    stats.workflows_seen = len(workflows)
    if not workflows:
        log.info("no ACTIVE workflows; nothing to do")
        return stats.as_dict()

    today = _today_str(n)
    enrolled_this_tick = 0

    for wf in workflows:
        wf_stats: dict[str, int] = {
            "candidates": 0,
            "ct_fetched": 0,
            "matched": 0,
            "skipped_cap": 0,
            "enrolled": 0,
            "hard_aborted": 0,
        }
        stats.per_workflow[wf.id] = wf_stats

        # Per-tick global cap.
        if enrolled_this_tick >= MAX_NEW_ENROLLMENTS_PER_TICK:
            log.info(
                "workflow id=%s skipped — per-tick global cap %d already reached",
                wf.id, MAX_NEW_ENROLLMENTS_PER_TICK,
            )
            continue

        # ---------------------------------------------------------- Candidates
        source_csv = wf.enrollment_trigger_config.get("source_csv")
        if not source_csv:
            log.warning(
                "workflow id=%s has no source_csv in enrollment_trigger_config; skipping",
                wf.id,
            )
            continue
        # Expand path tokens so the ENROLL config stays portable Mac↔AWS:
        #   {csv_dir}      → WF_ENROLLMENT_CSV_DIR env (default <repo>/state/
        #                    enrollment) — same default the fetch script writes to.
        #   {YYYY-MM-DD}   → today IST. Resolving against TODAY means a failed/late
        #                    upstream fetch leaves today's file ABSENT, so
        #                    _load_candidates raises FileNotFoundError and we skip
        #                    loudly rather than re-enrolling a stale cohort.
        csv_dir = os.environ.get(
            "WF_ENROLLMENT_CSV_DIR", str(_REPO_ROOT / "state" / "enrollment")
        )
        date_str = n.strftime("%Y-%m-%d")
        source_csv = (
            str(source_csv)
            .replace("{csv_dir}", csv_dir)
            .replace("{YYYY-MM-DD}", date_str)
            .replace("{date}", date_str)
        )
        try:
            cids = _load_candidates(source_csv)
        except (FileNotFoundError, ValueError) as exc:
            log.error("workflow id=%s candidate load failed: %s", wf.id, exc)
            continue
        wf_stats["candidates"] = len(cids)
        stats.candidates_total += len(cids)

        # ---------------------------------------------------- Bulk CT profiles
        if not cids:
            continue
        profiles = bulk_get_profiles(cids, concurrency=CT_BULK_CONCURRENCY)
        wf_stats["ct_fetched"] = sum(1 for p in profiles.values() if p is not None)

        # --------------------------------------------------- Evaluate condition
        expr = wf.enrollment_trigger_config.get("condition_expr")
        if not isinstance(expr, str) or not expr.strip():
            log.warning(
                "workflow id=%s has no condition_expr; skipping (would enrol everyone)",
                wf.id,
            )
            continue

        matched: list[str] = []
        for cid in cids:
            rec = profiles.get(cid)
            if rec is None:
                # 404 or fetch failed — skip silently; next tick retries.
                continue
            scratchpad = _profile_to_scratchpad(rec)
            try:
                if _evaluate_condition(expr, scratchpad):
                    matched.append(cid)
            except _ConditionError as exc:
                log.warning(
                    "workflow id=%s cid=%s condition eval failed: %s",
                    wf.id, cid, exc,
                )
                continue
        wf_stats["matched"] = len(matched)
        stats.matched_total += len(matched)

        # -------------------------------------------------- Hard-abort threshold
        if len(matched) > PER_WORKFLOW_HARD_ABORT and not force:
            log.error(
                "workflow id=%s name=%s would enrol %d (> hard_abort=%d). "
                "ABORTING this workflow. Re-run with --force after manual review.",
                wf.id, wf.name, len(matched), PER_WORKFLOW_HARD_ABORT,
            )
            wf_stats["hard_aborted"] = len(matched)
            stats.aborted_workflows.append(wf.id)
            continue

        if not matched:
            continue

        # ------------------------------------------------------- Daily-cap gap
        # Open a fresh connection (we closed the load one above; HTTP took
        # the wall time).
        conn = get_workflow_db(workflow_db_path)
        try:
            already = _todays_enrollment_count(conn, wf.id, today)
            cap = wf.max_new_enrollments_per_day
            remaining_daily = max(cap - already, 0)
            if remaining_daily <= 0:
                log.info(
                    "workflow id=%s daily cap reached (%d/%d); skipping",
                    wf.id, already, cap,
                )
                wf_stats["skipped_cap"] = len(matched)
                continue

            # Apply the per-tick global cap on the remaining slice.
            tick_remaining = (
                MAX_NEW_ENROLLMENTS_PER_TICK - enrolled_this_tick
            )
            slot_count = min(remaining_daily, tick_remaining, len(matched))
            to_enrol = matched[:slot_count]
            wf_stats["skipped_cap"] = len(matched) - len(to_enrol)

            # ------------------------------------------- INSERT OR IGNORE
            now_str = _now_ist_str(n)
            # ENROLL node id from the graph — we need it to seed
            # current_node_id. Reload from graph_json once per workflow.
            enroll_node_id = _find_enroll_node_id(conn, wf.active_version_id)
            if enroll_node_id is None:
                log.warning(
                    "workflow id=%s no ENROLL node_id; skipping",
                    wf.id,
                )
                continue

            inserted_now = 0
            if dry_run:
                # Print what we WOULD insert; do nothing.
                for cid in to_enrol:
                    ek = _format_enrollment_key(
                        wf.enrollment_key_template, cid, n,
                    )
                    log.info(
                        "[dry-run] would enrol wf=%s cid=%s key=%s",
                        wf.id, cid, ek,
                    )
                inserted_now = 0  # explicit — no writes happened
            else:
                # ONE transaction per workflow batch — atomic; if anything
                # fails, the whole batch rolls back.
                with transaction(conn):
                    for cid in to_enrol:
                        ek = _format_enrollment_key(
                            wf.enrollment_key_template, cid, n,
                        )
                        cur = conn.execute(
                            """
                            INSERT OR IGNORE INTO workflow_runs (
                              workflow_id, version_id, customer_id,
                              enrollment_key, current_node_id, current_node_type,
                              entered_node_at_ist, status, scratchpad_json,
                              enrolled_at_ist, updated_at_ist
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', '{}', ?, ?)
                            """,
                            (
                                wf.id, wf.active_version_id, cid, ek,
                                enroll_node_id, "ENROLL", now_str, now_str, now_str,
                            ),
                        )
                        if cur.rowcount == 1:
                            inserted_now += 1

            wf_stats["enrolled"] = inserted_now
            stats.enrolled_total += inserted_now
            enrolled_this_tick += inserted_now
        finally:
            conn.close()

    return stats.as_dict()


def _find_enroll_node_id(conn, version_id: int) -> Optional[str]:
    """Pull the ENROLL node's id out of the active version's graph_json."""
    row = conn.execute(
        "SELECT graph_json FROM workflow_versions WHERE id = ?",
        (version_id,),
    ).fetchone()
    if row is None or not row["graph_json"]:
        return None
    try:
        graph = json.loads(row["graph_json"])
    except json.JSONDecodeError as exc:
        # Operator-visible misconfiguration: an ACTIVE workflow version has
        # malformed graph_json. We can't enrol against it; log loudly so the
        # operator sees this in the launchd stderr and returns None so the
        # caller skips this workflow (documented in `_load_active_workflows`
        # which performs the same check at load time).
        log.error(
            "workflow_versions id=%s graph_json malformed; cannot find ENROLL node. err=%s",
            version_id, exc,
        )
        return None
    nodes: list[dict[str, Any]]
    if isinstance(graph, dict) and "nodes" in graph:
        nodes = graph.get("nodes") or []  # type: ignore[assignment]
    elif isinstance(graph, list):
        nodes = graph
    else:
        return None
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if str(n.get("type", "")).upper() == "ENROLL":
            # Generator writes "node_id"; older graphs used "id" — accept both.
            nid = n.get("node_id") or n.get("id")
            if nid is not None:
                return str(nid)
    return None


# ------------------------------------------------------------------ CLI


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow.enrollment_poller",
        description=(
            "Poll CleverTap for entry-condition matches and create "
            "workflow_runs rows. Kill-switch-aware, capped, idempotent."
        ),
    )
    parser.add_argument(
        "--workflow-db",
        required=True,
        help="Absolute path to state/workflow.db.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log intended enrollments without inserting.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Bypass the per-workflow hard-abort threshold "
            f"({PER_WORKFLOW_HARD_ABORT}). Use only after manual review."
        ),
    )
    parser.add_argument(
        "--lock-file",
        default=None,
        help=(
            "Optional override for the flock sidecar path "
            f"(default: {DEFAULT_LOCK_FILE})."
        ),
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

    try:
        stats = run(
            workflow_db_path=args.workflow_db,
            force=args.force,
            dry_run=args.dry_run,
            lock_file=args.lock_file,
        )
    except LockContended as exc:
        # Phase 8 P1-1 fix: a second concurrent tick is benign — log and
        # exit 0 so launchd doesn't escalate (overlap window is small and
        # the next tick at +30 min will catch up).
        log.info("enrollment_poller: %s — exiting cleanly", exc)
        return 0
    except ImportError as exc:
        # Phase 8 P1-2 fix: pre_call_gate symlink broken / external package
        # missing. This is an ops failure that must surface — exit non-zero
        # so launchd stderr captures it and Phase 8.5 detector A wakes
        # the operator within 30 min via the missing-heartbeat path.
        log.error("enrollment_poller: pre_call_gate import failed: %s", exc)
        return 3
    except Exception as exc:  # noqa: BLE001 — CLI top-level
        log.exception("enrollment_poller crashed: %s", exc)
        return 1

    log.info("enrollment_poller stats: %s", json.dumps(stats, default=str))

    # Non-zero exit when any workflow hit the hard-abort path (unless --force
    # was used, in which case it would have been bypassed and the list would
    # be empty anyway). This makes the launchd `StandardErrorPath` and the
    # operator alerting (Phase 8.5) catch the failure.
    if stats.get("aborted_workflows"):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(_main())
