"""Phase 9 — workflow_orchestrator.

Single CLI entrypoint dispatching by ``--mode`` to the workflow-system daemons.
Each mode wraps the underlying ``run()`` with heartbeat stamping into
``wf_agent_events`` and exit-code mapping for launchd surfacing.

The daemon-name contract (per Phase 8.5 audit) — these MUST match the
``agent`` field that Phase 8.5's alert detector A scans for. Diverge and
the alert fires forever:

    workflow_executor    — WorkflowAgent.tick()
    workflow_scheduler   — workflow_scheduler.run()
    workflow_ingest      — workflow_ingest.run()
    workflow_enrollment  — enrollment_poller.run()
    workflow_alerts      — alerts.run()
    workflow_digest      — workflow_digest.run()

Heartbeat semantics:
- Before invoking the daemon's run(): stamp ``status='started'``.
- After clean return: stamp ``status='ok'`` + summary_json with the run's stats.
- On exception: stamp ``status='down'`` + summary_json with exception type+msg.
- Crash mid-run: launchd sees non-zero exit; the ``started`` heartbeat without
  a matching ``ok``/``down`` is itself diagnostic.

Exit codes:
- 0 — clean run (including LockContended / outside_window / killed —
       these are expected operational states, not failures).
- 1 — daemon raised an unexpected exception. Launchd's stderr captures.
- 2 — invalid CLI arguments / missing required config.
- 3 — ImportError on a critical external module (e.g., pre_call_gate
       symlink broken). Phase 8.5 detector A catches this within 30 min.
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import sqlite3
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger(__name__)

# The 6 valid --mode values + their (agent_name, module_path, callable_name)
# triples. Daemon names MUST match Phase 8.5's contract.
_MODE_REGISTRY: dict[str, tuple[str, str, str]] = {
    "executor":   ("workflow_executor",   "workflow.agents.workflow",   "_run_executor_mode"),
    "scheduler":  ("workflow_scheduler",  "workflow.workflow_scheduler", "run"),
    "ingest":     ("workflow_ingest",     "workflow.workflow_ingest",    "run"),
    "enrollment": ("workflow_enrollment", "workflow.enrollment_poller",  "run"),
    "alerts":     ("workflow_alerts",     "workflow.alerts",             "run"),
    "digest":     ("workflow_digest",     "workflow.workflow_digest",    "run"),
}


def _now_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _emit_heartbeat(
    workflow_db_path: Path,
    agent: str,
    status: str,
    summary: dict[str, Any] | None = None,
) -> None:
    """Stamp one row into ``wf_agent_events``.

    Never raises — heartbeat failures are themselves logged but the daemon's
    own work is the priority. A broken heartbeat surfaces via Phase 8.5
    detector A (no heartbeat in 30 min).
    """
    try:
        cn = sqlite3.connect(str(workflow_db_path))
        try:
            cn.execute(
                "INSERT INTO wf_agent_events (ts_ist, agent, status, summary_json) "
                "VALUES (?, ?, ?, ?)",
                (_now_ist_str(), agent, status, json.dumps(summary or {})),
            )
            cn.commit()
        finally:
            cn.close()
    except sqlite3.Error as exc:
        log.warning(
            "heartbeat write failed agent=%s status=%s err=%s",
            agent, status, exc,
        )


def _run_executor_mode(
    *,
    workflow_db_path: Path,
    dry_run: bool = False,
    batch_limit: int = 100,
    **_ignored,
) -> dict[str, Any]:
    """Adapter — WorkflowAgent.tick() doesn't follow the run(*, ...) shape
    exactly, so wrap it here."""
    from workflow.agents.workflow import WorkflowAgent

    agent = WorkflowAgent(
        workflow_db_path=str(workflow_db_path),
        batch_limit=batch_limit,
    )
    return agent.tick(dry_run=dry_run)


def _dispatch(
    mode: str,
    workflow_db_path: Path,
    extra_kwargs: dict[str, Any],
) -> int:
    """Run one daemon mode end-to-end: heartbeat → call → heartbeat → exit."""
    if mode not in _MODE_REGISTRY:
        log.error("invalid --mode: %r (valid: %s)", mode, sorted(_MODE_REGISTRY))
        return 2

    agent_name, module_path, callable_name = _MODE_REGISTRY[mode]

    _emit_heartbeat(workflow_db_path, agent_name, "started",
                    {"mode": mode, "started_at_ist": _now_ist_str()})

    try:
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            # Critical external dep missing (e.g., pre_call_gate symlink).
            # Phase 8.5 detector A catches via missing-heartbeat path.
            log.exception("import failed for mode=%s: %s", mode, exc)
            _emit_heartbeat(workflow_db_path, agent_name, "down",
                            {"error_type": "ImportError",
                             "error_msg": str(exc),
                             "mode": mode})
            return 3

        fn: Callable[..., dict[str, Any]] = getattr(module, callable_name)

        # Call with the right shape per daemon. All daemons accept
        # workflow_db_path; the rest depends on mode.
        result = fn(workflow_db_path=workflow_db_path, **extra_kwargs)

        _emit_heartbeat(workflow_db_path, agent_name, "ok",
                        {"mode": mode, "result": result or {}})
        log.info("agent=%s status=ok result=%s", agent_name, result)
        return 0

    except Exception as exc:  # noqa: BLE001 — CLI top-level
        log.exception("daemon %s raised: %s", mode, exc)
        _emit_heartbeat(
            workflow_db_path, agent_name, "down",
            {"error_type": type(exc).__name__,
             "error_msg": str(exc),
             "traceback_tail": traceback.format_exc().splitlines()[-5:],
             "mode": mode},
        )
        return 1


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Vibrium workflow daemon dispatcher (Phase 9).",
    )
    parser.add_argument(
        "--mode",
        choices=sorted(_MODE_REGISTRY.keys()),
        required=True,
        help="Which daemon to run.",
    )
    parser.add_argument(
        "--workflow-db",
        required=True,
        type=Path,
        help="Path to state/workflow.db (the engine's own DB).",
    )
    parser.add_argument(
        "--vibrium-db",
        type=Path,
        default=None,
        help="Path to vibrium.db (shared with adhoc). Required for scheduler mode.",
    )
    parser.add_argument(
        "--ct-creds",
        type=Path,
        default=None,
        help="CT credentials JSON path (scheduler live mode requires this).",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip side-effects; log intent only.")
    parser.add_argument("--shadow", action="store_true",
                        help="Scheduler-only: shadow_mode (no live CT calls).")
    parser.add_argument("--force", action="store_true",
                        help="Enrollment-only: bypass the >5000 hard-abort.")
    parser.add_argument("--batch-limit", type=int, default=100,
                        help="Executor-only: TICK_BATCH_LIMIT.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Map CLI flags into per-mode kwargs.
    extra: dict[str, Any] = {"dry_run": args.dry_run}
    if args.mode == "scheduler":
        if args.vibrium_db is None:
            log.error("--vibrium-db required for mode=scheduler")
            return 2
        extra["vibrium_db_path"] = args.vibrium_db
        extra["ct_creds_path"] = args.ct_creds
        extra["shadow_mode"] = args.shadow
    elif args.mode == "enrollment":
        extra["force"] = args.force
    elif args.mode == "executor":
        extra["batch_limit"] = args.batch_limit
    # ingest / alerts / digest accept only workflow_db_path + dry_run.

    return _dispatch(args.mode, args.workflow_db, extra)


if __name__ == "__main__":
    sys.exit(_main())
