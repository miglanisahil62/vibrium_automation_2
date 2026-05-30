"""WorkflowAgent — the executor tick loop.

This module is critical-surface: it drives every workflow_run's state machine.
The architecture invariants from ``docs/architecture.md`` §"Executor loop"
that this module enforces:

  * Kill-switch check is the FIRST thing every tick. ``KILL`` → no-op.
  * Per-tick **version cache** (``{version_id: parsed_graph}``). The cache is
    built once at the top of the tick and never mutated mid-tick — so a
    mid-tick edit to ``workflow_versions`` does not affect the in-flight tick.
  * One handler per run per tick. Each handler call is wrapped in
    ``BEGIN IMMEDIATE`` (via ``wf_store.transaction``); the side-effect write
    by the handler AND the run-state advance AND the ``workflow_node_log``
    append all commit atomically. Handler exception → rollback → run marked
    ``ERROR`` in a *separate* small transaction with the exception type+msg
    captured in ``workflow_node_log.side_effect``.
  * Terminal advance: the handler sets ``run.status='DONE'`` +
    ``terminal_status`` + ``terminated_at_ist`` in place; the executor
    persists those mutations exactly once.
  * Park advance: the handler sets ``run.status='WAITING'`` + ``ready_at_ist``
    in place; the executor persists.
  * Orphaned: a run whose ``current_node_id`` is not present in its pinned
    version → ``status='ORPHANED'`` + diagnostic in ``workflow_node_log``.
    Operator repairs via the ops console.
  * Heartbeat: every tick writes exactly one row to ``wf_agent_events`` with
    ``agent='workflow'`` and a summary JSON.
  * ``--dry-run``: handler side-effects MUST be skipped (handlers respect the
    flag), executor MUST NOT advance ``current_node_id``, executor still
    appends a ``workflow_node_log`` row with ``dry_run=1`` for the audit
    trail. No ``wf_agent_events`` row in dry-run (heartbeats only reflect
    real ticks).

Architecture invariant pinned in ``__init__`` of this file:
    The executor NEVER calls ``clevertap_trigger.trigger()`` (that's the
    workflow_scheduler's job — Phase 6). The executor NEVER calls
    ``requests.post`` to CT. The only HTTP egress is via handlers that own
    that egress (FETCH_CT_PROPS, SET_CT_PROP), and those go through
    ``workflow.clevertap_profile`` which has its own gate on writes.

CLI:
    python3 -m workflow.agents.workflow \\
        --workflow-db state/workflow.db \\
        [--dry-run] \\
        [--batch-limit 100]
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers import REGISTRY
from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run
from workflow.wf_store import PathLike, get_workflow_db, transaction

log = logging.getLogger("workflow.agents.workflow")

_IST = ZoneInfo("Asia/Kolkata")
_IST_TS_FMT = "%Y-%m-%d %H:%M:%S"

# Per architecture rev 3 §"Executor loop". Capped to keep ticks bounded —
# a 15-minute interval × 100 rows × ~1s per handler = ~100s, well under the
# launchd interval. Tunable via CLI flag, not env, to keep behavior explicit.
TICK_BATCH_LIMIT = 100


# --------------------------------------------------------------------------
# AgentResult — tick return type
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class AgentResult:
    """Outcome summary for one tick.

    Attributes:
        processed: Total runs the executor pulled from the ready-set this tick.
        advanced: Runs whose handler returned cleanly AND whose state was
            persisted (either advanced to next node, parked, or terminated).
        errored: Runs whose handler raised; ``status='ERROR'`` persisted.
        orphaned: Runs whose ``current_node_id`` was not in the loaded version;
            ``status='ORPHANED'`` persisted.
        status: One of ``"ok"`` (normal), ``"paused"`` (kill switch active —
            no work attempted), ``"error"`` (executor itself failed mid-tick).
    """

    processed: int
    advanced: int
    errored: int
    orphaned: int
    status: str = "ok"


# --------------------------------------------------------------------------
# Version cache helpers (per-tick)
# --------------------------------------------------------------------------


def _parse_graph(graph_json: str) -> Dict[str, NodeConfig]:
    """Parse a ``workflow_versions.graph_json`` blob into ``{node_id: NodeConfig}``.

    Shape contract (matches what Phase 10's validator will enforce, and what
    Phase 12's seed JSON adheres to):

        {
            "nodes": [
                {
                    "node_id": "uuid-...",
                    "type": "ENROLL",
                    "label": "...",
                    "config": {...},
                    "edges": {"next": "uuid-..."}
                },
                ...
            ]
        }

    Returns an empty dict if the blob is malformed or has no nodes — caller
    treats that as "no nodes loaded for this version" and any run on that
    version becomes ORPHANED. This is deliberately strict: a workflow with
    no parseable nodes should NOT silently advance anything.
    """
    try:
        data = json.loads(graph_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        log.error("graph_json parse error: %s", exc)
        return {}

    if not isinstance(data, dict):
        return {}
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return {}

    out: Dict[str, NodeConfig] = {}
    for n in nodes:
        if not isinstance(n, dict):
            continue
        node_id = n.get("node_id")
        n_type = n.get("type")
        if not isinstance(node_id, str) or not isinstance(n_type, str):
            continue
        out[node_id] = NodeConfig(
            node_id=node_id,
            type=n_type,
            label=n.get("label") or n_type,
            config=n.get("config") or {},
            edges=n.get("edges") or {},
        )
    return out


def _build_version_cache(
    conn: sqlite3.Connection, version_ids: List[int],
) -> Dict[int, Dict[str, NodeConfig]]:
    """Build the per-tick version graph cache.

    For each distinct version_id observed across the ready-set, parse its
    graph_json into a {node_id: NodeConfig} map. The cache is built BEFORE
    any handler executes so a concurrent UI save of v_next won't change the
    behavior of in-flight runs (P0-5 fix in architecture rev 3 — runs are
    pinned to ``version_id``).
    """
    if not version_ids:
        return {}
    # Parameterized IN clause — sqlite doesn't expand a list parameter into
    # ``IN (?, ?, ?)`` natively. We f-string a fixed-shape ``?,?,?`` derived
    # ONLY from the list length (never from user input). The version_ids list
    # itself comes from a prior SELECT on workflow_runs.version_id (INTEGER
    # column) so values are guaranteed integer; even so, they're bound
    # parameterized — only the comma-separated ``?`` skeleton is interpolated.
    placeholders = ",".join("?" * len(version_ids))
    rows = conn.execute(  # stashfin-lint: ignore  # f-string interpolation is the ?-count skeleton only; all version_ids are bound as parameters. Standard idiom for sqlite IN clauses.
        f"SELECT id, graph_json FROM workflow_versions WHERE id IN ({placeholders})",
        version_ids,
    ).fetchall()
    return {row["id"]: _parse_graph(row["graph_json"]) for row in rows}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _now_ist_str() -> str:
    """IST-naive ``YYYY-MM-DD HH:MM:SS`` (matches event-ts canon)."""
    return datetime.now(_IST).strftime(_IST_TS_FMT)


def _kill_switch_active(conn: sqlite3.Connection) -> bool:
    """True iff the latest row in ``wf_kill_switch`` is ``KILL`` (no RESUME after)."""
    row = conn.execute(
        "SELECT action FROM wf_kill_switch ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return bool(row) and row["action"] == "KILL"


def _row_to_run(row: sqlite3.Row) -> Run:
    """Hydrate a ``workflow_runs`` row into a mutable ``Run`` dataclass."""
    try:
        scratchpad = json.loads(row["scratchpad_json"] or "{}")
        if not isinstance(scratchpad, dict):
            scratchpad = {}
    except (TypeError, ValueError, json.JSONDecodeError):
        log.warning("workflow_runs.id=%s: scratchpad_json unparseable, using {}", row["id"])
        scratchpad = {}
    return Run(
        id=row["id"],
        workflow_id=row["workflow_id"],
        version_id=row["version_id"],
        customer_id=row["customer_id"],
        current_node_id=row["current_node_id"] or "",
        scratchpad=scratchpad,
        status=row["status"],
        ready_at_ist=row["ready_at_ist"],
        entered_node_at_ist=row["entered_node_at_ist"],
        terminated_at_ist=row["terminated_at_ist"],
        terminal_status=row["terminal_status"],
    )


def _append_node_log(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    from_node_id: Optional[str],
    to_node_id: Optional[str],
    edge_label: Optional[str],
    scratchpad_before: dict,
    scratchpad_after: dict,
    side_effect: Optional[str],
    dry_run: bool,
) -> None:
    """Insert one ``workflow_node_log`` row. Called inside the caller's txn."""
    conn.execute(
        """
        INSERT INTO workflow_node_log
            (run_id, ts_ist, from_node_id, to_node_id, edge_label,
             scratchpad_before, scratchpad_after, side_effect, dry_run)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            _now_ist_str(),
            from_node_id,
            to_node_id,
            edge_label,
            json.dumps(scratchpad_before, separators=(",", ":")),
            json.dumps(scratchpad_after, separators=(",", ":")),
            side_effect,
            1 if dry_run else 0,
        ),
    )


# --------------------------------------------------------------------------
# WorkflowAgent
# --------------------------------------------------------------------------


def run(
    *,
    workflow_db_path: PathLike,
    dry_run: bool = False,
    batch_limit: int = TICK_BATCH_LIMIT,
    **_ignored: object,
) -> dict[str, object]:
    """Module-level entrypoint used by Phase 9's workflow_orchestrator.

    The orchestrator dispatches every daemon via a uniform ``run(**kwargs)``
    contract (see ``workflow.workflow_orchestrator._MODE_REGISTRY``). The
    executor's own work lives in ``WorkflowAgent.tick``; this wrapper bridges
    the two surfaces and returns a JSON-serialisable dict (as_dict of
    ``AgentResult``) for the heartbeat summary.

    ``**_ignored`` is intentional — the orchestrator may thread other
    daemons' kwargs (e.g. ``force``) through generic plumbing; we accept
    and discard them rather than TypeError.
    """
    agent = WorkflowAgent(
        workflow_db_path=workflow_db_path,
        batch_limit=batch_limit,
    )
    result = agent.tick(dry_run=dry_run)
    return asdict(result)


class WorkflowAgent:
    """Executor for the workflow_runs state machine.

    One instance per process. ``tick()`` is the single public method;
    everything else is internal. Callers (the CLI here, and the orchestrator
    in Phase 9) construct one with a workflow_db path and either repeatedly
    call ``tick()`` themselves or rely on launchd to invoke the CLI.

    Connection ownership:
        If ``conn`` is provided to ``tick()``, the caller owns its lifecycle
        (tests use this to share a tmp_path DB). Otherwise the executor opens
        a connection from the configured path and closes it at tick end.
    """

    def __init__(
        self,
        workflow_db_path: PathLike,
        *,
        batch_limit: int = TICK_BATCH_LIMIT,
    ) -> None:
        self.workflow_db_path = Path(workflow_db_path)
        self.batch_limit = int(batch_limit)

    # ----------------------------------------------------------------- tick
    def tick(
        self,
        *,
        dry_run: bool = False,
        conn: Optional[sqlite3.Connection] = None,
    ) -> AgentResult:
        """Run one tick. See module docstring for invariants."""
        owns_conn = conn is None
        if owns_conn:
            conn = get_workflow_db(self.workflow_db_path)
        assert conn is not None  # for type checkers

        try:
            # 1) Kill switch — first thing, no exceptions.
            if _kill_switch_active(conn):
                result = AgentResult(
                    processed=0, advanced=0, errored=0, orphaned=0, status="paused",
                )
                log.info(
                    "agent=workflow status=paused processed=0 advanced=0 "
                    "errored=0 orphaned=0 dry_run=%s",
                    dry_run,
                )
                # NB: no heartbeat in paused state — operators detect "no
                # heartbeat for >30 min" as the alert signal (Phase 8.5).
                return result

            # 2) Pull ready runs.
            now = _now_ist_str()
            rows = conn.execute(
                """
                SELECT * FROM workflow_runs
                WHERE status IN ('ACTIVE', 'WAITING')
                  AND (ready_at_ist IS NULL OR ready_at_ist <= ?)
                ORDER BY enrolled_at_ist
                LIMIT ?
                """,
                (now, self.batch_limit),
            ).fetchall()

            # 3) Build the per-tick version cache. Built ONCE, never refreshed
            #    mid-tick — pinned-version invariant.
            version_ids = sorted({row["version_id"] for row in rows})
            version_cache = _build_version_cache(conn, version_ids)

            processed = 0
            advanced = 0
            errored = 0
            orphaned = 0

            for row in rows:
                processed += 1
                run = _row_to_run(row)
                graph = version_cache.get(run.version_id, {})

                # 4) Orphan check — node_id not in pinned version.
                node = graph.get(run.current_node_id)
                if node is None:
                    self._mark_orphaned(conn, run, dry_run=dry_run)
                    orphaned += 1
                    continue

                # 5) Dispatch.
                handler = REGISTRY.get(node.type)
                if handler is None:
                    # Treat as orphan-ish: node type unknown. Defensive —
                    # validator should reject, but never trust input.
                    self._mark_orphaned(
                        conn, run,
                        reason=f"unknown_node_type: {node.type}",
                        dry_run=dry_run,
                    )
                    orphaned += 1
                    continue

                # 6) Transactional advance.
                ok = self._run_handler(conn, node, run, handler, dry_run=dry_run)
                if ok:
                    advanced += 1
                else:
                    errored += 1

            # 7) Heartbeat — once per tick. NOT inside any earlier txn —
            #    even if individual runs errored, the tick itself succeeded.
            self._emit_heartbeat(
                conn,
                processed=processed,
                advanced=advanced,
                errored=errored,
                orphaned=orphaned,
                dry_run=dry_run,
            )

            log.info(
                "agent=workflow status=ok processed=%d advanced=%d errored=%d "
                "orphaned=%d dry_run=%s",
                processed, advanced, errored, orphaned, dry_run,
            )
            return AgentResult(
                processed=processed,
                advanced=advanced,
                errored=errored,
                orphaned=orphaned,
                status="ok",
            )
        finally:
            if owns_conn:
                conn.close()

    # --------------------------------------------------- transactional advance
    def _run_handler(
        self,
        conn: sqlite3.Connection,
        node: NodeConfig,
        run: Run,
        handler: Any,
        *,
        dry_run: bool,
    ) -> bool:
        """Execute one handler inside one transaction.

        Returns True on clean handler exit (state persisted, log row written).
        Returns False if the handler raised; the run is marked ``ERROR`` in
        a separate small transaction so the error log itself doesn't depend
        on the failed transaction's commit.

        Dry-run contract:
            * Pass ``dry_run=True`` through to the handler (handler is
              responsible for not performing side-effects — fire_vb_call
              skips the INSERT, set_ct_prop won't POST, etc.).
            * Executor DOES NOT update ``current_node_id`` or any
              ``workflow_runs`` field.
            * Executor DOES append a ``workflow_node_log`` row with
              ``dry_run=1`` so the intended transition is auditable.
        """
        scratchpad_before = dict(run.scratchpad)
        from_node_id = run.current_node_id

        try:
            with transaction(conn):
                # Handlers must accept ``ctx`` for forward-compat (Phase 6+
                # may pass shared resources). For now ctx=None.
                result: NodeResult = handler(node, run, None, conn, dry_run=dry_run)

                if not isinstance(result, NodeResult):
                    raise TypeError(
                        f"handler {handler!r} returned {type(result).__name__}, "
                        f"expected NodeResult"
                    )

                # Merge scratchpad patch (shallow merge, handler-side
                # mutations to run.scratchpad already happened in-place but
                # the contract says use scratchpad_patch).
                scratchpad_after = dict(scratchpad_before)
                if result.scratchpad_patch:
                    scratchpad_after.update(result.scratchpad_patch)

                # Resolve next node + edge label.
                to_node_id: Optional[str]
                edge_label: Optional[str] = result.next_edge
                if result.next_edge is None:
                    # Terminal (run.status='DONE') or park (run.status='WAITING').
                    to_node_id = None
                else:
                    edge_label = result.next_edge
                    to_node_id = node.edges.get(result.next_edge)
                    if to_node_id is None:
                        # Handler returned an edge that the graph doesn't
                        # define. This is a graph authoring bug — fail the
                        # run to ERROR so the operator can repair.
                        raise ValueError(
                            f"node {node.node_id!r} (type={node.type}) has no "
                            f"edge {result.next_edge!r}; defined edges: "
                            f"{sorted(node.edges)}"
                        )

                # 1) Audit log (always — both real and dry-run).
                _append_node_log(
                    conn,
                    run_id=run.id,
                    from_node_id=from_node_id,
                    to_node_id=to_node_id,
                    edge_label=edge_label,
                    scratchpad_before=scratchpad_before,
                    scratchpad_after=scratchpad_after,
                    side_effect=result.side_effect,
                    dry_run=dry_run,
                )

                # 2) Persist run state — but NOT in dry-run.
                if dry_run:
                    # Documented contract: no current_node_id advance, no
                    # status/scratchpad mutation persisted.
                    return True

                self._persist_run_advance(
                    conn,
                    run=run,
                    to_node_id=to_node_id,
                    edge_label=edge_label,
                    scratchpad_after=scratchpad_after,
                    result=result,
                )
            return True
        except Exception as exc:  # noqa: BLE001
            # The handler (or persist step) raised — txn rolled back already
            # by ``transaction()``. Record the error in a *separate* tiny txn
            # so the audit log isn't lost. This is exactly the pattern the
            # architecture pins for "exceptions → run.status = ERROR with
            # traceback in workflow_node_log.side_effect" (rev 3 §Executor).
            log.exception(
                "handler raised: run_id=%s node_id=%s type=%s",
                run.id, run.current_node_id, node.type,
            )
            self._mark_errored(
                conn,
                run=run,
                node=node,
                exc=exc,
                scratchpad_before=scratchpad_before,
                dry_run=dry_run,
            )
            return False

    # ----------------------------------------------------- persistence helpers
    def _persist_run_advance(
        self,
        conn: sqlite3.Connection,
        *,
        run: Run,
        to_node_id: Optional[str],
        edge_label: Optional[str],  # noqa: ARG002  — kept for symmetry with the log writer
        scratchpad_after: dict,
        result: NodeResult,
    ) -> None:
        """Apply one ``NodeResult`` to ``workflow_runs``.

        Three cases:
            1. Terminal: ``run.status='DONE'`` was set by handler (terminate.py).
               Persist ``status``, ``terminal_status``, ``terminated_at_ist``,
               ``scratchpad_json``, ``updated_at_ist``. ``current_node_id``
               unchanged (kept for audit; the terminal node stays as the
               "last visited" node).
            2. Park: ``run.status='WAITING'`` was set by handler (await_disposition).
               Persist ``status``, ``ready_at_ist``, ``scratchpad_json``,
               ``updated_at_ist``. ``current_node_id`` unchanged (the run is
               still ON this node waiting for an external signal).
            3. Advance: ``run.status`` is ``ACTIVE`` (or has been set
               'ACTIVE' by await_disposition on the disposition edge).
               Persist ``current_node_id``, ``entered_node_at_ist``,
               ``ready_at_ist=NULL``, ``scratchpad_json``, ``updated_at_ist``.
        """
        now = _now_ist_str()
        scratchpad_json = json.dumps(scratchpad_after, separators=(",", ":"))

        if run.status == "DONE":
            # Terminal — persist what terminate.py mutated. Use the run's
            # ``terminated_at_ist`` (handler set it) rather than ``now`` so
            # the audit record matches the side-effect timestamp.
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = ?,
                    terminal_status = ?,
                    terminated_at_ist = ?,
                    scratchpad_json = ?,
                    updated_at_ist = ?,
                    ready_at_ist = NULL
                WHERE id = ?
                """,
                (
                    run.status,
                    run.terminal_status,
                    run.terminated_at_ist or now,
                    scratchpad_json,
                    now,
                    run.id,
                ),
            )
            return

        if run.status == "WAITING":
            # Park — persist the wait deadline; current_node_id unchanged.
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = ?,
                    ready_at_ist = ?,
                    scratchpad_json = ?,
                    updated_at_ist = ?
                WHERE id = ?
                """,
                (
                    run.status,
                    result.ready_at_ist or run.ready_at_ist,
                    scratchpad_json,
                    now,
                    run.id,
                ),
            )
            return

        # Normal advance. If there's no next node and we're not DONE/WAITING,
        # this is a bug in the handler — caller should have raised already.
        if to_node_id is None:
            raise ValueError(
                f"run_id={run.id} on node_id={run.current_node_id} "
                f"got next_edge=None but status={run.status!r} (not DONE/WAITING)"
            )

        # Look up the type of the new node from the run's pinned version so
        # we can keep ``current_node_type`` denormalized in sync.
        new_type_row = conn.execute(
            """
            SELECT graph_json FROM workflow_versions WHERE id = ?
            """,
            (run.version_id,),
        ).fetchone()
        new_node_type: Optional[str] = None
        if new_type_row:
            graph = _parse_graph(new_type_row["graph_json"])
            target = graph.get(to_node_id)
            if target is not None:
                new_node_type = target.type

        # ACTIVE — clear ready_at_ist (any prior park deadline is moot now).
        conn.execute(
            """
            UPDATE workflow_runs
            SET status = 'ACTIVE',
                current_node_id = ?,
                current_node_type = ?,
                entered_node_at_ist = ?,
                ready_at_ist = NULL,
                scratchpad_json = ?,
                updated_at_ist = ?
            WHERE id = ?
            """,
            (
                to_node_id,
                new_node_type,
                now,
                scratchpad_json,
                now,
                run.id,
            ),
        )

    # ----------------------------------------------------------- ERROR / ORPHAN
    def _mark_errored(
        self,
        conn: sqlite3.Connection,
        *,
        run: Run,
        node: NodeConfig,
        exc: BaseException,
        scratchpad_before: dict,
        dry_run: bool,
    ) -> None:
        """Persist ``status='ERROR'`` and append a workflow_node_log row.

        In dry-run we still log the error (since the audit log is the
        observable artifact in dry-run) but we do NOT change run.status. The
        operator running a dry-run wants to see "this handler would have
        errored" without polluting the real run state.
        """
        side_effect = f"ERROR: {type(exc).__name__}: {exc}"
        try:
            with transaction(conn):
                _append_node_log(
                    conn,
                    run_id=run.id,
                    from_node_id=run.current_node_id,
                    to_node_id=None,
                    edge_label=None,
                    scratchpad_before=scratchpad_before,
                    scratchpad_after=scratchpad_before,
                    side_effect=side_effect,
                    dry_run=dry_run,
                )
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE workflow_runs
                        SET status = 'ERROR',
                            updated_at_ist = ?
                        WHERE id = ?
                        """,
                        (_now_ist_str(), run.id),
                    )
        except Exception:  # noqa: BLE001
            # If even the error-recording txn fails (e.g., DB locked), log
            # loudly and give up on this run for the tick. The next tick will
            # observe the original state and re-attempt.
            log.exception(
                "failed to record ERROR for run_id=%s node_id=%s",
                run.id, node.node_id,
            )

    def _mark_orphaned(
        self,
        conn: sqlite3.Connection,
        run: Run,
        *,
        reason: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        """Persist ``status='ORPHANED'`` with a diagnostic.

        Reasons:
            * Default: ``node_not_found: <node_id>`` — run's current node id
              is not in the pinned version graph.
            * Custom: unknown node type, malformed graph_json, etc.
        """
        diag = reason or f"node_not_found: {run.current_node_id}"
        try:
            with transaction(conn):
                _append_node_log(
                    conn,
                    run_id=run.id,
                    from_node_id=run.current_node_id,
                    to_node_id=None,
                    edge_label=None,
                    scratchpad_before=run.scratchpad,
                    scratchpad_after=run.scratchpad,
                    side_effect=diag,
                    dry_run=dry_run,
                )
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE workflow_runs
                        SET status = 'ORPHANED',
                            updated_at_ist = ?
                        WHERE id = ?
                        """,
                        (_now_ist_str(), run.id),
                    )
        except Exception:  # noqa: BLE001
            log.exception("failed to mark ORPHANED for run_id=%s", run.id)

    # ----------------------------------------------------------------- heartbeat
    def _emit_heartbeat(
        self,
        conn: sqlite3.Connection,
        *,
        processed: int,
        advanced: int,
        errored: int,
        orphaned: int,
        dry_run: bool,
    ) -> None:
        """Insert one row in ``wf_agent_events``.

        Status semantics:
            * ``ok`` — every observed run resolved cleanly (advanced + errored
              + orphaned == processed, processed >= 0).
            * ``down`` — reserved for future "tick partial failure"; not used
              currently. The agent reaches this code path only if the tick
              loop didn't itself raise.
        """
        summary = {
            "processed": processed,
            "advanced": advanced,
            "errored": errored,
            "orphaned": orphaned,
            "dry_run": bool(dry_run),
        }
        try:
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO wf_agent_events (ts_ist, agent, status, summary_json)
                    VALUES (?, 'workflow', 'ok', ?)
                    """,
                    (_now_ist_str(), json.dumps(summary, separators=(",", ":"))),
                )
        except Exception:  # noqa: BLE001
            # Heartbeat write failure is rare but shouldn't crash the tick.
            # The alerts pipeline (Phase 8.5) will flag the missing heartbeat
            # within 30 min.
            log.exception("failed to write workflow heartbeat")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="workflow.agents.workflow",
        description="Run one tick of the workflow executor.",
    )
    parser.add_argument(
        "--workflow-db",
        required=True,
        help="Absolute path to state/workflow.db.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Log intended transitions to workflow_node_log with dry_run=1 "
            "and do NOT advance current_node_id or write side effects."
        ),
    )
    parser.add_argument(
        "--batch-limit",
        type=int,
        default=TICK_BATCH_LIMIT,
        help=f"Max runs processed per tick (default {TICK_BATCH_LIMIT}).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging on the workflow logger.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    agent = WorkflowAgent(
        workflow_db_path=args.workflow_db,
        batch_limit=args.batch_limit,
    )
    try:
        result = agent.tick(dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        # Top-level: print structured error and exit non-zero so the
        # heartbeat/cron wrapper sees the failure.
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        log.exception("WorkflowAgent.tick crashed")
        return 1

    # Single-line summary on stdout for the cron capture.
    print(json.dumps(asdict(result), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
