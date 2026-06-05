"""FIRE_VB_CALL handler — queue a row in ``wf_pending_actions``.

Critical-surface: this handler is the ONLY path by which the workflow
engine can cause a CT externaltrigger fire. The row it inserts is picked
up by ``workflow_scheduler.py`` (Phase 6) which then calls
``pre_call_gate.check`` and ``clevertap_trigger.trigger``.

Per architecture rev 3 §"Side-effect rules":
  * The handler does NOT call ``clevertap_trigger`` directly.
  * The INSERT runs inside the executor-owned transaction (``txn``); the
    executor commits both the queue write and the run-state advance
    atomically.
  * ``INSERT OR IGNORE`` on the ``UNIQUE(run_id, node_id, attempt_count)``
    index makes the handler idempotent — a retried tick that ran the
    INSERT but crashed before COMMIT will see ``rowcount == 0`` on the
    second attempt and silently advance.

Per Phase 0a pivot (docs/phase_0a_decision.md):
  * NO ``tag_group`` is set on the row or passed to CT. Disposition
    wakeup uses (customer_id, fired_at_ist) triangulation; ``tag_group``
    is not a column in upstream ``collection_comment_data``.

cohort_name format:
    ``f"workflow:{workflow_id}:v{version_id}"`` — lets the workflow_scheduler
    + digest distinguish workflow rows from any future cohort source while
    staying free-text (matches existing adhoc convention).
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.fire_vb_call")

_IST = ZoneInfo("Asia/Kolkata")


def _now_ist_str() -> str:
    """Return ``YYYY-MM-DD HH:MM:SS`` IST-naive (matches event-ts canon)."""
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: sqlite3.Connection,
    dry_run: bool = False,
) -> NodeResult:
    """INSERT OR IGNORE one row into ``wf_pending_actions``.

    The ``txn`` argument is the executor's open ``workflow.db`` connection,
    already inside a ``BEGIN IMMEDIATE`` transaction (see
    ``workflow.wf_store.transaction``). This handler issues ONE statement
    on it and does NOT commit — the executor does that.

    Returns ``next_edge="queued"`` whether the INSERT actually wrote a new
    row or hit the UNIQUE constraint (idempotent advance per the architecture
    Dedupe Key rule).
    """
    # WS6 3-key split + cutover safety. The dedupe key is
    # UNIQUE(run_id, node_id, attempt_count); same-day reattempts re-enter the
    # SAME FIRE node, so attempt_count MUST differ per fire or the second fire
    # silently dedupe-collides (INSERT OR IGNORE → no row → no call).
    #   * v8 runs are enrolment-seeded with `fire_seq` (a globally-monotonic
    #     per-run fire counter). FIRE reads it as attempt_count and self-bumps it,
    #     so every fire (first-of-day OR same-day retry) gets a unique count.
    #   * in-flight v7 runs (drain after the v8 cutover) have NO `fire_seq`; they
    #     keyed on `attempts` (bumped once/day by the old COUNTER). Fall back to
    #     `attempts` and do NOT introduce fire_seq, so a v7 run keeps its original
    #     keying and can't collide with its own earlier fires.
    if "fire_seq" in run.scratchpad:
        attempt_count = int(run.scratchpad.get("fire_seq", 0) or 0)
        fire_seq_patch = {"fire_seq": attempt_count + 1}
    else:
        attempt_count = int(run.scratchpad.get("attempts", 0) or 0)
        fire_seq_patch = {}
    now_ist = _now_ist_str()
    cohort_name = f"workflow:{run.workflow_id}:v{run.version_id}"

    if dry_run:
        # Dry-run contract: log intended write to workflow_node_log
        # (executor handles the log row); do NOT INSERT.
        return NodeResult(
            next_edge="queued",
            scratchpad_patch={"last_fire_at": now_ist, **fire_seq_patch},
            side_effect=(
                f"DRY-RUN would INSERT wf_pending_actions "
                f"cid={run.customer_id} run_id={run.id} node_id={node.node_id} "
                f"attempt={attempt_count} cohort={cohort_name}"
            ),
            ready_at_ist=None,
        )

    # WS7/2c reserved-bandwidth: stamp priority_class from scratchpad ('reserve'
    # for callback / unfulfilled-PTP/Agree follow-ups set by the WAIT_CB/PTP/EOD
    # branches; 'general' otherwise). The scheduler ranks reserve rows first so a
    # committed promise jumps the queue ahead of general first-attempts. Default
    # 'general' so any unstamped run is treated as a normal fire.
    priority_class = str(run.scratchpad.get("priority_class") or "general")
    if priority_class not in ("reserve", "general"):
        priority_class = "general"

    # scheduled_at_ist = now: the workflow_scheduler picks up PENDING rows
    # whose scheduled_at_ist <= now. Setting it to now means "fire on the
    # next scheduler tick" (typically within 5 minutes).
    cursor = txn.execute(
        """
        INSERT OR IGNORE INTO wf_pending_actions
            (run_id, node_id, attempt_count, customer_id,
             scheduled_at_ist, status, created_at_ist, cohort_name, priority_class)
        VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
        """,
        (
            run.id,
            node.node_id,
            attempt_count,
            run.customer_id,
            now_ist,
            now_ist,
            cohort_name,
            priority_class,
        ),
    )

    if cursor.rowcount == 0:
        # Dedupe hit: an earlier tick already queued this exact
        # (run_id, node_id, attempt_count). Idempotent advance — the
        # scheduler will fire the existing row on its own cadence.
        log.info(
            "wf_pending_actions dedupe-hit run_id=%s node_id=%s attempt=%s cid=%s",
            run.id, node.node_id, attempt_count, run.customer_id,
        )
        side_effect = (
            f"wf_pending_actions dedupe-hit (already queued) cid={run.customer_id} "
            f"run_id={run.id} node_id={node.node_id} attempt={attempt_count}"
        )
    else:
        side_effect = (
            f"wf_pending_actions row queued cid={run.customer_id} "
            f"run_id={run.id} node_id={node.node_id} attempt={attempt_count}"
        )

    return NodeResult(
        next_edge="queued",
        # fire_seq_patch self-bumps the monotonic counter (v8). It rides the
        # executor's atomic advance+commit, so a re-ticked fire (crash before
        # commit) re-reads the un-bumped value, dedupe-hits idempotently, and
        # bumps exactly once — no double-increment.
        scratchpad_patch={"last_fire_at": now_ist, **fire_seq_patch},
        side_effect=side_effect,
        ready_at_ist=None,
    )
