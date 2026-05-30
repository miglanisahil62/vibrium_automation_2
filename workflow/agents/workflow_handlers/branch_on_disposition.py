"""BRANCH_ON_DISPOSITION handler — multi-way branch on disposition action_class.

Reads ``run.scratchpad["last_disposition_action_class"]`` (written by Phase 7's
``workflow_ingest.py`` when a disposition wakeup matches the run). Branches by
looking the value up in ``node.config["cases"]`` and following the resolved
edge label.

Canonical action_class enum (Phase 0a-aligned with vibrium-automation/scripts/
ingest.py's ``decision_v2.classify`` output):

    NOOP, RETRY, PTP_CALL, AGREE_EOD_CALL, CALLBACK_CALL, RTP_NEEDS_LLM, ESCALATE

Node config shape:
    {
        "cases": {
            "NOOP": "to_terminate",
            "RETRY": "to_counter",
            "PTP_CALL": "to_ptp_wait",
            "AGREE_EOD_CALL": "to_eod_wait",
            "CALLBACK_CALL": "to_callback_wait",
            "RTP_NEEDS_LLM": "to_llm",
            "ESCALATE": "to_assign_agent"
        },
        "default": "to_terminate"
    }

Behavior:
  * Missing scratchpad key → ``error`` edge with
    ``branch_error="no_disposition"``.
  * action_class present but not in ``cases`` → fall through to ``default``
    edge (NOT an error). Unknown enum values from a future ingest revision
    won't break in-flight runs — they'll route via default.
  * Missing ``default`` AND missing case → ``error`` edge. (The save-time
    validator in Phase 10 enforces ``default`` is present.)

After a successful branch (case match OR default), the handler CLEARS
``last_disposition_action_class`` from scratchpad by setting it to ``None`` in
``scratchpad_patch``. This is the "BRANCH_ON_DISPOSITION's job in Phase 4b"
flagged by Phase 4a's auditor: prevents a subsequent revisit (e.g. via a loop
back to AWAIT_DISPOSITION) from picking up stale disposition data.

NB on the patch shape: the executor merges ``scratchpad_patch`` shallow-style.
Setting the value to ``None`` is the documented signal to clear; downstream
``await_disposition.py`` checks ``scratchpad.get("last_disposition_action_class")``
which is falsy for ``None``, so cleared = absent for routing purposes.
"""
from __future__ import annotations

from typing import Any

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

# Canonical action_class enum. Mirrors the values produced by
# vibrium-automation/scripts/decision_v2.classify(). The Phase 10 save-time
# validator must reject any BRANCH_ON_DISPOSITION node whose ``cases`` dict
# contains a key not in this set.
CANONICAL_ACTION_CLASSES: frozenset = frozenset({
    "NOOP",
    "RETRY",
    "PTP_CALL",
    "AGREE_EOD_CALL",
    "CALLBACK_CALL",
    "RTP_NEEDS_LLM",
    "ESCALATE",
})


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Branch by disposition action_class; clear the scratchpad key on exit."""
    action_class = run.scratchpad.get("last_disposition_action_class")

    if not action_class:
        # No disposition recorded. This is a routing error: the upstream
        # AWAIT_DISPOSITION should have parked the run if no disposition,
        # and ingest should have set this key before waking it. Surface to
        # the audit log via ``branch_error``.
        return NodeResult(
            next_edge="error",
            scratchpad_patch={"branch_error": "no_disposition"},
            side_effect="BRANCH_ON_DISPOSITION no disposition in scratchpad",
            ready_at_ist=None,
        )

    cases = node.config.get("cases") or {}
    default_edge = node.config.get("default")

    edge = cases.get(action_class)
    routed_via = "case"
    if edge is None:
        if default_edge is None:
            # No case + no default → error. Do NOT clear the scratchpad key
            # here — we want it preserved for a manual repair operation
            # (Phase 10's repair API can re-route the run after the operator
            # adds the missing edge).
            return NodeResult(
                next_edge="error",
                scratchpad_patch={
                    "branch_error": (
                        f"no case for action_class={action_class!r} "
                        "and no default edge"
                    ),
                },
                side_effect=(
                    f"BRANCH_ON_DISPOSITION action_class={action_class} "
                    "no case + no default"
                ),
                ready_at_ist=None,
            )
        edge = default_edge
        routed_via = "default"

    # On successful branch (case match OR default), clear the scratchpad key
    # so a future revisit of this node doesn't pick up stale data. The
    # executor merges scratchpad_patch shallow-style; None signals clear.
    return NodeResult(
        next_edge=edge,
        scratchpad_patch={"last_disposition_action_class": None},
        side_effect=(
            f"BRANCH_ON_DISPOSITION action_class={action_class} "
            f"→ {edge} ({routed_via})"
        ),
        ready_at_ist=None,
    )
