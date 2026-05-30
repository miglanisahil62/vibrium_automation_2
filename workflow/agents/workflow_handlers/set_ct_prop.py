"""SET_CT_PROP handler — write CT user properties via Phase 2 ``set_profile``.

Critical-surface: this handler writes to CleverTap. It is the only path by
which the workflow engine can mutate CT profile state.

Node config shape:
    {
        "properties": {
            "coll_workflow_state": "in_progress",
            "coll_last_journey_node": "fire_vb_call_1"
        }
    }

Routing:
  * Success (CT 2xx + identity NOT in unprocessed[]):
        next_edge="success", scratchpad_patch={"last_ct_prop_set_at": now_ist}.
  * Failure (CT returns success=False, e.g. unprocessed-hit or top-level fail):
        next_edge="error", scratchpad_patch={"set_ct_prop_error_code": <code>}.
        Does NOT raise — handler trapping is the executor contract.
  * Forbidden property guard (``coll_bot_calling`` in any key):
        next_edge="error",
        scratchpad_patch={"set_ct_prop_error": "forbidden_property: ..."}.
        See Phase 0a invariant below.

Phase 0a invariant (docs/phase_0a_decision.md §1):
    ``coll_bot_calling`` is set by an upstream system and READ ONLY by the
    engine. The engine polls CT for transitions to its granular values
    (``ai_vb_calling_highv1`` etc.) and enrolls customers. Writing it from a
    workflow handler would race the upstream writer and corrupt enrollment
    state. The guard below enforces this at runtime — not just at code-review
    time — so a malformed graph_json cannot violate the invariant even if it
    slips past Phase 10's validator.

Dry-run:
    ``ctx.get("dry_run")`` True → skip the real CT call; side_effect notes
    DRY-RUN; scratchpad still patched with ``last_ct_prop_set_at`` so
    downstream nodes can branch on the would-be timestamp.

Edges expected from graph_json: ``success``, ``error``.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from workflow import clevertap_profile
from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.handlers.set_ct_prop")

_IST = ZoneInfo("Asia/Kolkata")

# Properties the engine MUST NOT write — read-only per Phase 0a. Kept as a
# frozenset so a future contributor can extend it without touching execute().
_FORBIDDEN_PROPERTIES: frozenset[str] = frozenset({"coll_bot_calling"})


def _now_ist_str() -> str:
    """Return ``YYYY-MM-DD HH:MM:SS`` IST-naive (matches event-ts canon)."""
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Write CT profile properties via ``clevertap_profile.set_profile``.

    ``ctx`` may be None or a dict-like with optional ``dry_run`` flag. Both
    the explicit ``dry_run`` kwarg AND ``ctx["dry_run"]`` are honored — either
    True suppresses the real HTTP call. This mirrors how Phase 5's executor
    will pass dry-run state.

    ``txn`` is unused — SET_CT_PROP writes only to CT, not to the workflow DB.
    Kept in the signature for handler-uniformity (executor calls every handler
    with the same shape).
    """
    properties = node.config.get("properties")
    if not isinstance(properties, dict) or not properties:
        return NodeResult(
            next_edge="error",
            scratchpad_patch={
                "set_ct_prop_error": "missing or empty 'properties' config",
            },
            side_effect="SET_CT_PROP misconfigured: no properties",
            ready_at_ist=None,
        )

    # GUARD — Phase 0a invariant. Enforce at runtime so a malformed graph_json
    # cannot write ``coll_bot_calling`` even if validator missed it. Check
    # BEFORE the HTTP call so we never even attempt the bad write.
    forbidden_hits = sorted(set(properties.keys()) & _FORBIDDEN_PROPERTIES)
    if forbidden_hits:
        msg = (
            f"forbidden_property: {forbidden_hits[0]} "
            f"(engine reads, never writes per Phase 0a)"
        )
        log.warning(
            "SET_CT_PROP refused forbidden write cid=%s props=%s",
            run.customer_id, forbidden_hits,
        )
        return NodeResult(
            next_edge="error",
            scratchpad_patch={"set_ct_prop_error": msg},
            side_effect=f"SET_CT_PROP forbidden cid={run.customer_id} keys={forbidden_hits}",
            ready_at_ist=None,
        )

    # Honor either an explicit dry_run kwarg or a ctx-level dry_run flag.
    ctx_dry_run = False
    if isinstance(ctx, dict):
        ctx_dry_run = bool(ctx.get("dry_run", False))
    effective_dry_run = bool(dry_run) or ctx_dry_run

    now_ist = _now_ist_str()
    keys = list(properties)

    if effective_dry_run:
        log.info(
            "SET_CT_PROP DRY-RUN cid=%s keys=%s", run.customer_id, keys,
        )
        return NodeResult(
            next_edge="success",
            scratchpad_patch={"last_ct_prop_set_at": now_ist},
            side_effect=(
                f"DRY-RUN would set_profile cid={run.customer_id} keys={keys}"
            ),
            ready_at_ist=None,
        )

    # Real call. set_profile raises on HTTP-level catastrophe (e.g. 3x 429
    # exhaustion, 5xx); the executor catches those and marks run.status=ERROR.
    # SetResult.success=False is a per-record / batch rejection — we route
    # to the 'error' edge with the code in scratchpad so the graph author
    # can branch on it.
    result = clevertap_profile.set_profile(
        run.customer_id, properties, dry_run=False,
    )

    if result.success:
        return NodeResult(
            next_edge="success",
            scratchpad_patch={"last_ct_prop_set_at": now_ist},
            side_effect=f"set_profile cid={run.customer_id} keys={keys}",
            ready_at_ist=None,
        )

    return NodeResult(
        next_edge="error",
        scratchpad_patch={"set_ct_prop_error_code": result.error_code},
        side_effect=(
            f"set_profile FAIL cid={run.customer_id} keys={keys} "
            f"code={result.error_code}"
        ),
        ready_at_ist=None,
    )
