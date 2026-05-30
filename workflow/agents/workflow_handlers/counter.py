"""COUNTER handler — increment named scratchpad counter; branch on limit.

Node config shape:
    {"name": "attempts", "limit": 2}

Behavior:
  * Reads ``run.scratchpad.get(name, 0)`` → call it ``current``.
  * Increments by 1 → ``current + 1`` is written to scratchpad.
  * If ``current + 1 < limit``: edge ``under_limit``.
  * If ``current + 1 >= limit``: edge ``at_limit``.

No reset behavior — once at limit, stays at limit unless another node
explicitly writes the counter back down (e.g. a separate SET_SCRATCHPAD node,
not in this phase).

Type robustness:
  * Non-integer existing value is coerced via ``int(value)``. A
    ``TypeError``/``ValueError`` routes to ``error`` edge with
    ``counter_error="non_integer_current"``.
  * Non-integer ``limit`` in config → ``error`` edge with
    ``counter_error="bad_limit"``.

Edges: ``under_limit``, ``at_limit``, ``error``.
"""
from __future__ import annotations

from typing import Any

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run


def _error(msg: str, side: str) -> NodeResult:
    return NodeResult(
        next_edge="error",
        scratchpad_patch={"counter_error": msg},
        side_effect=side,
        ready_at_ist=None,
    )


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Increment and branch."""
    name = node.config.get("name")
    if not isinstance(name, str) or not name:
        return _error(
            "missing or empty 'name' config",
            "COUNTER misconfigured: no name",
        )

    raw_limit = node.config.get("limit")
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: bad config routes to 'error' edge with counter_error='bad_limit', not a silent default.
        return _error(
            "bad_limit",
            f"COUNTER bad limit {raw_limit!r}",
        )
    if limit <= 0:
        return _error(
            "bad_limit",
            f"COUNTER limit must be > 0 (got {limit})",
        )

    raw_current = run.scratchpad.get(name, 0)
    try:
        current = int(raw_current)
    except (TypeError, ValueError):  # stashfin-lint: ignore  # documented contract: non-integer scratchpad value routes to 'error' edge with counter_error='non_integer_current', not a silent default.
        return _error(
            "non_integer_current",
            f"COUNTER existing value {raw_current!r} not int-coercible",
        )

    incremented = current + 1
    edge = "at_limit" if incremented >= limit else "under_limit"

    return NodeResult(
        next_edge=edge,
        scratchpad_patch={name: incremented},
        side_effect=(
            f"COUNTER {name} {current} → {incremented} "
            f"(limit={limit}) → {edge}"
        ),
        ready_at_ist=None,
    )
