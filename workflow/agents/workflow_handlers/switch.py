"""SWITCH handler — multi-way branch on a single scratchpad value.

Node config shape:
    {
        "on": "risk_segmentation",
        "cases": {
            "1": "high", "2": "high", "3": "high", "4": "high",
            "5": "mid", "6": "mid", "7": "mid"
        },
        "default": "low_risk_todo"
    }

Design choices:
  * ``cases`` keys are STRINGS (JSON-safe, lossless round-trip via
    ``json.loads``/``json.dumps`` in graph_json). The handler coerces the
    looked-up scratchpad value to ``str(...)`` before lookup, so a numeric
    scratchpad value (e.g. ``risk_segmentation = 3``) matches a string case
    key (``"3"``).
  * ``default`` is the edge label returned when the value is not in
    ``cases``. Missing ``default`` raises (validator job — Phase 10 — to
    enforce its presence at save-time).
  * If ``node.config["on"]`` is missing from scratchpad → routes to
    ``error`` edge with ``switch_error="missing key: <name>"`` recorded in
    scratchpad. This mirrors the CONDITION handler's error-edge pattern so
    the validator can statically check edge wiring.

Edges: any ``case`` value, any ``default`` value, plus mandatory ``error``.
"""
from __future__ import annotations

from typing import Any

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Look up ``run.scratchpad[node.config['on']]`` in ``cases``."""
    on_key = node.config.get("on")
    if not isinstance(on_key, str) or not on_key:
        return NodeResult(
            next_edge="error",
            scratchpad_patch={"switch_error": "missing or empty 'on' config"},
            side_effect="SWITCH misconfigured: no 'on' key",
            ready_at_ist=None,
        )

    cases = node.config.get("cases") or {}
    default_edge = node.config.get("default")

    if on_key not in run.scratchpad:
        return NodeResult(
            next_edge="error",
            scratchpad_patch={"switch_error": f"missing key: {on_key}"},
            side_effect=f"SWITCH missing scratchpad key {on_key!r}",
            ready_at_ist=None,
        )

    # Coerce value to str so numeric scratchpad values match string case
    # keys. ``str(True)`` is ``'True'`` — note for graph authors.
    raw_value = run.scratchpad[on_key]
    lookup_key = str(raw_value)

    edge = cases.get(lookup_key)
    if edge is None:
        if default_edge is None:
            return NodeResult(
                next_edge="error",
                scratchpad_patch={
                    "switch_error": (
                        f"no case for {lookup_key!r} and no default edge"
                    ),
                },
                side_effect=(
                    f"SWITCH on={on_key} value={lookup_key!r} no case + no default"
                ),
                ready_at_ist=None,
            )
        edge = default_edge
        return NodeResult(
            next_edge=edge,
            scratchpad_patch={},
            side_effect=f"SWITCH on={on_key} value={lookup_key!r} → default={edge}",
            ready_at_ist=None,
        )

    return NodeResult(
        next_edge=edge,
        scratchpad_patch={},
        side_effect=f"SWITCH on={on_key} value={lookup_key!r} → {edge}",
        ready_at_ist=None,
    )
