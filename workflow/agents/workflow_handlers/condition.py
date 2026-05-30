"""CONDITION handler — boolean expression over scratchpad via simpleeval.

Node config shape:
    {
        "expr": "dpd > 1 and coll_collection_risk_segmentation < 5"
    }

Hard rules (architecture rev 3 §"CONDITION DSL"):
  * Names-only access — ``functions={}``. No ``int()``, no ``len()``, nothing
    callable. If the expression references a function it MUST route to the
    ``error`` edge with a clear message.
  * ``MAX_STRING_LENGTH = 1024`` and ``MAX_POWER = 100`` are simpleeval
    module-level constants; we set them at import time. They cap a class of
    DoS expressions (e.g. ``'x' * 10**9``, ``2 ** 10**6``).
  * Bad expressions NEVER raise to the executor. The handler catches
    ``InvalidExpression`` (which covers FunctionNotDefined, NameNotDefined,
    AttributeDoesNotExist, etc.) plus the bare-Python ``SyntaxError`` that
    simpleeval lets through, plus ``TypeError`` / ``ZeroDivisionError`` from
    runtime evaluation of an otherwise-valid expression.
  * The result MUST coerce to bool; anything truthy → "true" edge, falsy →
    "false" edge. Non-bool truthiness is tolerated (Python convention) but
    will be caught at save-time validation in Phase 10 if desired.

Edge labels: ``true``, ``false``, ``error``.

NB: simpleeval 0.9.13's exception class for "name not defined" is
``NameNotDefined`` and for "function not defined" is ``FunctionNotDefined``;
both inherit from ``InvalidExpression``. We catch the parent.
"""
from __future__ import annotations

from typing import Any

import simpleeval
from simpleeval import InvalidExpression, SimpleEval

from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run


# Pin the safety caps. simpleeval reads these at evaluation time from the
# module namespace, so setting them once at import covers every SimpleEval
# instance we ever construct.
simpleeval.MAX_STRING_LENGTH = 1024
simpleeval.MAX_POWER = 100

# Exceptions that map to the ``error`` edge. Listed explicitly (no bare
# ``Exception``) so we don't swallow a programming bug in the handler itself.
# Order matters only for documentation — all are subclasses of Exception.
_HANDLED_EXCEPTIONS: tuple = (
    InvalidExpression,    # parent of FunctionNotDefined, NameNotDefined, AttributeDoesNotExist, FeatureNotAvailable
    SyntaxError,          # simpleeval propagates ast.parse SyntaxError bare
    TypeError,            # e.g. comparing None < 5
    ValueError,           # e.g. int() of "abc" (shouldn't occur with functions={}; defense in depth)
    ZeroDivisionError,    # e.g. "x / 0"
)


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Evaluate ``node.config['expr']`` against ``run.scratchpad``."""
    expr = node.config.get("expr")
    if not isinstance(expr, str) or not expr.strip():
        return NodeResult(
            next_edge="error",
            scratchpad_patch={
                "condition_error": "missing or empty 'expr' config",
            },
            side_effect="CONDITION misconfigured: no expr",
            ready_at_ist=None,
        )

    # Build a fresh evaluator per call. Cheap; avoids any cross-run state
    # leakage. ``functions={}`` is the load-bearing safety guarantee —
    # rejects ``int(x)``, ``len(...)``, custom calls, anything callable.
    evaluator = SimpleEval(names=run.scratchpad, functions={})

    try:
        result = evaluator.eval(expr)
    except _HANDLED_EXCEPTIONS as exc:  # stashfin-lint: ignore  # documented contract per architecture rev 3 §CONDITION DSL: bad exprs route to 'error' edge; never raise to executor.

        # Documented contract: bad expressions route to error edge with the
        # message captured for the audit log. Do NOT raise to executor.
        return NodeResult(
            next_edge="error",
            scratchpad_patch={
                "condition_error": f"{type(exc).__name__}: {exc}",
            },
            side_effect=f"CONDITION eval error: {type(exc).__name__}",
            ready_at_ist=None,
        )

    edge = "true" if bool(result) else "false"
    return NodeResult(
        next_edge=edge,
        scratchpad_patch={},
        # Compact audit string — no scratchpad values (could be PII-adjacent).
        side_effect=f"CONDITION {edge}",
        ready_at_ist=None,
    )
