"""Server-side graph validator for workflow ``graph_json``.

Phase 10 deliverable. Invoked by the Ops Console v2 ``/api/workflows/{id}/version``
endpoint before persisting a new ``workflow_versions`` row, and re-used by
``scripts/seed_workflow.py`` in Phase 12.

Validation rules (each maps to one or more ``ValidationError.code``):

* ``E_NO_ENROLL`` / ``E_MULTIPLE_ENROLL`` — exactly one ``ENROLL`` node.
* ``E_MISSING_EDGE`` — every non-terminal node has every required edge populated.
* ``E_UNKNOWN_NODE_TYPE`` — node type is not in ``workflow_handlers.REGISTRY``.
* ``E_BAD_UUID`` — node_id is not a parseable UUID.
* ``E_DUPLICATE_NODE_ID`` — node_id appears twice.
* ``E_EDGE_TO_MISSING_NODE`` — an edge points at a node_id not in the graph.
* ``E_CYCLE`` — graph contains a cycle that does NOT pass through a
  ``WAIT_UNTIL`` node.
* ``E_BAD_CONDITION_EXPR`` — ``CONDITION.config["expr"]`` does not parse.
* ``E_UNKNOWN_ACTION_CLASS`` — ``BRANCH_ON_DISPOSITION.config["cases"]``
  contains a key not in ``CANONICAL_ACTION_CLASSES``.
* ``E_MISSING_DEFAULT`` — ``BRANCH_ON_DISPOSITION`` lacks the mandatory
  ``default`` edge.

graph_json shape (matches Phase 4a/4b handler contract):

    {
        "nodes": [
            {
                "node_id": "<uuid>",
                "type": "ENROLL" | "CONDITION" | ...,
                "label": "<free text>",
                "config": { ... type-specific ... },
                "edges": { "<label>": "<target_node_id>", ... }
            },
            ...
        ]
    }

The validator is a pure function: graph_json (dict OR JSON string) in,
``ValidationResult`` out. No I/O. Safe to call from any context.
"""
from __future__ import annotations

import ast
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from workflow.agents.workflow_handlers import REGISTRY as _HANDLER_REGISTRY
from workflow.agents.workflow_handlers.branch_on_disposition import (
    CANONICAL_ACTION_CLASSES,
)

# Try simpleeval for CONDITION.expr parsing; fall back to ast.parse if absent.
try:  # pragma: no cover — import-time fallback
    import simpleeval as _simpleeval
except ImportError:  # pragma: no cover
    _simpleeval = None  # type: ignore[assignment]


# Required outgoing edges per node type. Terminal types map to ``set()`` and
# are exempt from the missing-edge check entirely. Phase 4a/4b/4c handlers
# define these edge labels in their respective module docstrings; the
# canonical source is the handler ``execute()`` return values.
REQUIRED_EDGES: dict[str, set[str]] = {
    # Phase 4a
    "ENROLL": {"next"},
    "FETCH_CT_PROPS": {"success", "error"},
    "CONDITION": {"true", "false"},
    "FIRE_VB_CALL": {"queued"},
    "AWAIT_DISPOSITION": {"timeout"},
    "TERMINATE": set(),  # terminal
    # Phase 4b
    "SWITCH": set(),  # SWITCH validates its own edges dynamically (see below)
    "WAIT_UNTIL": {"next"},
    "BRANCH_ON_DISPOSITION": {"default", "error"},
    "COUNTER": {"under_limit", "at_limit"},
    # Phase 4c
    "SET_CT_PROP": {"success", "error"},
    "ASSIGN_AGENT": {"next"},
}

# Node types that count as terminal for cycle-detection / required-edge skips.
TERMINAL_TYPES: frozenset[str] = frozenset({"TERMINATE"})

# Node types that "break" a cycle (a cycle is fine as long as at least one
# node on it parks the run). Currently only WAIT_UNTIL — AWAIT_DISPOSITION
# also parks but is event-driven and Phase 4b/4c flows commonly cycle through
# it without WAIT_UNTIL, so we accept either.
CYCLE_SAFE_TYPES: frozenset[str] = frozenset({"WAIT_UNTIL", "AWAIT_DISPOSITION"})


@dataclass(frozen=True)
class ValidationError:
    """One validation failure.

    ``node_id`` may be ``None`` for graph-wide errors (e.g. ``E_NO_ENROLL``).
    ``detail`` is a free-text human-readable explanation; the ``code`` is what
    UI / tests should switch on.
    """

    code: str
    message: str
    node_id: Optional[str] = None
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationResult:
    """Aggregate result; ``valid`` is true iff ``errors`` is empty."""

    valid: bool
    errors: list[ValidationError]


def _ensure_dict(graph_json: Any) -> dict:
    if isinstance(graph_json, str):
        try:
            return json.loads(graph_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"graph_json is not valid JSON: {exc}") from exc
    if not isinstance(graph_json, dict):
        raise TypeError(
            f"graph_json must be dict or JSON string, got {type(graph_json).__name__}"
        )
    return graph_json


def _is_valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):  # stashfin-lint: ignore  # validator contract: bad input → False, not raise
        return False
    return True


def _condition_expr_parses(expr: str) -> bool:
    """Best-effort syntactic check that an expression compiles.

    Tries simpleeval.SimpleEval().parse(expr) first because it matches what
    the runtime CONDITION handler actually uses (so e.g. ``a if b else c`` is
    accepted iff simpleeval accepts it). Falls back to ``ast.parse(mode='eval')``
    if simpleeval is not installed in this environment (e.g. tests on a bare
    interpreter).

    A pass here means "parses cleanly" — names don't have to resolve. The
    runtime is responsible for handling unknown names at execution time.
    """
    if not isinstance(expr, str) or not expr.strip():
        return False
    if _simpleeval is not None:
        try:
            _simpleeval.SimpleEval(names={}, functions={}).parse(expr)
            return True
        except SyntaxError:
            return False  # stashfin-lint: ignore  # validator contract: SyntaxError = invalid expression, surface as ValidationError code
        except Exception:  # pragma: no cover — defensive
            # Non-syntax errors here (e.g. simpleeval's own NameNotDefined)
            # mean the expression PARSED but referenced an unknown name —
            # which is fine for validation. We only fail on SyntaxError.
            return True  # stashfin-lint: ignore  # name-resolution errors mean parsed-but-unbound — valid at save time
    try:
        ast.parse(expr, mode="eval")
        return True
    except SyntaxError:
        return False  # stashfin-lint: ignore  # validator contract: SyntaxError = invalid expression, surface as ValidationError code


def _iter_outgoing(node: dict) -> Iterable[tuple[str, str]]:
    edges = node.get("edges") or {}
    if not isinstance(edges, dict):
        return
    for label, target in edges.items():
        if isinstance(target, str) and target:
            yield label, target


def _detect_unsafe_cycles(
    nodes_by_id: dict[str, dict],
) -> list[tuple[str, list[str]]]:
    """Find every cycle that does NOT pass through a CYCLE_SAFE_TYPES node.

    Returns a list of (start_node_id, cycle_path) tuples. ``cycle_path`` is
    the ordered list of node_ids forming the cycle. Order is deterministic
    per insertion order of nodes.

    Implementation: iterative DFS with three-color marking. When we find a
    back-edge (target is GRAY), we walk the current DFS stack to extract the
    cycle nodes, then check whether any of them is in CYCLE_SAFE_TYPES. If
    not, record it.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {nid: WHITE for nid in nodes_by_id}
    unsafe: list[tuple[str, list[str]]] = []

    for root in list(nodes_by_id):
        if color[root] != WHITE:
            continue
        # Iterative DFS: stack of (node_id, iterator-of-outgoing-targets).
        stack: list[tuple[str, Any]] = [(root, iter(_iter_outgoing(nodes_by_id[root])))]
        path: list[str] = [root]
        color[root] = GRAY
        while stack:
            cur_id, it = stack[-1]
            advanced = False
            for _label, target in it:
                if target not in nodes_by_id:
                    # E_EDGE_TO_MISSING_NODE — caught separately. Don't recurse.
                    continue
                tc = color[target]
                if tc == WHITE:
                    color[target] = GRAY
                    path.append(target)
                    stack.append((target, iter(_iter_outgoing(nodes_by_id[target]))))
                    advanced = True
                    break
                if tc == GRAY:
                    # Back-edge → cycle. Extract the cycle slice from path.
                    try:
                        idx = path.index(target)
                    except ValueError:
                        continue
                    cycle_nodes = path[idx:]
                    # A cycle is "safe" iff any node on it parks the run.
                    safe = any(
                        nodes_by_id[nid].get("type") in CYCLE_SAFE_TYPES
                        for nid in cycle_nodes
                    )
                    if not safe:
                        unsafe.append((target, list(cycle_nodes)))
                # BLACK → cross-edge to a fully-explored subtree. No cycle.
            if not advanced:
                color[cur_id] = BLACK
                stack.pop()
                if path and path[-1] == cur_id:
                    path.pop()
    return unsafe


def validate_graph(graph_json: Any) -> ValidationResult:
    """Validate a workflow ``graph_json`` payload.

    Args:
        graph_json: Either a dict matching the documented shape, or a JSON
            string that will be parsed first.

    Returns:
        ValidationResult with all errors accumulated. The validator does NOT
        short-circuit on the first error — callers want the full list so the
        UI can highlight every problem at once.

    Raises:
        ValueError if ``graph_json`` is a string that doesn't parse as JSON.
        TypeError if ``graph_json`` is not a dict or string.
    """
    graph = _ensure_dict(graph_json)
    errors: list[ValidationError] = []

    raw_nodes = graph.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        errors.append(ValidationError(
            code="E_NO_NODES",
            message="graph_json.nodes must be a non-empty list",
        ))
        return ValidationResult(valid=False, errors=errors)

    # Pass 1: index nodes by id, catch duplicates + bad UUIDs + unknown types.
    nodes_by_id: dict[str, dict] = {}
    enroll_node_ids: list[str] = []
    for idx, node in enumerate(raw_nodes):
        if not isinstance(node, dict):
            errors.append(ValidationError(
                code="E_BAD_NODE",
                message=f"nodes[{idx}] is not an object",
            ))
            continue
        node_id = node.get("node_id")
        node_type = node.get("type")

        if not _is_valid_uuid(node_id):
            errors.append(ValidationError(
                code="E_BAD_UUID",
                message=f"node_id {node_id!r} is not a valid UUID",
                node_id=node_id if isinstance(node_id, str) else None,
            ))
            # We still index by whatever the id is so downstream checks don't
            # blow up. If the id isn't even a string, skip indexing.
            if not isinstance(node_id, str):
                continue

        if node_id in nodes_by_id:
            errors.append(ValidationError(
                code="E_DUPLICATE_NODE_ID",
                message=f"node_id {node_id!r} appears more than once",
                node_id=node_id,
            ))
            # Keep the first occurrence; subsequent ones are dropped from
            # the index so traversal stays deterministic.
            continue
        nodes_by_id[node_id] = node

        if node_type not in _HANDLER_REGISTRY:
            errors.append(ValidationError(
                code="E_UNKNOWN_NODE_TYPE",
                message=(
                    f"node {node_id!r} has unknown type {node_type!r} "
                    "(not in workflow_handlers.REGISTRY)"
                ),
                node_id=node_id,
                detail={"type": node_type},
            ))

        if node_type == "ENROLL":
            enroll_node_ids.append(node_id)

    # Pass 2: ENROLL cardinality.
    if len(enroll_node_ids) == 0:
        errors.append(ValidationError(
            code="E_NO_ENROLL",
            message="graph must contain exactly one ENROLL node (found 0)",
        ))
    elif len(enroll_node_ids) > 1:
        errors.append(ValidationError(
            code="E_MULTIPLE_ENROLL",
            message=(
                f"graph must contain exactly one ENROLL node "
                f"(found {len(enroll_node_ids)}: {enroll_node_ids})"
            ),
            detail={"node_ids": enroll_node_ids},
        ))

    # Pass 3: per-node edge + config checks.
    for node_id, node in nodes_by_id.items():
        node_type = node.get("type")
        edges = node.get("edges") or {}
        if not isinstance(edges, dict):
            edges = {}

        # 3a: required edges populated (non-empty target).
        if node_type in REQUIRED_EDGES and node_type not in TERMINAL_TYPES:
            required = REQUIRED_EDGES[node_type]
            for label in required:
                target = edges.get(label)
                if not isinstance(target, str) or not target.strip():
                    errors.append(ValidationError(
                        code="E_MISSING_EDGE",
                        message=(
                            f"{node_type} node {node_id!r} is missing required "
                            f"edge {label!r}"
                        ),
                        node_id=node_id,
                        detail={"edge": label, "type": node_type},
                    ))

        # 3b: SWITCH is dynamic — its edges are derived from config["cases"].
        # We at least require one case + a default.
        if node_type == "SWITCH":
            cases = (node.get("config") or {}).get("cases") or {}
            if not isinstance(cases, dict) or not cases:
                errors.append(ValidationError(
                    code="E_MISSING_EDGE",
                    message=(
                        f"SWITCH node {node_id!r} must define at least one "
                        "case in config.cases"
                    ),
                    node_id=node_id,
                ))
            else:
                for case_label, target in cases.items():
                    if not isinstance(target, str) or not target.strip():
                        errors.append(ValidationError(
                            code="E_MISSING_EDGE",
                            message=(
                                f"SWITCH node {node_id!r} case "
                                f"{case_label!r} has no target"
                            ),
                            node_id=node_id,
                            detail={"case": case_label},
                        ))
            default_edge = edges.get("default") or (node.get("config") or {}).get("default")
            if not isinstance(default_edge, str) or not default_edge.strip():
                errors.append(ValidationError(
                    code="E_MISSING_EDGE",
                    message=f"SWITCH node {node_id!r} missing default edge",
                    node_id=node_id,
                    detail={"edge": "default"},
                ))

        # 3c: every edge target must exist in the graph.
        for label, target in edges.items():
            if not isinstance(target, str) or not target.strip():
                continue
            if target not in nodes_by_id:
                errors.append(ValidationError(
                    code="E_EDGE_TO_MISSING_NODE",
                    message=(
                        f"node {node_id!r} edge {label!r} points at "
                        f"missing node {target!r}"
                    ),
                    node_id=node_id,
                    detail={"edge": label, "target": target},
                ))

        # 3d: CONDITION.expr parses.
        if node_type == "CONDITION":
            expr = (node.get("config") or {}).get("expr")
            if not isinstance(expr, str) or not expr.strip():
                errors.append(ValidationError(
                    code="E_BAD_CONDITION_EXPR",
                    message=(
                        f"CONDITION node {node_id!r} missing config.expr"
                    ),
                    node_id=node_id,
                ))
            elif not _condition_expr_parses(expr):
                errors.append(ValidationError(
                    code="E_BAD_CONDITION_EXPR",
                    message=(
                        f"CONDITION node {node_id!r} expression does not "
                        f"parse: {expr!r}"
                    ),
                    node_id=node_id,
                    detail={"expr": expr},
                ))

        # 3e: BRANCH_ON_DISPOSITION cases + default.
        if node_type == "BRANCH_ON_DISPOSITION":
            config = node.get("config") or {}
            cases = config.get("cases") or {}
            if not isinstance(cases, dict):
                cases = {}
            for action_class in cases:
                if action_class not in CANONICAL_ACTION_CLASSES:
                    errors.append(ValidationError(
                        code="E_UNKNOWN_ACTION_CLASS",
                        message=(
                            f"BRANCH_ON_DISPOSITION node {node_id!r} has "
                            f"non-canonical action_class {action_class!r} "
                            f"(allowed: {sorted(CANONICAL_ACTION_CLASSES)})"
                        ),
                        node_id=node_id,
                        detail={"action_class": action_class},
                    ))
            # mandatory default edge — the handler also requires this at
            # execute() time, but the save-time validator is the official
            # enforcement point per branch_on_disposition.py docstring.
            default_edge = edges.get("default") or config.get("default")
            if not isinstance(default_edge, str) or not default_edge.strip():
                errors.append(ValidationError(
                    code="E_MISSING_DEFAULT",
                    message=(
                        f"BRANCH_ON_DISPOSITION node {node_id!r} is missing "
                        "mandatory 'default' edge"
                    ),
                    node_id=node_id,
                ))

    # Pass 4: cycle check. Only meaningful if the graph indexed cleanly enough
    # to traverse — we still run it; missing-edge targets are skipped by
    # _iter_outgoing's guards.
    if nodes_by_id:
        unsafe_cycles = _detect_unsafe_cycles(nodes_by_id)
        for entry_node, cycle in unsafe_cycles:
            errors.append(ValidationError(
                code="E_CYCLE",
                message=(
                    f"unsafe cycle detected (does not pass through "
                    f"WAIT_UNTIL / AWAIT_DISPOSITION): {' → '.join(cycle)}"
                ),
                node_id=entry_node,
                detail={"cycle": cycle},
            ))

    return ValidationResult(valid=not errors, errors=errors)


__all__ = [
    "CANONICAL_ACTION_CLASSES",
    "REQUIRED_EDGES",
    "TERMINAL_TYPES",
    "CYCLE_SAFE_TYPES",
    "ValidationError",
    "ValidationResult",
    "validate_graph",
]
