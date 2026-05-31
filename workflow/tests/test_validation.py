"""Tests for ``workflow.validation.validate_graph``.

One test per rule (valid + invalid case where applicable). The fixture
``_uid()`` generates fresh UUIDs so tests don't share state.
"""
from __future__ import annotations

import uuid

import pytest

from workflow.validation import (
    CANONICAL_ACTION_CLASSES,
    ValidationResult,
    validate_graph,
)


def _uid() -> str:
    return str(uuid.uuid4())


def _enroll(node_id: str, success: str) -> dict:
    return {
        "node_id": node_id,
        "type": "ENROLL",
        "label": "enroll",
        "config": {},
        "edges": {"next": success},
    }


def _terminate(node_id: str) -> dict:
    return {
        "node_id": node_id,
        "type": "TERMINATE",
        "label": "term",
        "config": {"reason": "done"},
        "edges": {},
    }


def _condition(node_id: str, t: str, f: str, expr: str = "scratchpad.get('x') > 1") -> dict:
    return {
        "node_id": node_id,
        "type": "CONDITION",
        "label": "cond",
        "config": {"expr": expr},
        "edges": {"true": t, "false": f},
    }


def _fire(node_id: str, queued: str) -> dict:
    return {
        "node_id": node_id,
        "type": "FIRE_VB_CALL",
        "label": "fire",
        "config": {"campaign": "vb_test"},
        "edges": {"queued": queued},
    }


def _await(node_id: str, timeout: str) -> dict:
    return {
        "node_id": node_id,
        "type": "AWAIT_DISPOSITION",
        "label": "await",
        "config": {"timeout_hours": 24},
        "edges": {"timeout": timeout},
    }


def _wait_until(node_id: str, ready: str) -> dict:
    return {
        "node_id": node_id,
        "type": "WAIT_UNTIL",
        "label": "wait",
        "config": {"hours": 1},
        "edges": {"next": ready},
    }


def _bod(node_id: str, default: str, cases: dict, error: str) -> dict:
    return {
        "node_id": node_id,
        "type": "BRANCH_ON_DISPOSITION",
        "label": "bod",
        "config": {"cases": cases},
        "edges": {"default": default, "error": error},
    }


def _minimal_valid_graph() -> dict:
    """ENROLL → FIRE_VB_CALL → AWAIT_DISPOSITION → TERMINATE — happy path."""
    e, f, a, t = _uid(), _uid(), _uid(), _uid()
    return {
        "nodes": [
            _enroll(e, f),
            _fire(f, a),
            _await(a, t),
            _terminate(t),
        ]
    }


# ─── 1. exactly one ENROLL ────────────────────────────────────────────────
def test_valid_minimal_graph_passes():
    res = validate_graph(_minimal_valid_graph())
    assert isinstance(res, ValidationResult)
    assert res.valid, f"expected valid, got errors: {res.errors}"


def test_no_enroll_fails():
    e_unused = _uid()  # noqa: F841 — just to mirror structure
    t = _uid()
    graph = {"nodes": [_terminate(t)]}
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_NO_ENROLL" for err in res.errors)


def test_multiple_enroll_fails():
    e1, e2, t = _uid(), _uid(), _uid()
    graph = {
        "nodes": [
            _enroll(e1, t),
            _enroll(e2, t),
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_MULTIPLE_ENROLL" for err in res.errors)


# ─── 2. required edges populated ──────────────────────────────────────────
def test_missing_required_edge_fails():
    e, f, a, t = _uid(), _uid(), _uid(), _uid()
    fire = _fire(f, a)
    fire["edges"] = {}  # strip the required "queued" edge
    graph = {
        "nodes": [
            _enroll(e, f),
            fire,
            _await(a, t),
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(
        err.code == "E_MISSING_EDGE" and err.detail.get("edge") == "queued"
        for err in res.errors
    )


# ─── 3. cycle detection ───────────────────────────────────────────────────
def test_unsafe_cycle_fails():
    # ENROLL → CONDITION ⇄ (cycles back to CONDITION on true)
    e, c, t = _uid(), _uid(), _uid()
    cond = _condition(c, c, t, expr="True")  # true edge loops back to self
    graph = {
        "nodes": [
            _enroll(e, c),
            cond,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_CYCLE" for err in res.errors)


def test_safe_cycle_through_wait_until_passes():
    # ENROLL → COND ─true→ WAIT_UNTIL ─ready→ COND ... cycle is safe
    e, c, w, t = _uid(), _uid(), _uid(), _uid()
    graph = {
        "nodes": [
            _enroll(e, c),
            _condition(c, w, t, expr="True"),  # true → wait
            _wait_until(w, c),  # ready → back to cond → cycle through WAIT_UNTIL
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    # No E_CYCLE in errors — but there may be E_BAD_CONDITION_EXPR if anything
    # else slipped. Be specific.
    assert not any(err.code == "E_CYCLE" for err in res.errors), res.errors
    assert res.valid, f"expected valid, got: {res.errors}"


# ─── 4. CONDITION.expr parses ─────────────────────────────────────────────
def test_bad_condition_expr_fails():
    e, c, t = _uid(), _uid(), _uid()
    bad_cond = _condition(c, t, t, expr="this is not (valid python")
    graph = {
        "nodes": [
            _enroll(e, c),
            bad_cond,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_BAD_CONDITION_EXPR" for err in res.errors)


# ─── 5. BRANCH_ON_DISPOSITION canonical cases + default ──────────────────
def test_branch_on_disposition_unknown_action_class_fails():
    e, b, t = _uid(), _uid(), _uid()
    bod = _bod(
        b,
        default=t,
        cases={"NOOP": t, "WAT_IS_DIS": t},  # WAT_IS_DIS not canonical
        error=t,
    )
    graph = {
        "nodes": [
            _enroll(e, b),
            bod,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(
        err.code == "E_UNKNOWN_ACTION_CLASS"
        and err.detail.get("action_class") == "WAT_IS_DIS"
        for err in res.errors
    )


def test_branch_on_disposition_missing_default_fails():
    e, b, t = _uid(), _uid(), _uid()
    bod = _bod(b, default=t, cases={"NOOP": t}, error=t)
    # Strip the default edge
    bod["edges"].pop("default")
    bod["config"].pop("default", None)
    graph = {
        "nodes": [
            _enroll(e, b),
            bod,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_MISSING_DEFAULT" for err in res.errors)


def test_branch_on_disposition_all_canonical_passes():
    """All canonical action classes accepted with default + error edges."""
    e, b, t = _uid(), _uid(), _uid()
    cases = {ac: t for ac in CANONICAL_ACTION_CLASSES}
    bod = _bod(b, default=t, cases=cases, error=t)
    graph = {
        "nodes": [
            _enroll(e, b),
            bod,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert res.valid, f"expected valid, got: {res.errors}"


# ─── 6. UUID validation ───────────────────────────────────────────────────
def test_bad_uuid_fails():
    bad = "not-a-uuid"
    t = _uid()
    graph = {
        "nodes": [
            _enroll(bad, t),
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_BAD_UUID" for err in res.errors)


# ─── 7. edge to missing node ──────────────────────────────────────────────
def test_edge_to_missing_node_fails():
    e, t = _uid(), _uid()
    missing = _uid()
    enroll_node = _enroll(e, missing)  # success points at a non-existent node
    graph = {
        "nodes": [
            enroll_node,
            _terminate(t),
        ]
    }
    res = validate_graph(graph)
    assert not res.valid
    assert any(err.code == "E_EDGE_TO_MISSING_NODE" for err in res.errors)
