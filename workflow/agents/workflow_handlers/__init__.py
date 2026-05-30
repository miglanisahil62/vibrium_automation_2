"""Workflow node handler registry.

Each handler module exports a single ``execute(node, run, ctx, txn, dry_run)
-> NodeResult`` function. The executor (Phase 5) looks up the function by
node type string via ``REGISTRY[node.type]``.

This file is the single source of truth for "which node types are
implemented." Phase 4a covers the 6 core types needed for the smallest
end-to-end journey:

    ENROLL → FETCH_CT_PROPS → CONDITION → FIRE_VB_CALL → AWAIT_DISPOSITION → TERMINATE

Phase 4b adds SWITCH, WAIT_UNTIL, BRANCH_ON_DISPOSITION, COUNTER.
Phase 4c adds SET_CT_PROP, ASSIGN_AGENT.
"""
from __future__ import annotations

from typing import Callable

from workflow.agents.workflow_handlers.await_disposition import execute as _await_disposition
from workflow.agents.workflow_handlers.condition import execute as _condition
from workflow.agents.workflow_handlers.enroll import execute as _enroll
from workflow.agents.workflow_handlers.fetch_ct_props import execute as _fetch_ct_props
from workflow.agents.workflow_handlers.fire_vb_call import execute as _fire_vb_call
from workflow.agents.workflow_handlers.terminate import execute as _terminate
from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

# Public registry. Keys are the canonical node type strings used in
# graph_json. The validator (Phase 10) rejects any node whose type is not
# a key here.
REGISTRY: dict = {
    "ENROLL": _enroll,
    "FETCH_CT_PROPS": _fetch_ct_props,
    "CONDITION": _condition,
    "FIRE_VB_CALL": _fire_vb_call,
    "AWAIT_DISPOSITION": _await_disposition,
    "TERMINATE": _terminate,
}

__all__ = ["REGISTRY", "NodeConfig", "NodeResult", "Run"]
