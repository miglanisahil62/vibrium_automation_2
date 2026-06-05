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

from workflow.agents.workflow_handlers.assign_agent import execute as _assign_agent
from workflow.agents.workflow_handlers.await_disposition import execute as _await_disposition
from workflow.agents.workflow_handlers.branch_on_disposition import execute as _branch_on_disposition
from workflow.agents.workflow_handlers.condition import execute as _condition
from workflow.agents.workflow_handlers.counter import execute as _counter
from workflow.agents.workflow_handlers.enroll import execute as _enroll
from workflow.agents.workflow_handlers.fetch_ct_props import execute as _fetch_ct_props
from workflow.agents.workflow_handlers.fire_vb_call import execute as _fire_vb_call
from workflow.agents.workflow_handlers.same_day_gate import execute as _same_day_gate
from workflow.agents.workflow_handlers.set_ct_prop import execute as _set_ct_prop
from workflow.agents.workflow_handlers.switch import execute as _switch
from workflow.agents.workflow_handlers.terminate import execute as _terminate
from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run
from workflow.agents.workflow_handlers.wait_until import execute as _wait_until

# Public registry. Keys are the canonical node type strings used in
# graph_json. The validator (Phase 10) rejects any node whose type is not
# a key here.
REGISTRY: dict = {
    # Phase 4a — 6 core handlers (the minimal end-to-end journey)
    "ENROLL": _enroll,
    "FETCH_CT_PROPS": _fetch_ct_props,
    "CONDITION": _condition,
    "FIRE_VB_CALL": _fire_vb_call,
    "AWAIT_DISPOSITION": _await_disposition,
    "TERMINATE": _terminate,
    # Phase 4b — 4 branching + scheduling handlers
    "SWITCH": _switch,
    "WAIT_UNTIL": _wait_until,
    "BRANCH_ON_DISPOSITION": _branch_on_disposition,
    "COUNTER": _counter,
    # WS3 same-day-retry decision node (no-connect → retry today vs next day)
    "SAME_DAY_GATE": _same_day_gate,
    # Phase 4c — 2 side-effect handlers
    "SET_CT_PROP": _set_ct_prop,
    "ASSIGN_AGENT": _assign_agent,
}

__all__ = ["REGISTRY", "NodeConfig", "NodeResult", "Run"]
