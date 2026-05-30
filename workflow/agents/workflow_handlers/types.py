"""Shared dataclasses for workflow node handlers.

Three contracts cross handler boundaries:

* ``NodeResult`` — every handler returns one. Captures the edge to follow,
  scratchpad mutations to apply, a free-text side-effect description for the
  audit log, and an optional ``ready_at_ist`` for park semantics.

* ``Run`` — a mutable wrapper around a ``workflow_runs`` row. Handlers MAY
  mutate ``status``, ``ready_at_ist``, ``terminated_at_ist``, ``terminal_status``
  (terminate.py + await_disposition.py do this). The executor (Phase 5) is the
  one that persists those mutations back to SQLite at transaction commit.

* ``NodeConfig`` — what a parsed node in ``graph_json`` looks like.

These dataclasses contain NO logic — they're typed records. Handler logic lives
in each handler module. The executor (Phase 5) constructs ``Run`` from SQLite
rows and ``NodeConfig`` from the parsed graph_json.

next_edge semantics:
    * Non-None string → executor follows ``node.edges[next_edge]`` to the next
      node.
    * None → either a terminal node (``terminate.py``; executor reads
      ``run.status == 'DONE'``) or a park (``await_disposition.py``; executor
      reads ``run.status == 'WAITING'`` and ``run.ready_at_ist`` for the next
      tick eligibility).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class NodeResult:
    """Return value for every handler's ``execute()``.

    Attributes:
        next_edge: Edge label to follow (e.g. ``"success"``, ``"true"``).
            ``None`` for terminal nodes and parks; see module docstring.
        scratchpad_patch: Keys to merge into the run's scratchpad. Always
            shallow-merge; handlers must NOT mutate the run's existing
            scratchpad in place. Empty dict if no patch.
        side_effect: One-line free-text description of any external write
            (e.g. ``"wf_pending_actions row queued cid=8968249"``). Recorded
            in ``workflow_node_log.side_effect`` for the audit trail.
            ``None`` if no observable side effect.
        ready_at_ist: When set, the executor parks the run with this wake
            time (IST-naive ``YYYY-MM-DD HH:MM:SS``). Used by parking
            handlers (``await_disposition``, eventual ``wait_until``).
            ``None`` for immediate-advance handlers.
    """

    next_edge: Optional[str]
    scratchpad_patch: dict
    side_effect: Optional[str]
    ready_at_ist: Optional[str]


@dataclass
class Run:
    """Mutable in-memory view of one ``workflow_runs`` row.

    Constructed by the executor at the top of each per-run iteration from a
    SELECT. Handlers may mutate the listed fields; the executor persists the
    diff inside the same transaction as the side-effect write.

    ``scratchpad`` is the already-parsed dict (the SQLite column is
    ``scratchpad_json`` TEXT); the executor handles the JSON round-trip.

    Field names mirror the workflow_runs schema 1:1 for clarity at the SQL
    boundary.
    """

    id: int
    workflow_id: int
    version_id: int
    customer_id: str
    current_node_id: str
    scratchpad: dict = field(default_factory=dict)
    status: str = "ACTIVE"
    ready_at_ist: Optional[str] = None
    entered_node_at_ist: Optional[str] = None
    terminated_at_ist: Optional[str] = None
    terminal_status: Optional[str] = None


@dataclass(frozen=True)
class NodeConfig:
    """Parsed node from ``workflow_versions.graph_json``.

    ``config`` is the type-specific config object (e.g. for FETCH_CT_PROPS:
    ``{"properties": {"dpd": "int", ...}}``). ``edges`` maps edge labels to
    target node_ids: ``{"true": "node-uuid-1", "false": "node-uuid-2"}``.

    Frozen to prevent accidental mutation during a tick; the executor's
    per-tick version cache shares NodeConfig instances across all runs on the
    same version.
    """

    node_id: str
    type: str
    label: str
    config: dict
    edges: dict
