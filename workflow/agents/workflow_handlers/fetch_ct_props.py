"""FETCH_CT_PROPS handler — read CT user properties into typed scratchpad.

Node config shape:
    {
        "properties": {
            "dpd": "int",
            "coll_collection_risk_segmentation": "int",
            "coll_notification_replied": "str",
            "coll_bot_calling": "str"
        }
    }

The handler:
  1. Calls ``clevertap_profile.get_profile(customer_id)``.
  2. On None (404 in CT) → ``next_edge="not_found"``.
  3. For each requested property, reads from ``record["profileData"]`` (the
     pinned fixture confirms this is the canonical path) and coerces using
     the declared schema type.
  4. On coercion failure for ANY property → ``next_edge="error"`` with
     ``coercion_failed_property = <name>`` recorded in scratchpad_patch.
  5. On success → ``next_edge="success"`` with all typed properties merged
     into scratchpad.

Why coerce at fetch time rather than at CONDITION eval time:
    The architecture (rev 3 §"CONDITION DSL — pin syntax, types") explicitly
    pins coercion HERE so simpleeval evaluates pure typed values. The class
    of ``TypeError: '<' not supported between str and int`` errors vanishes.

Casing note (per docs/phase_0a_decision.md):
    CT property names are LOWERCASE — ``coll_*``, ``dpd``. The VB_Prompt_Doc
    uses uppercase but those are wrong; lowercase is what the actual CT
    profile returns.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from workflow import clevertap_profile
from workflow.agents.workflow_handlers.types import NodeConfig, NodeResult, Run

log = logging.getLogger("workflow.fetch_ct_props")
_IST = ZoneInfo("Asia/Kolkata")


def _read_cached_record(txn: Any, customer_id: str) -> "dict | None":
    """WS1 cache-first: read today's prefetched CT record from ct_profile_cache
    (same workflow.db the executor's `txn` is on). Returns the CT `record` dict
    on a 'found' hit, else None (caller falls back to a live get_profile).

    Avoids a live CT call per run — decisive when the executor walks tens of
    thousands of X-bucket runs. Tolerant of a missing table / bad JSON (logs +
    returns None so the live path takes over)."""
    if txn is None:
        return None
    try:
        cohort_date = datetime.now(_IST).strftime("%Y-%m-%d")
        row = txn.execute(
            "SELECT profile_json FROM ct_profile_cache "
            "WHERE customer_id = ? AND cohort_date = ? AND status = 'found'",
            (str(customer_id), cohort_date),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        log.warning("ct_profile_cache unavailable (%s) — live fetch for cid=%s",
                    exc, customer_id)
        return None
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except (ValueError, TypeError) as exc:
        log.warning("ct_profile_cache bad JSON cid=%s (%s) — live fetch", customer_id, exc)
        return None


# Allowed schema types. Extending this requires updating the coercion table
# below AND the validator in Phase 10 (workflow/validation.py).
_ALLOWED_SCHEMA_TYPES: tuple = ("int", "float", "str", "bool")


def _parse_schema_type(prop_type: str) -> tuple[str, bool]:
    """Split a schema type into (base_type, optional).

    A trailing ``?`` marks the property OPTIONAL: if it is absent from the CT
    profile, the handler sets it to ``None`` in scratchpad instead of routing
    to the ``error`` edge. This lets one FETCH_CT_PROPS node serve a graph
    whose segments key on DIFFERENT properties (e.g. ``coll_bot_calling`` is
    present for some segments, absent for a risk-rule segment) without every
    customer missing one optional field being dropped as FETCH_FAILED.

    Examples: ``"str"`` -> ("str", False); ``"str?"`` -> ("str", True).
    """
    if isinstance(prop_type, str) and prop_type.endswith("?"):
        return prop_type[:-1], True
    return prop_type, False


def _coerce(value: Any, target_type: str) -> Any:
    """Coerce a single CT value to the declared schema type.

    Coercion rules (strict; ambiguous source values raise):
        int:  ``int(value)`` — accepts numeric strings, floats, ints. Booleans
              are rejected (would silently coerce ``True -> 1``).
        float: ``float(value)`` — same accepts as int. Booleans rejected.
        str:  ``str(value)`` — universal; only None raises.
        bool: only literal True/False/"true"/"false"/"True"/"False"/0/1
              accepted. Everything else raises so ``bool("anything")`` 's
              well-known truthiness trap can't hide a bad value.

    Raises ValueError on any failure. The handler converts that into the
    ``error`` edge.
    """
    if target_type == "int":
        if isinstance(value, bool):
            raise ValueError(f"refusing to coerce bool to int: {value!r}")
        if value is None:
            raise ValueError("cannot coerce None to int")
        return int(value)
    if target_type == "float":
        if isinstance(value, bool):
            raise ValueError(f"refusing to coerce bool to float: {value!r}")
        if value is None:
            raise ValueError("cannot coerce None to float")
        return float(value)
    if target_type == "str":
        if value is None:
            raise ValueError("cannot coerce None to str")
        return str(value)
    if target_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            low = value.strip().lower()
            if low in ("true", "1"):
                return True
            if low in ("false", "0"):
                return False
        raise ValueError(f"cannot coerce to bool: {value!r}")
    raise ValueError(
        f"unsupported schema type {target_type!r}; allowed: {_ALLOWED_SCHEMA_TYPES}"
    )


def execute(
    node: NodeConfig,
    run: Run,
    ctx: Any,
    txn: Any,
    dry_run: bool = False,
) -> NodeResult:
    """Fetch CT properties, coerce, merge into scratchpad."""
    schema = node.config.get("properties") or {}
    if not isinstance(schema, dict) or not schema:
        # Save-time validation (Phase 10) should prevent this, but defend
        # in depth at runtime too.
        return NodeResult(
            next_edge="error",
            scratchpad_patch={"fetch_ct_props_error": "missing or empty 'properties' config"},
            side_effect="FETCH_CT_PROPS misconfigured",
            ready_at_ist=None,
        )

    # Validate all schema entries up front — fail before any HTTP call.
    # A trailing '?' marks the property optional; validate the BASE type.
    for prop_name, prop_type in schema.items():
        base_type, _optional = _parse_schema_type(prop_type)
        if base_type not in _ALLOWED_SCHEMA_TYPES:
            return NodeResult(
                next_edge="error",
                scratchpad_patch={
                    "coercion_failed_property": prop_name,
                    "fetch_ct_props_error": (
                        f"unsupported schema type {prop_type!r} for property "
                        f"{prop_name!r}; allowed: {_ALLOWED_SCHEMA_TYPES}"
                    ),
                },
                side_effect=f"FETCH_CT_PROPS bad schema type for {prop_name}",
                ready_at_ist=None,
            )

    # Cache-first (WS1): read the day's prefetched profile from ct_profile_cache
    # (this same workflow.db `txn`); live get_profile only on a miss.
    record = _read_cached_record(txn, run.customer_id)
    if record is None:
        record = clevertap_profile.get_profile(run.customer_id)
    if record is None:
        return NodeResult(
            next_edge="not_found",
            scratchpad_patch={},
            side_effect=f"CT profile not_found cid={run.customer_id}",
            ready_at_ist=None,
        )

    profile_data = record.get("profileData") if isinstance(record, dict) else None
    if not isinstance(profile_data, dict):
        return NodeResult(
            next_edge="error",
            scratchpad_patch={
                "fetch_ct_props_error": "CT response missing profileData object",
            },
            side_effect="FETCH_CT_PROPS no profileData",
            ready_at_ist=None,
        )

    patch: dict = {}
    for prop_name, prop_type in schema.items():
        base_type, optional = _parse_schema_type(prop_type)
        if prop_name not in profile_data:
            if optional:
                # Absent optional property → None in scratchpad. Downstream
                # CONDITION rules that reference it just won't match (e.g.
                # `coll_bot_calling == 'X'` is False when it's None); this is
                # how a segment NOT keyed on this property survives the fetch.
                patch[prop_name] = None
                continue
            return NodeResult(
                next_edge="error",
                scratchpad_patch={
                    "coercion_failed_property": prop_name,
                    "fetch_ct_props_error": (
                        f"property {prop_name!r} absent from CT profileData"
                    ),
                },
                side_effect=f"FETCH_CT_PROPS missing prop={prop_name}",
                ready_at_ist=None,
            )
        raw = profile_data[prop_name]
        try:
            patch[prop_name] = _coerce(raw, base_type)
        except (ValueError, TypeError) as exc:  # stashfin-lint: ignore  # documented contract: coercion failure routes to 'error' edge with property name in scratchpad; executor logs side_effect.

            return NodeResult(
                next_edge="error",
                scratchpad_patch={
                    "coercion_failed_property": prop_name,
                    "fetch_ct_props_error": str(exc),
                },
                side_effect=f"FETCH_CT_PROPS coercion fail prop={prop_name}",
                ready_at_ist=None,
            )

    return NodeResult(
        next_edge="success",
        scratchpad_patch=patch,
        side_effect=f"CT profile fetched cid={run.customer_id} props={len(patch)}",
        ready_at_ist=None,
    )
