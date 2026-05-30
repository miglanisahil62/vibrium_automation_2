"""Unit tests for Phase 4a workflow handlers.

One class per handler. Each class covers the happy path + ≥2 edge cases.

External boundary policy:
    * ``fetch_ct_props`` mocks ``workflow.clevertap_profile.get_profile`` via
      ``monkeypatch.setattr``. The pinned fixture
      ``workflow/tests/fixtures/ct_profile_response.json`` is the source of
      record for what a real CT 200 response looks like.
    * ``fire_vb_call`` uses an in-memory SQLite created by applying
      migration 001 (the real schema). No mocking of the DB layer — the
      INSERT OR IGNORE behavior is part of what we're testing.
    * ``await_disposition`` mutates the in-memory ``Run`` directly; no DB.
    * ``terminate`` mutates the in-memory ``Run`` directly; no DB.
    * ``condition`` is pure-Python evaluation; no mocking needed.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from zoneinfo import ZoneInfo

from workflow import clevertap_profile as ctp
from workflow.agents.workflow_handlers import (
    REGISTRY,
    NodeConfig,
    NodeResult,
    Run,
)
from workflow.agents.workflow_handlers import await_disposition as await_mod
from workflow.agents.workflow_handlers import condition as condition_mod
from workflow.agents.workflow_handlers import enroll as enroll_mod
from workflow.agents.workflow_handlers import fetch_ct_props as fetch_mod
from workflow.agents.workflow_handlers import fire_vb_call as fire_mod
from workflow.agents.workflow_handlers import terminate as terminate_mod
from workflow.wf_store import get_workflow_db, transaction


# Migration 001 is the file "001_init.py" — Python doesn't allow ``import
# 001_init`` so we do a runtime import via importlib.
import importlib.util
_MIGRATION_PATH = Path(__file__).resolve().parent.parent / "migrations" / "001_init.py"
_spec = importlib.util.spec_from_file_location("_mig_001", _MIGRATION_PATH)
_mig_001 = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec is not None and _spec.loader is not None
_spec.loader.exec_module(_mig_001)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ct_profile_response.json"


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def workflow_db(tmp_path: Path):
    """Fresh workflow.db with schema 001 applied."""
    db_path = tmp_path / "workflow.db"
    _mig_001.up(db_path)
    conn = get_workflow_db(db_path)
    yield conn
    conn.close()


@pytest.fixture
def base_run() -> Run:
    """A minimal Run for handlers that don't care about timing/state."""
    return Run(
        id=42,
        workflow_id=7,
        version_id=3,
        customer_id="8968249",
        current_node_id="node-uuid-test",
        scratchpad={},
        status="ACTIVE",
        entered_node_at_ist="2026-05-30 12:00:00",
    )


def _node(node_type: str, config: dict, edges: dict = None) -> NodeConfig:
    return NodeConfig(
        node_id=f"{node_type.lower()}-uuid-1",
        type=node_type,
        label=node_type,
        config=config,
        edges=edges or {},
    )


# --------------------------------------------------------------------------
# Registry sanity
# --------------------------------------------------------------------------


class TestRegistry:
    def test_six_keys_exact(self):
        assert set(REGISTRY.keys()) == {
            "ENROLL",
            "FETCH_CT_PROPS",
            "CONDITION",
            "FIRE_VB_CALL",
            "AWAIT_DISPOSITION",
            "TERMINATE",
        }

    def test_all_handlers_callable(self):
        for k, v in REGISTRY.items():
            assert callable(v), f"REGISTRY[{k!r}] not callable"


# --------------------------------------------------------------------------
# ENROLL
# --------------------------------------------------------------------------


class TestEnroll:
    def test_happy_path_returns_next_edge(self, base_run: Run):
        result = enroll_mod.execute(_node("ENROLL", {}), base_run, ctx=None, txn=None)
        assert isinstance(result, NodeResult)
        assert result.next_edge == "next"
        assert result.scratchpad_patch == {}
        assert result.side_effect is None
        assert result.ready_at_ist is None

    def test_ignores_config_payload(self, base_run: Run):
        # Even with non-empty config, ENROLL just routes onward.
        result = enroll_mod.execute(
            _node("ENROLL", {"junk": "ignored"}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge == "next"

    def test_dry_run_no_op(self, base_run: Run):
        result = enroll_mod.execute(
            _node("ENROLL", {}), base_run, ctx=None, txn=None, dry_run=True,
        )
        assert result.next_edge == "next"
        assert result.scratchpad_patch == {}


# --------------------------------------------------------------------------
# FETCH_CT_PROPS
# --------------------------------------------------------------------------


class TestFetchCtProps:
    @pytest.fixture
    def fixture_record(self) -> dict:
        with open(FIXTURE_PATH) as f:
            return json.load(f)["record"]

    def test_happy_path_coerces_typed_props(
        self, monkeypatch, base_run: Run, fixture_record: dict,
    ):
        # The fixture has coll_collection_risk_segmentation=8 (int),
        # coll_notification_replied='Agent Calling' (str),
        # coll_bot_calling='AI Calling' (str), dpd=29 (int).
        monkeypatch.setattr(ctp, "get_profile", lambda cid: fixture_record)
        node = _node("FETCH_CT_PROPS", {
            "properties": {
                "dpd": "int",
                "coll_collection_risk_segmentation": "int",
                "coll_notification_replied": "str",
                "coll_bot_calling": "str",
            },
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert result.scratchpad_patch["dpd"] == 29
        assert isinstance(result.scratchpad_patch["dpd"], int)
        assert result.scratchpad_patch["coll_collection_risk_segmentation"] == 8
        assert result.scratchpad_patch["coll_notification_replied"] == "Agent Calling"
        assert result.scratchpad_patch["coll_bot_calling"] == "AI Calling"

    def test_not_found_returns_not_found_edge(self, monkeypatch, base_run: Run):
        monkeypatch.setattr(ctp, "get_profile", lambda cid: None)
        node = _node("FETCH_CT_PROPS", {"properties": {"dpd": "int"}})
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "not_found"
        assert result.scratchpad_patch == {}

    def test_coercion_failure_records_property_name(
        self, monkeypatch, base_run: Run,
    ):
        # Force a non-numeric value into a property declared as int.
        bad_record = {
            "profileData": {"dpd": "not-a-number", "coll_bot_calling": "AI"},
        }
        monkeypatch.setattr(ctp, "get_profile", lambda cid: bad_record)
        node = _node("FETCH_CT_PROPS", {
            "properties": {"dpd": "int", "coll_bot_calling": "str"},
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["coercion_failed_property"] == "dpd"
        assert "fetch_ct_props_error" in result.scratchpad_patch

    def test_unsupported_schema_type_routes_to_error(
        self, monkeypatch, base_run: Run,
    ):
        monkeypatch.setattr(ctp, "get_profile", lambda cid: {"profileData": {"x": 1}})
        node = _node("FETCH_CT_PROPS", {"properties": {"x": "decimal"}})
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["coercion_failed_property"] == "x"

    def test_missing_property_in_profileData_errors(
        self, monkeypatch, base_run: Run,
    ):
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {"dpd": 5}},
        )
        node = _node("FETCH_CT_PROPS", {
            "properties": {"dpd": "int", "coll_bot_calling": "str"},
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["coercion_failed_property"] == "coll_bot_calling"


# --------------------------------------------------------------------------
# CONDITION
# --------------------------------------------------------------------------


class TestCondition:
    def test_happy_path_true(self, base_run: Run):
        base_run.scratchpad = {"dpd": 29, "coll_collection_risk_segmentation": 8}
        node = _node("CONDITION", {"expr": "dpd > 1 and coll_collection_risk_segmentation >= 5"})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "true"

    def test_happy_path_false(self, base_run: Run):
        base_run.scratchpad = {"dpd": 0}
        node = _node("CONDITION", {"expr": "dpd > 5"})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "false"

    def test_functions_disabled_rejects_int_call(self, base_run: Run):
        # CRITICAL: int(x), len(...), any function call must be rejected.
        base_run.scratchpad = {"x": "5"}
        node = _node("CONDITION", {"expr": "int(x) > 3"})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "condition_error" in result.scratchpad_patch
        # The error string should mention something about the function rejection.
        err = result.scratchpad_patch["condition_error"]
        assert "FunctionNotDefined" in err or "Function" in err or "int" in err

    def test_undefined_name_routes_to_error(self, base_run: Run):
        base_run.scratchpad = {"dpd": 5}
        node = _node("CONDITION", {"expr": "missing_var > 3"})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "condition_error" in result.scratchpad_patch

    def test_syntax_error_routes_to_error_not_raise(self, base_run: Run):
        node = _node("CONDITION", {"expr": "5 + + +"})
        # Must NOT raise.
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "condition_error" in result.scratchpad_patch

    def test_missing_expr_routes_to_error(self, base_run: Run):
        node = _node("CONDITION", {})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"

    def test_string_equality_works(self, base_run: Run):
        base_run.scratchpad = {"wa_status": "WA_Available"}
        node = _node("CONDITION", {"expr": "wa_status == 'WA_Available'"})
        result = condition_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "true"


# --------------------------------------------------------------------------
# FIRE_VB_CALL
# --------------------------------------------------------------------------


class TestFireVbCall:
    def test_happy_path_inserts_row(self, workflow_db, base_run: Run):
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            result = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        assert result.next_edge == "queued"
        assert "last_fire_at" in result.scratchpad_patch
        # Verify the row landed.
        rows = workflow_db.execute(
            "SELECT run_id, node_id, attempt_count, customer_id, status, cohort_name "
            "FROM wf_pending_actions"
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["run_id"] == base_run.id
        assert row["node_id"] == node.node_id
        assert row["attempt_count"] == 0
        assert row["customer_id"] == base_run.customer_id
        assert row["status"] == "PENDING"
        assert row["cohort_name"] == f"workflow:{base_run.workflow_id}:v{base_run.version_id}"

    def test_dedupe_second_insert_no_op(self, workflow_db, base_run: Run):
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            r1 = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        with transaction(workflow_db):
            r2 = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        # Both calls return queued (idempotent advance).
        assert r1.next_edge == "queued"
        assert r2.next_edge == "queued"
        # But only one row exists.
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM wf_pending_actions"
        ).fetchone()["c"]
        assert count == 1
        # The second call's side_effect mentions dedupe.
        assert "dedupe-hit" in (r2.side_effect or "")

    def test_attempts_from_scratchpad(self, workflow_db, base_run: Run):
        base_run.scratchpad = {"attempts": 3}
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute(
            "SELECT attempt_count FROM wf_pending_actions"
        ).fetchone()
        assert row["attempt_count"] == 3

    def test_dry_run_does_not_insert(self, workflow_db, base_run: Run):
        node = _node("FIRE_VB_CALL", {})
        result = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db, dry_run=True)
        assert result.next_edge == "queued"
        assert "DRY-RUN" in (result.side_effect or "")
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM wf_pending_actions"
        ).fetchone()["c"]
        assert count == 0


# --------------------------------------------------------------------------
# AWAIT_DISPOSITION
# --------------------------------------------------------------------------


class TestAwaitDisposition:
    def test_disposition_arrived_routes_to_disposition(self, base_run: Run):
        base_run.scratchpad = {"last_disposition_action_class": "PTP_CALL"}
        # entered_node_at_ist is recent → deadline not elapsed; disposition wins.
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": 24}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge == "disposition"
        assert "PTP_CALL" in (result.side_effect or "")
        assert base_run.status == "ACTIVE"

    def test_parks_when_no_disposition_and_not_timed_out(self, base_run: Run):
        # entered_node_at_ist is now-ish; timeout=24h → park.
        now = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        base_run.entered_node_at_ist = now.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": 24}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge is None
        assert result.ready_at_ist is not None
        assert base_run.status == "WAITING"
        assert base_run.ready_at_ist == result.ready_at_ist

    def test_timeout_after_deadline_routes_to_timeout(self, base_run: Run):
        # Set entered 48h ago with timeout 1h → way past deadline.
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(hours=48)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": 1}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge == "timeout"
        assert base_run.status == "ACTIVE"

    def test_disposition_wins_even_past_deadline(self, base_run: Run):
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(hours=48)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {"last_disposition_action_class": "RETRY"}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": 1}),
            base_run, ctx=None, txn=None,
        )
        # Disposition check happens before timeout check.
        assert result.next_edge == "disposition"

    def test_bad_timeout_hours_falls_back_to_default(self, base_run: Run):
        # entered_node_at_ist is way in the past → with default 24h still
        # past deadline, but we want to assert it doesn't crash.
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(hours=48)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": "garbage"}),
            base_run, ctx=None, txn=None,
        )
        # With default 24h and anchor 48h ago, deadline elapsed → timeout.
        assert result.next_edge == "timeout"


# --------------------------------------------------------------------------
# TERMINATE
# --------------------------------------------------------------------------


class TestTerminate:
    def test_happy_path_sets_done_and_terminal_status(self, base_run: Run):
        result = terminate_mod.execute(
            _node("TERMINATE", {"status": "PAID"}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge is None
        assert base_run.status == "DONE"
        assert base_run.terminal_status == "PAID"
        assert base_run.terminated_at_ist is not None

    def test_default_terminal_status_when_missing(self, base_run: Run):
        result = terminate_mod.execute(
            _node("TERMINATE", {}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge is None
        assert base_run.status == "DONE"
        assert base_run.terminal_status == "DONE"

    def test_empty_status_falls_back_to_done(self, base_run: Run):
        result = terminate_mod.execute(
            _node("TERMINATE", {"status": "   "}),
            base_run, ctx=None, txn=None,
        )
        assert base_run.terminal_status == "DONE"

    def test_clears_ready_at_ist(self, base_run: Run):
        base_run.ready_at_ist = "2026-05-30 18:00:00"
        terminate_mod.execute(
            _node("TERMINATE", {"status": "MAX_ATTEMPTS"}),
            base_run, ctx=None, txn=None,
        )
        assert base_run.ready_at_ist is None
