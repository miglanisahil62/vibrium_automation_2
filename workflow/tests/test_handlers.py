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
from workflow.agents.workflow_handlers import branch_on_disposition as branch_mod
from workflow.agents.workflow_handlers import condition as condition_mod
from workflow.agents.workflow_handlers import counter as counter_mod
from workflow.agents.workflow_handlers import enroll as enroll_mod
from workflow.agents.workflow_handlers import fetch_ct_props as fetch_mod
from workflow.agents.workflow_handlers import fire_vb_call as fire_mod
from workflow.agents.workflow_handlers import switch as switch_mod
from workflow.agents.workflow_handlers import terminate as terminate_mod
from workflow.agents.workflow_handlers import wait_until as wait_mod
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
    def test_twelve_keys_exact(self):
        assert set(REGISTRY.keys()) == {
            # Phase 4a
            "ENROLL",
            "FETCH_CT_PROPS",
            "CONDITION",
            "FIRE_VB_CALL",
            "AWAIT_DISPOSITION",
            "TERMINATE",
            # Phase 4b
            "SWITCH",
            "WAIT_UNTIL",
            "BRANCH_ON_DISPOSITION",
            "COUNTER",
            # Phase 4c
            "SET_CT_PROP",
            "ASSIGN_AGENT",
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

    def test_optional_property_absent_sets_none_not_error(
        self, monkeypatch, base_run: Run,
    ):
        # coll_bot_calling marked optional ('str?') and absent from profile →
        # success, value None (so a segment NOT keyed on it survives the fetch).
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {"dpd": 5, "coll_collection_risk_segmentation": 3}},
        )
        node = _node("FETCH_CT_PROPS", {
            "properties": {
                "dpd": "int",
                "coll_collection_risk_segmentation": "int?",
                "coll_bot_calling": "str?",
            },
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert result.scratchpad_patch["dpd"] == 5
        assert result.scratchpad_patch["coll_collection_risk_segmentation"] == 3
        assert result.scratchpad_patch["coll_bot_calling"] is None

    def test_optional_property_present_is_coerced(self, monkeypatch, base_run: Run):
        # Present optional prop coerces using the base type (the '?' is stripped).
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {"dpd": 5, "coll_bot_calling": "ai_vb_calling_highv1"}},
        )
        node = _node("FETCH_CT_PROPS", {
            "properties": {"dpd": "int", "coll_bot_calling": "str?"},
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert result.scratchpad_patch["coll_bot_calling"] == "ai_vb_calling_highv1"

    def test_optional_property_uncoercible_sets_none_not_error(
        self, monkeypatch, base_run: Run,
    ):
        # CT stores dpd='NaN' (string) for ~19% of the X-bucket base. With dpd
        # OPTIONAL ('int?'), an un-coercible present value must degrade to None
        # and the customer must still reach classification — NOT route to error
        # (which would be FETCH_FAILED, dropping ~3,137 customers/day, the
        # 2026-06-05 regression). The classification props still coerce normally.
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {
                "dpd": "NaN",
                "coll_collection_risk_segmentation": 3,
                "coll_bot_calling": "ai_vb_calling_highv1",
            }},
        )
        node = _node("FETCH_CT_PROPS", {
            "properties": {
                "dpd": "int?",
                "coll_collection_risk_segmentation": "int?",
                "coll_bot_calling": "str?",
            },
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert result.scratchpad_patch["dpd"] is None
        assert result.scratchpad_patch["coll_collection_risk_segmentation"] == 3
        assert result.scratchpad_patch["coll_bot_calling"] == "ai_vb_calling_highv1"

    def test_required_property_uncoercible_still_errors(
        self, monkeypatch, base_run: Run,
    ):
        # The optional-degrades-to-None path must NOT leak to required props:
        # a REQUIRED prop that is present-but-un-coercible still routes to error.
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {"dpd": "NaN"}},
        )
        node = _node("FETCH_CT_PROPS", {"properties": {"dpd": "int"}})
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["coercion_failed_property"] == "dpd"

    def test_optional_float_nonfinite_degrades_to_none(
        self, monkeypatch, base_run: Run,
    ):
        # float('NaN') SUCCEEDS in stdlib (unlike int('NaN')), so without the
        # math.isfinite guard a 'NaN' on a float? prop would slip past the
        # optional-degrade contract as an actual nan. Confirm it degrades to None.
        monkeypatch.setattr(
            ctp, "get_profile",
            lambda cid: {"profileData": {"some_score": "NaN"}},
        )
        node = _node("FETCH_CT_PROPS", {"properties": {"some_score": "float?"}})
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert result.scratchpad_patch["some_score"] is None

    def test_required_property_still_errors_when_absent(
        self, monkeypatch, base_run: Run,
    ):
        # A required (no '?') prop that is absent must still route to error.
        monkeypatch.setattr(
            ctp, "get_profile", lambda cid: {"profileData": {"coll_bot_calling": "x"}},
        )
        node = _node("FETCH_CT_PROPS", {
            "properties": {"dpd": "int", "coll_bot_calling": "str?"},
        })
        result = fetch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["coercion_failed_property"] == "dpd"


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


# --------------------------------------------------------------------------
# SET_CT_PROP  (Phase 4c)
# --------------------------------------------------------------------------


from workflow.agents.workflow_handlers import set_ct_prop as set_ct_prop_mod
from workflow.agents.workflow_handlers import assign_agent as assign_agent_mod
from workflow.types import SetResult


class TestSetCtProp:
    def test_happy_path_success(self, monkeypatch, base_run: Run):
        """CT 2xx + identity NOT in unprocessed → next_edge='success'."""
        captured: dict = {}

        def fake_set_profile(identity, properties, *, dry_run=False, creds_path=None):
            captured["identity"] = identity
            captured["properties"] = properties
            captured["dry_run"] = dry_run
            return SetResult(success=True, error_code=None, raw_response={"status": "success"})

        monkeypatch.setattr(set_ct_prop_mod.clevertap_profile, "set_profile", fake_set_profile)
        node = _node("SET_CT_PROP", {"properties": {"coll_workflow_state": "in_progress"}})
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert "last_ct_prop_set_at" in result.scratchpad_patch
        assert captured["identity"] == base_run.customer_id
        assert captured["properties"] == {"coll_workflow_state": "in_progress"}
        assert captured["dry_run"] is False
        assert "set_profile cid=" in (result.side_effect or "")

    def test_ct_failure_routes_to_error_with_code(self, monkeypatch, base_run: Run):
        """SetResult.success=False → next_edge='error', error_code in scratchpad."""
        monkeypatch.setattr(
            set_ct_prop_mod.clevertap_profile,
            "set_profile",
            lambda identity, properties, *, dry_run=False, creds_path=None:
                SetResult(success=False, error_code=516, raw_response={}),
        )
        node = _node("SET_CT_PROP", {"properties": {"coll_workflow_state": "x"}})
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["set_ct_prop_error_code"] == 516
        assert "FAIL" in (result.side_effect or "")

    def test_coll_bot_calling_guard_fires(self, monkeypatch, base_run: Run):
        """Phase 0a invariant: handler MUST refuse to write coll_bot_calling.

        The guard must fire WITHOUT calling set_profile — i.e. no CT request
        is ever issued. We assert that by setting set_profile to raise.
        """
        def boom(*a, **k):
            raise AssertionError("set_profile must NOT be called when guard fires")

        monkeypatch.setattr(set_ct_prop_mod.clevertap_profile, "set_profile", boom)
        node = _node("SET_CT_PROP", {
            "properties": {
                "coll_workflow_state": "x",
                "coll_bot_calling": "ai_vb_calling_highv1",  # FORBIDDEN
            },
        })
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "forbidden_property" in result.scratchpad_patch["set_ct_prop_error"]
        assert "coll_bot_calling" in result.scratchpad_patch["set_ct_prop_error"]

    def test_dry_run_skips_real_ct_call(self, monkeypatch, base_run: Run):
        """dry_run=True via ctx → no HTTP call; side_effect notes DRY-RUN."""
        called = {"n": 0}

        def fake_set_profile(*a, **k):
            called["n"] += 1
            return SetResult(success=True, error_code=None, raw_response={})

        monkeypatch.setattr(set_ct_prop_mod.clevertap_profile, "set_profile", fake_set_profile)
        node = _node("SET_CT_PROP", {"properties": {"k": "v"}})
        result = set_ct_prop_mod.execute(node, base_run, ctx={"dry_run": True}, txn=None)
        assert result.next_edge == "success"
        assert called["n"] == 0
        assert "DRY-RUN" in (result.side_effect or "")
        assert "last_ct_prop_set_at" in result.scratchpad_patch

    def test_dry_run_via_kwarg_also_works(self, monkeypatch, base_run: Run):
        """dry_run=True via kwarg (executor's call path) → no HTTP call."""
        def boom(*a, **k):
            raise AssertionError("set_profile must NOT be called under dry_run")

        monkeypatch.setattr(set_ct_prop_mod.clevertap_profile, "set_profile", boom)
        node = _node("SET_CT_PROP", {"properties": {"k": "v"}})
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None, dry_run=True)
        assert result.next_edge == "success"
        assert "DRY-RUN" in (result.side_effect or "")

    def test_multiple_properties_passed_through(self, monkeypatch, base_run: Run):
        """All requested keys reach set_profile in a single call."""
        captured: dict = {}

        def fake_set_profile(identity, properties, *, dry_run=False, creds_path=None):
            captured["properties"] = dict(properties)
            return SetResult(success=True, error_code=None, raw_response={})

        monkeypatch.setattr(set_ct_prop_mod.clevertap_profile, "set_profile", fake_set_profile)
        node = _node("SET_CT_PROP", {
            "properties": {
                "coll_workflow_state": "in_progress",
                "coll_last_journey_node": "fire_vb_call_1",
                "coll_attempt_count": 2,
            },
        })
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "success"
        assert captured["properties"] == {
            "coll_workflow_state": "in_progress",
            "coll_last_journey_node": "fire_vb_call_1",
            "coll_attempt_count": 2,
        }

    def test_missing_properties_config_errors(self, base_run: Run):
        node = _node("SET_CT_PROP", {})
        result = set_ct_prop_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "set_ct_prop_error" in result.scratchpad_patch


# --------------------------------------------------------------------------
# ASSIGN_AGENT  (Phase 4c)
# --------------------------------------------------------------------------


class TestAssignAgent:
    def test_happy_path_inserts_row(self, workflow_db, base_run: Run):
        """One INSERT, scratchpad records assigned_at + assigned_reason."""
        node = _node("ASSIGN_AGENT", {"reason": "dispute_or_nrp"})
        with transaction(workflow_db):
            result = assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        assert result.next_edge == "next"
        assert result.scratchpad_patch["assigned_reason"] == "dispute_or_nrp"
        assert "assigned_at" in result.scratchpad_patch

        rows = workflow_db.execute(
            "SELECT customer_id, reason, source, assigned_at_ist, run_id "
            "FROM agent_assignments"
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["customer_id"] == base_run.customer_id
        assert row["reason"] == "dispute_or_nrp"
        assert row["run_id"] == base_run.id
        assert row["assigned_at_ist"] is not None

    def test_source_field_format(self, workflow_db, base_run: Run):
        """source = 'workflow:<wf_id>:v<ver_id>' — matches FIRE_VB_CALL cohort."""
        node = _node("ASSIGN_AGENT", {"reason": "max_attempts_reached"})
        with transaction(workflow_db):
            assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute(
            "SELECT source FROM agent_assignments"
        ).fetchone()
        assert row["source"] == f"workflow:{base_run.workflow_id}:v{base_run.version_id}"

    def test_no_dedupe_two_calls_two_rows(self, workflow_db, base_run: Run):
        """Two consecutive ASSIGN_AGENT executes → two rows. No UNIQUE
        constraint on this table by design (see module docstring).
        """
        node = _node("ASSIGN_AGENT", {"reason": "unhandled_disposition"})
        with transaction(workflow_db):
            assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        with transaction(workflow_db):
            assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM agent_assignments"
        ).fetchone()["c"]
        assert count == 2

    def test_caller_owned_transaction(self, workflow_db, base_run: Run):
        """Handler uses the passed-in txn, does NOT commit on its own.

        We start a transaction, run the handler, then ROLLBACK and assert
        no row landed — proving the handler does not commit.
        """
        node = _node("ASSIGN_AGENT", {"reason": "test_rollback"})
        workflow_db.execute("BEGIN IMMEDIATE")
        try:
            assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
            # Sanity: row visible inside the open txn.
            mid = workflow_db.execute(
                "SELECT COUNT(*) AS c FROM agent_assignments WHERE reason='test_rollback'"
            ).fetchone()["c"]
            assert mid == 1
        finally:
            workflow_db.execute("ROLLBACK")
        after = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM agent_assignments WHERE reason='test_rollback'"
        ).fetchone()["c"]
        assert after == 0

    def test_missing_reason_routes_to_error(self, workflow_db, base_run: Run):
        node = _node("ASSIGN_AGENT", {})
        with transaction(workflow_db):
            result = assign_agent_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        assert result.next_edge == "error"
        # And no row inserted.
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM agent_assignments"
        ).fetchone()["c"]
        assert count == 0

    def test_dry_run_skips_insert(self, workflow_db, base_run: Run):
        node = _node("ASSIGN_AGENT", {"reason": "dispute_or_nrp"})
        result = assign_agent_mod.execute(
            node, base_run, ctx={"dry_run": True}, txn=workflow_db,
        )
        assert result.next_edge == "next"
        assert "DRY-RUN" in (result.side_effect or "")
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM agent_assignments"
        ).fetchone()["c"]
        assert count == 0


# --------------------------------------------------------------------------
# SWITCH (Phase 4b)
# --------------------------------------------------------------------------


class TestSwitch:
    def test_happy_path_matches_case(self, base_run: Run):
        base_run.scratchpad = {"risk_segmentation": 3}
        node = _node("SWITCH", {
            "on": "risk_segmentation",
            "cases": {"1": "high", "2": "high", "3": "high",
                      "4": "high", "5": "mid", "6": "mid", "7": "mid"},
            "default": "low_risk_todo",
        })
        result = switch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "high"
        assert result.scratchpad_patch == {}

    def test_default_fallthrough_when_no_case(self, base_run: Run):
        # Value 9 is not in cases → default.
        base_run.scratchpad = {"risk_segmentation": 9}
        node = _node("SWITCH", {
            "on": "risk_segmentation",
            "cases": {"1": "high", "5": "mid"},
            "default": "low_risk_todo",
        })
        result = switch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "low_risk_todo"

    def test_missing_key_routes_to_error(self, base_run: Run):
        base_run.scratchpad = {"other_key": 5}
        node = _node("SWITCH", {
            "on": "risk_segmentation",
            "cases": {"5": "mid"},
            "default": "low_risk_todo",
        })
        result = switch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert "missing key: risk_segmentation" in result.scratchpad_patch["switch_error"]

    def test_numeric_value_matches_string_key(self, base_run: Run):
        # Scratchpad value is int 5; case key is the string "5".
        base_run.scratchpad = {"risk_segmentation": 5}
        node = _node("SWITCH", {
            "on": "risk_segmentation",
            "cases": {"5": "mid"},
            "default": "low",
        })
        result = switch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "mid"

    def test_multiple_distinct_case_values(self, base_run: Run):
        # Several values from the same `cases` dict each map correctly.
        cases = {"1": "high", "2": "high", "5": "mid", "7": "mid"}
        for value, expected_edge in [(1, "high"), (2, "high"),
                                     (5, "mid"), (7, "mid")]:
            base_run.scratchpad = {"v": value}
            node = _node("SWITCH", {"on": "v", "cases": cases, "default": "x"})
            result = switch_mod.execute(node, base_run, ctx=None, txn=None)
            assert result.next_edge == expected_edge, f"failed for {value}"


# --------------------------------------------------------------------------
# WAIT_UNTIL (Phase 4b)
# --------------------------------------------------------------------------


class TestWaitUntil:
    def test_relative_t_plus_1_day(self, base_run: Run):
        node = _node("WAIT_UNTIL", {"relative": "T+1 day at 08:00"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        assert result.ready_at_ist is not None
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        assert parsed.hour == 8
        assert parsed.minute == 0
        now = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        delta = parsed - now
        # Loose bound — depending on wall-clock, +1 day at 08:00 can be from
        # a few minutes (just before 08:00) to nearly 2 days (just after 08:00).
        assert timedelta(minutes=-1) < delta < timedelta(days=2)
        assert base_run.status == "WAITING"
        assert base_run.ready_at_ist == result.ready_at_ist

    def test_relative_t_plus_n_hour(self, base_run: Run):
        node = _node("WAIT_UNTIL", {"relative": "T+3 hour"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        now = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        delta = parsed - now
        assert timedelta(hours=2, minutes=59) <= delta <= timedelta(hours=3, minutes=1)

    def test_absolute_valid_yyyy_mm_dd(self, base_run: Run):
        base_run.scratchpad = {"target_date": "2027-01-15"}
        node = _node("WAIT_UNTIL", {"absolute": "target_date"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        assert result.ready_at_ist == "2027-01-15 00:00:00"
        assert base_run.status == "WAITING"

    def test_absolute_invalid_format_routes_to_error(self, base_run: Run):
        # MM/DD/YYYY not accepted.
        base_run.scratchpad = {"target_date": "01/15/2027"}
        node = _node("WAIT_UNTIL", {"absolute": "target_date"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch["wait_error"] == "invalid_date_format"

    def test_late_wakeup_advances_immediately(self, base_run: Run):
        # Deadline in the past — handler must set run.status='ACTIVE' and return
        # ready_at_ist=None so _persist_run_advance takes the advance path,
        # not the park path. The old behaviour (re-park) caused an infinite loop.
        base_run.scratchpad = {"target_date": "2020-01-01"}
        node = _node("WAIT_UNTIL", {"absolute": "target_date"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        assert result.ready_at_ist is None       # no park deadline on late path
        assert base_run.status == "ACTIVE"       # executor will advance, not park
        assert "already passed" in (result.side_effect or "").lower()


# --------------------------------------------------------------------------
# BRANCH_ON_DISPOSITION (Phase 4b — critical surface)
# --------------------------------------------------------------------------


_BRANCH_CASES = {
    "NOOP": "to_terminate",
    "RETRY": "to_counter",
    "PTP_CALL": "to_ptp_wait",
    "AGREE_EOD_CALL": "to_eod_wait",
    "CALLBACK_CALL": "to_callback_wait",
    "RTP_NEEDS_LLM": "to_llm",
    "ESCALATE": "to_assign_agent",
}


class TestBranchOnDisposition:
    @pytest.mark.parametrize("action_class,expected_edge", list(_BRANCH_CASES.items()))
    def test_each_canonical_action_class_branches_correctly(
        self, base_run: Run, action_class: str, expected_edge: str,
    ):
        base_run.scratchpad = {"last_disposition_action_class": action_class}
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": _BRANCH_CASES,
            "default": "to_terminate",
        })
        result = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == expected_edge
        # Scratchpad-clear contract: every successful branch clears the key.
        assert result.scratchpad_patch.get("last_disposition_action_class") is None

    def test_unknown_action_class_falls_through_to_default(self, base_run: Run):
        # action_class not in canonical enum should NOT error — falls through
        # to default. Protects in-flight runs against a future ingest revision
        # that adds a new enum value.
        base_run.scratchpad = {"last_disposition_action_class": "FUTURE_NEW_CLASS"}
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": _BRANCH_CASES,
            "default": "to_terminate",
        })
        result = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "to_terminate"
        assert result.scratchpad_patch.get("last_disposition_action_class") is None

    def test_missing_disposition_routes_to_error(self, base_run: Run):
        base_run.scratchpad = {}
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": _BRANCH_CASES,
            "default": "to_terminate",
        })
        result = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"
        assert result.scratchpad_patch.get("branch_error") == "no_disposition"
        # On error we do NOT clear; the key was already absent.
        assert "last_disposition_action_class" not in result.scratchpad_patch

    def test_scratchpad_cleared_on_successful_branch(self, base_run: Run):
        # Most important contract: after BRANCH_ON_DISPOSITION fires, a
        # subsequent revisit must NOT see the same action_class.
        base_run.scratchpad = {"last_disposition_action_class": "PTP_CALL"}
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": _BRANCH_CASES,
            "default": "to_terminate",
        })
        result = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "to_ptp_wait"
        # The patch explicitly contains None (signal: clear key).
        assert "last_disposition_action_class" in result.scratchpad_patch
        assert result.scratchpad_patch["last_disposition_action_class"] is None
        # If the executor merged shallow-style, the key would be None —
        # falsy — so await_disposition correctly would NOT see a stale
        # disposition on re-entry.
        merged = {**base_run.scratchpad, **result.scratchpad_patch}
        assert not merged.get("last_disposition_action_class")

    def test_default_fallthrough_with_missing_case(self, base_run: Run):
        # Cases dict missing PTP_CALL entirely → default fires.
        base_run.scratchpad = {"last_disposition_action_class": "PTP_CALL"}
        partial_cases = {"NOOP": "to_terminate"}
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": partial_cases,
            "default": "to_default",
        })
        result = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "to_default"
        # Clear still happens on default-fallthrough success.
        assert result.scratchpad_patch.get("last_disposition_action_class") is None

    def test_multiple_branches_do_not_interfere(self, base_run: Run):
        # Run handler twice with different action_classes; each branches
        # independently. Simulates two distinct disposition wakeups.
        node = _node("BRANCH_ON_DISPOSITION", {
            "cases": _BRANCH_CASES,
            "default": "to_terminate",
        })
        base_run.scratchpad = {"last_disposition_action_class": "RETRY"}
        r1 = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert r1.next_edge == "to_counter"
        # Simulate executor merging the patch (shallow merge).
        base_run.scratchpad = {**base_run.scratchpad, **r1.scratchpad_patch}
        # A second disposition arrives — ingest writes a new action_class.
        base_run.scratchpad["last_disposition_action_class"] = "ESCALATE"
        r2 = branch_mod.execute(node, base_run, ctx=None, txn=None)
        assert r2.next_edge == "to_assign_agent"


# --------------------------------------------------------------------------
# COUNTER (Phase 4b)
# --------------------------------------------------------------------------


class TestCounter:
    def test_first_increment_from_absent_key(self, base_run: Run):
        base_run.scratchpad = {}
        node = _node("COUNTER", {"name": "attempts", "limit": 2})
        result = counter_mod.execute(node, base_run, ctx=None, txn=None)
        # 0 → 1, 1 < 2 → under_limit.
        assert result.next_edge == "under_limit"
        assert result.scratchpad_patch == {"attempts": 1}

    def test_under_limit_branch(self, base_run: Run):
        base_run.scratchpad = {"attempts": 0}
        node = _node("COUNTER", {"name": "attempts", "limit": 3})
        result = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "under_limit"
        assert result.scratchpad_patch == {"attempts": 1}

    def test_at_limit_branch(self, base_run: Run):
        # current=1, increment to 2, limit=2 → at_limit.
        base_run.scratchpad = {"attempts": 1}
        node = _node("COUNTER", {"name": "attempts", "limit": 2})
        result = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "at_limit"
        assert result.scratchpad_patch == {"attempts": 2}

    def test_repeated_calls_keep_advancing(self, base_run: Run):
        # Simulate successive ticks through the same COUNTER node.
        base_run.scratchpad = {}
        node = _node("COUNTER", {"name": "attempts", "limit": 3})

        r1 = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert r1.next_edge == "under_limit"
        assert r1.scratchpad_patch == {"attempts": 1}
        base_run.scratchpad = {**base_run.scratchpad, **r1.scratchpad_patch}

        r2 = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert r2.next_edge == "under_limit"
        assert r2.scratchpad_patch == {"attempts": 2}
        base_run.scratchpad = {**base_run.scratchpad, **r2.scratchpad_patch}

        r3 = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert r3.next_edge == "at_limit"
        assert r3.scratchpad_patch == {"attempts": 3}
        base_run.scratchpad = {**base_run.scratchpad, **r3.scratchpad_patch}

        # Once at limit, subsequent calls stay at limit (no reset).
        r4 = counter_mod.execute(node, base_run, ctx=None, txn=None)
        assert r4.next_edge == "at_limit"
        assert r4.scratchpad_patch == {"attempts": 4}
