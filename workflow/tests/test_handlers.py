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
from workflow.agents.workflow_handlers import same_day_gate as sdg_mod
from workflow.agents.workflow_handlers import switch as switch_mod
from workflow.agents.workflow_handlers import terminate as terminate_mod
from workflow.agents.workflow_handlers import wait_until as wait_mod
from workflow.wf_store import get_workflow_db, transaction


# Apply the FULL migration chain (001..NNN) so the test schema matches prod —
# FIRE now writes priority_class (migration 005), so 001 alone is insufficient.
from workflow.migrations import runner as _mig_runner


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ct_profile_response.json"


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def workflow_db(tmp_path: Path):
    """Fresh workflow.db with the FULL migration chain applied (matches prod)."""
    db_path = tmp_path / "workflow.db"
    _mig_runner.run(str(db_path), str(tmp_path / "vibrium.db"))
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


def _ist_now_str() -> str:
    """IST-naive 'now' string — WAIT_UNTIL anchors relative/rotate offsets to the
    run's entered_node_at_ist, so park-asserting tests must set a RECENT entry
    (a freshly-entered run has entry≈now). See test_relative_hour_treadmill_breaks
    for the past-entry advance case."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------
# Registry sanity
# --------------------------------------------------------------------------


class TestRegistry:
    def test_registry_keys_exact(self):
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
            # WS3 same-day-retry
            "SAME_DAY_GATE",
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

    def test_attempts_from_scratchpad_v7_fallback(self, workflow_db, base_run: Run):
        # v7 drain path: no fire_seq → falls back to `attempts`, no self-bump.
        base_run.scratchpad = {"attempts": 3}
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            result = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute(
            "SELECT attempt_count FROM wf_pending_actions"
        ).fetchone()
        assert row["attempt_count"] == 3
        assert "fire_seq" not in result.scratchpad_patch  # v7 must not introduce it

    def test_fire_seq_used_and_self_bumped_v8(self, workflow_db, base_run: Run):
        # v8 path: fire_seq present → used as attempt_count AND bumped for the
        # NEXT fire, so same-day reattempts at the same FIRE node never collide.
        base_run.scratchpad = {"fire_seq": 2, "attempts": 99}
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            result = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute(
            "SELECT attempt_count FROM wf_pending_actions"
        ).fetchone()
        assert row["attempt_count"] == 2          # fire_seq wins over attempts
        assert result.scratchpad_patch["fire_seq"] == 3   # self-bumped

    def test_priority_class_stamped_from_scratchpad(self, workflow_db, base_run: Run):
        # WS7/2c: reserve marker in scratchpad → row.priority_class='reserve'.
        base_run.scratchpad = {"priority_class": "reserve"}
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute("SELECT priority_class FROM wf_pending_actions").fetchone()
        assert row["priority_class"] == "reserve"

    def test_priority_class_defaults_general(self, workflow_db, base_run: Run):
        base_run.scratchpad = {}     # no marker → general
        node = _node("FIRE_VB_CALL", {})
        with transaction(workflow_db):
            fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        row = workflow_db.execute("SELECT priority_class FROM wf_pending_actions").fetchone()
        assert row["priority_class"] == "general"

    def test_fire_seq_same_node_two_attempts_distinct_rows(self, workflow_db, base_run: Run):
        # Two fires at the SAME node with the bumped fire_seq → two distinct rows
        # (no dedupe-collision) — the core same-day-retry guarantee.
        node = _node("FIRE_VB_CALL", {})
        base_run.scratchpad = {"fire_seq": 0}
        with transaction(workflow_db):
            r1 = fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        base_run.scratchpad["fire_seq"] = r1.scratchpad_patch["fire_seq"]  # 1
        with transaction(workflow_db):
            fire_mod.execute(node, base_run, ctx=None, txn=workflow_db)
        count = workflow_db.execute(
            "SELECT COUNT(*) AS c FROM wf_pending_actions"
        ).fetchone()["c"]
        assert count == 2  # attempt_count 0 and 1 — both fired

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

    def test_timeout_no_txn_reparks_not_no_connect(self, base_run: Run):
        # WS3: past deadline but txn=None (can't confirm a fire) AND within the
        # MAX_AWAIT bound → CONSERVATIVE re-park, NEVER a false no-connect. The
        # genuine no-connect path (fired >=90m) + the starved-escalate path are
        # covered in TestAwaitRequeue with a real txn.
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(minutes=90)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": 1}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge is None       # re-parked (within MAX_AWAIT)
        assert base_run.status == "WAITING"

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
        # Bad timeout_hours → 24h fallback (no crash). Entered 90m ago → with the
        # 24h fallback the deadline is still in the future → normal park.
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(minutes=90)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {}
        result = await_mod.execute(
            _node("AWAIT_DISPOSITION", {"timeout_hours": "garbage"}),
            base_run, ctx=None, txn=None,
        )
        assert result.next_edge is None       # parked at the 24h-fallback deadline
        assert base_run.status == "WAITING"


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
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → future deadline → park
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
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → +3h is future → park
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

    # ---- WS4 best-hour rotation ----
    def _rotate_node(self, day_offset=1, **extra):
        cfg = {
            "rotate_day_offset": day_offset,
            "rotate_hours_key": "best_hours",
            "rotate_index_key": "attempts",
        }
        cfg.update(extra)
        return _node("WAIT_UNTIL", cfg)

    def test_rotate_picks_indexed_hour_tomorrow(self, base_run: Run):
        # attempts=1 → best_hours[1]=14; day_offset=1 → parks tomorrow 14:00.
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → tomorrow is future → park
        base_run.scratchpad = {"best_hours": [9, 14, 17], "attempts": 1}
        result = wait_mod.execute(self._rotate_node(), base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        assert parsed.hour == 14
        assert base_run.status == "WAITING"

    def test_rotate_wraps_index(self, base_run: Run):
        # attempts=4, 3 hours → 4 % 3 = 1 → best_hours[1]=14.
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → tomorrow is future → park
        base_run.scratchpad = {"best_hours": [9, 14, 17], "attempts": 4}
        result = wait_mod.execute(self._rotate_node(), base_run, ctx=None, txn=None)
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        assert parsed.hour == 14

    def test_rotate_reset_keys_applied(self, base_run: Run):
        # Day rollover resets attempts_today=0 via reset_keys on the exit edge.
        base_run.scratchpad = {"best_hours": [9, 14, 17], "attempts": 0,
                               "attempts_today": 2}
        node = self._rotate_node(reset_keys={"attempts_today": 0})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.scratchpad_patch.get("attempts_today") == 0

    def test_rotate_fallback_when_best_hours_absent(self, base_run: Run):
        # No best_hours → population default [10,13,16]; attempts=0 → 10:00.
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → tomorrow is future → park
        base_run.scratchpad = {"attempts": 0}
        result = wait_mod.execute(self._rotate_node(), base_run, ctx=None, txn=None)
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        assert parsed.hour == 10

    def test_rotate_ignores_out_of_window_hours(self, base_run: Run):
        # 23 is outside [8,18] → filtered; remaining [9,16]; attempts=1 → 16.
        base_run.entered_node_at_ist = _ist_now_str()  # fresh entry → tomorrow is future → park
        base_run.scratchpad = {"best_hours": [23, 9, 16], "attempts": 1}
        result = wait_mod.execute(self._rotate_node(), base_run, ctx=None, txn=None)
        parsed = datetime.strptime(result.ready_at_ist, "%Y-%m-%d %H:%M:%S")
        # after filtering 23: [9,16]; index 1 % 2 = 1 → 16
        assert parsed.hour == 16

    def test_rotate_ambiguous_config_errors(self, base_run: Run):
        # rotate + relative both set → error edge.
        node = _node("WAIT_UNTIL", {"rotate_day_offset": 1, "relative": "T+1 day"})
        result = wait_mod.execute(node, base_run, ctx=None, txn=None)
        assert result.next_edge == "error"

    # ---- Treadmill regression (the production bug this fix closes) ----
    # A forward relative/rotate offset anchored to `now` re-parks forever because
    # the handler only re-runs once the prior deadline lands, at which point
    # now+offset is again future. Anchoring to entered_node_at_ist makes the
    # deadline fixed, so once `offset` has elapsed since entry the run ADVANCES.
    def test_relative_hour_treadmill_breaks(self, base_run: Run):
        # Entered 2h ago, T+1 hour → deadline (entry+1h) is now 1h in the PAST
        # → must advance (ACTIVE, ready_at_ist=None), NOT re-park.
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(hours=2)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        result = wait_mod.execute(_node("WAIT_UNTIL", {"relative": "T+1 hour"}), base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        assert result.ready_at_ist is None
        assert base_run.status == "ACTIVE"

    def test_rotate_treadmill_breaks(self, base_run: Run):
        # Entered 3 days ago, day_offset=0, best_hour=18:00. Clock frozen to 08:30
        # so the discriminator is the ANCHOR, not the wall-clock:
        #   OLD (now-anchored): target = TODAY 18:00 → future → PARK (regression).
        #   NEW (entry-anchored): target = 3-days-ago 18:00 → past → ADVANCE.
        # Without the freeze this test is vacuous (day_offset=0 + now-anchor lands
        # on today, already past during most call-window hours).
        from unittest.mock import patch
        anchor = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None) - timedelta(days=3)
        base_run.entered_node_at_ist = anchor.strftime("%Y-%m-%d %H:%M:%S")
        base_run.scratchpad = {"best_hours": [18], "attempts": 0}
        fake_now = datetime.now(ZoneInfo("Asia/Kolkata")).replace(
            hour=8, minute=30, second=0, microsecond=0, tzinfo=None)
        with patch.object(wait_mod, "_now_ist", return_value=fake_now):
            result = wait_mod.execute(self._rotate_node(day_offset=0), base_run, ctx=None, txn=None)
        assert result.next_edge == "next"
        assert result.ready_at_ist is None
        assert base_run.status == "ACTIVE"


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


# --------------------------------------------------------------------------
# SAME_DAY_GATE (WS3 same-day retry)
# --------------------------------------------------------------------------


class TestSameDayGate:
    def _node(self, **cfg):
        base = {"attempts_today_key": "attempts_today", "max_per_day": 3,
                "min_gap_hours": 1, "window_close_hour": 19}
        base.update(cfg)
        return _node("SAME_DAY_GATE", base)

    def _at(self, monkeypatch, hour):
        from datetime import datetime as _dt
        monkeypatch.setattr(sdg_mod, "_now_ist",
                            lambda: _dt(2026, 6, 5, hour, 0, 0))

    def test_retry_today_when_budget_and_room(self, monkeypatch, base_run: Run):
        self._at(monkeypatch, 12)               # midday → room before 19:00
        base_run.scratchpad = {"attempts_today": 0}
        r = sdg_mod.execute(self._node(), base_run, ctx=None, txn=None)
        assert r.next_edge == "retry_today"
        # P2-2: also resets priority_class→general (no-connect reattempt isn't reserve)
        assert r.scratchpad_patch == {"attempts_today": 1, "priority_class": "general"}

    def test_next_day_when_budget_spent(self, monkeypatch, base_run: Run):
        # max_per_day=3 → max retries=2; attempts_today=2 → no budget.
        self._at(monkeypatch, 12)
        base_run.scratchpad = {"attempts_today": 2}
        r = sdg_mod.execute(self._node(), base_run, ctx=None, txn=None)
        assert r.next_edge == "next_day"
        assert r.scratchpad_patch == {"priority_class": "general"}

    def test_low_catchall_stays_low_on_retry(self, monkeypatch, base_run: Run):
        # 2026-06-07 owner rule: a no-connected one_time (priority_class='low')
        # row must NOT be promoted into the eligible 750-capped tier on its
        # same-day reattempt — it stays 'low'.
        self._at(monkeypatch, 12)
        base_run.scratchpad = {"attempts_today": 0, "priority_class": "low"}
        r = sdg_mod.execute(self._node(), base_run, ctx=None, txn=None)
        assert r.next_edge == "retry_today"
        assert r.scratchpad_patch == {"attempts_today": 1, "priority_class": "low"}

    def test_low_catchall_stays_low_on_next_day(self, monkeypatch, base_run: Run):
        # Same invariant on the next_day edge (budget spent).
        self._at(monkeypatch, 12)
        base_run.scratchpad = {"attempts_today": 2, "priority_class": "low"}
        r = sdg_mod.execute(self._node(), base_run, ctx=None, txn=None)
        assert r.next_edge == "next_day"
        assert r.scratchpad_patch == {"priority_class": "low"}

    def test_next_day_when_window_closing(self, monkeypatch, base_run: Run):
        # 18:30 + 1h = 19:30 → past 19:00 → no room today → next_day.
        self._at(monkeypatch, 18)
        # set minute via a custom now
        from datetime import datetime as _dt
        monkeypatch.setattr(sdg_mod, "_now_ist", lambda: _dt(2026, 6, 5, 18, 30, 0))
        base_run.scratchpad = {"attempts_today": 0}
        r = sdg_mod.execute(self._node(), base_run, ctx=None, txn=None)
        assert r.next_edge == "next_day"

    def test_retry_caps_at_three_total(self, monkeypatch, base_run: Run):
        # Walk the budget: 0→retry(1), 1→retry(2), 2→next_day. So 2 retries =
        # 3 total calls/day (1 initial + 2), matching ≤3/day.
        self._at(monkeypatch, 10)
        edges = []
        sp = {"attempts_today": 0}
        for _ in range(3):
            r = sdg_mod.execute(self._node(), Run(
                id=1, workflow_id=1, version_id=1, customer_id="c",
                current_node_id="n", scratchpad=dict(sp), status="ACTIVE",
                entered_node_at_ist="2026-06-05 10:00:00"), ctx=None, txn=None)
            edges.append(r.next_edge)
            sp.update(r.scratchpad_patch)
        assert edges == ["retry_today", "retry_today", "next_day"]

    def test_bad_config_routes_error(self, base_run: Run):
        r = sdg_mod.execute(_node("SAME_DAY_GATE", {"max_per_day": "x"}),
                            base_run, ctx=None, txn=None)
        assert r.next_edge == "error"


# --------------------------------------------------------------------------
# AWAIT_DISPOSITION 60-min requeue (WS3 P0-3)
# --------------------------------------------------------------------------


def _seed_fired(conn, run_id, fired_at_ist, customer_id="c1"):
    _seed_action(conn, run_id, status="FIRED", fired_at_ist=fired_at_ist,
                 customer_id=customer_id)


def _seed_action(conn, run_id, *, status, fired_at_ist=None, customer_id="c1",
                 attempt_count=0):
    with transaction(conn):
        conn.execute(
            "INSERT INTO wf_pending_actions (run_id, node_id, attempt_count, "
            "customer_id, scheduled_at_ist, status, created_at_ist, fired_at_ist) "
            "VALUES (?, 'fire', ?, ?, ?, ?, ?, ?)",
            (run_id, attempt_count, customer_id, fired_at_ist or _now_str_h(),
             status, fired_at_ist or _now_str_h(), fired_at_ist),
        )


def _now_str_h():
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _Z
    return _dt.now(_Z("Asia/Kolkata")).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


class TestAwaitRequeue:
    def _past(self, minutes):
        from datetime import datetime as _dt, timedelta as _td
        from zoneinfo import ZoneInfo as _Z
        return (_dt.now(_Z("Asia/Kolkata")).replace(tzinfo=None)
                - _td(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")

    def _run(self, entered_minutes_ago):
        return Run(id=555, workflow_id=1, version_id=1, customer_id="c1",
                   current_node_id="await", scratchpad={}, status="WAITING",
                   entered_node_at_ist=self._past(entered_minutes_ago))

    def test_timeout_not_fired_reparks(self, workflow_db):
        # Entered 120m ago (timeout elapsed), but NO FIRED row → still queued →
        # re-park (no edge), NOT a no-connect.
        run = self._run(120)
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge is None            # parked
        assert run.status == "WAITING"

    def test_timeout_fired_recently_reparks(self, workflow_db):
        # Fired 30m ago (< 90m SLA) but timeout elapsed → disposition in-flight
        # → re-park, do NOT route no-connect.
        run = self._run(120)
        _seed_fired(workflow_db, 555, self._past(30))
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge is None
        assert run.status == "WAITING"

    def test_timeout_fired_past_sla_routes_no_connect(self, workflow_db):
        # Fired 100m ago (>= 90m SLA), still silent → genuine no-connect → timeout edge.
        run = self._run(120)
        _seed_fired(workflow_db, 555, self._past(100))
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge == "timeout"
        assert run.status == "ACTIVE"

    def test_disposition_still_wins_over_requeue(self, workflow_db):
        # A returned disposition routes 'disposition' regardless of timers.
        run = self._run(120)
        run.scratchpad = {"last_disposition_action_class": "RETRY"}
        _seed_fired(workflow_db, 555, self._past(100))
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge == "disposition"

    def test_timeout_suppressed_escalates(self, workflow_db):
        # The queued fire was SUPPRESSED (cooldown/cap/window) → will never
        # happen → escalate via 'timeout', NOT park forever (P0-1 half2 / P1-1).
        run = self._run(120)
        _seed_action(workflow_db, 555, status="SUPPRESSED")
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge == "timeout"
        assert run.status == "ACTIVE"

    def test_timeout_starved_escalates(self, workflow_db, monkeypatch):
        # Never fired (no row / stuck PENDING) but waited past MAX_AWAIT → escalate
        # so the run can't park at AWAIT forever (P0-1 second half).
        monkeypatch.setenv("WF_MAX_AWAIT_HOURS", "2")
        run = self._run(180)   # entered 3h ago > 2h MAX
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge == "timeout"
        assert run.status == "ACTIVE"

    def test_pending_within_bound_reparks(self, workflow_db):
        # Queued PENDING (not yet fired), within MAX_AWAIT → re-park, not escalate.
        run = self._run(120)
        _seed_action(workflow_db, 555, status="PENDING")
        node = _node("AWAIT_DISPOSITION", {"timeout_hours": 1})
        r = await_mod.execute(node, run, ctx=None, txn=workflow_db)
        assert r.next_edge is None
        assert run.status == "WAITING"
