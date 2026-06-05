"""Structural tests for the segment-registry generator.

Pins the invariants the audit cared about:
  * total_calls -> COUNTER limit (the fire-count knob is exact + auditable),
  * total_calls == 1 uses no COUNTER (first RETRY assigns),
  * the classification chain is first-match-wins with error->next-segment,
  * every segment match expr passes the seed-time lint,
  * the generated graph passes the engine validator.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import seed_workflows.generate_vb_collections_v2 as gen  # noqa: E402


def _loop_by_id(prefix_fmt: str, total_calls: int) -> dict:
    nodes = gen._call_loop(prefix_fmt, "seg", total_calls)
    return {n["node_id"]: n for n in nodes}


# ----------------------------------------------------------- fire-count invariant


@pytest.mark.parametrize("total_calls", [2, 3, 4])
def test_counter_limit_equals_total_calls(total_calls):
    pfx = gen._seg_prefix(0)
    by_id = _loop_by_id(pfx, total_calls)
    counter = by_id[pfx.format(0x09)]
    assert counter["type"] == "COUNTER"
    assert counter["config"]["limit"] == total_calls
    # under_limit re-fires (loops back to FIRE via WAIT_RETRY); at_limit assigns.
    assert counter["edges"]["under_limit"] == pfx.format(0x0a)   # WAIT_RETRY
    assert by_id[pfx.format(0x0a)]["edges"]["next"] == pfx.format(0x01)  # → FIRE
    assert counter["edges"]["at_limit"] == pfx.format(0x0b)      # ASSIGN_LIMIT


def test_single_call_has_no_counter_and_retry_assigns():
    pfx = gen._seg_prefix(0)
    by_id = _loop_by_id(pfx, 1)
    assert pfx.format(0x09) not in by_id   # no COUNTER node
    assert pfx.format(0x0a) not in by_id   # no WAIT_RETRY node
    branch = by_id[pfx.format(0x03)]
    # First RETRY routes straight to ASSIGN_LIMIT (one call, then assign).
    assert branch["edges"]["retry"] == pfx.format(0x0b)


# ----------------------------------------------------------- classification chain


def test_classification_is_first_match_wins_with_error_fallthrough():
    graph = gen.build_graph()
    by_id = {n["node_id"]: n for n in graph["nodes"]}
    n_seg = len(gen.SEGMENTS)
    for i in range(n_seg):
        classify = by_id[gen.r(gen._CLASSIFY_BASE + i)]
        assert classify["type"] == "CONDITION"
        # true → this segment's entry wait
        assert classify["edges"]["true"] == gen.r(gen._ENTRY_WAIT_BASE + i)
        # false AND error both fall through identically
        expected_fallthrough = (
            gen.r(gen._IDX_TERM_OOS) if i == n_seg - 1
            else gen.r(gen._CLASSIFY_BASE + i + 1)
        )
        assert classify["edges"]["false"] == expected_fallthrough
        assert classify["edges"]["error"] == expected_fallthrough


def test_entry_wait_uses_best_hour_rotation():
    """WS4: entry waits park at the customer's rotating best hour (day 1 =
    best_hours[attempts=0]), not a fixed entry_time. day_offset carries the
    segment's entry_offset_days; the index key is the existing `attempts`
    call-day counter."""
    graph = gen.build_graph()
    by_id = {n["node_id"]: n for n in graph["nodes"]}
    for i, seg in enumerate(gen.SEGMENTS):
        wait = by_id[gen.r(gen._ENTRY_WAIT_BASE + i)]
        assert wait["type"] == "WAIT_UNTIL"
        cfg = wait["config"]
        assert cfg["rotate_day_offset"] == int(seg["entry_offset_days"])
        assert cfg["rotate_hours_key"] == "best_hours"
        assert cfg["rotate_index_key"] == "attempts"
        assert "relative" not in cfg  # rotation replaced the fixed-hour form


# ----------------------------------------------------------- lint + validation


def test_seed_time_lint_passes_current_registry():
    # Must not raise for the shipped SEGMENTS.
    gen._lint_segment_matches()


def test_lint_rejects_unguarded_none_comparison(monkeypatch):
    bad = list(gen.SEGMENTS)
    # Replace high_nowa with the unguarded (buggy) form.
    bad = [
        {**s, "match": "coll_collection_risk_segmentation < 5"}
        if s["name"] == "high_nowa" else s
        for s in bad
    ]
    monkeypatch.setattr(gen, "SEGMENTS", bad)
    with pytest.raises(SystemExit):
        gen._lint_segment_matches()


def test_lint_rejects_unfetched_property_reference(monkeypatch):
    bad = list(gen.SEGMENTS) + [{
        "name": "typo_seg",
        "match": "coll_bot_callng == 'x'",   # misspelled, not in FETCH_PROPERTIES
        "total_calls": 1, "entry_offset_days": 0, "entry_time": "08:00",
    }]
    monkeypatch.setattr(gen, "SEGMENTS", bad)
    with pytest.raises(SystemExit):
        gen._lint_segment_matches()


def test_generated_graph_validates():
    from workflow.validation import validate_graph
    graph = gen.build_graph()
    res = validate_graph(graph)
    valid = res.valid if hasattr(res, "valid") else res
    assert valid is True
