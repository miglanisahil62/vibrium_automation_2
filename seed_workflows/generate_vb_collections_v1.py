"""Generate seed_workflows/vb_collections_v1.json.

Run from the vibrium-workflow repo root:
    python seed_workflows/generate_vb_collections_v1.py

Writes vb_collections_v1.json in the same directory.
"""
from __future__ import annotations
import json
import pathlib

# ── Node ID helpers ─────────────────────────────────────────────────────────
# Readable UUID4-format IDs. Version nibble = 4, variant nibble = a.

ROOT   = "00000000-0000-4000-a000-0000000000{:02x}"
HWA    = "aa000000-0000-4000-a000-0000000000{:02x}"  # High + WA loop
HNWA   = "bb000000-0000-4000-a000-0000000000{:02x}"  # High + NoWA loop
MWA    = "cc000000-0000-4000-a000-0000000000{:02x}"  # Mid + WA loop
MNWA   = "dd000000-0000-4000-a000-0000000000{:02x}"  # Mid + NoWA loop


def r(n: int) -> str:
    return ROOT.format(n)


def _call_loop(
    prefix_fmt: str,
    label_prefix: str,
) -> list[dict]:
    """Build the 16-node call loop for one branch.

    prefix_fmt — format string with one {:02x} slot, e.g. HWA
    label_prefix — human label, e.g. "High+WA"

    Node index assignments (hex):
        01 FIRE_VB_CALL
        02 AWAIT_DISPOSITION
        03 BRANCH_ON_DISPOSITION
        04 TERMINATE PAID
        05 WAIT_PTP → back to FIRE (01)
        06 WAIT_EOD → back to FIRE (01)
        07 WAIT_CALLBACK → back to FIRE (01)
        08 ASSIGN_ESC → TERM_ASSIGNED (0f)
        09 COUNTER
        0a WAIT_RETRY → back to FIRE (01)
        0b TERMINATE MAX_ATTEMPTS
        0c ASSIGN_RTP → TERM_ASSIGNED (0f)
        0d ASSIGN_DEFAULT → TERM_ASSIGNED (0f)
        0e ASSIGN_TIMEOUT → TERM_ASSIGNED (0f)
        0f TERMINATE ASSIGNED (agent pickup)
        10 TERMINATE GATE_SUPPRESSED
    """

    def n(idx: int) -> str:
        return prefix_fmt.format(idx)

    FIRE = n(0x01)
    AWAIT = n(0x02)
    BRANCH = n(0x03)
    TERM_PAID = n(0x04)
    WAIT_PTP = n(0x05)
    WAIT_EOD = n(0x06)
    WAIT_CB = n(0x07)
    ASSIGN_ESC = n(0x08)
    COUNTER = n(0x09)
    WAIT_RETRY = n(0x0a)
    TERM_MAX = n(0x0b)
    ASSIGN_RTP = n(0x0c)
    ASSIGN_DEF = n(0x0d)
    ASSIGN_TIMEOUT = n(0x0e)
    TERM_ASSIGNED = n(0x0f)
    TERM_SUPPRESSED = n(0x10)

    p = label_prefix
    return [
        {
            "node_id": FIRE,
            "type": "FIRE_VB_CALL",
            "label": f"{p}: Fire VB Call",
            "config": {},
            "edges": {
                "queued": AWAIT,
                "suppressed": TERM_SUPPRESSED,
            },
        },
        {
            "node_id": AWAIT,
            "type": "AWAIT_DISPOSITION",
            "label": f"{p}: Await Disposition",
            "config": {"timeout_hours": 24},
            "edges": {
                "disposition": BRANCH,
                "timeout": ASSIGN_TIMEOUT,
            },
        },
        {
            "node_id": BRANCH,
            "type": "BRANCH_ON_DISPOSITION",
            "label": f"{p}: Branch on Disposition",
            "config": {
                "cases": {
                    "NOOP":           "paid",
                    "PTP_CALL":       "ptp",
                    "AGREE_EOD_CALL": "eod",
                    "CALLBACK_CALL":  "callback",
                    "ESCALATE":       "escalate",
                    "RETRY":          "retry",
                    "RTP_NEEDS_LLM":  "rtp",
                },
                "default": "default_assign",
            },
            "edges": {
                "paid":           TERM_PAID,
                "ptp":            WAIT_PTP,
                "eod":            WAIT_EOD,
                "callback":       WAIT_CB,
                "escalate":       ASSIGN_ESC,
                "retry":          COUNTER,
                "rtp":            ASSIGN_RTP,
                "default_assign": ASSIGN_DEF,
            },
        },
        {
            "node_id": TERM_PAID,
            "type": "TERMINATE",
            "label": f"{p}: Terminate — Paid",
            # NOOP action_class = bot detected no outstanding balance / customer
            # has paid. Canonical mapping from ingest.py action_class vocabulary.
            # NOTE: enrollment gate must filter vib=False NOOPs before graph
            # entry — vib=False also produces NOOP but the customer was never
            # called and should not be terminated as PAID.
            "config": {"status": "PAID"},
            "edges": {},
        },
        {
            "node_id": WAIT_PTP,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — PTP date",
            "config": {"relative": "T+3 day at 08:00"},
            "edges": {
                "next": FIRE,
                "error": TERM_MAX,
            },
        },
        {
            "node_id": WAIT_EOD,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — EOD call",
            "config": {"relative": "T+0 day at 18:00"},
            "edges": {
                "next": FIRE,
                "error": TERM_MAX,
            },
        },
        {
            "node_id": WAIT_CB,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — Callback",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {
                "next": FIRE,
                "error": TERM_MAX,
            },
        },
        {
            "node_id": ASSIGN_ESC,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Escalation",
            "config": {"reason": "dispute_or_nrp"},
            "edges": {
                "next": TERM_ASSIGNED,
                "error": TERM_ASSIGNED,
            },
        },
        {
            "node_id": COUNTER,
            "type": "COUNTER",
            "label": f"{p}: Retry Counter",
            # limit=2 → at_limit fires after 2nd RETRY disposition (3 total VB fires
            # including the initial one before AWAIT_DISPOSITION).
            "config": {"name": "attempts", "limit": 2},
            "edges": {
                "under_limit": WAIT_RETRY,
                "at_limit":    TERM_MAX,
                "error":       TERM_MAX,
            },
        },
        {
            "node_id": WAIT_RETRY,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — Retry T+1",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {
                "next": FIRE,
                "error": TERM_MAX,
            },
        },
        {
            "node_id": TERM_MAX,
            "type": "TERMINATE",
            "label": f"{p}: Terminate — Max Attempts",
            "config": {"status": "MAX_ATTEMPTS"},
            "edges": {},
        },
        {
            "node_id": ASSIGN_RTP,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — RTP Review",
            "config": {"reason": "rtp_needs_review"},
            "edges": {
                "next": TERM_ASSIGNED,
                "error": TERM_ASSIGNED,
            },
        },
        {
            "node_id": ASSIGN_DEF,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Unhandled Disposition",
            "config": {"reason": "unhandled_disposition"},
            "edges": {
                "next": TERM_ASSIGNED,
                "error": TERM_ASSIGNED,
            },
        },
        {
            "node_id": ASSIGN_TIMEOUT,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Disposition Timeout",
            "config": {"reason": "disposition_timeout_24h"},
            "edges": {
                "next": TERM_ASSIGNED,
                "error": TERM_ASSIGNED,
            },
        },
        {
            "node_id": TERM_ASSIGNED,
            "type": "TERMINATE",
            "label": f"{p}: Terminate — Assigned to Agent",
            "config": {"status": "ASSIGNED_TO_AGENT"},
            "edges": {},
        },
        {
            "node_id": TERM_SUPPRESSED,
            "type": "TERMINATE",
            "label": f"{p}: Terminate — Gate Suppressed",
            "config": {"status": "GATE_SUPPRESSED"},
            "edges": {},
        },
    ]


def build_graph() -> dict:
    # ── Root nodes ──────────────────────────────────────────────────────────
    nodes = [
        {
            "node_id": r(0x01),
            "type": "ENROLL",
            "label": "Enroll",
            "config": {},
            "edges": {"next": r(0x02)},
        },
        {
            "node_id": r(0x02),
            "type": "FETCH_CT_PROPS",
            "label": "Fetch CT Properties",
            "config": {
                "properties": {
                    "dpd":                             "int",
                    "coll_collection_risk_segmentation": "int",
                    "coll_notification_replied":       "str",
                    "coll_bot_calling":                "str",
                }
            },
            "edges": {
                "success":   r(0x04),
                "not_found": r(0x03),
                "error":     r(0x03),
            },
        },
        {
            "node_id": r(0x03),
            "type": "TERMINATE",
            "label": "Terminate — Fetch Failed",
            "config": {"status": "FETCH_FAILED"},
            "edges": {},
        },
        {
            "node_id": r(0x04),
            "type": "CONDITION",
            "label": "DPD > 1?",
            "config": {"expr": "dpd > 1"},
            "edges": {
                "true":  r(0x06),
                "false": r(0x05),
                "error": r(0x05),
            },
        },
        {
            "node_id": r(0x05),
            "type": "TERMINATE",
            "label": "Terminate — Ineligible",
            "config": {"status": "INELIGIBLE"},
            "edges": {},
        },
        {
            "node_id": r(0x06),
            "type": "SWITCH",
            "label": "Risk Segmentation",
            "config": {
                "on": "coll_collection_risk_segmentation",
                # SWITCH handler coerces the scratchpad value via str() before
                # case lookup, so string keys correctly match integer CT values.
                "cases": {
                    "1": "high",
                    "2": "high",
                    "3": "high",
                    "4": "high",
                    "5": "mid",
                    "6": "mid",
                    "7": "mid",
                },
                "default": "out_of_scope",
            },
            "edges": {
                "high":        r(0x08),
                "mid":         r(0x0b),
                "out_of_scope": r(0x07),
                "error":       r(0x07),
            },
        },
        {
            "node_id": r(0x07),
            "type": "TERMINATE",
            "label": "Terminate — Out of Scope",
            "config": {"status": "OUT_OF_SCOPE"},
            "edges": {},
        },
        # ── High risk: WA branch ─────────────────────────────────────────
        {
            "node_id": r(0x08),
            "type": "SWITCH",
            "label": "High Risk — WA Status",
            "config": {
                "on": "coll_notification_replied",
                "cases": {
                    "WA_Available":   "wa",
                    "WA_Unavailable": "nowa",
                },
                "default": "nowa",
            },
            "edges": {
                "wa":    r(0x09),
                "nowa":  r(0x0a),
                "error": r(0x0a),
            },
        },
        {
            "node_id": r(0x09),
            "type": "WAIT_UNTIL",
            "label": "High+WA — Wait T+1 08:00",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {
                "next":  HWA.format(0x01),
                "error": r(0x0e),
            },
        },
        {
            "node_id": r(0x0a),
            "type": "WAIT_UNTIL",
            "label": "High+NoWA — Wait T+0 08:00",
            "config": {"relative": "T+0 day at 08:00"},
            "edges": {
                "next":  HNWA.format(0x01),
                "error": r(0x0e),
            },
        },
        # ── Mid risk: WA branch ──────────────────────────────────────────
        {
            "node_id": r(0x0b),
            "type": "SWITCH",
            "label": "Mid Risk — WA Status",
            "config": {
                "on": "coll_notification_replied",
                "cases": {
                    "WA_Available":   "wa",
                    "WA_Unavailable": "nowa",
                },
                "default": "nowa",
            },
            "edges": {
                "wa":    r(0x0c),
                "nowa":  r(0x0d),
                "error": r(0x0d),
            },
        },
        {
            "node_id": r(0x0c),
            "type": "WAIT_UNTIL",
            "label": "Mid+WA — Wait T+1 08:00",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {
                "next":  MWA.format(0x01),
                "error": r(0x0e),
            },
        },
        {
            "node_id": r(0x0d),
            "type": "WAIT_UNTIL",
            "label": "Mid+NoWA — Wait T+1 08:00",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {
                "next":  MNWA.format(0x01),
                "error": r(0x0e),
            },
        },
        # Dedicated terminal for scheduler errors during entry-phase waits.
        # Distinct from OUT_OF_SCOPE so ops dashboards can separate "not our
        # segment" (expected) from "scheduler runtime error" (unexpected/alert).
        {
            "node_id": r(0x0e),
            "type": "TERMINATE",
            "label": "Terminate — Entry Wait Error",
            "config": {"status": "ENTRY_WAIT_ERROR"},
            "edges": {},
        },
    ]

    # ── 4 call loops ─────────────────────────────────────────────────────
    nodes += _call_loop(HWA,  "High+WA")
    nodes += _call_loop(HNWA, "High+NoWA")
    nodes += _call_loop(MWA,  "Mid+WA")
    nodes += _call_loop(MNWA, "Mid+NoWA")

    return {"nodes": nodes}


def main() -> None:
    graph = build_graph()
    node_count = len(graph["nodes"])

    # Sanity: every edge target must exist as a node_id in the graph.
    node_ids = {n["node_id"] for n in graph["nodes"]}
    broken: list[str] = []
    for node in graph["nodes"]:
        for edge_label, target in node.get("edges", {}).items():
            if target not in node_ids:
                broken.append(
                    f"  {node['node_id']} ({node['label']}) "
                    f"edge '{edge_label}' → {target} NOT FOUND"
                )
    if broken:
        print("❌ Broken edges:")
        for b in broken:
            print(b)
        raise SystemExit(1)

    out = pathlib.Path(__file__).parent / "vb_collections_v1.json"
    out.write_text(json.dumps(graph, indent=2))
    print(f"✅ Written {out} — {node_count} nodes, 0 broken edges")


if __name__ == "__main__":
    main()
