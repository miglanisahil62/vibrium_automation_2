"""Generate seed_workflows/vb_collections_v2.json from a SEGMENT REGISTRY.

Run from the vibrium-workflow repo root:
    python seed_workflows/generate_vb_collections_v2.py

Writes vb_collections_v2.json in the same directory.

Design — segment-first, declarative, dynamic
---------------------------------------------
The whole calling strategy lives in one table: ``SEGMENTS``. Each row is a
segment with TWO parts:

  * ``match``        — a boolean expression (simpleeval) over the customer's
                       fetched CleverTap properties. This is the membership
                       rule: "who is in this segment".
  * calling strategy — ``total_calls`` + ``entry_offset_days`` + ``entry_time``.

The generator turns that table into the graph automatically:

    ENROLL → FETCH_CT_PROPS → CONDITION(dpd>=1)
        → classify chain: CONDITION(match_0) → CONDITION(match_1) → ...
              each true → that segment's entry WAIT_UNTIL → its call loop
              all false → TERMINATE OUT_OF_SCOPE
        → per-segment call loop (FIRE → AWAIT → BRANCH → retries/assign/term)

Classification is FIRST-MATCH-WINS, top to bottom. Put specific rules above
broad ones. A CONDITION whose expression errors (e.g. a referenced property is
None) routes to the NEXT segment — a missing field just means "not this
segment", never a crash.

To ADD or MODIFY a segment: edit the ``SEGMENTS`` list, re-run this script,
re-seed. No node is hand-edited. New segments may key on ANY fetched property
(see ``FETCH_PROPERTIES`` — properties are fetched as OPTIONAL so a customer
missing one field is not dropped).

Call-count model (see _call_loop): the initial FIRE does not touch the retry
COUNTER; only a RETRY disposition does, and it stops at ``incremented >=
limit``. So total VB fires on the RETRY path == COUNTER limit == ``total_calls``
(``total_calls == 1`` uses no COUNTER — first RETRY assigns to an agent).
"""
from __future__ import annotations
import json
import pathlib

# ─── Node-id helpers ─────────────────────────────────────────────────────────
# Root/classification nodes use the 00 prefix. Each segment's call loop gets its
# own one-byte prefix (a0, a1, ...) derived from its index.

ROOT = "00000000-0000-4000-a000-0000000000{:02x}"


def r(n: int) -> str:
    return ROOT.format(n)


def _seg_prefix(index: int) -> str:
    """Per-segment UUID prefix template, e.g. index 0 -> 'a0000000-...-{:02x}'."""
    return f"{0xa0 + index:02x}000000-0000-4000-a000-0000000000{{:02x}}"


# ─── Properties fetched once at FETCH_CT_PROPS ──────────────────────────────
# dpd is REQUIRED (it gates eligibility and collection_view guarantees it for
# the DPD-1 cohort). The classification properties are OPTIONAL ('?' suffix):
# a customer missing one (e.g. an HNWA customer has no coll_bot_calling) is NOT
# dropped as FETCH_FAILED — the absent prop becomes None and simply fails to
# match any segment rule that references it.
FETCH_PROPERTIES = {
    "dpd": "int",
    "coll_collection_risk_segmentation": "int?",
    "coll_notification_replied": "str?",
    "coll_bot_calling": "str?",
}

# ─── Enrollment source (read by the enrollment poller) ───────────────────────
# The 07:30 fetch writes a date-stamped CSV; the poller expands {csv_dir}
# (env WF_ENROLLMENT_CSV_DIR, default <repo>/state/enrollment) and {YYYY-MM-DD}
# (today IST). A missing dated file → the poller skips loud (never re-uses a
# stale cohort). The enrollment-side gate is just dpd >= 1 (freshness re-check
# matching the graph's own gate); SEGMENT membership is decided IN the graph,
# so segments stay defined in exactly one place — this table. Customers who
# match no segment terminate OUT_OF_SCOPE in-graph.
ENROLL_SOURCE_CSV = "{csv_dir}/dpd1_candidates_{YYYY-MM-DD}.csv"
ENROLL_CONDITION_EXPR = "dpd >= 1"

# ─── THE SEGMENT REGISTRY ────────────────────────────────────────────────────
# Edit this table to add / modify segments. Order matters: first match wins, so
# specific rules (exact coll_bot_calling) sit above the broad risk-rule fallback.
#
#   name              membership rule (match)                              calls  first call
SEGMENTS: list[dict] = [
    {
        "name": "high_wa",
        "match": "coll_bot_calling == 'ai_vb_calling_highv1'",
        "total_calls": 2,
        "entry_offset_days": 1,   # T+1 — day AFTER criteria met
        "entry_time": "08:00",
    },
    {
        "name": "mid_wa",
        "match": "coll_bot_calling == 'ai_vb_calling_midv1'",
        "total_calls": 2,
        "entry_offset_days": 1,
        "entry_time": "08:00",
    },
    {
        "name": "mid_nowa_1",
        "match": "coll_bot_calling == 'ai_vb_calling_mid_v2'",
        "total_calls": 2,
        "entry_offset_days": 0,   # T+0 per VB_Prompt_Doc (1): "day they fulfil"
        "entry_time": "08:00",
    },
    {
        "name": "mid_nowa_2",
        "match": "coll_bot_calling == 'ai_vb_calling_mid_v3'",
        "total_calls": 3,
        "entry_offset_days": 0,   # T+0 — same day
        "entry_time": "08:00",
    },
    {
        "name": "mid_nowa_3",
        "match": "coll_bot_calling == 'ai_vb_calling_mid_v4'",
        "total_calls": 1,
        "entry_offset_days": 0,
        "entry_time": "08:00",
    },
    {
        "name": "low_wa",
        "match": "coll_bot_calling == 'ai_vb_calling_lowv1'",
        "total_calls": 3,
        "entry_offset_days": 1,   # T+1
        "entry_time": "08:00",
    },
    {
        "name": "low_nowa_1",
        "match": "coll_bot_calling == 'ai_vb_calling_lowv2'",
        "total_calls": 3,
        "entry_offset_days": 0,   # T+0
        "entry_time": "08:00",
    },
    {
        "name": "low_nowa_2",
        "match": "coll_bot_calling == 'ai_vb_calling_lowv3'",
        "total_calls": 4,         # first 4-call segment
        "entry_offset_days": 0,   # T+0
        "entry_time": "08:00",
    },
    {
        # High-risk + WhatsApp-unavailable. No coll_bot_calling value — defined
        # purely by risk + notification fields. This is the broad fallback; it
        # sits LAST so the specific coll_bot_calling rules win first.
        "name": "high_nowa",
        # None-guard FIRST: coll_collection_risk_segmentation is fetched OPTIONAL
        # and can be None. `None < 5` raises TypeError in simpleeval (→ the
        # CONDITION error edge → mis-routes to OUT_OF_SCOPE). The `!= None`
        # left-guard short-circuits the `and` before the comparison. The
        # seed-time lint in main() enforces this guard for every ordering rule.
        "match": "coll_collection_risk_segmentation != None and coll_collection_risk_segmentation < 5 and coll_notification_replied == 'WA_Unavailable'",
        "total_calls": 2,
        "entry_offset_days": 0,
        "entry_time": "08:00",
    },
]

# Root-node index map (single-byte). Classification + entry-wait nodes are
# placed in dedicated bands so up to 16 segments fit without collision.
_IDX_ENROLL          = 0x01
_IDX_FETCH           = 0x02
_IDX_TERM_FETCH_FAIL = 0x03
_IDX_DPD_COND        = 0x04
_IDX_TERM_INELIGIBLE = 0x05
_IDX_TERM_OOS        = 0x06
_IDX_TERM_WAIT_ERR   = 0x07
_CLASSIFY_BASE       = 0x10   # classify_i at 0x10 + i
_ENTRY_WAIT_BASE     = 0x20   # enter_wait_i at 0x20 + i


def _call_loop(prefix_fmt: str, label_prefix: str, total_calls: int) -> list[dict]:
    """Build the call-loop nodes for one segment.

    total_calls = max VB call attempts on the RETRY path (== COUNTER limit).
        total_calls == 1  → no COUNTER; first RETRY routes straight to
                            ASSIGN_AGENT (one call, then assign).
        total_calls >= 2  → COUNTER limit = total_calls; (total_calls - 1)
                            re-fires allowed before assigning.

    Node index map (hex, within the segment prefix):
        01 FIRE_VB_CALL          02 AWAIT_DISPOSITION   03 BRANCH_ON_DISPOSITION
        04 TERMINATE PAID        05 WAIT_PTP            06 WAIT_EOD
        07 WAIT_CALLBACK         08 ASSIGN_ESC          09 COUNTER (if multi-call)
        0a WAIT_RETRY (if multi) 0b ASSIGN_LIMIT        0c ASSIGN_RTP
        0d ASSIGN_DEFAULT        0e ASSIGN_TIMEOUT      0f TERMINATE ASSIGNED
        10 TERMINATE GATE_SUPPRESSED
    """

    def n(idx: int) -> str:
        return prefix_fmt.format(idx)

    FIRE            = n(0x01)
    AWAIT           = n(0x02)
    BRANCH          = n(0x03)
    TERM_PAID       = n(0x04)
    WAIT_PTP        = n(0x05)
    WAIT_EOD        = n(0x06)
    WAIT_CB         = n(0x07)
    ASSIGN_ESC      = n(0x08)
    COUNTER_NODE    = n(0x09)
    WAIT_RETRY      = n(0x0a)
    ASSIGN_LIMIT    = n(0x0b)
    ASSIGN_RTP      = n(0x0c)
    ASSIGN_DEF      = n(0x0d)
    ASSIGN_TIMEOUT  = n(0x0e)
    TERM_ASSIGNED   = n(0x0f)
    TERM_SUPPRESSED = n(0x10)

    p = label_prefix
    use_counter = total_calls >= 2
    retry_target = COUNTER_NODE if use_counter else ASSIGN_LIMIT

    nodes = [
        {
            "node_id": FIRE,
            "type": "FIRE_VB_CALL",
            "label": f"{p}: Fire VB Call",
            "config": {},
            "edges": {"queued": AWAIT, "suppressed": TERM_SUPPRESSED},
        },
        {
            "node_id": AWAIT,
            "type": "AWAIT_DISPOSITION",
            "label": f"{p}: Await Disposition",
            "config": {"timeout_hours": 24},
            "edges": {"disposition": BRANCH, "timeout": ASSIGN_TIMEOUT},
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
                "retry":          retry_target,
                "rtp":            ASSIGN_RTP,
                "default_assign": ASSIGN_DEF,
                "default":        ASSIGN_DEF,
                "error":          ASSIGN_DEF,
            },
        },
        {
            "node_id": TERM_PAID,
            "type": "TERMINATE",
            "label": f"{p}: Terminate — Paid",
            # NOTE: NOOP is produced by the bot for paid / part-payment AND for
            # vib=False (call never placed). The enrollment gate MUST filter
            # vib=False before graph entry, or a vib=False NOOP wrongly
            # terminates as PAID.
            "config": {"status": "PAID"},
            "edges": {},
        },
        {
            "node_id": WAIT_PTP,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — PTP date",
            "config": {"relative": "T+3 day at 08:00"},
            "edges": {"next": FIRE, "error": ASSIGN_LIMIT},
        },
        {
            "node_id": WAIT_EOD,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — EOD call",
            "config": {"relative": "T+0 day at 18:00"},
            "edges": {"next": FIRE, "error": ASSIGN_LIMIT},
        },
        {
            "node_id": WAIT_CB,
            "type": "WAIT_UNTIL",
            "label": f"{p}: Wait — Callback",
            "config": {"relative": "T+1 day at 08:00"},
            "edges": {"next": FIRE, "error": ASSIGN_LIMIT},
        },
        {
            "node_id": ASSIGN_ESC,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Escalation",
            "config": {"reason": "dispute_or_nrp"},
            "edges": {"next": TERM_ASSIGNED, "error": TERM_ASSIGNED},
        },
    ]

    if use_counter:
        nodes += [
            {
                "node_id": COUNTER_NODE,
                "type": "COUNTER",
                "label": f"{p}: Retry Counter (max {total_calls} calls)",
                "config": {"name": "attempts", "limit": total_calls},
                "edges": {
                    "under_limit": WAIT_RETRY,
                    "at_limit":    ASSIGN_LIMIT,
                    "error":       ASSIGN_LIMIT,
                },
            },
            {
                "node_id": WAIT_RETRY,
                "type": "WAIT_UNTIL",
                "label": f"{p}: Wait — Retry T+1",
                "config": {"relative": "T+1 day at 08:00"},
                "edges": {"next": FIRE, "error": ASSIGN_LIMIT},
            },
        ]
    # else total_calls == 1: BRANCH "retry" edge already points to ASSIGN_LIMIT.

    nodes += [
        {
            "node_id": ASSIGN_LIMIT,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Max Attempts",
            "config": {"reason": "max_attempts_reached"},
            "edges": {"next": TERM_ASSIGNED, "error": TERM_ASSIGNED},
        },
        {
            "node_id": ASSIGN_RTP,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — RTP Review",
            "config": {"reason": "rtp_needs_review"},
            "edges": {"next": TERM_ASSIGNED, "error": TERM_ASSIGNED},
        },
        {
            "node_id": ASSIGN_DEF,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Unhandled Disposition",
            "config": {"reason": "unhandled_disposition"},
            "edges": {"next": TERM_ASSIGNED, "error": TERM_ASSIGNED},
        },
        {
            "node_id": ASSIGN_TIMEOUT,
            "type": "ASSIGN_AGENT",
            "label": f"{p}: Assign — Disposition Timeout",
            "config": {"reason": "disposition_timeout_24h"},
            "edges": {"next": TERM_ASSIGNED, "error": TERM_ASSIGNED},
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
    return nodes


def build_graph() -> dict:
    n_seg = len(SEGMENTS)
    if n_seg == 0:
        raise SystemExit("SEGMENTS registry is empty — nothing to build")
    if n_seg > 16:
        # classify band 0x10-0x1f and entry-wait band 0x20-0x2f each hold 16;
        # segment prefixes a0-af hold 16. Beyond that the index scheme collides.
        raise SystemExit(f"too many segments ({n_seg}); max 16 with this id scheme")

    # ── Root section ────────────────────────────────────────────────────────
    nodes = [
        {
            "node_id": r(_IDX_ENROLL),
            "type": "ENROLL",
            "label": "Enroll",
            # The poller reads this node's config as enrollment_trigger_config.
            "config": {
                "source_csv": ENROLL_SOURCE_CSV,
                "condition_expr": ENROLL_CONDITION_EXPR,
            },
            "edges": {"next": r(_IDX_FETCH)},
        },
        {
            "node_id": r(_IDX_FETCH),
            "type": "FETCH_CT_PROPS",
            "label": "Fetch CT Properties",
            "config": {"properties": dict(FETCH_PROPERTIES)},
            "edges": {
                "success":   r(_IDX_DPD_COND),
                "not_found": r(_IDX_TERM_FETCH_FAIL),
                "error":     r(_IDX_TERM_FETCH_FAIL),
            },
        },
        {
            "node_id": r(_IDX_TERM_FETCH_FAIL),
            "type": "TERMINATE",
            "label": "Terminate — Fetch Failed",
            "config": {"status": "FETCH_FAILED"},
            "edges": {},
        },
        {
            "node_id": r(_IDX_DPD_COND),
            "type": "CONDITION",
            # Targeting is DPD-1 (collection_view ageing == 1, selected at the
            # 07:30 fetch). dpd >= 1 here is a freshness check: route only
            # still-overdue customers onward; dpd == 0 (cured) -> INELIGIBLE.
            "label": "DPD >= 1 (still overdue)?",
            "config": {"expr": "dpd >= 1"},
            "edges": {
                "true":  r(_CLASSIFY_BASE),   # first segment's classify node
                "false": r(_IDX_TERM_INELIGIBLE),
                "error": r(_IDX_TERM_INELIGIBLE),
            },
        },
        {
            "node_id": r(_IDX_TERM_INELIGIBLE),
            "type": "TERMINATE",
            "label": "Terminate — Ineligible (DPD < 1 / cured)",
            "config": {"status": "INELIGIBLE"},
            "edges": {},
        },
        {
            "node_id": r(_IDX_TERM_OOS),
            "type": "TERMINATE",
            "label": "Terminate — Out of Scope (no segment matched)",
            "config": {"status": "OUT_OF_SCOPE"},
            "edges": {},
        },
        {
            "node_id": r(_IDX_TERM_WAIT_ERR),
            "type": "TERMINATE",
            "label": "Terminate — Entry Wait Error",
            "config": {"status": "ENTRY_WAIT_ERROR"},
            "edges": {},
        },
    ]

    # ── Classification chain + entry waits + per-segment call loops ──────────
    for i, seg in enumerate(SEGMENTS):
        classify_id   = r(_CLASSIFY_BASE + i)
        entry_wait_id = r(_ENTRY_WAIT_BASE + i)
        seg_prefix    = _seg_prefix(i)
        seg_fire      = seg_prefix.format(0x01)

        # On no-match (false) OR eval-error, fall through to the NEXT segment's
        # classify; the last segment falls through to OUT_OF_SCOPE. Treating a
        # CONDITION error as "not this segment" means a missing/None property
        # referenced by the rule never crashes the run — it just doesn't match.
        is_last = i == n_seg - 1
        fallthrough = r(_IDX_TERM_OOS) if is_last else r(_CLASSIFY_BASE + i + 1)

        offset = int(seg["entry_offset_days"])
        entry_relative = f"T+{offset} day at {seg['entry_time']}"

        nodes.append({
            "node_id": classify_id,
            "type": "CONDITION",
            "label": f"Classify: {seg['name']}?",
            "config": {"expr": seg["match"]},
            "edges": {
                "true":  entry_wait_id,
                "false": fallthrough,
                "error": fallthrough,
            },
        })
        nodes.append({
            "node_id": entry_wait_id,
            "type": "WAIT_UNTIL",
            "label": f"{seg['name']} — Entry Wait ({entry_relative})",
            "config": {"relative": entry_relative},
            "edges": {
                "next":  seg_fire,
                "error": r(_IDX_TERM_WAIT_ERR),
            },
        })
        nodes += _call_loop(seg_prefix, seg["name"], int(seg["total_calls"]))

    return {"nodes": nodes}


def _lint_segment_matches() -> None:
    """Seed-time guard on the SEGMENTS registry.

    Every segment's ``match`` must EVALUATE (not raise) against BOTH:
      * a fully-populated scratchpad (all fetched props present, typed), and
      * a scratchpad where every OPTIONAL property is None.

    This catches three failure classes before the graph is ever seeded:
      1. The None-in-ordering-comparison trap (e.g. `risk < 5` when risk is an
         absent OPTIONAL → `None < 5` raises → mis-routes to OUT_OF_SCOPE).
         Fix: guard with `prop != None and prop < 5`.
      2. A typo / bad operator that makes a rule ALWAYS error (it would match
         zero customers forever, silently).
      3. A rule referencing a property NOT in FETCH_PROPERTIES (NameNotDefined),
         which would never be populated.

    Uses the same evaluator contract as the CONDITION handler (names=scratchpad,
    functions={}) so the lint sees exactly what runtime sees.
    """
    import simpleeval
    from simpleeval import SimpleEval

    simpleeval.MAX_STRING_LENGTH = 1024
    simpleeval.MAX_POWER = 100

    _dummies = {"int": 1, "float": 1.0, "str": "x", "bool": True}

    all_present: dict = {}
    opt_none: dict = {}
    for prop, ptype in FETCH_PROPERTIES.items():
        optional = isinstance(ptype, str) and ptype.endswith("?")
        base = ptype[:-1] if optional else ptype
        dummy = _dummies.get(base, "x")
        all_present[prop] = dummy
        opt_none[prop] = None if optional else dummy

    failures: list[str] = []
    for seg in SEGMENTS:
        for label, sp in (("all-present", all_present), ("optional-None", opt_none)):
            try:
                SimpleEval(names=dict(sp), functions={}).eval(seg["match"])
            except Exception as exc:  # noqa: BLE001 — any raise = a bad rule
                failures.append(
                    f"  segment '{seg['name']}' match RAISED on {label} scratchpad: "
                    f"{type(exc).__name__}: {exc}\n    expr: {seg['match']}"
                )
    if failures:
        print("FAIL — segment match expressions that raise at runtime "
              "(fix the rule, add a `!= None` guard, or only reference fetched props):")
        for f in failures:
            print(f)
        raise SystemExit(1)


def main() -> None:
    _lint_segment_matches()
    graph = build_graph()
    node_count = len(graph["nodes"])

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
        print("FAIL — broken edges:")
        for b in broken:
            print(b)
        raise SystemExit(1)

    seen: set[str] = set()
    dupes: list[str] = []
    for node in graph["nodes"]:
        nid = node["node_id"]
        if nid in seen:
            dupes.append(f"  {nid} ({node['label']})")
        seen.add(nid)
    if dupes:
        print("FAIL — duplicate node_ids:")
        for d in dupes:
            print(d)
        raise SystemExit(1)

    out = pathlib.Path(__file__).parent / "vb_collections_v2.json"
    out.write_text(json.dumps(graph, indent=2))
    print(f"OK — written {out} — {node_count} nodes, {len(SEGMENTS)} segments, 0 broken edges")
    print()
    print("Segment registry:")
    for seg in SEGMENTS:
        when = ("same day" if int(seg["entry_offset_days"]) == 0
                else f"T+{seg['entry_offset_days']}")
        print(f"  {seg['name']:12s}  {seg['total_calls']} call(s)  first {when} {seg['entry_time']}"
              f"  | match: {seg['match']}")


if __name__ == "__main__":
    main()
