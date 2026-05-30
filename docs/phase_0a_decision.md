# Phase 0a Decision — 2026-05-30

**Test customer:** 8968249 (Sandeep Singh — from `wasim_ptp_21_may.csv`)
**Mode:** Smoke test (no live triggers — per Sahil's directive)
**CT call made:** ONE `GET /1/profile.json?identity=8968249` (read-only)

---

## Finding 1: All 4 target properties exist on CT, but their **values** differ massively from VB_Prompt_Doc

| Property | VB_Prompt_Doc expects | Actual on customer 8968249 | Mismatch |
|---|---|---|---|
| `coll_collection_risk_segmentation` | numeric `<5` / `5-7` (segment buckets) | `8` (segment value) | ✓ matches type; customer is **low-risk** (>7) so they'd land in the unspec'd Low branch |
| `coll_notification_replied` | `"WA_Available"` or `"WA_Unavailable"` | `"Agent Calling"` | ❌ totally different vocabulary |
| `coll_bot_calling` | `"ai_vb_calling_highv1"`, `"ai_vb_calling_midv1"`, `"ai_vb_calling_mid_v2/v3/v4"` | `"AI Calling"` | ❌ totally different vocabulary |
| `dpd` | `>1` (numeric threshold) | `29` (numeric) | ✓ type matches |

Casing also differs from the doc:
- Doc: `COLL_collection_risk_segmentation` (uppercase prefix) — actual: `coll_collection_risk_segmentation` (lowercase).
- Doc: `DPD` (uppercase) — actual: `dpd` (lowercase).

CT property names ARE case-sensitive. The case difference is a real bug in the doc; we use the actual CT names.

---

## Finding 2: VB_Prompt_Doc values appear to be **prescriptive, not current**

The doc reads as a **proposal**: "trigger AI call when `coll_bot_calling` is updated to `ai_vb_calling_highv1`." The actual CT property today only carries simple strings like `"AI Calling"`. There is no machinery (today) that sets it to `ai_vb_calling_highv1`.

**This means the upstream pipeline that updates these CT properties does not yet exist.** Someone — presumably the team that drafted VB_Prompt_Doc — needs to either:
- (a) **Build the upstream pipeline** that segments customers by risk × WA-status and sets `coll_bot_calling` to the appropriate trigger value, OR
- (b) **Confirm that the workflow engine should be the one setting these values** (i.e., the upstream pipeline IS the new `SET_CT_PROP` node in our workflow).

If (b) is the answer, the seed workflow's entry condition must be on the **current** properties (e.g., the actual `coll_notification_replied` values + risk + dpd) — NOT on `coll_bot_calling == 'ai_vb_calling_highv1'`, because nothing sets that yet.

---

## Finding 3: This customer (DPD 29, risk 8) is in the unspec'd Low branch

The doc fully spec's High (risk <5) and Mid (risk 5–7) branches. The Low (risk ≥ 8) branch is left as a header with no content. Customer 8968249 has `risk_segmentation = 8` — i.e., the only branch the seed workflow can route them to is `TERMINATE(OUT_OF_SCOPE)`. The seed workflow won't be testable with this customer until either (a) we pick High/Mid customers for the test cohort, or (b) the doc's Low branch gets filled in.

---

## Finding 4: `coll_notification_replied = "Agent Calling"` is a "current state" indicator, not a routing key

The doc treats `coll_notification_replied` as a binary `WA_Available`/`WA_Unavailable` routing key. Actual data has values like `"Agent Calling"` — that's an **outcome** (the customer has been handed off to agent calling), not a WA-reachability flag. The mental model in the doc is wrong, OR there's a separate "WA available" indicator we need to find.

---

## Decision

**Phase 0a smoke test PASSED on technical mechanics** — CT profile GET works, 4 properties are extractable, JSON fixture pinned. But it **FAILED on design assumptions** — the VB_Prompt_Doc references values that do not exist in CT today.

**Recommended path forward (Sahil decision needed):**

| Option | Description | Cost |
|---|---|---|
| **A. Pause and clarify with the doc author** | Find who wrote VB_Prompt_Doc; ask whether the property values are existing or proposed, and what upstream sets them. | 0 dev-days; depends on response time. |
| **B. Pivot the seed workflow to use ACTUAL CT vocabulary** | Rewrite `vb_collections_v1.json` conditions using `coll_notification_replied IN ('Agent Calling', ...other-actual-values...)` and `coll_bot_calling IN ('AI Calling', ...)`. Stop assuming the doc's prescriptive values. | 1 dev-day; design owned by us. |
| **C. Make the workflow engine BOTH set and read** | Add `SET_CT_PROP coll_bot_calling=ai_vb_calling_highv1` as the FIRST node of every branch (the workflow IS the upstream pipeline). Then the doc's prescribed values become a reality. | Doable but couples journey-routing to CT-state-machine; risk of drift. |
| **D. Defer engine build; build the upstream pipeline first** | The doc is asking for "trigger WA campaign when CT property changes" — that's a different product (a CT-property-watcher → WA-fire pipeline). Build that first; engine later. | Largest scope change; reframes the project. |

**My recommendation: Option B with a side conversation per Option A.** Build the engine assuming current CT vocabulary; if/when the doc's prescribed values come online via an upstream pipeline, the workflow's CONDITION nodes just need updating in the UI. No engine code changes.

---

## What was NOT verified in Phase 0a (deferred to a later step)

- **`tag_group` column in `collection_comment_data`** — needs Redshift access. Will run separately via AWS server. If it doesn't exist, Phase 7's primary disposition-wakeup mechanism must change to the (customer_id + time-bound) triangulation fallback.
- **Other customers' property values** — we sampled one. Need to look at the distribution of `coll_notification_replied` and `coll_bot_calling` across the wasim cohort (~2,630 customers) to confirm "Agent Calling" / "AI Calling" are the canonical strings, not idiosyncratic to this customer.

---

## Artifacts

- `workflow/tests/fixtures/ct_profile_response.json` — full GET response, pinned.
- `docs/phase_0a_decision.md` — this file.
