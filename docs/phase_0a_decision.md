# Phase 0a Decision — 2026-05-30 (updated)

**Test customer:** 8968249 (Sandeep Singh — wasim_ptp_21_may)
**Sample probe:** 25 wasim cohort customers (read-only)
**Mode:** Smoke test — no live triggers, no property writes

---

## Verified findings

### 1. All 4 CT properties exist; doc vocabulary IS real

| Property | Type | Verified values from wasim sample (n=25) | Status |
|---|---|---|---|
| `coll_notification_replied` | str | `'WA_Available'` (10), `'Agent Calling'` (15). Doc's `'WA_Unavailable'` not seen in this small sample but reported by author to exist. | ✅ doc vocabulary confirmed |
| `coll_bot_calling` | str | `'AI Calling'` (5), missing (20). The granular doc values (`ai_vb_calling_highv1` etc.) NOT seen — author confirmed those are set "directly in the journey." | ✅ engine will wait for granular values to appear |
| `coll_collection_risk_segmentation` | int | 0..10 observed. Doc bucket boundaries `<5` (High), `5–7` (Mid), `8+` (Low). | ✅ |
| `dpd` | int | 0..29 observed in wasim (median 29). | ✅ |

Casing note: actual CT names are lowercase (`coll_*`, `dpd`). VB_Prompt_Doc uses uppercase (`COLL_*`, `DPD`); use the actual lowercase names — CT property names are case-sensitive.

### 2. Clarifications from doc author (Sahil)

- **The engine READS `coll_bot_calling`, never writes it.** Granular values (`ai_vb_calling_highv1`, `_midv1`, `_mid_v2/v3/v4`) are set upstream "in the journey" — possibly by another team / script / future system. The engine's entry trigger is "this property transitioned to a granular value" → enroll the customer.
- **`WA_Available` / `WA_Unavailable` are real CT values** for `coll_notification_replied`. Some customers also show `'Agent Calling'` and other states — operators can filter on whatever values the data shows.
- **Low Risk branch (`risk_segmentation ≥ 8`) is a TODO node in the workflow UI.** The visual editor lets the operator add filters + define actions for any state. No code change needed when the Low branch gets defined.

---

## Engine design implications

1. **No `SET_CT_PROP` on `coll_bot_calling`.** The engine consumes this property, doesn't produce it.
2. **Enrollment trigger:** poll CT (via `enrollment_poller.py`) for customers where `coll_bot_calling` is in the set `{ai_vb_calling_highv1, ai_vb_calling_midv1, ai_vb_calling_mid_v2, ai_vb_calling_mid_v3, ai_vb_calling_mid_v4, …}`. Each value enrolls into the matching branch.
3. **Seed workflow `vb_collections_v1`:** CONDITION nodes reference doc vocabulary verbatim. They'll only match customers whose CT profile actually has those values — so until the upstream pipeline starts writing them, the engine has nothing to do. That's fine; the engine is built ahead of the producer.
4. **Low Risk branch:** seed workflow includes the SWITCH on risk_segmentation with the Low case → a placeholder node `LOW_RISK_TODO` whose "out edge" is `TERMINATE(NEEDS_DEFINITION)`. Operator edits the node in UI to add filters + actions when the Low policy is finalized. **This is the engine's value prop: pipeline definition lives in the UI, not in code.**
5. **The `vb_collections_v1` workflow's seed enrollment trigger reads `coll_bot_calling`** — when it transitions to any of the granular values, the engine creates a run; otherwise it doesn't.

---

## What was NOT verified in Phase 0a (deferred)

- **`WA_Unavailable` distribution** — only 10 `WA_Available` and 15 `Agent Calling` seen in n=25. Larger sample (e.g., 1000 customers) needed before final SWITCH cases are pinned.

## ⚠ Major finding: `tag_group` is NOT a column in `collection_comment_data`

Verified by running `SVV_EXTERNAL_COLUMNS` + `information_schema.columns` introspection on `sttash_website_live.collection_comment_data` via AWS Redshift. The table is queryable but the schema introspection returned 0 columns through both paths (typical for Spectrum-external tables).

**Crucially:** sampling 3 actual vibrium-tagged rows shows the metadata is encoded as **free text key:value pairs in the `comment` column**:

```
merchant_name : vibrium, disposition : Call Not Connected, sub_disposition : Customer Busy, comment : Call ended with reason: customer-busy
```

There is no structured `tag_group` field anywhere in the row.

### Implications for the workflow engine

The architecture rev 3 design — "embed `vbwf:R:N:K` in CT externaltrigger's `tag_group` payload, recover it from the `decision_log` row" — does NOT work because `tag_group` is not a column. The plan's `tag_group` mechanism would need to land somewhere else.

**Three options for the disposition-wakeup join:**

| Option | Mechanism | Risk |
|---|---|---|
| **A. Embed vbwf:R:N:K in the comment TEXT** | Pass a custom string via CT externaltrigger Props that the bot prepends/appends to its disposition comment. Workflow_ingest extracts it via regex. | Untested — requires live CT fire to confirm preservation. CT may strip custom Props that aren't in the campaign template. |
| **B. Separate CT campaign for workflow fires** | Use a distinct `campaign_id` for workflow-fired triggers vs adhoc. Both ingests filter on which campaign produced the comment. The campaign template can include a literal `[vbwf:run_id]` marker that bot preserves into the comment. | Requires creating + approving a new CT campaign (template + DLT compliance). 1-2 day side-quest. |
| **C. (customer_id + time-bound) triangulation as PRIMARY join** | Workflow scheduler records exact `fired_at_ist` in `wf_pending_actions`. Workflow_ingest matches on `(customer_id, comment_create_date BETWEEN fired_at AND fired_at + 24h, status=FIRED)` — picks the closest in time. No comment-text dependency. | Works regardless of CT behavior. Ambiguous when two workflow runs for the same customer fire on the same day (must enforce 3h cooldown to make ambiguity vanishingly small) — which is already a per-customer rule. |

**Recommendation: Option C (triangulation) as primary.** Eliminates the unverified tag_group survival assumption entirely. Option A becomes a confirmation hint only — if a comment happens to contain `vbwf:R:N:K`, use it; otherwise fall back to triangulation. Option B is unnecessary complexity for v1.

This also simplifies Phase 3's adhoc patches:
- The proposed `WHERE tag_group NOT LIKE 'vbwf:%'` filter on `ingest.py` can't work (no such column).
- Instead, the workflow system distinguishes its own rows by **looking them up in `wf_pending_actions` by (customer_id, time-bound)**. The adhoc `ingest.py` doesn't need to filter anything out — if a comment matches a workflow row in `wf_pending_actions`, workflow_ingest claims it; if not, adhoc `ingest.py` handles it normally. The two ingests don't overlap because they consult different reference tables.

### Updates to PHASES.md needed

- **Phase 3** — remove the `WHERE tag_group NOT LIKE 'vbwf:%'` patch to `ingest.py`. The adhoc system is untouched (zero edits beyond `audit.record_fire(...)` after each fire).
- **Phase 6** (workflow_scheduler) — DON'T pass `tag_group` in the CT externaltrigger payload. Just record `fired_at_ist` precisely in `wf_pending_actions`.
- **Phase 7** (workflow_ingest) — primary join becomes the triangulation; tag_group fallback removed.

### Also worth pinning: sample comment values

The 3 sample rows pulled are identical (`Call Not Connected / Customer Busy`). That's almost certainly LIMIT artifacts, not the actual disposition distribution. Phase 7 implementation should query for a broader sample (50+ rows across different days + dispositions) to confirm the parser handles the full vocabulary.

---

## Phase 0a outcome

✅ **PASS.** The engine design is sound given clarified semantics. Proceed to Phase 0 (repo foundation) and Wave 1.

The seed workflow vocabulary uses the doc's prescribed values verbatim. The Low branch is intentionally a TODO node (UI-editable). The `coll_bot_calling` upstream pipeline is out of scope for this project.

---

## Artifacts

- `workflow/tests/fixtures/ct_profile_response.json` — full CT profile GET response for customer 8968249, pinned for Phase 2.
- `docs/phase_0a_decision.md` — this file.
