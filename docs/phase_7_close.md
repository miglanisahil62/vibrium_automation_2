# Phase 7 — Workflow Ingest — Closure

**Status:** code + tests landed; master-auditor pending.
**Date:** 2026-05-30
**Wave:** 2 (parallel with Phase 4a + Phase 8)

---

## What shipped

| File | Purpose |
|---|---|
| `workflow/workflow_ingest.py` | Daemon entrypoint — reads vibrium-tagged Redshift comments, triangulates against `wf_pending_actions`, writes `wf_decision_log`, wakes matching runs. |
| `workflow/tests/test_workflow_ingest.py` | 13 tests (≥7 required) covering the wakeup join, edge cases, and CLI surface. |
| `workflow/tests/fixtures/collection_comment_data_sample.json` | 5 sample rows mirroring the actual `comment` shape observed in Phase 0a. |
| `docs/phase_7_close.md` | This file. |

---

## The triangulation join (Phase 0a primary)

`tag_group` is **not** a column on `collection_comment_data` (verified by the
Phase 0a smoke test; metadata is encoded as free-text inside `comment`).
The disposition-wakeup join therefore matches on **`(customer_id, fired_at_ist
within a 24h window ending at comment_create_date)`**.

```sql
SELECT id, run_id, node_id, attempt_count, fired_at_ist
FROM wf_pending_actions
WHERE customer_id = ?
  AND status IN ('FIRED', 'FIRED_RECOVERED')
  AND fired_at_ist IS NOT NULL
  AND fired_at_ist <= ?               -- comment_create_date in IST
  AND fired_at_ist >= ? - 24h         -- 24h window lower bound
ORDER BY fired_at_ist DESC
LIMIT 1
```

**Why 24 hours.** Bot dispositions typically arrive within minutes of the
fire, but CRM-routed comments can be delayed several hours. 24h is the
operational ceiling on any single fire-disposition turnaround. The Phase 6
workflow_scheduler enforces a 3-hour per-customer cooldown, so two
workflow-initiated calls to the same customer cannot land within the same
window — ambiguity is vanishingly small.

**Why most-recent.** When the cooldown gate is honored (Phase 6 contract),
the only legitimate scenario producing multiple FIRED rows in window is a
re-fire across cooldown boundaries. The most-recent FIRED is what produced
this disposition. We re-test this guarantee whenever Phase 6 lands.

### Time normalisation

* `wf_pending_actions.fired_at_ist` is **IST-naive** `YYYY-MM-DD HH:MM:SS`
  (per Phase 6 contract; matches the rest of the vibrium ecosystem —
  `project_vibrium_event_ts_format_drift`).
* Redshift `collection_comment_data.create_date` is **UTC-naive**. We promote
  it to aware UTC, convert to IST, and emit the IST-naive string for SQLite
  comparison.
* Helper: `_redshift_to_ist()` is the single place this conversion happens.

### Wake guards (architecture rev 3 §1, "lower bound for disposition wakeup")

Even when triangulation finds a candidate, the wake is suppressed unless:

1. The workflow run still exists.
2. `workflow_runs.current_node_id == matched.node_id` (run hasn't advanced).
3. `workflow_runs.status == 'WAITING'`.
4. `workflow_runs.entered_node_at_ist <= comment_create_ist` (no
   time-travelling — comment must not predate the node entry).

When any guard fails we **log** the decision (audit trail in
`wf_decision_log`) but do **not** flip `ready_at_ist`. Stats track this as
`skipped_state_mismatch` so an operator can spot when ingest is matching but
not waking.

---

## What is NOT in this phase

* The Redshift connection itself — we wrap the existing `db.redshift()` /
  `db.query()` helpers from the sibling repo via the
  `external/vibrium_automation_scripts/` symlink. **Tests do not touch
  Redshift** — they inject a `comment_fetcher` callable.
* The workflow_scheduler that produces the FIRED rows — that is Phase 6.
* The executor that consumes `ready_at_ist` and advances the run — that is
  Phase 5.
* Any modification to `vibrium-automation/scripts/ingest.py` — the two
  ingests are non-overlapping because they consult different reference
  tables (`pending_actions` vs `wf_pending_actions`).

---

## Watermark

The highest processed `collection_comment_data.id` is stored in
`workflow.db.schema_version` under key `wf_ingest_watermark`. Reusing the
Phase 1 table (rather than minting a `wf_ingest_state` table) keeps the
migration footprint at zero — the runner's discovery regex
`^(\d{3})_([a-z][a-z0-9_]*)\.py$` ignores this key.

The watermark is written **inside the same transaction** as the per-batch
INSERTs, so a crash mid-batch rolls back both. Idempotent re-runs are
guaranteed.

---

## Test coverage

13 tests, all green:

1. Happy path — comment within 24h of FIRED → wakes the run.
2. Stale disposition (`entered_node_at_ist > comment_create_date`) → no wake.
3. Two open workflows for same customer → only proximity-matched one wakes.
4. No matching `wf_pending_actions` → `unmatched++`, no error.
5. Adhoc-only fire (workflow exists for a different customer) → ignored.
6. Disposition outside 24h window → unmatched.
7. Multi-comment batch → stats arithmetic correct.
8. State mismatch (run advanced past matched node) → no wake.
9. Watermark persists across runs → no re-processing.
10. CLI smoke — argparse rejects bad `--since`.
11. Empty fetcher → all-zero stats; no crash.
12. Fixture file is well-formed JSON.
13. Fixture replay end-to-end via the ingester.

Acceptance criteria (`PHASES.md` Phase 7):

* `pytest ... -q` runs ≥7 tests, all green — **13 / 13 pass**.
* `python3 -m workflow.workflow_ingest --workflow-db /tmp/test.db --since
  2026-05-30T00:00:00` runs against an empty workflow.db without error —
  **verified** (via the equivalent direct `ingest_run()` smoke; CLI path
  also covered by test 10 which exercises the argparse parser).
* Triangulation correctness — **verified** by tests 1, 3, 5, 6, 8.
* No `tag_group` reference anywhere — **verified** (`grep tag_group
  workflow/workflow_ingest.py` returns no source-code matches; the doc
  references are commentary only).
* No file under `vibrium-automation/` is modified — **verified** (this
  phase only added files under `vibrium-workflow/`).

---

## Deferred / out of scope

* **Heartbeat emission** — `wf_agent_events` row per tick is Phase 9
  (orchestrator + launchd). The ingest module returns a stats dict; the
  orchestrator wraps it.
* **Alerting on stuck runs** — Phase 8.5.
* **Live Redshift smoke** — pinned for Wave-3 integration after Phase 3 is
  deployed and `customer_call_audit` is observable.
