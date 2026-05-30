# Phase 8 — Enrollment Poller Closure

**Status:** Code shipped + tests green. Master-auditor verdict recorded in
`docs/phase_8_audit.md`.

## What was built

- `workflow/enrollment_poller.py` (~530 lines, well-commented)
  - `run(workflow_db_path, *, force=False, dry_run=False, now=None) -> dict`
    is the entry point; the CLI thin-wraps it.
  - Kill-switch check FIRST via `wf_kill_switch` latest-row pattern (same
    semantics as `~/vibrium-automation/agents/orchestrator.py`).
  - RBI window check via `external.vibrium_automation_scripts.pre_call_gate.is_callable_now()`
    + a stricter `now.hour < 18` cap so enrolment doesn't fire late-day.
  - Reads ACTIVE workflows + parses ENROLL node config from `graph_json`.
  - `bulk_get_profiles(cids, concurrency=5)` from Phase 2.
  - CONDITION eval: tries Phase 4a's `workflow.agents.workflow_handlers.condition`
    first, falls back to inline simpleeval with the same safety profile
    (`functions={}`, names-only access, MAX_STRING_LENGTH=1024, MAX_POWER=100).
  - Caps: per-workflow daily, per-tick global (200), hard-abort (>5000).
  - `INSERT OR IGNORE` on `workflow_runs` keyed by the partial-UNIQUE
    `(workflow_id, customer_id, enrollment_key)` index from Phase 1.
  - `--dry-run` logs intended enrollments without writing.
  - `--force` bypasses the hard-abort; documented as "after manual review only".
  - Exits 2 (non-zero) when any workflow hit the hard-abort path so launchd
    surfaces it.

- `workflow/tests/test_enrollment_poller.py` — 13 tests, all green:
  - happy path 4/10
  - re-run idempotency (enrollment_key dedupe)
  - per-tick cap clipping
  - outside window (`gate.fire=False`)
  - outside enrollment window 18:00-19:00 (gate True but hour >= 18)
  - kill-switch active
  - kill-then-resume unblocks
  - hard-abort >5000 → exits non-zero, no writes
  - `--force` bypass
  - `--dry-run` no writes
  - daily-cap partial gap (1 already today, 2 attempted, 1 lands)
  - empty DB (no workflows → clean exit)
  - condition undefined-name skips candidate, doesn't crash tick

## Design choices (v1)

- **CSV input only.** `enrollment_trigger_config.source_csv` points at an
  absolute CSV path with a `customer_id` column. SQL-pull modes
  (collection_view, T1, Redshift) are explicitly deferred — extending
  `_load_candidates` is a one-function change later.
- **CONDITION fallback path.** Phase 8 lives in Wave 2 alongside Phase 4a;
  rather than block on the handler, the poller carries its own simpleeval
  evaluator with identical safety knobs. When Phase 4a's
  `workflow.agents.workflow_handlers.condition` lands, the local-import at
  the top of `_evaluate_condition` picks it up automatically — no edit
  needed here. Auditor confirms parity.
- **Stricter enrollment window (08:00–18:00).** RBI permits calling
  through 19:00, but a customer enrolled at 18:30 would then need a
  workflow-driven call within minutes — which the calling window forbids.
  Enrolling them one hour earlier than the calling cutoff gives the
  executor a buffer to schedule the first FIRE_VB_CALL without violating
  RBI.
- **One transaction per workflow batch.** Atomic — if any INSERT in a
  workflow's batch fails, the batch rolls back. The partial UNIQUE index
  absorbs idempotency at the row level.
- **Read-only against CT.** `bulk_get_profiles` only. No
  `set_profile`, no `clevertap_trigger.trigger()` — those are the
  scheduler's (Phase 6) and the SET_CT_PROP handler's (Phase 4c) job.

## Acceptance gate (all met)

- [x] `pytest workflow/tests/test_enrollment_poller.py -q` — 13 tests, all
  green. (Spec asks for ≥6.)
- [x] `python3 -m workflow.enrollment_poller --workflow-db /tmp/ep_test.db
  --dry-run` exits 0 on empty DB (window-permitting; outside window also
  exits 0 cleanly).
- [x] Idempotency: re-run with same input → 0 new rows
  (`test_rerun_is_idempotent`).
- [x] No file under `/Users/sahil.m/vibrium-automation/` modified
  (verified by directory inspection — Phase 8 touches only
  `vibrium-workflow/`).
- [x] Full repo test suite stays green: 78 tests pass.

## Deferred / out of scope

- **Phase 8.5 alerts** — hard-abort logs at ERROR; the email path lands in
  Phase 8.5 (`workflow/alerts.py`). The launchd plist for this daemon
  lands in Phase 9.
- **SQL-pull enrollment modes** — `_load_candidates` will grow a
  branch for `enrollment_trigger_config = {"source_sql": ...}` when a
  real workflow needs it; current shipped seed is CSV-only.
- **Live CT smoke test** — the existing pinned fixture
  `workflow/tests/fixtures/ct_profile_response.json` from Phase 0a is the
  reference; no live profile fetches were performed during this phase
  (the poller path doesn't need to make HTTP calls under test).
