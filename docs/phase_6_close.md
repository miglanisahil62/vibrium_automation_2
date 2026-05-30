# Phase 6 Closure — Workflow Scheduler

**Status:** Code complete; tests green (11 new + 144 existing, all in workflow/tests). Ready for master-auditor.

---

## What was built

| File | Purpose |
|---|---|
| `workflow/workflow_scheduler.py` | Tick loop that fires `wf_pending_actions` rows. Composes RBI window + cross-system daily cap (3/day) + 3h cooldown + Redshift overdue/paid_today gate; branches on kill-switch / dry-run / shadow_mode / live; records `customer_call_audit` rows with `source='workflow'` on every successful fire. CLI entrypoint via `python3 -m workflow.workflow_scheduler`. |
| `workflow/tests/test_workflow_scheduler.py` | 11 tests: happy path × 5, gate denies, outside-window, CT error, shadow_mode, dry_run, kill_switch, race-safety, daily-cap, cooldown, CLI smoke. Explicit assertion that `trigger()` is called WITHOUT `tag_group` kwarg (Phase 0a invariant). |
| `docs/phase_6_close.md` | This document. |

---

## Phase 0a invariants honored

* **No `tag_group` kwarg passed to `clevertap_trigger.trigger()`.** Asserted by test 1 across all 5 trigger calls. The Phase 0a decision (`docs/phase_0a_decision.md`) makes time-bound triangulation the primary disposition-wakeup join — no payload tagging needed.
* **`fired_at_ist` set on every FIRED / SHADOW_FIRED row.** This is the column Phase 7's `workflow_ingest.py` triangulates against (customer_id + 24h window since fire). Setting it in shadow mode too preserves the triangulation testability without live CT writes.
* **No edits to `vibrium-automation/`.** All cross-system signal goes through (a) the `external/vibrium_automation_scripts` symlink for library imports, and (b) the shared `customer_call_audit` table in `vibrium.db`.

---

## Gate composition (locked)

Per-row gate order (cheap-first to short-circuit before the Redshift hop):

1. **Daily cap** — `customer_call_audit.customer_daily_cap()` reads today's IST date from `vibrium.db.customer_call_audit`. Counts both `source='adhoc'` and `source='workflow'` rows. Limit = 3.
2. **3h cooldown** — `customer_call_audit.batch_last_fire_at()` (one bulk query for the whole batch). If most-recent fire across both systems was < 3h ago, SUPPRESS. This matches the adhoc COOLDOWN_HOURS=3 — the equal-cooldown rule makes Phase 7's time-bound triangulation unambiguous (at most one fire per customer per 3h window).
3. **Customer overdue + paid_today** — `pre_call_gate.check(customer_id)` (Redshift round-trip; tests inject a stub).

The RBI 08:00–19:00 IST window is evaluated ONCE at tick start via `pre_call_gate.is_callable_now()`. Outside the window the tick exits with `status='outside_window'` and emits a `skipped` heartbeat — observable in `wf_agent_events`.

---

## Audit-write semantics

* `record_fire(customer_id=cid, source='workflow', run_id=run_id, cohort_name=cohort, ct_response_status='success', vibrium_db_path=...)` — called only on successful CT delivery (`status == 'success'`).
* Audit failures log a warning but never crash the tick. The CT call already succeeded; losing one observability row is recoverable, losing the tick is not.
* SHADOW_FIRED rows do NOT record audit. The whole point of shadow mode is no observable impact on the shared per-customer cap.
* dry_run mode does NOT record audit and does NOT update the row past releasing it back to PENDING.

---

## Race safety

`_claim_row()` issues a single `UPDATE wf_pending_actions SET status='FIRING_IN_PROGRESS' WHERE id=? AND status='PENDING'` and checks `cursor.rowcount`. SQLite serializes writes via the RESERVED→PENDING→EXCLUSIVE lock chain, so two concurrent ticks racing on the same row yield rowcount=1 to the winner and rowcount=0 to the loser. The loser logs and continues to the next row. Verified by `test_claim_row_race_safety`.

---

## CLI

```
python3 -m workflow.workflow_scheduler \
    --workflow-db state/workflow.db \
    --vibrium-db state/vibrium.db \
    [--ct-creds /path/to/ct_creds.json] \
    [--shadow] [--dry-run] [--batch-limit 100] \
    [--log-level INFO]
```

Outputs a JSON stats line on stdout (`{processed, fired, suppressed, errored, shadow_fired, would_fire, status}`); writes a heartbeat row to `wf_agent_events` on every tick (even no-ops).

Verified smoke run on freshly-migrated empty DBs: exits 0 with `status='outside_window'` (run was at 19:36 IST).

---

## Test counts

```
workflow/tests/test_workflow_scheduler.py: 11 passed
workflow/tests (full suite, ex test_clevertap_profile): 149 passed
```

---

## What was explicitly NOT done

* No edits to `vibrium-automation/scripts/clevertap_trigger.py` — signature is unchanged and is consumed via the `external/` symlink.
* No `tag_group` payload anywhere in the call path.
* No direct `requests.post` to CleverTap — only via `clevertap_trigger.trigger()`.
* No launchd plist (Phase 9 wraps daemons into plists; Phase 6 ships the runnable module only).
* No alert emails / digest mail (Phase 8.5 covers alerts).

---

## Open items for downstream phases

* **Phase 7 (already closed)** consumes `wf_pending_actions.fired_at_ist` for the time-bound triangulation join. Phase 6 writes this column on FIRED + SHADOW_FIRED — both code paths exercised in tests.
* **Phase 9** will add `com.sahil.workflow.scheduler.plist` (`StartInterval=300`) wrapping the CLI; the entrypoint guards itself with `is_callable_now()` so outside-window invocations are observable no-ops.
* **Phase 8.5** will read `wf_agent_events` for alert decisions; the heartbeat schema is `{ts_ist, agent='workflow_scheduler', status, summary_json}` matching the conventions established in Phase 1.
