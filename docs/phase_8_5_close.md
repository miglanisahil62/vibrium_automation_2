# Phase 8.5 — Observability + Alerts — closure

## Status

- Implementation: complete.
- Tests: 10/10 green (`pytest workflow/tests/test_alerts.py -q`).
- CLI smoke: `python3 -m workflow.alerts --workflow-db /tmp/empty.db --dry-run` exits 0.
- SMTP send: **INTENTIONALLY OFF** — payload-only.

## Deliverables

| File | Purpose |
|------|---------|
| `workflow/alerts.py` | Watcher: reads `state/workflow.db`, emits 5 alert types, never sends real email |
| `workflow/tests/test_alerts.py` | 10 tests covering all 5 conditions, cooldown, dry-run, payload shape |
| `docs/runbook.md` | Operator-facing one-pager: symptom → diagnostic command → recovery, per alert |
| `docs/phase_8_5_close.md` | This file |

## The 5 alert conditions

| Code | Severity | Trigger |
|------|----------|---------|
| `A_DAEMON_DOWN`   | P0 | Any of 4 tracked daemons (`workflow_executor` / `workflow_scheduler` / `workflow_ingest` / `workflow_enrollment`) has no heartbeat in `wf_agent_events` within the last 30 min — or has never been seen. |
| `B_RUN_ERROR`     | P1 | Any `workflow_runs.status='ERROR'` with `updated_at_ist` older than 2 h. |
| `C_RUN_WAITING`   | P2 | Any `workflow_runs.status='WAITING'` with `updated_at_ist` older than 7 days. |
| `D_KILL_SWITCH`   | P1 | Latest `wf_kill_switch.action='KILL'` (no subsequent `RESUME`) older than 1 h. |
| `E_QUEUE_BUILDUP` | P1 | A tick in the last 60 min processed more than `TICK_BATCH_LIMIT × 0.9` (= 90) rows. Read from `wf_agent_events.summary_json` (`rows_processed` or `processed` key). |

## Cooldown

- 60 minutes per condition.
- State lives in `alert_state(condition PRIMARY KEY, last_fired_at_ist TEXT)` on `state/workflow.db`.
- The watcher creates the table on first run (idempotent `CREATE TABLE IF NOT EXISTS`).
- `--dry-run` produces payloads but does **not** update `alert_state`.

## SMTP gating — intentional

We do **not** send real email in this phase. The `Alert.as_email_payload()`
method returns a dict shaped `{to, from, subject, body, severity, condition, details}`
that is currently consumed only by tests and the watcher's own `log.warning`.

Reasons:

1. `feedback_smtp_governance_block` — the literal token `smtplib` in any
   bash / script context is hard-blocked by `guard.py` without explicit
   `I AUTHORIZE THIS SEND: <reason>` from the operator.
2. The five conditions and their cooldowns are unproven against real-world
   noise. We want a week of dry-run logs before authorising real sends.

The send path becomes a separate, small module — `workflow/alert_sender.py`
— gated behind a `--send` flag and a per-run owner approval. Out of scope
for Phase 8.5.

## Phase 9 hook

- `workflow/launchd/com.sahil.workflow.alerts.plist` will be added in Phase
  9. `StartInterval=900` (every 15 min), `ProgramArguments` runs
  `python3 -m workflow.alerts --workflow-db /Users/sahil.m/vibrium-workflow/state/workflow.db`
  with stdout/stderr redirected to `~/Library/Logs/vibrium-workflow/alerts.{out,err}.log`.
- The watcher also writes its own heartbeat row to `wf_agent_events`
  (`agent='workflow_alerts'`) once Phase 9 wires up the orchestrator — for
  now it's a pure read-only worker.

## What's not in scope

- Real SMTP send (deferred — owner approval required).
- Slack / PagerDuty integration.
- Per-recipient routing rules (single recipient: Sahil).
- Alert dashboard UI (Phase 11 candidate).

## Acceptance check (self-verified)

- [x] `python3 -c "from workflow.alerts import run"` succeeds.
- [x] `pytest workflow/tests/test_alerts.py -q` → 10 passed.
- [x] `python3 -m workflow.alerts --workflow-db /tmp/empty.db --dry-run` exits 0 with no alerts.
- [x] No file under `/Users/sahil.m/vibrium-automation/` or `/Users/sahil.m/ops_console_v2/` touched.
- [x] No `smtplib` import anywhere in the alerts module.
- [x] No new third-party dependency.
