# vibrium-workflow operator runbook

One-page operator-facing playbook for the five alert conditions emitted by
`workflow/alerts.py`. The watcher fires every 15 min (Phase 9 launchd plist).
Each alert email contains a `condition` field — match it to the section
below.

**Email payload shape:** `to=sahil.miglani@stashfin.com`,
`from=vibrium-workflow@stashfin.com`, subject prefix `[P0]` / `[P1]` / `[P2]`.
Real SMTP send is currently OFF (payload-only) pending owner approval per
`feedback_smtp_governance_block`.

Cooldown: 60 min per condition (tracked in `alert_state` table on
`state/workflow.db`).

---

## A — `A_DAEMON_DOWN` (P0)

**Symptom:** One or more of the 4 tracked daemons (`workflow_executor`,
`workflow_scheduler`, `workflow_ingest`, `workflow_enrollment`) hasn't
heartbeated to `wf_agent_events` in 30+ minutes.

**Diagnose:**
```bash
sqlite3 state/workflow.db \
  "SELECT agent, MAX(ts_ist) FROM wf_agent_events GROUP BY agent;"
launchctl list | grep com.sahil.workflow
tail -200 ~/Library/Logs/vibrium-workflow/<daemon>.err.log
```

**Recover:**
1. Confirm the launchd plist is loaded: `launchctl list | grep <daemon>`.
2. If unloaded, reload: `launchctl load ~/Library/LaunchAgents/com.sahil.workflow.<daemon>.plist`.
3. If loaded but not firing, check `.err.log` for Python tracebacks.
4. Manual one-shot run: `python3 -m workflow.workflow_orchestrator --mode <daemon> --once`.
5. If three restarts in a row fail, escalate to Sahil.

---

## B — `B_RUN_ERROR` (P1)

**Symptom:** One or more `workflow_runs.status='ERROR'` for >2 hours.
Usually a node handler crashed and the run was parked.

**Diagnose:**
```bash
sqlite3 state/workflow.db <<'SQL'
SELECT id, workflow_id, customer_id, current_node_id, updated_at_ist
FROM workflow_runs
WHERE status='ERROR' AND updated_at_ist <= datetime('now', '-2 hours')
ORDER BY updated_at_ist ASC LIMIT 50;

SELECT ts_ist, side_effect FROM workflow_node_log
WHERE run_id=<RUN_ID> ORDER BY id DESC LIMIT 10;
SQL
```

**Recover:**
1. Read the latest `workflow_node_log.side_effect` for the run — that's the
   handler's exception string.
2. If transient (network, CT timeout): use the ops console
   `POST /api/workflows/{id}/runs/{run_id}/repair` to nudge the run forward.
3. If logic bug: pause the workflow via kill_switch
   (`INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by) VALUES (datetime('now'), 'KILL', '<reason>', '<you>');`),
   patch the handler, redeploy, then `RESUME`.
4. Escalate if >25 runs error in the same node within an hour — that's a
   handler-level bug, not noise.

---

## C — `C_RUN_WAITING` (P2)

**Symptom:** Runs sitting in `status='WAITING'` for >7 days. Usually a
`WAIT_UNTIL` whose disposition never arrived (the bot returned a value not
in the canonical `BRANCH_ON_DISPOSITION` enum) or a customer who is no
longer being called.

**Diagnose:**
```bash
sqlite3 state/workflow.db <<'SQL'
SELECT id, customer_id, current_node_id, ready_at_ist, updated_at_ist
FROM workflow_runs
WHERE status='WAITING' AND updated_at_ist <= datetime('now', '-7 days')
ORDER BY updated_at_ist ASC LIMIT 50;

SELECT * FROM wf_pending_actions WHERE run_id=<RUN_ID>;
SELECT * FROM wf_decision_log WHERE run_id=<RUN_ID> ORDER BY id DESC LIMIT 5;
SQL
```

**Recover:**
1. Check `wf_pending_actions` for the run — is there an unfulfilled fire?
2. Check `wf_decision_log` for unrecognised dispositions (action_class IS
   NULL).
3. Repair via ops console `POST /api/workflows/{id}/runs/{run_id}/repair`
   or terminate manually if the customer is no longer relevant.
4. If 50+ runs hit this in a week, the BRANCH_ON_DISPOSITION mapping needs
   widening — escalate.

---

## D — `D_KILL_SWITCH` (P1)

**Symptom:** Latest `wf_kill_switch` row has `action='KILL'` and no
subsequent `RESUME` for >1 hour. While KILL is active, no scheduler fires.

**Diagnose:**
```bash
sqlite3 state/workflow.db \
  "SELECT ts_ist, action, reason, set_by FROM wf_kill_switch ORDER BY id DESC LIMIT 5;"
```

**Recover:**
1. Confirm whether KILL is intentional (planned maintenance, vendor issue).
2. If issue is resolved, insert a RESUME row:
   ```sql
   INSERT INTO wf_kill_switch(ts_ist, action, reason, set_by)
   VALUES (datetime('now'), 'RESUME', '<reason>', '<you>');
   ```
3. If KILL was set by an unknown actor, escalate to Sahil before resuming.

---

## E — `E_QUEUE_BUILDUP` (P1)

**Symptom:** A tick in the last hour processed more than 90 rows
(TICK_BATCH_LIMIT=100 × 0.9). Queue is filling faster than it drains.

**Diagnose:**
```bash
sqlite3 state/workflow.db <<'SQL'
SELECT ts_ist, agent, summary_json FROM wf_agent_events
WHERE ts_ist >= datetime('now', '-1 hours')
ORDER BY ts_ist DESC LIMIT 30;

SELECT status, COUNT(*) FROM workflow_runs GROUP BY status;
SELECT status, COUNT(*) FROM wf_pending_actions GROUP BY status;
SQL
```

**Recover:**
1. Identify whether the buildup is upstream (ingest spike, CT batch arriving)
   or downstream (executor crashing mid-tick — see condition A).
2. Confirm the executor is firing on schedule (`launchctl list` + heartbeat).
3. If sustained, consider raising `TICK_BATCH_LIMIT` in
   `workflow.agents.workflow` (test first; default is 100). Coordinate with
   Sahil before changing.
4. Use ops console `/workflows` → runs view to confirm rows are draining,
   not stuck.

---

## Escalation

Sahil Miglani — sahil.miglani@stashfin.com (placeholder; replace with
on-call rotation when Phase 13 ships).

## Related files

- `workflow/alerts.py` — the watcher itself
- `state/workflow.db` — the read-only source of truth
- `state/workflow.db::alert_state` — cooldown sidecar (60-min)
- `workflow/launchd/com.sahil.workflow.alerts.plist` — Phase 9 cron
