# vibrium-workflow — AWS deployment

Mac → GitHub → server-pulls-before-run. Nothing is edited directly on the server.

## One-time server setup

```bash
ssh ubuntu@<server>
cd /home/ubuntu
git clone <repo> vibrium-workflow        # or: cd vibrium-workflow && git pull
cd vibrium-workflow
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python3 -m workflow.migrations.runner --workflow-db state/workflow.db \
        --vibrium-db /home/ubuntu/vibrium-automation/state/vibrium.db   # if not migrated
mkdir -p logs state/enrollment state/failures state/locks secrets
```

### Secrets (never committed — `secrets/` is gitignored)

- `secrets/redshift.env` — exports the Redshift creds the fetch + ingest use
  (`REDSHIFT_*` / whatever `external/.../db.py` reads).
- `secrets/ops.env`     — `OPS_WEBHOOK_URL` + `OPS_SHARED_SECRET` for heartbeats.
  Optionally `WF_SHADOW=0` here when going live, and `WF_HOURLY_CALL_CAP=750`.

The `external/vibrium_automation_scripts` symlink must resolve on the server
(it points at the adhoc repo's db.py / pre_call_gate.py).

## Seed / activate the workflow

The workflow is already seeded as **id=7 "VB Collections v2"** (shadow_mode ON).
To push a new graph version after editing `SEGMENTS`:

```bash
python3 seed_workflows/generate_vb_collections_v2.py     # regenerate JSON (runs the lint)
python3 scripts/seed_workflow.py                          # save version + activate (shadow stays ON)
```

## Install cron

```bash
crontab -l > /tmp/crontab.bak           # back up existing
crontab deploy/crontab.txt              # install (review the file first)
crontab -l                              # confirm
```

## Shadow → live cutover (do NOT skip)

The system ships in **shadow mode** (scheduler marks `SHADOW_FIRED`, no real CT
calls). Validate a full day first:

1. Let the crons run one full day in shadow.
2. Check enrollment: `state/enrollment/dpd1_candidates_<today>.csv` exists and
   the poller created runs (`wf_<mode>` heartbeats green in morning_summary).
3. Inspect routing in the console (`/workflows/7`) — runs distributed across the
   6 segments, `wf_pending_actions` rows are `SHADOW_FIRED`, branch counts sane.
4. Run the shadow gate test: `python3 scripts/e2e_shadow_test.py`.
5. **Go live** only after Sahil's "go": set `WF_SHADOW=0` in `secrets/ops.env`
   AND flip the workflow row's `shadow_mode` off in the console. Both are
   required — the scheduler `--shadow` flag and the DB column are independent.

## Daily timeline (IST)

| Time | Job | Purpose |
|---|---|---|
| 07:30 | run_fetch.sh | collection_view ageing=1 → dated CSV |
| 07:35 | run.sh enrollment | CT-fetch + create parked runs (prep slot) |
| 08:15 | run_fetch.sh fallback | retry fetch iff today's CSV missing |
| 08:30/10:00/12:00 | run.sh enrollment | idempotent catch-up |
| 07:30–20:25 /5min | run.sh executor | advance runs |
| 07:30–19:25 /5min | run.sh scheduler | fire (gated: window + 750/hr + 3/day) |
| 07:30–20:25 /15min | run.sh ingest | pull dispositions |
| 19:30 | run.sh digest | summary email |
