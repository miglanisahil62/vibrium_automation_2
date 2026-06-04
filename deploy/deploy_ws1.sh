#!/usr/bin/env bash
# WS1 deploy — X-Bucket Vibrium robust CT fetch + midv2/3/4 segment fix.
# Run on the AWS server (in /home/ubuntu/vibrium-workflow). Idempotent + safe to
# re-run. Does NOT touch shadow_mode (already live) and does NOT fire any calls.
set -uo pipefail
BASE=/home/ubuntu/vibrium-workflow
VIB_DB=/home/ubuntu/vibrium-automation/state/vibrium.db
cd "$BASE"

echo "=== 1. git pull ==="
git pull --ff-only

echo "=== 2. apply migrations (003 = ct_profile_cache; 001/002 idempotent) ==="
python3 -m workflow.migrations.runner --workflow-db state/workflow.db --vibrium-db "$VIB_DB"

echo "=== 3. reseed graph (dry-run) — underscore fix midv2/3/4 ==="
python3 scripts/seed_workflow_direct.py --dry-run

echo "=== 4. reseed graph (APPLY) — appends a new version, repoints active ==="
python3 scripts/seed_workflow_direct.py

echo "=== 5. add prefetch cron (idempotent; 07:32 IST = 02:02 UTC under CRON_TZ=UTC) ==="
if crontab -l 2>/dev/null | grep -q 'run.sh prefetch'; then
  echo "  prefetch cron already present"
else
  ( crontab -l 2>/dev/null; \
    echo '2  2 * * *  $BASE/run.sh prefetch  >> $BASE/logs/cron.log 2>&1   # 07:32 IST (after fetch, before enrollment)' \
  ) | crontab -
  echo "  prefetch cron added"
fi

echo "=== 6. verify: cache table present ==="
python3 - <<'PY'
import sqlite3
c = sqlite3.connect("state/workflow.db")
r = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ct_profile_cache'").fetchone()
print("  ct_profile_cache table:", r[0] if r else "MISSING")
PY

echo "=== 7. verify: cron entries ==="
crontab -l | grep -E 'CRON_TZ|run_fetch|run.sh (prefetch|enrollment|scheduler|executor)' || true

echo "=== 8. prefetch smoke (dry-run via orchestrator — no fetch, no writes) ==="
python3 -m workflow.workflow_orchestrator --mode prefetch --workflow-db state/workflow.db --dry-run 2>&1 | tail -6

echo "=== WS1 DEPLOY COMPLETE ==="
