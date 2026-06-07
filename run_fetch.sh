#!/usr/bin/env bash
# Cron wrapper for the daily DPD-1 candidate fetch (scripts/fetch_dpd1_candidates.py).
# Activates the venv, sources secrets, runs under a wall-clock timeout, writes a
# dated log, emits a heartbeat (job_id wf_fetch_dpd1), and persists a failure
# marker on non-zero exit. Canonical pattern: vibrium-automation/run.sh.
#
# Usage:
#   ./run_fetch.sh            # primary 07:30 run — always fetches + writes today's CSV
#   ./run_fetch.sh fallback   # 08:15 retry — only runs if today's CSV is still MISSING
#
# Cron (AWS server is UTC; 07:30 IST = 02:00 UTC, 08:15 IST = 02:45 UTC):
#   0  2 * * * /home/ubuntu/vibrium-workflow/run_fetch.sh
#   45 2 * * * /home/ubuntu/vibrium-workflow/run_fetch.sh fallback
set -u

MODE="${1:-primary}"
BASE_DIR="/home/ubuntu/vibrium-workflow"
LOG_DIR="${BASE_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/fetch_dpd1_$(date +%F).log"
TIMEOUT_SEC=600  # 10m — a single collection_view SELECT; generous for slow Redshift.

# CSV output dir must match the enrollment poller's expectation. Keep in sync
# with WF_ENROLLMENT_CSV_DIR if the poller's cron overrides the default.
CSV_DIR="${WF_ENROLLMENT_CSV_DIR:-${BASE_DIR}/state/enrollment}"
TODAY_CSV="${CSV_DIR}/dpd1_candidates_$(date +%F).csv"

cd "${BASE_DIR}"
# shellcheck disable=SC1091
[[ -f "${BASE_DIR}/venv/bin/activate" ]] && source "${BASE_DIR}/venv/bin/activate"

# Redshift creds + Ops heartbeat env (present on AWS, absent on dev Mac).
for envf in redshift.env ops.env; do
    if [[ -f "${BASE_DIR}/secrets/${envf}" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${BASE_DIR}/secrets/${envf}"
        set +a
    fi
done

# Fallback mode: skip ONLY if today's cohort CSV already exists AND carries at
# least one candidate (the primary 07:30 run genuinely succeeded). An EMPTY CSV
# (header only) means the 07:30 fetch hit collection_view while it was empty /
# mid-refresh and wrote a 0-row cohort — that is a failure the primary cannot
# self-heal, since it never re-queries. So in fallback mode we treat a present
# but empty file as "not done yet" and fall through to re-query collection_view,
# giving the upstream refresh ~45 extra minutes to land. Count DATA rows (skip
# the header) so a missing trailing newline can't be mistaken for empty/non-empty.
if [[ "${MODE}" == "fallback" && -f "${TODAY_CSV}" ]]; then
    DATA_ROWS=$(tail -n +2 "${TODAY_CSV}" 2>/dev/null | grep -c .)
    if [[ "${DATA_ROWS}" -gt 0 ]]; then
        echo "$(date '+%F %T %Z') — fallback: ${TODAY_CSV} already present with ${DATA_ROWS} candidate(s); nothing to do" >> "${LOG_FILE}"
        exit 0
    fi
    echo "$(date '+%F %T %Z') — fallback: ${TODAY_CSV} present but EMPTY (0 candidates) — re-querying collection_view" >> "${LOG_FILE}"
fi

echo "===== $(date '+%F %T %Z') — starting fetch_dpd1 (mode=${MODE}, timeout=${TIMEOUT_SEC}s) =====" >> "${LOG_FILE}"
# Exit 124 = timeout → treated as failure (heartbeat 'down').
timeout --kill-after=30 "${TIMEOUT_SEC}" \
    python3 "${BASE_DIR}/scripts/fetch_dpd1_candidates.py" >> "${LOG_FILE}" 2>&1
RC=$?
if [[ "${RC}" -eq 124 ]]; then
    echo "===== TIMEOUT after ${TIMEOUT_SEC}s — process killed =====" >> "${LOG_FILE}"
fi
echo "===== $(date '+%F %T %Z') — finished fetch_dpd1 (exit=${RC}) =====" >> "${LOG_FILE}"

# Chain the CT prefetch on the cohort this fetch just wrote. This guarantees the
# prefetch ALWAYS runs AFTER fetch and on the FULL, freshly-written CSV —
# eliminating the fetch/prefetch ordering race that left ~5k customers uncached
# on 2026-06-05 (a standalone prefetch ran on a stale/partial cohort before the
# full fetch landed). The standalone 07:32 cron prefetch remains as a resumable,
# idempotent catch-up (UPSERT keyed on (customer_id, cohort_date) → re-running is
# harmless). Runs only on a successful fetch that produced today's CSV; a chained
# prefetch failure is non-fatal here (the cron catch-up + the executor's live
# fallback both cover it) so it must not flip THIS job's fetch heartbeat.
if [[ "${RC}" -eq 0 && -f "${TODAY_CSV}" ]]; then
    echo "===== $(date '+%F %T %Z') — chaining prefetch on fresh cohort =====" >> "${LOG_FILE}"
    "${BASE_DIR}/run.sh" prefetch >> "${LOG_DIR}/cron.log" 2>&1 \
        && echo "$(date '+%F %T %Z') — chained prefetch ok" >> "${LOG_FILE}" \
        || echo "$(date '+%F %T %Z') — WARN chained prefetch non-zero; cron catch-up + live fallback will cover" >> "${LOG_FILE}"
fi

# Heartbeat: ok on success, down on any non-zero (incl. zero-rows-failure and
# timeout). morning_summary reads this; a 'down' here means no cohort today.
if [[ -f /home/ubuntu/heartbeat_lib.py ]]; then
    python3 /home/ubuntu/heartbeat_lib.py wf_fetch_dpd1 \
        "$([ "${RC}" -eq 0 ] && echo ok || echo down)" \
        "mode=${MODE} exit=${RC} (see ${LOG_FILE})" >/dev/null 2>&1 || true
fi

# Persist a failure marker (independent of heartbeat transport) so the failure
# is visible even if the Ops webhook is down.
if [[ "${RC}" -ne 0 ]]; then
    STATE_DIR="${BASE_DIR}/state/failures"
    mkdir -p "${STATE_DIR}"
    MARKER="${STATE_DIR}/fetch_dpd1_$(date +%F_%H%M%S).json"
    python3 - <<MARKEREOF || true
import json
from datetime import datetime
from zoneinfo import ZoneInfo
try:
    with open("${LOG_FILE}") as f:
        tail = "".join(f.readlines()[-40:])
except OSError:
    tail = "(log unreadable)"
# tz-aware IST — the AWS server runs UTC; a naive now() would mislabel the key.
ts_ist = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
with open("${MARKER}", "w") as f:
    json.dump({"job": "wf_fetch_dpd1", "mode": "${MODE}", "exit_code": ${RC},
               "ts_ist": ts_ist, "log_tail": tail}, f, indent=2)
MARKEREOF
fi

exit "${RC}"
