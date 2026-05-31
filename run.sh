#!/usr/bin/env bash
# Cron wrapper for the vibrium-workflow daemons (Phase 9 orchestrator).
# Activates venv, sources secrets, runs one orchestrator --mode under a
# wall-clock timeout, writes a dated log, emits a heartbeat (job_id wf_<mode>),
# and persists a failure marker on non-zero exit. Pairs with run_fetch.sh
# (which handles the 07:30 collection_view fetch).
#
# Usage:
#   ./run.sh executor      # tick loop — advance runs node→node
#   ./run.sh scheduler     # fire queued calls (shadow by default; see WF_SHADOW)
#   ./run.sh ingest        # pull dispositions, wake AWAIT runs
#   ./run.sh enrollment    # create runs from the daily CSV (07:00 prep slot)
#   ./run.sh alerts        # missing-heartbeat watchdog
#   ./run.sh digest        # daily summary email
#
# Safety: the scheduler runs in SHADOW mode unless WF_SHADOW=0 is exported.
# Going live requires BOTH WF_SHADOW=0 here AND the workflow row's shadow_mode
# flipped off in the console.
set -u

MODE="${1:-executor}"
BASE_DIR="/home/ubuntu/vibrium-workflow"
LOG_DIR="${BASE_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/${MODE}_$(date +%F).log"

WORKFLOW_DB="${WF_WORKFLOW_DB:-${BASE_DIR}/state/workflow.db}"
VIBRIUM_DB="${WF_VIBRIUM_DB:-/home/ubuntu/vibrium-automation/state/vibrium.db}"
CT_CREDS="${WF_CT_CREDS:-/home/ubuntu/Collections_v3/Clevertap campaigns/config_CT_credentials.json}"
HOURLY_CALL_CAP="${WF_HOURLY_CALL_CAP:-750}"

cd "${BASE_DIR}"
# shellcheck disable=SC1091
[[ -f "${BASE_DIR}/venv/bin/activate" ]] && source "${BASE_DIR}/venv/bin/activate"

for envf in redshift.env ops.env; do
    if [[ -f "${BASE_DIR}/secrets/${envf}" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${BASE_DIR}/secrets/${envf}"
        set +a
    fi
done

# Shadow flag for the scheduler: ON unless explicitly disabled.
SHADOW_FLAG="--shadow"
[[ "${WF_SHADOW:-1}" == "0" ]] && SHADOW_FLAG=""

# Per-mode timeout + orchestrator flags. FLAGS is an ARRAY so paths containing
# spaces (e.g. the CT creds live under ".../Clevertap campaigns/...") survive as
# single argv elements — a plain string would word-split on the space and break
# argparse.
FLAGS=()
case "${MODE}" in
    executor)
        TIMEOUT_SEC=900;  FLAGS=(--batch-limit 200) ;;
    scheduler)
        TIMEOUT_SEC=1500
        FLAGS=(--vibrium-db "${VIBRIUM_DB}" --ct-creds "${CT_CREDS}" --hourly-call-cap "${HOURLY_CALL_CAP}")
        [[ -n "${SHADOW_FLAG}" ]] && FLAGS+=("${SHADOW_FLAG}") ;;
    ingest)
        TIMEOUT_SEC=1500 ;;
    enrollment)
        TIMEOUT_SEC=900
        # Enable the 07:00 pre-window prep slot (firing stays gated at 08:00 by
        # the scheduler). Exported BEFORE python starts so the poller reads it.
        export WF_ENROLLMENT_PREP_START_HOUR="${WF_ENROLLMENT_PREP_START_HOUR:-7}" ;;
    alerts)
        TIMEOUT_SEC=300 ;;
    digest)
        TIMEOUT_SEC=600 ;;
    *)
        echo "unknown mode '${MODE}' (executor|scheduler|ingest|enrollment|alerts|digest)" >&2
        exit 2 ;;
esac

echo "===== $(date '+%F %T %Z') — starting wf ${MODE} (timeout=${TIMEOUT_SEC}s, shadow=${SHADOW_FLAG:-off}) =====" >> "${LOG_FILE}"
# ${FLAGS[@]+...} is the set -u-safe empty-array expansion (modes like ingest
# pass no extra flags); plain "${FLAGS[@]}" can trip "unbound" on bash < 4.4.
timeout --kill-after=30 "${TIMEOUT_SEC}" \
    python3 -m workflow.workflow_orchestrator \
        --mode "${MODE}" --workflow-db "${WORKFLOW_DB}" ${FLAGS[@]+"${FLAGS[@]}"} \
        >> "${LOG_FILE}" 2>&1
RC=$?
if [[ "${RC}" -eq 124 ]]; then
    echo "===== TIMEOUT after ${TIMEOUT_SEC}s — process killed =====" >> "${LOG_FILE}"
fi
echo "===== $(date '+%F %T %Z') — finished ${MODE} (exit=${RC}) =====" >> "${LOG_FILE}"

if [[ -f /home/ubuntu/heartbeat_lib.py ]]; then
    python3 /home/ubuntu/heartbeat_lib.py "wf_${MODE}" \
        "$([ "${RC}" -eq 0 ] && echo ok || echo down)" \
        "exit=${RC} (see ${LOG_FILE})" >/dev/null 2>&1 || true
fi

if [[ "${RC}" -ne 0 ]]; then
    STATE_DIR="${BASE_DIR}/state/failures"
    mkdir -p "${STATE_DIR}"
    MARKER="${STATE_DIR}/${MODE}_$(date +%F_%H%M%S).json"
    python3 - <<MARKEREOF || true
import json
from datetime import datetime
from zoneinfo import ZoneInfo
try:
    with open("${LOG_FILE}") as f:
        tail = "".join(f.readlines()[-40:])
except OSError:
    tail = "(log unreadable)"
ts_ist = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
with open("${MARKER}", "w") as f:
    json.dump({"job": "wf_${MODE}", "exit_code": ${RC},
               "ts_ist": ts_ist, "log_tail": tail}, f, indent=2)
MARKEREOF
fi

exit "${RC}"
