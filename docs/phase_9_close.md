# Phase 9 — Workflow Orchestrator + Launchd — Closure

**Status:** delivered inline (creator-agent dispatches in Wave 5 hit transient socket-close errors twice; finished by orchestrator).

## Deliverables

### Phase 8 deferred P1 fixes (in `workflow/enrollment_poller.py`)

| Fix | Mechanism |
|---|---|
| **P1-1 — Daily-cap race** | Non-blocking `fcntl.flock` on a sidecar lock file (`state/locks/enrollment_poller.lock`). Second concurrent tick raises `LockContended`; CLI catches it and exits 0 (benign overlap — next tick at +30 min catches up). |
| **P1-2 — ImportError silent swallow** | `_is_callable_now()` ImportError now re-raised from `run()`. CLI catches at top level and exits 3 (operational failure — broken pre_call_gate symlink). Phase 8.5 detector A catches the missing-heartbeat within 30 min as a backstop. Split `_Stats.outside_window` into `outside_rbi_window` + `outside_enrollment_window` so the digest + alerts can distinguish "evening idle" from "symlink broken". |

Tests in `workflow/tests/test_enrollment_poller_p1_fixes.py` — 7 tests covering: lock acquire/release, contention, breadcrumb write, daemon-level contention, CLI contention exit-code, ImportError propagation from run(), CLI ImportError exit-code 3.

### Orchestrator + daemons

| File | Purpose |
|---|---|
| `workflow/workflow_orchestrator.py` | Single CLI dispatcher. `--mode {executor, scheduler, ingest, enrollment, alerts, digest}`. Stamps `wf_agent_events` with `started` → `ok`/`down` per tick. Exit codes: 0 clean, 1 daemon raised, 2 invalid CLI, 3 ImportError. |
| `workflow/workflow_digest.py` | Daily summary builder — per-workflow run-status counts, per-daemon 24h heartbeat aggregates, shadow-run count. Payload-only (no SMTP send — gated behind Sahil approval per `feedback_smtp_governance_block`). |

### Daemon-name contract (pinned by Phase 8.5 audit)

| `--mode` | `agent` value | Spec source |
|---|---|---|
| `executor` | `workflow_executor` | Phase 8.5 audit |
| `scheduler` | `workflow_scheduler` | Phase 8.5 audit |
| `ingest` | `workflow_ingest` | Phase 8.5 audit |
| `enrollment` | `workflow_enrollment` | Phase 8.5 audit |
| `alerts` | `workflow_alerts` | Phase 9 (new) |
| `digest` | `workflow_digest` | Phase 9 (new) |

`test_daemon_name_contract_six_keys` in `test_orchestrator.py` is the regression test. If a future edit drifts the name strings, Phase 8.5's detector A fires forever — this test catches it at PR-time.

### Launchd plists (6 files in `workflow/launchd/`)

All pass `plutil -lint`. Run-as: `KeepAlive=false` + `RunAtLoad=false` (cron-style, not always-on).

| Plist | Cadence | Window guard |
|---|---|---|
| `com.sahil.workflow.executor.plist` | `StartInterval=900` (15 min, all day) | none — executor advances waits + transitions outside the call window too (no fire). |
| `com.sahil.workflow.scheduler.plist` | `StartInterval=300` (5 min, all day) | daemon-side `is_callable_now()` — emits "skipped — outside window" heartbeat outside 08:00–19:00 IST. `USE_SHARED_AUDIT_CAP=true` env var set to match adhoc system's wrapper. |
| `com.sahil.workflow.ingest.plist` | `StartInterval=900` (15 min, all day) | none — dispositions arrive whenever. |
| `com.sahil.workflow.enroll.plist` | `StartInterval=1800` (30 min) | daemon-side 08:00–18:00 IST narrower window + `fcntl.flock` (P1-1 fix). |
| `com.sahil.workflow.alerts.plist` | `StartInterval=900` (15 min, all day) | none — alert detection is read-only on workflow.db. |
| `com.sahil.workflow.digest.plist` | `StartCalendarInterval` daily at 09:01 IST | once-per-day. `:01` minute avoids the round-0 thundering herd. |

Logs all land in `state/logs/<daemon>.log`.

### Install instructions (operator-facing)

```bash
# One-time setup on Sahil's Mac:
cd /Users/sahil.m/vibrium-workflow
mkdir -p state/logs state/locks

# Copy plists into LaunchAgents:
cp workflow/launchd/com.sahil.workflow.*.plist ~/Library/LaunchAgents/

# Load each (or `launchctl bootstrap gui/$UID ~/Library/LaunchAgents/<file>` on modern macOS):
for f in ~/Library/LaunchAgents/com.sahil.workflow.*.plist; do
  launchctl load -w "$f"
done

# Verify all 6 are scheduled:
launchctl list | grep com.sahil.workflow
```

To unload (e.g., for a Phase 12 dry-run window):
```bash
for f in ~/Library/LaunchAgents/com.sahil.workflow.*.plist; do
  launchctl unload "$f"
done
```

## Skipped (deferred per spec)

### Cross-repo heartbeat registration in `~/vibrium-automation/scripts/agents_health_check.py`

The spec's Phase 9 §5 deliverable was to register the 6 new daemons in the existing `agents_health_check.py` registry so `pipeline-integrity-auditor` skill can see them. **Deferred per the original Phase 9 prompt's instruction** ("cross-repo edit is too risky for Phase 9"). The pipeline-integrity-auditor can see workflow daemons via `wf_agent_events` directly; documented for operators as:

```bash
# Ad-hoc inspection of workflow daemon health:
python3 -c "
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
IST = ZoneInfo('Asia/Kolkata')
cutoff = (datetime.now(IST) - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
cn = sqlite3.connect('/Users/sahil.m/vibrium-workflow/state/workflow.db')
rows = cn.execute(
    'SELECT agent, MAX(ts_ist) FROM wf_agent_events '
    'WHERE ts_ist >= ? GROUP BY agent', (cutoff,)
).fetchall()
print('Recent (last 30 min):', rows or '(none)')
cn.close()
"
```

A future small phase can wire this into `agents_health_check.py` formally.

## Tests

- `test_orchestrator.py`: **8 tests** — dispatch happy path, exception → down, ImportError → 3, kwarg threading, daemon-name contract, invalid mode, missing required CLI args, heartbeat-write swallows sqlite errors.
- `test_enrollment_poller_p1_fixes.py`: **7 tests** — P1-1 + P1-2 coverage.
- `test_digest.py`: **7 tests** — empty DB, multi-workflow counts, erroring>24h flag, waiting>7d flag, daemon aggregation, shadow run count, dry-run + no-smtplib assertion.

**22 new tests; full suite 215/215 green.**

## Outstanding (deferred to follow-up phases)

- **SMTP send for digest + alerts** — payload-only today. Real send requires Sahil-approved SMTP wrapper.
- **agents_health_check.py registry** — see above.
- **The Phase 8.5 audit's daemon-name contract is now enforced by `test_daemon_name_contract_six_keys`** — any drift fails CI before reaching prod.

## Verdict

**PASS_WITH_NOTES** (self-assessment; formal master-auditor dispatch follows). All acceptance criteria met:
- ✅ `pytest workflow/tests/test_orchestrator.py -q` — 8 green.
- ✅ `python3 -m workflow.workflow_orchestrator --mode alerts --workflow-db /tmp/empty.db --dry-run` — clean exit on empty DB.
- ✅ All 6 plists `plutil -lint` clean.
- ✅ Daemon-name contract matches Phase 8.5 spec (test-enforced).
- ✅ Phase 8's 2 deferred P1s addressed + tested.
- ✅ No file under `/Users/sahil.m/vibrium-automation/` modified.
