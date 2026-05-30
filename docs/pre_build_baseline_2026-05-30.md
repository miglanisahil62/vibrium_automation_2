# Pre-Build Baseline — 2026-05-30 17:31 IST

Captured before any code lands in this repo. Snapshot of all production systems via `/pipeline-integrity-auditor full`.

**Overall: ❌ FAIL — 15 PASS / 2 WARN / 4 FAIL**

## What's pre-existing and NOT caused by this project

| System | Status | Note |
|---|---|---|
| Vibrium last orchestrator tick | ❌ 23.4h ago | Crontab is `*/30 8-18 * * *`; should be ticking every 30 min during call hours. Something is broken. **Investigate before this project's Phase 3 starts** — Phase 3 closure requires adhoc heartbeat-green for ≥24h. |
| Vibrium escalations | ❌ 6 unbriefed | Backlog, unrelated. |
| Vibrium PENDING queue | ✅ 6,599 rows | Queue is full — adhoc has work to do once it resumes ticking. |
| Vibrium snapshot freshness | ✅ 1m | Snapshot pull is fine; just the orchestrator tick is stale. |
| Vibrium git HEAD | `3033a2e` (gate fixes from 2026-05-30) | Latest commit landed today. |
| Ops Console v1 (port 8500) | ❌ down | Expected — v1 is MAINTENANCE-ONLY since 2026-05-24. |
| Ops Console v2 (port 8550) | ✅ HTTP 200 | Good — Phase 10/11 will land routes here. |
| Streamlit :8505 (settlement_policy) | ❌ down | Pre-existing. |
| Streamlit :8506 (x_bucket_dashboard) | ❌ down | Pre-existing. |
| Streamlit :8510 (CI pilot_dashboard) | ❌ down | Pre-existing. |
| Streamlit :8520 (xirr_general) | ❌ down | Pre-existing. |
| CI latest parquet | ⚠️ 9.7d ago | Outside the 14-day FAIL threshold but stale. |
| Collection risk scoring | ✅ all green | Daily cron OK. |
| Loan closure emailer | ✅ all green | Daily cron OK. |
| AWS disk | ✅ 77% used | Within 75–90% WARN window — keep an eye on it; this project will add `workflow.db` over time. |

## Implications for this project

1. **Phase 3 closure is blocked** until adhoc Vibrium tick resumes. Phase 3 closure definition explicitly requires `adhoc heartbeat green for ≥24h` after the cap-refactor flag is flipped. If adhoc is sitting stale, the closure timer can't start.

2. **AWS disk at 77%** — `workflow.db` will live at `state/workflow.db` on AWS. Plan for the disk to grow. Set up the Phase 8.5 alert to fire if disk > 85%.

3. **CI pilot dashboard down** — not blocking, but the workflow engine could plausibly grow a similar dashboard. If we want to reuse the :8510 pattern, fix the existing one first.

4. **Recommended action before Phase 0a fires:** investigate why adhoc orchestrator hasn't ticked in 23h — could be a lock-file issue, a CT credential expiry, a Redshift outage, or something the gate-fix in commit 3033a2e introduced. Until that's diagnosed, firing the Phase 0a `tag_group` spike could disturb a system that's already in a bad state.

## Decision required

Sahil should look at the Vibrium tick issue before Wave 0 starts. The workflow project doesn't depend on the adhoc system being healthy for Phase 0a (it just needs CT writes + 24h observation), but it does for Phase 3.
