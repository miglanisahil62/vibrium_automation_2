# Phase Plan Audit — 2026-05-30

**Auditor:** master-auditor (agent ab5e15ba919f964e3)
**Subject:** `/Users/sahil.m/vibrium-workflow/PHASES.md` rev 1.0
**Verdict:** PASS_WITH_NOTES

## P0 findings (all addressed in PHASES.md rev 1.1)

- **P0-1** — Phase 3 backwards-compat: existing `check_cap_and_cooldown(customer_id, today_fired_rows)` takes pre-loaded rows; naive refactor breaks batched callers. **Fix:** feature-flagged migration, keep old signature, add new batched path via `customer_call_audit.batch_count_today()`.
- **P0-2** — Phase 7's `tag_group` filter relies on a column that may not exist in `collection_comment_data` and whose round-trip survival is unverified. **Fix:** Phase 0a spike (10 test fires, per-channel observation, KB note).
- **P0-3** — Wave 2 starts immediately after Phase 3 audit-PASS, but Phase 6 needs Phase 3 *deployed* on AWS with verified observability. **Fix:** Phase Closure Definition section added; cross-repo phases require merge + deploy + 24h heartbeat green.

## P1 findings (addressed in PHASES.md rev 1.1)

- **P1-1** — Acceptance "within 5%" was unanchored. **Fix:** Phase 12 now has `expected_distribution.json`; Phase 13 acceptance rewritten as workflow-internal measurable criteria.
- **P1-2** — Phase 11 accessibility under-specified. **Fix:** explicit a11y acceptance criteria added (tab order, aria-labels, JSON-fallback parity, ESC/Enter behavior).
- **P1-3** — Phase 6's clevertap_trigger import path unresolved. **Fix:** locked to symlink approach; Phase 0 ships the symlink + check_external_links.py.
- **P1-4** — Phase 12 conflated hard guarantees with soft tolerances. **Fix:** split into "Hard gates" and "Soft gates" sections.
- **P1-5** — Phase 9 launchd "every 5 min, 08:00–19:00 IST" was ambiguous (StartInterval vs StartCalendarInterval). **Fix:** pinned StartInterval + daemon-side `is_callable_now()` guard pattern.
- **P1-6** — Phase 4 was one coarse phase delivering 12 handlers. **Fix:** split into 4a (core, unblocks Phase 5), 4b (branching), 4c (side-effects); critical-surface handlers get individual master-auditor passes.
- **P1-7** — No observability/alerts phase. **Fix:** Phase 8.5 added (`workflow/alerts.py` + runbook + simulated-failure tests).

## P2 findings (addressed in PHASES.md rev 1.1)

- **P2-1** — Python version pin added (`>=3.11,<3.13`).
- **P2-2** — `e2e_shadow_test.py` now emits structured `shadow_run.json` for auditor ingestion.
- **P2-3** — Drawflow `SOURCES.md` with SHA256 + vendor date added to Phase 11.
- **P2-4** — Phase 9 digest format pinned (columns + thresholds).
- **P2-5** — `clevertap_trigger.py` patch must specify exact JSON path for `tag_group` (decided in Phase 0a).

## Assumptions documented in the audit

- `tag_group` is meant to flow through CT externaltrigger → bot → CRM comment field (Phase 0a must verify or reject).
- Phase 13 "baseline" reinterpreted as workflow-internal criteria, not adhoc-system outcome comparison.
- `vibrium-automation` is not a Python package; symlink chosen over `pip install -e`.

## Pipeline-integrity-auditor

Pipeline-integrity-auditor is a **skill**, not a subagent — it audits live operational state of all production systems (requires tsh SSH approval to AWS). Deferred to a pre-build baseline run if Sahil approves; not applicable to phase-plan review itself.

## Outcome

Plan revised to rev 1.1. The 3 P0s and most P1s/P2s are reflected directly in PHASES.md. Ready for Sahil's go-ahead on Wave 0 (Phase 0a spike).
