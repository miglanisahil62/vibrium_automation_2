# Phase 4b Close-Out Audit — master-auditor verdict (final)

**Date:** 2026-05-30
**Scope:** `branch_on_disposition.py` (critical-surface), `switch.py`, `wait_until.py`, `counter.py`, `__init__.py` registry, Phase 4b tests in `test_handlers.py`.
**Auditor:** master-auditor (overwriting creator placeholder per task instructions).

## Verdict: **PASS**

## TL;DR
All four Phase 4b handlers implement their documented contracts. Critical-surface `branch_on_disposition.py` clears `last_disposition_action_class` on every successful branch — verified by both the parametrized test across all 7 canonical action_classes and the dedicated `test_scratchpad_cleared_on_successful_branch`. REGISTRY exposes exactly the 12 expected keys. Tests: **73 passed, 0 failed** (delta vs. spec'd 69 is creator-added richer 4c/SetCtProp coverage, not a regression). No P0/P1/P2 findings.

## Load-bearing claims — verified

### 1. branch_on_disposition.py (CRITICAL-SURFACE)
- Reads `run.scratchpad["last_disposition_action_class"]` at line 76.
- Missing/falsy key → `next_edge="error"`, `scratchpad_patch={"branch_error":"no_disposition"}` (lines 78–88). Uses `if not action_class` — empty-string also routes to error, consistent with documented "no disposition recorded" intent.
- `cases.get(action_class)` with `default` fallthrough (lines 90–116).
- **Scratchpad-clear on every successful branch** (case-match OR default): `scratchpad_patch={"last_disposition_action_class": None}` at line 123. On no-case-no-default error path, the key is intentionally preserved (lines 101–114) so a Phase 10 repair API can re-route — defensible.
- `CANONICAL_ACTION_CLASSES` frozenset (lines 57–65) holds all 7 enum values; consumable by Phase 10 validator.
- No PII in side_effect. action_class is a non-PII enum.

### 2. switch.py
- `on` / `cases` / `default` config; rejects missing/empty `on` (lines 44–51) → error.
- `str(raw_value)` coercion at line 67 — numeric scratchpad matches string case key (proven by `test_numeric_value_matches_string_key`).
- Missing scratchpad key → `next_edge="error"`, `switch_error="missing key: <name>"` (lines 56–62).
- Missing case + missing default → error edge (no raise from handler).

### 3. wait_until.py
- Three relative grammars (`T+N day`, `T+N day at HH:MM`, `T+N hour`); strict — unparseable → error.
- Absolute `YYYY-MM-DD` only; any deviation → `wait_error="invalid_date_format"`.
- All outputs IST-naive `%Y-%m-%d %H:%M:%S` via `_IST_TS_FMT`; anchor is `datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)` (CLAUDE.md rule 1 satisfied).
- **Both `relative` AND `absolute` set → error** (lines 126–130, `ambiguous spec`) — creator-added strictness, verified in source.
- Late wakeup: `ready_at_ist` still set; side_effect tags `(late — fires next tick)`; executor's `ready_at_ist <= now` filter handles immediate-fire.
- HH validation `0 <= hh <= 23 and 0 <= mm <= 59` (line 76) — correct.

### 4. counter.py
- `node.config = {"name": ..., "limit": N}`; reads `scratchpad.get(name, 0)`.
- Increment by 1; `incremented >= limit` → `at_limit`, else `under_limit` (line 80).
- `scratchpad_patch={name: incremented}` — single-key (line 84).
- Repeated at-limit calls keep advancing — proven in `test_repeated_calls_keep_advancing` (r4 → `attempts=4`).
- `limit <= 0` and non-integer current both route to `error` with named `counter_error`.

### 5. REGISTRY
- Live check: `python3 -c "from workflow.agents.workflow_handlers import REGISTRY; print(sorted(REGISTRY.keys()))"` →
  `['ASSIGN_AGENT', 'AWAIT_DISPOSITION', 'BRANCH_ON_DISPOSITION', 'CONDITION', 'COUNTER', 'ENROLL', 'FETCH_CT_PROPS', 'FIRE_VB_CALL', 'SET_CT_PROP', 'SWITCH', 'TERMINATE', 'WAIT_UNTIL']` — exact match. Count = 12.

### 6. Tests
- `pytest workflow/tests/test_handlers.py -q` → **73 passed**, 0 failed, 1 unrelated urllib3/LibreSSL warning.
- `test_each_canonical_action_class_branches_correctly` parametrized across all 7 enum values; asserts both edge routing AND `scratchpad_patch.get("last_disposition_action_class") is None` (the clear contract).

### 7. Cross-cutting
- `/Users/sahil.m/vibrium-automation/` — working tree clean (`git status` confirmed). No modifications outside vibrium-workflow.
- `tag_group` — grep shows references only in 4a's `fire_vb_call.py`, `workflow_scheduler.py`, `workflow_ingest.py`, migrations, and tests, all as docstring/comment no-op references documenting the Phase 0a invariant. Zero occurrences in any of the four Phase 4b handler bodies.
- Three `# stashfin-lint: ignore` suppressions (wait_until.py:101, counter.py:59, counter.py:73) — all on `except (TypeError, ValueError)` paths that return `next_edge="error"` with a structured `*_error` key in scratchpad_patch. Each has a justification comment naming the documented contract. Not silent failures.

## P0 — none
## P1 — none
## P2 — none

## Assumptions
- Test count delta (73 actual vs. spec-stated 69) is creator-added richer coverage in TestSetCtProp/TestAssignAgent (8 + 6 = 14 tests for 4c, not 19; total stays consistent). All green — treated as additive, not a regression.
- The executor's interpretation of `scratchpad_patch={"key": None}` as a clear-signal is documented in `types.py` and `branch_on_disposition.py` but not yet exercised by Phase 5 code in scope.

## What I did not audit
- Phase 5 executor's actual merge semantics for `None`-valued patches (out of scope; Phase 5 will be audited separately).
- The "Phase 10 save-time validator" — referenced in comments but not yet implemented.

## Recommendations
Close Phase 4b. Two follow-ups for Phase 10 validator (track in close-out doc):
1. Enforce `BRANCH_ON_DISPOSITION.cases` keys ⊆ `CANONICAL_ACTION_CLASSES` (warn on unknown).
2. Enforce `WAIT_UNTIL` has exactly one of `relative`/`absolute`.
3. Enforce `BRANCH_ON_DISPOSITION` + `SWITCH` either have `default` OR a wired `error` edge in the graph.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no HTTP surface in Phase 4b |
| 3 | Console wiring | N/A — handler-internal change, no UI surface |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface |

Phase 4b is handler-internal. Ship on gate 1 alone. Gates 2–4 reactivate when Phase 5+ surfaces routes/UI.
