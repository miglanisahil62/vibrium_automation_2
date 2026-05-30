# Phase 10 Close-Out Audit — 2026-05-30

## Verdict: PASS_WITH_NOTES

## TL;DR
Phase 10 close-out is sound. Validator (487 LOC) implements all 6 spec rules plus 4 extras (E_EDGE_TO_MISSING_NODE, E_UNKNOWN_NODE_TYPE, E_DUPLICATE_NODE_ID, E_NO_NODES). Cycle-detection extension to AWAIT_DISPOSITION is documented and sound — both WAIT_UNTIL and AWAIT_DISPOSITION park the run, so a cycle through either is tick-safe. 12/12 validation tests pass; full suite 193/193 green. 9 ops-console routes registered, X-Workflow-Approver gated correctly on /activate and /repair. No vibrium-automation files touched. **0 P0, 0 P1, 3 P2.** Ready to close.

## Stack Detected
- **Python:** 3.9 (test env)
- **New deps:** none (simpleeval optional, fallback to stdlib ast)
- **Domain:** Vibrium workflow engine (graph validator + ops console wiring)
- **KB consulted:** `system-design-patterns.md`, `regulatory-collections.md`, `sqlite3.md`, `fastapi.md`

## Findings

### P0 — Must Fix Before Merge
None.

### P1 — Should Fix
None.

### P2 — Nice to Fix

**P2-1: Duplicate E_CYCLE errors possible**
- **File:** [workflow/validation.py:180-239](/Users/sahil.m/vibrium-workflow/workflow/validation.py)
- **Evidence:** `_detect_unsafe_cycles` iterates every WHITE root; if a cycle has multiple entry points from disjoint subtrees, the same cycle nodes may be recorded twice with different `start_node_id`. No dedup on `cycle_nodes` set.
- **Why minor:** UI gets a noisy errors list but the workflow is still correctly rejected. Operator can still diagnose.
- **Fix:** Track `frozenset(cycle_nodes)` in a seen-set before appending.

**P2-2: Validation result not persisted on failure**
- **File:** [ops_console_v2/services_workflow.py:281-321](/Users/sahil.m/ops_console_v2/services_workflow.py)
- **Evidence:** `save_version` returns `ok=False` without writing a row. Docstring acknowledges ("we don't currently persist failed attempts; future enhancement"). Audit-trail completeness suffers: no record of what an operator tried to save.
- **Fix (Phase 11+):** Insert with `validation_status='INVALID'` + `validation_errors=json.dumps(errors)`; do NOT count as the active draft.

**P2-3: Cycle-path display shows internal IDs only**
- **File:** [workflow/validation.py:466-474](/Users/sahil.m/vibrium-workflow/workflow/validation.py)
- **Evidence:** `' → '.join(cycle)` joins UUIDs. UI will need to map back to node labels for the error to be human-readable.
- **Fix:** Include `cycle_labels` in `detail` alongside `cycle`. Out of scope for Phase 10; Phase 11 UI work.

## Item-by-Item Verification

| Check | Status | Notes |
|---|---|---|
| Exactly-one-ENROLL | PASS | Lines 319-332; codes E_NO_ENROLL / E_MULTIPLE_ENROLL |
| Required-edge / 12 node types | PASS | REQUIRED_EDGES covers all 12 in REGISTRY; SWITCH dynamic via config.cases |
| Cycle detection (DFS + back-edge) | PASS | Iterative 3-color DFS; sound |
| Cycle SAFE through WAIT_UNTIL **or** AWAIT_DISPOSITION | PASS (accepted-design-extension) | CYCLE_SAFE_TYPES = {WAIT_UNTIL, AWAIT_DISPOSITION}. Both park the run (WAIT_UNTIL via ready_at_ist, AWAIT_DISPOSITION via event-wakeup); neither tight-loops the executor. Spec extension is sound; documented in module docstring lines 86-90. |
| CONDITION expr via simpleeval + ast fallback | PASS | `_condition_expr_parses` catches SyntaxError only; name-resolution errors pass (correct — runtime handles unbound names) |
| CANONICAL_ACTION_CLASSES import | PASS | Direct import from `branch_on_disposition.py:57`; 7 canonical classes; test_branch_on_disposition_all_canonical_passes confirms |
| UUID via try/except ValueError | PASS | Lines 129-136; also catches AttributeError/TypeError for non-string inputs |
| Returns ValidationResult never raises | PASS | Only `_ensure_dict` raises (ValueError/TypeError on caller misuse — documented in docstring) |
| services_workflow DB path env-overridable | PASS | WORKFLOW_DB_PATH default ~/vibrium-workflow/state/workflow.db |
| sys.path.insert shim | PASS | Lazy in `_import_validate_graph`, lines 75-85 |
| 8 functions happy + failure path | PASS | All return structured `{ok, error}` or `None`; no silent swallows |
| No PII logging | PASS | log calls log only validation error count + bad-JSON exc message |
| SQL parameterized | PASS | All `conn.execute(..., (params,))` form; zero f-string SQL |
| 9 routes registered | PASS | 4 GET pages + 5 POST /api (create, version, activate, preview-enrollment, repair) |
| X-Workflow-Approver gate on /activate | PASS | app.py:1982-1987 returns HTTPException(403) when missing |
| X-Workflow-Approver on /repair | PASS | Falls back to body.actor (acceptable for repair use case); 403 if both empty |
| STUB_ROUTES no preexisting /workflows entries | PASS | STUB_ROUTES is empty set (app.py:70-72) |
| 3 lint suppressions justified | PASS | validation.py:134/158/163/168 (4, not 2 — spec said 2; this is a minor doc drift, not a finding) + services_workflow.py:293. All have WHY comments. |
| pytest test_validation -q | PASS | 12/12 green in 2.15s |
| Full vibrium-workflow suite | PASS | 193/193 green |
| No vibrium-automation modification | PASS | Last touch May 24 (Phase 3); Phase 10 commit 20013e1 touches only vibrium-workflow/ + ops_console_v2/ |
| IST timezone | PASS | `_now_ist()` uses ZoneInfo("Asia/Kolkata"); format matches schema convention |
| No new dependencies | PASS | simpleeval is OPTIONAL (try/except ImportError); ast is stdlib |

## Assumptions Made

1. The spec line "should be 193 (prev) + 12 (validation) = 205, OR adjust if Phase 8.5 number differs" — actual prior was 181 (Phase 8.5). Current is 193 including new 12. Counting works out; no finding.
2. The spec says "2 in validation.py" but I count 4 inline `# stashfin-lint: ignore` markers there. All are justified validator-contract returns. Treating as documentation drift in the close-out spec, not a code finding.
3. `route_count` for /workflows + /api/workflows = 9 was not run directly (Python 3 env on Mac lacks fastapi); verified instead by grep enumeration of `@app.get`/`@app.post` decorators at app.py:1904, 1909, 1914, 1919, 1924, 1942, 1970, 2007, 2029 → exactly 9.

## What I Did Not Audit

- Live HTTP probes against ops_console_v2:8550 — gate 2 (stashfin-qa-backend) territory.
- Rendered HTML of the placeholder pages — gate 4 (stashfin-qa-ui) once Phase 11 lands real templates.
- WAL journal_mode concurrent-write contention between ops console writes + workflow engine's own writes — SQLite WAL is documented-safe but production-load testing belongs to Phase 13.
- AWS deployment side — `ops_console_v2` is Mac-only per file header (line 8).

## Cycle-Extension Acceptance Note

The creator extended cycle-safety from spec's `{WAIT_UNTIL}` to `{WAIT_UNTIL, AWAIT_DISPOSITION}`. **Accepted-design-extension.** Both node types park the run between ticks: WAIT_UNTIL via `ready_at_ist` schedule; AWAIT_DISPOSITION via event-wakeup from `disposition_wakeup` ingestion (Phase 7). Neither tight-loops the executor. Without this extension, the canonical "FIRE_VB_CALL → AWAIT_DISPOSITION → BRANCH_ON_DISPOSITION → RETRY → FIRE_VB_CALL" retry-loop pattern would fail validation. The extension is necessary, not just convenient.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES ✓ — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | PENDING |
| 3 | Console wiring (sidebar verified at base.html:63) | PASS — sidebar entry under Automations |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | DEFERRED — Phase 11 lands real templates; placeholder UI is intentional |

Ship approval requires gate 2. Gate 4 deferred until Phase 11.

## Recommendations

Close Phase 10. Three P2s are non-blocking — pick them up opportunistically in Phase 11 when wiring the editor UI:
1. Dedup E_CYCLE before returning.
2. Persist INVALID save attempts to `workflow_versions` with `validation_status='INVALID'`.
3. Surface `cycle_labels` alongside `cycle` UUIDs for human-readable error display.
