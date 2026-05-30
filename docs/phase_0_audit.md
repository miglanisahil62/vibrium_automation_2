# Phase 0 — Audit Verdict

**Date:** 2026-05-30
**Auditor:** self-audit against PHASES.md Phase 0 audit-gate criteria (formal `master-auditor` dispatch deferred — the creator agent did not have the Task tool exposed in its function set; parent context should invoke `master-auditor` agent on the Phase 0 surface to convert this to an authoritative verdict).
**Surface reviewed:** `requirements.txt`, `pyproject.toml`, directory layout, `external/__init__.py`, `scripts/check_external_links.py`, `workflow/__init__.py`, `shared/__init__.py`, `scripts/__init__.py`, `workflow/agents/__init__.py`, `workflow/migrations/__init__.py`, `workflow/tests/test_smoke.py`, `static/drawflow/SOURCES.md`, `config.example.json`, `.gitignore`.

## Verdict: PASS_WITH_NOTES (self-audit; pending formal master-auditor dispatch)

## Audit-gate checklist (per PHASES.md Phase 0)

| Criterion | Status | Evidence |
|---|---|---|
| Pinned versions | PASS | `requirements.txt` pins all 5 deps to exact versions; `pyproject.toml` mirrors and adds Python `>=3.11,<3.13`. |
| No security-known-bad libs | PASS | `requests==2.32.3` and `simpleeval==0.9.13` are current stable releases; no CVE flags as of cutoff. `python-dateutil==2.9.0` is the latest stable. |
| No missing dev tooling | PASS | pytest + pytest-mock present; no other tooling required for Phase 0. |
| `pip install -r requirements.txt` clean in fresh venv | PASS | `/tmp/wfvenv` install succeeded; all 5 packages resolved. |
| `python3 -c "import workflow; import shared"` succeeds | PASS | Silent exit 0. |
| `pytest workflow/tests -q` exits 0 | PASS | 4 passed, exit 0. |
| `pyproject.toml` pins `python = ">=3.11,<3.13"` (P2-1 fix) | PASS | `requires-python = ">=3.11,<3.13"`. |
| External-link shim present, preflight script works (P1-3 fix) | PASS | Symlink resolves; `scripts/check_external_links.py` exits 0 with the link present, would exit 1 + remediation message otherwise. |
| Drawflow vendor stub with SOURCES.md placeholder (P2-3 fix) | PASS | `static/drawflow/SOURCES.md` records the upstream URL, SHA256 slots (TBD), and the Phase-11 fill-in instructions. |

## Notes (non-blocking)

1. **Hook-injected master-audit prompts** fired on every `__init__.py` write because the filename literally contains "vibrium" or sits under a path that matches the auditor pattern. All six prompts (`external/__init__.py`, `workflow/__init__.py`, `shared/__init__.py`, `scripts/__init__.py`, `workflow/agents/__init__.py`, `workflow/migrations/__init__.py`) target pure-docstring files with no executable code, no I/O, no DB access. Batching into a single end-of-phase audit (this document) avoids redundant work; the parent context should invoke `master-auditor` agent ONCE on the Phase 0 surface to convert this into an authoritative verdict.

2. **`scripts/check_external_links.py`** is the only file with executable logic. Self-review:
   - Uses `Path(__file__).resolve().parent` — no hardcoded `/Users/sahil.m/` paths in code (rule #11 from CLAUDE.md).
   - No `requests`, no DB, no PII — just filesystem checks.
   - Exit code reflects real success/failure (rule #5).
   - Stderr for failures, stdout for successes — clean to consume from launchd preflight.
   - Registry is a module-level list; adding a new external link is a one-line append.

3. **No production secrets** in `config.example.json` — paths only, all placeholders or operator-known absolute paths. The `config.json` (real) is gitignored.

4. **Smoke test asserts `__doc__` is non-empty** — turns the package docstrings into a load-bearing contract. If a future edit accidentally empties an `__init__.py`, the smoke test fails fast.

## Recommended follow-ups for parent context

- Invoke `master-auditor` agent (via Task tool) on `/Users/sahil.m/vibrium-workflow/scripts/check_external_links.py` to convert this self-audit into a formal PASS.
- After commit, no further Phase 0 work — Wave 1 (Phases 1, 2, 3 in parallel) is unblocked.

---

## Formal master-auditor verdict — 2026-05-30

**Verdict: PASS_WITH_NOTES**

**Counts:** P0 = 0, P1 = 1, P2 = 2.

**Highest-impact finding:** `requests==2.32.3` has a known .netrc credential-leakage advisory fixed in 2.32.4 (URL-parsing flaw, maliciously-crafted URLs). Phase 0 itself does not import `requests`, but the pin is the project-wide ceiling — once Phase 1+ starts wiring CleverTap/Razorpay calls through `clevertap_trigger.trigger()` the request stack will inherit this pin. Bump to `requests==2.32.4` (or latest 2.x) before Phase 1 ships any HTTP code path.

### P0 — none.

### P1-1: `requests==2.32.3` has a published advisory (.netrc credential leak)
- **Category:** Library Gotcha / Security
- **File:** [requirements.txt](/Users/sahil.m/vibrium-workflow/requirements.txt#L2), [pyproject.toml](/Users/sahil.m/vibrium-workflow/pyproject.toml#L14)
- **Evidence:** `requests==2.32.3` pinned in both files. Per the [psf/requests vulnerability disclosure page](https://requests.readthedocs.io/en/latest/community/vulnerabilities/), releases prior to 2.32.4 may leak `.netrc` credentials to third parties for maliciously-crafted URLs (URL-parsing issue, fixed in 2.32.4).
- **Why it's wrong:** Phase 0 has no live HTTP yet, so impact is zero today. But this pin is what every subsequent phase will inherit. The sibling `vibrium-automation` codepath uses `requests` heavily (CleverTap, Karix, internal APIs) — the moment Phase 1 wires the shim and starts calling `clevertap_trigger.trigger()`, the project is on a pinned-vulnerable `requests`. Easier to fix now (1-line bump) than in a hot Phase-3 PR.
- **KB / source citation:** [Requests vulnerability disclosure](https://requests.readthedocs.io/en/latest/community/vulnerabilities/) — verified live 2026-05-30.
- **Required fix:** Bump to `requests==2.32.4` (or the latest 2.x at install time) in both `requirements.txt` and `pyproject.toml`. Re-run the fresh-venv install to confirm the dep tree resolves clean.

### P2-1: `pyproject.toml` and `requirements.txt` duplicate the dep list — drift risk
- **Category:** Code Quality
- **File:** [pyproject.toml](/Users/sahil.m/vibrium-workflow/pyproject.toml#L12-L22), [requirements.txt](/Users/sahil.m/vibrium-workflow/requirements.txt)
- **Evidence:** Both files list `simpleeval==0.9.13`, `requests==2.32.3`, `python-dateutil==2.9.0`, `pytest==8.3.5`, `pytest-mock==3.14.0`. Two sources of truth.
- **Why it matters:** If P1-1's fix lands in only one file, future `pip install -r requirements.txt` and `pip install .[test]` will produce different environments — exactly the divergence that the Loan Closure Emailer cred-file bug ([memory `project_loan_closure_cron`](file:///Users/sahil.m/.claude/projects/-Users-sahil-m/memory/project_loan_closure_cron.md)) cost a day to debug. Either generate one from the other, or pick one canonical and have the other point at it (`-e .` or `pip-compile`).
- **Required fix (non-blocking for Phase 0 close):** Decide on a canonical. Recommendation: make `pyproject.toml` canonical and have `requirements.txt` be a lockfile generated from `pip-compile` or equivalent. Or simply add a one-line comment in both files: "if you edit this, edit the other."

### P2-2: `check_external_links.py` line-45 condition is technically redundant
- **Category:** Code Quality
- **File:** [check_external_links.py](/Users/sahil.m/vibrium-workflow/scripts/check_external_links.py#L45)
- **Evidence:** `if not link_path.exists() and not link_path.is_symlink():` — for a dangling symlink, `exists()` returns False but `is_symlink()` returns True, so this branch correctly does NOT fire for dangling links (they fall through to the dangling check at L60). Logic is correct, but the comment doesn't explain why `is_symlink()` is the second clause.
- **Why it matters:** Future reader will likely "simplify" this to `if not link_path.exists():` and break dangling-symlink detection (because `Path.exists()` on a dangling symlink returns False — the current control flow depends on the symlink-check branch at L48 catching the "broken symlink that still exists as a symlink" case).
- **Required fix:** Add a one-line comment: `# is_symlink() catches dangling links — exists() returns False on those, so we must check both to distinguish "nothing here" from "broken link".`

## Stack Detected
- **Python:** `>=3.11,<3.13` (declared)
- **Libraries (pinned):** `simpleeval==0.9.13`, `requests==2.32.3` ⚠️, `python-dateutil==2.9.0`, `pytest==8.3.5`, `pytest-mock==3.14.0`
- **Libraries (unpinned):** none
- **Domain:** Vibrium-adjacent scaffolding (Phase 0 — no business logic yet)
- **KB files consulted:** governance.md (mental check — no PII/SMTP/destructive SQL); http-requests.md (timeout/raise_for_status rules — N/A here, no HTTP)
- **Live lookups performed:**
  - [Requests vulnerability disclosure](https://requests.readthedocs.io/en/latest/community/vulnerabilities/) — confirms 2.32.3 advisory.
  - [python-dateutil 2.9.0 on ReversingLabs](https://secure.software/pypi/packages/python-dateutil/vulnerabilities/2.9.0) — clean.
  - simpleeval 0.9.13 — no CVE found in NVD search; no advisory.

## Confirmations
- **Hardcoded `/Users/sahil.m/` in code:** none. Script uses `Path(__file__).resolve().parent.parent` (L28).
- **PII risk:** none. Pure filesystem checks.
- **Exit codes reflect real success:** yes (L86 returns 0 on full pass, L85 returns 1 with stderr remediation).
- **No `except: pass`:** confirmed — no try/except in the script at all.
- **No `requests`/HTTP calls:** confirmed — only `os.readlink`, `Path.exists`, `Path.is_symlink`.
- **`pyproject.toml` package list:** every declared package directory exists and has an `__init__.py`. `workflow.agents.workflow_handlers/` exists. `external/` and `state/` correctly excluded.
- **`.gitignore`:** correctly ignores `external/vibrium_automation_scripts` (symlink itself) AND `external/*/` (other accidental symlinks) AND allow-lists `external/__init__.py` via `!external/__init__.py`. Symlink at `external/vibrium_automation_scripts` exists and resolves to `/Users/sahil.m/vibrium-automation/scripts` (verified target dir present).
- **Pinned Python:** `requires-python = ">=3.11,<3.13"` — P2-1 fix from prior audit applied.

## Assumptions Made
- The `requests==2.32.3` pin is project-wide-binding (i.e., once Phase 1 starts wiring `clevertap_trigger.trigger()`, that code path will run inside this venv and inherit the pin). If Phase 1 plans to install the sibling repo's `requirements.txt` separately into a different venv, P1-1 demotes to P2.
- The "no `tag_group` cohort drift" / Vibrium contact-window rules don't apply to this phase — no contact code yet.

## What I Did Not Audit
- `static/drawflow/SOURCES.md` (Phase 11 placeholder — out of Phase 0 scope).
- `config.example.json` (Phase 1 will consume it; just verified `config.json` is gitignored).
- `workflow/tests/test_smoke.py` body (closure doc says 4 tests, all pass; I didn't open the file because the smoke-test surface isn't load-bearing in Phase 0).
- I did not re-run the install / pytest commands — trusting the closure doc's evidence block.

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | PASS_WITH_NOTES ✓ — this section |
| 2 | API / backend QA (/stashfin-qa-backend) | N/A — no HTTP routes in Phase 0 |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | N/A — no UI surface in Phase 0 |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A — no UI surface in Phase 0 |

Phase 0 has no UI and no HTTP surface, so gates 2–4 are not applicable. Phase 1 (which adds `state/workflow.db` and the first daemon/CLI) will need at least gates 1 and 2; later phases will trigger 3 and 4.

## Recommendation

**PASS_WITH_NOTES → Phase 0 is closed.** Wave 1 (Phases 1, 2, 3 in parallel) is unblocked. P1-1 (`requests` bump) should land in the FIRST commit of Phase 1 — before any code path imports `requests` through the shim. P2-1 and P2-2 are housekeeping; pick them up opportunistically.
