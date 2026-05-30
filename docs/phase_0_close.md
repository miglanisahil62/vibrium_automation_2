# Phase 0 — Closure Summary

**Date closed:** 2026-05-30
**Branch:** main
**Predecessor phase:** Phase 0a (tag_group smoke test — closed 2026-05-30)

## What this phase delivered

Repo skeleton, pinned tool versions, external-symlink contract, pytest harness, and the placeholder for the Phase 11 Drawflow vendor drop. No business logic.

## Files created or modified

| Path | Purpose |
|---|---|
| `requirements.txt` | 5 pinned production+test deps. |
| `pyproject.toml` | Declares `vibrium_workflow` package, pins Python `>=3.11,<3.13` (P2-1 fix), sets `setuptools` build backend, scopes `setuptools.packages` so `external/` + `state/` never get packaged, owns the `[tool.pytest.ini_options]` config (single location — no separate `pytest.ini`). |
| `config.example.json` | Operator-facing config template — paths, RBI window, caps, alert email. `shadow_mode` defaults to `true`. No real credentials. |
| `external/__init__.py` | Shim package that lets workflow code resolve `from external.vibrium_automation_scripts.<module> import ...`. |
| `external/vibrium_automation_scripts -> /Users/sahil.m/vibrium-automation/scripts` | Per-machine symlink (not committed; gitignored). P1-3 fix: locked to "symlink" approach, not pip-install-e. |
| `scripts/__init__.py` | Marks `scripts/` as a Python package. |
| `scripts/check_external_links.py` | Preflight: exits 0 if every required external symlink resolves, exits 1 with remediation message otherwise. Uses `Path(__file__).resolve().parent` — works on Mac dev and AWS. |
| `workflow/__init__.py` | Marks `workflow/` as a package. |
| `workflow/agents/__init__.py` | Package marker. |
| `workflow/migrations/__init__.py` | Package marker. |
| `workflow/tests/test_smoke.py` | 4 import tests covering `workflow`, `shared`, `scripts`, and the workflow subpackages. |
| `shared/__init__.py` | Package marker; `customer_call_audit.py` lands here in Phase 3. |
| `static/drawflow/SOURCES.md` | Phase 11 vendor-drop placeholder (P2-3 fix). Records the upstream URL, SHA256 slots, vendor date — to be filled when the JS/CSS is downloaded. |
| `.gitignore` | Added: `config.json`, `external/<symlink>`, allow-rule for `external/__init__.py`. |
| `docs/phase_0_close.md` | This document. |

## Pinned versions (requirements.txt)

```
simpleeval==0.9.13
requests==2.32.3
python-dateutil==2.9.0
pytest==8.3.5
pytest-mock==3.14.0
```

Python: `>=3.11,<3.13` (pyproject.toml — P2-1 fix).

## Acceptance commands — all pass

```bash
$ python3 -m venv /tmp/wfvenv && /tmp/wfvenv/bin/pip install -r requirements.txt
# pytest-8.3.5, pytest-mock-3.14.0, python-dateutil-2.9.0, requests-2.32.3, simpleeval-0.9.13 installed

$ python3 -c "import workflow; import shared"
# (silent, exit 0)

$ /tmp/wfvenv/bin/pytest workflow/tests -q
# .... [100%] — 4 passed
# exit=0

$ python3 scripts/check_external_links.py
# ok: /Users/sahil.m/vibrium-workflow/external/vibrium_automation_scripts -> /Users/sahil.m/vibrium-automation/scripts
# exit=0
```

## Choices made beyond literal task spec

- **No separate `pytest.ini`** — kept all pytest config in `pyproject.toml` per the task's "ONE location, not both" instruction.
- **`setuptools.packages` is explicit, not auto-discovered** — prevents `external/` (symlinked) and `state/` (DB files) from ever being packaged. Listed: `workflow`, `workflow.agents`, `workflow.agents.workflow_handlers`, `workflow.migrations`, `workflow.tests`, `shared`, `scripts`.
- **`.gitignore` extended** for `config.json` and the per-machine symlink under `external/`, with an allow-rule for `external/__init__.py`. Without this, the symlink path would either need to be committed (breaks portability) or the shim would silently disappear from VCS.
- **Smoke test asserts docstring presence**, not just import success — catches accidentally-empty `__init__.py` regressions early. The hook is now load-bearing because each package's docstring is the only thing carrying intent at this phase.

## Explicitly NOT done in Phase 0 (deferred to later phases per PHASES.md)

- No `state/workflow.db` creation — Phase 1.
- No Drawflow JS/CSS downloaded — Phase 11.
- No `config.json` (real config) created — operator does this at deploy time.
- No business logic, no migrations, no daemons, no CLI scripts beyond the symlink preflight.

## Audit gate

Master-auditor pass on `requirements.txt` + `pyproject.toml` + directory layout per PHASES.md Phase 0 audit-gate spec. Verdict to be recorded in `docs/phase_0_audit.md`.
