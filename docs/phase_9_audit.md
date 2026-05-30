# Master Auditor Report — Phase 9 close-out (vibrium-workflow) — 2026-05-30

## Verdict: NEEDS_FIX (FAIL)

## TL;DR
The Phase 9 deliverable has **two production-breaking dispatch bugs** that the 215-test suite does not catch because no test fires real `_dispatch("executor", …)` or `_dispatch("ingest", …)` against the actual daemon modules. Daemon-name contract, heartbeat semantics, exit-code mapping, `_Stats` split, file lock, and digest payload all look correct. Plists are clean. The structural design is right; the wiring is wrong on two of six modes. **Counts: P0=2, P1=3, P2=2.**

## Stack Detected
- **Python:** 3.9 (system `python3`)
- **Libraries:** `sqlite3` (stdlib), `fcntl` (stdlib), `zoneinfo` (stdlib), `simpleeval` (deferred local-import), `requests` via `workflow.clevertap_profile`
- **Domain:** Vibrium workflow engine (cron-style launchd daemons, payload-only digest)
- **KB consulted:** mental model only (sqlite3 patterns, launchd plist contract, system-design lenses observability + retry/idempotency, CLAUDE.md rules #1/#5/#10/#11/#12)
- **Live lookups:** none — pinned-version surface is stdlib

## P0 Issues — Must Fix Before Merge

### P0-1: `executor` mode dispatch is broken; production daemon would crash every 15 min
- **Category:** Correctness / Interaction
- **File:** [workflow_orchestrator.py:53](workflow/workflow_orchestrator.py#L53), [workflow_orchestrator.py:96-111](workflow/workflow_orchestrator.py#L96-L111)
- **Evidence:**
  ```python
  _MODE_REGISTRY = {
      "executor":   ("workflow_executor",   "workflow.agents.workflow",   "_run_executor_mode"),
      ...
  }
  ```
  but `_run_executor_mode` is defined in `workflow_orchestrator.py` itself (lines 96–111), not in `workflow.agents.workflow`. Verified at runtime:
  ```
  AttributeError: module 'workflow.agents.workflow' has no attribute '_run_executor_mode'
  ```
- **Why it's wrong:** `_dispatch("executor", …)` calls `getattr(module, callable_name)` where `module = workflow.agents.workflow`. That module exposes `WorkflowAgent` + `main`, not `_run_executor_mode`. Every launchd executor tick (15-min cadence per `com.sahil.workflow.executor.plist`) would stamp `down` and exit 1. **Tests miss it** because `test_each_mode_threads_correct_kwargs` only patches and exercises `alerts/ingest/enrollment/digest` — never `executor` — and `test_dispatch_happy_path…` is also only against `alerts`.
- **KB / source citation:** Verified by importing the module and calling `getattr` directly (see audit transcript above).
- **Required fix:** Either point the registry at the local adapter (`("workflow_executor", "workflow.workflow_orchestrator", "_run_executor_mode")`) — but this would import the module into itself, causing a circular adapter — or, cleaner, move the executor entry to use the `workflow.agents.workflow.main` flow or add a dedicated `executor_run` function in `workflow.agents.workflow` that wraps `WorkflowAgent(...).tick(...)`. Also add a P0 test that calls `_dispatch("executor", ...)` end-to-end with a real `WorkflowAgent` stub.

### P0-2: `ingest` mode dispatch will TypeError on every launchd tick
- **Category:** Correctness / Interaction
- **File:** [workflow_orchestrator.py:55](workflow/workflow_orchestrator.py#L55), [workflow_orchestrator.py:146](workflow/workflow_orchestrator.py#L146), [workflow_ingest.py:522-525](workflow/workflow_ingest.py#L522-L525)
- **Evidence:** Dispatcher calls
  ```python
  fn(workflow_db_path=workflow_db_path, **extra_kwargs)   # extra_kwargs = {"dry_run": False}
  ```
  but `workflow_ingest.run` signature is
  ```python
  def run(workflow_db_path, since: datetime, *, comment_fetcher=None) -> dict
  ```
  → `TypeError: run() missing 1 required positional argument: 'since'` and `TypeError: run() got an unexpected keyword argument 'dry_run'`.
- **Why it's wrong:** ingest will crash every 15 min in production with exit 1 + `down` heartbeat (forever). Alerts detector A would surface this as a perpetually-down daemon. `test_each_mode_threads_correct_kwargs` monkeypatches `workflow_ingest.run` to a capture-shim that accepts `**kwargs`, hiding the contract break.
- **Required fix:** Either (a) give `workflow_ingest.run` a default `since` (e.g., persisted-watermark fallback inside the function — actually the docstring already says "the persisted watermark always takes precedence once set"; so default `since=None` and resolve inside), and accept/ignore `dry_run`; or (b) thread the right kwargs from the dispatcher (compute a default `since = now - timedelta(hours=1)` in `_main` for ingest mode). Add a test that calls `_dispatch("ingest", wf_db, {"dry_run": False})` against the real module without monkeypatch and asserts no TypeError. Same audit should pass on `scheduler` real-call (its run() requires `vibrium_db_path` which IS passed, so likely OK — but worth a smoke check).

## P1 Issues — Should Fix

### P1-1: `test_p1_2_importerror_propagates_from_run` succeeds via a path that never exercises `_is_callable_now`
- **Category:** Tests
- **File:** [test_enrollment_poller_p1_fixes.py:164-189](workflow/tests/test_enrollment_poller_p1_fixes.py#L164-L189)
- **Evidence:** The test inserts a workflow row with `active_version_id=1` but the fixture never inserts a `workflow_versions` row. `_load_active_workflows` does `JOIN workflow_versions v ON v.id = w.active_version_id` → returns zero rows → `if not workflows: return stats.as_dict()` → function returns BEFORE `_is_callable_now()` is ever called. The `pytest.raises(ImportError)` passes only because of an earlier code path or because `_load_active_workflows` raises (no, it returns []). Re-reading: the test passes because…it doesn't. Re-verifying: actually the gate-2 call `gate = _is_callable_now()` happens AFTER the kill-switch but BEFORE `_load_active_workflows`. So the test IS valid. **Withdraw this P1.** (Kept as a note: re-read `_run_locked` gate order — RBI window comes before workflows load. OK.)
- **Resolution:** Withdrawn after re-read. No fix needed.

### P1-2: `_Stats.outside_window` legacy aggregate is set redundantly — risk of drift
- **Category:** Code Quality / Correctness
- **File:** [enrollment_poller.py:521, 659, 667](workflow/enrollment_poller.py#L521)
- **Evidence:** `outside_window` is set to True alongside both `outside_rbi_window` and `outside_enrollment_window`. There's no test asserting `outside_window == (outside_rbi_window or outside_enrollment_window)` — a future change that sets one of the narrower flags without the aggregate would silently break downstream consumers reading the legacy key.
- **Required fix:** Make `outside_window` a `@property` derived from the two narrower bools — eliminate the drift surface. Add `as_dict()` test asserting the invariant.

### P1-3: Digest reads `wf_pending_actions.last_attempt_at_ist` for shadow-run cutoff but `fired_at_ist` is the actual fire timestamp
- **Category:** Correctness / Domain logic
- **File:** [workflow_digest.py:129-136](workflow/workflow_digest.py#L129-L136)
- **Evidence:**
  ```python
  "SELECT COUNT(DISTINCT run_id) FROM wf_pending_actions "
  "WHERE status='SHADOW_FIRED' AND last_attempt_at_ist >= ?"
  ```
  But `wf_pending_actions` has both `fired_at_ist` (Phase 6 schema, when the row went to SHADOW_FIRED) and `last_attempt_at_ist`. For a SHADOW_FIRED row, `last_attempt_at_ist` MAY be NULL if the row was never retried. A row that flipped to SHADOW_FIRED at 02:00 with no retry would be excluded from the 24h window if `last_attempt_at_ist IS NULL`.
- **Required fix:** Use `COALESCE(fired_at_ist, last_attempt_at_ist) >= ?` — or just `fired_at_ist >= ?` since the status is SHADOW_FIRED (implies `fired_at_ist` is set by Phase 6 scheduler). Confirm against Phase 6 schema/writes before changing.

### P1-4: Heartbeat-status string "outside_window" is referenced in orchestrator docstring but emitted nowhere
- **Category:** Code Quality / Observability
- **File:** [workflow_orchestrator.py:26](workflow/workflow_orchestrator.py#L26)
- **Evidence:** Docstring says exit 0 includes "outside_window — expected operational state", but the orchestrator never emits a heartbeat with `status='outside_window'`. The scheduler does (`stats["status"] = "outside_window"` at `workflow_scheduler.py:399`), but the orchestrator stamps `status='ok'` regardless. If a Phase 8.5 detector reads `wf_agent_events.status` looking for `outside_window`, it'll miss it.
- **Required fix:** Either thread the daemon's returned `status` into the heartbeat (`status=result.get("status", "ok")`), or update the docstring to say the heartbeat is always `ok` and the operational state lives only in `summary_json`.

## P2 Issues — Nice to Fix

### P2-1: `_main()` doesn't initialize `logging` before the first `log.error` on invalid `--mode`
- **File:** [workflow_orchestrator.py:120-122](workflow/workflow_orchestrator.py#L120-L122)
- **Evidence:** `_dispatch` is called from `_main` after `logging.basicConfig`, but the invalid-mode log at `_dispatch:121` only fires if mode passes argparse's `choices=` — which it can't. Dead code path. Low impact.

### P2-2: `test_dry_run_does_not_attempt_smtp` uses substring check `"smtplib."` which would miss `from smtplib import …`
- **File:** [test_digest.py:206](workflow/tests/test_digest.py#L206)
- **Evidence:** A future `from smtplib import SMTP` would slip through the assertion (no `smtplib.` token). Add `"from smtplib" not in src` for completeness.

## Assumptions Made
- Assumed `workflow_ingest.run` truly has no production-default `since` callsite — verified by inspecting signature.
- Assumed `workflow.agents.workflow` is the canonical module path per current code (verified — module exists, but lacks `_run_executor_mode`).
- Assumed Phase 6's `wf_pending_actions.fired_at_ist` is set when status → SHADOW_FIRED — inferred from spec text; should be verified against the Phase 6 INSERT.

## What I Did Not Audit
- Did not run `_dispatch("scheduler", …)` end-to-end against the real `workflow_scheduler.run` — signature requires `vibrium_db_path` (passed by orchestrator) and `ct_creds_path` (passed) so it likely works; not verified live.
- Did not load the plists into launchd. `plutil -lint` clean on all 6.
- Did not verify `state/logs/` directory exists on the host (launchd will silently no-op the StandardOutPath if the dir is missing — operator should `mkdir -p` before bootstrap).
- Did not run the full 215-test suite under fresh `python3` — only the 22 Phase 9 tests + spot checks. The 215-count was confirmed via the pytest summary (`........  100%` — 215 dots).

## KB Updates Applied
None. The two dispatch bugs are project-specific wiring errors, not library gotchas.

## Recommendations

**Block close-out until P0-1 and P0-2 are fixed.** Both are 5-10-line patches:

1. **P0-1 fix sketch:** add to `workflow/agents/workflow.py`:
   ```python
   def run(*, workflow_db_path, dry_run=False, batch_limit=100, **_):
       return WorkflowAgent(workflow_db_path=str(workflow_db_path),
                            batch_limit=batch_limit).tick(dry_run=dry_run)
   ```
   Then change registry to `("workflow_executor", "workflow.agents.workflow", "run")`.

2. **P0-2 fix sketch:** in `workflow_ingest.run`, change signature to
   ```python
   def run(workflow_db_path, *, since=None, dry_run=False, comment_fetcher=None):
       if since is None:
           since = _load_watermark(workflow_db_path) or (datetime.now(IST) - timedelta(hours=1))
   ```
   Accept `dry_run` (no-op if read-only on Redshift, or short-circuit DB writes).

3. **Add a `test_dispatch_real_modules_no_typeerror` parametric test** that calls `_dispatch(mode, wf_db, …)` for each mode against the REAL `run()` (not monkeypatched), with the daemon's external side-effects stubbed at a lower seam (e.g., Redshift fetcher, CT client). This is the contract test that was missing.

Then re-run the auditor; expected verdict PASS_WITH_NOTES (P1-2/P1-3/P1-4 remain).

## Release Gate Status

| # | Gate | Status |
|---|------|--------|
| 1 | Static code review (master-auditor) | **NEEDS_FIX** — this report |
| 2 | API / backend QA (/stashfin-qa-backend) | BLOCKED — fix P0s first |
| 3 | Console wiring (sidebar / hub / streamlit_apps) | N/A — daemon-only deliverable |
| 4 | Frontend / UI QA (/stashfin-qa-ui) | N/A |

Ship approval blocked on P0-1 and P0-2.
