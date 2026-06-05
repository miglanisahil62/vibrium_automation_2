# TECH_LEAD_PLAN — WS3–WS7 X-Bucket Vibrium (multi-day same-day-retry + best-hour + scheduler priority)

**Repo:** `/Users/sahil.m/vibrium-workflow` (Mac dev → GitHub → AWS `/home/ubuntu/vibrium-workflow` via `git pull`).
**Deploy:** `git push` → server pull → `scripts/seed_workflow_direct.py` (reseed graph, lands as a NEW version, shadow_mode=1) → `python -m workflow.migrations.runner` (migrations).
**Critical surface:** contact-gate / RBI 08:00–18:59 IST window / per-customer call caps. Every claim below is grounded in the actual code (file:line cited).
**Status: PLAN ONLY. No code. Master-auditor must gate before any Write/Edit.**

---

## 0. GROUNDING — what the code actually is today (verified, not assumed)

| Component | File | Reality found |
|---|---|---|
| Per-segment call loop | `seed_workflows/generate_vb_collections_v2.py:187` `_call_loop` | **ONE counter only** named `attempts`, COUNTER `limit=total_calls`. There is **NO `day_index` counter and no N-day loop**. RETRY → COUNTER → WAIT_RETRY(`T+1 day at 08:00`) → FIRE. So today "N calls" == "N RETRY re-dials across N days", NOT "N days × ≤3 same-day attempts". |
| FIRE dedupe key | `fire_vb_call.py:66` | `attempt_count = int(run.scratchpad.get("attempts", 0))`. Dedupe = `UNIQUE(run_id,node_id,attempt_count)` (`migrations/001_init.py:118`). **The `attempts` scratchpad key is SHARED**: COUNTER increments it (RETRY budget) AND FIRE reads it (dedupe). They are the SAME variable today. |
| COUNTER | `counter.py` | No reset; `current+1 >= limit → at_limit`, else `under_limit`. Coerces non-int → `error` edge. |
| WAIT_UNTIL grammar | `wait_until.py:53-55` | Only `T+<N> day`, `T+<N> day at HH:MM`, `T+<N> hour`, and `{absolute: <scratchpad-key>}` (date-only). **No "rotate by scratchpad index" / "hour from scratchpad" grammar exists.** Late-deadline path correctly sets `run.status='ACTIVE'` (`:191`) — the "call ASAP if hour passed" primitive already works. |
| AWAIT_DISPOSITION | `await_disposition.py:47,83` | `_DEFAULT_TIMEOUT_HOURS = 24`; seed sets `config:{timeout_hours:24}` (`generate_...v2.py:249`). Timeout edge → `ASSIGN_TIMEOUT` (an ASSIGN_AGENT terminal). **Today timeout = give up + hand to agent. WS3 needs timeout = re-queue without burning budget — a semantic INVERSION, not a number tweak.** |
| BRANCH_ON_DISPOSITION | `branch_on_disposition.py` | action_classes: NOOP/RETRY/PTP_CALL/AGREE_EOD_CALL/CALLBACK_CALL/RTP_NEEDS_LLM/ESCALATE. Clears `last_disposition_action_class` on successful branch (`:123`). |
| Enrollment scratchpad seed | `enrollment_poller.py:979` | Literal `scratchpad_json='{}'` in the INSERT. **This is the WS5 seed site.** `_profile_to_scratchpad` (`:480`) is only used for the enrollment-time CONDITION; it emits keys `bot_calling`/`risk_segmentation`/`wa_status`/`dpd` (DIFFERENT names from the in-graph props). |
| In-graph CT props | `fetch_ct_props.py:275` | Writes property names **verbatim from node config** → `coll_bot_calling`, `coll_collection_risk_segmentation` into scratchpad on the graph walk. **WS7 ORDER-BY must key on `$.coll_collection_risk_segmentation`, NOT the poller's `risk_segmentation`.** |
| Scheduler | `workflow_scheduler.py` | `DEFAULT_HOURLY_CALL_CAP=750` (`:88`), `COOLDOWN_HOURS=3` (`:94`), `MAX_CALLS_PER_DAY=3` (`:99`). ORDER BY (`:519-523`) = spillover-day → attempt_count==0-first → scheduled_at. Reads `wf_pending_actions JOIN workflow_runs wr JOIN workflows w` (`:515-516`) — **`wr.scratchpad_json` is already in scope for a risk JSON-extract.** |
| Ingest match | `workflow_ingest.py:197` `_find_matching_pending_action` | Most-recent FIRED within **24h window**, `ORDER BY fired_at_ist DESC LIMIT 1`. `_wake_run` (`:273`) guards on `current_node_type=='AWAIT_DISPOSITION'` + `entered_node_at_ist <= comment_create_ist`. **P2: with multiple FIRED rows/run/day, the 24h-most-recent match can bind the disposition to the WRONG fire's node_id/attempt_count.** |
| decision_log | `migrations/001_init.py:123` + `_insert_decision_log` (`workflow_ingest.py:235`) | `wf_decision_log` has **NO UNIQUE constraint**; only a non-unique index `idx_wfdl_run(run_id,node_id,attempt_count)` (`:230`). Plain INSERT → **P2: replay duplicates audit rows.** |
| Pre-call gate cooldown | KB `regulatory-collections.md:20` | `pre_call_gate.py` has its OWN `COOLDOWN_HOURS=3` (Redshift no-connect gate), independent of the scheduler constant. **WS7 1h cooldown is a TWO-PLACE change — see Risk R7.** |

---

## 1. THE RISKIEST COUPLING (read this first)

**`attempts` is overloaded into three roles that WS3/WS6 split apart.** Today the single scratchpad key `attempts`:
1. is the RETRY budget counter (COUNTER `limit=total_calls`),
2. is the FIRE dedupe `attempt_count`,
3. implicitly orders scheduler Tier-1 vs Tier-2 (`attempt_count==0` first, `workflow_scheduler.py:521`).

WS3 introduces **`day_index`** (limit N, ticks per DAY) + **`attempts_today`** (limit 3, per same-day no-connect, resets on rollover). WS5/WS6 demand a **globally-monotonic `attempts`** that bumps on EVERY fire (same-day retries included) or dedupe collides them into silence.

**If we reuse the name `attempts` for the day counter (as the COUNTER node does today) AND for the monotonic dedupe counter, every same-day retry will either (a) collide on `UNIQUE(run_id,node_id,attempt_count)` and silently no-fire, or (b) wrongly tick the N-day budget.** This is the single highest-impact bug in the whole feature set.

**Decision (LOCKED for the build):** THREE distinct scratchpad keys, never aliased:
- `day_index` — INTEGER, COUNTER name, limit = N (segment `total_calls` reinterpreted as DAYS). Ticks once per day after the same-day loop closes.
- `attempts_today` — INTEGER, COUNTER name, limit = 3. Ticks per same-day no-connect. **Reset to 0 on day rollover** (see WS4 reset mechanism — COUNTER has no reset, so the reset must be a `scratchpad_patch` on the day's WAIT_UNTIL or a dedicated SET node).
- `fire_seq` (rename — do NOT keep calling it `attempts`) — INTEGER, **globally monotonic**, bumped immediately before EVERY FIRE. This is the value FIRE stamps into `attempt_count`. Never decremented, never reset.

**`fire_vb_call.py:66` must change from `run.scratchpad.get("attempts",0)` to `run.scratchpad.get("fire_seq",0)`.** And the bump must happen on the edge INTO every FIRE (both first-of-day and same-day retry), not inside FIRE (FIRE runs inside the executor txn and its `scratchpad_patch` is the place — but a retry that loops FIRE→AWAIT→retry-gate→FIRE must bump between iterations). **Mechanism options to evaluate in WS6 (auditor to confirm):** (a) a dedicated `INCREMENT fire_seq` node on every edge into FIRE, or (b) FIRE itself returns `scratchpad_patch={"fire_seq": current+1}` and stamps `attempt_count=current+1` in the same call. Option (b) is fewer nodes and keeps the bump atomic with the insert; **prefer (b)** but verify the dedupe semantics: a crashed-then-retried tick must re-stamp the SAME `fire_seq` (idempotent), so the increment must be derived from the PRE-fire scratchpad value already persisted, not a fresh ++ each tick. → **This needs an explicit idempotency note in the design before coding (see Failure Mode FM-2).**

---

## 2. FAILURE MODE MATRIX

| # | Failure | Prob | Impact | Mitigation (must be in code) |
|---|---|---|---|---|
| FM-1 | **Counter double-tick**: `day_index` ticks both on same-day retry and on day rollover → customer's N-day budget burns in one day; OR `attempts_today` ticks on the 60-min requeue (a call that never went out). | likely | wrong result / over- or under-calling, RBI exposure if over | Structural separation: same-day-retry edge loops to FIRE WITHOUT passing the `day_index` COUNTER (mirrors today's "initial FIRE doesn't touch COUNTER" invariant, `generate_...v2.py:38-39`). `day_index` COUNTER sits ONLY on the day-rollover edge. The 60-min-timeout requeue edge must touch NEITHER counter (FM-5). |
| FM-2 | **attempt_count collision / non-idempotent bump**: same-day retries reuse a stale `fire_seq` → `INSERT OR IGNORE` silently no-ops → call never queued, customer goes dark. OR a retried tick double-bumps `fire_seq` → two queue rows for one intended call. | likely | **silent data loss (customer never called)** | `fire_seq` monotonic, bumped on EVERY fire path. Idempotency: derive the stamped `attempt_count` deterministically from persisted pre-fire scratchpad so a crash-replay re-stamps the SAME value and `INSERT OR IGNORE` correctly de-dupes the replay (not a real new attempt). Add an e2e gate asserting: K same-day fires for one run/day produce K DISTINCT `attempt_count` rows, all FIRED/SHADOW_FIRED, zero dedupe-silent-drops. |
| FM-3 | **Reserve-bandwidth starvation**: reserve fraction for callbacks/unfulfilled-PTP too large → first-attempt breadth starves; OR general bucket consumes the reserve → high-intent follow-ups never serviced. | possible | wrong prioritization, missed promises | `effective_limit` split = `reserve = ceil(limit * RESERVE_FRAC)` + `general = limit - reserve`, with **fallback bleed**: if reserve demand < reserve slots, the unused reserve slots go to general (and vice versa) — never leave headroom idle while rows are PENDING (the existing `:461-482` "fill to cap" discipline). RESERVE_FRAC env-tunable, default conservative (0.20). Reserved class identified by a `priority_class` marker on the row (migration + set by WAIT_PTP/WAIT_CB/WAIT_EOD FIRE re-entries). Reserved-bandwidth-health alert (WS12) confirms it's actually serviced. |
| FM-4 | **Day-rollover `attempts_today` reset missed**: COUNTER has no reset (`counter.py:12-14`); if the rollover WAIT_UNTIL doesn't patch `attempts_today=0`, day-2's same-day budget starts already-exhausted → only 1 call ever per day after day 1. | likely | under-calling (silent) | The day's entry WAIT_UNTIL (the rotation park) returns `scratchpad_patch={"attempts_today": 0}` on the FUTURE-park path. Verify the patch is applied on park (today WAIT_UNTIL returns `scratchpad_patch={}` on both paths — `wait_until.py:182,196`; WS4 must add it). e2e gate: after a day rollover, `attempts_today==0`. |
| FM-5 | **60-min-timeout requeue semantics wrong**: timeout treated as no-connect → burns a same-day attempt or ticks `day_index` for a call that NEVER WENT OUT (still PENDING under the cap). | likely | **wrong result + budget loss + customer under-called** | Re-interpret AWAIT timeout. The timeout edge must route to a **re-queue path** that loops back toward FIRE/AWAIT WITHOUT incrementing `day_index` OR `attempts_today` (it MAY bump `fire_seq` ONLY if it re-INSERTs a new pending row; if the original PENDING/FIRING row still exists, do NOT double-queue — see FM-6). The semantic: "no disposition in 60 min ⇒ the scheduler hasn't fired it yet (cap/window) ⇒ it's still in the pipeline, not a no-connect." Only a real RETURNED disposition advances logic. **Auditor must confirm: how does AWAIT distinguish "fired-but-no-disposition-yet" (true no-connect, should eventually no-connect-retry) from "never-fired-still-PENDING" (pure requeue)?** The discriminator is whether a FIRED row exists for this run's current `fire_seq`/node since `entered_node_at_ist`. This is the trickiest single semantic in WS3 — see §3 WS3 for the resolution. |
| FM-6 | **Double-queue / orphan PENDING**: requeue path re-INSERTs while the prior row is still PENDING → two live rows; or a FIRED-but-no-disposition row gets a second FIRE queued → double-dial (RBI cap breach risk). | possible | RBI cap breach / vendor over-drive | Before any requeue/re-fire, the path must reconcile against `wf_pending_actions` state for (run_id, current node, latest `fire_seq`). The scheduler's per-customer 3/day cap (`cca.customer_daily_cap`) + 1h cooldown is the backstop, but the FSM must not RELY on the gate to fix a structural double-queue. e2e gate: at most one live (PENDING/FIRING_IN_PROGRESS) row per (run_id,node_id) at any tick. |
| FM-7 | **Dynamic-cap thrash**: cap oscillates 750↔1000 every tick on noisy disposition-rate signal → unstable vendor load, hard to reason about. | possible | operational instability | Make the cap change **hysteretic + slow**: ramp up only after a sustained (e.g. ≥3 consecutive ticks / rolling-30-min) healthy disposition-return ratio, step by a bounded delta (e.g. +50/step), back off faster than ramp-up. Cap is `WF_HOURLY_CALL_CAP` env (already wired as `hourly_call_cap` kwarg, `:390`) as the CEILING; the adaptive value never exceeds it and never drops below a FLOOR (e.g. 750). Persist the current adaptive value (so it survives tick restarts) or recompute deterministically from observed signal each tick. Default behavior with signal absent = stay at floor 750 (never auto-ramp blind — design plan §WS7 "not blind"). |
| FM-8 | **Risk ORDER-BY keys on the wrong scratchpad field** → priority ordering silently degrades to time-only (every row reads NULL risk → all tie → falls back to old order). | likely | wrong prioritization (silent) | Key on `json_extract(wr.scratchpad_json,'$.coll_collection_risk_segmentation')` (the FETCH_CT_PROPS output, verified `fetch_ct_props.py` writes config names verbatim). NOT `risk_segmentation` (that's the poller-only key). Handle NULL/'NaN' risk: `COALESCE` to a mid/low default so missing-risk rows don't jump the queue. Add a one-time verification query on real shadow rows confirming the key is populated (Risk R3). |
| FM-9 | **Spell over-call across days**: paid/cured customer keeps getting called because the paid-check only runs at the scheduler, not before the day's FSM advance. | possible | RBI / customer-harm (calling a paid customer) | Design plan locks "paid/still-overdue gate before EVERY attempt." The scheduler's `_gate_check` already calls `pre_call_gate.check` (paid_today + collection_view) per row (`:236-239`) — this IS the before-every-fire gate. Confirm the day-rollover and same-day-retry paths ALL terminate at a FIRE that the scheduler gates; no path may fire without traversing the scheduler gate. No NEW paid-check node needed IF every fire goes through `wf_pending_actions`. Verify no FSM edge places a call outside `wf_pending_actions`. |
| FM-10 | **Ingest mis-attribution** (P2): multiple FIRED rows/run/day → 24h-most-recent binds disposition to wrong fire's node_id/attempt_count → wrong `wf_decision_log` audit + potentially wrong wake. | likely once multi-attempt ships | wrong audit / wrong branch | Tie matching to the parked run's identity: in `_find_matching_pending_action`, narrow the candidate set to the FIRED row whose `(run_id,node_id)` matches the run currently WAITING at AWAIT_DISPOSITION AND whose `fired_at_ist >= run.entered_node_at_ist`, picking the most-recent within that node-entry window (not a blind 24h). The wake already requires `entered_node_at_ist <= comment_create_ist` (`workflow_ingest.py:299`); extend the MATCH side symmetrically. See §3 P2-A. |
| FM-11 | **decision_log replay dupes** (P2): plain INSERT, no UNIQUE → re-ingest doubles rows → inflated disposition counts, wrong digest. | likely on replay | wrong reporting | Add `UNIQUE(run_id,node_id,attempt_count,comment_id)` index + switch `_insert_decision_log` to `INSERT OR IGNORE`. Migration-backed (see §4). Confirm existing rows don't violate the new unique before creating it. See §3 P2-B. |
| FM-12 | **Idempotency on cron re-run / stall-recovery**: a re-tick re-fires a same-day attempt. | possible | RBI cap breach | Backstopped by `cca.customer_daily_cap` (3/day, cross-system) + 1h cooldown + `INSERT OR IGNORE` dedupe on `fire_seq`. The FSM advance is persisted per-tick; recovery resumes from current node. No path may advance a counter twice for one logical event (FM-1/FM-2). |
| FM-13 | **Timezone**: any new `datetime.now()` naive → Mac/AWS divergence in day-rollover + best-hour math. | possible | wrong day boundary, off-window calls | All new datetime work uses `ZoneInfo("Asia/Kolkata")` IST-naive `%Y-%m-%d %H:%M:%S` (existing canon, `fire_vb_call.py:45`, `wait_until.py:43-44`). Day rollover boundary = IST midnight. The WAIT_UNTIL `T+N day` math is already IST (`wait_until.py:58`). No UTC anywhere. |
| FM-14 | **RBI window leak**: a same-day retry or "ASAP if missed" path queues a call near 18:59 that the scheduler fires past 19:00. | possible | **P0 RBI complaint surface** | The scheduler's `is_callable_now` (`:431`) is the hard gate — it short-circuits the whole tick outside 08:00–19:00. A near-window row simply stays PENDING and expires for the day (rolls forward, never burns budget — design plan "missed call-day roll-forward"). Confirm NO FSM path bypasses the scheduler window gate. The "call ASAP if hour passed" (WS4) must still produce a `wf_pending_actions` row, never a direct dial. |
| FM-15 | **Graph reseed clobbers in-flight runs**: new graph version seeded while v7 runs are mid-journey → counter-key mismatch (`attempts` vs `day_index`/`fire_seq`) → in-flight runs read missing keys. | likely at cutover | crash / wrong branch on live runs | `seed_workflow_direct.py` appends a NEW version, repoints `active_version_id` (`:120-128`) — in-flight runs keep their `version_id`. BUT counter-key rename means old runs on the old graph still use `attempts`; new runs use `day_index`/`fire_seq`. Cutover plan: drain or terminate v7 in-flight runs before activating the new version, OR seed new version with shadow_mode=1 and only enroll NEW runs onto it. **Migration of in-flight runs is the cutover risk — see §5 Cutover.** |

---

## 3. FILE-LEVEL CHANGES (the exact edits, ordered for independent testability)

### WS3 — N-day + same-day-retry graph (`seed_workflows/generate_vb_collections_v2.py`, `await_disposition.py`)

**Graph generator `_call_loop` rewrite (the core change):**
- Reinterpret `total_calls` as **N = number of DAYS** (`day_index` COUNTER limit), not RETRY re-dials.
- New node topology per segment (extends the hex index map at `:196-202`; we have room — current loop uses 0x01–0x11):
  - Day-entry WAIT_UNTIL (rotation park, WS4) → sets `attempts_today=0` on park.
  - FIRE (stamps `attempt_count = fire_seq`, bumps `fire_seq`) → AWAIT_DISPOSITION (timeout 60 min).
  - AWAIT timeout edge → **REQUEUE path** (NOT ASSIGN_TIMEOUT): re-park / re-queue without ticking `day_index` or `attempts_today`. See timeout-semantics resolution below.
  - AWAIT disposition edge → BRANCH.
  - BRANCH `retry` (no-connect) edge → **same-day-retry gate**: a COUNTER on `attempts_today` (limit 3). `under_limit` → WAIT ≥1h (`T+0 day at` next-hour, or a `T+1 hour` relative) → FIRE (loops WITHOUT touching `day_index`). `at_limit` → day-rollover path.
  - Day-rollover path → COUNTER on `day_index` (limit N). `under_limit` → next-day rotation WAIT_UNTIL (resets `attempts_today`). `at_limit` → ASSIGN_LIMIT (`max_attempts_reached`, the clean exhaustion terminal WS10 keys on — `generate_...v2.py:226-231`).
  - PTP/EOD/Callback waits → still loop to FIRE; these are reserve-class same-day follow-ups (WS7), must NOT consume `day_index`. Confirm they bump `fire_seq` (FM-2).
- Keep `ASSIGN_LOOP_ERR` for COUNTER/WAIT `error` edges (distinct from `max_attempts_reached` — preserves WS10 P1-1, `:226-231`).
- `total_calls == 1` (midv4) edge case: N=1 means one day, ≤3 same-day attempts, then ASSIGN_LIMIT. Verify the `use_counter = total_calls >= 2` short-circuit (`:234`) is re-derived for `day_index` (N=1 still needs the same-day `attempts_today` loop; only `day_index` COUNTER is skipped).

**AWAIT_DISPOSITION 60-min + requeue semantics (`await_disposition.py` + seed config):**
- Seed config `timeout_hours: 24 → 1.0` (the handler already accepts float, `:85`).
- **Timeout-semantics resolution (FM-5, the hard one):** The cleanest design that needs the LEAST handler surgery: keep the AWAIT timeout edge as today, but route it to a **REQUEUE node** that:
  1. checks whether a FIRED row exists for this run since `entered_node_at_ist` (i.e. did the scheduler actually fire?);
  2. if NO FIRED row (still PENDING/SUPPRESSED-this-tick/cap-deferred) → re-enter AWAIT (re-park another 60 min) WITHOUT touching any counter and WITHOUT re-queuing a duplicate (the original PENDING row is still live → FM-6);
  3. if a FIRED row exists but no disposition came → that IS a no-connect after the vendor's disposition SLA → route to the same-day-retry gate (`attempts_today`).
  - **This discriminator (FIRED-since-entry?) is new logic.** Auditor must decide whether it lives in a new handler node or as a branch inside an extended AWAIT. **Recommendation:** a small new `CHECK_FIRED` node (or extend AWAIT to read `wf_pending_actions` via `txn`) rather than overloading WAIT_UNTIL. Flag for auditor: AWAIT currently has NO DB read; giving it a `txn` query is a surface change to review.
- `ASSIGN_TIMEOUT` node (`reason: disposition_timeout_24h`, `:382`) — rename reason or repurpose; with 60-min requeue, a true "give up" only happens at `day_index` exhaustion (ASSIGN_LIMIT), so ASSIGN_TIMEOUT may become dead — verify and remove or repoint.

### WS4 — WAIT_UNTIL best-hour rotation (`wait_until.py`)
- Add a NEW grammar form, e.g. config `{"rotate": {"hours_key": "best_hours", "index_key": "day_index", "offset_days_key": null, "reset": {"attempts_today": 0}}}`. Computes target hour = `best_hours[day_index % len(best_hours)]` for TODAY (or T+offset), at that hour:00.
- Reuse the existing late-deadline path (`:186-199`): if the computed hour is already past → `status='ACTIVE'`, advance immediately = "call ASAP in remaining window today". **Free win — already implemented.**
- **Reset mechanism (FM-4):** return `scratchpad_patch={"attempts_today": 0}` on BOTH the park and the late-advance paths (today both return `{}` — must change). This is how `attempts_today` resets on day rollover WITHOUT a COUNTER reset (COUNTER can't reset, `counter.py:12-14`).
- Validation: `best_hours` missing/empty/non-list → route to `error` edge (then ASSIGN_LOOP_ERR), do NOT silently default mid-journey (the seed already padded `[10,13,16]` at enrollment — WS5 — so empty here is a real fault).
- Keep grammar tight (the file's stated philosophy `:51-52` — "add new regexes/forms, don't loosen the parser"). The rotate form is a separate config key, not a loosened string parser.
- **Edge: `day_index % len(best_hours)` when N > len** (lowv3 N=4, 3 hours) → day 4 wraps to hour #1. Matches design plan §"Best time to call" `:36`.

### WS5 — Seed scratchpad at enrollment (`enrollment_poller.py:979`)
- Replace literal `scratchpad_json='{}'` with a per-customer JSON seeded with:
  - `best_hours`: `best_call_hours(cid)` via `external.vibrium_automation_scripts.call_timing` (lazy import behind the `external` symlink, mirror scheduler's lazy-import pattern `:359-379`); fallback `[10,13,16]` on ANY exception (module missing in CI / no history / parquet stale). Never let a best-hours lookup failure block enrollment.
  - `day_index`: 0
  - `attempts_today`: 0
  - `fire_seq`: 0
- The seed must be a parameterized JSON string per row (the INSERT is in a loop, `:967-985`); build the dict, `json.dumps`, bind as the `scratchpad_json` param. Currently the value is a SQL literal `'{}'` — change to a `?` bind.
- **Note:** this scratchpad seed is the ENROLLMENT scratchpad. FETCH_CT_PROPS later MERGES `coll_bot_calling`/`coll_collection_risk_segmentation` in (shallow merge by executor). Both coexist — best_hours seed must not be clobbered by FETCH_CT_PROPS (it writes different keys; confirm no key collision).
- best_hours parquet **freshness check** (design plan top-risk `:169`): add a staleness alert (WS12), not in this WS, but the fallback `[10,13,16]` here is the safety net.

### WS6 — FIRE attempt_count global monotonicity (`fire_vb_call.py:66`)
- Change `attempt_count = int(run.scratchpad.get("attempts",0))` → read `fire_seq`.
- Implement the bump (prefer option (b) §1): FIRE stamps `attempt_count = current_fire_seq` and returns `scratchpad_patch={"fire_seq": current_fire_seq + 1, "last_fire_at": now}`. The stamped value derives from the PERSISTED pre-fire `fire_seq` so a crash-replay re-stamps the SAME `attempt_count` → `INSERT OR IGNORE` correctly de-dupes the replay (not a phantom new attempt). **Auditor: confirm the executor persists `scratchpad_patch` atomically with the INSERT in the same `txn` (FIRE runs inside executor txn, `fire_vb_call.py:58-64`) so the bump and the insert commit together — else a crash between them desyncs.**
- The dry-run path (`:70-82`) must also report the `fire_seq`-derived attempt and the intended bump (without writing).
- Every edge that re-enters FIRE (same-day retry, PTP/EOD/CB waits, day-rollover) inherits the bump because the bump is in FIRE itself. No per-edge increment node needed (option (b) advantage).

### WS7 — Scheduler priority + caps (`workflow_scheduler.py`)
- **Risk-band ORDER BY** (`:519-523`): add a leading-ish key `CASE band: 0-4→0, 5-7→1, 8-10→2` from `json_extract(wr.scratchpad_json,'$.coll_collection_risk_segmentation')` with `COALESCE` to a low-priority default (so missing-risk doesn't jump the queue, FM-8). Place AFTER spillover-day (a spilled high-risk still beats a fresh high-risk? — design plan `:140` says spillover → risk → breadth → time; keep that order). `wr` is already JOINed (`:515`).
- **Reserve bandwidth** (`:461-482`): split `effective_limit` into `reserve` + `general` with bleed-back (FM-3). Need a `priority_class` column on `wf_pending_actions` (migration, §4) set when a reserve-class fire is queued (PTP/Agree/Callback same-day follow-ups). Two-pass claim: fill reserve from reserve-class PENDING rows, fill general from the rest, then bleed unused capacity either direction. Keep the "never leave headroom idle while rows PENDING" invariant.
- **1h cooldown for X-bucket** (`:94,571`): pass `cooldown_hours=1` to `_gate_check` for workflow rows (keyed off `cohort_name LIKE 'workflow:%'` — `fire_vb_call.py:68`). **R7 CRITICAL:** the scheduler's `_gate_check` cooldown is one place; but `pre_call_gate.check()` (`:238`) may enforce its OWN 3h cooldown (KB `regulatory-collections.md:20` — `pre_call_gate.py:28,52-65`). If `pre_call_gate.check` independently rejects <3h, the scheduler's 1h is moot and same-day retries are silently SUPPRESSED. **Must verify what `pre_call_gate.check` enforces and whether it takes a cooldown override** before claiming 1h works. The 3/day cap (`cca.customer_daily_cap`) is correct as-is (`:570`).
- **Dynamic hourly cap** (`:88,390,461`): `hourly_call_cap` becomes adaptive between FLOOR=750 and CEILING=`WF_HOURLY_CALL_CAP` (default 1000). Gate ramp on observed disposition-return ratio over a rolling window (read `wf_decision_log` / `wf_agent_events` recent counts vs fires). Hysteretic (FM-7): ramp slow (+50/step after sustained healthy ratio), back off fast. Surface `fired` vs `dispositions_returned` in the heartbeat `summary_json` (`:730`) and the 19:30 digest. With no signal → stay at 750 (never blind-ramp).

### P2-A — Ingest mis-attribution (`workflow_ingest.py:197,499`)
- Narrow `_find_matching_pending_action`: constrain candidates to the FIRED row(s) for the run currently WAITING at AWAIT_DISPOSITION, with `fired_at_ist >= that run's entered_node_at_ist`, most-recent within THAT node-entry window — not a blind customer-wide 24h. Tie the match to `(run_id, node_id, attempt_count)` of the parked node. The caller (`:499`) already feeds into `_wake_run` which re-checks `entered_node_at_ist <= comment_create_ist` (`:299`); make the MATCH symmetric so the decision-log `attempt_count` (`:521`) binds to the correct fire.
- **Caution:** a customer with two concurrent runs (shouldn't happen — `enrollment_key` dedup, `:218`) would break this; confirm one-active-run-per-customer invariant holds for X-bucket.

### P2-B — decision_log dedup (`workflow_ingest.py:235` + migration)
- Add `UNIQUE(run_id,node_id,attempt_count,comment_id)` index (migration, §4); switch `_insert_decision_log` (`:250`) to `INSERT OR IGNORE`. Pre-check existing rows don't violate uniqueness before creating the unique index (build a non-unique first, dedupe, then unique — or `CREATE UNIQUE INDEX` will fail if dupes exist).

---

## 4. MIGRATION NEEDS (`workflow/migrations/005_*.py`, `006_*.py`)

Migration runner contract (verified `migrations/runner.py:106-132`, `003_ct_profile_cache.py:20-56`): each migration module exposes `SCHEMA_KEY`, `SCHEMA_VERSION`, and `up(workflow_db_path=..., vibrium_db_path=...)`; numeric-prefix ordered; records into `schema_version`; idempotent (`IF NOT EXISTS`).

- **005_wfpa_priority_class** — `ALTER TABLE wf_pending_actions ADD COLUMN priority_class TEXT` (nullable; NULL = general). SQLite ADD COLUMN is safe/online. Add `idx_wfpa_priority` if the two-pass claim needs it. (WS7 reserve bandwidth.)
- **006_wfdl_unique** — dedupe existing `wf_decision_log` rows, then `CREATE UNIQUE INDEX idx_wfdl_unique ON wf_decision_log(run_id,node_id,attempt_count,comment_id)`. **Must handle NULL comment_id** (adhoc-source dispositions, `:259`) — SQLite treats NULLs as distinct in UNIQUE, so adhoc rows won't collide; verify that's the intended semantic. (P2-B.)
- No `attempt_count`/dedupe-key schema change needed — `UNIQUE(run_id,node_id,attempt_count)` already exists (`001_init.py:118`); WS6 only changes WHICH scratchpad var feeds it.
- **No vibrium.db migration** for WS3-7 (cap/cooldown live in workflow.db + the shared `customer_call_audit` is untouched).

---

## 5. CHECKPOINT / VERIFICATION STRATEGY (shadow-first, e2e before live)

**Build order (each step independently testable; depends-on noted):**
1. **WS6 + WS5 scratchpad keys** (no graph dependency): unit-test `fire_seq` monotonicity + idempotent re-stamp; enrollment seed unit-test (best_hours fallback, day_index/attempts_today/fire_seq present). `python3 -m pytest`.
2. **WS4 WAIT_UNTIL rotate grammar** (depends on WS5 key names): unit-test rotation index, wrap (N>len), late-advance, `attempts_today` reset patch, error edges. Pure-function, fully unit-testable offline.
3. **WS3 graph generator** (depends on WS4/WS6 node contracts): run `python seed_workflows/generate_vb_collections_v2.py` → the built-in `_lint_segment_matches` + broken-edge + dupe-node-id checks (`generate_...v2.py:526,586,600`) must pass. Add generator assertions: `day_index` COUNTER on rollover only, `attempts_today` COUNTER on same-day only, no FIRE without a `fire_seq` bump path, requeue edge touches no counter.
4. **WS3 AWAIT 60-min + requeue** (depends on WS3 topology): unit-test the FIRED-since-entry discriminator (FM-5) with mocked `wf_pending_actions` states.
5. **WS7 scheduler** (depends on migration 005 + WS3 priority_class writes): unit-test ORDER BY (risk-band ordering on synthetic scratchpad), reserve split + bleed-back, 1h cooldown override, dynamic-cap hysteresis. Inject `gate_check_fn` (existing seam `:396`) — NO Redshift.
6. **P2-A / P2-B** (depends on migration 006): unit-test multi-FIRED attribution + replay-idempotent decision_log.
7. **E2E shadow** (`scripts/e2e_shadow_test.py`, extend the v1 harness to a multi-day-aware variant): seed the new graph with **`shadow_mode=1`** (`seed_workflow_direct.py` sets shadow=1 by default, `:98`); enroll synthetic customers; **simulate multiple ticks across simulated day boundaries** (the harness drives `WorkflowAgent.tick()`, `e2e_shadow_test.py:236-244`). Gates to ADD:
   - 0 FIRED, only SHADOW_FIRED (existing Gate 1, `:251`).
   - K same-day no-connects → K distinct `attempt_count` SHADOW_FIRED rows, zero silent dedupe-drops (FM-2).
   - `day_index` ticks exactly once per simulated day; `attempts_today` resets to 0 each new day (FM-1, FM-4).
   - 60-min timeout with no FIRED row → requeue, no counter tick (FM-5).
   - At most one live PENDING/FIRING row per (run_id,node_id) per tick (FM-6).
   - Priority ORDER BY puts a synthetic high-risk row ahead of a low-risk first-attempt (WS7/FM-8).
   - decision_log has no dupes after a replayed ingest (FM-11).

**Pre-live AWS verification (GET-only, no mass-fire):**
- Apply migrations 005/006 on the server `workflow.db`.
- Reseed the new graph version (shadow_mode=1).
- One real shadow day: confirm SHADOW_FIRED rows, verify `$.coll_collection_risk_segmentation` is actually POPULATED on real runs (FM-8/R3) — a one-off `SELECT json_extract(scratchpad_json,'$.coll_collection_risk_segmentation') ...` on shadow runs.
- `workflow_scheduler --dry-run` against the populated shadow rows → confirm ORDER BY + reserve buckets + cooldown decisions in logs, zero live `trigger()`.
- Flip `shadow_mode=0` only after a CLEAN shadow day + manual Sahil "go" (manual Vibrium trigger rule, memory `feedback_manual_vibrium_confirm`).

**Cutover of in-flight v7 runs (FM-15):** Before activating the new version live, decide per the design: either (a) let v7 runs drain on the OLD graph (they keep `version_id=7`, use `attempts`; new enrollments go to the new version) — safest, no migration; or (b) migrate in-flight scratchpads (`attempts` → `day_index`+`fire_seq`) via a one-off script (`scripts/migrate_runs_to_version.py` exists as a starting pattern). **Recommend (a)** — zero in-flight scratchpad surgery. Confirm the executor loads each run's OWN `version_id` graph (not always-active) so v7 runs don't crash on missing new keys.

---

## 6. SUCCESS CRITERIA (observable from outside)

- `python3 -m pytest` green (all new unit tests + existing suite).
- Generator runs clean: `OK — written ... 0 broken edges`, lint passes, new counter-topology assertions pass.
- `migrations/runner.py` applies 005/006 idempotently; `schema_version` records both.
- E2E shadow: all gates PASS, **0 FIRED rows**, day/same-day counter + best-hour + priority + dedup gates green.
- AWS shadow day: SHADOW_FIRED > 0, risk key populated, `--dry-run` scheduler shows correct priority/reserve/cooldown, **0 live CT fires**.
- RBI invariant proof: 0 rows fired outside 08:00–18:59 IST; per-customer ≤3/day held; ≥1h gap held; 0 calls to paid/cured customers.
- No P0 from master-auditor on the changed critical-surface files (`fire_vb_call.py`, `workflow_scheduler.py`, `await_disposition.py`, `wait_until.py`, generator).
- Heartbeat/digest surfaces fired-vs-disposition-returned for cap tuning.

---

## 7. TOP RISKS TO CARRY INTO BUILD + AUDIT

- **R1 (HIGHEST):** the `attempts` overload — three roles, must become `day_index` / `attempts_today` / `fire_seq`. Get this wrong → silent no-fires or budget burn (FM-1/FM-2). The riskiest coupling; §1.
- **R2:** 60-min-timeout requeue discriminator (FIRED-since-entry vs never-fired) — new DB-read logic in/near AWAIT; the single trickiest semantic (FM-5). Auditor to bless the placement (new node vs extend AWAIT with `txn`).
- **R3:** `$.coll_collection_risk_segmentation` is the FETCH_CT_PROPS key, NOT the poller's `risk_segmentation`. Verify on real shadow rows before the ORDER BY relies on it (FM-8).
- **R4:** reserve-bandwidth bleed-back to avoid first-attempt starvation AND idle headroom (FM-3).
- **R5:** dynamic-cap hysteresis to avoid thrash; floor 750 / ceiling 1000 / never blind-ramp (FM-7).
- **R6:** day-rollover `attempts_today` reset must be a WAIT_UNTIL `scratchpad_patch` (COUNTER can't reset) (FM-4).
- **R7:** 1h cooldown is TWO places — scheduler `_gate_check` AND possibly `pre_call_gate.check`'s own 3h. Verify `pre_call_gate` honors an override or the 1h is silently defeated by SUPPRESSED.
- **R8:** ingest mis-attribution + decision_log dedup go live the moment multi-attempt ships (P2-A/P2-B) — land them in the SAME release, not after.
- **R9:** cutover of in-flight v7 runs — recommend drain-on-old-version, no scratchpad migration (FM-15).
- **R10:** 750/hr is cross-system (`customer_call_audit`) but priority/reserve is workflow-only; adhoc Vibrium firing concurrently can exceed the combined vendor envelope (documented `:85-87`).

---

**TECH_LEAD_PLAN written. Master-auditor must review before coding begins.**

---

# P0 RESOLUTIONS — folded 2026-06-05 (master-auditor APPROVED_WITH_CONDITIONS → all 4 conditions closed)

**P0-1 (cooldown is ONE place).** Cooldown for workflow rows is enforced SOLELY by
`workflow_scheduler.COOLDOWN_HOURS`. `pre_call_gate.check()` (the injected
`gate_check_fn`) enforces NO cooldown — leave it untouched. Do NOT modify
`pre_call_gate.check_cap_and_cooldown` (that is the ADHOC system's path — cross-system
regression). The 1h change = a workflow-row-scoped value (kwarg keyed off
`cohort_name LIKE 'workflow:%'`, default stays 3h for any non-workflow caller). Update
the "3h cooldown" docstrings in `workflow_scheduler.py` + `workflow_ingest.py` in the
same edit, AND re-check the disposition-attribution window (see P0-3/P0-4) since a
shorter cooldown narrows triangulation.

**P0-2 (best_hours type).** `best_call_hours(cid)` returns `list[HourScore]`
(NamedTuple hour/p_pickup/source), NOT `list[int]`. WS5 seed:
`best_hours = [hs.hour for hs in best_call_hours(cid)] or [10,13,16]`, deduped, sorted,
each asserted ∈ range(8,19) at seed time. WS4 rotation consumes a plain int list only.

**P0-3 (60-min AWAIT requeue vs no-connect — RESOLVED, SLA from owner).**
Owner confirmed: the bot disposition lands IMMEDIATELY via webhook once the call FIRES;
the only lag is the 750/hr queue before the call is actually fired. Therefore:
`VENDOR_DISPOSITION_SLA_MIN = 90` (env `WF_VENDOR_DISPOSITION_SLA_MIN`, conservative).
AWAIT 60-min timeout edge logic (the run must STAY current_node_type='AWAIT_DISPOSITION'
on every re-park so `_wake_run`'s guard still passes):
  1. No FIRED row for this run since `entered_node_at_ist` → call still queued → re-park,
     do NOT tick day_index, do NOT burn attempts_today.
  2. FIRED row exists since entry AND `now - fired_at_ist < 90min` → disposition still
     legitimately in-flight (webhook + 15-min ingest cron lag) → re-park, no counter touch.
  3. FIRED row exists AND `now - fired_at_ist >= 90min` AND still no disposition →
     genuine no-connect → route to the attempts_today same-day-retry gate.
AWAIT handler gains a `txn` read (FIRED-since-entry query on wf_pending_actions). e2e gate:
a disposition arriving at SLA+ must wake correctly and must NOT have burned an attempt.

**P0-4 (cross-spell concurrent runs — dual-anchor match).** A cured-then-relapsed
customer gets a new `{customer_id}_{spell_start}` enrollment_key → a second non-terminal
`workflow_runs` row is schema-permissible. `_find_matching_pending_action` +`_wake_run`
must jointly tie a disposition to the SINGLE run currently WAITING at AWAIT_DISPOSITION:
match on `customer_id` + the parked run's `run_id` + `fired_at_ist >= that run's
entered_node_at_ist`; if >1 candidate parked-at-AWAIT run exists, LOG + SKIP (never guess).
No partial-unique-index (avoids a relapse-collision policy).

**P1-2 (decision_log uniqueness).** Migration 006 UNIQUE key =
`(run_id, node_id, attempt_count, COALESCE(comment_id,''))` (SQLite treats NULLs as
distinct, so adhoc NULL-comment_id rows would otherwise escape dedup). Dedup existing
rows (keep MIN(id) per key) BEFORE `CREATE UNIQUE INDEX`. Use `INSERT OR IGNORE`.

**P1-3 (dynamic cap env is NET-NEW).** `WF_HOURLY_CALL_CAP` is NOT currently read
(`DEFAULT_HOURLY_CALL_CAP=750` hardcoded). Adding the env read is new code; unset →
stay 750 floor; adaptive value clamped `[750, min(1000, env_ceiling)]`.

**P1-1 / P2-1 / P2-2:** per-segment fire-day×max-attempt assertion table in the generator
test; ASSIGN_TIMEOUT node removed (dead under requeue) — its WS10 reason retired; shadow
e2e must exercise the reserve/general two-pass split (assert via SHADOW_FIRED rows).

Build proceeds shadow-first; live flip requires explicit Sahil "go" (feedback_manual_vibrium_confirm).
