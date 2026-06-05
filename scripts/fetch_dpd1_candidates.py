#!/usr/bin/env python3
"""Fetch today's DPD-1 enrollment candidates from collection_view → daily CSV.

Runs at ~07:30 IST on the AWS server, BEFORE the enrollment poller. Queries
``sttash_website_live.collection_view`` for customers at exactly ``ageing = 1``
(the DPD-1 / X-bucket cohort) and writes their customer_ids to a date-stamped
CSV. The enrollment poller (running in its 07:35 prep slot) reads that CSV,
fetches each customer's CleverTap profile, applies the entry condition, and
creates parked workflow runs so the same-day cohort is ready when the 08:00
calling window opens.

Empty days are EXPECTED: ``ageing == 1`` is a thin one-day slice, and on many
days no customer is exactly one day past due (the cohort populates as customers
cross the boundary). So a successful query returning 0 rows is a normal empty
day, NOT a failure — we write a header-only CSV and exit 0 (the poller reads it
and enrols nobody). The failure signal is reserved for a genuine
query/connection error, which raises an exception → exit 1 → 'down' heartbeat +
alert. The output is also DATE-STAMPED so a failed run cannot leave a stale file
that the poller would mistake for today's cohort.

CLI:
    python3 fetch_dpd1_candidates.py                 # full production run
    python3 fetch_dpd1_candidates.py --dry-run       # query + log count; no file
    python3 fetch_dpd1_candidates.py --limit 100     # smoke test, capped cohort

Cron entry (example — AWS server is UTC; 07:30 IST = 02:00 UTC):
    # 07:30 IST primary
    0 2 * * * /home/ubuntu/vibrium-workflow/run_fetch.sh
    # 08:15 IST fallback (the `fallback` arg makes it skip if today's file exists)
    45 2 * * * /home/ubuntu/vibrium-workflow/run_fetch.sh fallback

Pairs with: vibrium-workflow/run_fetch.sh wrapper
Heartbeat job_id: wf_fetch_dpd1
Alert recipient: sahil.miglani@stashfin.com
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# ─── Paths (absolute) ──────────────────────────────────────────────────────
REPO_DIR = Path(__file__).resolve().parent.parent
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

# Output dir for the daily candidate CSVs. Overridable via env so the cron /
# tests can redirect it; defaults to the repo's state/ tree.
OUTPUT_DIR = Path(
    os.environ.get("WF_ENROLLMENT_CSV_DIR", str(REPO_DIR / "state" / "enrollment"))
)

# ─── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(Path(__file__).stem)

# ─── Timezone (Stashfin is IST-anchored) ───────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")

# X-bucket selector. collection_view's days-past-due column is ``ageing`` (NOT
# ``dpd`` — that column does not exist there). We target the whole early bucket
# ageing 1..30. Parameterised below; no string interpolation into SQL.
# (Membership in collection_view IS the authoritative DPD filter — DPD 0 = cured =
# absent from the view. The CT `dpd` profile property is sparse/unreliable, so the
# graph does NOT re-gate on it; see ENROLL_CONDITION_EXPR in generate_vb_collections_v2.)
_CANDIDATE_SQL = """
    SELECT customer_id, MIN(ageing) AS ageing
    FROM sttash_website_live.collection_view
    WHERE ageing BETWEEN %(lo)s AND %(hi)s
      AND customer_id IS NOT NULL
    GROUP BY customer_id
"""

_AGEING_LO = 1
_AGEING_HI = 30
_TARGET_AGEING = _AGEING_LO  # retained for log/back-compat messages


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true",
                    help="Run the query and log the row count; write no file.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap the cohort to N customers (smoke test only; N >= 1).")
    ap.add_argument("--date", default=None,
                    help="Override the cohort date (YYYY-MM-DD); default today IST.")
    args = ap.parse_args()
    if args.limit is not None and args.limit < 1:
        # --limit 0 would slice to an empty cohort and then trip the
        # zero-rows-is-failure path, raising a spurious 'down'. The flag is a
        # smoke-test cap; the minimum useful value is 1.
        ap.error("--limit must be >= 1 (it caps a smoke-test cohort)")
    return args


def _today_ist_str(override: "str | None") -> str:
    if override:
        # Validate format early — a malformed date must fail loud, not silently
        # write to a weird path.
        datetime.strptime(override, "%Y-%m-%d")
        return override
    return datetime.now(IST).strftime("%Y-%m-%d")


def _normalize_ids(raw_ids: "list", limit: "int | None") -> list[str]:
    """Normalise raw customer_id values → clean, de-duped list[str].

    Pure (no I/O) so it is unit-testable without Redshift. Handles the
    float-stored-id trap (strip a trailing '.0'), drops None/empty, de-dups
    while preserving order, and applies the optional smoke-test ``limit``.
    """
    ids: list[str] = []
    for raw in raw_ids:
        if raw is None:
            continue
        s = str(raw).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s:
            ids.append(s)

    seen: set[str] = set()
    deduped = [i for i in ids if not (i in seen or seen.add(i))]

    if limit is not None and limit >= 1:
        deduped = deduped[:limit]
    return deduped


def _fetch_candidates(limit: "int | None", cohort_date: str) -> "list[tuple[str, str]]":
    """Query collection_view for ageing 1-30 customers. Returns a list of
    ``(customer_id, spell_start)`` pairs (deduped, order-preserved).

    ``spell_start`` (WS8 spell-dedup) = ``cohort_date - (ageing - 1) days`` — the
    date the customer's CURRENT continuous overdue spell began. It is stable for
    a customer across the whole spell (ageing +1/day, date +1/day → spell_start
    constant), so keying enrollment on ``{customer_id}_{spell_start}`` enrolls
    them ONCE per spell. A cure (drops out of collection_view) + relapse restarts
    ageing → a new spell_start → eligible to re-enter.

    Uses the in-repo Redshift helper. Lazy-imported so the module is importable
    in test envs without the symlink/creds.
    """
    from datetime import date, timedelta

    from external.vibrium_automation_scripts.db import redshift, query  # type: ignore

    with redshift() as cn:
        df = query(
            cn,
            _CANDIDATE_SQL,
            params={"lo": _AGEING_LO, "hi": _AGEING_HI},
            rationale="vibrium-workflow daily X-bucket (ageing 1-30) enrollment candidate fetch",
        )

    base = date.fromisoformat(cohort_date)
    seen: set[str] = set()
    out: "list[tuple[str, str]]" = []
    for raw, ag in zip(df["customer_id"].tolist(), df["ageing"].tolist()):
        if raw is None:
            continue
        s = str(raw).strip()
        if s.endswith(".0"):           # float-stored-id trap
            s = s[:-2]
        if not s or s in seen:
            continue
        seen.add(s)
        try:
            a = max(1, int(ag))
        except (TypeError, ValueError):
            a = 1                       # missing ageing → treat as fresh spell
        spell_start = (base - timedelta(days=a - 1)).isoformat()
        out.append((s, spell_start))
        if limit is not None and limit >= 1 and len(out) >= limit:
            break
    return out


def _write_csv_atomic(path: Path, rows: "list[tuple[str, str]]") -> None:
    """Write the candidate CSV atomically (tmp + os.replace).

    Atomic so the poller — which may run concurrently in its 07:35 prep slot —
    never reads a half-written file. Columns: ``customer_id`` (the name the
    poller's _load_candidates looks for, case-insensitively) and ``spell_start``
    (WS8 spell-dedup key component; ignored by older/other readers).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["customer_id", "spell_start"])
            for cid, spell_start in rows:
                writer.writerow([cid, spell_start])
        os.replace(tmp_name, str(path))  # atomic on POSIX
    except Exception:
        # Clean up the tmp file on any failure so we don't litter state/.
        try:
            os.unlink(tmp_name)
        except OSError as cleanup_exc:
            log.warning("could not remove tmp file %s: %s", tmp_name, cleanup_exc)
        raise


def do_work(args: argparse.Namespace) -> dict:
    now = datetime.now(IST)
    cohort_date = _today_ist_str(args.date)
    out_path = OUTPUT_DIR / f"dpd1_candidates_{cohort_date}.csv"
    log.info(
        "starting  now=%s  cohort_date=%s  dry_run=%s  out=%s",
        now.isoformat(), cohort_date, args.dry_run, out_path,
    )

    candidates = _fetch_candidates(args.limit, cohort_date)
    n = len(candidates)
    log.info("collection_view returned %d distinct customer_ids (ageing %d-%d / X-bucket)",
             n, _AGEING_LO, _AGEING_HI)

    # 0 rows is NORMAL for ageing == 1: on many days no customer is exactly one
    # day past due — the DPD-1 slice populates as customers cross the boundary.
    # A genuine query/connection FAILURE raises an exception (caught in main →
    # exit 1 → 'down' heartbeat + alert) and never reaches here; so a
    # successful query that returns 0 rows is a real empty day, not a failure.
    # We write the header-only CSV (so the enrollment poller reads it cleanly
    # and enrolls nobody, rather than erroring on a missing file) and exit 0
    # (no false alert). Distinction: exception = failure; 0 rows = empty day.
    if n == 0:
        log.info("0 X-bucket candidates today (ageing %d-%d) — writing empty cohort",
                 _AGEING_LO, _AGEING_HI)

    if args.dry_run:
        log.info("--dry-run set; not writing %s (%d rows planned)", out_path, n)
        return {"dry_run": True, "cohort_date": cohort_date, "n_planned": n}

    _write_csv_atomic(out_path, candidates)
    log.info("wrote %d customer_ids → %s", n, out_path)
    return {"cohort_date": cohort_date, "n_written": n, "path": str(out_path)}


def main() -> None:
    args = parse_args()
    try:
        summary = do_work(args)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit
        log.exception("fetch_dpd1_candidates FAILED: %s", exc)
        sys.exit(1)
    log.info("done  %s", summary)


if __name__ == "__main__":
    main()
