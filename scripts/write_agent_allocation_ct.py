#!/usr/bin/env python3
"""Set ``coll_agent_allocation = Agent_calling_Recommended`` in CleverTap for a
list of customer_ids — the cases handed to human agents that must be marked as
agent-allocated (and thereby excluded from bot calling downstream).

coll_bot_calling is read-only (martech-set), so coll_agent_allocation is the
writable agent-handoff property (same one WS10's exit node uses).

Resilient + idempotent:
  * reads ids from --ids-file (one per line; header tolerated);
  * skips ids already written (tracked in --done-file) so a re-run RESUMES
    rather than re-writing — safe to re-run after an interruption;
  * rate-limited (CT_SET_QPS) to avoid 429s on a large batch;
  * one bad id never aborts the batch (logged, left for the next run);
  * --dry-run validates the payload shape via CT (?dryRun=1), writes nothing,
    and does not record the id as done.

CLI:
    python3 scripts/write_agent_allocation_ct.py --ids-file state/bot_exclusion_ids.csv --dry-run
    python3 scripts/write_agent_allocation_ct.py --ids-file state/bot_exclusion_ids.csv
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from workflow import clevertap_profile  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(Path(__file__).stem)

CT_PROPERTY = "coll_agent_allocation"
CT_VALUE = "Agent_calling_Recommended"
SET_QPS = float(os.environ.get("CT_SET_QPS", "6"))  # writes/sec ceiling
DEFAULT_DONE = str(_REPO / "state" / "agent_alloc_ct_written.csv")


def _load_ids(path: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if not s or s.lower() in ("customer_id", "customer id"):
                continue
            if s.endswith(".0"):  # float-stored export → clean integer-string
                s = s[:-2]
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def _load_done(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}


def run(ids_file: str, done_file: str, *, dry_run: bool, creds_path: str | None) -> dict:
    ids = _load_ids(ids_file)
    done = _load_done(done_file)
    todo = [c for c in ids if c not in done]
    log.info("ids=%d already_written=%d todo=%d dry_run=%s",
             len(ids), len(done), len(todo), dry_run)
    if not todo:
        return {"ids": len(ids), "already": len(done), "written": 0, "failed": 0}

    interval = 1.0 / SET_QPS if SET_QPS > 0 else 0.0
    written = failed = 0
    creds = Path(creds_path) if creds_path else None
    # Append-as-we-go so an interruption preserves progress for the resume.
    done_fh = None if dry_run else open(done_file, "a")
    try:
        for i, cid in enumerate(todo, 1):
            try:
                res = clevertap_profile.set_profile(
                    cid, {CT_PROPERTY: CT_VALUE}, dry_run=dry_run, creds_path=creds)
            except Exception as exc:  # noqa: BLE001 — one bad id must not abort the batch
                failed += 1
                log.warning("ct set FAILED cid=%s (%s) — will retry next run", cid, exc)
                continue
            if not res.success:
                failed += 1
                log.warning("ct set rejected cid=%s code=%s — will retry next run",
                            cid, res.error_code)
                continue
            written += 1
            if done_fh is not None:
                done_fh.write(cid + "\n")
                if written % 200 == 0:
                    done_fh.flush()
            if i % 500 == 0:
                log.info("progress: %d/%d (written=%d failed=%d)", i, len(todo), written, failed)
            if interval:
                time.sleep(interval)
    finally:
        if done_fh is not None:
            done_fh.flush()
            done_fh.close()
    log.info("DONE: written=%d failed=%d (of %d todo)", written, failed, len(todo))
    return {"ids": len(ids), "already": len(done), "written": written, "failed": failed}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ids-file", required=True)
    ap.add_argument("--done-file", default=DEFAULT_DONE)
    ap.add_argument("--creds-path", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        summary = run(args.ids_file, args.done_file, dry_run=args.dry_run,
                      creds_path=args.creds_path)
    except Exception as exc:  # noqa: BLE001 — top-level: log + non-zero exit
        log.exception("write_agent_allocation_ct FAILED: %s", exc)
        sys.exit(1)
    log.info("summary: %s", summary)
    if summary.get("failed"):
        sys.exit(1)


if __name__ == "__main__":
    main()
