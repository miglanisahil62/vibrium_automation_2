#!/usr/bin/env python3
"""Verify the `external/` symlink targets resolve on this machine.

Phase 0 / P1-3 fix: the workflow engine reuses `clevertap_trigger.trigger()`
and friends from the sibling adhoc-vibrium repo via a symlink under
`external/`. The symlink target is environment-specific (Mac dev tree vs
AWS deploy tree), so this script is the single source of truth for
"does the import path actually resolve?".

Exit codes:
    0 — every required symlink target exists.
    1 — at least one target is missing; remediation message printed to stderr.

Run as:
    python3 scripts/check_external_links.py

Intended caller: launchd preflight, CI, and the operator before any
workflow daemon comes up.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Repo root resolved relative to this file — no hardcoded user paths so the
# script works on both Mac dev and the AWS server.
REPO_ROOT = Path(__file__).resolve().parent.parent
EXTERNAL_DIR = REPO_ROOT / "external"

# Each entry: (symlink path relative to repo root, expected target description).
# When you add a new external dependency, register it here so this preflight
# catches missing clones before any daemon starts.
REQUIRED_LINKS = [
    (
        EXTERNAL_DIR / "vibrium_automation_scripts",
        "vibrium-automation/scripts — clone the sibling repo at $HOME first "
        "(see PHASES.md Phase 0).",
    ),
]


def _check_one(link_path: Path, remediation: str) -> tuple[bool, str]:
    """Return (ok, message) for a single symlink entry."""
    if not link_path.exists() and not link_path.is_symlink():
        return False, f"missing entirely: {link_path}\n  fix: {remediation}"

    if not link_path.is_symlink():
        return False, (
            f"present but not a symlink: {link_path}\n"
            f"  expected a symlink; got a real file/dir. "
            f"fix: remove and re-link per Phase 0 setup."
        )

    target = os.readlink(link_path)
    resolved = Path(target)
    if not resolved.is_absolute():
        resolved = (link_path.parent / resolved).resolve()

    if not resolved.exists():
        return False, (
            f"dangling symlink: {link_path} -> {target}\n"
            f"  resolved target does not exist: {resolved}\n"
            f"  fix: {remediation}"
        )

    return True, f"ok: {link_path} -> {resolved}"


def main() -> int:
    failures: list[str] = []
    for link_path, remediation in REQUIRED_LINKS:
        ok, msg = _check_one(link_path, remediation)
        stream = sys.stdout if ok else sys.stderr
        print(msg, file=stream)
        if not ok:
            failures.append(msg)

    if failures:
        print(
            f"\n{len(failures)} external link(s) failed preflight. "
            f"Workflow daemons must NOT start until this is resolved.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
