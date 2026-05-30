"""Shim package for cross-repo imports from sibling repos checked out at $HOME.

The symlink `external/vibrium_automation_scripts` points at the sibling repo's
`scripts/` directory (resolved at clone time — see `scripts/check_external_links.py`
for the canonical path resolution).

Importing through this shim lets workflow code resolve, e.g.:

    from external.vibrium_automation_scripts.clevertap_trigger import trigger

without pip-installing the sibling repo. Phase 0 / P1-3 decision: symlink
approach, not pip-install-e — the sibling repo's `scripts/` is a flat module
namespace, not a package, and `setuptools` would complain.

If the symlink target is missing, `scripts/check_external_links.py` exits
non-zero with a remediation message (clone the sibling repo at $HOME first).
"""
