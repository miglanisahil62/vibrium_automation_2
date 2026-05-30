"""Phase 0 smoke test: confirms the package skeleton imports.

Acceptance criterion from PHASES.md Phase 0:
    python3 -c "import workflow; import shared" succeeds.

This test is the pytest version of that check. Any future structural break in
the package layout (missing __init__.py, accidental syntax error in a top-level
module) will fail here first.
"""
from __future__ import annotations


def test_workflow_package_imports() -> None:
    import workflow  # noqa: F401

    assert workflow.__doc__, "workflow package should have a module docstring"


def test_shared_package_imports() -> None:
    import shared  # noqa: F401

    assert shared.__doc__, "shared package should have a module docstring"


def test_scripts_package_imports() -> None:
    import scripts  # noqa: F401

    assert scripts.__doc__, "scripts package should have a module docstring"


def test_workflow_subpackages_import() -> None:
    """The 4 declared subpackages all resolve."""
    import workflow.agents  # noqa: F401
    import workflow.agents.workflow_handlers  # noqa: F401
    import workflow.migrations  # noqa: F401
    import workflow.tests  # noqa: F401
