"""Shared types used across workflow modules.

Kept deliberately minimal — only contracts that cross module boundaries belong
here. Per-module result types stay local to that module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SetResult:
    """Outcome of a CleverTap profile-write call (`set_profile`).

    `success=True` requires BOTH:
      1. HTTP 2xx response.
      2. The identity we wrote does NOT appear in the response's `unprocessed[]`
         array. (CT KB hard rule — top-level `status:"success"` alone is not
         trusted; see ~/.claude/auditor_kb/clevertap.md.)

    `error_code` is the per-record CT error code (e.g. 516 for bad phone, 514
    for bad gender) pulled from the `unprocessed[]` entry. None on success.

    `raw_response` is the full parsed JSON body for diagnostic surfaces.
    """

    success: bool
    error_code: int | None
    raw_response: dict[str, Any]
