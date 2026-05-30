"""Stateless CleverTap profile HTTP client for the workflow engine.

Three operations:
  - get_profile(identity)        — GET /1/profile.json?identity=<id>
  - bulk_get_profiles(identities)— concurrent GETs with shared 429 governor
  - set_profile(identity, props) — POST /1/upload; inspects unprocessed[]

No DB access, no file I/O beyond the credential cache. Mirrors the
session-pool + cred-mtime-cache pattern from
~/vibrium-automation/scripts/clevertap_trigger.py to avoid the ulimit-1024 FD
leak that took down the adhoc scheduler in 2026-05.

CT KB hard rules followed (~/.claude/auditor_kb/clevertap.md):
  - `unprocessed[]` is inspected on every `/upload` response. `status:"success"`
    is NEVER trusted alone.
  - `Retry-After` header honored on 429; fallback 5s when header absent.
  - `timeout=(5, 30)` on every request.
  - Region pinned to `in1` via the creds file `base_url` (Stashfin's CT
    residency).
  - This module NEVER fires an externaltrigger campaign. That path lives in
    ~/vibrium-automation/scripts/clevertap_trigger.py and is gated by Sahil
    approval per `feedback_cleverap_campaign_approval`.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests

from workflow.types import SetResult

log = logging.getLogger("workflow.clevertap_profile")


# --------------------------------------------------------------------------
# Tunables
# --------------------------------------------------------------------------

_DEFAULT_CREDS_PATH = Path(
    os.path.expanduser("~/Collections_v3/Clevertap campaigns/config_CT_credentials.json")
)

_TIMEOUT: tuple[int, int] = (5, 30)                  # (connect, read) seconds
_MAX_RETRIES_ON_429: int = 3
_DEFAULT_429_SLEEP_SEC: int = 5                       # used when Retry-After absent
_THROTTLE_WINDOW_SEC: int = 60                        # halving window for bulk
_SESSION_TTL_SEC: int = 600                           # rebuild pooled session every 10 min
_POOL_CONNECTIONS: int = 4
_POOL_MAXSIZE: int = 16                               # > default ThreadPool concurrency=5

# Profile GET endpoint (region from creds.base_url).
_GET_PATH: str = "/profile.json"
# Profile upload endpoint — NOT /profiles.json. See CT KB: profile upload goes
# to /1/upload with type=profile in each record.
_UPLOAD_PATH: str = "/upload"


# --------------------------------------------------------------------------
# Credential cache (mtime-invalidated) + session pool
# --------------------------------------------------------------------------

_CRED_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}   # path → (mtime, creds)
_CRED_LOCK = threading.Lock()

_SESSION: Optional[requests.Session] = None
_SESSION_CREATED_AT: float = 0.0
_SESSION_LOCK = threading.Lock()


def _resolve_creds_path(creds_path: Path | None) -> Path:
    """Resolution order: explicit arg → CT_CREDS_FILE env → default path."""
    if creds_path is not None:
        return Path(creds_path)
    env_override = os.environ.get("CT_CREDS_FILE")
    if env_override:
        return Path(os.path.expanduser(env_override))
    return _DEFAULT_CREDS_PATH


def _load_creds(creds_path: Path | None = None) -> dict[str, Any]:
    """Load + cache credentials. Reloads if the file's mtime changed since the
    last load — supports passcode rotation without process restart.
    """
    path = _resolve_creds_path(creds_path)
    key = str(path)
    try:
        mtime = path.stat().st_mtime
    except OSError as exc:
        raise FileNotFoundError(f"CT creds file not found: {path}") from exc

    with _CRED_LOCK:
        cached = _CRED_CACHE.get(key)
        if cached and cached[0] == mtime:
            return cached[1]
        with open(path) as f:
            creds = json.load(f)
        _CRED_CACHE[key] = (mtime, creds)
    return creds


def _session() -> requests.Session:
    """Pooled requests Session with a TTL so dead keep-alives get refreshed.

    Mirrors clevertap_trigger.py:43-66 — necessary to keep FD count below the
    1024 ulimit when the scheduler fires hundreds of calls per tick.
    """
    global _SESSION, _SESSION_CREATED_AT
    now = time.monotonic()
    with _SESSION_LOCK:
        if _SESSION is None or (now - _SESSION_CREATED_AT) > _SESSION_TTL_SEC:
            if _SESSION is not None:
                try:
                    _SESSION.close()
                except Exception:  # stashfin-lint: ignore  # closing an expired session is benign
                    pass
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=_POOL_CONNECTIONS,
                pool_maxsize=_POOL_MAXSIZE,
                max_retries=0,                                 # we own retry policy
            )
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            _SESSION = s
            _SESSION_CREATED_AT = now
    return _SESSION


def _auth_headers(creds: dict[str, Any]) -> dict[str, str]:
    return {
        "X-CleverTap-Account-Id": creds["ACCOUNT_ID"],
        "X-CleverTap-Passcode": creds["PASSCODE"],
        "Content-Type": "application/json; charset=utf-8",
    }


def _base_url(creds: dict[str, Any]) -> str:
    """Resolve the regional base URL. Stashfin's `in1` lives in the creds JSON
    (matches every other CT script in the tree).
    """
    base = creds.get("BASE_URL") or "https://in1.api.clevertap.com/1"
    return str(base).rstrip("/")


def _retry_after_seconds(resp: requests.Response) -> int:
    """Honor `Retry-After` header (CT KB rule). Header is seconds (RFC 7231 also
    permits HTTP-date; CT only emits integer seconds). Falls back to
    _DEFAULT_429_SLEEP_SEC when absent or unparseable.
    """
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return _DEFAULT_429_SLEEP_SEC
    try:
        sleep_s = int(raw)
        return max(sleep_s, 0)
    except (TypeError, ValueError):
        # Documented contract per CT KB: CT emits integer-seconds Retry-After.
        # If a malformed header arrives, log loudly and use the documented
        # default fallback rather than crashing the retry loop.
        log.warning(
            "CT returned malformed Retry-After header: %r — falling back to %ds",
            raw, _DEFAULT_429_SLEEP_SEC,
        )
        return _DEFAULT_429_SLEEP_SEC


# --------------------------------------------------------------------------
# Public — get_profile
# --------------------------------------------------------------------------

def get_profile(
    identity: str,
    *,
    creds_path: Path | None = None,
) -> dict[str, Any] | None:
    """GET /1/profile.json?identity=<id>.

    Returns the parsed `record` dict on HTTP 200 — caller extracts the 4
    workflow-relevant properties from `record["profileData"]`:
      - coll_collection_risk_segmentation (int)
      - coll_notification_replied (str)
      - coll_bot_calling (str)
      - dpd (int)

    Returns None on HTTP 404 (identity not in CT user store).

    On 429: sleeps `Retry-After` seconds (or 5s default), retries up to 3
    times. Raises after the final 429.

    On other non-2xx: raises HTTPError via `raise_for_status()`.
    """
    if not identity:
        raise ValueError("identity must be a non-empty string")

    creds = _load_creds(creds_path)
    url = f"{_base_url(creds)}{_GET_PATH}?{urlencode({'identity': str(identity)})}"
    headers = _auth_headers(creds)

    for attempt in range(1, _MAX_RETRIES_ON_429 + 1):
        resp = _session().get(url, headers=headers, timeout=_TIMEOUT)
        if resp.status_code == 404:
            log.info("CT profile not found: identity=%s", identity)
            return None
        if resp.status_code == 429:
            sleep_s = _retry_after_seconds(resp)
            log.warning(
                "CT profile 429 attempt=%d/%d sleep=%ds identity=%s",
                attempt, _MAX_RETRIES_ON_429, sleep_s, identity,
            )
            if attempt >= _MAX_RETRIES_ON_429:
                resp.raise_for_status()                # surfaces 429 traceback
            time.sleep(sleep_s)
            continue
        # Any non-2xx other than 404/429 raises — caller decides recovery.
        resp.raise_for_status()
        body = resp.json()
        record = body.get("record")
        if not isinstance(record, dict):
            # CT shape contract violated — surface loudly rather than handing
            # back garbage downstream.
            raise RuntimeError(
                f"CT profile response missing 'record' object: identity={identity} "
                f"body_keys={list(body.keys())}"
            )
        return record

    # Defensive — loop always returns or raises above.
    raise RuntimeError(f"get_profile fell through retry loop: identity={identity}")


# --------------------------------------------------------------------------
# Public — bulk_get_profiles
# --------------------------------------------------------------------------

class _ThrottleGovernor:
    """Tracks 429 events in a rolling window. After the second 429 within
    THROTTLE_WINDOW_SEC, signal that callers should halve concurrency.

    Thread-safe: bulk_get_profiles workers call .record_429() concurrently.
    """

    def __init__(self, window_sec: int = _THROTTLE_WINDOW_SEC) -> None:
        self._window_sec = window_sec
        self._events: list[float] = []
        self._lock = threading.Lock()

    def record_429(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._events.append(now)
            # Prune outside the window so the list doesn't grow unbounded.
            cutoff = now - self._window_sec
            self._events = [t for t in self._events if t >= cutoff]

    def should_halve(self) -> bool:
        now = time.monotonic()
        cutoff = now - self._window_sec
        with self._lock:
            self._events = [t for t in self._events if t >= cutoff]
            return len(self._events) >= 2


def bulk_get_profiles(
    identities: list[str],
    *,
    concurrency: int = 5,
    creds_path: Path | None = None,
) -> dict[str, dict[str, Any] | None]:
    """Concurrent GET /profile.json for many identities.

    Returns an order-stable map `{identity: record_or_None}`. A None value
    means EITHER "404 not in CT" OR "failed after 3 retries"; the caller
    treats both as "don't advance the run; retry next tick".

    Concurrency policy: starts at `concurrency`; on the SECOND 429 within
    a 60-second window, halves concurrency for the REMAINDER of this call.
    A single 429 (rare blip) does not halve.
    """
    if not identities:
        return {}
    if concurrency < 1:
        raise ValueError(f"concurrency must be >= 1; got {concurrency}")

    # Dedupe input but preserve first-seen order for the output map.
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for i in identities:
        if i not in seen:
            seen.add(i)
            ordered_unique.append(i)

    governor = _ThrottleGovernor()
    results: dict[str, dict[str, Any] | None] = {i: None for i in ordered_unique}

    def _fetch_one(ident: str) -> tuple[str, dict[str, Any] | None]:
        try:
            rec = _get_profile_with_governor(ident, governor, creds_path)
            return ident, rec
        except Exception as exc:                       # noqa: BLE001 — bounded by retry
            log.warning("bulk_get_profiles fail identity=%s err=%s", ident, exc)
            return ident, None

    # Phase 1: initial-concurrency workers. If governor trips halving partway
    # through, Phase 2 reruns the remainder at concurrency // 2.
    remaining = list(ordered_unique)
    current_concurrency = max(concurrency, 1)

    while remaining:
        in_flight: list[str] = []
        executor = ThreadPoolExecutor(max_workers=current_concurrency)
        try:
            future_to_id = {executor.submit(_fetch_one, ident): ident for ident in remaining}
            in_flight = list(remaining)
            remaining = []
            halve_triggered = False

            for fut in as_completed(future_to_id):
                # Cancelled futures (from a halving event below) surface here
                # too. Their identities will be re-queued — skip the result.
                if fut.cancelled():
                    continue
                try:
                    ident, rec = fut.result()
                except Exception as exc:                          # noqa: BLE001
                    # Unbounded exception from _fetch_one shouldn't happen
                    # (it traps internally), but if it does, the identity
                    # stays None in the result map.
                    log.warning("bulk_get_profiles unexpected exc=%s", exc)
                    continue
                results[ident] = rec
                if governor.should_halve() and not halve_triggered and current_concurrency > 1:
                    halve_triggered = True
                    # Cancel any not-yet-started futures and re-queue them at
                    # the lower concurrency. Already-running futures complete
                    # under the old pool.
                    new_remaining: list[str] = []
                    for fut2, ident2 in future_to_id.items():
                        if fut2.cancel():
                            new_remaining.append(ident2)
                    if new_remaining:
                        new_concurrency = max(current_concurrency // 2, 1)
                        log.warning(
                            "bulk_get_profiles: 2nd 429 within %ds; halving concurrency "
                            "%d -> %d, requeueing %d identities",
                            _THROTTLE_WINDOW_SEC, current_concurrency, new_concurrency,
                            len(new_remaining),
                        )
                        remaining = new_remaining
                        current_concurrency = new_concurrency
        finally:
            executor.shutdown(wait=True, cancel_futures=False)

        # If no halving happened, remaining is [] and the while-loop exits.
        # If halving did happen, the loop iterates once more at lower concurrency.

    return results


def _get_profile_with_governor(
    identity: str,
    governor: _ThrottleGovernor,
    creds_path: Path | None,
) -> dict[str, Any] | None:
    """Internal: like `get_profile` but reports 429s to the shared governor so
    `bulk_get_profiles` can halve concurrency across all workers.
    """
    if not identity:
        raise ValueError("identity must be a non-empty string")

    creds = _load_creds(creds_path)
    url = f"{_base_url(creds)}{_GET_PATH}?{urlencode({'identity': str(identity)})}"
    headers = _auth_headers(creds)

    for attempt in range(1, _MAX_RETRIES_ON_429 + 1):
        resp = _session().get(url, headers=headers, timeout=_TIMEOUT)
        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            governor.record_429()
            sleep_s = _retry_after_seconds(resp)
            if attempt >= _MAX_RETRIES_ON_429:
                resp.raise_for_status()
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        body = resp.json()
        record = body.get("record")
        if not isinstance(record, dict):
            raise RuntimeError(
                f"CT profile response missing 'record' object: identity={identity}"
            )
        return record

    raise RuntimeError(f"_get_profile_with_governor fell through: identity={identity}")


# --------------------------------------------------------------------------
# Public — set_profile
# --------------------------------------------------------------------------

def set_profile(
    identity: str,
    properties: dict[str, Any],
    *,
    dry_run: bool = False,
    creds_path: Path | None = None,
) -> SetResult:
    """POST /1/upload with one record of type='profile'.

    CT KB hard rule: a top-level `status:"success"` is NOT trusted. The
    identity is matched against `unprocessed[]`; absence-from-unprocessed
    means success.

    `dry_run=True` appends `?dryRun=1` to the URL — CT validates the payload
    shape and returns the same response structure without persisting.

    Returns SetResult(success, error_code, raw_response). `error_code` is
    populated from the unprocessed-entry's `code` field when the identity
    is rejected; None on success.
    """
    if not identity:
        raise ValueError("identity must be a non-empty string")
    if not isinstance(properties, dict) or not properties:
        raise ValueError("properties must be a non-empty dict")

    creds = _load_creds(creds_path)
    url = f"{_base_url(creds)}{_UPLOAD_PATH}"
    if dry_run:
        url = f"{url}?dryRun=1"
    headers = _auth_headers(creds)

    payload = {
        "d": [
            {
                "identity": str(identity),
                "type": "profile",
                "profileData": properties,
            }
        ]
    }

    for attempt in range(1, _MAX_RETRIES_ON_429 + 1):
        resp = _session().post(url, headers=headers, json=payload, timeout=_TIMEOUT)
        if resp.status_code == 429:
            sleep_s = _retry_after_seconds(resp)
            log.warning(
                "CT upload 429 attempt=%d/%d sleep=%ds identity=%s",
                attempt, _MAX_RETRIES_ON_429, sleep_s, identity,
            )
            if attempt >= _MAX_RETRIES_ON_429:
                resp.raise_for_status()
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        data = resp.json()
        return _classify_upload(identity, data)

    raise RuntimeError(f"set_profile fell through retry loop: identity={identity}")


def _classify_upload(identity: str, data: dict[str, Any]) -> SetResult:
    """Apply the CT-KB hard rule: walk `unprocessed[]` and match our identity.

    Three top-level outcomes:
      - status == "fail"     → entire batch rejected; success=False.
      - identity in unprocessed → per-record rejection; success=False; error_code populated.
      - otherwise             → success=True (whether status is "success" OR "partial",
                                 because for our single-record batch, absence from
                                 unprocessed[] is dispositive).
    """
    top_status = data.get("status")
    unprocessed = data.get("unprocessed") or []
    unprocessed_count = len(unprocessed)
    if top_status == "fail":
        log.warning(
            "CT upload rejected top_status=fail unprocessed_count=%d",
            unprocessed_count,
        )
        return SetResult(success=False, error_code=None, raw_response=data)

    if top_status == "partial":
        log.warning(
            "CT upload partial top_status=partial unprocessed_count=%d",
            unprocessed_count,
        )
        # Don't return yet — still need to check whether OUR identity is the
        # one that failed. The unprocessed-walk below decides.

    for rec in unprocessed:
        rec_identity = (rec.get("record") or {}).get("identity")
        if rec_identity is not None and str(rec_identity) == str(identity):
            code = rec.get("code")
            try:
                code_int = int(code) if code is not None else None
            except (TypeError, ValueError):
                code_int = None
            log.warning(
                "CT upload per-record fail identity=%s code=%s error=%s",
                identity, code, rec.get("error"),
            )
            return SetResult(success=False, error_code=code_int, raw_response=data)

    return SetResult(success=True, error_code=None, raw_response=data)
