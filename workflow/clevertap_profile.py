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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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

def _env_int(name: str, default: int) -> int:
    """Read an int tunable from env, falling back to `default` on absent/garbage."""
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.warning("env %s=%r not an int — using default %d", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        log.warning("env %s=%r not a float — using default %s", name, raw, default)
        return default


# (connect, read) seconds — env-overridable per WS1.
_TIMEOUT: tuple[int, int] = (
    _env_int("CT_FETCH_CONNECT_TIMEOUT", 5),
    _env_int("CT_FETCH_READ_TIMEOUT", 30),
)
_MAX_RETRIES_ON_429: int = _env_int("CT_FETCH_MAX_RETRIES", 4)
_DEFAULT_429_SLEEP_SEC: int = 5                       # used when Retry-After absent
_BACKOFF_BASE_SEC: float = _env_float("CT_FETCH_BACKOFF_BASE", 2.0)   # exp backoff base
_BACKOFF_CAP_SEC: float = _env_float("CT_FETCH_BACKOFF_CAP", 30.0)    # max single sleep
_THROTTLE_WINDOW_SEC: int = 60                        # 429 observation window
_SESSION_TTL_SEC: int = 600                           # rebuild pooled session every 10 min
_POOL_CONNECTIONS: int = 4
# CT throttles the profile API on CONCURRENCY (~15 parallel max per CT KB), not a
# time-window QPS — so max-concurrency is the PRIMARY control (audit P2-4). Keep a
# token bucket as a secondary smoother. Both env-overridable.
_MAX_CONCURRENCY: int = max(1, min(_env_int("CT_FETCH_MAX_CONCURRENCY", 8), 15))
_FETCH_QPS: float = _env_float("CT_FETCH_QPS", 6.0)   # secondary smoother
_FETCH_BURST: int = _env_int("CT_FETCH_BURST", 8)
_POOL_MAXSIZE: int = max(16, _MAX_CONCURRENCY + 4)    # pool ≥ concurrency

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
# Global rate limiter (token bucket) — secondary smoother under the
# concurrency cap. Shared process-wide across all fetch workers.
# --------------------------------------------------------------------------

class _TokenBucket:
    """Thread-safe token bucket. `acquire()` blocks until a token is available.

    Refill math runs under the lock; the sleep happens OUTSIDE the lock so one
    waiting worker never serializes the others (audit P2-2). `rate` can be
    scaled down on sustained 429s and ramped back up after a quiet window.
    """

    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self._base_rate = max(0.1, float(rate_per_sec))
        self._rate = self._base_rate
        self._capacity = max(1, int(burst))
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self, now: float) -> None:
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._refill_locked(now)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # how long until one token is available
                deficit = 1.0 - self._tokens
                wait_s = deficit / self._rate if self._rate > 0 else 0.1
            time.sleep(min(max(wait_s, 0.005), 1.0))   # sleep OUTSIDE the lock

    def scale_down(self, factor: float = 0.5, floor: float = 0.5) -> None:
        with self._lock:
            self._rate = max(floor, self._rate * factor)

    def ramp_up(self, factor: float = 1.5) -> None:
        with self._lock:
            self._rate = min(self._base_rate, self._rate * factor)

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate


# Module-wide bucket — one limiter for every CT GET this process makes.
_BUCKET = _TokenBucket(_FETCH_QPS, _FETCH_BURST)


def _backoff_sleep_seconds(resp: requests.Response, attempt: int) -> float:
    """429 sleep: honor Retry-After if present (CT KB), else exponential backoff
    with a cap. attempt is 1-based."""
    raw = resp.headers.get("Retry-After")
    if raw is not None:
        try:
            return float(max(int(raw), 0))
        except (TypeError, ValueError):
            log.warning("CT malformed Retry-After=%r — using exp backoff", raw)
    return min(_BACKOFF_CAP_SEC, _DEFAULT_429_SLEEP_SEC * (_BACKOFF_BASE_SEC ** (attempt - 1)))


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

    Routes through `_get_profile_classified` so single + bulk share one global
    token bucket + exponential backoff. On 429 it honors `Retry-After` (else
    exp backoff), retrying up to `_MAX_RETRIES_ON_429` times.

    CONTRACT (changed in WS1): on a persistent error — 429-exhausted, network
    failure, 5xx, non-JSON, or a CT response missing `record` — this raises
    `RuntimeError` (NOT `requests.HTTPError`). The executor's FETCH_CT_PROPS
    handler treats None as the 'not_found' edge and any exception as the
    'error' edge, so the run lands on ERROR/not_found correctly either way.
    """
    if not identity:
        raise ValueError("identity must be a non-empty string")

    # Route through the shared bucket-aware fetch so single + bulk obey the same
    # global rate limit and backoff. Contract preserved: record on found, None
    # on 404, raise on persistent error (the executor's FETCH_CT_PROPS handler
    # treats None as the 'not_found' edge and an exception as the 'error' edge).
    status, record = _get_profile_classified(identity, creds_path, _Backoff429Tracker())
    if status == "found":
        return record
    if status == "not_found":
        log.info("CT profile not found: identity=%s", identity)
        return None
    raise RuntimeError(f"get_profile failed (status=error) for identity={identity}")


# --------------------------------------------------------------------------
# Public — bulk_get_profiles
# --------------------------------------------------------------------------

class _Backoff429Tracker:
    """Tracks 429s in a rolling window. Signals scale-down on a 2nd-in-window
    429, and reports whether we've been quiet long enough to ramp the bucket
    back up. Thread-safe (bulk workers call concurrently)."""

    def __init__(self, window_sec: int = _THROTTLE_WINDOW_SEC) -> None:
        self._window_sec = window_sec
        self._events: list[float] = []
        self._last_429: float = 0.0
        self._lock = threading.Lock()

    def record_429(self) -> int:
        now = time.monotonic()
        with self._lock:
            self._events.append(now)
            self._last_429 = now
            self._events = [t for t in self._events if t >= now - self._window_sec]
            return len(self._events)

    def quiet_for(self, secs: float) -> bool:
        with self._lock:
            if self._last_429 == 0.0:
                return True
            return (time.monotonic() - self._last_429) >= secs


def _get_profile_classified(
    identity: str,
    creds_path: Path | None,
    tracker: _Backoff429Tracker,
) -> tuple[str, dict[str, Any] | None]:
    """Single GET via the shared token bucket + exponential backoff.

    Returns (status, record) where status ∈ {'found','not_found','error'}.
    Never raises — the prefetch caller persists the status verbatim, so a
    transient 'error' is retried next pass under the per-day attempt cap.
    """
    if not identity:
        return ("error", None)

    creds = _load_creds(creds_path)
    url = f"{_base_url(creds)}{_GET_PATH}?{urlencode({'identity': str(identity)})}"
    headers = _auth_headers(creds)

    for attempt in range(1, _MAX_RETRIES_ON_429 + 1):
        _BUCKET.acquire()                                   # global rate gate
        try:
            resp = _session().get(url, headers=headers, timeout=_TIMEOUT)
        except requests.RequestException as exc:
            log.warning("CT GET network err identity=%s attempt=%d err=%s",
                        identity, attempt, exc)
            if attempt >= _MAX_RETRIES_ON_429:
                return ("error", None)
            time.sleep(min(_BACKOFF_CAP_SEC,
                           _DEFAULT_429_SLEEP_SEC * (_BACKOFF_BASE_SEC ** (attempt - 1))))
            continue

        if resp.status_code == 404:
            return ("not_found", None)
        if resp.status_code == 429:
            n_in_window = tracker.record_429()
            if n_in_window >= 2:
                _BUCKET.scale_down()                        # back off the whole process
            sleep_s = _backoff_sleep_seconds(resp, attempt)
            log.warning("CT GET 429 identity=%s attempt=%d/%d sleep=%.1fs rate=%.2f/s",
                        identity, attempt, _MAX_RETRIES_ON_429, sleep_s, _BUCKET.rate)
            if attempt >= _MAX_RETRIES_ON_429:
                return ("error", None)
            time.sleep(sleep_s)
            continue

        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            log.warning("CT GET http err identity=%s status=%s err=%s",
                        identity, resp.status_code, exc)
            return ("error", None)

        # success — ramp the bucket back up once 429s have stopped for a window
        if tracker.quiet_for(_THROTTLE_WINDOW_SEC):
            _BUCKET.ramp_up()
        try:
            body = resp.json()
        except ValueError:
            log.warning("CT GET non-JSON body identity=%s", identity)
            return ("error", None)
        record = body.get("record")
        if not isinstance(record, dict):
            log.warning("CT GET response missing 'record' identity=%s", identity)
            return ("error", None)
        return ("found", record)

    return ("error", None)


def _bulk_fetch_core(
    identities: list[str],
    *,
    concurrency: int,
    creds_path: Path | None,
) -> dict[str, tuple[str, dict[str, Any] | None]]:
    """Persistent-pool bulk fetch with BOUNDED submission (no submit-all-at-once)
    and the shared token bucket. Returns `{identity: (status, record)}`.

    Concurrency is capped at `_MAX_CONCURRENCY` (CT throttles on parallelism);
    request RATE is bounded by the global token bucket; 429s scale the bucket
    down across all workers and ramp back up after a quiet window. No
    concurrency-halving / cancel-requeue (that was the stall class)."""
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for i in identities:
        s = str(i).strip()
        if s and s not in seen:
            seen.add(s)
            ordered_unique.append(s)

    out: dict[str, tuple[str, dict[str, Any] | None]] = {}
    if not ordered_unique:
        return out

    workers = max(1, min(int(concurrency), _MAX_CONCURRENCY))
    tracker = _Backoff429Tracker()
    window = workers * 4                                    # bounded in-flight
    pending = iter(ordered_unique)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures: dict[Any, str] = {}
        for _ in range(window):
            ident = next(pending, None)
            if ident is None:
                break
            futures[ex.submit(_get_profile_classified, ident, creds_path, tracker)] = ident

        while futures:
            done, _pendingset = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for fut in done:
                ident = futures.pop(fut)
                try:
                    status, record = fut.result()
                except Exception as exc:                   # noqa: BLE001 — worker traps internally
                    log.warning("bulk fetch worker exc identity=%s err=%s", ident, exc)
                    status, record = ("error", None)
                out[ident] = (status, record)
                nxt = next(pending, None)
                if nxt is not None:
                    futures[ex.submit(_get_profile_classified, nxt, creds_path, tracker)] = nxt

    return out


def bulk_fetch_status(
    identities: list[str],
    *,
    concurrency: int = _MAX_CONCURRENCY,
    creds_path: Path | None = None,
) -> dict[str, tuple[str, dict[str, Any] | None]]:
    """Bulk fetch returning `{identity: (status, record)}` with status ∈
    {'found','not_found','error'} — the shape `ct_prefetch` persists to cache."""
    return _bulk_fetch_core(identities, concurrency=concurrency, creds_path=creds_path)


def bulk_get_profiles(
    identities: list[str],
    *,
    concurrency: int = _MAX_CONCURRENCY,
    creds_path: Path | None = None,
) -> dict[str, dict[str, Any] | None]:
    """Backward-compatible wrapper: `{identity: record_or_None}` (None = 404 OR
    error). Prefer `bulk_fetch_status` when you need to distinguish the two."""
    core = _bulk_fetch_core(identities, concurrency=concurrency, creds_path=creds_path)
    return {ident: (rec if status == "found" else None) for ident, (status, rec) in core.items()}


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
