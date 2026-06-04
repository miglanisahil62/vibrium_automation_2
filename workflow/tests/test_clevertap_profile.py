"""Tests for workflow/clevertap_profile.py.

No live CT calls — every test mocks `requests.Session.get/post` (or the
session itself). The pinned fixture `ct_profile_response.json` proves the
4 target properties are extractable at the documented path.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
import requests

from workflow import clevertap_profile as ctp
from workflow.types import SetResult


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ct_profile_response.json"
TARGET_PROPS = (
    "coll_collection_risk_segmentation",
    "coll_notification_replied",
    "coll_bot_calling",
    "dpd",
)


# ---------------------------------------------------------------- helpers


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Clear cred cache + session between tests so each starts clean.

    Also neutralize the global token bucket: tests mock `time.sleep`, which
    would make `_TokenBucket.acquire()` busy-spin if tokens ran out. Pinning a
    huge capacity/rate means acquire() never has to wait — we test fetch logic,
    not the limiter (the limiter has its own dedicated tests).
    """
    ctp._CRED_CACHE.clear()
    if ctp._SESSION is not None:
        try:
            ctp._SESSION.close()
        except Exception:
            pass
    ctp._SESSION = None
    ctp._SESSION_CREATED_AT = 0.0
    _huge = 10 ** 9
    ctp._BUCKET._capacity = _huge
    ctp._BUCKET._tokens = float(_huge)
    ctp._BUCKET._base_rate = float(_huge)
    ctp._BUCKET._rate = float(_huge)
    yield
    ctp._CRED_CACHE.clear()
    ctp._SESSION = None
    ctp._SESSION_CREATED_AT = 0.0


@pytest.fixture
def fake_creds_file(tmp_path: Path) -> Path:
    p = tmp_path / "ct_creds.json"
    p.write_text(json.dumps({
        "ACCOUNT_ID": "test_account",
        "PASSCODE": "test_passcode",
        "BASE_URL": "https://in1.api.clevertap.com/1",
    }))
    return p


@pytest.fixture
def fixture_body() -> dict[str, Any]:
    with open(FIXTURE_PATH) as f:
        return json.load(f)


class _FakeResponse:
    """Minimal stand-in for requests.Response. We use `pytest-mock` to swap
    `Session.get`/`Session.post` to return one of these directly.
    """

    def __init__(
        self,
        *,
        status_code: int = 200,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)  # type: ignore[arg-type]


# ---------------------------------------------------------------- fixture sanity


def test_fixture_has_four_target_properties(fixture_body: dict[str, Any]) -> None:
    """The Phase 0a-pinned fixture must contain all 4 target properties at the
    expected path. If this fails, Phase 0a's contract is broken — halt.
    """
    profile_data = fixture_body["record"]["profileData"]
    for key in TARGET_PROPS:
        assert key in profile_data, f"missing target property '{key}' in fixture"
    assert isinstance(profile_data["coll_collection_risk_segmentation"], int)
    assert isinstance(profile_data["coll_notification_replied"], str)
    assert isinstance(profile_data["coll_bot_calling"], str)
    assert isinstance(profile_data["dpd"], int)


# ---------------------------------------------------------------- get_profile


def test_get_profile_happy_path(
    mocker, fake_creds_file: Path, fixture_body: dict[str, Any]
) -> None:
    mocker.patch.object(
        requests.Session, "get", return_value=_FakeResponse(body=fixture_body),
    )
    record = ctp.get_profile("8968249", creds_path=fake_creds_file)
    assert record is not None
    pd = record["profileData"]
    for key in TARGET_PROPS:
        assert key in pd
    assert pd["coll_collection_risk_segmentation"] == 8
    assert pd["coll_notification_replied"] == "Agent Calling"
    assert pd["coll_bot_calling"] == "AI Calling"
    assert pd["dpd"] == 29


def test_get_profile_404_returns_none(mocker, fake_creds_file: Path) -> None:
    mocker.patch.object(
        requests.Session, "get", return_value=_FakeResponse(status_code=404),
    )
    assert ctp.get_profile("nonexistent", creds_path=fake_creds_file) is None


def test_get_profile_429_with_retry_after_header(
    mocker, fake_creds_file: Path, fixture_body: dict[str, Any]
) -> None:
    sleep_mock = mocker.patch.object(time, "sleep")
    responses = [
        _FakeResponse(status_code=429, headers={"Retry-After": "1"}),
        _FakeResponse(body=fixture_body),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)

    record = ctp.get_profile("8968249", creds_path=fake_creds_file)
    assert record is not None
    # The Retry-After: 1 must be honored verbatim.
    sleep_calls = [call.args[0] for call in sleep_mock.call_args_list]
    assert 1 in sleep_calls, f"expected sleep(1); got {sleep_calls}"


def test_get_profile_429_without_retry_after_uses_default(
    mocker, fake_creds_file: Path, fixture_body: dict[str, Any]
) -> None:
    sleep_mock = mocker.patch.object(time, "sleep")
    responses = [
        _FakeResponse(status_code=429),                          # no Retry-After
        _FakeResponse(body=fixture_body),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)

    record = ctp.get_profile("8968249", creds_path=fake_creds_file)
    assert record is not None
    sleep_calls = [call.args[0] for call in sleep_mock.call_args_list]
    assert ctp._DEFAULT_429_SLEEP_SEC in sleep_calls, (
        f"expected sleep({ctp._DEFAULT_429_SLEEP_SEC}); got {sleep_calls}"
    )


def test_get_profile_429_max_retries_then_raises(
    mocker, fake_creds_file: Path
) -> None:
    # WS1 contract: persistent 429 exhausts _MAX_RETRIES_ON_429 attempts then
    # _get_profile_classified returns ('error', None) → get_profile raises
    # RuntimeError (NOT HTTPError). Provide exactly _MAX_RETRIES_ON_429 responses.
    mocker.patch.object(time, "sleep")
    mocker.patch.object(
        requests.Session,
        "get",
        side_effect=[_FakeResponse(status_code=429, headers={"Retry-After": "1"})]
        * ctp._MAX_RETRIES_ON_429,
    )
    with pytest.raises(RuntimeError):
        ctp.get_profile("8968249", creds_path=fake_creds_file)


def test_get_profile_raises_on_500(mocker, fake_creds_file: Path) -> None:
    # WS1 contract: a 5xx is classified as 'error' → get_profile raises
    # RuntimeError (the executor treats any exception as the 'error' edge).
    mocker.patch.object(time, "sleep")
    mocker.patch.object(
        requests.Session, "get", return_value=_FakeResponse(status_code=500),
    )
    with pytest.raises(RuntimeError):
        ctp.get_profile("8968249", creds_path=fake_creds_file)


# ---------------------------------------------------------------- bulk_get_profiles


def test_bulk_get_profiles_mixed_success_and_failure(
    mocker, fake_creds_file: Path, fixture_body: dict[str, Any]
) -> None:
    mocker.patch.object(time, "sleep")
    identities = [f"cid_{i}" for i in range(10)]
    # 8 succeed (200 with body), 2 fail (404). 404 -> None.
    def _side_effect(url, **kwargs):
        for failing in ("cid_3", "cid_7"):
            if f"identity={failing}" in url:
                return _FakeResponse(status_code=404)
        return _FakeResponse(body=fixture_body)

    mocker.patch.object(requests.Session, "get", side_effect=_side_effect)

    result = ctp.bulk_get_profiles(
        identities, concurrency=4, creds_path=fake_creds_file,
    )
    assert len(result) == 10
    assert set(result.keys()) == set(identities)
    assert result["cid_3"] is None
    assert result["cid_7"] is None
    for ident in identities:
        if ident in ("cid_3", "cid_7"):
            continue
        assert result[ident] is not None
        assert result[ident]["profileData"]["dpd"] == 29


def test_backoff_tracker_counts_429s_in_window() -> None:
    """_Backoff429Tracker.record_429 returns the rolling count; the 2nd-in-window
    is what triggers a bucket scale-down in _get_profile_classified."""
    t = ctp._Backoff429Tracker(window_sec=60)
    assert t.record_429() == 1
    assert t.record_429() == 2          # 2nd in window → scale-down trigger


def test_backoff_tracker_quiet_for() -> None:
    """quiet_for is True before any 429, and False immediately after one."""
    t = ctp._Backoff429Tracker(window_sec=60)
    assert t.quiet_for(5.0) is True     # no 429 ever recorded
    t.record_429()
    assert t.quiet_for(5.0) is False    # just recorded — not quiet


def test_backoff_tracker_prunes_outside_window(mocker) -> None:
    import itertools
    # record at t=0, record at t=0, then check count after a fresh record at t=120.
    fake_clock = itertools.chain([0.0, 0.0, 120.0, 120.0, 120.0])
    mocker.patch.object(ctp.time, "monotonic", side_effect=lambda: next(fake_clock))
    t = ctp._Backoff429Tracker(window_sec=60)
    t.record_429()  # t=0
    t.record_429()  # t=0
    # At t=120 both prior events are outside the 60s window → only the new one counts.
    assert t.record_429() == 1


def test_token_bucket_burst_then_refill(mocker) -> None:
    """The burst token is instant; the next acquire must WAIT (sleep) until the
    clock advances enough to refill. Clock returns 0 for the first few calls
    then jumps far ahead, so the empty-acquire sleeps then proceeds — fast and
    not dependent on an exact monotonic() call count."""
    calls = {"n": 0}

    def _clock() -> float:
        calls["n"] += 1
        return 0.0 if calls["n"] <= 5 else 100.0

    mocker.patch.object(ctp.time, "monotonic", side_effect=_clock)
    sleep_mock = mocker.patch.object(ctp.time, "sleep")
    b = ctp._TokenBucket(rate_per_sec=1.0, burst=1)
    b.acquire()   # burst token — instant
    b.acquire()   # empty → sleeps until the clock jumps and refills
    assert sleep_mock.called


def test_token_bucket_scale_down_and_ramp_up() -> None:
    b = ctp._TokenBucket(rate_per_sec=8.0, burst=8)
    assert b.rate == 8.0
    b.scale_down(factor=0.5, floor=0.5)
    assert b.rate == 4.0
    b.scale_down(factor=0.5, floor=0.5)
    assert b.rate == 2.0
    b.ramp_up(factor=1.5)
    assert b.rate == 3.0
    # ramp never exceeds base_rate
    for _ in range(10):
        b.ramp_up(factor=2.0)
    assert b.rate == 8.0


def test_token_bucket_rate_floor_no_divide_by_zero() -> None:
    """scale_down respects the floor so acquire()'s deficit/rate never /0."""
    b = ctp._TokenBucket(rate_per_sec=1.0, burst=1)
    for _ in range(50):
        b.scale_down(factor=0.5, floor=0.5)
    assert b.rate >= 0.5


def test_bulk_fetch_status_classifies(mocker, fake_creds_file: Path, fixture_body) -> None:
    """bulk_fetch_status returns (status, record) with found/not_found/error."""
    mocker.patch.object(time, "sleep")

    def _side_effect(url, **kwargs):
        if "identity=missing" in url:
            return _FakeResponse(status_code=404)
        if "identity=boom" in url:
            return _FakeResponse(status_code=500)
        return _FakeResponse(body=fixture_body)

    mocker.patch.object(requests.Session, "get", side_effect=_side_effect)
    out = ctp.bulk_fetch_status(["ok1", "missing", "boom"], concurrency=3, creds_path=fake_creds_file)
    assert out["ok1"][0] == "found" and out["ok1"][1] is not None
    assert out["missing"] == ("not_found", None)
    assert out["boom"] == ("error", None)


def test_bulk_get_profiles_empty_list(mocker, fake_creds_file: Path) -> None:
    assert ctp.bulk_get_profiles([], creds_path=fake_creds_file) == {}


# ---------------------------------------------------------------- set_profile


def test_set_profile_happy_path(mocker, fake_creds_file: Path) -> None:
    mocker.patch.object(
        requests.Session,
        "post",
        return_value=_FakeResponse(body={"status": "success", "processed": 1, "unprocessed": []}),
    )
    result = ctp.set_profile(
        "8968249", {"coll_bot_calling": "ai_vb_calling_highv1"},
        creds_path=fake_creds_file,
    )
    assert isinstance(result, SetResult)
    assert result.success is True
    assert result.error_code is None
    assert result.raw_response["processed"] == 1


def test_set_profile_partial_failure_identity_in_unprocessed(
    mocker, fake_creds_file: Path,
) -> None:
    """CRITICAL TEST — the CT KB hard rule. Top-level `status:"success"` but
    the identity IS in `unprocessed[]` → must return success=False with the
    per-record error code.
    """
    body = {
        "status": "success",
        "processed": 0,
        "unprocessed": [
            {
                "status": "fail",
                "code": 516,
                "error": "Invalid phone number",
                "record": {
                    "identity": "8968249",
                    "type": "profile",
                    "profileData": {"Phone": "bad"},
                },
            }
        ],
    }
    mocker.patch.object(requests.Session, "post", return_value=_FakeResponse(body=body))

    result = ctp.set_profile(
        "8968249", {"Phone": "bad"}, creds_path=fake_creds_file,
    )
    assert result.success is False
    assert result.error_code == 516
    assert result.raw_response == body


def test_set_profile_unprocessed_other_identity_is_success(
    mocker, fake_creds_file: Path,
) -> None:
    """If unprocessed[] contains some OTHER identity (not ours), our record
    succeeded. Per the round-3 KB finding on _classify_delivery.
    """
    body = {
        "status": "success",
        "processed": 1,
        "unprocessed": [
            {
                "status": "fail",
                "code": 523,
                "error": "missing identity",
                "record": {"identity": "some_other_cid", "type": "profile", "profileData": {}},
            }
        ],
    }
    mocker.patch.object(requests.Session, "post", return_value=_FakeResponse(body=body))

    result = ctp.set_profile(
        "8968249", {"foo": "bar"}, creds_path=fake_creds_file,
    )
    assert result.success is True
    assert result.error_code is None


def test_set_profile_top_level_fail(mocker, fake_creds_file: Path) -> None:
    body = {"status": "fail", "error": "Bad request"}
    mocker.patch.object(requests.Session, "post", return_value=_FakeResponse(body=body))

    result = ctp.set_profile(
        "8968249", {"foo": "bar"}, creds_path=fake_creds_file,
    )
    assert result.success is False
    assert result.error_code is None


def test_set_profile_dry_run_appends_query_param(
    mocker, fake_creds_file: Path,
) -> None:
    post_mock = mocker.patch.object(
        requests.Session,
        "post",
        return_value=_FakeResponse(body={"status": "success", "processed": 1, "unprocessed": []}),
    )
    ctp.set_profile(
        "8968249", {"foo": "bar"}, dry_run=True, creds_path=fake_creds_file,
    )
    # First positional arg to post() is the URL.
    call_url = post_mock.call_args.args[0] if post_mock.call_args.args else post_mock.call_args.kwargs["url"]
    assert "dryRun=1" in call_url, f"dry_run did not append ?dryRun=1; url={call_url}"


def test_set_profile_dry_run_default_is_false(
    mocker, fake_creds_file: Path,
) -> None:
    post_mock = mocker.patch.object(
        requests.Session,
        "post",
        return_value=_FakeResponse(body={"status": "success", "processed": 1, "unprocessed": []}),
    )
    ctp.set_profile("8968249", {"foo": "bar"}, creds_path=fake_creds_file)
    call_url = post_mock.call_args.args[0] if post_mock.call_args.args else post_mock.call_args.kwargs["url"]
    assert "dryRun=1" not in call_url


def test_set_profile_empty_properties_rejected(fake_creds_file: Path) -> None:
    with pytest.raises(ValueError):
        ctp.set_profile("8968249", {}, creds_path=fake_creds_file)


# ---------------------------------------------------------------- cred + session caching


def test_creds_cache_reload_on_mtime_change(
    mocker, tmp_path: Path, fixture_body: dict[str, Any],
) -> None:
    creds_path = tmp_path / "creds.json"
    creds_path.write_text(json.dumps({
        "ACCOUNT_ID": "acct_v1", "PASSCODE": "pass_v1",
        "BASE_URL": "https://in1.api.clevertap.com/1",
    }))

    captured_headers: list[dict[str, str]] = []

    def _capture(url, headers=None, **kwargs):
        captured_headers.append(dict(headers or {}))
        return _FakeResponse(body=fixture_body)

    mocker.patch.object(requests.Session, "get", side_effect=_capture)

    ctp.get_profile("cid1", creds_path=creds_path)
    assert captured_headers[-1]["X-CleverTap-Account-Id"] == "acct_v1"

    # Rotate the file. Bump mtime explicitly so the test isn't flaky on a
    # fast filesystem where same-second writes share an mtime.
    creds_path.write_text(json.dumps({
        "ACCOUNT_ID": "acct_v2", "PASSCODE": "pass_v2",
        "BASE_URL": "https://in1.api.clevertap.com/1",
    }))
    import os
    new_mtime = creds_path.stat().st_mtime + 10
    os.utime(creds_path, (new_mtime, new_mtime))

    ctp.get_profile("cid1", creds_path=creds_path)
    assert captured_headers[-1]["X-CleverTap-Account-Id"] == "acct_v2", (
        "creds cache failed to reload after mtime bump"
    )


def test_session_reused_across_calls(
    mocker, fake_creds_file: Path, fixture_body: dict[str, Any],
) -> None:
    """5 sequential get_profile calls should construct requests.Session exactly
    ONCE (the session is pooled with a 10-min TTL).

    Approach: patch GET first so it's a no-op, then patch `requests.Session`
    in the module's namespace to count constructions while still delegating
    to the real class (so the `.get` patched on the real class still fires).
    """
    mocker.patch.object(
        requests.Session, "get", return_value=_FakeResponse(body=fixture_body),
    )

    session_ctor_count = {"n": 0}
    real_session_cls = requests.Session

    def _counting_ctor(*args, **kwargs):
        session_ctor_count["n"] += 1
        return real_session_cls(*args, **kwargs)

    # Patch the name `requests.Session` only as the module references it
    # via `requests.Session(...)` in `_session()`. The earlier .get patch
    # on `real_session_cls` stays effective because instances are still of
    # that class.
    mocker.patch.object(ctp.requests, "Session", side_effect=_counting_ctor)

    for _ in range(5):
        ctp.get_profile("8968249", creds_path=fake_creds_file)

    assert session_ctor_count["n"] == 1, (
        f"expected 1 Session construction across 5 calls; got {session_ctor_count['n']}"
    )


def test_env_var_overrides_creds_path(
    mocker, tmp_path: Path, fixture_body: dict[str, Any], monkeypatch,
) -> None:
    env_creds = tmp_path / "env_creds.json"
    env_creds.write_text(json.dumps({
        "ACCOUNT_ID": "env_acct", "PASSCODE": "env_pass",
        "BASE_URL": "https://in1.api.clevertap.com/1",
    }))
    monkeypatch.setenv("CT_CREDS_FILE", str(env_creds))

    captured: list[dict[str, str]] = []

    def _capture(url, headers=None, **kwargs):
        captured.append(dict(headers or {}))
        return _FakeResponse(body=fixture_body)

    mocker.patch.object(requests.Session, "get", side_effect=_capture)
    # Call without explicit creds_path → must pick up env var.
    ctp.get_profile("8968249")
    assert captured[-1]["X-CleverTap-Account-Id"] == "env_acct"
