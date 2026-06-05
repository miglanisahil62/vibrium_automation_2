"""Tests for WS9 workflow_whatsapp.py — shared-cap WhatsApp parity dispatcher.

No live CleverTap: clevertap_trigger.trigger is injected via a fake module.
Covers candidate selection (WA_Available + promise dispositions only), the
shared whatsapp_log caps (1/day, 3/7d, global), the RBI window guard, dry-run,
and that fires are logged to the SHARED vibrium.db whatsapp_log.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import scripts.workflow_whatsapp as ww  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")


def _ts(days_ago=0, minutes_ago=0):
    return (datetime.now(IST).replace(tzinfo=None)
            - timedelta(days=days_ago, minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _today():
    return datetime.now(IST).replace(tzinfo=None).strftime("%Y-%m-%d")


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    wf = tmp_path / "workflow.db"
    vib = tmp_path / "vibrium.db"
    cfg = tmp_path / "config.json"
    # minimal workflow.db: wf_decision_log + workflow_runs
    w = sqlite3.connect(str(wf))
    w.executescript("""
        CREATE TABLE wf_decision_log (id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ist TEXT, customer_id TEXT, run_id INTEGER, action_class TEXT);
        CREATE TABLE workflow_runs (id INTEGER PRIMARY KEY, customer_id TEXT,
            scratchpad_json TEXT);
    """)
    w.commit(); w.close()
    # minimal vibrium.db: shared whatsapp_log (matches real schema: _row_index
    # AUTOINCREMENT + shadow_mode column)
    v = sqlite3.connect(str(vib))
    v.execute("CREATE TABLE whatsapp_log (_row_index INTEGER PRIMARY KEY AUTOINCREMENT, "
              "ts_ist TEXT, customer_id TEXT, campaign_id TEXT, trigger_origin TEXT, "
              "status TEXT, http_status TEXT, response_body TEXT, shadow_mode TEXT)")
    v.commit(); v.close()
    cfg.write_text(json.dumps({
        "whatsapp": {"campaign_id": 1779082096, "contact_type": "whatsapp",
                     "daily_cap_global": 2000, "per_customer_daily_cap": 1,
                     "per_customer_weekly_cap": 3, "fire_hour_min": 8, "fire_hour_max": 19},
        "clevertap": {"bot_id": "VB00000005", "credentials_path": "/dev/null"},
    }))
    monkeypatch.setattr(ww, "_WORKFLOW_DB", str(wf))
    monkeypatch.setattr(ww, "_VIBRIUM_DB", str(vib))
    monkeypatch.setattr(ww, "_ADHOC_CONFIG", str(cfg))
    monkeypatch.setattr(ww, "_WA_LOCK_PATH", str(tmp_path / "wa.lock"))  # temp, not /home/ubuntu
    return wf, vib, cfg


def _add_disp(wf, cid, action_class, wa="WA_Available", when=None):
    c = sqlite3.connect(str(wf))
    with c:
        rid = cid
        c.execute("INSERT OR IGNORE INTO workflow_runs (id, customer_id, scratchpad_json) VALUES (?,?,?)",
                  (rid, str(cid), json.dumps({"coll_notification_replied": wa})))
        c.execute("INSERT INTO wf_decision_log (ts_ist, customer_id, run_id, action_class) VALUES (?,?,?,?)",
                  (when or _ts(), str(cid), rid, action_class))
    c.close()


def _add_wa_log(vib, cid, when, status="success"):
    c = sqlite3.connect(str(vib))
    with c:  # _row_index AUTOINCREMENT — omit it
        c.execute("INSERT INTO whatsapp_log (ts_ist, customer_id, campaign_id, "
                  "trigger_origin, status, shadow_mode) VALUES (?,?,?,?,?,?)",
                  (when, str(cid), "1779082096", "X", status, "no"))
    c.close()


def _fake_trigger(monkeypatch, recorder, status="success"):
    fake = types.ModuleType("external.vibrium_automation_scripts.clevertap_trigger")
    def trigger(customer_id, action_class, **kw):
        recorder.append((customer_id, action_class, kw.get("campaign_id")))
        return {"status": status, "http_status": 200, "body": {}}
    fake.trigger = trigger
    monkeypatch.setitem(sys.modules, "external.vibrium_automation_scripts.clevertap_trigger", fake)


def _at_hour(monkeypatch, hour):
    base = datetime.now(IST).replace(tzinfo=None).replace(hour=hour, minute=0, second=0)
    monkeypatch.setattr(ww, "_now", lambda: base)


def test_fires_for_wa_available_promises(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 101, "PTP_CALL")
    _add_disp(wf, 102, "AGREE_EOD_CALL")
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["fired"] == 2
    assert sorted(c[0] for c in rec) == [101, 102]
    # P0-1: assert the fired campaign == the CONFIG value (not a hardcoded
    # literal) — validates "fires whatever config says", can't mask divergence.
    cfg_campaign = json.loads(Path(ww._ADHOC_CONFIG).read_text())["whatsapp"]["campaign_id"]
    assert all(c[2] == cfg_campaign for c in rec)
    # logged to the shared whatsapp_log
    v = sqlite3.connect(str(vib))
    n = v.execute("SELECT COUNT(*) FROM whatsapp_log WHERE trigger_origin LIKE 'WF_%'").fetchone()[0]
    v.close()
    assert n == 2


def test_skips_not_wa_available(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 201, "PTP_CALL", wa="WA_Unavailable")
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["candidates"] == 0 and stats["fired"] == 0 and rec == []


def test_skips_non_promise_dispositions(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 301, "RETRY")        # not a promise
    _add_disp(wf, 302, "NOOP")
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["candidates"] == 0 and rec == []


def test_daily_cap_shared_log(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 401, "PTP_CALL")
    _add_wa_log(vib, 401, _ts())       # already WA'd today (e.g. by adhoc) → skip
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["skipped_cap"] == 1 and stats["fired"] == 0 and rec == []


def test_weekly_cap(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 501, "PTP_CALL")
    for d in (1, 2, 3):                 # 3 in last 7d → weekly cap hit
        _add_wa_log(vib, 501, _ts(days_ago=d))
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["skipped_cap"] == 1 and rec == []


def test_outside_window_skips_all(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 20)           # 20:00 > 19 → outside
    _add_disp(wf, 601, "PTP_CALL")
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=False)
    assert stats["skipped_window"] == 1 and rec == []


def test_global_cap_engages_in_shadow(dbs, monkeypatch):
    # P0-1 regression: the global daily cap must engage even in SHADOW mode
    # (counts success+shadow). With cap=1 + 2 shadow candidates → only 1 logged.
    wf, vib, cfg = dbs
    c = json.loads(Path(cfg).read_text()); c["whatsapp"]["daily_cap_global"] = 1
    Path(cfg).write_text(json.dumps(c))
    monkeypatch.setenv("WF_WA_SHADOW", "1")
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 801, "PTP_CALL")
    _add_disp(wf, 802, "PTP_CALL")
    rec = []; _fake_trigger(monkeypatch, rec, status="shadow")
    stats = ww.run(dry_run=False)
    assert stats["shadow"] == 1            # global cap stopped the 2nd
    v = sqlite3.connect(str(vib))
    n = v.execute("SELECT COUNT(*) FROM whatsapp_log").fetchone()[0]
    v.close()
    assert n == 1


def test_dry_run_fires_nothing(dbs, monkeypatch):
    wf, vib, _ = dbs
    _at_hour(monkeypatch, 12)
    _add_disp(wf, 701, "PTP_CALL")
    rec = []; _fake_trigger(monkeypatch, rec)
    stats = ww.run(dry_run=True)
    assert rec == []
    v = sqlite3.connect(str(vib))
    assert v.execute("SELECT COUNT(*) FROM whatsapp_log").fetchone()[0] == 0
    v.close()
