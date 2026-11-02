"""A campaign whose fetch fails is tried again, then named in FAILED.

30-31 Oct 2026: Render's 9:00 run kept 12, then 5, of Melior's 26 HeyReach
campaigns (locally all 26), so the stats post had no earlier numbers for
most of them. A failed fetch was printed and the campaign silently left out.
"""
import asyncio
from datetime import datetime, timezone

from smartlead import metrics_collect as mc


class _WS:
    name, api_key = "MELIOR", "k"


def _fake_client(fail_times: dict):
    calls: dict = {}

    class Client:
        def __init__(self, *a):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

        async def list_campaigns(self):
            return [{"id": 1, "name": "Steady", "status": "IN_PROGRESS"},
                    {"id": 2, "name": "Flaky", "status": "IN_PROGRESS"},
                    {"id": 3, "name": "Down", "status": "IN_PROGRESS"}]

        async def get_overall_stats(self, cid, **kw):
            if not kw:                      # the first call of each attempt
                calls[cid] = calls.get(cid, 0) + 1
                if calls[cid] <= fail_times.get(cid, 0):
                    raise RuntimeError("429")
            return {}

        async def get_campaign_leads(self, cid):
            return []

    return Client


def _run(monkeypatch, fail_times):
    monkeypatch.setattr(mc, "discover_heyreach_workspaces", lambda: [_WS()])
    monkeypatch.setattr(mc, "HeyReachClient", _fake_client(fail_times))
    monkeypatch.setattr(mc.cm, "row_client_wanted", lambda *a: True)
    monkeypatch.setattr(mc.cm, "should_include_heyreach_campaign", lambda *a: True)
    monkeypatch.setattr(mc.cm, "heyreach_metric_row", lambda camp, *a, **k: {"campaign": camp["name"]})
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr(mc.asyncio, "sleep", fake_sleep)
    mc.FAILED.clear()
    now = datetime(2026, 11, 2, tzinfo=timezone.utc)
    rows = asyncio.run(mc._heyreach_rows(now, now, now, "[t]"))
    return [r["campaign"] for r in rows], slept


def test_a_campaign_that_fails_once_is_kept_after_a_retry(monkeypatch):
    names, slept = _run(monkeypatch, {2: 1})
    assert sorted(names) == ["Down", "Flaky", "Steady"]
    assert slept == [mc.RETRY_PAUSE] and mc.FAILED == []


def test_a_campaign_that_keeps_failing_is_named_for_the_alert(monkeypatch):
    names, _ = _run(monkeypatch, {3: 2})
    assert sorted(names) == ["Flaky", "Steady"]
    assert mc.FAILED == ["HeyReach MELIOR: Down"]


def test_no_failure_means_no_pause(monkeypatch):
    _, slept = _run(monkeypatch, {})
    assert slept == []
