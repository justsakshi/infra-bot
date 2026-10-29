"""Only active clients reach the campaign tabs; past clients never do (Manveen,
29 Oct 2026). Plus the case-blind tab lookup that had frozen BettrData's tabs.
No network."""
import asyncio
import importlib

import gspread
import pytest

from smartlead import campaign_metrics as cm


def test_default_scope_is_the_active_clients_and_the_old_variable_is_ignored(monkeypatch):
    import smartlead.config as config
    monkeypatch.delenv("ACTIVE_CLIENTS", raising=False)
    monkeypatch.setenv("CAMPAIGN_METRICS_CLIENTS", "DARLEAN,BELARDI WONG")   # Render's old value
    try:
        importlib.reload(config)
        assert config.ACTIVE_CLIENTS == {"PRECISE LEADS", "MELIOR", "BETTRDATA"}
        assert config.CAMPAIGN_METRICS_CLIENTS == config.ACTIVE_CLIENTS
    finally:
        monkeypatch.undo()
        importlib.reload(config)


@pytest.mark.parametrize("name,shown", [("BETTRDATA", "BettrData"), ("Bettrdata", "BettrData"),
                                        ("PRECISE_LEADS", "Precise Leads"), ("MELIOR", "Melior"),
                                        ("Someone New", "Someone New")])
def test_one_display_name_per_client(name, shown):
    assert cm.display_client(name) == shown


def test_past_clients_are_out_of_scope():
    active = {"PRECISE LEADS", "MELIOR", "BETTRDATA"}
    for past in ("DARLEAN", "Darlean", "Belardi Wong", "MYTHIC", "OSC"):
        assert not cm.row_client_wanted(past, active)
    for current in ("Precise Leads", "PRECISE_LEADS", "Melior", "BettrData"):
        assert cm.row_client_wanted(current, active)
    assert cm.account_in_scope("PRECISE_LEADS", active)
    assert not cm.account_in_scope("Darlean", active)


class _WS:
    def __init__(self, title):
        self.title = title


class _Sheet:
    def __init__(self, titles):
        self.tabs = [_WS(t) for t in titles]
        self.added = []

    def worksheet(self, title):
        for ws in self.tabs:
            if ws.title == title:
                return ws
        raise gspread.exceptions.WorksheetNotFound(title)

    def worksheets(self):
        return self.tabs

    def add_worksheet(self, title, rows, cols):
        self.added.append(title)
        self.tabs.append(_WS(title))
        return self.tabs[-1]


def _writer(sheet, account=None):
    from smartlead.sheets import SheetsWriter
    w = SheetsWriter.__new__(SheetsWriter)
    w._sh, w.prefix = sheet, (f"{account} - " if account else "")
    return w


def test_tab_lookup_ignores_case_instead_of_failing_to_create_a_duplicate():
    sheet = _Sheet(["BETTRDATA - Campaign Summary", "Campaign Metrics — BETTRDATA"])
    ws = _writer(sheet, "BettrData")._get_or_create_tab("Campaign Summary")
    assert ws.title == "BETTRDATA - Campaign Summary" and sheet.added == []
    ws = _writer(sheet)._get_or_create_shared_tab("Campaign Metrics — BettrData")
    assert ws.title == "Campaign Metrics — BETTRDATA" and sheet.added == []
    _writer(sheet)._get_or_create_shared_tab("Campaign History")
    assert sheet.added == ["Campaign History"]


# ── collector: other clients' and finished campaigns cost no extra calls ─────

class _FakeSmartlead:
    calls: list = []
    campaigns = [
        {"id": 1, "name": "Draft", "status": "DRAFTED"},
        {"id": 2, "name": "OSC thing", "status": "ACTIVE", "client_id": 145916},
        {"id": 6, "name": "Darlean | Events", "status": "ACTIVE", "client_id": None},
        {"id": 3, "name": "Old PL", "status": "COMPLETED", "client_id": None},
        {"id": 4, "name": "Melior live", "status": "ACTIVE", "client_id": 12256},
        {"id": 5, "name": "PL live", "status": "ACTIVE", "client_id": None},
    ]

    def __init__(self, key, name):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def list_campaigns(self):
        return self.campaigns

    async def get_analytics_by_date(self, cid, start, end):
        self.calls.append((cid, "by_date"))
        return {"sent_count": 0, "reply_count": 0}

    async def get_campaign_analytics(self, cid):
        self.calls.append((cid, "analytics"))
        return {"id": cid, "name": f"c{cid}", "status": "ACTIVE", "sent_count": 7,
                "campaign_lead_stats": {"total": 1, "inprogress": 1}}

    async def get_campaign_leads(self, cid):
        self.calls.append((cid, "leads"))
        return []


def test_collector_skips_before_spending_calls(monkeypatch):
    from datetime import datetime, timezone
    from smartlead import metrics_collect as mc
    from smartlead.config import AccountConfig
    _FakeSmartlead.calls = []
    monkeypatch.setattr(mc, "SmartleadClient", _FakeSmartlead)
    monkeypatch.setattr(mc, "discover_accounts", lambda: [
        AccountConfig(name="PRECISE_LEADS", api_key="k", sheet_id="s")])
    monkeypatch.setattr(mc, "CAMPAIGN_METRICS_CLIENTS", {"PRECISE LEADS", "MELIOR", "BETTRDATA"})
    now = datetime(2026, 10, 29, tzinfo=timezone.utc)
    rows = asyncio.run(mc._smartlead_rows(now, now.replace(day=1), now, "[t]"))
    called = {cid for cid, _ in _FakeSmartlead.calls}
    assert called == {"3", "4", "5"}                         # no call for the draft or OSC
    assert [k for cid, k in _FakeSmartlead.calls if cid == "3"] == ["by_date"]   # one look, done
    assert sorted(r["client"] for r in rows) == ["Melior", "Precise Leads"]
    assert all(r["channel"] == "Email" and r["life_sent"] == 7 for r in rows)


@pytest.mark.parametrize("campaign,past", [
    ({"name": "Darlean | Events"}, True),
    ({"name": "BW | UK list"}, True),
    ({"name": "Staff AI - list"}, True),
    ({"name": "anything", "client_id": 145916}, True),          # OSC inside PRECISE_LEADS
    ({"name": "Oscar's outreach"}, False),                       # whole words only
    ({"name": "Rainbow retail"}, False),
    ({"name": "PL | Trade shows"}, False),
    ({"name": "Melior - Campaign X", "client_id": 12256}, False),
    ({"name": "Roundtable | Data owners"}, False),
])
def test_past_client_campaigns_are_recognised(campaign, past):
    from smartlead.client_filter import is_past_client_campaign
    assert is_past_client_campaign(campaign) is past


def test_smartlead_row_counts_each_reply_kind():
    from datetime import datetime, timezone
    leads = [{"lead_category_id": c} for c in (1, 2, 5, 3, 3, 7, 4, 1326, 6, 9, None)]
    row = cm.smartlead_metric_row({"campaign_id": 1, "sent": 50}, leads, 0, 0,
                                  datetime(2026, 10, 29, tzinfo=timezone.utc), {1, 2, 5})
    # 10 categorised leads, 2 of them automatic (OOO 6, bounce 9) -> 8 human replies
    assert row["life_replies"] == 8
    assert (row["life_positive"], row["life_not_interested"], row["life_wrong_person"]) == (3, 2, 1)
