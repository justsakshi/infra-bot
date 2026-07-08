"""stale_campaign_names must (a) classify correctly and (b) only fetch leads
for campaigns with zero sends in the window — the perf contract."""
import asyncio
from datetime import date

from smartlead.campaign_freshness import stale_campaign_names


class FakeClient:
    """Stub of SmartleadClient recording which campaigns had leads fetched."""

    def __init__(self, campaigns, sent_by_id, leads_by_id):
        self._campaigns = campaigns
        self._sent = sent_by_id
        self._leads = leads_by_id
        self.leads_fetched_for: list[str] = []

    async def list_campaigns(self):
        return self._campaigns

    async def get_analytics_by_date(self, cid, start, end):
        return {"sent_count": self._sent.get(str(cid), 0)}

    async def get_campaign_leads(self, cid):
        self.leads_fetched_for.append(str(cid))
        return self._leads.get(str(cid), [])


def test_sending_campaign_skips_lead_fetch_and_is_fresh():
    c = FakeClient(
        campaigns=[{"id": 1, "name": "sender", "status": "ACTIVE"}],
        sent_by_id={"1": 500},
        leads_by_id={},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()
    assert c.leads_fetched_for == []  # perf contract: no lead fetch


def test_zero_sent_with_old_leads_is_stale_and_fetches_leads():
    c = FakeClient(
        campaigns=[{"id": 2, "name": "zombie", "status": "ACTIVE"}],
        sent_by_id={"2": 0},
        leads_by_id={"2": [{"created_at": "2026-05-01T00:00:00Z"}]},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == {"zombie"}
    assert c.leads_fetched_for == ["2"]


def test_zero_sent_with_fresh_leads_is_not_stale():
    c = FakeClient(
        campaigns=[{"id": 3, "name": "just-loaded", "status": "ACTIVE"}],
        sent_by_id={"3": 0},
        leads_by_id={"3": [{"created_at": "2026-07-07T00:00:00Z"}]},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()


def test_non_active_campaigns_ignored_entirely():
    c = FakeClient(
        campaigns=[{"id": 4, "name": "done", "status": "COMPLETED"}],
        sent_by_id={},
        leads_by_id={},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()
    assert c.leads_fetched_for == []
