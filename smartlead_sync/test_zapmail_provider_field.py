"""Zapmail ignores the x-service-provider header on list calls (live 2026-10-13:
the MICROSOFT request returned the same 213 domains as GOOGLE). Each domain row
carries `serviceProvider`; that is the truth. No network."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from smartlead import domain_lifecycle as dl
from smartlead import zapmail_fleet as zf

ROWS = [{"id": "g1", "domain": "a.com", "serviceProvider": "GOOGLE", "status": "ACTIVE"},
        {"id": "g2", "domain": "b.com", "serviceProvider": "GOOGLE", "status": "ACTIVE"},
        {"id": "m1", "domain": "out.com", "serviceProvider": "MICROSOFT", "status": "ACTIVE"}]


class FakeZ:
    """Answers the same whatever provider it was opened with (today's behaviour)."""
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def list_domains(self, contains=None, page=1, limit=50):
        rows = [r for r in ROWS if not contains or contains in r["domain"]]
        rows = rows[(page - 1) * limit: page * limit]
        return {"data": {"domains": rows, "totalSearchedCount": len(ROWS)}}

    async def list_mailboxes(self, contains=None, page=1, limit=50):
        return {"data": {"domains": [], "totalActiveMailboxes": 69}}

    async def get_user(self):
        return {"data": {"email": "x@y.z"}}

    async def get_wallet_balance(self):
        return {"walletBalance": 38.5}

    async def placement_credits(self):
        return {"data": {"totalAvailableCredits": 31}}

    async def get_domain_health(self, _id):
        return {"data": {}}


ACC = SimpleNamespace(name="PRECISE_LEADS", api_key="k", workspace_key=None)


def test_locate_domain_reports_the_rows_own_provider_once(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakeZ())

    async def boxes(z, d):
        return []
    monkeypatch.setattr(zf, "_mailboxes_on", boxes)
    hits = asyncio.run(zf.locate_domain("out.com"))
    assert [(h["provider"], h["domain_id"]) for h in hits] == [("MICROSOFT", "m1")]


def test_domain_provider_uses_the_row(monkeypatch):
    @asynccontextmanager
    async def fake_open(client, provider=None):
        yield FakeZ()
    monkeypatch.setattr(dl, "open_client", fake_open)
    assert asyncio.run(dl.domain_provider("out.com", client="Precise Leads")) == "MICROSOFT"
    assert asyncio.run(dl.domain_provider("a.com", client="Precise Leads")) == "GOOGLE"


def test_fleet_status_counts_domains_by_their_provider(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakeZ())
    s = asyncio.run(zf.fleet_status())["PRECISE_LEADS"]
    assert s["domains"] == {"GOOGLE": 2, "MICROSOFT": 1}
    assert s["active_mailboxes_total"] == 69
