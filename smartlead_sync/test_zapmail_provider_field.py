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


class FakeLists(FakeZ):
    """Renewal-soon and mailbox lists, also identical whatever provider is asked."""
    async def list_renewal_soon(self, page=1, limit=200):
        return {"data": {"domains": [dict(r, expireOn="2026-10-29T00:00:00Z", assignedMailboxesCount=0) for r in ROWS]}}

    async def list_mailboxes(self, contains=None, page=1, limit=50):
        groups = [{"id": r["id"], "domain": r["domain"], "status": "ACTIVE",
                   "mailboxes": [{"id": "mb-" + r["id"], "email": "x@" + r["domain"], "status": "ACTIVE"}]}
                  for r in ROWS if not contains or contains in r["domain"]]
        return {"data": {"domains": groups, "totalPages": 1}}


def _lists(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakeLists())


def test_all_domains_lists_each_domain_once_with_its_own_provider(monkeypatch):
    _lists(monkeypatch)
    rows = asyncio.run(zf.all_domains())
    assert sorted((r["domain"], r["provider"]) for r in rows) == [
        ("a.com", "GOOGLE"), ("b.com", "GOOGLE"), ("out.com", "MICROSOFT")]


def test_all_mailboxes_lists_each_mailbox_once_with_its_domains_provider(monkeypatch):
    _lists(monkeypatch)
    rows = asyncio.run(zf.all_mailboxes())
    assert sorted((r["email"], r["provider"]) for r in rows) == [
        ("x@a.com", "GOOGLE"), ("x@b.com", "GOOGLE"), ("x@out.com", "MICROSOFT")]


def test_renewals_list_each_domain_once(monkeypatch):
    _lists(monkeypatch)
    import smartlead.zapmail_asset_sync as sync
    import smartlead.zapmail_clients as clients
    monkeypatch.setattr(sync, "read_tracker", lambda: {})
    monkeypatch.setattr(clients, "infer_client", lambda d, a, t: None)
    rows = asyncio.run(zf.renewals_asof())
    assert sorted((r["domain"], r["provider"]) for r in rows) == [
        ("a.com", "GOOGLE"), ("b.com", "GOOGLE"), ("out.com", "MICROSOFT")]


class FakeSubs(FakeLists):
    """/v2/subscriptions answers the same whatever provider is asked (live 2026-10-13)."""
    async def _request(self, method, path, json=None, **kw):
        if path == "/v2/subscriptions":
            return {"data": [{"subscriptionId": "sub_g", "subscriptionStatus": "ACTIVE", "price": 9.75,
                              "totalMailboxQuantity": 3, "periodEnd": "2026-10-27T00:00:00Z", "plan": "x"},
                             {"subscriptionId": "sub_m", "subscriptionStatus": "ACTIVE", "price": 9.75,
                              "totalMailboxQuantity": 3, "periodEnd": "2026-10-28T00:00:00Z", "plan": "x"}]}
        if path == "/v2/subscriptions/mailboxes":
            dom = "a.com" if json["subscriptionId"] == "sub_g" else "out.com"
            return {"data": [{"username": u, "domain": dom} for u in ("x", "y", "z")]}
        raise AssertionError(path)

    async def prewarmed_subscriptions(self):
        return {"data": {"subscriptions": []}}


def test_each_inbox_subscription_once_with_its_inboxes_provider(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakeSubs())
    import smartlead.zapmail_clients as clients
    monkeypatch.setattr(clients, "infer_client", lambda d, a: "Bettrdata")
    rows = asyncio.run(zf.subscriptions_billing(days=60, today="2026-10-13"))
    assert sorted((r["subscription_id"], r["provider"], r["domains"][0]) for r in rows) == [
        ("sub_g", "GOOGLE", "a.com"), ("sub_m", "MICROSOFT", "out.com")]


PRE = [{"subscriptionId": "sub_g", "subscriptionStatus": "ACTIVE", "price": 105, "totalMailboxQuantity": 15,
        "periodEnd": "2026-10-18T00:00:00Z", "serviceProvider": "GOOGLE", "plan": "Growth Addon"},
       {"subscriptionId": "sub_o", "subscriptionStatus": "ACTIVE", "price": 84, "totalMailboxQuantity": 12,
        "periodEnd": "2026-10-14T00:00:00Z", "serviceProvider": "MICROSOFT", "plan": "Growth Addon"}]


class FakePre(FakeSubs):
    """Pre-warmed plans: both plans whatever provider is asked; each says its own serviceProvider."""
    async def prewarmed_subscriptions(self):
        return {"data": {"subscriptions": PRE}}

    async def _request(self, method, path, json=None, **kw):
        if path == "/v2/subscriptions":
            return {"data": []}
        return await super()._request(method, path, json=json, **kw)

    async def list_mailboxes(self, contains=None, page=1, limit=50):
        return {"data": {"domains": [], "totalAssignedPreWarmedUpMailboxes": 27}}

    async def prewarmed_count(self):
        return {"data": {"google": 460, "microsoft": 52}}

    async def prewarmed_domains(self, page=1, limit=10):
        return {"data": {"domains": []}}


def test_prewarmed_plans_once_with_their_own_provider(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakePre())
    rows = asyncio.run(zf.subscriptions_billing(days=60, today="2026-10-13"))
    assert sorted((r["provider"], r["mailboxes"]) for r in rows) == [("GOOGLE", 15), ("MICROSOFT", 12)]


def test_prewarmed_slots_per_provider(monkeypatch):
    monkeypatch.setattr(zf, "discover_zapmail_accounts", lambda: [ACC])
    monkeypatch.setattr(zf, "_client", lambda acc, provider: FakePre())
    acc = asyncio.run(zf.prewarmed_overview())["accounts"]["PRECISE_LEADS"]
    assert acc["GOOGLE"]["slots"] == 15 and acc["MICROSOFT"]["slots"] == 12
