"""GoDaddy: gates, name rules, purchase ledger (never buys twice), tracker sync.
No network: an httpx MockTransport and a fake client stand in for GoDaddy."""
import json

import httpx
import pytest

from smartlead import godaddy_orders as orders
from smartlead.godaddy import (GoDaddyBlocked, GoDaddyClient, GoDaddyHTTPError,
                               GoDaddyOutcomeUnknown, idempotency_key)
from smartlead.godaddy_asset_sync import plan_sync


class MemCol:
    def __init__(self):
        self.docs = {}

    def update_one(self, q, u, upsert=False):
        pid = q["plan_id"]
        if "$setOnInsert" in u:
            self.docs.setdefault(pid, dict(u["$setOnInsert"]))
        if "$set" in u and pid in self.docs:
            self.docs[pid].update(u["$set"])

    def find_one(self, q, proj=None):
        d = self.docs.get(q["plan_id"])
        return dict(d) if d else None

    def find(self, q, proj=None):
        return [dict(d) for d in self.docs.values()]

    def find_one_and_update(self, q, u):
        d = self.docs.get(q["plan_id"])
        if d and d["status"] in q["status"]["$in"]:
            before = dict(d)
            d.update(u["$set"])
            return before
        return None


class FakeGD:
    """Records every call; registrations succeed unless told otherwise."""

    def __init__(self, *, available=True, price=979, register_error=None, owned=()):
        self.available, self.price, self.register_error = available, price, register_error
        self.owned = set(owned)
        self.registered, self.keys, self.ns = [], [], []

    def check(self, names):
        return [{"domain": n, "available": self.available, "definitive": True,
                 "price_cents": self.price, "renewal_cents": 1499, "fees": []} for n in names]

    def quote(self, domain, period=1):
        return {"quoteToken": "qt", "requiredAgreements": [{"agreementType": "API_DPA"}],
                "items": [{"domain": domain, "period": 1, "price": {"currencyCode": "USD", "value": self.price}}]}

    def register(self, domain, quote, *, agreed_at, key, period=1, approve=False):
        assert approve and agreed_at.endswith("Z")
        self.keys.append(key)
        if self.register_error:
            err, self.register_error = self.register_error, None
            raise err
        self.registered.append(domain)
        self.owned.add(domain)
        return {"registrationId": "r-" + domain, "status": "COMPLETED", "expiresAt": "2027-10-09T00:00:00Z"}

    def registration(self, rid):
        return {"registrationId": rid, "status": "COMPLETED"}

    def domain(self, name):
        if name in self.owned:
            return {"domain": name, "expiresAt": "2027-10-09T00:00:00Z"}
        raise GoDaddyHTTPError("404", 404)

    def set_nameservers(self, domain, ns, *, key, approve=False):
        self.ns.append((domain, tuple(ns)))
        return {}


@pytest.fixture
def store():
    return orders.PlanStore(MemCol())


@pytest.fixture
def spend(monkeypatch):
    monkeypatch.setenv("GODADDY_ALLOW_SPEND", "true")


# ── name rules ──────────────────────────────────────────────────────────────
def test_brand_and_cliche_names_are_refused():
    assert orders.name_problems("preciseleadshub.com")
    assert orders.name_problems("meliorflow.com")
    assert orders.name_problems("outboundcalendar.com")
    assert orders.name_problems("warmmeetings.net")
    assert orders.name_problems("warmmeetings.com") == []


def test_stage_refuses_dirty_or_taken_names(store):
    r = orders.stage_plan(FakeGD(), client="Precise Leads", domains=["preciseleadsteam.com"], store=store)
    assert not r["staged"] and r["problems"]
    r = orders.stage_plan(FakeGD(available=False), client="Precise Leads", domains=["warmmeetings.com"],
                          store=store)
    assert not r["staged"] and r["taken"] == ["warmmeetings.com"]
    r = orders.stage_plan(FakeGD(price=4000), client="Precise Leads", domains=["warmmeetings.com"],
                          store=store)
    assert not r["staged"] and r["over_ceiling"]
    assert store.all() == []


# ── buying ──────────────────────────────────────────────────────────────────
def test_place_needs_approval_and_the_spend_switch(store, monkeypatch):
    gd = FakeGD()
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    with pytest.raises(GoDaddyBlocked):
        orders.place_plan(gd, pid, approve=False, store=store)
    monkeypatch.delenv("GODADDY_ALLOW_SPEND", raising=False)
    with pytest.raises(GoDaddyBlocked):
        orders.place_plan(gd, pid, approve=True, store=store)
    assert gd.registered == []


def test_place_buys_each_domain_once_and_points_nameservers(store, spend):
    gd = FakeGD()
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com", "suremeetings.com"],
                            nameservers=["ns1.sm.com", "ns2.sm.com"], store=store)["plan_id"]
    r = orders.place_plan(gd, pid, approve=True, store=store)
    assert r["status"] == "placed" and sorted(gd.registered) == ["suremeetings.com", "warmmeetings.com"]
    assert len(gd.ns) == 2
    with pytest.raises(GoDaddyBlocked):              # a placed plan cannot be placed again
        orders.place_plan(gd, pid, approve=True, store=store)
    assert len(gd.registered) == 2


def test_lost_answer_is_replayed_with_the_same_key_and_never_double_buys(store, spend):
    gd = FakeGD(register_error=GoDaddyOutcomeUnknown("timeout"))
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    r = orders.place_plan(gd, pid, approve=True, store=store)
    assert r["status"] == "unknown" and gd.registered == []
    first_key = gd.keys[0]
    r = orders.place_plan(gd, pid, approve=True, store=store)   # retry: same key, GoDaddy dedupes
    assert r["status"] == "placed" and gd.keys[1] == first_key


def test_retry_finds_a_domain_that_was_bought_after_all(store, spend):
    gd = FakeGD(register_error=GoDaddyOutcomeUnknown("timeout"))
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    orders.place_plan(gd, pid, approve=True, store=store)
    gd.owned.add("warmmeetings.com")                 # it went through on GoDaddy's side
    r = orders.place_plan(gd, pid, approve=True, store=store)
    assert r["status"] == "placed" and len(gd.keys) == 1   # no second register call


def test_definitive_failure_retries_with_a_new_key(store, spend):
    gd = FakeGD(register_error=GoDaddyHTTPError("422 QUOTE_MISMATCH", 422))
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    assert orders.place_plan(gd, pid, approve=True, store=store)["status"] == "failed"
    assert orders.place_plan(gd, pid, approve=True, store=store)["status"] == "placed"
    assert gd.keys[0] != gd.keys[1]


def test_price_jump_at_quote_time_is_not_bought(store, spend):
    gd = FakeGD()
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    gd.price = 4000
    r = orders.place_plan(gd, pid, approve=True, store=store)
    assert r["status"] == "failed" and gd.registered == []


def test_cannot_drop_a_plan_with_bought_domains(store, spend):
    gd = FakeGD()
    pid = orders.stage_plan(gd, client="Precise Leads", domains=["warmmeetings.com"], store=store)["plan_id"]
    orders.place_plan(gd, pid, approve=True, store=store)
    with pytest.raises(GoDaddyBlocked):
        orders.drop_plan(pid, store=store)
    assert orders.client_by_domain(store) == {"warmmeetings.com": "Precise Leads"}


# ── HTTP client ─────────────────────────────────────────────────────────────
def _client(handler):
    return GoDaddyClient("pat", transport=httpx.MockTransport(handler))


def test_register_sends_consent_and_idempotency_key(monkeypatch):
    monkeypatch.setenv("GODADDY_ALLOW_SPEND", "true")
    seen = {}

    def handler(req):
        seen["key"] = req.headers.get("Idempotency-Key")
        seen["auth"] = req.headers.get("Authorization")
        seen["body"] = json.loads(req.content)
        return httpx.Response(202, json={"registrationId": "r1", "status": "CONFIRMED"})

    quote = {"quoteToken": "qt", "requiredAgreements": [{"agreementType": "API_DPA"}], "items": [{}]}
    with _client(handler) as gd:
        gd.register("a.com", quote, agreed_at="2026-10-09T10:00:00Z", key="k1", approve=True)
    assert seen["key"] == "k1" and seen["auth"] == "Bearer pat"
    assert seen["body"]["consent"] == {"agreedAt": "2026-10-09T10:00:00Z", "agreementTypes": ["API_DPA"]}


def test_register_is_never_retried_on_a_5xx(monkeypatch):
    monkeypatch.setenv("GODADDY_ALLOW_SPEND", "true")
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(503, text="busy")

    quote = {"quoteToken": "qt", "requiredAgreements": [], "items": [{}]}
    with _client(handler) as gd, pytest.raises(GoDaddyOutcomeUnknown):
        gd.register("a.com", quote, agreed_at="t", key="k", approve=True)
    assert len(calls) == 1


def test_premium_fees_are_refused_before_sending():
    with _client(lambda r: httpx.Response(500)) as gd, pytest.raises(GoDaddyBlocked):
        gd.register("a.com", {"quoteToken": "q", "items": [{"fees": [{"type": "PREMIUM"}]}]},
                    agreed_at="t", key="k", approve=True)


def test_check_parses_prices_in_cents():
    def handler(req):
        return httpx.Response(200, json={"items": [{"domain": "a.com", "available": True, "definitive": True,
                                                    "prices": [{"period": 1, "price": {"value": 1199},
                                                                "firstTermPrice": {"value": 979},
                                                                "renewalPrice": {"value": 1499}}]}]})
    with _client(handler) as gd:
        r = gd.check(["a.com"])[0]
    assert (r["price_cents"], r["renewal_cents"], r["available"]) == (979, 1499, True)


def test_idempotency_key_is_stable():
    assert idempotency_key("p", "a.com", "0") == idempotency_key("p", "a.com", "0")
    assert idempotency_key("p", "a.com", "0") != idempotency_key("p", "a.com", "1")


# ── tracker sync ────────────────────────────────────────────────────────────
def test_sync_adds_bot_bought_domains_and_follows_expiry():
    gd_domains = [{"domain": "warmmeetings.com", "status": "ACTIVE", "expiresAt": "2027-10-09T00:00:00Z",
                   "createdAt": "2026-10-09T00:00:00Z", "autoRenew": True},
                  {"domain": "random.com", "status": "ACTIVE", "expiresAt": "2027-01-01T00:00:00Z"}]
    plan = plan_sync(gd_domains, {}, {"warmmeetings.com": "Precise Leads"}, "2026-10-09")
    ins = [o for o in plan["ops"] if o["action"] == "insert"]
    assert [o["name"] for o in ins] == ["warmmeetings.com"]
    assert ins[0]["doc"]["provider"] == "GoDaddy" and ins[0]["doc"]["client"] == "Precise Leads"
    assert plan["skipped"][0]["name"] == "random.com"
