"""Tests for every guard between a command and a Zapmail charge.

No network, no Mongo, no spend: HTTP goes through ``httpx.MockTransport``,
the ledger is an in-memory fake, and the purchase/availability calls are
monkeypatched. Run with:

    .venv\\Scripts\\python.exe -m pytest test_zapmail_spend_guards.py -q
"""
from __future__ import annotations

import asyncio
import copy
import os
from datetime import date

import httpx
import pytest

from smartlead import domain_batch, domain_purchase
from smartlead.domain_availability import _cached
from smartlead.domain_batch import BatchStore, execute_one, reconcile_one, stage_batches
from smartlead.domain_lifecycle import generate_identities, identities_from_names
from smartlead.domain_purchase import PurchaseItem, PurchasePlan, execute_purchase, verify_prices
from smartlead.zapmail import (
    ZapmailClient, ZapmailHTTPError, ZapmailOutcomeUnknown, ZapmailSpendBlocked,
)
from smartlead.zapmail_accounts import resolve_account_for_client


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """No real key, map, or kill-switch leaks in from the machine."""
    for k in list(os.environ):
        if k.startswith("ZAPMAIL_"):
            monkeypatch.delenv(k, raising=False)


def spend_on(monkeypatch):
    monkeypatch.setenv("ZAPMAIL_ALLOW_SPEND", "true")


# ── client gate ──────────────────────────────────────────────────────────────

def test_spend_refused_without_approve(monkeypatch):
    spend_on(monkeypatch)
    with pytest.raises(ZapmailSpendBlocked):
        run(ZapmailClient(api_key="k").buy_domains(["a.com"]))


def test_spend_refused_without_kill_switch():
    with pytest.raises(ZapmailSpendBlocked, match="ZAPMAIL_ALLOW_SPEND"):
        run(ZapmailClient(api_key="k").buy_domains(["a.com"], approve=True))


@pytest.mark.parametrize("call", [
    lambda z: z.renew_domains(["id1"], approve=True),
    lambda z: z.quick_setup(["a.com"], {}, approve=True),
    lambda z: z.purchase_placement_test(placement_type="ONE_TIME", test_name="t",
                                        mailbox_ids=["m"], seed_accounts=["google"],
                                        approve=True),
    lambda z: z.purchase_dns_shield(plan_type="MONTHLY", approve=True),
    lambda z: z.purchase_prewarmed("X", approve=True),
    lambda z: z.purchase_aged_domains(["a.com"], approve=True),
])
def test_every_spend_method_honours_kill_switch(call):
    with pytest.raises(ZapmailSpendBlocked):
        run(call(ZapmailClient(api_key="k")))


def test_approved_spend_is_single_shot(monkeypatch):
    spend_on(monkeypatch)
    z = ZapmailClient(api_key="k")
    seen = {}

    async def fake_request(method, path, **kw):
        seen.update(path=path, retry=kw.get("retry"))
        return {"ok": True}

    z._request = fake_request
    run(z.buy_domains(["a.com"], approve=True))
    assert seen == {"path": "/v2/domains/buy", "retry": False}


def test_renew_requires_explicit_ids(monkeypatch):
    spend_on(monkeypatch)
    with pytest.raises(ZapmailSpendBlocked):
        run(ZapmailClient(api_key="k").renew_domains([], approve=True))


def test_write_gate_still_requires_approve():
    with pytest.raises(ZapmailSpendBlocked):
        run(ZapmailClient(api_key="k").connect_domains(["a.com"]))


# ── headers ──────────────────────────────────────────────────────────────────

def test_provider_header_only_when_needed():
    z = ZapmailClient(api_key="k")
    assert "x-service-provider" not in z._headers()
    assert z._headers(service_provider=True)["x-service-provider"] == "GOOGLE"
    ms = ZapmailClient(api_key="k", service_provider="microsoft")
    assert ms._headers()["x-service-provider"] == "MICROSOFT"


# ── error classification ─────────────────────────────────────────────────────

def _client_with(handler) -> ZapmailClient:
    z = ZapmailClient(api_key="k")
    z._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return z


def test_4xx_is_definitive():
    z = _client_with(lambda req: httpx.Response(400, json={"error": "bad"}))
    with pytest.raises(ZapmailHTTPError) as ei:
        run(z._request("POST", "/v2/domains/buy", json={}, retry=False))
    assert ei.value.status_code == 400


def test_5xx_on_single_shot_is_outcome_unknown():
    z = _client_with(lambda req: httpx.Response(502, text="gateway"))
    with pytest.raises(ZapmailOutcomeUnknown):
        run(z._request("POST", "/v2/domains/buy", json={}, retry=False))


def test_network_error_is_outcome_unknown():
    def boom(req):
        raise httpx.ReadTimeout("slow", request=req)
    z = _client_with(boom)
    with pytest.raises(ZapmailOutcomeUnknown):
        run(z._request("POST", "/v2/domains/buy", json={}, retry=False))


# ── account resolution ───────────────────────────────────────────────────────

def test_strict_resolution_never_falls_back(monkeypatch):
    monkeypatch.setenv("ZAPMAIL_API_KEY", "primary-key")
    # Mapped client whose own key is missing:
    assert resolve_account_for_client("Bettrdata", strict=True) is None
    # Typo / unmapped client:
    assert resolve_account_for_client("Betrdata", strict=True) is None
    # Non-strict (read paths) still falls back, as before:
    assert resolve_account_for_client("Betrdata").api_key == "primary-key"
    # Blank client = explicit primary:
    assert resolve_account_for_client("", strict=True).api_key == "primary-key"


def test_strict_resolution_finds_named_account(monkeypatch):
    monkeypatch.setenv("ZAPMAIL_API_KEY_PRECISE_LEADS", "pl-key")
    assert resolve_account_for_client("Bettrdata", strict=True).api_key == "pl-key"
    monkeypatch.setenv("ZAPMAIL_CLIENT_ACCOUNTS", "Acme=Acme Co")
    monkeypatch.setenv("ZAPMAIL_API_KEY_ACME_CO", "acme-key")
    assert resolve_account_for_client("acme", strict=True).api_key == "acme-key"


def test_primary_can_be_named(monkeypatch):
    monkeypatch.setenv("ZAPMAIL_API_KEY", "pl-key")
    monkeypatch.setenv("ZAPMAIL_PRIMARY_ACCOUNT_NAME", "Precise Leads")
    assert resolve_account_for_client("Melior", strict=True).api_key == "pl-key"


# ── execute_purchase + live re-check ─────────────────────────────────────────

def _fake_avail(table):
    async def fake(domains, **kw):
        assert kw.get("use_cache") is False, "pre-buy check must bypass the cache"
        return {d: table.get(d, (None, None)) for d in domains}
    return fake


def test_execute_purchase_refuses_unmapped_client(monkeypatch):
    spend_on(monkeypatch)
    monkeypatch.setenv("ZAPMAIL_API_KEY", "primary-key")
    with pytest.raises(ZapmailSpendBlocked, match="does not resolve"):
        run(execute_purchase(["a.com"], approve=True, client="Betrdata"))


@pytest.mark.parametrize("row,expected,match", [
    ((False, None), None, "taken"),
    ((None, None), None, "unknown"),
    ((True, None), None, "no price"),
    ((True, 2000.0), None, "ceiling"),
    ((True, 14.99), 12.99, "staged"),
])
def test_verify_prices_refuses(monkeypatch, row, expected, match):
    monkeypatch.setattr(domain_purchase, "check_availability_bulk",
                        _fake_avail({"a.com": row}))
    with pytest.raises(ZapmailSpendBlocked, match=match):
        run(verify_prices(["a.com"], api_key="k",
                          expected_prices={"a.com": expected} if expected else None))


def test_verify_prices_passes(monkeypatch):
    monkeypatch.setattr(domain_purchase, "check_availability_bulk",
                        _fake_avail({"a.com": (True, 12.99)}))
    assert run(verify_prices(["a.com"], api_key="k",
                             expected_prices={"a.com": 12.99})) == {"a.com": 12.99}


# ── Zapmail support answers (2026-09-29) ─────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("12.99", 12.99), ("$1,200.50", 1200.5), ("NaN", None), ("nan", None),
    ("inf", None), ("-5", None), ("", None), (None, None), ("abc", None),
])
def test_parse_price_rejects_nan_and_junk(raw, expected):
    from smartlead.domain_availability import parse_price
    assert parse_price(raw) == expected


def test_bulk_rows_nan_price_is_unknown_not_buyable():
    from smartlead.domain_availability import _parse_bulk_rows
    out = _parse_bulk_rows({"domains": [
        {"domainName": "x.io", "status": "AVAILABLE", "domainPrice": "NaN"}]})
    assert out["x.io"] == (True, None)   # verify_prices then refuses "no price"


class _FakeBuyZ:
    def __init__(self, balance):
        self.balance, self.bought = balance, None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def get_wallet_balance(self):
        return {"walletBalance": self.balance, "autoRechargeEnabled": True}

    async def buy_domains(self, domains, **kw):
        self.bought = domains
        return {"message": "Domains purchased", "invoiceLink": "https://inv.example/1"}


def _buy_setup(monkeypatch, balance):
    spend_on(monkeypatch)
    monkeypatch.setenv("ZAPMAIL_API_KEY_PRECISE_LEADS", "pl-key")
    monkeypatch.setattr(domain_purchase, "check_availability_bulk",
                        _fake_avail({"a.com": (True, 12.99), "b.com": (True, 12.99)}))
    fake = _FakeBuyZ(balance)
    monkeypatch.setattr(domain_purchase, "ZapmailClient", lambda **kw: fake)
    return fake


def test_short_wallet_refuses_before_buy(monkeypatch):
    fake = _buy_setup(monkeypatch, balance=20)
    with pytest.raises(ZapmailSpendBlocked, match=r"wallet has \$20.00.*costs \$25.98"):
        run(execute_purchase(["a.com", "b.com"], approve=True, client="Bettrdata"))
    assert fake.bought is None   # /buy never called


def test_covered_wallet_buys_and_records_invoice(monkeypatch):
    fake = _buy_setup(monkeypatch, balance=64)
    res = run(execute_purchase(["a.com", "b.com"], approve=True, client="Bettrdata"))
    assert fake.bought == ["a.com", "b.com"]
    assert res["total_usd"] == 25.98
    assert res["invoice_link"] == "https://inv.example/1"
    assert res["message"] == "Domains purchased"


def test_multi_year_total_checked_against_wallet(monkeypatch):
    _buy_setup(monkeypatch, balance=30)
    with pytest.raises(ZapmailSpendBlocked, match=r"costs \$51.96"):
        run(execute_purchase(["a.com", "b.com"], years=2, approve=True, client="Bettrdata"))


# ── ledger: execute_one ──────────────────────────────────────────────────────

class FakeCollection:
    def __init__(self, docs=()):
        self.docs = {d["batch_id"]: copy.deepcopy(d) for d in docs}

    @staticmethod
    def _match(doc, q):
        for k, v in q.items():
            if isinstance(v, dict) and "$in" in v:
                if doc.get(k) not in v["$in"]:
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def find_one(self, q, projection=None):
        for d in self.docs.values():
            if self._match(d, q):
                return copy.deepcopy(d)
        return None

    def find(self, q, projection=None):
        return [copy.deepcopy(d) for d in self.docs.values() if self._match(d, q)]

    def find_one_and_update(self, q, update, projection=None, return_document=None):
        for d in self.docs.values():
            if self._match(d, q):
                before = copy.deepcopy(d)
                d.update(update["$set"])
                return before
        return None

    def update_one(self, q, update):
        for d in self.docs.values():
            if self._match(d, q):
                d.update(update["$set"])
                return

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            update = getattr(op, "_doc", None) or getattr(op, "_update")
            doc = update["$setOnInsert"]
            self.docs.setdefault(doc["batch_id"], copy.deepcopy(doc))


def _batch(**over):
    doc = {"batch_id": "b1", "status": "planned", "client": "Bettrdata",
           "domains": ["a.com", "b.com"], "earliest_date": "2026-09-28",
           "prices": {"a.com": 12.99, "b.com": 12.99}}
    doc.update(over)
    return doc


def _store(**over):
    return BatchStore(collection=FakeCollection([_batch(**over)]))


TODAY = date(2026, 9, 28)


def test_execute_one_needs_kill_switch():
    with pytest.raises(ZapmailSpendBlocked):
        run(execute_one("b1", approve=True, store=_store(), today=TODAY))


@pytest.mark.parametrize("over,kwargs,match", [
    ({"status": "purchased"}, {}, "already purchased"),
    ({"status": "unknown"}, {}, "reconcile"),
    ({"status": "in_progress"}, {}, "reconcile"),
    ({"status": "partial"}, {}, "reconcile"),
    ({"earliest_date": "2026-10-02"}, {}, "scheduled for 2026-10-02"),
    ({}, {"client": "Precise Leads"}, "planned for client"),
])
def test_execute_one_refusals(monkeypatch, over, kwargs, match):
    spend_on(monkeypatch)
    called = []
    monkeypatch.setattr(domain_batch, "execute_purchase",
                        lambda *a, **k: called.append(1))
    with pytest.raises(ZapmailSpendBlocked, match=match):
        run(execute_one("b1", approve=True, store=_store(**over), today=TODAY, **kwargs))
    assert not called


def test_execute_one_success_uses_ledger_client_and_prices(monkeypatch):
    spend_on(monkeypatch)
    seen = {}

    async def fake_purchase(domains, **kw):
        seen.update(domains=domains, **kw)
        return {"response": "ok"}

    monkeypatch.setattr(domain_batch, "execute_purchase", fake_purchase)
    store = _store()
    res = run(execute_one("b1", approve=True, client="bettrdata", store=store, today=TODAY))
    assert res["status"] == "purchased"
    assert seen["client"] == "Bettrdata"
    assert seen["expected_prices"] == {"a.com": 12.99, "b.com": 12.99}
    assert store.get("b1")["status"] == "purchased"


def test_execute_one_cannot_buy_twice(monkeypatch):
    spend_on(monkeypatch)

    async def fake_purchase(domains, **kw):
        return {}

    monkeypatch.setattr(domain_batch, "execute_purchase", fake_purchase)
    store = _store()
    run(execute_one("b1", approve=True, store=store, today=TODAY))
    with pytest.raises(ZapmailSpendBlocked, match="already purchased"):
        run(execute_one("b1", approve=True, store=store, today=TODAY))


def test_claim_is_exclusive():
    store = _store()
    assert store.claim("b1")["status"] == "planned"
    assert store.claim("b1") is None  # second concurrent run loses


@pytest.mark.parametrize("exc,status", [
    (ZapmailOutcomeUnknown("timeout"), "unknown"),
    (RuntimeError("weird"), "unknown"),
    (ZapmailHTTPError("402 no funds", 402), "failed"),
])
def test_execute_one_records_outcome(monkeypatch, exc, status):
    spend_on(monkeypatch)

    async def fake_purchase(domains, **kw):
        raise exc

    monkeypatch.setattr(domain_batch, "execute_purchase", fake_purchase)
    store = _store()
    with pytest.raises(type(exc)):
        run(execute_one("b1", approve=True, store=store, today=TODAY))
    assert store.get("b1")["status"] == status


def test_preflight_refusal_releases_claim(monkeypatch):
    spend_on(monkeypatch)

    async def fake_purchase(domains, **kw):
        raise ZapmailSpendBlocked("price moved")

    monkeypatch.setattr(domain_batch, "execute_purchase", fake_purchase)
    store = _store(status="failed")
    with pytest.raises(ZapmailSpendBlocked):
        run(execute_one("b1", approve=True, store=store, today=TODAY))
    assert store.get("b1")["status"] == "failed"  # back to where it was


# ── reconcile ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("owned,status", [
    ({"a.com", "b.com"}, "purchased"),
    (set(), "failed"),
    ({"a.com"}, "partial"),
])
def test_reconcile(monkeypatch, owned, status):
    monkeypatch.setenv("ZAPMAIL_API_KEY_PRECISE_LEADS", "pl-key")

    async def fake_owned(z, domains):
        return owned

    monkeypatch.setattr(domain_batch, "_owned_domains", fake_owned)
    store = _store(status="unknown")
    res = run(reconcile_one("b1", store=store))
    assert res["status"] == status == store.get("b1")["status"]


# ── staging ──────────────────────────────────────────────────────────────────

def test_stage_plans_only_buyable_and_unheld(monkeypatch):
    held = FakeCollection([_batch(batch_id="old", domains=["held.com"])])
    store = BatchStore(collection=held)

    async def fake_stage(domains, **kw):
        table = {"ok1.com": (True, 12.99), "ok2.com": (True, 12.99),
                 "taken.com": (False, None), "prem.com": (True, 900.0),
                 "noprice.com": (True, None)}
        return PurchasePlan([PurchaseItem(d, *table.get(d, (None, None)))
                             for d in domains])

    monkeypatch.setattr(domain_batch, "stage_purchase", fake_stage)
    plan = run(stage_batches(
        ["ok1.com", "OK2.com", "taken.com", "prem.com", "noprice.com", "held.com"],
        client="Bettrdata", store=store, per_batch=3))
    planned = [d for b in plan["batches"] for d in b["domains"]]
    assert sorted(planned) == ["ok1.com", "ok2.com"]
    assert plan["unavailable"] == ["taken.com"]
    assert plan["over_ceiling"] == ["prem.com"]
    assert plan["unknown"] == ["noprice.com"]
    assert plan["already_planned"] == ["held.com"]
    assert plan["spend_account"] is None  # no BETTRDATA key configured
    assert plan["total_usd"] == 25.98


# ── identities + cache ───────────────────────────────────────────────────────

def test_topup_identities_skip_existing():
    first = generate_identities("example.com", 2)
    existing = {i["mailboxUsername"] for i in first}
    more = generate_identities("example.com", 2, exclude=existing)
    assert not existing & {i["mailboxUsername"] for i in more}
    assert len(more) == 2


def test_identities_from_names():
    ids = identities_from_names("x.com", ["Jane Doe", "Mary Ann Lee"])
    assert [i["mailboxUsername"] for i in ids] == ["janedoe", "maryannlee"]
    assert ids[1]["lastName"] == "Ann Lee"


def test_bulk_path_ignores_legacy_inferred_cache_rows():
    import time
    cache = {"a.com": {"available": True, "price": 12.99, "ts": time.time()}}
    assert _cached(cache, "a.com") == (True, 12.99)
    assert _cached(cache, "a.com", bulk_only=True) is None
    cache["a.com"]["src"] = "bulk"
    assert _cached(cache, "a.com", bulk_only=True) == (True, 12.99)
