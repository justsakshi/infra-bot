"""Tests for the infrabot <-> Zapmail wiring: strict client routing, the
provisioning chain, and the daily digest. No network, no spend.

    .venv\\Scripts\\python.exe -m pytest test_zapmail_infrabot.py -q
"""
from __future__ import annotations

import asyncio
import os
from datetime import date

import pytest

from smartlead import domain_provision
from smartlead.zapmail_accounts import (
    ZapmailAccountMissing, client_account_map, open_client, require_account,
)
from zapmail_digest import format_digest


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def fleet_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("ZAPMAIL_"):
            monkeypatch.delenv(k, raising=False)
    # Mirrors the real .env: plain key = Belardi Wong, named key = Precise Leads.
    monkeypatch.setenv("ZAPMAIL_API_KEY", "bw-key")
    monkeypatch.setenv("ZAPMAIL_PRIMARY_ACCOUNT_NAME", "Belardi Wong")
    monkeypatch.setenv("ZAPMAIL_API_KEY_PRECISE_LEADS", "pl-key")


# ── routing ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("client,key", [
    ("Precise Leads", "pl-key"),
    ("Melior", "pl-key"),
    ("Bettrdata", "pl-key"),
    ("Better Data", "pl-key"),
    ("Belardi Wong", "bw-key"),
])
def test_clients_route_to_their_own_account(client, key):
    assert require_account(client).api_key == key
    assert open_client(client)._api_key == key


@pytest.mark.parametrize("client,target", [
    ("Bettrdata", "cc44e5a8-b4c0-4d00-b9f8-6a9a62225782"),
    ("Belardi Wong", "e2d96503-c696-4f13-8f6f-1726c33c83aa"),
    ("Precise Leads", None),  # no Smartlead target registered in Zapmail yet
    ("Melior", None),
])
def test_default_export_targets(client, target):
    from smartlead.zapmail_accounts import export_target_for_client
    assert export_target_for_client(client) == target


@pytest.mark.parametrize("client", ["Darlean", "Betrdata", "Acme"])
def test_unmapped_or_unconfigured_clients_are_refused(client):
    with pytest.raises(ZapmailAccountMissing):
        require_account(client)


def test_darlean_dropped_from_map():
    assert "darlean" not in client_account_map()


# ── export target ────────────────────────────────────────────────────────────

class _FakeExportZ:
    """Mirrors live 2026-09-28: PL's Zapmail has ONE Smartlead target — BettrData's."""

    async def list_third_party_accounts(self, app):
        return {"data": {"accounts": [
            {"email": "amanda@bettrdata.io", "id": "cc44e5a8-b4c0"}]}}


def test_export_never_guesses_the_only_target():
    from smartlead.domain_export import _pick_account_id
    acc, err = run(_pick_account_id(_FakeExportZ(), "SMARTLEAD", None))
    assert acc is None and "amanda@bettrdata.io" in err


def test_export_target_comes_from_client_config(monkeypatch):
    from smartlead.domain_export import _pick_account_id
    from smartlead.zapmail_accounts import export_target_for_client
    monkeypatch.setenv("ZAPMAIL_EXPORT_TARGETS", "Bettrdata=cc44e5a8-b4c0")
    assert export_target_for_client("BETTRDATA") == "cc44e5a8-b4c0"
    assert export_target_for_client("Precise Leads") is None
    acc, err = run(_pick_account_id(_FakeExportZ(), "SMARTLEAD",
                                    export_target_for_client("Bettrdata")))
    assert acc == "cc44e5a8-b4c0" and err is None


# ── provisioning chain ───────────────────────────────────────────────────────

def _patch(monkeypatch, *, connected=True, mailboxes=None, export=None):
    calls = []

    async def fake_status(domain, client=None):
        return {"connected": connected, "status": "ACTIVE" if connected else "NOT_FOUND"}

    async def fake_connect(domains, **kw):
        calls.append("connect")
        return {domains[0]: {"status": "SUCCESS", "ok": True}}

    async def fake_mailboxes(domain, **kw):
        calls.append("mailboxes")
        return mailboxes or {"domain": domain, "ok": True, "created": ["a@x"], "pending": []}

    async def fake_export(domain, **kw):
        calls.append("export")
        return export or {"ok": True, "export_id": 1}

    monkeypatch.setattr(domain_provision, "connect_status", fake_status)
    monkeypatch.setattr(domain_provision, "connect_and_wait", fake_connect)
    monkeypatch.setattr(domain_provision, "assign_mailboxes_and_wait", fake_mailboxes)
    monkeypatch.setattr(domain_provision, "export_domain", fake_export)
    return calls


def test_provision_skips_connect_when_live_and_export_is_opt_in(monkeypatch):
    calls = _patch(monkeypatch)
    r = run(domain_provision.provision_domain("x.com", client="Precise Leads", approve=True))
    assert calls == ["mailboxes"]
    assert r["ok"] is True and r["account"] == "PRECISE_LEADS"
    assert "skipped" in r["steps"]["export"]


def test_provision_full_chain(monkeypatch):
    calls = _patch(monkeypatch, connected=False)
    r = run(domain_provision.provision_domain(
        "x.com", client="Precise Leads", export=True, approve=True))
    assert calls == ["connect", "mailboxes", "export"]
    assert r["ok"] is True


def test_provision_stops_at_failed_mailboxes(monkeypatch):
    calls = _patch(monkeypatch, mailboxes={"ok": False, "error": "not assignable"})
    r = run(domain_provision.provision_domain(
        "x.com", client="Precise Leads", export=True, approve=True))
    assert calls == ["mailboxes"]
    assert r["ok"] is False


def test_provision_wont_export_pending_mailboxes(monkeypatch):
    calls = _patch(monkeypatch, mailboxes={"ok": False, "pending": ["a@x"], "created": ["a@x"]})
    r = run(domain_provision.provision_domain(
        "x.com", client="Precise Leads", export=True, approve=True))
    assert "export" not in calls and r["ok"] is False


def test_provision_refuses_unmapped_client(monkeypatch):
    calls = _patch(monkeypatch)
    with pytest.raises(ZapmailAccountMissing):
        run(domain_provision.provision_domain("x.com", client="Darlean", approve=True))
    assert calls == []


# ── polling semantics from Zapmail support (2026-09-29) ─────────────────────

class _PollZ:
    """Fake client: connection-requests returns fixed statuses; mailboxes too."""

    def __init__(self, connect_status=None, mailbox_status=None):
        self.connect_status, self.mailbox_status = connect_status, mailbox_status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def connect_domains(self, names, approve=False):
        return {}

    async def list_connection_requests(self, **kw):
        return {"data": {"data": [{"domainName": "x.com", "status": self.connect_status}]}}

    async def list_domains(self, **kw):
        return {"data": {"domains": [{"domain": "x.com", "id": "d1", "status": "ACTIVE"}]}}

    async def list_assignable_domains(self, **kw):
        return {"data": {"domains": [{"domain": "x.com", "id": "d1",
                                      "assignedMailboxesCount": 0}]}}

    async def assign_mailboxes(self, payload, approve=False):
        self.assigned = payload
        return {}

    async def list_mailboxes(self, **kw):
        boxes = [{"email": f"{i['mailboxUsername']}@x.com", "status": self.mailbox_status}
                 for ids in getattr(self, "assigned", {}).values() for i in ids]
        return {"data": {"domains": [{"mailboxes": boxes}]}}


def test_ns_not_changed_is_reported_not_timeout(monkeypatch):
    from smartlead import domain_lifecycle as dl
    z = _PollZ(connect_status="NS_NOT_CHANGED")
    monkeypatch.setattr(dl, "open_client", lambda client, **kw: z)
    monkeypatch.setattr(dl, "_is_active_domain", lambda *a: _false())
    res = run(dl.connect_and_wait(["x.com"], approve=True, client="Precise Leads",
                                  timeout_s=0.05, interval_s=0.01))
    assert res["x.com"]["status"] == "NS_NOT_CHANGED"
    assert "24-48h" in res["x.com"]["detail"]


async def _false():
    return False


def test_failed_mailboxes_stop_polling_and_report(monkeypatch):
    from smartlead import domain_lifecycle as dl
    z = _PollZ(mailbox_status="FAILED")
    monkeypatch.setattr(dl, "open_client", lambda client, **kw: z)

    async def fake_provider(domain, *, client):
        return "GOOGLE"

    monkeypatch.setattr(dl, "domain_provider", fake_provider)
    res = run(dl.assign_mailboxes_and_wait("x.com", count=2, approve=True,
                                           client="Precise Leads",
                                           timeout_s=5, interval_s=0.01))
    assert res["ok"] is False and len(res["failed"]) == 2 and "FAILED" in res["error"]
    assert res["pending"] == []


def test_polling_defaults_follow_zapmail():
    from smartlead import domain_lifecycle as dl
    assert dl.CONNECT_POLL_S == 60 and dl.MAILBOX_POLL_S == 180


# ── digest ───────────────────────────────────────────────────────────────────

TODAY = date(2026, 9, 29)
OK_STATUS = {
    "PRECISE_LEADS": {"ok": True, "wallet_balance": 64, "auto_recharge": True,
                      "assigned_mailboxes": 69, "purchased_mailboxes": 54,
                      "placement_credits": 31},
}


def test_digest_never_says_none_when_renewals_failed():
    text, actions = format_digest(
        OK_STATUS, [], [], today=TODAY, ledger_ok=True,
        renewal_errors=["PRECISE_LEADS/MICROSOFT: 422"])
    assert "could not check renewals" in text and "_none_" not in text.split("*Domain")[0]
    assert actions == 1


def test_null_filters_are_not_sent():
    from smartlead.zapmail import ZapmailClient
    z = ZapmailClient(api_key="k")
    seen = {}

    async def fake_request(method, path, **kw):
        seen[path] = kw.get("json")
        return {}

    z._request = fake_request
    run(z.list_renewal_soon(page=1, limit=200))
    run(z.get_renewal_price(domain_ids=["1"]))
    assert seen["/v2/domains/renewal-soon"] == {}
    assert seen["/v2/domains/get-renewal-price"] == {"domainIds": ["1"]}


def test_quiet_status_counts_both_providers():
    status = {"PRECISE_LEADS": {**OK_STATUS["PRECISE_LEADS"],
                                "active_mailboxes": {"GOOGLE": 54, "MICROSOFT": 15}}}
    text, _ = format_digest(status, [], [], today=TODAY, ledger_ok=True)
    assert "54 Google + 15 Outlook" in text


def test_digest_ignores_past_client_expiries():
    renewals = [
        {"domain": "gomelior.com", "expire_on": "2026-10-05", "account": "PRECISE_LEADS",
         "client": "Melior"},
        {"domain": "kombinatorfunds.com", "expire_on": "2026-10-05",
         "account": "PRECISE_LEADS", "client": None},
    ]
    text, actions = format_digest(OK_STATUS, renewals, [], today=TODAY, ledger_ok=True)
    assert "gomelior.com" in text and "kombinatorfunds.com" not in text
    assert "1 past-client domain(s) lapsing" in text
    assert actions == 1


def test_digest_quiet_day():
    text, actions = format_digest(OK_STATUS, [], [], today=TODAY, ledger_ok=True)
    assert actions == 0 and "Nothing needs action" in text


def test_digest_flags_everything_actionable():
    status = {**OK_STATUS,
              "Belardi Wong": {"ok": True, "wallet_balance": 0, "auto_recharge": False,
                               "assigned_mailboxes": 48, "purchased_mailboxes": 48},
              "Broken": {"ok": False, "error": "401"}}
    renewals = [
        {"domain": "soon.com", "expire_on": "2026-10-05", "account": "Belardi Wong"},
        {"domain": "later.com", "expire_on": "2026-11-20", "account": "Belardi Wong"},
    ]
    batches = [
        {"batch_id": "due1", "status": "planned", "earliest_date": "2026-09-29",
         "domains": ["a.com"], "client": "Precise Leads", "estimated_usd": 12.99},
        {"batch_id": "future", "status": "planned", "earliest_date": "2026-10-03",
         "domains": ["b.com"]},
        {"batch_id": "stuck", "status": "unknown", "domains": ["c.com"]},
        {"batch_id": "done", "status": "purchased", "domains": ["d.com"]},
    ]
    text, actions = format_digest(status, renewals, batches, today=TODAY, ledger_ok=True)
    assert "unreachable" in text and "auto-recharge off" in text
    assert "soon.com" in text and "later.com" not in text
    assert "due1" in text and "future" not in text and "stuck" in text and "done" not in text
    # broken account + low BW wallet + 1 expiring + 1 due + 1 stuck
    assert actions == 5
