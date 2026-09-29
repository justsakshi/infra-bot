"""Inbox name / signature / client / new-inbox warmup (step 1 of the inbox pipeline)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from smartlead import inbox_setup as ix
from smartlead.zapmail import ZapmailClient, ZapmailSpendBlocked

PROF = {"smartlead_account": "PRECISE_LEADS", "smartlead_client_id": 12256,
        "company": "Melior", "website": "getmelior.com", "title": "Partner",
        "signature": "<p>{full_name}<br>{title}, {company}<br>{website}</p>"}
ACC = {"id": 7, "from_email": "ann@gomelior.com", "from_name": "Ann Lee",
       "signature": "", "client_id": None}


# ── pure planning ────────────────────────────────────────────────────────────

def test_rename_changes_name_not_address():
    ch = ix.plan_changes(ACC, PROF, email="ann@gomelior.com", first="Anna", last="Reyes")
    assert ch.fields["from_name"] == "Anna Reyes"
    assert ch.before["from_name"] == "Ann Lee"
    assert ch.zapmail_rename == {"firstName": "Anna", "lastName": "Reyes"}
    assert "from_email" not in ch.fields and "username" not in str(ch.fields)


def test_signature_is_rendered_with_the_inbox_name_and_escaped():
    prof = {**PROF, "company": "Melior & Co"}
    ch = ix.plan_changes(ACC, prof, email="ann@gomelior.com", set_signature=True)
    assert ch.fields["signature"] == "<p>Ann Lee<br>Partner, Melior &amp; Co<br>getmelior.com</p>"


def test_a_rename_with_signature_uses_the_new_name():
    ch = ix.plan_changes(ACC, PROF, email="ann@gomelior.com", first="Anna", last="Reyes",
                         set_signature=True)
    assert "Anna Reyes" in ch.fields["signature"]


def test_no_template_is_a_clear_error_not_a_blank_signature():
    ch = ix.plan_changes(ACC, {**PROF, "signature": ""}, email="ann@gomelior.com", set_signature=True)
    assert ch.error and "no signature template" in ch.error and not ch.fields


def test_unknown_placeholder_is_refused():
    with pytest.raises(ValueError, match="unknown placeholder"):
        ix.render_signature("{full_name} {phone}", {"full_name": "A"})


def test_melior_inboxes_are_filed_under_the_melior_client():
    ch = ix.plan_changes(ACC, PROF, email="ann@gomelior.com")
    assert ch.fields == {"client_id": 12256}


def test_nothing_to_do_is_empty():
    acc = {**ACC, "client_id": 12256}
    ch = ix.plan_changes(acc, PROF, email="ann@gomelior.com", first="Ann", last="Lee")
    assert ch.empty


def test_new_inbox_gets_the_team_standard_warmup_only_when_asked():
    assert ix.plan_changes(ACC, PROF, email="ann@gomelior.com").warmup is None
    w = ix.plan_changes(ACC, PROF, email="ann@gomelior.com", new_inbox=True).warmup
    assert w == {"enabled": True, "total_per_day": 40, "daily_rampup": 5,
                 "reply_rate": 25, "auto_adjust": False}


@pytest.mark.parametrize("bad", ["", "4nn", "Robert'); DROP", "x" * 41])
def test_bad_names_are_refused(bad):
    ch = ix.plan_changes(ACC, PROF, email="ann@gomelior.com", first=bad, last="Lee")
    assert ch.error and not ch.fields


def test_inbox_not_in_smartlead_is_reported():
    assert "not in this client's Smartlead" in ix.plan_changes(None, PROF, email="x@y.com").error


# ── apply against fakes ──────────────────────────────────────────────────────

class FakeSmartlead:
    calls: list = []
    accounts = [dict(ACC), {"id": 8, "from_email": "bo@gomelior.com", "from_name": "Bo Park",
                            "signature": "", "client_id": 12256}]

    def __init__(self, key, account_name=""):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def list_email_accounts(self):
        return [dict(a) for a in self.accounts]

    async def update_email_account(self, account_id, fields):
        FakeSmartlead.calls.append(("update", account_id, fields))
        return {"ok": True}

    async def set_warmup(self, account_id, enabled, total, rampup, reply, auto):
        FakeSmartlead.calls.append(("warmup", account_id, total, rampup, reply, auto))
        return {"ok": True}


class FakeZapmail:
    renamed: list = []

    async def list_mailboxes(self, contains=None, page=1, limit=10):
        return {"data": {"domains": [{"mailboxes": [
            {"id": "mb-1", "email": "ann@gomelior.com", "username": "ann"}]}]}}

    async def update_mailbox_names(self, changes, approve=False):
        assert approve
        FakeZapmail.renamed.extend(changes)
        return {"status": 200}


@pytest.fixture
def fakes(monkeypatch):
    FakeSmartlead.calls = []
    FakeZapmail.renamed = []
    import smartlead.api as api
    import smartlead.zapmail_accounts as za
    monkeypatch.setattr(api, "SmartleadClient", FakeSmartlead)
    monkeypatch.setattr(ix, "profile_for", lambda c: ("Melior", PROF))
    monkeypatch.setattr(ix, "smartlead_key_for", lambda p: ("PRECISE_LEADS", "k"))

    @asynccontextmanager
    async def fake_open(client, provider=None):
        yield FakeZapmail()
    monkeypatch.setattr(za, "open_client", fake_open)


def test_plan_for_a_domain_covers_every_inbox_on_it(fakes):
    _, changes = asyncio.run(ix.plan_for("Melior", domain="gomelior.com", set_signature=True))
    assert [c.email for c in changes] == ["ann@gomelior.com", "bo@gomelior.com"]


def test_rename_needs_exactly_one_inbox(fakes):
    with pytest.raises(ValueError, match="one inbox at a time"):
        asyncio.run(ix.plan_for("Melior", domain="gomelior.com", first="A", last="B"))


def test_apply_writes_smartlead_then_mirrors_the_name_in_zapmail(fakes):
    _, changes = asyncio.run(ix.plan_for("Melior", emails=["ann@gomelior.com"],
                                         first="Anna", last="Reyes", new_inbox=True))
    res = asyncio.run(ix.apply_changes("Melior", changes, approve=True))
    assert res[0]["ok"] and set(res[0]["done"]) >= {"from_name", "client_id", "warmup", "zapmail"}
    assert ("update", "7", {"from_name": "Anna Reyes", "client_id": 12256}) in FakeSmartlead.calls
    assert ("warmup", "7", 40, 5, 25, False) in FakeSmartlead.calls
    assert FakeZapmail.renamed == [{"mailboxId": "mb-1", "username": "ann",
                                    "firstName": "Anna", "lastName": "Reyes"}]


def test_apply_refuses_without_approval(fakes):
    _, changes = asyncio.run(ix.plan_for("Melior", emails=["ann@gomelior.com"]))
    with pytest.raises(PermissionError):
        asyncio.run(ix.apply_changes("Melior", changes))
    assert FakeSmartlead.calls == []


def test_zapmail_rename_guard_and_keeps_the_username():
    z = ZapmailClient(api_key="k")
    with pytest.raises(ZapmailSpendBlocked):
        asyncio.run(z.update_mailbox_names([{"mailboxId": "m", "username": "u",
                                             "firstName": "A", "lastName": "B"}]))
    with pytest.raises(ValueError, match="current username"):
        asyncio.run(z.update_mailbox_names([{"mailboxId": "m", "firstName": "A",
                                             "lastName": "B"}], approve=True))


def test_the_real_profiles_file_is_valid():
    profiles = ix.load_profiles()
    assert {"Bettrdata", "Belardi Wong", "Precise Leads", "Melior"} <= set(profiles)
    assert profiles["Melior"]["smartlead_account"] == profiles["Precise Leads"]["smartlead_account"]
    for p in profiles.values():
        if p.get("signature"):
            ix.render_signature(p["signature"], {})
