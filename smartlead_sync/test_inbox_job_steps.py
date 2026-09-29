"""The real job steps against fakes: ledger statuses, slots bought once, direct add."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, timedelta

import pytest

from smartlead import inbox_jobs as ij
from smartlead.inbox_job_steps import RealSteps


class Saves:
    def __init__(self):
        self.n = 0

    def save(self, job):
        self.n += 1


def mk(kind="owned", **kw):
    base = dict(client="Melior", kind=kind, provider="GOOGLE", domains=["gomelior.com"], inboxes_per_domain=2)
    base.update(kw)
    return ij.new_job(**base)


def step_of(job, name):
    return next(s for s in job["steps"] if s["name"] == name)


def run(c):
    return asyncio.run(c)


# ── buy_domains ─────────────────────────────────────────────────────────────

class FakeLedger:
    def __init__(self, batches):
        self.b = {x["batch_id"]: x for x in batches}

    def get(self, bid):
        return self.b.get(bid)

    def load_all(self):
        return list(self.b.values())


@pytest.fixture
def ledger(monkeypatch):
    import smartlead.domain_batch as db
    state = {"ledger": FakeLedger([]), "executed": [], "staged": 0}

    async def fake_stage(domains, client="", store=None):
        state["staged"] += 1
        b = {"batch_id": "b1", "domains": domains, "status": "planned", "client": client,
             "earliest_date": date.today().isoformat()}
        state["ledger"].b["b1"] = b
        return {"batches": [b], "unavailable": [], "unknown": [], "over_ceiling": [],
                "already_planned": [], "price_ceiling": 25}

    async def fake_execute(bid, approve=False, client="", store=None):
        assert approve
        state["executed"].append(bid)
        state["ledger"].b[bid]["status"] = "purchased"
        return {"status": "purchased"}

    monkeypatch.setattr(db, "BatchStore", lambda: state["ledger"])
    monkeypatch.setattr(db, "stage_batches", fake_stage)
    monkeypatch.setattr(db, "execute_one", fake_execute)
    return state


def test_buy_plans_then_buys_once(ledger):
    j = mk("new"); s = RealSteps(Saves())
    out = run(s.buy_domains(j, step_of(j, "buy_domains")))
    assert out.state == "done" and ledger["executed"] == ["b1"]
    out = run(s.buy_domains(j, step_of(j, "buy_domains")))      # a second run
    assert out.state == "done" and ledger["executed"] == ["b1"]  # not bought again


def test_crash_after_planning_reuses_our_own_batch(ledger):
    ledger["ledger"].b["b9"] = {"batch_id": "b9", "domains": ["gomelior.com"], "status": "planned",
                                "client": "Melior", "earliest_date": date.today().isoformat()}
    j = mk("new")
    out = run(RealSteps(Saves()).buy_domains(j, step_of(j, "buy_domains")))
    assert out.state == "done" and ledger["staged"] == 0 and ledger["executed"] == ["b9"]


@pytest.mark.parametrize("status, state", [("unknown", "needs_check"), ("in_progress", "needs_check"),
                                           ("partial", "needs_check"), ("failed", "failed")])
def test_unclear_or_refused_purchases_are_not_retried(ledger, status, state):
    ledger["ledger"].b["b1"] = {"batch_id": "b1", "domains": ["gomelior.com"], "status": status,
                                "client": "Melior", "result": {"error": "wallet short"}}
    j = mk("new"); st = step_of(j, "buy_domains"); st["data"]["batch_ids"] = ["b1"]
    out = run(RealSteps(Saves()).buy_domains(j, st))
    assert out.state == state and ledger["executed"] == []
    if status == "failed":
        assert "nothing was charged" in out.detail


def test_a_later_batch_waits_for_its_date(ledger):
    later = (date.today() + timedelta(days=2)).isoformat()
    ledger["ledger"].b["b2"] = {"batch_id": "b2", "domains": ["gomelior.com"], "status": "planned",
                                "client": "Melior", "earliest_date": later}
    j = mk("new"); st = step_of(j, "buy_domains"); st["data"]["batch_ids"] = ["b2"]
    out = run(RealSteps(Saves()).buy_domains(j, st))
    assert out.state == "waiting" and later in out.detail and ledger["executed"] == []


# ── inbox slots ─────────────────────────────────────────────────────────────

class FakeZ:
    def __init__(self, free, boxes=None, refuse=False):
        self.free, self.boxes, self.bought, self.refuse = free, boxes or {}, [], refuse

    async def list_mailboxes(self, page=1, limit=1, contains=None):
        if contains:
            return {"data": {"domains": [{"mailboxes": self.boxes.get(contains, [])}]}}
        return {"data": {"availableMailboxes": self.free}}

    async def buy_addon_mailboxes(self, qty, approve=False):
        assert approve
        if self.refuse:
            from smartlead.zapmail import ZapmailHTTPError
            raise ZapmailHTTPError("400 no card", 400)
        self.bought.append(qty)
        return {"paymentLink": "https://invoice.example/1"}


@pytest.fixture
def zap(monkeypatch):
    import smartlead.zapmail_accounts as za
    import smartlead.zapmail_fleet as zf
    holder = {"z": FakeZ(0)}

    @asynccontextmanager
    async def fake_open(client, provider=None):
        yield holder["z"]

    async def fake_locate(domain):
        return [{"account": "PRECISE_LEADS", "provider": "GOOGLE", "status": "ACTIVE", "mailboxes": []}]

    monkeypatch.setattr(za, "open_client", fake_open)
    monkeypatch.setattr(zf, "locate_domain", fake_locate)
    return holder


def test_enough_free_slots_buys_nothing(zap):
    zap["z"] = FakeZ(free=5)
    j = mk()
    out = run(RealSteps(Saves()).inbox_slots(j, step_of(j, "inbox_slots")))
    assert out.state == "done" and zap["z"].bought == []


def test_missing_slots_are_bought_once_then_waited_for(zap):
    zap["z"] = FakeZ(free=0)
    j = mk(); st = step_of(j, "inbox_slots"); saves = Saves(); s = RealSteps(saves)
    out = run(s.inbox_slots(j, st))
    st["data"].update(out.data)
    assert zap["z"].bought == [2] and out.state == "waiting" and "invoice.example" in out.detail
    assert saves.n >= 1                                  # 'attempted' saved BEFORE the call
    out = run(s.inbox_slots(j, st))
    assert zap["z"].bought == [2] and out.state == "waiting"   # never bought twice


def test_attempted_without_a_result_needs_a_person(zap):
    zap["z"] = FakeZ(free=0)
    j = mk(); st = step_of(j, "inbox_slots"); st["data"]["attempted"] = True
    out = run(RealSteps(Saves()).inbox_slots(j, st))
    assert out.state == "needs_check" and zap["z"].bought == []


def test_a_refused_slot_purchase_clears_the_marker(zap):
    from smartlead.zapmail import ZapmailHTTPError
    zap["z"] = FakeZ(free=0, refuse=True)
    j = mk(); st = step_of(j, "inbox_slots")
    with pytest.raises(ZapmailHTTPError):
        run(RealSteps(Saves()).inbox_slots(j, st))
    assert st["data"]["attempted"] is False


# ── into Smartlead, option 2 ────────────────────────────────────────────────

class FakeSL:
    present: list = []
    saved: list = []

    def __init__(self, key, account_name=""):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def list_email_accounts(self):
        return [{"from_email": e} for e in FakeSL.present]

    async def save_email_account(self, fields):
        FakeSL.saved.append(fields)
        return {"ok": True}


@pytest.fixture
def direct(monkeypatch, zap):
    import smartlead.api as api
    import smartlead.inbox_setup as ixs
    import smartlead.zapmail_accounts as za
    FakeSL.present, FakeSL.saved = [], []
    monkeypatch.setattr(api, "SmartleadClient", FakeSL)
    monkeypatch.setattr(ixs, "profile_for", lambda c: ("Melior", {"smartlead_account": "PRECISE_LEADS"}))
    monkeypatch.setattr(ixs, "smartlead_key_for", lambda p: ("PRECISE_LEADS", "k"))
    monkeypatch.setattr(za, "export_target_for_client", lambda c: None)
    return zap


def test_google_inbox_is_added_with_its_app_password(direct):
    direct["z"] = FakeZ(free=0, boxes={"gomelior.com": [
        {"email": "ann@gomelior.com", "firstName": "Ann", "lastName": "Lee",
         "password": "pw", "appPassword": "abcd efgh ijkl mnop"}]})
    j = mk(); j["inboxes"] = [{"email": "ann@gomelior.com"}]
    out = run(RealSteps(Saves()).into_smartlead(j, step_of(j, "into_smartlead")))
    assert out.state == "done"
    f = FakeSL.saved[0]
    assert f["password"] == "abcdefghijklmnop" and f["smtp_host"] == "smtp.gmail.com"
    assert f["from_name"] == "Ann Lee" and f["warmup_enabled"] is True
    assert "abcd" not in str(j)                         # the secret is never stored on the job


def test_already_in_smartlead_adds_nothing(direct):
    FakeSL.present = ["ann@gomelior.com"]
    j = mk(); j["inboxes"] = [{"email": "ann@gomelior.com"}]
    out = run(RealSteps(Saves()).into_smartlead(j, step_of(j, "into_smartlead")))
    assert out.state == "done" and FakeSL.saved == []


def test_no_app_password_is_a_clear_failure(direct):
    direct["z"] = FakeZ(free=0, boxes={"gomelior.com": [{"email": "ann@gomelior.com", "password": "pw"}]})
    j = mk(); j["inboxes"] = [{"email": "ann@gomelior.com"}]
    out = run(RealSteps(Saves()).into_smartlead(j, step_of(j, "into_smartlead")))
    assert out.state == "failed" and "no app password" in out.detail


def test_outlook_failure_says_what_to_do(direct, monkeypatch):
    async def boom(self, fields):
        raise RuntimeError("535 authentication failed")
    direct["z"] = FakeZ(free=0, boxes={"gomelior.com": [{"email": "ann@gomelior.com", "password": "pw"}]})
    monkeypatch.setattr(FakeSL, "save_email_account", boom)   # restored after the test
    j = mk(provider="MICROSOFT"); j["inboxes"] = [{"email": "ann@gomelior.com"}]
    out = run(RealSteps(Saves()).into_smartlead(j, step_of(j, "into_smartlead")))
    assert out.state == "failed" and "Connect Microsoft" in out.detail


# ── Smartlead setup waits for the inboxes to show ───────────────────────────

def test_setup_waits_until_smartlead_shows_the_inboxes(monkeypatch):
    import smartlead.inbox_setup as ixs
    from smartlead.inbox_setup import InboxChange
    monkeypatch.setattr(ixs, "profile_for", lambda c: ("Melior", {"signature": ""}))

    async def plan(client, emails=None, **kw):
        return "PRECISE_LEADS", [InboxChange(email=e, error="not in this client's Smartlead account")
                                 for e in emails]
    monkeypatch.setattr(ixs, "plan_for", plan)
    j = mk(); j["inboxes"] = [{"email": "ann@gomelior.com"}]
    out = run(RealSteps(Saves()).smartlead_setup(j, step_of(j, "smartlead_setup")))
    assert out.state == "waiting"
