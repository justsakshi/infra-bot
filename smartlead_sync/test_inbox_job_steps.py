"""The real job steps against fakes: ledger statuses, slots bought once, direct add."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, timedelta

import pytest

from smartlead import inbox_jobs as ij
from smartlead.inbox_job_steps import RealSteps


@pytest.fixture(autouse=True)
def _zapmail_key(monkeypatch):
    """Tests must not depend on the developer's .env (they failed on a clean
    machine: Melior's Zapmail account resolves through this key)."""
    monkeypatch.setenv("ZAPMAIL_API_KEY_PRECISE_LEADS", "test-key")


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

    async def fake_stage(domains, client="", store=None, provider="GOOGLE"):
        state["staged"] += 1
        state["provider"] = provider
        b = {"batch_id": "b1", "domains": domains, "status": "planned", "client": client,
             "earliest_date": date.today().isoformat(), "provider": provider}
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


def test_an_outlook_job_plans_its_purchase_for_outlook(ledger):
    j = mk("new", provider="MICROSOFT")
    run(RealSteps(Saves()).buy_domains(j, step_of(j, "buy_domains")))
    assert ledger["provider"] == "MICROSOFT"


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
    def __init__(self, free, boxes=None, refuse=False, wallet=100.0, sale=None):
        self.free, self.boxes, self.bought, self.refuse = free, boxes or {}, [], refuse
        self.wallet, self.sale, self.assigned = wallet, sale or [], []

    async def get_user(self):
        return {"data": {"activePlan": "Growth"}}

    async def get_wallet_balance(self):
        return {"walletBalance": self.wallet}

    async def assign_prewarmed(self, ids, approve=False):
        assert approve
        self.assigned.append(ids)
        return {"data": [{"username": "christy", "domain": "apexdemandcraft.co",
                          "firstName": "Christy", "lastName": "Hughes"}]}

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
    assert zap["z"].bought == [2] and out.state == "waiting" and "from the wallet" in out.detail
    assert saves.n >= 1                                  # 'attempted' saved BEFORE the call
    out = run(s.inbox_slots(j, st))
    assert zap["z"].bought == [2] and out.state == "waiting"   # never bought twice


def test_slots_are_refused_when_the_wallet_is_short(zap):
    from smartlead.zapmail import ZapmailSpendBlocked
    zap["z"] = FakeZ(free=0, wallet=5.0)                  # 2 slots x $3.25 = $6.50
    j = mk(); st = step_of(j, "inbox_slots")
    with pytest.raises(ZapmailSpendBlocked, match="wallet has \\$5.00"):
        run(RealSteps(Saves()).inbox_slots(j, st))
    assert zap["z"].bought == [] and not st["data"].get("attempted")


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


# ── pre-warmed: a free slot (plan only if none), then a free assign ─────────

def pw_job():
    return ij.new_job(client="Melior", kind="prewarmed", provider="GOOGLE",
                      domains=["apexdemandcraft.co"], prewarmed_domain_id="5daaf256")


class FakeZP(FakeZ):
    def __init__(self, wallet=100.0):
        super().__init__(free=0, wallet=wallet)
        self.plans = []

    async def purchase_prewarmed(self, plan, approve=False):
        assert approve
        self.plans.append(plan)
        return {"paymentLink": None, "useWallet": True}


@pytest.fixture
def pw(monkeypatch, zap):
    import smartlead.zapmail_accounts as za
    import smartlead.zapmail_fleet as zf
    holder = {"mine": [], "free": 0}

    async def locate(domain):
        return holder["mine"]

    async def overview(sample=1):
        return {"accounts": {"PRECISE_LEADS": {"GOOGLE": {"free": holder["free"]}}}}
    monkeypatch.setattr(zf, "locate_domain", locate)
    monkeypatch.setattr(zf, "prewarmed_overview", overview)
    monkeypatch.setattr(za, "require_account", lambda c: type("A", (), {"name": "PRECISE_LEADS"})())
    zap["pw"] = holder
    return zap


def test_a_free_prewarmed_slot_buys_nothing(pw):
    pw["z"] = FakeZP(); pw["pw"]["free"] = 2
    j = pw_job()
    out = run(RealSteps(Saves()).prewarmed_slot(j, step_of(j, "prewarmed_slot")))
    assert out.state == "done" and pw["z"].plans == []


def test_no_free_slot_buys_one_plan_from_the_wallet_once(pw):
    pw["z"] = FakeZP(wallet=100)
    j = pw_job(); st = step_of(j, "prewarmed_slot"); s = RealSteps(Saves())
    out = run(s.prewarmed_slot(j, st)); st["data"].update(out.data)
    assert out.state == "waiting" and pw["z"].plans == ["starter"]
    out = run(s.prewarmed_slot(j, st))
    assert out.state == "waiting" and pw["z"].plans == ["starter"]       # never twice


def test_no_plan_when_the_wallet_cannot_cover_the_first_month(pw):
    from smartlead.zapmail import ZapmailSpendBlocked
    pw["z"] = FakeZP(wallet=20)                                         # starter is $39
    j = pw_job(); st = step_of(j, "prewarmed_slot")
    with pytest.raises(ZapmailSpendBlocked):
        run(RealSteps(Saves()).prewarmed_slot(j, st))
    assert pw["z"].plans == [] and not st["data"].get("attempted")


def test_assigning_is_free_and_happens_once(pw):
    pw["z"] = FakeZP(wallet=0)                                          # no money needed
    j = pw_job(); st = step_of(j, "assign_prewarmed")
    out = run(RealSteps(Saves()).assign_prewarmed(j, st))
    assert out.state == "done" and pw["z"].assigned == [["5daaf256"]]
    assert j["inboxes"][0]["email"] == "christy@apexdemandcraft.co"
    st["data"]["attempted"] = True; pw["z"].assigned.clear(); j["inboxes"] = []
    out = run(RealSteps(Saves()).assign_prewarmed(j, st))
    assert out.state == "waiting" and pw["z"].assigned == []


def test_prewarmed_already_ours_is_done_without_assigning(pw):
    pw["z"] = FakeZP()
    pw["pw"]["mine"] = [{"account": "PRECISE_LEADS", "mailboxes": [{"email": "christy@apexdemandcraft.co"}]}]
    j = pw_job()
    out = run(RealSteps(Saves()).assign_prewarmed(j, step_of(j, "assign_prewarmed")))
    assert out.state == "done" and pw["z"].assigned == []