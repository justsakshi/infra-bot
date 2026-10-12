"""ScaledMail integration: gates, client routing, billing dates, order ledger,
tracker sync. No network, no Mongo, no spend.

    python3 -m pytest test_scaledmail.py -q
"""
import copy
from datetime import date, datetime

import pytest

from smartlead import scaledmail as smod
from smartlead.scaledmail import (ScaledMailBlocked, ScaledMailClient, ScaledMailError,
                                  ScaledMailHTTPError, ScaledMailOutcomeUnknown)
from smartlead.scaledmail_fleet import client_for, next_billing, renewals, report_summary
from smartlead import scaledmail_orders as so
from smartlead.scaledmail_asset_sync import plan_sync


# ── gates ────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _no_flags(monkeypatch):
    monkeypatch.delenv("SCALEDMAIL_ALLOW_SPEND", raising=False)
    monkeypatch.delenv("SCALEDMAIL_ALLOW_CANCEL", raising=False)


def _client():
    return ScaledMailClient(api_key="k", org_id="org")   # never entered: no HTTP


@pytest.mark.parametrize("call", [
    lambda c: c.set_order_tag("o1", "melior"),
    lambda c: c.swap_sender_names("a.com", [("A", "B")]),
    lambda c: c.swap_redirect("a.com", "b.com"),
    lambda c: c.swap_domain("a.com", "b.com"),
    lambda c: c.swap_masking("a.com", "b.com"),
    lambda c: c.cancel_order("o1"),
    lambda c: c.create_custom_order({}),
    lambda c: c.buy_domains(["a.com"]),
    lambda c: c.buy_prewarmed([]),
])
def test_every_change_needs_approve(call):
    with pytest.raises(ScaledMailBlocked, match="approve=True"):
        call(_client())


@pytest.mark.parametrize("call", [
    lambda c: c.create_custom_order({}, approve=True),
    lambda c: c.buy_domains(["a.com"], approve=True),
    lambda c: c.buy_prewarmed([], approve=True),
])
def test_spend_needs_kill_switch(call):
    with pytest.raises(ScaledMailBlocked, match="SCALEDMAIL_ALLOW_SPEND"):
        call(_client())


def test_cancel_needs_its_own_switch(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")      # spend switch does not open cancel
    with pytest.raises(ScaledMailBlocked, match="SCALEDMAIL_ALLOW_CANCEL"):
        _client().cancel_order("o1", approve=True)


def test_sender_mode_checked():
    with pytest.raises(ScaledMailError, match="mode must be"):
        _client().swap_sender_names("a.com", [("A", "B")], mode="bogus", approve=True)


# ── client routing ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("kw,want", [
    ({"tracker_client": "Melior"}, "Melior"),
    ({"tracker_client": "Preciseleads"}, "Precise Leads"),
    ({"tracker_client": "OSC - Srivatsan"}, None),                 # past client stays theirs
    ({"tag": "preciseleads-a1b2c3"}, "Precise Leads"),
    ({"tag": "bettrdata"}, "Bettrdata"),
    ({"redirect": "https://getmelior.com/x"}, "Melior"),
    ({"redirect": "preciseleads.in"}, "Precise Leads"),
    ({}, None),                                                    # agencyforumco.com: unassigned
])
def test_client_for(kw, want):
    assert client_for("agencyforumco.com", **kw) == want


def test_client_from_name_last():
    assert client_for("hubmelior.com") == "Melior"


# ── billing dates ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("created,today,want", [
    ("2026-07-06", date(2026, 10, 8), "2026-11-06"),
    ("2026-07-08", date(2026, 10, 8), "2026-10-08"),    # bills today
    ("2026-01-31", date(2026, 2, 10), "2026-02-28"),    # short month
    ("2026-10-07", date(2026, 10, 8), "2026-11-07"),
    ("2026-10-12", date(2026, 10, 12), "2026-11-12"),  # created today: first charge was today, next is next month
    ("bad", date(2026, 10, 8), ""),
])
def test_next_billing(created, today, want):
    assert next_billing(created, today) == want


def test_renewals_windows():
    snap = {"today": "2026-10-08",
            "domains": [{"domain": "a.com", "renewal_at": "2026-11-01", "renewal_price": 15, "client": "Melior",
                         "provider": "google", "mailboxes": 3},
                        {"domain": "b.com", "renewal_at": "2027-07-01", "renewal_price": 15, "client": "Melior",
                         "provider": "google", "mailboxes": 3}],
            "inventory": [{"domain": "c.com", "renewal_at": "2026-10-20", "renewal_price": 15}],
            "orders": [{"billing_day": "2026-10-10", "id": "o1"}, {"billing_day": "2026-11-06", "id": "o2"}]}
    r = renewals(snap)
    assert [d["domain"] for d in r["domains"]] == ["c.com", "a.com"]
    assert [o["id"] for o in r["billing"]] == ["o1"]


def test_report_summary_flags_low_and_blacklisted():
    rep = [{"weekof": "2026-10-05", "avgInboxScore": 90, "avgReputation": 80, "domains": [
        {"domain_name": "a.com", "stats": {"overall_score": 95, "blacklisted": []}},
        {"domain_name": "b.com", "stats": {"overall_score": 60, "blacklisted": []}},
        {"domain_name": "c.com", "stats": {"overall_score": 99, "blacklisted": ["spamhaus.org"]}}]}]
    s = report_summary(rep)
    assert [r["domain"] for r in s["flagged"]] == ["b.com", "c.com"]
    assert report_summary([]) == {"has_reports": False}


# ── order payload + ledger ───────────────────────────────────────────────────

def test_payload_shapes():
    p = so.build_payload("google", ["a.com", "b.com"], [("Jane", "Doe"), ("John", "Roe")], per_domain=3)
    assert p["google"]["mailboxes_per_domain"] == 3
    assert [d["first_name"] for d in p["google"]["domains"]] == ["Jane", "John"]
    assert "mailboxes_per_domain" not in so.build_payload("smtp", ["a.com"], [("A", "B")])["smtp"]
    with pytest.raises(ScaledMailError):
        so.build_payload("outlook", ["a.com"], [("A", "B")], per_domain=3)
    with pytest.raises(ScaledMailError):
        so.build_payload("google", ["a.com"], [])
    with pytest.raises(ScaledMailError):
        so.build_payload("outlook", [f"d{i}.com" for i in range(31)], [("A", "B")])


def test_costs_and_tag():
    assert so.monthly_cost("google", 8, 3) == 84.0
    assert so.monthly_cost("outlook", 2, 25) == 100.0
    assert so.monthly_cost("smtp", 24, 4) == 90.0
    assert len(so.make_tag("Precise Leads", "abc123")) <= 20
    assert so.make_tag("Melior", "abc123") == "melior-abc123"


class FakeCol:
    def __init__(self):
        self.docs = {}

    @staticmethod
    def _m(d, q):
        for k, v in q.items():
            if isinstance(v, dict) and "$in" in v:
                if d.get(k) not in v["$in"]:
                    return False
            elif d.get(k) != v:
                return False
        return True

    def update_one(self, q, u, upsert=False):
        for d in self.docs.values():
            if self._m(d, q):
                d.update(u.get("$set", {}))
                for k, v in u.get("$push", {}).items():
                    d.setdefault(k, []).append(copy.deepcopy(v))
                return
        if upsert and "$setOnInsert" in u:
            self.docs[u["$setOnInsert"]["plan_id"]] = copy.deepcopy(u["$setOnInsert"])

    def find_one(self, q, proj=None):
        return next((copy.deepcopy(d) for d in self.docs.values() if self._m(d, q)), None)

    def find(self, q, proj=None):
        return [copy.deepcopy(d) for d in self.docs.values() if self._m(d, q)]

    def find_one_and_update(self, q, u):
        for d in self.docs.values():
            if self._m(d, q):
                before = copy.deepcopy(d)
                d.update(u["$set"])
                for k, v in u.get("$push", {}).items():
                    d.setdefault(k, []).append(copy.deepcopy(v))
                return before
        return None


class FakeSM:
    def __init__(self, taken=(), order_exc=None, orders=()):
        self.taken, self.order_exc, self._orders = set(taken), order_exc, list(orders)
        self.placed = []

    def search_domains(self, names):
        return [{"domain": n, "status": "taken" if n in self.taken else "available",
                 "price": None if n in self.taken else 15.5, "blacklisted": []} for n in names]

    def create_custom_order(self, providers, *, source, tag, approve):
        assert approve and smod.spend_allowed()
        if self.order_exc:
            raise self.order_exc
        self.placed.append((providers, source, tag))
        (prov, block), = providers.items()
        names = [d["domain"] for d in block["domains"]]
        oid = f"rec{len(self.placed)}"
        self._orders.append({"id": oid, "tag": tag, "status": "Active", "amount": self.amount,
                             "packages": [{"type": prov.title(), "status": "New Order",
                                           "domains": [{"name": n, "status": ""} for n in names]}]})
        return {"message": "Order created successfully", "order_id": oid, "domains_purchased": names}

    amount = 21.0

    def orders(self):
        return [{"id": o["id"]} for o in self._orders]

    def order(self, oid):
        return next((o for o in self._orders if o["id"] == oid), {})


def _stage(sm, store, **kw):
    args = dict(client="Melior", provider="google", domains=["a.com", "b.com"],
                senders_text="Jane Doe, John Roe", per_domain=3, user="U1", store=store)
    args.update(kw)
    return so.stage_order(sm, **args)


def test_stage_records_a_plan_and_prices_it():
    store = so.PlanStore(FakeCol())
    r = _stage(FakeSM(), store)
    assert r["staged"] and r["monthly_usd"] == 21.0 and r["mailboxes"] == 6 and r["domains_usd"] == 31.0
    plan = store.get(r["plan_id"])
    assert plan["status"] == "planned" and plan["tag"].startswith("melior-")


def test_stage_refuses_taken_and_unknown_client():
    store = so.PlanStore(FakeCol())
    r = _stage(FakeSM(taken={"b.com"}), store)
    assert not r["staged"] and r["taken"] == ["b.com"] and not store.all()
    assert r["refused_raw"] == [{"domain": "b.com", "status": "taken", "price": None, "blacklisted": []}]
    with pytest.raises(ScaledMailError):
        _stage(FakeSM(), store, client="Darlean")


def test_place_is_gated_and_once(monkeypatch):
    store = so.PlanStore(FakeCol())
    sm = FakeSM()
    pid = _stage(sm, store)["plan_id"]
    with pytest.raises(ScaledMailBlocked):
        so.place_order(sm, pid, approve=True, store=store)          # switch off
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    with pytest.raises(ScaledMailBlocked):
        so.place_order(sm, pid, approve=False, store=store)         # not approved
    assert so.place_order(sm, pid, approve=True, store=store)["status"] == "placed"
    assert len(sm.placed) == 1 and sm.placed[0][2].startswith("melior-")
    with pytest.raises(ScaledMailBlocked, match="already ordered"):
        so.place_order(sm, pid, approve=True, store=store)
    assert len(sm.placed) == 1


def test_place_4xx_fails_retryably_timeout_blocks(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM(order_exc=ScaledMailHTTPError("400 bad", 400))
    pid = _stage(sm, store)["plan_id"]
    assert so.place_order(sm, pid, approve=True, store=store)["status"] == "failed"
    sm.order_exc = ScaledMailOutcomeUnknown("timeout")
    assert so.place_order(sm, pid, approve=True, store=store)["status"] == "unknown"
    with pytest.raises(ScaledMailBlocked, match="reconcile"):
        so.place_order(sm, pid, approve=True, store=store)


def test_place_rechecks_availability(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    pid = _stage(FakeSM(), store)["plan_id"]
    sm = FakeSM(taken={"a.com"})
    r = so.place_order(sm, pid, approve=True, store=store)
    assert r["status"] == "failed" and not sm.placed


def test_reconcile_by_tag_and_never_guesses_failed(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM(order_exc=ScaledMailOutcomeUnknown("timeout"))
    pid = _stage(sm, store)["plan_id"]
    so.place_order(sm, pid, approve=True, store=store)
    tag = store.get(pid)["tag"]
    assert so.reconcile(FakeSM(orders=[{"id": "o9", "tag": "other"}]), pid, store=store)["status"] == "unknown"
    assert so.reconcile(FakeSM(orders=[{"id": "o9", "tag": tag}]), pid, store=store)["status"] == "placed"


def test_mark_failed_only_unknown(monkeypatch):
    store = so.PlanStore(FakeCol())
    pid = _stage(FakeSM(), store)["plan_id"]
    with pytest.raises(ScaledMailError):
        so.mark_failed(pid, store=store)


def test_history_keeps_every_request_and_answer(monkeypatch):
    """Money trail: a later step never erases an earlier answer."""
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM(order_exc=ScaledMailHTTPError("400 on /create-custom-order: Subscription created but "
                                              "unable to charge", 400, body='{"error":"x"}'))
    pid = _stage(sm, store)["plan_id"]
    assert so.place_order(sm, pid, approve=True, user="U1", store=store)["status"] == "payment_pending"
    so.mark_failed(pid, user="U1", note="refunded to credits", store=store)
    sm.order_exc = None
    assert so.place_order(sm, pid, approve=True, user="U1", store=store)["status"] == "placed"
    h = store.get(pid)["history"]
    assert [e["status"] for e in h] == ["in_progress", "payment_pending", "failed", "in_progress", "placed", "verified"]
    assert h[1]["raw_response"] == '{"error":"x"}' and "unable to charge" in h[1]["error"]
    assert h[1]["request"] == {"method": "POST", "path": "/create-custom-order", "params": {"provider": "buy"},
                               "body": {"providers": store.get(pid)["payload"], "tag": store.get(pid)["tag"]}}
    assert h[2]["note"] == "refunded to credits" and h[2]["by"] == "U1"
    assert h[4]["response"]["order_id"] == "rec1" and h[4]["request"] == h[1]["request"]
    assert h[5]["status"] == "verified" and h[5]["ok"]
    assert all(e.get("at") for e in h)


def test_place_reads_its_order_back(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM()
    pid = _stage(sm, store)["plan_id"]
    r = so.place_order(sm, pid, approve=True, store=store)
    v = r["verify"]
    assert v["ok"] and v["order_id"] == "rec1" and v["amount"] == 21.0 and not v["problems"]
    assert store.get(pid)["order_id"] == "rec1"


@pytest.mark.parametrize("break_it,problem", [
    (lambda o: o.update(tag="other"), "tag"),
    (lambda o: o["packages"][0]["domains"].pop(), "missing"),
    (lambda o: o.update(amount=99.0), "amount"),
    (lambda o: o.update(id="other"), "not found"),
])
def test_verify_flags_a_mismatch(break_it, problem):
    plan = {"tag": "melior-abc", "domains": ["a.com", "b.com"], "monthly_usd": 21.0}
    sm = FakeSM(orders=[{"id": "o1", "tag": "melior-abc", "status": "Active", "amount": 21.0,
                         "packages": [{"domains": [{"name": "a.com"}, {"name": "b.com"}]}]}])
    break_it(sm._orders[0])
    v = so.verify_order(sm, plan, "o1")
    assert not v["ok"] and any(problem in p for p in v["problems"])


def test_verify_never_raises_after_a_spend(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM()
    pid = _stage(sm, store)["plan_id"]
    sm.order = lambda oid: (_ for _ in ()).throw(ScaledMailOutcomeUnknown("502"))
    r = so.place_order(sm, pid, approve=True, store=store)
    assert r["status"] == "placed" and not r["verify"]["ok"] and "502" in r["verify"]["problems"][0]


# ── tracker sync ─────────────────────────────────────────────────────────────

def _dom(name, **kw):
    d = {"domain": name, "provider": "google", "status": "Active", "mailboxes": 3, "order_status": "Active",
         "order_id": "o1", "billing_day": "2026-11-06", "renewal_at": "2027-07-06", "renewal_price": 15.12,
         "registered_on": "2026-07-06", "client": "Melior",
         "mailbox_rows": [{"email": f"a@{name}", "status": "Active"}]}
    d.update(kw)
    return d


def _snap(*doms):
    return {"today": "2026-10-08", "domains": list(doms)}


def test_sync_adds_live_client_domain_and_inbox():
    p = plan_sync(_snap(_dom("new.com")), {"x": {}})
    kinds = {(o["type"], o["action"]) for o in p["ops"]}
    assert kinds == {("DOMAIN", "insert"), ("INBOX", "insert")}
    ins = next(o for o in p["ops"] if o["type"] == "INBOX")["doc"]
    assert ins["provider"] == "Scaledmail" and ins["monthlyCost"] == 3.5
    assert ins["expiryDate"] == datetime(2026, 11, 6, tzinfo=ins["expiryDate"].tzinfo)


def test_sync_skips_unassigned_and_in_progress():
    p = plan_sync(_snap(_dom("a.com", client=None), _dom("b.com", status="In Progress")), {"x": {}})
    assert not p["ops"]
    assert {s["name"] for s in p["skipped"]} >= {"a.com", "b.com"}


def test_inbox_expiry_follows_the_order_bill_day():
    tracker = {"a@m.com": {"type": "INBOX", "status": "Active", "workspace": "Google", "domain": "m.com",
                           "expiryDate": datetime(2026, 10, 6)},
               "a@n.com": {"type": "INBOX", "status": "Active", "workspace": "Google", "domain": "n.com",
                           "expiryDate": datetime(2026, 10, 20)},
               "m.com": {"type": "DOMAIN", "status": "Active", "workspace": "Google",
                         "expiryDate": datetime(2027, 7, 6), "purchaseDate": datetime(2026, 7, 6)},
               "n.com": {"type": "DOMAIN", "status": "Active", "workspace": "Google",
                         "expiryDate": datetime(2027, 7, 6), "purchaseDate": datetime(2026, 7, 6)}}
    p = plan_sync(_snap(_dom("m.com"), _dom("n.com")), tracker)
    # both move to the order's bill day (2026-11-06): a past date and a wrong future one
    assert sorted((o["name"], o["reason"]) for o in p["ops"]) == [("a@m.com", "expiryDate"), ("a@n.com", "expiryDate")]


def test_unknown_alias_on_tracked_domain_not_added():
    tracker = {"real@g.com": {"type": "INBOX", "status": "Active", "workspace": "Outlook", "domain": "g.com",
                              "expiryDate": datetime(2026, 11, 6)}}
    p = plan_sync(_snap(_dom("g.com", provider="outlook", mailbox_rows=[{"email": "ghost@g.com", "status": "Active"}])),
                  tracker)
    assert not [o for o in p["ops"] if o["type"] == "INBOX" and o["action"] == "insert"]


def test_cancelled_order_downgrades_and_gone_marked_inactive():
    tracker = {"m.com": {"type": "DOMAIN", "status": "Active", "provider": "Scaled Mail", "workspace": "Google",
                         "expiryDate": datetime(2027, 7, 6), "purchaseDate": datetime(2026, 7, 6)},
               "old.com": {"type": "DOMAIN", "status": "Active", "provider": "Scaledmail"},
               "z.com": {"type": "DOMAIN", "status": "Active", "provider": "Zapmail"}}
    p = plan_sync(_snap(_dom("m.com", order_status="Cancelled", mailbox_rows=[])), tracker)
    sets = {o["name"]: o["set"] for o in p["ops"]}
    assert sets["m.com"] == {"status": "Inactive"} and sets["old.com"] == {"status": "Inactive"}
    assert "z.com" not in sets
    assert not plan_sync(_snap(), tracker)["ops"]          # empty answer: nothing marked gone


def test_dropped_plan_can_never_be_placed(monkeypatch):
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM()
    pid = _stage(sm, store)["plan_id"]
    assert so.drop_plan(pid, store=store)["status"] == "dropped"
    with pytest.raises(ScaledMailBlocked, match="dropped"):
        so.place_order(sm, pid, approve=True, store=store)
    assert not sm.placed


def test_own_domains_order_mailboxes_only_and_never_send_a_registrar_login(monkeypatch):
    """GoDaddy domains (2026-10-09): provider=other, no hosting block, and only for
    domains really in our GoDaddy account — checked at stage AND at place."""
    store = so.PlanStore(FakeCol())
    sm = FakeSM(taken={"a.com", "b.com"})            # ScaledMail's search says taken: ignored for own domains
    owned = {"a.com", "b.com"}
    ownership = lambda ds: [d for d in ds if d not in owned]
    r = _stage(sm, store, own_domains=True, ownership=ownership)
    assert r["staged"] and r["source"] == "other" and r["domains_usd"] == 0
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    owned.discard("b.com")                           # lost between stage and place
    res = so.place_order(sm, r["plan_id"], approve=True, ownership=ownership, store=store)
    assert res["status"] == "failed" and "b.com" in res["error"] and sm.placed == []
    owned.add("b.com")
    res = so.place_order(sm, r["plan_id"], approve=True, ownership=ownership, store=store)
    providers, source, tag = sm.placed[0]
    assert res["status"] == "placed" and source == "other" and "hosting" not in providers


def test_own_domains_refused_when_not_ours():
    store = so.PlanStore(FakeCol())
    r = _stage(FakeSM(), store, own_domains=True, ownership=lambda ds: ["b.com"])
    assert not r["staged"] and r["not_ours"] == ["b.com"] and not store.all()


def test_subscription_created_but_not_charged_is_never_placed_again(monkeypatch):
    """Live 2026-10-09: '400 Subscription created but unable to charge'. A 400 that
    already made a subscription must not read as a retryable failure."""
    monkeypatch.setenv("SCALEDMAIL_ALLOW_SPEND", "true")
    store = so.PlanStore(FakeCol())
    sm = FakeSM(order_exc=ScaledMailHTTPError(
        "400 on /create-custom-order: Subscription created but unable to charge", 400))
    pid = _stage(sm, store)["plan_id"]
    r = so.place_order(sm, pid, approve=True, store=store)
    assert r["status"] == "payment_pending" and "web app" in r["next"]
    sm.order_exc = None
    with pytest.raises(ScaledMailBlocked):
        so.place_order(sm, pid, approve=True, store=store)
    assert sm.placed == []
    with pytest.raises(ScaledMailError):
        so.drop_plan(pid, store=store)


# ── setup watcher ────────────────────────────────────────────────────────────

def _placed(pid, oid, domains, placed_on, *extra):
    return {"plan_id": pid, "status": "placed", "order_id": oid, "domains": domains, "client": "Precise Leads",
            "history": [{"at": f"{placed_on}T08:00:00+00:00", "status": "placed"}, *extra]}


def _row(name, oid, status, n=3):
    return {"domain": name, "order_id": oid, "status": status, "mailboxes": n}


def test_business_days():
    assert so.business_days(date(2026, 10, 12), date(2026, 10, 12)) == 0     # Monday, same day
    assert so.business_days(date(2026, 10, 12), date(2026, 10, 15)) == 3     # Mon -> Thu
    assert so.business_days(date(2026, 10, 16), date(2026, 10, 19)) == 1     # Fri -> Mon


def test_setup_watch_reports_waiting_late_and_live():
    plans = [_placed("p1", "o1", ["a.com"], "2026-10-12"),
             _placed("p2", "o2", ["b.com", "c.com"], "2026-10-12"),
             _placed("p3", "o3", ["d.com"], "2026-10-05"),
             {"plan_id": "p4", "status": "planned", "domains": ["e.com"], "history": []}]
    snap = {"domains": [_row("a.com", "o1", "New Order"),
                        _row("b.com", "o2", "Active"), _row("c.com", "o2", "Active", 25),
                        _row("d.com", "o3", "In Progress")]}
    w = so.setup_watch(snap, plans, today=date(2026, 10, 14))
    text = "\n".join(w["lines"])
    assert w["newly_live"] == ["p2"]
    assert "b.com, c.com" in text and "28 inboxes" in text and "o2" in text and "Smartlead" in text
    assert 'inbox_setup.py --client "Precise Leads" --domain b.com --signature' in text
    assert "a.com" in text and "business day 2" in text
    assert "Late" in text and "d.com" in text
    assert "e.com" not in text
    assert w["covered"] == {"a.com", "b.com", "c.com", "d.com"}


def test_setup_watch_live_only_until_two_days_after():
    live = {"at": "2026-10-14T04:00:00+00:00", "status": "setup_live"}
    plans = [_placed("p2", "o2", ["b.com"], "2026-10-12", live)]
    snap = {"domains": [_row("b.com", "o2", "Active")]}
    assert so.setup_watch(snap, plans, today=date(2026, 10, 15))["lines"]        # still shown
    w = so.setup_watch(snap, plans, today=date(2026, 10, 17))
    assert not w["lines"] and not w["newly_live"]


def test_setup_watch_needs_every_domain_live():
    plans = [_placed("p2", "o2", ["b.com", "c.com"], "2026-10-12")]
    snap = {"domains": [_row("b.com", "o2", "Active")]}                           # c.com not listed yet
    w = so.setup_watch(snap, plans, today=date(2026, 10, 13))
    assert not w["newly_live"]

def test_prices_come_from_the_offer_file():
    offer = so.load_offer()
    assert offer["sending_days_per_month"] == 22
    assert so.PRICE == {"google": 3.5, "outlook": 50.0, "smtp": 3.75}
    assert so.PRICE_BASIS == {"google": "mailbox", "outlook": "domain", "smtp": "domain"}
    zm = {t["key"]: t for t in offer["providers"]["zapmail"]["types"]}
    assert set(zm) == {"GOOGLE", "MICROSOFT"} and all(t["price"] == 3.25 and t["per_domain"] == 3 for t in zm.values())
