"""New ScaledMail inboxes in Smartlead -> ready (signature, client, tags). No network.

Live 2026-10-13: ScaledMail uploaded order 77129b's 20 SMTP inboxes; all were
connected and warming but had no signature and no tags.
"""
import asyncio
import copy

import pytest

from smartlead import inbox_ready as ir
from smartlead import scaledmail_orders as so

PROF = {"signature_lines": ["{full_name}", "Precise Leads"], "company": "Precise Leads", "website": "preciseleads.in"}
TAGS = {"ScaledMail-SMTP": 535707, "ScaledMail-Google": 153335, "ScaledMail-Microsoft": 154156,
        "PreciseLeads": 3212, "Melior": 266037}


def plan(**kw):
    p = {"plan_id": "77129b", "status": "placed", "client": "Precise Leads", "provider": "smtp",
         "domains": ["a.com", "b.com"], "mailboxes": 4, "order_id": "rec1",
         "payload": {"smtp": {"domains": [{"domain": "a.com", "first_name": "Avinash", "last_name": "Haridas"},
                                          {"domain": "b.com", "first_name": "Aravind", "last_name": "Haridas"}]}},
         "history": [{"at": "2026-10-12T08:00:00+00:00", "status": "placed"}]}
    p.update(kw)
    return p


def acct(i, email, name, sig=""):
    return {"id": i, "from_email": email, "from_name": name, "signature": sig, "client_id": None}


ALL = [acct(1, "avinash@a.com", "Avinash Haridas"), acct(2, "avinash.h@a.com", "Avinash Haridas"),
       acct(3, "aravind@b.com", "Aravind Haridas"), acct(4, "aravind.h@b.com", "Aravind Haridas"),
       acct(9, "someone@other.com", "Other Person")]


def test_plan_ready_counts_found_and_picks_tags():
    r = ir.plan_ready(plan(), ALL, PROF, TAGS)
    assert r["expected"] == 4 and sorted(r["found"]) == ["aravind.h@b.com", "aravind@b.com", "avinash.h@a.com", "avinash@a.com"]
    assert r["complete"] and r["tag_ids"] == [535707, 3212] and r["account_ids"] == [1, 2, 3, 4]
    assert all("signature" in c.fields for c in r["changes"])
    assert not r["name_mismatch"] and not r["notes"]


def test_plan_ready_partial_and_never_touches_other_domains():
    r = ir.plan_ready(plan(), ALL[:2] + [ALL[4]], PROF, TAGS)
    assert r["expected"] == 4 and len(r["found"]) == 2 and not r["complete"]
    assert 9 not in r["account_ids"]


def test_plan_ready_flags_a_sender_not_on_the_order_and_a_missing_tag():
    accts = [acct(1, "x@a.com", "Ryan Markman")]
    r = ir.plan_ready(plan(client="Bettrdata"), accts, PROF, TAGS)
    assert r["name_mismatch"] == ["x@a.com (Ryan Markman)"]
    assert r["tag_ids"] == [535707] and any("Bettrdata" in n for n in r["notes"])


def test_plan_ready_keeps_an_existing_signature():
    sig_text = "Avinash Haridas\nPrecise Leads"
    accts = [acct(1, "avinash@a.com", "Avinash Haridas", sig="<div>Avinash Haridas</div><div>Precise Leads</div>")]
    r = ir.plan_ready(plan(), accts, PROF, TAGS)
    assert "signature" not in r["changes"][0].fields, sig_text


class FakeCol:
    def __init__(self, docs):
        self.docs = {d["plan_id"]: copy.deepcopy(d) for d in docs}

    def find(self, q, proj=None):
        return [copy.deepcopy(d) for d in self.docs.values()]

    def find_one(self, q, proj=None):
        d = self.docs.get(q.get("plan_id"))
        return copy.deepcopy(d) if d else None

    def update_one(self, q, u, upsert=False):
        d = self.docs[q["plan_id"]]
        d.update(u.get("$set", {}))
        for k, v in u.get("$push", {}).items():
            d.setdefault(k, []).append(copy.deepcopy(v))


class FakeSL:
    def __init__(self, accounts):
        self.accounts, self.tagged, self.updated = accounts, [], []

    async def list_email_accounts(self):
        return self.accounts

    async def list_tags(self):
        return [{"id": v, "name": k} for k, v in TAGS.items()]

    async def assign_tags(self, account_ids, tag_ids):
        assert len(account_ids) <= 25
        self.tagged.append((list(account_ids), list(tag_ids)))
        return {"ok": True}

    async def update_email_account(self, account_id, fields):
        self.updated.append((account_id, sorted(fields)))
        return {"ok": True}


def _run(store, sl, approve):
    return asyncio.run(ir.run_ready(store, approve=approve, sl_for=lambda client: sl,
                                    profile=lambda client: PROF))


def test_dry_run_reports_and_writes_nothing():
    store = so.PlanStore(FakeCol([plan()]))
    sl = FakeSL(ALL)
    out = _run(store, sl, approve=False)
    assert sl.tagged == [] and sl.updated == []
    assert any("would" in l.lower() for l in out["lines"]) and "ready" not in [e["status"] for e in store.get("77129b")["history"]]


def test_apply_makes_them_ready_once_and_logs_it():
    store = so.PlanStore(FakeCol([plan()]))
    sl = FakeSL(ALL)
    out = _run(store, sl, approve=True)
    assert sl.tagged == [([1, 2, 3, 4], [535707, 3212])]
    assert sorted(a for a, _ in sl.updated) == ["1", "2", "3", "4"]
    h = [e["status"] for e in store.get("77129b")["history"]]
    assert h[-2:] == ["in_smartlead", "ready"]
    assert any("Ready" in l and "4" in l for l in out["lines"])
    sl2 = FakeSL(ALL)
    _run(store, sl2, approve=True)                       # second run: nothing to do
    assert sl2.tagged == [] and sl2.updated == []


def test_partial_upload_waits_and_says_so():
    store = so.PlanStore(FakeCol([plan()]))
    sl = FakeSL(ALL[:2])
    out = _run(store, sl, approve=True)
    assert sl.tagged == [] and any("2 of 4" in l for l in out["lines"])


def test_more_than_25_inboxes_are_tagged_in_chunks():
    accts = [acct(i, f"u{i}@a.com", "Avinash Haridas") for i in range(1, 31)]
    store = so.PlanStore(FakeCol([plan(mailboxes=30, domains=["a.com"])]))
    sl = FakeSL(accts)
    _run(store, sl, approve=True)
    assert [len(ids) for ids, _ in sl.tagged] == [25, 5]


def test_only_placed_plans_are_looked_at():
    store = so.PlanStore(FakeCol([plan(status="dropped")]))
    sl = FakeSL(ALL)
    out = _run(store, sl, approve=True)
    assert out["lines"] == [] and sl.tagged == []
