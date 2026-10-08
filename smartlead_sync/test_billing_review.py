"""Billing watch + renewal review rules (no network).  python3 -m pytest test_billing_review.py -q"""
from datetime import date

from smartlead import billing_watch as bw
from smartlead import renewal_review as rr

T = date(2026, 10, 8)


def _z(id_, bills_on, status="ACTIVE", fail=None):
    return {"provider": "Zapmail", "id": id_, "label": "15 Outlook inboxes", "bills_on": bills_on,
            "status": status, "payment_failure": fail, "inboxes": 15, "clients": ["Bettrdata"]}


def _s(id_, bills_on, status="Active"):
    return {"provider": "ScaledMail", "id": id_, "label": "39 × Google", "bills_on": bills_on,
            "status": status, "payment_failure": None, "inboxes": 39, "clients": ["Precise Leads"]}


def state(*rows):
    return {bw.key_of(r): dict(r) for r in rows}


def ev(prev, now, today=T):
    return sorted((e["key"], e["event"]) for e in bw.compare(prev, now, today))


def test_first_run_is_silent():
    assert ev({}, [_z("a", "2026-10-08")]) == []


def test_renewed_vs_not_renewed():
    prev = state(_z("a", "2026-10-07"), _z("b", "2026-10-07"))
    assert ev(prev, [_z("a", "2026-11-07"), _z("b", "2026-10-07")]) == [("Zapmail:a", "renewed"), ("Zapmail:b", "not_renewed")]


def test_not_renewed_alerts_once():
    prev = state(_z("b", "2026-10-07"))
    events = bw.compare(prev, [_z("b", "2026-10-07")], T)
    saved = bw.remember(prev, [_z("b", "2026-10-07")], events, T)
    assert ev(saved, [_z("b", "2026-10-07")]) == []


def test_payment_failure_status_new_gone():
    prev = state(_z("a", "2026-10-08"), _z("gone", "2026-10-20"))
    now = [_z("a", "2026-10-08", fail="No payment method on file"), _z("c", "2026-11-01")]
    assert ev(prev, now) == [("Zapmail:a", "payment"), ("Zapmail:c", "new"), ("Zapmail:gone", "gone")]
    assert ev(state(_s("o", "2026-11-06")), [_s("o", "2026-11-06", status="Cancelled")]) == [("ScaledMail:o", "status")]


def test_scaledmail_bill_day_note_once():
    prev = state(_s("o", "2026-10-08"))
    e = bw.compare(prev, [_s("o", "2026-10-08")], T)
    assert [x["event"] for x in e] == ["bill_day"]
    saved = bw.remember(prev, [_s("o", "2026-10-08")], e, T)
    assert ev(saved, [_s("o", "2026-10-08")]) == []
    assert "check the card payment" in bw.format_events(e)


def _facts(**kw):
    base = {"in_smartlead": True, "connected": True, "reputation": "98%", "pct": 95.0, "tested_on": "2026-10-05", "campaigns": []}
    base.update(kw)
    return base


def test_judge_inbox():
    j = lambda f, spam=False: rr.judge_inbox("a@x.com", f, spam, T)[0]
    assert j(None) == "RETIRE"                                   # not in Smartlead
    assert j(_facts(pct=60.0)) == "RETIRE"
    assert j(_facts(), spam=True) == "RETIRE"
    assert j(_facts(connected=False)) == "CHECK"
    assert j(_facts(reputation="55%")) == "CHECK"
    assert j(_facts(tested_on="2026-09-01")) == "CHECK"           # stale test
    assert j(_facts(pct=60.0, tested_on="2026-09-01")) == "CHECK"  # stale bad test is not proof
    assert j(_facts()) == "KEEP"


def test_review_bill_counts_and_savings():
    facts = {"a@x.com": _facts(pct=50.0), "b@x.com": _facts(pct=50.0), "c@x.com": _facts(), "d@y.com": _facts()}
    r = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-10-10", "price": 13.0,
                        "inboxes": ["a@x.com", "b@x.com", "c@x.com", "d@y.com"]}, facts, T)
    assert r["counts"] == {"KEEP": 1, "RETIRE": 3, "CHECK": 0}      # c@x.com retired with its spam domain
    assert r["retire_saves_monthly"] == 9.75
    assert "saves *$9.75/month*" in rr.format_review([r], 3)


def test_failed_payment_carries_the_invoice_link():
    prev = state(_z("a", "2026-10-08"))
    now = [dict(_z("a", "2026-10-08", fail="card declined"), invoice_url="https://invoice.stripe.com/i/x")]
    txt = bw.format_events(bw.compare(prev, now, T))
    assert "<https://invoice.stripe.com/i/x|Pay / see invoice>" in txt


def test_unread_smartlead_is_never_a_retire():
    assert rr.judge_inbox("a@x.com", None, False, T, complete=False)[0] == "CHECK"
    r = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-10-08", "price": 7,
                        "inboxes": ["a@x.com", "b@x.com"]}, {}, T, complete=False)
    assert r["counts"]["RETIRE"] == 0 and r["retire_saves_monthly"] == 0
