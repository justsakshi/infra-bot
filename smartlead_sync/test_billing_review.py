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


def _facts(pct=95.0, tested_on="2026-10-05", seeds=20, history=(), **kw):
    """Facts with a test list (newest first); pct -> spam count on ``seeds`` seeds."""
    tests = [{"date": tested_on, "seeds": seeds, "spam": round(seeds * (100 - pct) / 100), "by": {}}]
    tests += [{"date": d, "seeds": seeds, "spam": round(seeds * (100 - p) / 100), "by": {}} for d, p in history]
    base = {"in_smartlead": True, "connected": True, "reputation": "98%", "tests": tests, "campaigns": []}
    base.update(kw)
    return base


def _judge(f):
    from smartlead import kill_rule as kr
    k = kr.judge_all({"a@x.com": f["tests"]}, T)["a@x.com"] if f else None
    return rr.judge_inbox("a@x.com", f, k, T)[0]


def test_judge_inbox_follows_the_kill_rule():
    assert _judge(None) == "RETIRE"                                          # not in Smartlead
    assert _judge(_facts(pct=60.0)) == "CHECK"                               # ONE strike: warn, retest
    assert _judge(_facts(pct=60.0, history=[("2026-09-28", 65.0)])) == "RETIRE"   # two strikes 7d apart
    assert _judge(_facts(pct=40.0)) == "RETIRE"                              # clearly dead (<50% on 20)
    assert _judge(_facts(connected=False)) == "CHECK"
    assert _judge(_facts(reputation="55%")) == "CHECK"
    assert _judge(_facts(tested_on="2026-09-01")) == "CHECK"                 # stale
    assert _judge(_facts(pct=40.0, tested_on="2026-09-01")) == "CHECK"       # stale bad test is not proof
    assert _judge(_facts(seeds=6, pct=0.0)) == "CHECK"                       # too few seeds to count
    assert _judge(_facts()) == "KEEP"


def test_review_bill_counts_and_savings():
    # x.com: 3 mailboxes at ~70% on 20 seeds each = 60 pooled seeds -> dead domain (rule c)
    facts = {"a@x.com": _facts(pct=70.0), "b@x.com": _facts(pct=70.0), "c@x.com": _facts(pct=70.0),
             "d@y.com": _facts()}
    r = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-10-10", "price": 13.0,
                        "inboxes": ["a@x.com", "b@x.com", "c@x.com", "d@y.com"]}, facts, T)
    assert r["counts"] == {"KEEP": 1, "RETIRE": 3, "CHECK": 0}
    assert r["retire_saves_monthly"] == 9.75
    assert "saves *$9.75/month*" in rr.format_review([r], 3)
    assert all(row["why"].startswith("should kill") for row in r["rows"] if row["verdict"] == "RETIRE")


def test_failed_payment_carries_the_invoice_link():
    prev = state(_z("a", "2026-10-08"))
    now = [dict(_z("a", "2026-10-08", fail="card declined"), invoice_url="https://invoice.stripe.com/i/x")]
    txt = bw.format_events(bw.compare(prev, now, T))
    assert "<https://invoice.stripe.com/i/x|Pay / see invoice>" in txt


def test_unread_smartlead_is_never_a_retire():
    assert rr.judge_inbox("a@x.com", None, None, T, complete=False)[0] == "CHECK"
    r = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-10-08", "price": 7,
                        "inboxes": ["a@x.com", "b@x.com"]}, {}, T, complete=False)
    assert r["counts"]["RETIRE"] == 0 and r["retire_saves_monthly"] == 0


def test_support_message_names_bad_domains_and_keeps_good_ones():
    facts = {"a@bad.com": _facts(pct=45.0), "b@bad.com": _facts(pct=45.0), "c@good.com": _facts()}
    r = rr.review_bill({"provider": "ScaledMail", "label": "39 × Google Inboxes", "bills_on": "2026-10-08", "price": 10.5,
                        "clients": ["Precise Leads"], "order_id": "rec3", "inboxes": list(facts)}, facts, T)
    m = r["support_message"]
    assert "bad.com (2 inboxes)" in m and "45% inbox" in m and "not charge us" in m
    assert "keep the rest" in m and "good.com" in m and "rec3" in m
    clean = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-10-08", "price": 3.5,
                            "inboxes": ["c@good.com"]}, facts, T)
    assert clean["support_message"] == ""


def test_tracker_flags_refuse_a_half_read_review():
    import renewal_review as cli
    out = cli.flag_tracker({"errors": ["Smartlead PRECISE_LEADS: 429"], "reviews": []})
    assert out["written"] == 0 and "not written" in out["skipped"]
