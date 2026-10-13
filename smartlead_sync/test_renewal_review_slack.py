"""Renewal review + billing watch as Slack headline + thread (spec 2026-10-13). No network."""
import re
from datetime import date

from smartlead import billing_watch as bw
from smartlead import renewal_review as rr

T = date(2026, 10, 13)
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
EXPLAIN = "removes the inboxes at this bill"


def _facts(pct=95.0, tested_on="2026-10-10", seeds=20, **kw):
    tests = [{"date": tested_on, "seeds": seeds, "spam": round(seeds * (100 - pct) / 100), "by": {}}]
    base = {"in_smartlead": True, "connected": True, "reputation": "98%", "tests": tests, "campaigns": []}
    base.update(kw)
    return base


def _reviews():
    """13 Oct shape: a Zapmail Outlook subscription with a dead domain, a
    ScaledMail order with a spam domain + a stale inbox, a clean Zapmail one."""
    facts = {f"{u}@dataingestx.com": _facts(pct=70.0, campaigns=["BD Ingest"] if u == "amy" else [])
             for u in ("amy", "ben", "cal")}
    facts.update({"dan@bettrhq.com": _facts(), "eve@bettrhq.com": _facts(reputation="55%")})
    facts.update({"fay@plbad.com": _facts(pct=45.0), "gus@plbad.com": _facts(pct=45.0),
                  "hal@plgood.com": _facts(), "ida@plgood.com": _facts(tested_on="2026-09-01")})
    facts.update({"jo@clean.com": _facts(), "kim@clean.com": _facts()})
    z = rr.review_bill({"provider": "Zapmail", "label": "5 Outlook inboxes", "bills_on": "2026-10-15", "price": 15.0,
                        "clients": ["Bettrdata"], "subscription_id": "sub_1",
                        "inboxes": ["amy@dataingestx.com", "ben@dataingestx.com", "cal@dataingestx.com",
                                    "dan@bettrhq.com", "eve@bettrhq.com"]}, facts, T)
    s = rr.review_bill({"provider": "ScaledMail", "label": "4 × Google Inboxes", "bills_on": "2026-10-17", "price": 14.0,
                        "clients": ["Precise Leads"], "order_id": "rec3",
                        "inboxes": ["fay@plbad.com", "gus@plbad.com", "hal@plgood.com", "ida@plgood.com"]}, facts, T)
    c = rr.review_bill({"provider": "Zapmail", "label": "2 Google inboxes", "bills_on": "2026-10-19", "price": 6.0,
                        "clients": ["Precise Leads"], "inboxes": ["jo@clean.com", "kim@clean.com"]}, facts, T)
    return [z, s, c]


def test_review_headline_is_short_and_has_no_addresses():
    head, thread = rr.format_review(_reviews(), 7)
    assert len(head) <= 4
    assert head[0].startswith("⚠️ Bills in the next 7 days: $35.00 across 3 bills")
    assert "saves $" in head[0] and "/mo" in head[0]
    assert not any(EMAIL.search(l) for l in head)
    assert any("Zapmail" in l and "ScaledMail" in l for l in head[1:])


def test_review_thread_has_every_inbox_and_one_part_per_bill():
    rv = _reviews()
    head, thread = rr.format_review(rv, 7)
    blob = "\n".join(thread)
    for r in rv:
        for row in r["rows"]:
            assert row["email"] in blob
    for r in rv:   # one reply per bill, opening with date · provider · what
        assert sum(p.startswith(f"*{r['bills_on']} · {r['provider']} · {r['label']}*") for p in thread) == 1
    assert "BD Ingest" in blob          # a retire inbox still in a live campaign is flagged
    assert "Hi Zapmail team" in blob and "Hi ScaledMail team" in blob


def test_review_boilerplate_once_in_the_last_reply():
    head, thread = rr.format_review(_reviews(), 7)
    assert sum(p.count(EXPLAIN) for p in thread) == 1 and EXPLAIN in thread[-1]
    assert sum(p.count("ScaledMail bills per order") for p in thread) == 1
    assert not any(EXPLAIN in l for l in head)


def test_support_messages_are_whole_code_blocks():
    head, thread = rr.format_review(_reviews(), 7)
    msgs = [p for p in thread if "```" in p]
    assert len(msgs) == 2 and all(p.count("```") == 2 for p in msgs)
    assert all(len(p.split("\n")) <= 40 for p in msgs)


def test_review_nothing_to_retire_is_green():
    clean = [r for r in _reviews() if r["label"] == "2 Google inboxes"]
    head, thread = rr.format_review(clean, 7)
    assert head[0] == "✅ Bills in the next 7 days: $6.00 across 1 bill, nothing to retire"
    assert "jo@clean.com" in "\n".join(thread)


def test_review_empty():
    head, thread = rr.format_review([], 7)
    assert head == ["✅ Nothing bills in the next 7 days"] and thread == []


# ---- billing watch ----

def _z(id_, bills_on, status="ACTIVE", fail=None, label="15 Outlook inboxes"):
    return {"provider": "Zapmail", "id": id_, "label": label, "bills_on": bills_on, "status": status,
            "payment_failure": fail, "inboxes": 15, "clients": ["Bettrdata"]}


ADVICE = "Unpaid inboxes get suspended"


def _events():
    prev = {bw.key_of(r): dict(r) for r in (_z("a", "2026-10-13"), _z("b", "2026-10-12"), _z("c", "2026-10-12"))}
    now = [_z("a", "2026-10-13", fail="No payment method on file"), _z("b", "2026-10-12"),
           _z("c", "2026-11-12")]
    return bw.compare(prev, now, T)


def test_billing_watch_one_line_per_event_advice_once_in_thread():
    ev = _events()
    head, thread = bw.format_events(ev, T)
    assert head[0].startswith("🚨 Inbox billing watch, 13 Oct")
    assert len(head) == 1 + len(ev)
    assert not any(ADVICE in l for l in head)
    assert sum(p.count(ADVICE) for p in thread) == 1


def test_billing_watch_only_renewals_is_green_and_has_no_advice():
    prev = {bw.key_of(_z("c", "2026-10-12")): _z("c", "2026-10-12")}
    head, thread = bw.format_events(bw.compare(prev, [_z("c", "2026-11-12")], T), T)
    assert head[0].startswith("✅ Inbox billing watch") and thread == []


def test_billing_watch_nothing():
    assert bw.format_events([], T) == ([], [])


def test_poster_run_splits_headline_and_thread_and_posts_both(monkeypatch):
    import sys
    import renewal_review as cli
    from smartlead import notify
    bills = [{"provider": "ScaledMail", "label": "2 × Google", "bills_on": "2026-10-15", "price": 7.0,
              "clients": ["Precise Leads"], "inboxes": ["a@x.com", "b@x.com"]}]
    monkeypatch.setattr(cli, "zapmail_bills", lambda d, e: e.append("Zapmail: 500") or [])
    monkeypatch.setattr(cli, "scaledmail_bills", lambda d, e: bills)
    monkeypatch.setattr(cli, "smartlead_facts", lambda e: {"a@x.com": _facts(), "b@x.com": _facts()})
    res = cli.run(7)
    assert len(res["headline"]) <= 4 and "could not be read" in res["headline"][-1]
    assert "Zapmail: 500" in "\n".join(res["thread"]) and "removes the inboxes" in res["thread"][-1]
    assert res["text"].startswith(res["headline"][0]) and "a@x.com" in res["text"]
    sent = []
    monkeypatch.setattr(cli, "run", lambda days: res)
    monkeypatch.setattr(notify, "post_with_thread", lambda *a: sent.append(a) or "1.1")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("RENEWAL_REVIEW_CHANNEL", "C1")
    monkeypatch.setattr(sys, "argv", ["renewal_review.py", "--post"])
    assert cli.main() == 0 and res["posted"] is True
    assert sent[0][2] == res["headline"] and sent[0][3] == res["thread"]
