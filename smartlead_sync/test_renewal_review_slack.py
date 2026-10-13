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


def test_review_headline_says_what_to_drop():
    head, parts = rr.format_review(_reviews(), 7)
    assert head == ["⚠️ Bills in the next 7 days ($35.00, 3 bills): drop 5 kill-list inboxes before they renew, "
                    "saves $16.00/mo"]
    assert not any(EMAIL.search(l) for l in head)


def test_review_drop_lines_per_bill_by_domain():
    head, parts = rr.format_review(_reviews(), 7)
    drop = parts[0].split("\n")
    assert drop[0] == "*Drop before the bill*"
    assert drop[1] == ("• Bill on 15 Oct · Zapmail · 5 Outlook inboxes · $15.00 (Bettrdata): drop dataingestx.com "
                       "(3 inboxes), saves $9.00/mo · ⚠️ amy@dataingestx.com is still in BD Ingest")
    assert drop[2] == ("• Bill on 17 Oct · ScaledMail · 4 × Google Inboxes · $14.00 (Precise Leads): drop plbad.com "
                       "(2 inboxes), saves $7.00/mo")


def test_review_checks_and_clean_bills_are_short():
    head, parts = rr.format_review(_reviews(), 7)
    check = next(p for p in parts if p.startswith("*Check before paying*")).split("\n")
    assert any(l.startswith("• `eve@bettrhq.com` (bill 15 Oct): ") and "55" in l for l in check)
    assert "• Bill on 17 Oct (Precise Leads): 1 inbox has no recent placement test" in check
    clean = next(p for p in parts if p.startswith("*Nothing to drop*"))
    assert clean == "*Nothing to drop*\n• Bill on 19 Oct · Zapmail · 2 Google inboxes · $6.00 (Precise Leads)"


def test_review_how_to_and_support_messages_last():
    head, parts = rr.format_review(_reviews(), 7)
    msgs = [p for p in parts if p.startswith("_Message for")]
    assert len(msgs) == 2 and all(p.count("```") == 2 for p in msgs)
    assert sum(p.count(EXPLAIN) for p in parts) == 1 and EXPLAIN in parts[-1]
    assert "/domains" in parts[-1]


def test_review_nothing_to_drop_is_green():
    clean = [r for r in _reviews() if r["counts"]["RETIRE"] == 0 and r["counts"]["CHECK"] == 0]
    head, parts = rr.format_review(clean, 7)
    assert head == ["✅ Bills in the next 7 days ($6.00, 1 bill): nothing on the kill list"]
    assert parts[0].startswith("*Nothing to drop*")


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
    assert "thread" not in res["text"]
    assert res["text"].startswith(res["headline"][0]) and "Bill on 15 Oct · ScaledMail · 2 × Google" in res["text"]
    sent = []
    monkeypatch.setattr(cli, "run", lambda days: res)
    monkeypatch.setattr(notify, "post_with_thread", lambda *a: sent.append(a) or "1.1")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("RENEWAL_REVIEW_CHANNEL", "C1")
    monkeypatch.setattr(sys, "argv", ["renewal_review.py", "--post"])
    assert cli.main() == 0 and res["posted"] is True
    assert sent[0][2] == [res["text"]] and sent[0][3] == []          # one message, no thread


def test_unused_and_setting_up_are_not_called_kill_list():
    facts = {"k1@dead.com": _facts(pct=45.0), "k2@dead.com": _facts(pct=45.0)}      # kill list (under 50%)
    b = rr.review_bill({"provider": "ScaledMail", "label": "7 × Google", "bills_on": "2026-11-12", "price": 31.5,
                        "clients": ["Precise Leads"], "order_id": "rec9",
                        "inboxes": ["k1@dead.com", "k2@dead.com", "u1@gone.com", "u2@gone.com",
                                    "s1@new.com", "s2@new.com", "s3@new.com"],
                        "setting_up": ["s1@new.com", "s2@new.com", "s3@new.com"]}, facts, T)
    head, parts = rr.format_review([b], 7)
    assert head == ["⚠️ Bills in the next 7 days ($31.50, 1 bill): drop 2 kill-list inboxes, "
                    "2 paid-for inboxes not in Smartlead, saves up to $18.00/mo"]
    drop = parts[0]
    assert "drop dead.com (2 inboxes), saves $9.00/mo" in drop and "3 inboxes still being set up, left alone" in drop
    assert "gone.com" not in drop and "new.com" not in "\n".join(parts)
    unused = next(p for p in parts if p.startswith("*Paying for inboxes not in Smartlead*"))
    assert "gone.com (2 inboxes)" in unused and "saves $9.00/mo" in unused


def test_no_recent_test_is_one_line_per_bill():
    facts = {f"n{i}@m.com": _facts(tested_on="2026-09-01") for i in range(5)}
    facts["r@m.com"] = _facts(reputation="55%")
    b = rr.review_bill({"provider": "ScaledMail", "label": "6 × Google", "bills_on": "2026-11-06", "price": 21.0,
                        "clients": ["Melior"], "inboxes": list(facts)}, facts, T)
    head, parts = rr.format_review([b], 7)
    check = next(p for p in parts if p.startswith("*Check before paying*")).split("\n")
    assert "• Bill on 6 Nov (Melior): 5 inboxes have no recent placement test" in check
    assert any(l.startswith("• `r@m.com` (bill 6 Nov): warmup reputation 55") for l in check)
    assert len(check) == 3


def test_scaledmail_bills_mark_mailboxes_still_being_set_up(monkeypatch):
    import renewal_review as cli
    import smartlead.scaledmail as sm_mod
    import smartlead.scaledmail_fleet as fleet

    class FakeSM:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def mailboxes(self, domain_id):
            return [{"email": "A@new.com", "status": "In Progress"}, {"email": "b@new.com", "status": "Active"}]

    monkeypatch.setattr(sm_mod, "configured", lambda: True)
    monkeypatch.setattr(sm_mod, "ScaledMailClient", FakeSM)
    monkeypatch.setattr(fleet, "snapshot", lambda sm: {
        "orders": [{"id": "rec9", "billing_day": "2026-10-15", "description": "2 × Google", "amount": 7, "clients": ["Precise Leads"]}],
        "domains": [{"order_id": "rec9", "domain_id": "d1"}]})
    bills = cli.scaledmail_bills(30, [])
    assert bills[0]["inboxes"] == ["a@new.com", "b@new.com"] and bills[0]["setting_up"] == ["a@new.com"]
