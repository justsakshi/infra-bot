"""Renewal review + billing watch as Slack headline + thread (spec 2026-10-13; rule = decision 4,
7 Nov 2026: v2 results, DO NOT USE tags and Spamhaus DBL decide, classic tests only warn). No network."""
import re
from datetime import date, timedelta

from smartlead import billing_watch as bw
from smartlead import renewal_review as rr

T = date(2026, 10, 13)          # a Tuesday: 5 Oct and 12 Oct are the weekly (Monday) v2 nights
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
EXPLAIN = "removes the inboxes at this bill"
DNU = "DO NOT USE - Google spam"


def _facts(pct=95.0, tested_on="2026-10-10", seeds=20, **kw):
    tests = [{"date": tested_on, "seeds": seeds, "spam": round(seeds * (100 - pct) / 100), "by": {}}]
    base = {"in_smartlead": True, "connected": True, "reputation": "98%", "tests": tests, "campaigns": [], "tags": []}
    base.update(kw)
    return base


def _v2(email, *nights, pulled_on=None):
    return [{"email": email, "domain": email.split("@")[1], "night": n, "verdict": v,
             "weekly": date.fromisoformat(n).weekday() == 0, "pulled_on": pulled_on} for n, v in nights]


def _reviews():
    """Four bills: a Zapmail Outlook subscription with a domain that failed v2 two Mondays running,
    a ScaledMail order with a DO NOT USE domain, a DBL domain (kept, with a note) and an untested inbox, a clean
    Zapmail one (one inbox with a bad classic test), and a pre-warmed plan with no inbox list."""
    facts = {f"{u}@twofails.com": _facts(campaigns=["BD Ingest"] if u == "amy" else [])
             for u in ("amy", "ben", "cal")}
    facts.update({"dan@mixed.com": _facts(), "eve@mixed.com": _facts(reputation="55%")})
    facts.update({"fay@plbad.com": _facts(tags=[DNU]), "gus@plbad.com": _facts(tags=[DNU]),
                  "hal@plgood.com": _facts(), "ida@plgood.com": _facts(),
                  "jen@listed.com": _facts(), "lou@listed.com": _facts()})
    facts.update({"jo@clean.com": _facts(), "kim@clean.com": _facts(pct=60.0)})
    v2 = {e: _v2(e, ("2026-10-05", "FAIL"), ("2026-10-12", "FAIL")) for e in facts if e.endswith("twofails.com")}
    v2.update({e: _v2(e, ("2026-10-12", "PASS")) for e in facts
               if e.split("@")[1] in ("mixed.com", "listed.com", "clean.com") or e == "hal@plgood.com"})
    dbl = {"listed.com": {"date": "2026-10-12", "listed_on": ["Spamhaus DBL (127.0.1.2)"]},
           "clean.com": {"date": "2026-10-12", "listed_on": []}}
    z = rr.review_bill({"provider": "Zapmail", "label": "5 Outlook inboxes", "bills_on": "2026-10-15", "price": 15.0,
                        "clients": ["Bettrdata"], "subscription_id": "sub_1",
                        "inboxes": ["amy@twofails.com", "ben@twofails.com", "cal@twofails.com",
                                    "dan@mixed.com", "eve@mixed.com"]}, facts, T, v2=v2, dbl=dbl)
    s = rr.review_bill({"provider": "ScaledMail", "label": "6 × Google Inboxes", "bills_on": "2026-10-17", "price": 14.0,
                        "clients": ["Precise Leads"], "order_id": "rec3",
                        "inboxes": ["fay@plbad.com", "gus@plbad.com", "hal@plgood.com", "ida@plgood.com",
                                    "jen@listed.com", "lou@listed.com"]}, facts, T, v2=v2, dbl=dbl)
    c = rr.review_bill({"provider": "Zapmail", "label": "2 Google inboxes", "bills_on": "2026-10-19", "price": 6.0,
                        "clients": ["Precise Leads"], "inboxes": ["jo@clean.com", "kim@clean.com"]}, facts, T, v2=v2, dbl=dbl)
    p = rr.review_bill({"provider": "Zapmail", "label": "12 Outlook pre-warmed", "bills_on": "2026-10-20", "price": 84,
                        "clients": [], "inboxes": [], "mailboxes": 12,
                        "note": "pre-warmed slots: Zapmail does not list which inboxes sit on this plan; review them by domain"},
                       facts, T, v2=v2, dbl=dbl)
    return [z, s, c, p]


def _part(parts, title):
    return next(p for p in parts if p.startswith(title)).split("\n")


def test_review_headline_counts_add_up():
    head, parts = rr.format_review(_reviews(), 7)
    assert head == ["⚠️ Bills in the next 7 days: 4 bills, $119.00",
                    "Of 13 inboxes: 5 to drop before they renew (saves $13.67/mo), 1 to check, 1 untested, 6 to keep",
                    "1 bill ($84.00) not reviewed: no inbox list"]
    assert not any(EMAIL.search(l) for l in head)


def test_each_bill_has_price_counts_and_cost_per_passing_inbox():
    head, parts = rr.format_review(_reviews(), 7)
    each = _part(parts, "*Each bill*")
    assert parts[0].startswith("*Each bill*")
    assert each[1:] == [
        "• Bill on 15 Oct · Zapmail · 5 Outlook inboxes · $15.00 (Bettrdata): 1 keep, 1 check, 3 drop, 0 untested "
        "· $7.50 per passing inbox (2 passed v2 in the last 8 days)",
        "• Bill on 17 Oct · ScaledMail · 6 × Google Inboxes · $14.00 (Precise Leads): 3 keep, 0 check, 2 drop, 1 untested "
        "· $4.67 per passing inbox (3 passed v2 in the last 8 days)",
        "• Bill on 19 Oct · Zapmail · 2 Google inboxes · $6.00 (Precise Leads): 2 keep, 0 check, 0 drop, 0 untested "
        "· $3.00 per passing inbox (2 passed v2 in the last 8 days)",
        "• Bill on 20 Oct · Zapmail · 12 Outlook pre-warmed · $84.00: not reviewed (no inbox list)"]


def test_review_drop_lines_per_bill_by_domain_with_the_reason():
    head, parts = rr.format_review(_reviews(), 7)
    drop = _part(parts, "*Drop before the bill*")
    assert drop[1] == ("• Bill on 15 Oct · Zapmail · 5 Outlook inboxes · $15.00 (Bettrdata): drop twofails.com "
                       "(3 inboxes: failed v2 on the weekly tests of 5 Oct and 12 Oct), saves $9.00/mo "
                       "· ⚠️ amy@twofails.com is still in BD Ingest")
    assert drop[2] == ("• Bill on 17 Oct · ScaledMail · 6 × Google Inboxes · $14.00 (Precise Leads): drop plbad.com "
                       f'(2 inboxes: tagged "{DNU}" in Smartlead), saves $4.67/mo')
    assert len(drop) == 3


def test_review_checks_untested_and_not_reviewed():
    head, parts = rr.format_review(_reviews(), 7)
    assert _part(parts, "*Check before paying*")[1:] == ["• `eve@mixed.com` (bill 15 Oct): warmup reputation 55%"]
    # a Spamhaus listing is a note under its own heading, not a reason to check (Manveen, 11 Nov 2026)
    assert _part(parts, "*On Spamhaus DBL (a note only: the v2 result decides keep, check or drop)*")[1:] == [
        "• Bill on 17 Oct (Precise Leads): listed.com (2 inboxes): domain on Spamhaus DBL (check of 12 Oct)"]
    assert _part(parts, "*Untested: no v2 result in the last 8 days, so not kept*")[1:] == [
        "• Bill on 17 Oct (Precise Leads): plgood.com (1 inbox)"]
    assert _part(parts, "*Not reviewed*")[1:] == [
        "• Bill on 20 Oct · Zapmail · 12 Outlook pre-warmed · $84.00: no inbox list, so no inbox was judged "
        "· pre-warmed slots: Zapmail does not list which inboxes sit on this plan; review them by domain · no client named"]
    assert not any(p.startswith("*Nothing to drop*") for p in parts)


def test_classic_results_are_a_note_not_a_verdict():
    head, parts = rr.format_review(_reviews(), 7)
    # kim's 60% pulls clean.com's pooled classic result to 78%: the old kill rule's "dead domain"
    assert _part(parts, "*Classic tests (early warning only, never a verdict)*")[1:] == [
        "• Bill on 19 Oct (Precise Leads): clean.com (2 inboxes): classic test: whole domain: 78% of 40 test "
        "emails reached the inbox (needs 80%), 1 of 2 inboxes failed"]
    c = _reviews()[2]
    assert c["counts"]["KEEP"] == 2 and c["counts"]["RETIRE"] == 0 and c["support_message"] == ""


def test_review_how_to_and_support_messages_last():
    head, parts = rr.format_review(_reviews(), 7)
    msgs = [p for p in parts if p.startswith("_Message for")]
    assert len(msgs) == 2 and all(p.count("```") == 2 for p in msgs)
    assert "failed our inbox-placement tests of 5 Oct and 12 Oct" in msgs[0] and "we no longer use them" in msgs[1]
    assert sum(p.count(EXPLAIN) for p in parts) == 1 and EXPLAIN in parts[-1]
    assert "/domains" in parts[-1]
    # ScaledMail: single domains are cancelled from the billing page, never the whole order (6 Nov)
    assert "cancel single domains from its billing page, not the whole order" in parts[-1]
    assert "cancelling the order" not in parts[-1]


def test_review_all_kept_is_green():
    clean = [r for r in _reviews() if r.get("reviewed") and r["counts"]["KEEP"] == len(r["rows"])]
    head, parts = rr.format_review(clean, 7)
    assert head == ["✅ Bills in the next 7 days: 1 bill, $6.00", "Of 2 inboxes: nothing to drop, 2 to keep"]
    assert parts[0].startswith("*Each bill*")


def test_unknown_price_is_said_not_shown_as_zero():
    r = rr.review_bill({"provider": "Zapmail", "label": "3 Google inboxes", "bills_on": "2026-10-15", "price": None,
                        "inboxes": ["a@x.com"]}, {"a@x.com": _facts()}, T,
                       v2={"a@x.com": _v2("a@x.com", ("2026-10-12", "PASS"))}, dbl={})
    head, parts = rr.format_review([r], 7)
    assert head[0] == "✅ Bills in the next 7 days: 1 bill, $0.00 + 1 price not known"
    assert _part(parts, "*Each bill*")[1] == ("• Bill on 15 Oct · Zapmail · 3 Google inboxes · price not known: "
                                               "1 keep, 0 check, 0 drop, 0 untested · 1 passed v2 in the last 8 days")


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


def test_scaledmail_bills_without_inbox_lists_make_no_mailbox_calls(monkeypatch):
    import renewal_review as cli
    import smartlead.scaledmail as sm_mod
    import smartlead.scaledmail_fleet as fleet

    class NoMailboxes:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def mailboxes(self, domain_id): raise AssertionError("no per-domain calls")

    monkeypatch.setattr(sm_mod, "configured", lambda: True)
    monkeypatch.setattr(sm_mod, "ScaledMailClient", NoMailboxes)
    monkeypatch.setattr(fleet, "snapshot", lambda sm: {
        "orders": [{"id": "rec9", "billing_day": "2026-10-15", "description": "2 × Google", "amount": 7, "clients": ["Precise Leads"]}],
        "domains": [{"order_id": "rec9", "domain_id": "d1", "domain": "new.com"}]})
    bills = cli.scaledmail_bills(30, [], with_inboxes=False)
    assert bills[0]["domains"] == ["new.com"] and bills[0]["inboxes"] == []


# ---- the poster (renewal_review.py): run, read errors, crashes ----

def _fake_run(monkeypatch, cli, bills, facts, zap_err=None, sm_err=None, v2=None, dbl=None, readers=True):
    monkeypatch.setattr(cli, "zapmail_bills", lambda d, e: (e.append(zap_err) if zap_err else None) or [])
    monkeypatch.setattr(cli, "scaledmail_bills", lambda d, e: (e.append(sm_err) if sm_err else None) or bills)
    monkeypatch.setattr(cli, "smartlead_facts", lambda e: facts)
    if readers:
        monkeypatch.setattr(cli, "v2_results", lambda doms, emails, today, e: {} if v2 is None else v2)
        monkeypatch.setattr(cli, "dbl_listings", lambda doms, e: {} if dbl is None else dbl)
        monkeypatch.setattr(cli, "reply_opinions", lambda doms, today, e: {})


def _capture_posts(monkeypatch, cli, *argv):
    import sys
    from smartlead import notify
    sent = []
    monkeypatch.setattr(notify, "post_with_thread", lambda *a: sent.append(a) or "1.1")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("RENEWAL_REVIEW_CHANNEL", "C1")
    monkeypatch.setattr(sys, "argv", ["renewal_review.py", *argv])
    return sent


def _bills(client="Precise Leads"):
    due = date.today() + timedelta(days=2)   # inside run(7)'s window whatever day the suite runs
    return due, [{"provider": "ScaledMail", "label": "2 × Google", "bills_on": due.isoformat(), "price": 7.0,
                  "clients": [client], "inboxes": ["a@x.com", "b@x.com"]}]


def test_poster_run_posts_headline_and_thread(monkeypatch):
    import renewal_review as cli
    due, bills = _bills()
    night = (date.today() - timedelta(days=3)).isoformat()
    _fake_run(monkeypatch, cli, bills, {"a@x.com": _facts(), "b@x.com": _facts()},
              zap_err="Zapmail PRECISE_LEADS/GOOGLE: 500",
              v2={e: _v2(e, (night, "PASS")) for e in ("a@x.com", "b@x.com")})
    res = cli.run(7)
    assert res["headline"][0] == "⚠️ Bills in the next 7 days: 1 bill, $7.00"       # never green over a half read
    assert res["headline"][1] == "Of 2 inboxes: nothing to drop, 2 to keep"
    assert res["headline"][-1] == ":warning: Could not read Zapmail, so a bill may be missing (details in the thread)"
    assert len(res["headline"]) <= 4
    assert "Zapmail PRECISE_LEADS/GOOGLE: 500" in "\n".join(res["thread"]) and "removes the inboxes" in res["thread"][-1]
    assert res["text"].startswith(res["headline"][0]) and f"Bill on {due.day} {due:%b}" in res["text"]
    sent = _capture_posts(monkeypatch, cli, "--post")
    monkeypatch.setattr(cli, "run", lambda days: res)
    assert cli.main() == 0 and res["posted"] is True
    assert sent[0][1] == "C1" and sent[0][2] == res["headline"] and sent[0][3] == res["thread"]


def test_read_errors_post_even_when_no_bill_is_due(monkeypatch):
    import renewal_review as cli
    _fake_run(monkeypatch, cli, [], {}, zap_err="Zapmail PRECISE_LEADS/GOOGLE: 500", sm_err="ScaledMail: 503")
    res = cli.run(7)
    assert res["reviews"] == []
    assert res["headline"] == ["⚠️ Renewal review: could not read Zapmail and ScaledMail, so a bill in the next "
                               "7 days may be missing"]
    assert res["thread"] == [":warning: could not read: Zapmail PRECISE_LEADS/GOOGLE: 500; ScaledMail: 503"]
    sent = _capture_posts(monkeypatch, cli, "--post", "--days", "7")
    assert cli.main() == 0
    assert sent and sent[0][2] == res["headline"] and sent[0][3] == res["thread"]


def test_nothing_due_and_nothing_wrong_posts_nothing(monkeypatch):
    import renewal_review as cli
    _fake_run(monkeypatch, cli, [], {})
    sent = _capture_posts(monkeypatch, cli, "--post")
    assert cli.main() == 0 and sent == []


def test_a_crash_is_posted(monkeypatch):
    import renewal_review as cli

    def boom(days):
        raise RuntimeError("Mongo went away")
    monkeypatch.setattr(cli, "run", boom)
    sent = _capture_posts(monkeypatch, cli, "--post", "--flag-tracker")
    assert cli.main() == 1
    assert sent[0][2] == ["🚨 Renewal review crashed, so no bill was checked today"]
    assert sent[0][3] == ["Error: RuntimeError: Mongo went away", "The tracker's renewal flags were not changed."]


def test_unread_v2_keeps_nothing_says_so_and_writes_no_flags(monkeypatch):
    import renewal_review as cli
    due, bills = _bills("Melior")
    _fake_run(monkeypatch, cli, bills, {"a@x.com": _facts(), "b@x.com": _facts(tags=["do not use"])}, readers=False)

    def unreadable(name):
        raise RuntimeError(f"{name}: server selection timed out")
    monkeypatch.setattr(cli, "_collection", unreadable)
    res = cli.run(7)
    rows = {x["email"]: (x["verdict"], x["why"]) for x in res["reviews"][0]["rows"]}
    assert rows == {"a@x.com": ("CHECK", "v2 results could not be read, so not kept"),
                    "b@x.com": ("RETIRE", 'tagged "do not use" in Smartlead')}     # a tag needs no v2
    assert res["headline"][-1] == (":warning: Could not read v2 results and blocklist checks, so no inbox is marked "
                                   "keep and Spamhaus listing notes are missing (details in the thread)")
    assert "v2 results (Mongo v2_verdicts): v2_verdicts: server selection timed out" in "\n".join(res["thread"])
    sent = _capture_posts(monkeypatch, cli, "--post", "--flag-tracker")
    monkeypatch.setattr(cli, "run", lambda days: res)
    assert cli.main() == 0
    assert res["flagged"]["written"] == 0
    assert "_Tracker renewal flags were not updated this run (some sources could not be read)._" in sent[0][3]
    assert res["thread"][0].split("\n")[1].endswith("0 keep, 1 check, 1 drop, 0 untested · v2 results not read")
    assert "1 of its inboxes is not working for us" in res["reviews"][0]["support_message"]


def test_a_classic_only_failure_still_gives_verdicts_a_true_headline_and_flags(monkeypatch):
    """Review R3: /email-accounts read, SmartDelivery classic raised 502. Every inbox has a
    verdict, so the post must not say Smartlead could not be read, and the flags are written."""
    import renewal_review as cli
    import smartlead.zapmail_asset_sync as zas
    due, bills = _bills()
    night = (date.today() - timedelta(days=3)).isoformat()
    _fake_run(monkeypatch, cli, bills, {}, v2={e: _v2(e, (night, "PASS")) for e in ("a@x.com", "b@x.com")})
    facts = {"a@x.com": _facts(), "b@x.com": _facts()}
    monkeypatch.setattr(cli, "smartlead_facts", lambda e: e.append("classic tests PRECISE_LEADS: 502") or facts)
    res = cli.run(7)
    assert [x["verdict"] for x in res["reviews"][0]["rows"]] == ["KEEP", "KEEP"]
    assert res["headline"] == ["⚠️ Bills in the next 7 days: 1 bill, $7.00",
                               "Of 2 inboxes: nothing to drop, 2 to keep",
                               ":warning: Could not read classic tests, so classic early warnings are missing "
                               "(details in the thread)"]
    assert "Smartlead" not in "\n".join(res["headline"])
    assert ":warning: could not read: classic tests PRECISE_LEADS: 502" in res["thread"]

    class Assets:
        def bulk_write(self, ops, ordered):
            return type("R", (), {"modified_count": len(ops)})()
    monkeypatch.setattr(zas, "_db", lambda: {"assets": Assets()})
    sent = _capture_posts(monkeypatch, cli, "--post", "--flag-tracker")
    monkeypatch.setattr(cli, "run", lambda days: res)
    assert cli.main() == 0 and res["flagged"] == {"written": 2}
    assert not any("Tracker renewal flags were not updated" in p for p in sent[0][3])


def test_unused_and_setting_up_are_not_called_drop():
    facts = {"k1@dead.com": _facts(tags=[DNU]), "k2@dead.com": _facts(tags=[DNU])}
    b = rr.review_bill({"provider": "ScaledMail", "label": "7 × Google", "bills_on": "2026-11-12", "price": 31.5,
                        "clients": ["Precise Leads"], "order_id": "rec9",
                        "inboxes": ["k1@dead.com", "k2@dead.com", "u1@gone.com", "u2@gone.com",
                                    "s1@new.com", "s2@new.com", "s3@new.com"],
                        "setting_up": ["s1@new.com", "s2@new.com", "s3@new.com"]}, facts, T, v2={}, dbl={})
    head, parts = rr.format_review([b], 7)
    assert head[1] == ("Of 4 inboxes: 2 to drop before they renew (saves $9.00/mo), "
                       "2 to check (2 of them paid for but not in Smartlead), 0 to keep")
    drop = _part(parts, "*Drop before the bill*")[1]
    assert "drop dead.com (2 inboxes" in drop and "saves $9.00/mo" in drop and "3 inboxes still being set up, left alone" in drop
    assert "gone.com" not in drop and "new.com" not in "\n".join(parts)
    unused = next(p for p in parts if p.startswith("*Paying for inboxes not in Smartlead*"))
    assert "gone.com (2 inboxes)" in unused and "($9.00/mo)" in unused and "Connect them" in unused
    assert "gone.com" not in next((p for p in parts if p.startswith("*Check before paying*")), "")
    assert _part(parts, "*Each bill*")[1].endswith("0 keep, 2 check, 2 drop, 0 untested, 3 still being set up "
                                                   "· none passed v2 in the last 8 days")


def test_untested_is_one_line_per_bill_with_domains():
    facts = {f"n{i}@m.com": _facts() for i in range(5)}
    facts["o@n.com"] = _facts()
    facts["r@m.com"] = _facts(reputation="55%")
    old = {e: _v2(e, ("2026-10-02", "PASS")) for e in facts}       # 11 days before T: too old to count
    b = rr.review_bill({"provider": "ScaledMail", "label": "7 × Google", "bills_on": "2026-11-06", "price": 21.0,
                        "clients": ["Melior"], "inboxes": list(facts)}, facts, T, v2=old, dbl={})
    head, parts = rr.format_review([b], 7)
    assert _part(parts, "*Untested: no v2 result in the last 8 days, so not kept*")[1:] == [
        "• Bill on 6 Nov (Melior): m.com (5 inboxes), n.com (1 inbox)"]
    assert _part(parts, "*Check before paying*")[1:] == [
        "• `r@m.com` (bill 6 Nov): warmup reputation 55%; no v2 result in the last 8 days"]
    assert b["counts"] == {"KEEP": 0, "CHECK": 1, "RETIRE": 0, "UNTESTED": 6}
    assert b["cost_per_passing"] is None
