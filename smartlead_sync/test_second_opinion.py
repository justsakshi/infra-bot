"""The bill review's second opinion (11 Nov 2026): Zapmail's own placement test beside the v2 result. One test
failed every Google inbox of a client while a second seed network and a real mailbox accepted half of them, so a
drop needs two tests that agree. No network."""
from datetime import date

from smartlead import renewal_review as rr
from smartlead import zapmail_placement as zp

T = date(2026, 10, 13)          # a Tuesday: 5 Oct and 12 Oct are weekly (Monday) v2 nights


def _row(email, provider, seed, placement, order="o1", at="2026-10-12T13:46:00.000Z", status="SUCCESS"):
    return {"fromEmail": email, "serviceProvider": provider, "accountType": seed, "placement": placement,
            "status": status, "createdAt": at, "placementTestCartOrderId": order}


def test_each_mailbox_is_read_at_its_own_provider():
    rows = [_row("g@x.com", "GOOGLE", "GOOGLE", "SPAM"), _row("g@x.com", "GOOGLE", "GOOGLE", "SPAM"),
            _row("g@x.com", "GOOGLE", "MICROSOFT365", "INBOX"), _row("g@x.com", "GOOGLE", "MICROSOFT365", "INBOX"),
            _row("o@x.com", "MICROSOFT", "MICROSOFT365", "INBOX"), _row("o@x.com", "MICROSOFT", "MICROSOFT365", "IMPORTANT"),
            _row("o@x.com", "MICROSOFT", "GOOGLE", "SPAM"),
            _row("m@x.com", "GOOGLE", "GOOGLE", "SPAM"), _row("m@x.com", "GOOGLE", "GOOGLE", "INBOX"),
            _row("m@x.com", "GOOGLE", "GOOGLE", "INBOX"),
            _row("p@x.com", "GOOGLE", "GOOGLE", "OTHER", status="IN_PROGRESS")]
    got = zp.own_provider_opinion(rows)
    assert got["g@x.com"] == {"verdict": "FAIL", "inbox": 0, "sent": 2, "on": "2026-10-12", "source": zp.SOURCE}
    assert got["o@x.com"]["verdict"] == "PASS" and got["o@x.com"]["sent"] == 2        # the Google-seed spam is not its own
    assert got["m@x.com"]["verdict"] == "MIXED"                                        # 1 of 3 in spam: no opinion
    assert "p@x.com" not in got                                                         # nothing finished yet


def test_only_the_newest_test_of_a_mailbox_counts():
    rows = [_row("g@x.com", "GOOGLE", "GOOGLE", "SPAM", order="old", at="2026-09-17T10:00:00.000Z"),
            _row("g@x.com", "GOOGLE", "GOOGLE", "INBOX", order="new", at="2026-10-12T10:00:00.000Z")]
    assert zp.own_provider_opinion(rows)["g@x.com"] == {"verdict": "PASS", "inbox": 1, "sent": 1, "on": "2026-10-12",
                                                        "source": zp.SOURCE}


def _facts():
    return {"in_smartlead": True, "connected": True, "reputation": "98%", "tests": [], "campaigns": [], "tags": []}


def _v2(email, *nights):
    return [{"email": email, "night": n, "verdict": v, "weekly": date.fromisoformat(n).weekday() == 0, "pulled_on": None}
            for n, v in nights]


def _review(v2_nights, opinion, tags=()):
    e = "a@x.com"
    facts = {e: {**_facts(), "tags": list(tags)}}
    bill = {"provider": "Zapmail", "label": "1 Google", "bills_on": "2026-10-20", "price": 3.25, "inboxes": [e]}
    second = None if opinion is None else ({e: opinion} if opinion else {})
    return rr.review_bill(bill, facts, T, True, v2={e: _v2(e, *v2_nights)}, second=second)["rows"][0]


def _op(verdict, on="2026-10-12", inbox=2, sent=2):
    return {"verdict": verdict, "inbox": inbox if verdict == "PASS" else 0, "sent": sent, "on": on, "source": "Zapmail's own test"}


TWO_FAILS = (("2026-10-05", "FAIL"), ("2026-10-12", "FAIL"))


def test_a_drop_on_v2_alone_becomes_a_check_when_the_second_test_disagrees():
    alone = _review(TWO_FAILS, None)
    assert alone["verdict"] == "RETIRE"                                   # as before: no second test read
    row = _review(TWO_FAILS, _op("PASS"))
    assert row["verdict"] == "CHECK"
    assert row["why"] == ("failed v2 on the weekly tests of 5 Oct and 12 Oct; but Zapmail's own test on 12 Oct put it "
                          "in the inbox (2 of 2): two tests disagree, so not dropped on one")


def test_two_tests_that_agree_keep_the_drop_and_say_so():
    row = _review(TWO_FAILS, _op("FAIL"))
    assert row["verdict"] == "RETIRE"
    assert row["why"].endswith("Zapmail's own test on 12 Oct put it in spam (0 of 2) too")


def test_a_pass_on_v2_becomes_a_check_when_the_second_test_puts_it_in_spam():
    assert _review((("2026-10-12", "PASS"),), {})["verdict"] == "KEEP"
    row = _review((("2026-10-12", "PASS"),), _op("FAIL"))
    assert row["verdict"] == "CHECK"
    assert row["why"] == ("passed v2 on 12 Oct; but Zapmail's own test on 12 Oct put it in spam (0 of 2): two tests "
                          "disagree")
    both = _review((("2026-10-12", "PASS"),), _op("PASS"))
    assert both["verdict"] == "KEEP" and both["why"].endswith("Zapmail's own test on 12 Oct put it in the inbox (2 of 2) too")


def test_an_old_or_mixed_second_test_and_a_persons_tag_change_nothing():
    assert _review(TWO_FAILS, _op("PASS", on="2026-09-17"))["verdict"] == "RETIRE"        # older than the fresh window
    assert _review(TWO_FAILS, {**_op("PASS"), "verdict": "MIXED"})["verdict"] == "RETIRE"
    tagged = _review(TWO_FAILS, _op("PASS"), tags=["DO NOT USE - Google spam"])
    assert tagged["verdict"] == "RETIRE" and tagged["why"].startswith('tagged "DO NOT USE')  # a person's call stands


def test_the_second_test_is_a_source_that_can_change_a_verdict():
    err = f"{rr.SECOND_SOURCE} (Zapmail placement): TimeoutError"
    assert rr.decision_errors([err]) == [err]
    assert "two tests disagree" in rr.read_warning([err], 7, True) or "second" in rr.read_warning([err], 7, True)
    assert "a bill may be missing" not in rr.read_warning([err], 7, True)       # not mistaken for the Zapmail bills read


# --- the reply check as a second opinion (12 Nov 2026): for inboxes with no second placement test ---------------

from smartlead import reply_stats as rs


def _day_doc(verdict, date_="2026-10-12", sent=420, replies=11, domain="x.com"):
    return {"domain": domain, "date": date_, "verdict": verdict, "window_sent": sent, "window_replies": replies}


def test_the_reply_check_is_an_opinion_only_when_it_has_a_view():
    ok = rs.second_opinion(_day_doc("OK"))
    assert ok["verdict"] == "PASS" and ok["source"] == "the reply check" and ok["on"] == "2026-10-12"
    assert ok["said"] == "its domain got 11 replies on 420 emails in 14 days, in line with the client's other domains"
    assert rs.second_opinion(_day_doc("ALERT", replies=1))["said"].endswith("1 reply on 420 emails in 14 days, far below "
                                                                           "the client's other domains")
    for none in ("WATCH", "TOO FEW", "NO REFERENCE", "NO_SENDS", None):
        assert rs.second_opinion(_day_doc(none)) is None
    assert rs.second_opinion(_day_doc("OK", sent=20)) is None                # under the reply check's own floor
    assert rs.second_opinion({}) is None


def test_only_a_domains_newest_fresh_day_is_read():
    docs = [_day_doc("ALERT", "2026-10-11"), _day_doc("OK", "2026-10-12"), _day_doc("OK", "2026-10-05", domain="old.com"),
            _day_doc("WATCH", "2026-10-12", domain="w.com"), _day_doc("OK", "2026-10-14", domain="later.com")]
    got = rs.second_opinions(docs, T)
    assert set(got) == {"@x.com"} and got["@x.com"]["verdict"] == "PASS"      # newest day wins; old, later and no-view are out


def test_replies_that_disagree_with_a_failed_test_make_it_a_check():
    e = "a@x.com"
    bill = {"provider": "ScaledMail", "label": "1 Google", "bills_on": "2026-10-20", "price": 3.5, "inboxes": [e]}
    second = rs.second_opinions([_day_doc("OK")], T)
    row = rr.review_bill(bill, {e: _facts()}, T, True, v2={e: _v2(e, *TWO_FAILS)}, second=second)["rows"][0]
    assert row["verdict"] == "CHECK"
    assert row["why"] == ("failed v2 on the weekly tests of 5 Oct and 12 Oct; but the reply check on 12 Oct: its domain "
                          "got 11 replies on 420 emails in 14 days, in line with the client's other domains: the test "
                          "and the replies disagree, so not dropped on one")
    agree = rs.second_opinions([_day_doc("ALERT", replies=0)], T)
    row = rr.review_bill(bill, {e: _facts()}, T, True, v2={e: _v2(e, *TWO_FAILS)}, second=agree)["rows"][0]
    assert row["verdict"] == "RETIRE" and row["why"].endswith("far below the client's other domains too")


def test_an_inboxs_own_second_test_is_read_before_its_domains_replies():
    e = "a@x.com"
    bill = {"provider": "Zapmail", "label": "1 Google", "bills_on": "2026-10-20", "price": 3.25, "inboxes": [e]}
    second = {**rs.second_opinions([_day_doc("OK")], T), e: _op("FAIL")}
    row = rr.review_bill(bill, {e: _facts()}, T, True, v2={e: _v2(e, *TWO_FAILS)}, second=second)["rows"][0]
    assert row["verdict"] == "RETIRE" and "Zapmail's own test" in row["why"] and "reply check" not in row["why"]


def test_a_reply_check_that_cannot_be_read_is_named_with_its_effect():
    err = f"{rr.REPLY_SOURCE} (Mongo domain_reply_stats): ServerSelectionTimeoutError"
    assert rr.decision_errors([err]) == [err]
    assert "a bill may be missing" not in rr.read_warning([err], 7, True)
