"""The 10:30 post's numbers, counted by date (smartlead/daily_stats.py). No network."""
from datetime import date

from smartlead import daily_stats as ds


def _p(day, key="day"):
    d = date.fromisoformat(day)
    return ds.Period(key, d, d)


# ── which periods a post covers ────────────────────────────────────────────

def test_monday_covers_friday_only():
    (p,) = ds.periods_for(date(2026, 11, 9))
    assert (p.key, p.start, p.end) == ("day", date(2026, 11, 6), date(2026, 11, 6))


def test_midweek_covers_yesterday():
    (p,) = ds.periods_for(date(2026, 11, 4))
    assert p.start == p.end == date(2026, 11, 3)


def test_friday_adds_the_seven_days_friday_to_thursday():
    day, week = ds.periods_for(date(2026, 11, 6))
    assert day.start == date(2026, 11, 5)
    assert (week.key, week.start, week.end) == ("week", date(2026, 10, 30), date(2026, 11, 5))


def test_first_working_day_adds_last_month():
    # 1 Nov 2026 is a Sunday, so Monday 2 Nov is the first working day.
    day, month = ds.periods_for(date(2026, 11, 2))
    assert day.start == date(2026, 10, 30)
    assert (month.key, month.start, month.end) == ("month", date(2026, 10, 1), date(2026, 10, 31))
    assert [p.key for p in ds.periods_for(date(2026, 11, 3))] == ["day"]
    assert [p.key for p in ds.periods_for(date(2026, 12, 1))] == ["day", "month"]   # a Tuesday


def test_a_friday_that_is_the_first_working_day_gets_all_three():
    assert [p.key for p in ds.periods_for(date(2027, 1, 1))] == ["day", "week", "month"]


def test_a_day_is_a_utc_day():
    p = _p("2026-10-30")
    assert p.holds("2026-10-30T00:00:00Z") and p.holds("2026-10-30T23:59:59.999Z")
    assert not p.holds("2026-10-31T00:00:00Z") and not p.holds("2026-10-29T23:59:59Z")
    assert p.holds("2026-10-31T05:29:00+05:30")          # 23:59 UTC, written in IST
    assert not p.holds(None) and not p.holds("")


# ── email ──────────────────────────────────────────────────────────────────

def _s(email, sent=None, seq=1, reply=None, cat=None, bounced=False):
    return {"lead_email": email, "sent_time": sent, "sequence_number": seq,
            "reply_time": reply, "lead_category": cat, "is_bounced": bounced}


DAY = _p("2026-10-30")


def test_sent_reached_and_bounced_count_emails_sent_that_day():
    stats = [_s("a@x.com", "2026-10-30T10:00:00Z"),
             _s("b@x.com", "2026-10-30T11:00:00Z", seq=2),
             _s("c@x.com", "2026-10-30T12:00:00Z", bounced=True),
             _s("d@x.com", "2026-10-29T12:00:00Z")]
    c = ds.email_counts(stats, DAY)
    assert (c["sent"], c["reached"], c["bounced"]) == (3, 2, 1)


def test_replies_are_people_by_category():
    stats = [_s("a@x.com", reply="2026-10-30T10:00:00Z", cat="Interested"),
             _s("a@x.com", seq=2, reply="2026-10-30T11:00:00Z", cat="Interested"),
             _s("b@x.com", reply="2026-10-30T12:00:00Z", cat="Not Interested"),
             _s("c@x.com", reply="2026-10-30T12:00:00Z", cat="Wrong Person"),
             _s("d@x.com", reply="2026-10-30T12:00:00Z", cat=None),
             _s("e@x.com", reply="2026-10-29T12:00:00Z", cat="Interested")]
    c = ds.email_counts(stats, DAY)
    assert (c["replies"], c["positive"], c["not_interested"], c["wrong_person"]) == (4, 1, 1, 1)


def test_bounce_notices_are_not_replies_tagged_or_not():
    # 30 Oct: two untagged bounce notices, seconds after sending.
    stats = [_s("a@x.com", "2026-10-30T13:36:28Z", reply="2026-10-30T13:36:34Z", bounced=True),
             _s("b@x.com", "2026-10-30T13:43:00Z", reply="2026-10-30T13:43:00Z",
                cat="Sender Originated Bounce", bounced=True)]
    c = ds.email_counts(stats, DAY)
    assert c["replies"] == 0 and c["bounced"] == 2


def test_out_of_office_is_counted_on_its_own():
    stats = [_s("a@x.com", reply="2026-10-30T10:00:00Z", cat="Out Of Office")]
    c = ds.email_counts(stats, DAY, {})
    assert (c["replies"], c["out_of_office"]) == (0, 1)


def test_message_history_counts_a_second_reply_on_a_later_day():
    # 30 Oct, Laurie: first replied on the 28th, wrote again on the 30th.
    stats = [_s("a@x.com", reply="2026-10-28T18:24:53Z", cat="Interested")]
    times = {"a@x.com": ["2026-10-28T18:24:53Z", "2026-10-30T15:07:00+00:00"]}
    assert ds.email_counts(stats, DAY, times)["positive"] == 1
    assert ds.email_counts(stats, _p("2026-10-29"), times)["replies"] == 0


def test_repliers_whose_history_is_read_skip_bounces_and_out_of_office():
    from datetime import datetime, timezone
    stats = [_s("a@x.com", reply="2026-10-20T10:00:00Z", cat="Interested"),
             _s("b@x.com", reply="2026-10-20T10:00:00Z", bounced=True),
             _s("c@x.com", reply="2026-10-20T10:00:00Z", cat="Out Of Office"),
             _s("d@x.com", reply="2026-09-01T10:00:00Z", cat="Interested")]
    since = datetime(2026, 10, 1, tzinfo=timezone.utc)
    assert ds.repliers(stats, since) == ["a@x.com"]


# ── LinkedIn ───────────────────────────────────────────────────────────────

def test_heyreach_counts():
    o = {"overallStats": {"connectionsSent": 26, "connectionsAccepted": 5,
                          "totalMessageReplies": 1, "totalInmailReplies": 1,
                          "autoTaggedInterested": 1}}
    assert ds.heyreach_counts(o) == {"invites": 26, "accepted": 5, "replies": 2, "positive": 1}


def test_waalaxy_counts_replies_and_accepts_by_day():
    replies = [["Campaign", "Received at", "Full Name", "Profile URL", "Category", "Reply from Lead"],
               ["clay kochi event", "2026-10-30T10:00:00Z", "A", "li/a", "POSITIVE", "yes"],
               ["clay kochi event", "2026-10-30T11:00:00Z", "A", "li/a", "POSITIVE", "again"],
               ["clay kochi event", "2026-10-29T11:00:00Z", "B", "li/b", "NEUTRAL", "hm"]]
    accepts = [["Campaign", "Connected on"], ["clay kochi event", "2026-10-30"],
               ["clay kochi event", "2026-10-31"]]
    (r,) = ds.waalaxy_rows(replies, accepts, [DAY])
    assert r["counts"]["day"] == {"invites": None, "accepted": 1, "replies": 1, "positive": 1}


def test_a_quiet_campaign_is_left_out():
    replies = [["Campaign", "Received at", "Full Name", "Profile URL", "Category", "Reply from Lead"],
               ["old", "2026-10-01T10:00:00Z", "A", "li/a", "POSITIVE", "yes"]]
    assert ds.waalaxy_rows(replies, [], [DAY]) == []


def test_a_lead_whose_history_could_not_be_read_falls_back_to_the_first_reply():
    stats = [_s("a@x.com", reply="2026-10-30T10:00:00Z", cat="Interested")]
    assert ds.email_counts(stats, DAY, {"a@x.com": None})["positive"] == 1


def test_a_campaign_without_the_crm_sync_step_has_no_accept_count():
    replies = [["Campaign", "Received at", "Full Name", "Profile URL", "Category", "Reply from Lead"],
               ["kochi", "2026-10-30T10:00:00Z", "A", "li/a", "NEUTRAL", "hm"]]
    (r,) = ds.waalaxy_rows(replies, [["Campaign", "Connected on"]], [DAY])
    assert r["counts"]["day"]["accepted"] is None


def test_an_unreadable_accept_date_is_an_error_not_a_zero():
    import pytest
    with pytest.raises(ValueError):
        ds.waalaxy_rows([], [["Campaign", "Connected on"], ["kochi", "30/10/2026"]], [DAY])


def test_expandi_invites_and_accepts_come_from_each_leads_dates():
    # 3 Nov: the cache said 0 invites for Monday; Expandi's lead rows had 31+.
    leads = [{"invited_at": "2026-10-30T09:00:00+0200", "connected_at": "2026-10-30T21:00:00+0200"},
             {"invited_at": "2026-10-30T01:30:00+0200", "connected_at": None},   # 29 Oct 23:30 UTC
             {"invited_at": None, "connected_at": None}]
    assert ds.expandi_counts(leads, DAY) == {"invites": 1, "accepted": 1}
