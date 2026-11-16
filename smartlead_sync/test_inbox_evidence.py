"""Per-inbox evidence from real sends (t17): exact sender per email, per inbox x recipient provider. No network."""
from datetime import date

from smartlead import inbox_evidence as ie

TODAY = date(2026, 11, 17)


def row(sid, day, lead="l@a.com", bounced=False, reply=None, cat=None):
    return {"stats_id": sid, "lead_email": lead, "sent_time": f"2026-11-{day:02d}T10:00:00Z", "is_bounced": bounced,
            "reply_time": reply, "lead_category": cat}


def hist(*sent, reply_body=None):
    h = [{"type": "SENT", "stats_id": s, "from": f}  for s, f in sent]
    if reply_body is not None:
        h.append({"type": "REPLY", "email_body": reply_body})
    return {"history": h}


def test_each_email_is_counted_for_the_inbox_that_sent_it_even_when_steps_came_from_two_inboxes():
    lead = ie.Lead(rows=[row("s1", 10), row("s2", 14, reply="2026-11-15T09:00:00Z")],
                   history=hist(("s1", "A@x.com"), ("s2", "b@x.com"), reply_body="Thanks, tell me more"))
    cells, unknown = ie.tally({"l@a.com": lead}, lambda d: "google", TODAY)
    assert cells[("a@x.com", "Gmail")]["sent"] == 1 and cells[("a@x.com", "Gmail")]["replies"] == 0
    assert cells[("b@x.com", "Gmail")]["replies"] == 1 and cells[("b@x.com", "Gmail")]["replies7"] == 1
    assert unknown == 0


def test_ooo_bounces_and_bounce_notices_are_kept_apart_from_replies():
    leads = {"o@a.com": ie.Lead([row("s1", 12, lead="o@a.com", reply="2026-11-12T11:00:00Z", cat="Out Of Office")],
                                hist(("s1", "a@x.com"), reply_body="I am out of the office")),
             "b@a.com": ie.Lead([row("s2", 12, lead="b@a.com", bounced=True)], hist(("s2", "a@x.com"))),
             "n@a.com": ie.Lead([row("s3", 12, lead="n@a.com", reply="2026-11-12T11:00:00Z")],
                                hist(("s3", "a@x.com"), reply_body="Delivery has failed to these recipients"))}
    c = ie.tally(leads, lambda d: "microsoft", TODAY)[0][("a@x.com", "Microsoft")]
    assert (c["sent"], c["replies"], c["ooo"], c["bounces"]) == (3, 0, 1, 2)


def test_sends_outside_the_window_are_not_counted_and_unknown_senders_are():
    leads = {"l@a.com": ie.Lead([row("old", 1), row("s9", 16)], hist(("old", "a@x.com")))}   # s9 not in history
    cells, unknown = ie.tally(leads, lambda d: "google", TODAY, days=14)
    assert sum(c["sent"] for c in cells.values()) == 0 and unknown == 1


def test_labels_never_say_fine_without_data():
    rate = {"Gmail": 0.02}
    assert ie.label({"sent": 40, "replies": 0, "ooo": 0}, "Gmail", rate) == "not enough data"     # 0.8 expected
    assert ie.label({"sent": 400, "replies": 0, "ooo": 0}, "Gmail", rate).startswith("quiet")     # 8 expected
    assert ie.label({"sent": 40, "replies": 0, "ooo": 1}, "Gmail", rate) == "reached the inbox"
    assert ie.label({"sent": 400, "replies": 0, "ooo": 0}, "Gmail", {}) == "not enough data"      # no base rate


def test_inbox_summary_puts_quiet_first():
    cells = {("a@x.com", "Gmail"): {"sent": 400, "replies": 3, "ooo": 0, "bounces": 1, "sent7": 90},
             ("a@x.com", "Microsoft"): {"sent": 300, "replies": 0, "ooo": 0, "bounces": 0, "sent7": 70}}
    rate = ie.base_rates(cells | {("b@x.com", "Microsoft"): {"sent": 1000, "replies": 20, "ooo": 5}})
    s = ie.inbox_rows(cells, rate)["a@x.com"]
    assert s["label"].startswith("quiet at Microsoft") and "reached the inbox at Gmail" in s["label"]
    assert "Gmail 400 sent, 3 replies, 0 OOO" in s["evidence"] and "bounces 1" in s["evidence"]


def test_crosscheck_against_the_teams_reply_log_counts_matches_and_names_each_miss():
    logged = [{"email": "A@a.com", "date": "2026-11-12"}, {"email": "b@a.com", "date": "2026-11-13"},
              {"email": "c@a.com", "date": "2026-11-13"}, {"email": "d@a.com", "date": "2026-11-14"},
              {"email": "old@a.com", "date": "2026-09-01"}]
    replied = {"a@a.com": "x@x.com", "d@a.com": None}
    emailed = {"a@a.com", "b@a.com", "d@a.com"}
    cc = ie.crosscheck(logged, replied, emailed, TODAY, days=28)
    assert (cc["logged"], cc["matched"], cc["pct"]) == (4, 1, 25)
    assert cc["misses"] == {"b@a.com": ie.MISS_NO_REPLY, "c@a.com": ie.MISS_NOT_READ, "d@a.com": ie.MISS_NO_SENDER}


def test_one_digest_line_per_client():
    line = ie.digest_line("Melior", {"inboxes": 155, "sent": 4210, "known_pct": 99.6, "replies": 61, "ooo": 12,
                                     "not_enough": 31, "quiet": 2},
                          {"logged": 63, "matched": 58, "pct": 92})
    assert line == ("Inbox evidence, Melior (28 days): 155 inboxes, 4,210 emails, sender known for 99.6%; 61 replies, "
                    "12 out-of-office; team reply log matched 58 of 63 (92%); 2 quiet, 31 not enough data.")
    assert "reply log not read" in ie.digest_line("PL", {"inboxes": 1, "sent": 1, "known_pct": 100, "replies": 0,
                                                         "ooo": 0, "not_enough": 1, "quiet": 0}, None)


def test_reply_log_dates_in_the_formats_the_team_uses():
    import inbox_evidence as job
    assert job.parse_day("11/12/2026") == date(2026, 11, 12) and job.parse_day("2026-11-12 10:00") == date(2026, 11, 12)
    assert job.parse_day("12 Nov 2026") == date(2026, 11, 12) and job.parse_day("soon") is None


def test_a_cached_lead_is_read_again_only_for_a_new_email_or_a_new_reply():
    import inbox_evidence as job
    cached = {"sids": {"s1": "a@x.com"}, "replied": False}
    assert not job._needs_read(cached, [row("s1", 10)])
    assert job._needs_read(cached, [row("s1", 10), row("s2", 14)])
    assert job._needs_read(cached, [row("s1", 10, reply="2026-11-11T00:00:00Z")])
    assert job._needs_read(None, [row("s1", 10)])


def test_the_week_record_splits_by_the_inboxs_client_and_checks_the_log():
    import inbox_evidence as job
    leads = {"l@a.com": ie.Lead([row("s1", 12, reply="2026-11-13T00:00:00Z")], hist(("s1", "a@x.com"), reply_body="Yes"))}
    rec = job.build(TODAY, leads, {"a@x.com": "Melior"}, lambda d: "google",
                    {"Melior": [{"email": "l@a.com", "date": "2026-11-13"}]}, {})
    m = rec["clients"]["Melior"]
    assert m["summary"]["replies"] == 1 and m["crosscheck"]["matched"] == 1 and rec["inboxes"]["a@x.com"]["client"] == "Melior"


def test_reply_log_rows_with_a_date_that_cannot_be_read_are_counted_not_dropped():
    logged = [{"email": "a@a.com", "date": "2026-11-12"}, {"email": "b@a.com", "date": ""}, {"email": "c@a.com", "date": ""}]
    cc = ie.crosscheck(logged, {"a@a.com": "x@x.com"}, {"a@a.com"}, TODAY)
    assert (cc["logged"], cc["matched"], cc["undated"]) == (1, 1, 2)


def test_a_logged_reply_smartlead_has_under_another_category_gets_its_own_reason():
    cc = ie.crosscheck([{"email": "w@a.com", "date": "2026-11-12"}], {}, {"w@a.com"}, TODAY,
                       other={"w@a.com": "Wrong Person"})
    assert cc["misses"]["w@a.com"] == ie.MISS_OTHER.format(cat="Wrong Person")


def test_sender_known_is_per_campaign_client_and_leads_with_two_senders_are_counted():
    a = ie.Lead([dict(row("s1", 10), _client="Melior"), dict(row("s2", 12), _client="Melior")],
                hist(("s1", "a@x.com"), ("s2", "b@x.com")))
    b = ie.Lead([dict(row("s3", 10, lead="m@a.com"), _client="Precise Leads")], hist())
    known = ie.sender_known({"l@a.com": a, "m@a.com": b}, TODAY)
    assert known == {"Melior": {"known": 2, "unknown": 0}, "Precise Leads": {"known": 0, "unknown": 1}}
    assert ie.two_senders({"l@a.com": a, "m@a.com": b}) == 1


def test_the_week_record_gives_each_client_its_own_sender_known_share():
    import inbox_evidence as job
    leads = {"l@a.com": ie.Lead([dict(row("s1", 12), _client="Melior")], hist(("s1", "a@x.com"))),
             "m@a.com": ie.Lead([dict(row("s2", 12, lead="m@a.com"), _client="Precise Leads"),
                                 dict(row("s3", 13, lead="m@a.com"), _client="Precise Leads")], hist(("s2", "p@x.com")))}
    rec = job.build(TODAY, leads, {"a@x.com": "Melior", "p@x.com": "Precise Leads"}, lambda d: "google", {}, {})
    assert rec["clients"]["Melior"]["summary"]["known_pct"] == 100.0
    assert rec["clients"]["Precise Leads"]["summary"]["known_pct"] == 50.0
    assert rec["two_senders"] == 0
