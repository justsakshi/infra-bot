"""Fleet watch (spec 2026-10-15-fleet-watch-design.md): checks, cases, acted-on, messages. Pure, no network."""
from datetime import date

from smartlead import fleet as fl

TODAY = date(2026, 10, 20)


def rec(email="a@d.com", **over):
    r = {"email": email, "domain": email.split("@")[1], "account": "PRECISE_LEADS", "client": "Precise Leads",
         "kind": "google", "limit": 15, "approved_limit": None, "setup": True, "warmup_on": True, "rep": 100,
         "connected": True, "campaigns": 1, "sending": True, "ready_on": "2026-10-01", "warmup_started": "2026-09-17",
         "test_date": "2026-10-13", "test_pct": 100.0, "verdict": "GOOD", "sent_14d": 0, "bounced_14d": 0}
    r.update(over)
    return r


def kinds(cases):
    return sorted((c["kind"], c["target"]) for c in cases)


def run(records, history=None, listed=None):
    return fl.checks(records, history or {}, listed or {}, TODAY)


# Checks ----------------------------------------------------------------------

def test_healthy_fleet_has_no_cases():
    assert run([rec(), rec("b@d.com")]) == []


def test_inbox_checks():
    cases = run([rec("w@d.com", warmup_on=False), rec("c@d.com", connected=False), rec("f@d.com", verdict="WATCH"),
                 rec("k@d.com", verdict="SHOULD KILL"), rec("h@d.com", limit=12),
                 rec("e@e.com", ready_on="2026-10-27", campaigns=1)])
    assert kinds(cases) == [("disconnected", "c@d.com"), ("early_campaign", "e@e.com"), ("failed_test", "f@d.com"),
                            ("hand_limit", "h@d.com"), ("kill", "k@d.com"), ("warmup_off", "w@d.com")]
    assert all(c["action"] for c in cases)


def test_hand_limit_uses_the_approved_limit_and_only_bot_inboxes():
    assert run([rec(limit=17, approved_limit=17)]) == []
    assert run([rec(limit=12, setup=False)]) == []


def test_bounces_are_judged_per_domain_on_100_plus_sends():
    two = [rec("a@d.com", sent_14d=60, bounced_14d=3), rec("b@d.com", sent_14d=60, bounced_14d=2)]   # 5 of 120 = 4.2%
    cases = run(two)
    assert kinds(cases) == [("bounce_high", "d.com")] and "4.2% of 120" in cases[0]["line"]
    assert kinds(run([rec(sent_14d=150, bounced_14d=4)])) == [("bounce_watch", "d.com")]
    assert run([rec(sent_14d=90, bounced_14d=9)]) == []


def test_low_reputation_needs_seven_days_running():
    low = [{"rep": 80}] * 7
    assert kinds(run([rec(rep=80)], history={"a@d.com": low})) == [("low_rep", "a@d.com")]
    assert run([rec(rep=80)], history={"a@d.com": [{"rep": 80}] * 6}) == []
    assert run([rec(rep=80)], history={"a@d.com": [{"rep": 80}] * 6 + [{"rep": 95}]}) == []


def test_smtp_ip_rotation_after_a_week_of_warmup():
    smtp = rec("s@smtp.com", kind="smtp", limit=7, rep=45, warmup_started="2026-10-10")
    assert kinds(run([smtp])) == [("smtp_ip", "smtp.com")]
    assert run([dict(smtp, warmup_started="2026-10-16")]) == []


def test_blocklist_and_untested_domains():
    assert kinds(run([rec()], listed={"d.com": ["Spamhaus DBL"]})) == [("blocklist", "d.com")]
    cases = run([rec(test_date="2026-09-20"), rec("b@d.com", test_date=None)])
    assert kinds(cases) == [("no_test", "d.com")]
    assert run([rec(test_date=None, sending=False)]) == []          # not sending: nothing to test yet


# Cases: open, acted on, close -------------------------------------------------

def test_a_new_problem_opens_a_case_and_a_standing_one_does_not_reopen():
    fired = run([rec(warmup_on=False)])
    opened, open_now, closed = fl.update_cases({}, fired, [rec(warmup_on=False)], TODAY)
    assert len(opened) == 1 and opened[0]["opened_on"] == "2026-10-20" and opened[0]["at_open"]["warmup_on"] is False
    again, open_now, closed = fl.update_cases({c["key"]: c for c in open_now}, fired, [rec(warmup_on=False)], TODAY)
    assert again == [] and len(open_now) == 1 and closed == []


def test_case_closes_when_the_check_stops_firing_and_records_that_it_was_acted_on():
    fired = run([rec(warmup_on=False)])
    _, open_now, _ = fl.update_cases({}, fired, [rec(warmup_on=False)], TODAY)
    later = date(2026, 10, 22)
    _, still, closed = fl.update_cases({c["key"]: c for c in open_now}, [], [rec()], later)
    assert still == [] and closed[0]["closed_on"] == "2026-10-22" and closed[0]["acted_on"] == "2026-10-22"


def test_acted_on_is_read_from_what_changed():
    assert fl.acted("bounce_high", {"limit_sum": 30, "campaigns": 2}, {"limit_sum": 26, "campaigns": 2})
    assert fl.acted("kill", {"campaigns": 2}, {"campaigns": 0})
    assert fl.acted("failed_test", {"test_date": "2026-10-13"}, {"test_date": "2026-10-20"})
    assert not fl.acted("failed_test", {"test_date": "2026-10-13"}, {"test_date": "2026-10-13"})
    assert not fl.acted("bounce_high", {"limit_sum": 30, "campaigns": 2}, {"limit_sum": 30, "campaigns": 2})


# Messages ----------------------------------------------------------------------

def test_watch_post_counts_per_action_in_the_channel_and_lists_in_the_thread():
    new = run([rec("w1@d.com", warmup_on=False), rec("w2@d.com", warmup_on=False), rec("k@e.com", verdict="SHOULD KILL")])
    for c in new:
        c["opened_on"] = "2026-10-20"
    head, thread = fl.watch_post(new, open_count={"warmup_off": 2, "kill": 1, "no_test": 4}, today=TODAY)
    assert head[0] == "⚠️ Fleet watch, 20 Oct: 3 new problems (suggestions only, nothing was changed)"
    assert head[1:3] == ["Warmup off: 2", "Kill list: 1"]                       # what is wrong, short
    assert any(l.startswith("Still open: 7 (") for l in head)
    assert "*Turn warmup back on* (warmup off, 2):\n• w1@d.com\n• w2@d.com" in thread
    assert "*Take it out of campaigns, do not renew* (kill list, 1):\n• k@e.com" in thread
    assert "w1@d.com" not in "\n".join(head)                      # the inboxes are in the thread only


def test_scorecard_compares_acted_and_not_acted_after_14_days():
    def case(kind, opened, acted=None, closed=None, target="a@d.com", domain="d.com"):
        return {"kind": kind, "target": target, "domain": domain, "opened_on": opened, "acted_on": acted,
                "closed_on": closed}
    cases = [case("warmup_off", "2026-10-01", "2026-10-02", "2026-10-02"),
             case("warmup_off", "2026-10-01", None, None),
             case("bounce_high", "2026-10-15", None, None, target="d.com"),
             case("kill", "2026-10-01", "2026-10-03", "2026-10-03", target="k@p.com", domain="p.com")]
    t = fl.scorecard_text(cases, TODAY, pilot={"p.com"}, control={"c.com"})
    assert "Warmup off: 2 opened, 1 acted on; cleared within 14 days: 1 of 1 acted on, 0 of 1 not" in t
    assert "Bounces 4%+: 1 opened, 0 acted on; too recent to judge (1)" in t
    assert "Pilot domains: 1 case (kill list 1). Comparison domains: 0 cases." in t


def test_baseline_records_standing_problems_without_reporting_them():
    # Manveen 15 Oct: "we act on the things that happen now", not on everything that was ever wrong.
    fired = run([rec(warmup_on=False)])
    opened, open_now, _ = fl.update_cases({}, fired, [rec(warmup_on=False)], TODAY, baseline=True)
    assert opened == [] and open_now[0]["baseline"] is True
    again, _, _ = fl.update_cases({c["key"]: c for c in open_now}, fired, [rec(warmup_on=False)], TODAY)
    assert again == []                                     # still standing: never posted
    t = fl.scorecard_text(open_now, TODAY, pilot=set(), control=set())
    assert "Warmup off" not in t                           # the scorecard judges only problems that started later


# Audit 7 Nov 2026: tell the truth, never lose an alert ---------------------------

def test_a_disconnected_inbox_opens_no_warmup_case():
    assert kinds(run([rec(connected=False, warmup_on=False)])) == [("disconnected", "a@d.com")]


def test_over_limit_flags_an_inbox_above_the_highest_limit_for_its_type():
    """10 Nov 2026: dedicated Outlook inboxes at 10 a day and Google at 20 were flagged against the start
    value (5 and 15), though those types go that high. The types differ; each is held to its own ceiling."""
    within = [rec("g@d.com", setup=False, limit=20), rec("o@o.com", kind="outlook", setup=False, limit=10),
              rec("sh@sh.com", kind="outlook_dense", setup=False, limit=8), rec("s@s.com", kind="smtp", setup=False, limit=10)]
    assert run(within) == []                                     # at the ceiling of its type: nothing to say
    cases = run([rec("g@d.com", setup=False, limit=25), rec("o@o.com", kind="outlook", setup=False, limit=12),
                 rec("sh@sh.com", kind="outlook_dense", setup=False, limit=10),      # fine for a dedicated inbox, too high shared
                 rec("a@a.com", setup=False, limit=25, approved_limit=25),          # approved by a person
                 rec("b@b.com", limit=25)])                      # a bot-set-up inbox raised by hand past the ceiling
    assert kinds(cases) == [("over_limit", "b@b.com"), ("over_limit", "g@d.com"), ("over_limit", "o@o.com"),
                            ("over_limit", "sh@sh.com")]
    by = {c["target"]: c for c in cases}
    assert by["g@d.com"]["action"] == "Lower to 20 or record it as approved"
    assert by["o@o.com"]["action"] == "Lower to 10 or record it as approved"
    assert by["sh@sh.com"]["line"] == "sh@sh.com (10/day, highest for its type 8)"
    assert kinds(run([rec(limit=12)])) == [("hand_limit", "a@d.com")]          # lowered by hand: still hand_limit
    assert kinds(run([rec(limit=20)])) == [("hand_limit", "a@d.com")]          # raised by hand within the ceiling


def test_warmup_mail_in_spam_is_a_case_with_the_numbers():
    """10 Nov 2026: 50 inboxes had 10%+ of their warm-up mail in spam and no post named them."""
    cases = run([rec("bad@d.com", warm_sent_7d=88, warm_spam_7d=43), rec("edge@d.com", warm_sent_7d=30, warm_spam_7d=3),
                 rec("fine@d.com", warm_sent_7d=90, warm_spam_7d=8),          # 8.9%: under the line
                 rec("few@d.com", warm_sent_7d=29, warm_spam_7d=20),          # too few warm-up emails to say
                 rec("unread@d.com", warm_sent_7d=None, warm_spam_7d=None), rec("old@d.com")])   # not read: no claim
    assert kinds(cases) == [("warmup_spam", "bad@d.com"), ("warmup_spam", "edge@d.com")]
    by = {c["target"]: c for c in cases}
    assert by["bad@d.com"]["line"] == "bad@d.com (43 of 88 warm-up emails in spam, 49%)"
    assert "v2" in by["bad@d.com"]["action"] and "classic test cannot clear it" in by["bad@d.com"]["action"]
    head, thread = fl.watch_post(cases, {}, TODAY)
    assert head[1] == "Warm-up mail landing in spam: 2"


def test_a_warmup_spam_case_closes_only_when_the_numbers_show_it():
    case = run([rec("bad@d.com", warm_sent_7d=88, warm_spam_7d=43)])[0]
    seen = lambda **kw: fl.observe(case, [rec("bad@d.com", **kw)], set())
    assert fl.acted("warmup_spam", {}, seen(warm_sent_7d=90, warm_spam_7d=4)) is True          # back under the line
    assert fl.acted("warmup_spam", {}, seen(warm_sent_7d=90, warm_spam_7d=30)) is False
    assert fl.acted("warmup_spam", {}, seen(warm_sent_7d=None, warm_spam_7d=None)) is False    # not read: no evidence
    assert fl.acted("warmup_spam", {}, seen(warm_sent_7d=10, warm_spam_7d=0)) is False         # too few to say
    assert fl._shows_fixed("warmup_spam", [rec("bad@d.com", warm_sent_7d=90, warm_spam_7d=4)]) is True
    assert fl._shows_fixed("warmup_spam", [rec("bad@d.com")]) is False


def test_do_not_use_inbox_in_a_live_campaign_is_a_case():
    tagged = rec(tags=["Bot-Setup", "DO NOT USE - failed v2 (auto)"], live_campaigns=["PL | Outbound"])
    [c] = run([tagged])
    assert (c["kind"], c["target"]) == ("dnu_live", "a@d.com")
    assert c["action"] == "Take it out of PL | Outbound or remove the tag" and c["line"] == "a@d.com (PL | Outbound)"
    assert kinds(run([rec(tags=["do not use"], live_campaigns=["X"])])) == [("dnu_live", "a@d.com")]
    assert run([dict(tagged, live_campaigns=[])]) == []                      # tagged, out of live campaigns: fine
    assert run([rec(tags=["Bot-Setup"], live_campaigns=["X"])]) == []


DBL = {"date": "2026-10-19", "listed_on": ["Spamhaus DBL (127.0.1.2)"], "lookup_errors": [], "inconclusive_zones": []}


def test_a_dbl_listed_domain_in_live_campaigns_needs_a_decision():
    cases = run([rec(live_campaigns=["C1"]), rec("b@d.com", live_campaigns=["C1", "C2"])], listed={"d.com": DBL})
    assert kinds(cases) == [("blocklist", "d.com"), ("listed_live", "d.com")]
    live = [c for c in cases if c["kind"] == "listed_live"][0]
    assert live["action"] == "Decide with the client lead: keep sending or move volume; no blind removal requests"
    assert live["line"] == "d.com (Spamhaus DBL (127.0.1.2); 2 inboxes in 2 live campaigns)"
    surbl = {**DBL, "listed_on": ["SURBL (ABUSE)"]}
    assert kinds(run([rec(live_campaigns=["C1"])], listed={"d.com": surbl})) == [("blocklist", "d.com")]
    assert kinds(run([rec(live_campaigns=[])], listed={"d.com": DBL})) == [("blocklist", "d.com")]


def test_no_advice_to_file_blind_removal_requests():
    assert all("request delisting" not in action.lower() for _, action, _ in fl.KINDS.values())


def test_an_old_listing_is_carried_forward_and_says_when_it_was_checked():
    old = {**DBL, "date": "2026-10-01"}
    [c] = run([rec()], listed={"d.com": old})
    assert c["kind"] == "blocklist" and c["line"] == "d.com (Spamhaus DBL (127.0.1.2), last checked 1 Oct)"


def opened(records, listed=None, day=TODAY):
    fired = fl.checks(records, {}, listed or {}, day)
    _, open_now, _ = fl.update_cases({}, fired, records, day, read={"PRECISE_LEADS"}, blocklist=listed or {})
    return {c["key"]: c for c in open_now}


def test_new_cases_wait_to_be_announced_and_baseline_ones_are_never_announced():
    fired = run([rec(warmup_on=False)])
    new, _, _ = fl.update_cases({}, fired, [rec(warmup_on=False)], TODAY)
    assert new[0]["announced"] is False and new[0]["account"] == "PRECISE_LEADS"
    _, base, _ = fl.update_cases({}, fired, [rec(warmup_on=False)], TODAY, baseline=True)
    assert base[0]["announced"] is None


def test_an_account_that_was_not_read_closes_nothing():
    cases = opened([rec(warmup_on=False)])
    later = date(2026, 10, 21)
    _, still, closed = fl.update_cases(cases, [], [], later, read=set(), blocklist={})
    assert closed == [] and still[0]["unknown"] is True and still[0]["acted_on"] is None


def test_old_cases_without_an_account_find_it_by_client():
    cases = opened([rec(warmup_on=False)])
    for c in cases.values():
        c.pop("account")
    other = rec("x@x.com", account="BETTRDATA", client="Bettrdata")
    _, still, closed = fl.update_cases(cases, [], [other], date(2026, 10, 21), read={"BETTRDATA"}, blocklist={})
    assert closed == [] and still[0]["unknown"] is True            # Precise Leads' account had no records today
    _, _, closed = fl.update_cases(cases, [], [rec()], date(2026, 10, 21), read={"PRECISE_LEADS"}, blocklist={})
    assert closed[0]["closed_why"] == "cleared"


def roster(present=(), dropped=(), tracked=("d.com",)):
    return {"tracked": set(tracked) if tracked is not None else None, "present": set(present),
            "dropped": set(dropped)}


def test_an_inbox_no_longer_watched_closes_as_gone_and_is_not_scored_as_cleared():
    cases = opened([rec(warmup_on=False)])
    _, still, closed = fl.update_cases(cases, [], [rec("b@d.com")], date(2026, 10, 21), read={"PRECISE_LEADS"},
                                       blocklist={}, roster=roster(present={"b@d.com"}))    # a@d.com left Smartlead
    assert still == [] and closed[0]["closed_why"] == "gone" and closed[0]["acted_on"] is None
    t = fl.scorecard_text(closed, date(2026, 11, 9), pilot=set(), control=set())
    assert "Warmup off: 1 opened, 0 acted on; cleared within 14 days: 0 of 0 acted on, 0 of 1 not" in t


def test_an_inbox_missing_from_the_records_but_still_in_smartlead_and_the_sheet_stays_open():
    """Audit 7 Nov (F1): the account was read without an error, but the inbox list stopped early."""
    cases = opened([rec(warmup_on=False)])
    later = date(2026, 10, 21)
    for r in (roster(present={"a@d.com", "b@d.com"}), None):                  # still listed / no roster at all
        _, still, closed = fl.update_cases(cases, [], [rec("b@d.com")], later, read={"PRECISE_LEADS"},
                                           blocklist={}, roster=r)
        assert closed == [] and still[0]["unknown"] is True and still[0]["acted_on"] is None


def test_gone_needs_the_domain_out_of_the_sheet_or_the_inbox_left_out_on_purpose():
    cases = opened([rec(warmup_on=False)])
    later = date(2026, 10, 21)
    for r in (roster(present={"a@d.com"}, tracked={"e.com"}),                  # domain left the sheet
              roster(present={"a@d.com"}, dropped={"a@d.com"})):              # listed, left out on purpose
        _, still, closed = fl.update_cases(cases, [], [rec("b@d.com")], later, read={"PRECISE_LEADS"},
                                           blocklist={}, roster=r)
        assert still == [] and closed[0]["closed_why"] == "gone"
    _, still, _ = fl.update_cases(cases, [], [rec("b@d.com")], later, read={"PRECISE_LEADS"}, blocklist={},
                                  roster=roster(present={"a@d.com"}, tracked=None))  # sheet unread: no evidence
    assert still[0]["unknown"] is True


def test_a_blocklist_case_whose_domain_is_missing_closes_only_when_its_inboxes_left():
    cases = opened([rec()], listed={"d.com": DBL})
    later = date(2026, 10, 21)
    others = [rec("x@x.com")]
    _, still, closed = fl.update_cases(cases, [], others, later, read={"PRECISE_LEADS"}, blocklist={"d.com": DBL},
                                       roster=roster(present={"a@d.com", "x@x.com"}, tracked={"d.com", "x.com"}))
    assert closed == [] and still[0]["unknown"] is True
    _, still, closed = fl.update_cases(cases, [], others, later, read={"PRECISE_LEADS"}, blocklist={"d.com": DBL},
                                       roster=roster(present={"x@x.com"}, tracked={"d.com", "x.com"}))
    assert still == [] and closed[0]["closed_why"] == "gone"


def test_a_warmup_case_on_an_inbox_that_then_disconnects_is_superseded_not_cleared():
    cases = opened([rec(warmup_on=False)])
    now = [rec(warmup_on=False, connected=False)]
    _, still, closed = fl.update_cases(cases, fl.checks(now, {}, {}, TODAY), now, date(2026, 10, 21))
    assert [c["kind"] for c in still] == ["disconnected"] and closed[0]["closed_why"] == "superseded"


def test_a_kill_case_needs_a_passing_test_to_close():
    cases = opened([rec(verdict="SHOULD KILL")])
    _, still, closed = fl.update_cases(cases, [], [rec(verdict="NO DATA")], date(2026, 10, 21))
    assert closed == [] and still[0]["unknown"] is True                 # the tests aged out: proves nothing
    _, _, closed = fl.update_cases(cases, [], [rec(verdict="GOOD")], date(2026, 10, 21))
    assert closed[0]["closed_why"] == "cleared"


def test_a_blocklist_case_closes_only_on_a_clean_check_newer_than_the_case():
    cases = opened([rec()], listed={"d.com": DBL})
    day2 = date(2026, 10, 21)
    _, still, closed = fl.update_cases(cases, [], [rec()], day2, blocklist=None)          # no blocklist data
    assert closed == [] and still[0]["unknown"] is True
    stale_clean = {**DBL, "date": "2026-10-12", "listed_on": []}                           # older than the case
    _, still, closed = fl.update_cases(cases, [], [rec()], day2, blocklist={"d.com": stale_clean})
    assert closed == []
    no_answer = {**DBL, "date": "2026-10-26", "listed_on": [], "lookup_errors": ["Spamhaus DBL"]}
    _, still, closed = fl.update_cases(cases, [], [rec()], day2, blocklist={"d.com": no_answer})
    assert closed == [] and still[0]["unknown"] is True                                    # DBL did not answer
    clean = {**DBL, "date": "2026-10-26", "listed_on": [], "inconclusive_zones": ["URIBL"]}
    _, still, closed = fl.update_cases(cases, [], [rec()], date(2026, 10, 26), blocklist={"d.com": clean})
    assert still == [] and closed[0]["closed_why"] == "cleared" and closed[0]["acted_on"] == "2026-10-26"


def test_a_listed_live_case_clears_when_the_domain_leaves_live_campaigns():
    cases = opened([rec(live_campaigns=["C1"])], listed={"d.com": DBL})
    now = [rec(live_campaigns=[])]
    _, still, closed = fl.update_cases(cases, fl.checks(now, {}, {"d.com": DBL}, TODAY), now, date(2026, 10, 21),
                                       blocklist={"d.com": DBL})
    assert [c["kind"] for c in still] == ["blocklist"]
    assert closed[0]["kind"] == "listed_live" and closed[0]["closed_why"] == "cleared" and closed[0]["acted_on"]


def test_watch_post_resends_earlier_cases_and_shows_a_listed_live_domain_once():
    cases = opened([rec(live_campaigns=["C1"], warmup_on=False)], listed={"d.com": DBL})
    earlier = dict(cases["warmup_off:a@d.com"], opened_on="2026-10-18")
    new = [earlier, cases["blocklist:d.com"], cases["listed_live:d.com"]]
    head, thread = fl.watch_post(new, {"no_test": 2}, TODAY, unknown=1)
    assert head[0] == "⚠️ Fleet watch, 20 Oct: 2 new problems (suggestions only, nothing was changed)"
    assert head[1:3] == ["On Spamhaus DBL or URIBL and in live campaigns: 1", "Warmup off: 1"]
    assert "1 of them was found on 18 Oct, but that post did not go out." in head
    assert "Still open: 2 (no placement test in 21 days 2); 1 of them could not be checked today" in head
    assert "• a@d.com, found 18 Oct" in "\n".join(thread)
    assert not any("on a blocklist, " in p for p in thread)            # shown under listed_live only


def test_backlog_is_one_count_line_with_the_list_in_the_thread():
    a = dict(opened([rec(warmup_on=False)])["warmup_off:a@d.com"], opened_on="2026-10-16", unknown=True)
    b = dict(opened([rec("b@d.com", connected=False)])["disconnected:b@d.com"], opened_on="2026-11-03")
    head, thread = fl.backlog_post([a, b], date(2026, 11, 8))
    assert head == ["📋 Fleet watch: 2 problems found since 16 Oct are still open (list in the thread, shown once; "
                    "suggestions only, nothing was changed)"]
    assert "• a@d.com, open since 16 Oct, could not be checked today" in "\n".join(thread)
    assert "• b@d.com, open since 3 Nov" in "\n".join(thread)


def test_scorecard_lists_every_open_blocklist_case_including_baseline_ones():
    base = {"key": "blocklist:old.com", "kind": "blocklist", "scope": "domain", "target": "old.com",
            "domain": "old.com", "client": "Bettrdata", "line": "old.com (SURBL (ABUSE))", "opened_on": "2026-10-15",
            "closed_on": None, "baseline": True}
    live = opened([rec(live_campaigns=["C1"])], listed={"d.com": DBL})
    cases = [base, live["blocklist:d.com"], live["listed_live:d.com"],
             dict(base, key="blocklist:gone.com", target="gone.com", closed_on="2026-10-20")]
    head, thread = fl.scorecard_post(cases, date(2026, 11, 9), pilot=set(), control=set())
    assert head[0] == "Fleet watch scorecard, 9 Nov" and head[1] == "Still on a blocklist: 2 domains (list in the thread)"
    listing = "\n".join(thread)
    assert "• old.com (SURBL (ABUSE)), Bettrdata, open since 15 Oct (already listed when fleet watch started)" in listing
    assert "• d.com (Spamhaus DBL (127.0.1.2)), Precise Leads, open since 20 Oct, in live campaigns" in listing
    assert "gone.com" not in listing



def test_the_headline_counts_add_up_when_one_kind_has_several_actions():
    cases = run([rec("g@d.com", setup=False, limit=25), rec("o@o.com", kind="outlook", setup=False, limit=12)])
    head, thread = fl.watch_post(cases, {}, TODAY)
    assert head == ["⚠️ Fleet watch, 20 Oct: 2 new problems (suggestions only, nothing was changed)",
                    "Above the highest daily limit for its type: 2"]
    assert thread == ["*Lower to 10 or record it as approved* (above the highest daily limit for its type, 1):\n• o@o.com (12/day, highest for its type 10)",
                      "*Lower to 20 or record it as approved* (above the highest daily limit for its type, 1):\n• g@d.com (25/day, highest for its type 20)"]
