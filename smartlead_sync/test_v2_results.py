"""smartlead/v2_results.py: reading Smart Delivery v2 CSVs and deciding pull / return. No network."""
from datetime import date

from smartlead import v2_results as v2

HEAD = "Mailbox,Provider,Landed,Sent at,Send status\n"
G, M = "GOOGLE_WORKSPACE", "MICROSOFT_365"


def csv_text(rows):
    """rows: (mailbox, provider, landed[, sent_at]) -> CSV text as Smartlead writes it."""
    out = [HEAD]
    for r in rows:
        mb, prov, landed = r[:3]
        sent = r[3] if len(r) > 3 else "2026-11-09T14:31:00.000Z"
        status = "SKIPPED" if landed == "NOT_SENT" else "SENT"
        out.append(f"{mb},{prov},{landed},{sent if landed != 'NOT_SENT' else ''},{status}\n")
    return "".join(out)


def seeds(mb, g_inbox, m_inbox, g_total=4, m_total=4, sent="2026-11-09T14:31:00.000Z"):
    rows = [(mb, G, "Inbox" if i < g_inbox else "Spam", sent) for i in range(g_total)]
    rows += [(mb, M, "Inbox" if i < m_inbox else "Spam", sent) for i in range(m_total)]
    return rows


# --- reading ---------------------------------------------------------------------

def test_only_files_with_the_v2_header_count():
    assert v2.is_v2_csv(HEAD + "a@x.com,GOOGLE_WORKSPACE,Inbox,2026-11-09T14:31:00Z,SENT\n")
    assert not v2.is_v2_csv("email,first_name\n")
    assert v2.is_v2_csv("﻿" + HEAD)               # Excel's BOM


def test_a_changed_v2_header_is_recognised_as_a_v2_file_it_cannot_read():
    changed = "Mailbox,Provider,Landed,Sent at,Send status,Seed\n"
    assert not v2.is_v2_csv(changed) and v2.looks_like_v2(changed)
    assert not v2.looks_like_v2(HEAD) and not v2.looks_like_v2("email,first_name\n")


def test_parse_lowercases_mailboxes_and_keeps_every_row():
    rows = v2.parse(csv_text([("A_Smith@Example.COM", G, "Inbox")]))
    assert rows == [{"mailbox": "a_smith@example.com", "provider": G, "landed": "Inbox",
                     "sent_at": "2026-11-09T14:31:00.000Z", "send_status": "SENT"}]


def test_night_is_the_india_date_of_the_first_send():
    # Mon 20:00 IST = 14:30 UTC; a run that goes past midnight IST is still Monday's night
    rows = v2.parse(csv_text([("a@x.com", G, "Inbox", "2026-11-09T14:31:00Z"),
                              ("a@x.com", G, "Inbox", "2026-11-09T19:40:00Z")]))   # 01:10 IST Tue
    assert v2.night_of(rows) == "2026-11-09"


# --- verdicts --------------------------------------------------------------------

def test_provider_verdicts_follow_pass_4_of_4_watch_2_3_fail_0_1():
    assert v2.provider_verdict(["Inbox"] * 4)["verdict"] == v2.PASS
    assert v2.provider_verdict(["Inbox"] * 3 + ["Spam"])["verdict"] == v2.WATCH
    assert v2.provider_verdict(["Inbox"] * 2 + ["Spam"] * 2)["verdict"] == v2.WATCH
    assert v2.provider_verdict(["Inbox"] + ["Spam"] * 3)["verdict"] == v2.FAIL
    assert v2.provider_verdict(["Spam"] * 4)["verdict"] == v2.FAIL


def test_a_pending_or_bounced_seed_is_never_judged():
    # audit PLACEMENT-10: 3 Inbox + 1 pending used to be WATCH, and a second WATCH pulled a working inbox
    assert v2.provider_verdict(["Inbox"] * 3 + ["Pending"])["verdict"] == v2.INCOMPLETE
    assert v2.provider_verdict(["Inbox"] * 3 + [""])["verdict"] == v2.INCOMPLETE
    # a bounce at a seed address is the seed's problem, not the sender's
    assert v2.provider_verdict(["Inbox"] * 3 + ["BOUNCED"])["verdict"] == v2.INCOMPLETE
    assert v2.provider_verdict(["Inbox", "Spam", "BOUNCED", "BOUNCED"])["verdict"] == v2.INCOMPLETE
    assert v2.provider_verdict(["Inbox"] * 3 + ["BOUNCED"])["reported"] == 3        # bounced is not reported
    # but 3 of 4 in spam is a fail whatever the fourth seed did
    assert v2.provider_verdict(["Spam", "Spam", "Spam", ""])["verdict"] == v2.FAIL
    assert v2.provider_verdict(["Spam", "Spam", "Spam", "BOUNCED"])["verdict"] == v2.FAIL


def test_a_test_that_sent_fewer_than_4_is_never_judged():
    assert v2.provider_verdict(["Inbox"] * 3)["verdict"] == v2.UNKNOWN         # 3 of 3 is not 4 of 4
    assert v2.provider_verdict(["Spam"] * 3)["verdict"] == v2.UNKNOWN
    assert v2.provider_verdict(["Inbox", "Inbox"])["verdict"] == v2.UNKNOWN
    assert v2.provider_verdict(["Inbox"] * 4 + ["NOT_SENT"])["verdict"] == v2.PASS   # a skipped row is not a seed
    assert v2.provider_verdict(["NOT_SENT"] * 4)["verdict"] == v2.NOT_SENT


def test_a_bounced_seed_is_final_so_the_file_is_ready():
    rows = v2.parse(csv_text(seeds("a@x.com", 4, 3)[:7] + [("a@x.com", M, "BOUNCED")]))
    assert v2.file_ready(rows) is True and v2.pending_seeds(rows) == 0


def test_mailbox_verdict_combines_both_providers():
    res = v2.summarise(v2.parse(csv_text(
        seeds("pass@x.com", 4, 4) + seeds("gfail@x.com", 0, 4) + seeds("mwatch@x.com", 4, 2)
        + [("half@x.com", G, "Inbox")] * 4 + [("half@x.com", M, "Pending")] * 4
        + [("dead@x.com", G, "NOT_SENT")] * 4 + [("dead@x.com", M, "NOT_SENT")] * 4)))
    assert res["pass@x.com"]["verdict"] == v2.PASS
    assert res["gfail@x.com"]["verdict"] == v2.FAIL
    assert res["gfail@x.com"]["google"] == {"inbox": 0, "reported": 4, "sent": 4, "bounced": 0, "verdict": v2.FAIL}
    assert res["mwatch@x.com"]["verdict"] == v2.WATCH
    assert res["half@x.com"]["verdict"] == v2.INCOMPLETE       # Microsoft not in yet: no action either way
    assert res["dead@x.com"]["verdict"] == v2.NOT_SENT


def test_a_file_with_many_results_still_pending_is_not_ready():
    pending = csv_text(seeds("a@x.com", 4, 4)[:4] + [("a@x.com", M, "Pending")] * 4)
    assert v2.file_ready(v2.parse(pending)) is False
    assert v2.file_ready(v2.parse(csv_text(seeds("a@x.com", 4, 4)))) is True


# --- test-side problems ------------------------------------------------------------

def test_a_provider_where_nearly_every_inbox_fails_is_held_as_a_test_problem():
    night = {f"i{n}@x.com": {"google": {"verdict": v2.FAIL}, "microsoft": {"verdict": v2.PASS}} for n in range(12)}
    assert v2.held_providers(night) == {"google"}
    night["ok@y.com"] = {"google": {"verdict": v2.PASS}, "microsoft": {"verdict": v2.PASS}}
    night.update({f"ok{n}@y.com": {"google": {"verdict": v2.PASS}, "microsoft": {"verdict": v2.PASS}} for n in range(3)})
    assert v2.held_providers(night) == set()                   # 12 of 16 = 75%: a real problem, act on it


def test_one_dead_seed_making_everyone_watch_is_held_too():
    night = {f"i{n}@x.com": {"google": {"verdict": v2.WATCH}, "microsoft": {"verdict": v2.PASS}} for n in range(12)}
    assert v2.held_providers(night) == {"google"}


def test_inboxes_already_known_bad_do_not_push_a_night_into_a_hold():
    night = {f"bad{n}@x.com": {"google": {"verdict": v2.FAIL}, "microsoft": {"verdict": v2.PASS}} for n in range(54)}
    night.update({f"ok{n}@y.com": {"google": {"verdict": v2.PASS}, "microsoft": {"verdict": v2.PASS}} for n in range(10)})
    night["new@z.com"] = {"google": {"verdict": v2.FAIL}, "microsoft": {"verdict": v2.PASS}}
    assert v2.held_providers(night) == {"google"}                              # 55 of 65 below 4/4
    assert v2.held_providers(night, exclude={f"bad{n}@x.com" for n in range(54)}) == set()


def test_a_small_test_is_never_held():
    night = {f"i{n}@x.com": {"google": {"verdict": v2.FAIL}, "microsoft": {"verdict": v2.FAIL}} for n in range(5)}
    assert v2.held_providers(night) == set()


# --- decisions ---------------------------------------------------------------------

def r(night, verdict, history=False):
    return {"night": night, "verdict": verdict, "history": history}


def test_a_fail_pulls():
    d = v2.decide([r("2026-11-09", v2.FAIL)], pulled=None)
    assert d["action"] == v2.PULL


def test_one_watch_waits_two_in_a_row_pull():
    assert v2.decide([r("2026-11-09", v2.WATCH)], pulled=None)["action"] is None
    assert v2.decide([r("2026-11-02", v2.WATCH), r("2026-11-09", v2.WATCH)], pulled=None)["action"] == v2.PULL
    assert v2.decide([r("2026-11-02", v2.PASS), r("2026-11-09", v2.WATCH)], pulled=None)["action"] is None


def test_history_results_never_trigger_an_action_and_never_count_as_last_week():
    assert v2.decide([r("2026-11-05", v2.FAIL, history=True)], pulled=None)["action"] is None
    # audit PLACEMENT-10: the 5 Nov history files must not pair with the first Monday
    assert v2.decide([r("2026-11-05", v2.WATCH, history=True), r("2026-11-09", v2.WATCH)],
                     pulled=None)["action"] is None
    assert v2.decide([r("2026-11-02", v2.WATCH, history=True), r("2026-11-09", v2.WATCH)],
                     pulled=None)["action"] is None


def test_watch_twice_pulls_only_for_two_monday_tests():
    mon2, mon9, tue10, mon16 = "2026-11-02", "2026-11-09", "2026-11-10", "2026-11-16"
    assert v2.decide([r(mon2, v2.WATCH), r(mon9, v2.WATCH)], pulled=None)["action"] == v2.PULL
    assert v2.decide([r(mon2, v2.FAIL), r(mon9, v2.WATCH)], pulled=None)["action"] == v2.PULL
    # a one-off retest the day after is not a second week, either way round
    assert v2.decide([r(mon9, v2.WATCH), r(tue10, v2.WATCH)], pulled=None)["action"] is None
    assert v2.decide([r(mon9, v2.WATCH), r(tue10, v2.WATCH), r(mon16, v2.WATCH)], pulled=None)["action"] is None
    assert v2.decide([r(mon9, v2.WATCH), r("2026-11-12", v2.PASS), r(mon16, v2.WATCH)], pulled=None)["action"] is None
    # a one-off FAIL still pulls on its own (a re-run after a held night must be able to act)
    assert v2.decide([r(tue10, v2.FAIL)], pulled=None)["action"] == v2.PULL
    assert v2.two_weeks_running(r(mon2, v2.WATCH), r(mon9, v2.WATCH))
    assert not v2.two_weeks_running(r(mon9, v2.WATCH), r(mon9, v2.WATCH))      # same night: 0 days apart


def test_incomplete_and_not_sent_results_are_skipped_not_judged():
    rs = [r("2026-11-02", v2.FAIL), r("2026-11-09", v2.INCOMPLETE)]
    assert v2.decide(rs, pulled=None)["action"] == v2.PULL     # last real result is still the fail
    rs = [r("2026-11-09", v2.NOT_SENT)]
    assert v2.decide(rs, pulled=None)["action"] is None


def test_a_pulled_inbox_returns_after_two_passes_six_days_apart_after_the_pull():
    pulled = {"on": "2026-11-10"}
    fail_then_two = [r("2026-11-09", v2.FAIL), r("2026-11-16", v2.PASS), r("2026-11-23", v2.PASS)]
    assert v2.decide(fail_then_two, pulled=pulled)["action"] == v2.RETURN
    one_pass = fail_then_two[:2]
    assert v2.decide(one_pass, pulled=pulled)["action"] is None
    too_close = [r("2026-11-09", v2.FAIL), r("2026-11-16", v2.PASS), r("2026-11-18", v2.PASS)]
    assert v2.decide(too_close, pulled=pulled)["action"] is None
    pass_before_pull = [r("2026-11-02", v2.PASS), r("2026-11-09", v2.PASS)]
    assert v2.decide(pass_before_pull, pulled={"on": "2026-11-10"})["action"] is None


def test_a_pulled_inbox_that_fails_again_stays_out():
    pulled = {"on": "2026-11-10"}
    rs = [r("2026-11-09", v2.FAIL), r("2026-11-16", v2.PASS), r("2026-11-23", v2.FAIL)]
    assert v2.decide(rs, pulled=pulled)["action"] is None      # already out: nothing new to do
    rs = [r("2026-11-16", v2.PASS), r("2026-11-23", v2.WATCH), r("2026-11-30", v2.PASS)]
    assert v2.decide(rs, pulled=pulled)["action"] is None      # the two latest are not both passes


def test_same_night_keeps_the_later_run():
    results = v2.merge_results([], [{"night": "2026-11-05", "verdict": v2.FAIL, "sent_last": "2026-11-05T08:00Z"}])
    results = v2.merge_results(results, [{"night": "2026-11-05", "verdict": v2.PASS, "sent_last": "2026-11-05T09:00Z"}])
    assert [x["verdict"] for x in results] == [v2.PASS]
    results = v2.merge_results(results, [{"night": "2026-11-05", "verdict": v2.FAIL, "sent_last": "2026-11-05T07:00Z"}])
    assert [x["verdict"] for x in results] == [v2.PASS]


# --- campaigns ---------------------------------------------------------------------

def test_test_campaigns_are_never_touched():
    for name in ("DT PL #1", "DT Melior #3", "PL Deliverability Test", "Deliverability check | ScaledMail short copy",
                 "Melior spam test 3", "Placement test - x"):
        assert v2.is_test_campaign(name)
    for name in ("PL | College Talks", "Melior | Agency space", "BettrData Ingest"):
        assert not v2.is_test_campaign(name)


def test_pull_plan_never_removes_the_last_sender():
    camps = {1: {"name": "PL | A", "status": "ACTIVE", "senders": {10, 11}},
             2: {"name": "PL | B", "status": "ACTIVE", "senders": {10}},
             3: {"name": "DT PL #1", "status": "ACTIVE", "senders": {10, 11}},
             4: {"name": "PL | Old", "status": "COMPLETED", "senders": {10, 12}}}
    plan = v2.pull_plan({10}, camps)
    assert plan["remove"] == {1: [10]}
    assert plan["blocked"] == {2: [10]}                       # only sender left: needs a person


def test_pull_plan_blocks_a_campaign_where_every_sender_fails():
    camps = {1: {"name": "PL | A", "status": "ACTIVE", "senders": {10, 11}}}
    plan = v2.pull_plan({10, 11}, camps)
    assert plan["remove"] == {} and plan["blocked"] == {1: [10, 11]}


def test_an_inbox_already_tagged_bad_does_not_count_as_a_sender_left():
    camps = {1: {"name": "PL | A", "status": "ACTIVE", "senders": {10, 12}}}
    plan = v2.pull_plan({10}, camps, also_bad={12})
    assert plan["remove"] == {} and plan["blocked"] == {1: [10]}


def test_live_statuses():
    assert v2.is_live("ACTIVE") and v2.is_live("paused") and v2.is_live("DRAFTED") and v2.is_live("SCHEDULED")
    assert not v2.is_live("COMPLETED") and not v2.is_live("STOPPED") and not v2.is_live("ARCHIVED")


def test_monday_of_week_and_reminder_due():
    assert v2.last_monday(date(2026, 11, 11)) == date(2026, 11, 9)
    assert v2.last_monday(date(2026, 11, 9)) == date(2026, 11, 9)
    # Tuesday noon IST onwards, only if that Monday's results never came in
    assert v2.reminder_due(now_ist=(date(2026, 11, 10), 12), nights_in={"2026-11-02"}, reminded=set()) == "2026-11-09"
    assert v2.reminder_due(now_ist=(date(2026, 11, 10), 11), nights_in=set(), reminded=set()) is None
    assert v2.reminder_due(now_ist=(date(2026, 11, 9), 22), nights_in=set(), reminded=set()) is None
    assert v2.reminder_due(now_ist=(date(2026, 11, 11), 12), nights_in={"2026-11-09"}, reminded=set()) is None
    assert v2.reminder_due(now_ist=(date(2026, 11, 12), 9), nights_in=set(), reminded={"2026-11-09"}) is None
    # never for a Monday before the job's first run (no false alarm on day one)
    assert v2.reminder_due(now_ist=(date(2026, 11, 6), 23), nights_in=set(), reminded=set(), since="2026-11-06") is None
    assert v2.reminder_due(now_ist=(date(2026, 11, 10), 12), nights_in=set(), reminded=set(), since="2026-11-06") == "2026-11-09"
    # one CSV in no longer satisfies it when other DT tests are still missing (audit PLACEMENT-14a)
    assert v2.reminder_due(now_ist=(date(2026, 11, 10), 12), nights_in={"2026-11-09"}, reminded=set(),
                           missing=["DT PL #2"]) == "2026-11-09"
    assert v2.reminder_due(now_ist=(date(2026, 11, 10), 12), nights_in={"2026-11-09"}, reminded={"2026-11-09"},
                           missing=["DT PL #2"]) is None


def test_dt_missing_names_the_tests_with_no_result():
    tests = {"DT PL #1": ["a@x.com", "b@x.com"], "DT PL #2": ["c@x.com"], "DT Melior #4": []}
    assert v2.dt_missing(tests, {"b@x.com"}) == ["DT PL #2"]                  # one inbox in is enough
    assert v2.dt_missing(tests, set()) == ["DT PL #1", "DT PL #2"]            # an empty campaign is not expected


def test_emails_per_provider_reads_the_test_setting():
    one = v2.parse(csv_text([("a@x.com", G, "Spam"), ("a@x.com", M, "Inbox"), ("b@x.com", G, "Inbox"), ("b@x.com", M, "Inbox")]))
    assert v2.emails_per_provider(one) == 1
    assert v2.emails_per_provider(v2.parse(csv_text(seeds("a@x.com", 4, 4)))) == 4
    assert v2.emails_per_provider([]) == 0


def test_emails_per_provider_is_the_thinner_provider():
    """A test that sends 1 Google and 4 Microsoft emails per inbox can't be judged at Google, so it counts as a
    1-email test (seen 11 Nov 2026: the tie between the two counts read as 4 and nobody was asked to fix it)."""
    mixed = v2.parse(csv_text([r for mb in ("a@x.com", "b@x.com", "c@x.com") for r in seeds(mb, 1, 4, g_total=1)]))
    assert v2.emails_by_provider(mixed) == {"google": 1, "microsoft": 4}
    assert v2.emails_per_provider(mixed) == 1
    # one inbox whose fourth Microsoft email did not send does not make a 4-email test thin
    rows = [r for mb in ("a@x.com", "b@x.com", "c@x.com") for r in seeds(mb, 4, 4)] + seeds("d@x.com", 4, 3, m_total=3)
    assert v2.emails_per_provider(v2.parse(csv_text(rows))) == 4
    assert v2.emails_by_provider([]) == {}


def test_the_weekly_test_starts_on_a_saturday_sunday_or_monday():
    """Monday only until 10 Nov 2026. A 5-minute gap makes a 50-mailbox test take 33 hours, so the tests now
    start on Sunday and are in on Monday morning."""
    sat, sun, mon = "2026-11-07", "2026-11-08", "2026-11-09"
    assert all(v2.is_weekly_night(n) for n in (sat, sun, mon))
    assert not any(v2.is_weekly_night(n) for n in ("2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"))
    assert v2.week_start(date(2026, 11, 9)) == date(2026, 11, 7)


def test_sunday_tests_count_as_weeks_running_and_as_this_weeks_result():
    mon9, sun15, mon16, sun22 = "2026-11-09", "2026-11-15", "2026-11-16", "2026-11-22"
    assert v2.decide([r(mon9, v2.WATCH), r(sun15, v2.WATCH)], pulled=None)["action"] == v2.PULL     # the switch-over week
    assert v2.decide([r(sun15, v2.WATCH), r(sun22, v2.WATCH)], pulled=None)["action"] == v2.PULL
    assert v2.decide([r(sun15, v2.WATCH), r(mon16, v2.WATCH)], pulled=None)["action"] is None       # a day apart: one week
    # Tuesday noon: a Sunday or Saturday test is the week's result; Friday's belongs to the week before
    for night, due in (("2026-11-15", None), ("2026-11-14", None), ("2026-11-13", "2026-11-16")):
        assert v2.reminder_due(now_ist=(date(2026, 11, 17), 12), nights_in={night}, reminded=set()) == due


# --- own provider, failed domains (Manveen 11 Nov 2026) ------------------------------

def both(g, m):
    """One stored result: g and m seeds of 4 in the inbox."""
    return v2.summarise(v2.parse(csv_text(seeds("a@x.com", g, m))))["a@x.com"]


def test_an_inbox_is_judged_at_its_own_provider():
    assert v2.own_verdict(both(4, 0), "GMAIL") == v2.PASS            # Google to Google
    assert v2.own_verdict(both(0, 4), "GMAIL") == v2.FAIL
    assert v2.own_verdict(both(0, 4), "OUTLOOK") == v2.PASS          # Outlook to Outlook
    assert v2.own_verdict(both(4, 0), "OUTLOOK") == v2.FAIL
    assert v2.own_verdict(both(3, 0), "gmail ") == v2.WATCH          # the type is read whatever its case


def test_an_smtp_or_unknown_inbox_is_judged_on_both_providers_as_before():
    for kind in ("SMTP", "", None, "ZOHO"):
        assert v2.own_verdict(both(0, 4), kind) == v2.FAIL
        assert v2.own_verdict(both(4, 0), kind) == v2.FAIL
        assert v2.own_verdict(both(4, 3), kind) == v2.WATCH
        assert v2.own_verdict(both(4, 4), kind) == v2.PASS


def test_holds_are_applied_before_the_own_provider_verdict():
    assert v2.own_verdict(both(0, 4), "GMAIL", {"google"}) == v2.INCOMPLETE      # own provider held: not judged
    assert v2.own_verdict(both(0, 0), "GMAIL", {"microsoft"}) == v2.FAIL         # the other one held: no matter
    assert v2.own_verdict(both(0, 4), "SMTP", {"google"}) == v2.INCOMPLETE       # as apply_hold says today
    assert v2.own_verdict(both(0, 0), "SMTP", {"google"}) == v2.FAIL


def test_an_own_provider_that_was_not_judged_is_incomplete():
    short = v2.summarise(v2.parse(csv_text(seeds("a@x.com", 1, 4, g_total=1))))["a@x.com"]
    assert v2.own_verdict(short, "GMAIL") == v2.INCOMPLETE           # 1 Google email: too few to judge
    assert v2.own_verdict(short, "OUTLOOK") == v2.PASS               # its Microsoft seeds are complete
    dead = v2.summarise(v2.parse(csv_text([("a@x.com", G, "NOT_SENT")] * 4 + [("a@x.com", M, "NOT_SENT")] * 4)))
    assert v2.own_verdict(dead["a@x.com"], "GMAIL") == v2.NOT_SENT
    # a stored result with no per-provider part keeps its combined verdict
    assert v2.own_verdict({"night": "2026-11-09", "verdict": v2.FAIL}, "GMAIL") == v2.FAIL


def test_a_domain_fails_when_half_or_more_of_its_tested_inboxes_fail():
    verdicts = {"a@bad.com": v2.FAIL, "b@bad.com": v2.FAIL, "c@bad.com": v2.PASS, "d@bad.com": v2.INCOMPLETE,
                "a@fine.com": v2.FAIL, "b@fine.com": v2.PASS, "c@fine.com": v2.WATCH, "d@fine.com": v2.PASS,
                "a@clean.com": v2.PASS}
    assert v2.failed_domains(verdicts) == {(None, "bad.com"): (2, 4)}          # 2 of 4 yes, 1 of 4 no
    assert v2.failed_domains({"a@bad.com": v2.FAIL, "b@bad.com": v2.PASS, "c@bad.com": v2.PASS}) == {}
    assert v2.failed_domains({}) == {}


def test_domains_are_counted_per_client():
    verdicts = {"a@bad.com": v2.FAIL, "b@bad.com": v2.PASS, "c@bad.com": v2.PASS}
    clients = {"a@bad.com": "One", "b@bad.com": "Two", "c@bad.com": "Two"}
    assert v2.failed_domains(verdicts, clients) == {("One", "bad.com"): (1, 1)}


def test_a_domain_is_judged_on_all_its_inboxes_not_only_the_tested_ones():
    """A one-off test of one inbox must not rest a 25-inbox domain: the share is of the domain's inboxes."""
    client = {f"i{n}@big.com": "C" for n in range(4)}
    one = {"i0@big.com": v2.FAIL}
    assert v2.failed_domains(one, client) == {("C", "big.com"): (1, 1)}                      # no sizes known: as tested
    assert v2.failed_domains(one, client, sizes={("C", "big.com"): 4}) == {}                 # 1 of 4 inboxes
    two = {"i0@big.com": v2.FAIL, "i1@big.com": v2.FAIL}
    assert v2.failed_domains(two, client, sizes={("C", "big.com"): 4}) == {("C", "big.com"): (2, 4)}
    assert v2.failed_domains(two, client, sizes={("C", "big.com"): 1}) == {("C", "big.com"): (2, 2)}   # never below tested


# --- the copy check: a small test that grades the email, not the inboxes (11 Nov 2026) --------------

DT = {"DT PL #1": [f"i{n}@x.com" for n in range(50)], "DT PL copy check": ["i1@x.com", "i2@x.com", "i3@x.com"]}


def test_a_copy_check_campaign_is_known_by_its_name():
    assert v2.is_copy_check_campaign("DT PL copy check") and v2.is_copy_check_campaign("dt melior Copy Check ")
    assert not v2.is_copy_check_campaign("DT PL #1") and not v2.is_copy_check_campaign("PL copy check")
    assert v2.is_test_campaign("DT PL copy check")                         # so no inbox is ever pulled from it


def test_a_copy_check_file_is_known_by_its_name_in_any_spelling():
    for name in ("DT PL copy-check.csv", "dt_pl_copy_check_run2.csv", "DT PL Copy Check.csv", "plcopycheck.csv",
                 "COPYCHECK (1).csv"):
        assert v2.is_copy_check_file(name, ["anyone@x.com"], {}), name
    for name in ("dt-pl-1.csv", "copy of check.csv", "checkcopy.csv"):
        assert not v2.is_copy_check_file(name, ["anyone@x.com"], {}), name


def test_a_copy_check_file_is_known_by_its_mailboxes_too():
    assert v2.is_copy_check_file("export.csv", ["i1@x.com", "i2@x.com", "i3@x.com"], DT)
    assert v2.is_copy_check_file("export.csv", ["i2@x.com"], DT)           # a test on some of its senders
    assert not v2.is_copy_check_file("export.csv", DT["DT PL #1"], DT)     # an ordinary inbox test: 50 mailboxes
    assert not v2.is_copy_check_file("export.csv", ["i1@x.com", "other@y.com"], DT)
    assert not v2.is_copy_check_file("export.csv", [], DT)
    assert not v2.is_copy_check_file("export.csv", ["i1@x.com"], {"DT PL #1": ["i1@x.com"]})


def test_a_small_inbox_test_whose_inboxes_were_all_picked_is_still_an_inbox_test():
    dt = {"DT PL #3": ["a@x.com", "b@x.com"], "DT PL copy check": ["a@x.com", "b@x.com", "c@y.com"]}
    assert not v2.is_copy_check_file("export.csv", ["a@x.com", "b@x.com"], dt)        # exactly DT PL #3's own list
    assert v2.is_copy_check_file("export copy check.csv", ["a@x.com", "b@x.com"], dt)  # the name settles it
    same = {"DT PL #3": ["a@x.com", "b@x.com"], "DT PL copy check": ["a@x.com", "b@x.com"]}
    assert v2.is_copy_check_file("export.csv", ["a@x.com", "b@x.com"], same)           # cannot tell: never an inbox result


def copy_rows(mb, g_inbox, m_inbox):
    return v2.parse(csv_text(seeds(mb, g_inbox, m_inbox)))


def test_the_copy_verdict_is_the_share_of_emails_in_the_inbox():
    types = {f"g{n}@x.com": "GMAIL" for n in range(5)}

    def google(inbox_of_4):
        rows = [r for n, k in enumerate(inbox_of_4) for r in copy_rows(f"g{n}@x.com", k, 0)]
        return v2.copy_verdict(rows, types, set(types))["google"]

    assert google([4, 4, 4, 3, 0]) == {"inbox": 15, "sent": 20, "senders": 5, "left_out": 0, "verdict": v2.PASS}  # 75%
    assert google([4, 4, 4, 2, 0])["verdict"] == v2.WATCH                  # 70%: borderline
    assert google([4, 4, 3, 0, 0])["verdict"] == v2.WATCH                  # 55%
    assert google([4, 4, 2, 0, 0])["verdict"] == v2.FAIL                   # 50%
    assert google([0, 0])["verdict"] == v2.FAIL                            # 8 emails is enough to judge
    one = google([4])
    assert one["verdict"] == v2.NOT_JUDGED and (one["inbox"], one["sent"], one["senders"]) == (4, 4, 1)


def test_only_known_good_senders_count_and_each_type_is_judged_at_its_own_provider():
    types = {"g1@x.com": "GMAIL", "g2@x.com": "GMAIL", "g3@x.com": "gmail", "o1@y.com": "OUTLOOK",
             "o2@y.com": "OUTLOOK", "s@z.com": "SMTP"}
    rows = (copy_rows("g1@x.com", 0, 4) + copy_rows("g2@x.com", 0, 4) + copy_rows("g3@x.com", 4, 4)
            + copy_rows("o1@y.com", 0, 4) + copy_rows("o2@y.com", 0, 4) + copy_rows("s@z.com", 0, 0)
            + copy_rows("new@q.com", 0, 0))
    got = v2.copy_verdict(rows, types, {"g1@x.com", "g2@x.com", "o1@y.com", "o2@y.com", "s@z.com"})
    # g3 is not known good, so its 4 of 4 is left out; the Outlook inboxes' Google spam is not their result
    assert got["google"] == {"inbox": 0, "sent": 8, "senders": 2, "left_out": 1, "verdict": v2.FAIL}
    assert got["outlook"] == {"inbox": 8, "sent": 8, "senders": 2, "left_out": 0, "verdict": v2.PASS}
    assert set(got) == {"google", "outlook"}                               # an SMTP or unknown inbox is in neither
    nobody = v2.copy_verdict(rows, types, set())
    assert nobody["google"] == {"inbox": 0, "sent": 0, "senders": 0, "left_out": 3, "verdict": v2.NOT_JUDGED}


def test_an_email_with_no_placement_is_not_counted_in_the_copy_verdict():
    rows = v2.parse(csv_text([("g1@x.com", G, x) for x in ("Inbox", "Inbox", "Inbox", "")]
                             + [("g2@x.com", G, x) for x in ("Inbox", "BOUNCED", "Spam", "NOT_SENT")]
                             + [("g3@x.com", G, "Inbox")] * 4))
    got = v2.copy_verdict(rows, {m: "GMAIL" for m in ("g1@x.com", "g2@x.com", "g3@x.com")},
                          {"g1@x.com", "g2@x.com", "g3@x.com"})["google"]
    assert (got["inbox"], got["sent"], got["verdict"]) == (8, 9, v2.PASS)   # pending, bounced, not sent: left out


def test_stored_copy_counts_give_the_same_verdict_as_the_rows():
    rows = copy_rows("g1@x.com", 1, 4) + copy_rows("g2@x.com", 0, 4)
    counts = v2.copy_counts(rows)
    assert counts == {"g1@x.com": {"google": [1, 4], "microsoft": [4, 4]}, "g2@x.com": {"google": [0, 4], "microsoft": [4, 4]}}
    types, good = {"g1@x.com": "GMAIL", "g2@x.com": "GMAIL"}, {"g1@x.com", "g2@x.com"}
    assert v2.judge_copy(counts, types, good) == v2.copy_verdict(rows, types, good)


def test_a_night_belongs_to_the_monday_its_weekly_test_counts_for():
    for night in ("2026-11-07", "2026-11-08", "2026-11-09", "2026-11-11", "2026-11-13"):   # Sat, Sun, Mon, Wed, Fri
        assert v2.week_of(night) == date(2026, 11, 9), night
    assert v2.week_of("2026-11-14") == date(2026, 11, 16)


def test_a_copy_check_campaign_is_missing_until_its_clients_copy_check_arrived():
    dt = {"DT PL #1": ["a@x.com"], "DT PL copy check": ["a@x.com"], "DT Melior copy check": ["m@y.com"]}
    clients = {"a@x.com": "Precise Leads", "m@y.com": "Melior"}
    # a@x.com's inbox-test result is not the copy check's result
    assert v2.dt_missing(dt, {"a@x.com"}) == ["DT Melior copy check", "DT PL copy check"]
    assert v2.dt_missing(dt, {"a@x.com"}, {"Precise Leads"}, clients) == ["DT Melior copy check"]
    assert v2.dt_missing(dt, set(), {"Precise Leads", "Melior"}, clients) == ["DT PL #1"]


def test_the_client_of_a_file_is_the_client_of_most_of_its_mailboxes():
    clients = {"a@x.com": "Melior", "b@x.com": "Melior", "c@y.com": "Precise Leads"}
    assert v2.majority_client(["a@x.com", "b@x.com", "c@y.com", "new@z.com"], clients) == "Melior"
    assert v2.majority_client(["new@z.com"], clients) == "Unknown" and v2.majority_client([], {}) == "Unknown"
