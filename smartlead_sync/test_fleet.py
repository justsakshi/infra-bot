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

def test_watch_text_groups_new_problems_by_action():
    new = run([rec("w1@d.com", warmup_on=False), rec("w2@d.com", warmup_on=False), rec("k@e.com", verdict="SHOULD KILL")])
    for c in new:
        c["opened_on"] = "2026-10-20"
    t = fl.watch_text(new, open_count={"warmup_off": 2, "kill": 1, "no_test": 4}, today=TODAY)
    lines = t.splitlines()
    assert lines[0] == "Fleet watch, 20 Oct: 3 new problems (suggestions only, nothing was changed)"
    assert any(l.startswith("Turn warmup back on (warmup off): w1@d.com, w2@d.com") for l in lines)
    assert any(l.startswith("Take it out of campaigns, do not renew (kill list): k@e.com") for l in lines)
    assert "Still open: 7" in t


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
