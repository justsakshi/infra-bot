"""Rules review (12 Nov 2026): each rule held up against an outcome it did not use. Pure tests, no network."""
from datetime import date

from smartlead import rules_review as rv

T = date(2026, 10, 20)


def _inbox(email, workspace="Google"):
    return {"type": "INBOX", "name": email, "workspace": workspace}


def _v(email, night, verdict, client="Acme"):
    return {"email": email, "night": night, "verdict": verdict, "client": client}


def _reply(domain, verdict, day="2026-10-19", sent=300, replies=6):
    return {"domain": domain, "date": day, "verdict": verdict, "window_sent": sent, "window_replies": replies,
            "expected": 6.0}


def _group(n, verdict, reply, workspace="Google", start=0):
    """n domains of 3 inboxes, every inbox with this test verdict, every domain with this reply-check verdict."""
    assets, v2, replies = [], [], []
    for d in range(start, start + n):
        dom = f"d{d}.com"
        for i in range(3):
            assets.append(_inbox(f"u{i}@{dom}", workspace))
            v2.append(_v(f"u{i}@{dom}", "2026-10-17", verdict))
        if reply:
            replies.append(_reply(dom, reply))
    return assets, v2, replies


def _rule(res, name="the test decides", group="Acme Google"):
    return next(r for r in res["rules"] if r["rule"] == name and r["group"] == group)


def test_a_test_that_fails_domains_people_answer_is_in_doubt_with_a_proposal():
    assets, v2, replies = _group(4, "FAIL", "OK")
    r = _rule(rv.review(v2, replies, assets, T))
    assert r["state"] == rv.DOUBT
    assert r["evidence"] == "4 of 4 domains that fail the test get replies at the same rate as the client's other domains"
    assert r["proposal"].startswith("For Acme Google inboxes, rest an inbox only when the test and the reply check agree")


def test_a_test_that_agrees_with_replies_holds():
    a1, v1, r1 = _group(3, "FAIL", "ALERT")
    a2, v2, r2 = _group(3, "PASS", "OK", start=3)
    r = _rule(rv.review(v1 + v2, r1 + r2, a1 + a2, T))
    assert r["state"] == rv.HOLDS and r["proposal"] == ""
    assert r["evidence"] == "test and replies agree on 6 of 6 domains"


def test_a_test_that_passes_domains_nobody_answers_is_in_doubt():
    assets, v2, replies = _group(4, "PASS", "ALERT", workspace="Outlook")
    r = _rule(rv.review(v2, replies, assets, T), group="Acme Outlook")
    assert r["state"] == rv.DOUBT and "far fewer replies" in r["evidence"] and "copy check" in r["proposal"]


def test_too_few_domains_with_both_sides_is_not_judged():
    assets, v2, replies = _group(3, "FAIL", "OK")                      # one short of MIN_DOMAINS
    a2, v22, _ = _group(5, "FAIL", None, start=3)                      # tested, but the reply check has no view
    r = _rule(rv.review(v2 + v22, replies, assets + a2, T))
    assert r["state"] == rv.THIN and r["proposal"] == ""
    assert r["evidence"].startswith("3 of 8 tested domains have both")


def test_only_fresh_tests_fresh_reply_days_and_clear_views_count():
    assets, v2, _ = _group(4, "FAIL", None)
    old_test = [{**r, "night": "2026-09-01"} for r in v2]
    assert rv.domain_tests(old_test, rv.inbox_types(assets), T) == {}
    views = rv.reply_views([_reply("d0.com", "OK", "2026-10-10"), _reply("d1.com", "WATCH"), _reply("d2.com", "TOO FEW"),
                            _reply("d3.com", "ALERT", "2026-10-18"), _reply("d3.com", "OK", "2026-10-19")], T)
    assert set(views) == {"d3.com"} and views["d3.com"]["view"] == "OK"         # newest fresh day; no-view verdicts out
    thin = rv.reply_views([_reply("d4.com", "OK", replies=3), _reply("d5.com", "OK", replies=0)], T)
    assert thin == {}                                         # "OK" with half the expected replies, or none, is no view


def test_a_domain_is_judged_like_the_job_judges_it_and_groups_are_by_client_and_type():
    assert rv.domain_test({"pass": 1, "watch": 0, "fail": 2}) == "FAIL"
    assert rv.domain_test({"pass": 2, "watch": 1, "fail": 0}) == "MIXED"
    assert rv.domain_test({"pass": 3, "watch": 0, "fail": 0}) == "PASS"
    assert rv.domain_test({"pass": 0, "watch": 0, "fail": 0}) == "MIXED"
    g, vg, rg = _group(4, "FAIL", "OK")
    o, vo, ro = _group(4, "PASS", "OK", workspace="Microsoft 365", start=4)
    res = rv.review(vg + vo, rg + ro, g + o, T)
    assert _rule(res)["state"] == rv.DOUBT and _rule(res, group="Acme Outlook")["state"] == rv.HOLDS
    untracked = rv.review(vg, rg, [], T)                                         # not in the tracker: still reviewed
    assert _rule(untracked, group=f"Acme {rv.UNKNOWN_TYPE}")["state"] == rv.DOUBT


def test_only_the_newest_result_of_an_inbox_is_its_test():
    assets = [_inbox("a@x.com")]
    tests = rv.domain_tests([_v("a@x.com", "2026-10-10", "FAIL"), _v("a@x.com", "2026-10-17", "PASS"),
                             _v("a@x.com", "2026-10-18", "INCOMPLETE")], rv.inbox_types(assets), T)
    assert tests[("Acme", "Google", "x.com")] == {"pass": 1, "watch": 0, "fail": 0, "night": "2026-10-17"}


def _history(email, *nights):
    return [_v(email, n, v) for n, v in nights]


def test_the_return_rule_counts_who_came_back_and_who_failed_again():
    back_ok = _history("a@x.com", ("2026-09-01", "FAIL"), ("2026-09-08", "PASS"), ("2026-09-15", "PASS"), ("2026-09-22", "PASS"))
    back_bad = _history("b@x.com", ("2026-09-01", "FAIL"), ("2026-09-08", "PASS"), ("2026-09-15", "PASS"), ("2026-09-22", "FAIL"))
    too_soon = _history("c@x.com", ("2026-09-01", "FAIL"), ("2026-09-08", "PASS"), ("2026-09-10", "PASS"))
    reset = _history("d@x.com", ("2026-09-01", "FAIL"), ("2026-09-08", "PASS"), ("2026-09-12", "WATCH"), ("2026-09-15", "PASS"))
    never_failed = _history("e@x.com", ("2026-09-08", "PASS"), ("2026-09-15", "PASS"))
    assert rv.returns(back_ok + back_bad + too_soon + reset + never_failed) == {"came_back": 2, "failed_again": 1}


def test_the_return_rule_needs_enough_returns_and_doubts_at_a_third():
    assert rv.judge_return_rule({"came_back": 2, "failed_again": 2})["state"] == rv.THIN
    doubt = rv.judge_return_rule({"came_back": 6, "failed_again": 2})
    assert doubt["state"] == rv.DOUBT and doubt["proposal"] == "Ask for three passes before an inbox comes back"
    assert rv.judge_return_rule({"came_back": 6, "failed_again": 1})["state"] == rv.HOLDS


def test_the_post_says_what_is_in_doubt_and_that_nothing_changes_by_itself():
    assets, v2, replies = _group(4, "FAIL", "OK")
    head, thread = rv.format_review(rv.review(v2, replies, assets, T))
    assert head == ["Rules review, 20 Oct: 1 in doubt, 0 holding, 1 without enough data yet",
                    "In doubt: the test decides (Acme Google)"]
    text = "\n".join(thread)
    assert "Proposal, needs Manveen's yes:" in text and "Nothing changes by itself" in text
    assert "0 inboxes have come back so far; 5 are needed" in text
