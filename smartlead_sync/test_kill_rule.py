"""Kill rule (warning only).   python3 -m pytest test_kill_rule.py -q"""
from datetime import date

from smartlead import kill_rule as kr

T = date(2026, 10, 8)


def t(d, seeds, spam, g=None, o=None):
    by = {}
    if g:
        by["Google"] = list(g)
    if o:
        by["Outlook"] = list(o)
    return {"date": d, "seeds": seeds, "spam": spam, "by": by}


def v(tests, dom=None):
    return kr.judge("a@x.com", tests, dom, T)["verdict"]


def test_small_tests_do_not_count():
    assert v([t("2026-10-05", 6, 6)]) == kr.NO_DATA            # 6 seeds, all spam: not evidence
    assert kr.grade(t("2026-10-05", 9, 0)) is None


def test_one_strike_is_watch_not_kill():
    assert v([t("2026-10-05", 20, 6)]) == kr.WATCH               # 70%
    assert v([t("2026-10-05", 20, 2)]) == kr.GOOD                # 90%


def test_two_strikes_seven_days_apart_kill():
    assert v([t("2026-10-05", 20, 6), t("2026-09-28", 20, 7)]) == kr.KILL
    assert v([t("2026-10-05", 20, 6), t("2026-10-02", 20, 7)]) == kr.WATCH   # only 3 days apart
    assert v([t("2026-10-05", 20, 6), t("2026-09-28", 20, 1)]) == kr.WATCH   # old test was fine


def test_clearly_dead_on_one_test():
    assert v([t("2026-10-05", 20, 12)]) == kr.KILL               # 40% on 20 seeds
    assert v([t("2026-10-05", 15, 10)]) == kr.WATCH              # 33% but only 15 seeds


def test_worse_provider_decides():
    # 85% overall, but Google 6/10 = 60%: a strike
    assert kr.grade(t("2026-10-05", 20, 3, g=(10, 4), o=(10, 0))) == 60.0
    assert v([t("2026-10-05", 20, 3, g=(10, 4), o=(10, 0))]) == kr.WATCH


def test_stale_tests_are_no_data():
    assert v([t("2026-09-01", 20, 12)]) == kr.NO_DATA


def test_domain_rule_pools_mailboxes():
    tests = {f"m{i}@d.com": [t("2026-10-05", 20, 6)] for i in range(3)}          # 70% x3 = 60 seeds
    out = kr.judge_all(tests, T)
    assert {r["verdict"] for r in out.values()} == {kr.KILL} and out["m0@d.com"]["rule"] == "c"
    few = {f"m{i}@e.com": [t("2026-10-05", 12, 4)] for i in range(3)}            # 36 pooled seeds: too few
    assert {r["verdict"] for r in kr.judge_all(few, T).values()} == {kr.WATCH}
    mixed = {"a@f.com": [t("2026-10-05", 20, 8)], "b@f.com": [t("2026-10-05", 20, 0)],
             "c@f.com": [t("2026-10-05", 20, 0)]}                                 # 1 of 3 struck
    out = kr.judge_all(mixed, T)
    assert out["a@f.com"]["verdict"] == kr.WATCH and out["b@f.com"]["verdict"] == kr.GOOD


def test_todays_precise_leads_case():
    """6 domains, 3 mailboxes each, ~20 seeds, 55-71% on 5 Oct -> kill by the domain rule."""
    tests = {f"{u}@gopreciseleads.com": [t("2026-10-05", 20, s)] for u, s in (("a", 8), ("b", 9), ("c", 6))}
    assert {r["verdict"] for r in kr.judge_all(tests, T).values()} == {kr.KILL}


def test_kill_report_groups_by_client_and_warns_live_campaigns():
    import kill_report as kp
    tests = [{"date": "2026-10-05", "seeds": 20, "spam": 12, "by": {}}]          # 40%: clearly dead
    facts = {"a@x.com": {"client": "Melior", "tests": tests, "campaigns": ["Exec search"]},
             "b@y.com": {"client": "Melior", "tests": [{"date": "2026-10-05", "seeds": 20, "spam": 6, "by": {}}]},
             "c@z.com": {"client": "Belardi Wong", "tests": tests}}                 # past client: left out
    res = kp.build(facts, T, {"a@x.com": "Scaledmail"})
    assert res["counts"][kr.KILL] == 1 and res["counts"][kr.WATCH] == 1
    txt = kp.format_report(res, [], T)
    assert "warning only" in txt and "`a@x.com` (Scaledmail)" in txt and "still in Exec search" in txt
    assert "c@z.com" not in txt
    assert "could not read" in kp.format_report(res, ["Smartlead X: 429"], T)
