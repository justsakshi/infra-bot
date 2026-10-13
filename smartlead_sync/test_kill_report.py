"""Kill warnings post: headline in the channel, details in the thread (spec 2026-10-13).

    python -m pytest test_kill_report.py -q
"""
import re
from datetime import date

import kill_report as kp
from smartlead import kill_rule as kr

TODAY = date(2026, 10, 13)
RULE = "How we decide: a test passes when 80%+ of its emails reach the inbox."
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


def _t(d, seeds, spam):
    return {"date": d, "seeds": seeds, "spam": spam, "by": {}}


def _fixture():
    """Shaped like the 13 Oct post: Bettrdata 13 kill, Precise Leads 6, 2 watch,
    129 good, 162 no recent test."""
    facts: dict[str, dict] = {}

    def add(email, client, tests, campaigns=None, sending=None):
        facts[email] = {"client": client, "tests": tests, "campaigns": campaigns or [], "sending": sending or []}

    # Bettrdata: three dead domains (rule c), 3 mailboxes each, ~65% inbox
    for dom in ("bettrdatasgroup.com", "ingestresolve.com", "dataingesthq.com"):
        for u in ("anna", "ben", "cara"):
            add(f"{u}@{dom}", "Bettrdata", [_t("2026-10-12", 20, 7)])
    # Bettrdata: 4 two-strike inboxes on a domain whose other mailboxes are fine
    # dan sends today; eve sits only in a switched-on campaign that sent nothing in 7 days
    for u in ("dan", "eve", "finn", "gia"):
        add(f"{u}@bettrdatahq.co", "Bettrdata", [_t("2026-10-12", 20, 6), _t("2026-10-05", 20, 7)],
            {"dan": ["Ingest | Main"], "eve": ["Marketplace | New Agencies"]}.get(u),
            {"dan": ["Ingest | Main"]}.get(u))
    for i in range(5):
        add(f"ok{i}@bettrdatahq.co", "Bettrdata", [_t("2026-10-12", 20, 1)])
    # Precise Leads: two dead domains
    for dom in ("preciseleadsagency.com", "gopreciseleads.com"):
        for u in ("max", "nia", "oli"):
            add(f"{u}@{dom}", "Precise Leads", [_t("2026-10-12", 20, 7)])
    # watch: one strike each, on domains with healthy siblings
    add("virginia@myscalegrid.co", "Bettrdata", [_t("2026-10-12", 20, 6)])
    add("rmarkman@meliorbase.com", "Melior", [_t("2026-10-12", 20, 6)])
    for i in range(3):
        add(f"good{i}@myscalegrid.co", "Bettrdata", [_t("2026-10-12", 20, 0)])
        add(f"good{i}@meliorbase.com", "Melior", [_t("2026-10-12", 20, 0)])
    # the rest: good, and no recent test
    n_good = 129 - 5 - 6
    for i in range(n_good):
        add(f"g{i}@fleet{i % 20}.com", ("Bettrdata", "Melior", "Precise Leads")[i % 3], [_t("2026-10-12", 20, 1)])
    for i in range(162):
        add(f"n{i}@quiet{i % 30}.com", ("Bettrdata", "Melior", "Precise Leads")[i % 3], [])
    # a past client: left out entirely
    add("x@belardi.com", "Belardi Wong", [_t("2026-10-12", 20, 15)])
    return facts


def _build():
    return kp.build(_fixture(), TODAY, {})


def test_fixture_counts_match_todays_post():
    c = _build()["counts"]
    assert (c[kr.KILL], c[kr.WATCH], c[kr.GOOD], c[kr.NO_DATA]) == (19, 2, 129, 162)


def test_headline_says_what_to_do_and_who_is_still_sending():
    head, thread = kp.format_report(_build(), [], TODAY)
    assert 1 <= len(head) <= 4
    assert head[0] == ("🚨 Kill warnings, 13 Oct: 19 inboxes are landing in spam, "
                       "take them out of campaigns (6 domains)")
    assert head[1] == "1 still sending: `dan@bettrdatahq.co`, start there"
    assert head[2] == "Bettrdata 13 · Precise Leads 6 · Melior 0 · 2 to retest"
    assert not any(w in "\n".join(head) for w in ("strike", "pooled", "seeds"))


def test_switched_on_but_silent_campaigns_are_not_called_sending():
    head, thread = kp.format_report(_build(), [], TODAY)
    body = "\n".join(thread)
    assert "🚨 still sending in: Ingest | Main" in body
    assert "eve@bettrdatahq.co" in body and "in switched-on campaigns that sent nothing in 7 days: Marketplace | New Agencies" in body
    assert "still sending in: Marketplace" not in body


def test_every_inbox_is_in_the_thread_once_per_client():
    res = _build()
    head, thread = kp.format_report(res, [], TODAY)
    body = "\n".join(thread)
    for d in res["by_client"].values():
        for r in d.get(kr.KILL, []) + d.get(kr.WATCH, []):
            assert r["email"] in body, r["email"]
    assert any(p.startswith("*Bettrdata: 13 inboxes to take out*") for p in thread)
    assert any(p.startswith("*Precise Leads: 6 inboxes to take out*") for p in thread)


def test_reasons_in_plain_words_domain_said_once():
    head, thread = kp.format_report(_build(), [], TODAY)
    bd = next(p for p in thread if p.startswith("*Bettrdata"))
    assert bd.count("*bettrdatasgroup.com* — whole domain: 65% of 60 test emails reached the inbox (needs 80%)") == 1
    assert bd.count("whole domain:") == 3
    assert "failed 2 tests: 65% (5 Oct), then 70% (12 Oct)" in bd
    assert not any(w in "\n".join(thread) for w in ("strike", "pooled", "seeds", "struck"))


def test_retest_list_then_how_we_decide_last():
    head, thread = kp.format_report(_build(), [], TODAY)
    watch = next(p for p in thread if p.startswith("*To retest (2)"))
    assert "virginia@myscalegrid.co" in watch and "rmarkman@meliorbase.com" in watch
    assert "failed 1 test: 70% of 20 test emails reached the inbox (12 Oct)" in watch
    assert thread.index(watch) == len(thread) - 2
    last = thread[-1]
    assert RULE in last and "2 failed tests a week apart" in last and "next bill" in last
    assert "Warning only: the bot changes nothing." in last
    everything = "\n".join(head + thread)
    assert everything.count(RULE) == 1


def test_nothing_to_take_out():
    facts = {"a@ok.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 0)]}}
    head, thread = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head[0] == "✅ Kill warnings, 13 Oct: nothing to take out"
    assert RULE in thread[-1]


def test_warning_icon_when_none_still_sending():
    facts = {"a@x.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 12)], "campaigns": ["Old"], "sending": []}}
    head, _ = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head[0].startswith("⚠️ Kill warnings, 13 Oct: 1 inbox is landing in spam, take it out of campaigns (1 domain)")
    assert head[1] == "None is sending now; 1 sits in switched-on campaigns that sent nothing in 7 days"


def test_read_errors_are_said_plainly():
    head, thread = kp.format_report(_build(), ["Smartlead (Bettrdata): timeout"], TODAY)
    assert len(head) <= 5
    assert any("Could not read Smartlead (Bettrdata)" in h for h in head)


def test_render_text_has_both_parts():
    head, thread = kp.format_report(_build(), [], TODAY)
    text = kp.render_text(head, thread)
    assert text.startswith(head[0]) and thread[-1] in text and "virginia@myscalegrid.co" in text
