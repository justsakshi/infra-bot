"""Kill warnings post: headline in the channel, details in the thread (spec 2026-10-13).

    python -m pytest test_kill_report.py -q
"""
import re
from datetime import date

import kill_report as kp
from smartlead import kill_rule as kr

TODAY = date(2026, 10, 13)
RULE = ("Should kill = two strikes 7+ days apart, under 50% on 20+ seeds, "
        "or a dead domain (40+ pooled seeds under 80%)")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


def _t(d, seeds, spam):
    return {"date": d, "seeds": seeds, "spam": spam, "by": {}}


def _fixture():
    """Shaped like the 13 Oct post: Bettrdata 13 kill, Precise Leads 6, 2 watch,
    129 good, 162 no recent test."""
    facts: dict[str, dict] = {}

    def add(email, client, tests, campaigns=None):
        facts[email] = {"client": client, "tests": tests, "campaigns": campaigns or []}

    # Bettrdata: three dead domains (rule c), 3 mailboxes each, ~65% inbox
    for dom in ("bettrdatasgroup.com", "ingestresolve.com", "dataingesthq.com"):
        for u in ("anna", "ben", "cara"):
            add(f"{u}@{dom}", "Bettrdata", [_t("2026-10-12", 20, 7)])
    # Bettrdata: 4 two-strike inboxes on a domain whose other mailboxes are fine
    for i, u in enumerate(("dan", "eve", "finn", "gia")):
        add(f"{u}@bettrdatahq.co", "Bettrdata", [_t("2026-10-12", 20, 6), _t("2026-10-05", 20, 7)],
            ["Marketplace | New Agencies"] if i == 0 else None)
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


def test_headline_is_short_with_counts_and_no_addresses():
    head, thread = kp.format_report(_build(), [], TODAY)
    assert 1 <= len(head) <= 4
    assert head[0] == ("🚨 Kill warnings, 13 Oct: 19 inboxes should come out of campaigns (6 domains), "
                       "2 to watch")
    assert head[1] == "Bettrdata 13 · Precise Leads 6 · Melior 0"
    assert "1 still in live campaigns" in head
    assert not EMAIL.search("\n".join(head))
    assert "Should kill" not in "\n".join(head) and "warning only" not in "\n".join(head).lower()


def test_every_inbox_is_in_the_thread_once_per_client():
    res = _build()
    head, thread = kp.format_report(res, [], TODAY)
    body = "\n".join(thread)
    for d in res["by_client"].values():
        for r in d.get(kr.KILL, []) + d.get(kr.WATCH, []):
            assert r["email"] in body, r["email"]
    assert any(p.startswith("*Bettrdata") for p in thread) and any(p.startswith("*Precise Leads") for p in thread)
    assert ":rotating_light: still in Marketplace | New Agencies" in body


def test_domain_reason_said_once_per_domain():
    head, thread = kp.format_report(_build(), [], TODAY)
    bd = next(p for p in thread if p.startswith("*Bettrdata"))
    # dead domain: one header line with the reason, its inboxes under it
    assert bd.count("*bettrdatasgroup.com*: whole domain") == 1
    assert bd.count("pooled over") == 3          # three dead Bettrdata domains, one reason each
    assert "two strikes" in bd


def test_watch_list_then_rule_last():
    head, thread = kp.format_report(_build(), [], TODAY)
    watch = next(p for p in thread if "To watch" in p)
    assert "virginia@myscalegrid.co" in watch and "rmarkman@meliorbase.com" in watch
    assert thread.index(watch) == len(thread) - 2
    last = thread[-1]
    assert RULE in last and "take them out of campaigns" in last.lower() and "next bill" in last
    assert "(warning only; nothing is changed)" in last
    everything = "\n".join(head + thread)
    assert everything.count(RULE) == 1 and everything.count("warning only") == 1


def test_nothing_to_take_out():
    facts = {"a@ok.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 0)]}}
    head, thread = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head[0] == "✅ Kill warnings, 13 Oct: nothing to take out"
    assert RULE in thread[-1]


def test_warning_icon_when_none_still_live():
    facts = {"a@x.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 12)]}}
    head, _ = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head[0].startswith("⚠️ Kill warnings, 13 Oct: 1 inbox should come out of campaigns (1 domain)")
    assert not any("still in live" in h for h in head)


def test_read_errors_are_said_plainly():
    head, thread = kp.format_report(_build(), ["Smartlead (Bettrdata): timeout"], TODAY)
    assert len(head) <= 4
    assert any("Could not read Smartlead (Bettrdata)" in h for h in head)


def test_render_text_has_both_parts():
    head, thread = kp.format_report(_build(), [], TODAY)
    text = kp.render_text(head, thread)
    assert text.startswith(head[0]) and thread[-1] in text and "virginia@myscalegrid.co" in text
