"""Kill warnings post: headline in the channel, details in the thread (spec 2026-10-13).

    python -m pytest test_kill_report.py -q
"""
import re
from datetime import date

import kill_report as kp
from smartlead import kill_rule as kr

TODAY = date(2026, 10, 13)
RULE = "Rule: an inbox passes a test when 80%+ of its test emails reach the inbox."
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
    add("rmarkman@meliorbase.com", "Melior", [_t("2026-10-12", 20, 6)],
        ["Accounting firm roundtable | Melior"], ["Accounting firm roundtable | Melior"])
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


BILLS = {"bettrdatasgroup.com": {"bills_on": "2026-11-08", "provider": "Zapmail"},
         "gopreciseleads.com": {"bills_on": "2026-11-12", "provider": "ScaledMail"}}


def _report(bills=BILLS):
    return kp.format_report(_build(), [], TODAY, bills)


def test_headline_counts_domains_and_says_what_to_do():
    head, parts = _report()
    assert head[0] == "🚨 Kill warnings, 13 Oct: 6 domains are landing in spam, stop using them"
    assert head[1] == "1 of their inboxes is still sending: take it out of campaigns today."
    assert len(head) == 2 and not EMAIL.search("\n".join(head))


def test_needs_a_look_today_comes_first():
    head, parts = _report()
    look = parts[0].split("\n")
    assert look[0] == "*Needs a look today*"
    assert any(l.startswith("• `dan@bettrdatahq.co` is sending in Ingest | Main") for l in look)
    assert any(l.startswith("• `eve@bettrdatahq.co` is in 1 campaign that is switched on but not sending "
                            "(Marketplace | New Agencies). Pause it") for l in look)
    assert any(l.startswith("• `rmarkman@meliorbase.com` is sending in Accounting firm roundtable | Melior "
                            "and failed its last test (70% reached the inbox). Retest it this week") for l in look)


def test_one_line_per_domain_with_its_bill():
    head, parts = _report()
    stop = next(p for p in parts if p.startswith("*Stop using these domains*")).split("\n")
    assert "• Bettrdata · bettrdatasgroup.com (3 inboxes) · 65% reached the inbox · bills 8 Nov" in stop
    assert "• Bettrdata · bettrdatahq.co (4 inboxes) · 70% reached the inbox · no bill found" in stop
    assert "• Precise Leads · gopreciseleads.com (3 inboxes) · 65% reached the inbox · bills 12 Nov" in stop
    assert len(stop) == 1 + 6 and not any("@" in l for l in stop)


def test_retest_then_rule_last_in_one_message():
    head, parts = _report()
    retest = next(p for p in parts if p.startswith("*Retest*"))
    assert retest == "*Retest*\n• `virginia@myscalegrid.co` (Bettrdata) · failed 1 test (70%)"
    assert parts[-1].startswith("_" + RULE) and "129 passed, 162 with no recent test" in parts[-1]
    assert "Warning only: the bot changes nothing." in parts[-1]
    text = kp.render_text(head, parts)
    assert text.startswith(head[0]) and "thread" not in text and text.count(RULE) == 1
    assert not any(w in text for w in ("strike", "pooled", "seeds", "struck"))


def test_without_bills_no_bill_text():
    head, parts = kp.format_report(_build(), [], TODAY, None)
    assert "bills " not in "\n".join(parts) and "no bill found" not in "\n".join(parts)


def test_nothing_to_take_out():
    facts = {"a@ok.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 0)]}}
    head, parts = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head == ["✅ Kill warnings, 13 Oct: no domain is landing in spam"]
    assert parts[-1].startswith("_" + RULE)


def test_none_sending_says_wait_for_the_bill():
    facts = {"a@x.com": {"client": "Melior", "tests": [_t("2026-10-12", 20, 12)], "campaigns": [], "sending": []}}
    head, _ = kp.format_report(kp.build(facts, TODAY, {}), [], TODAY)
    assert head == ["⚠️ Kill warnings, 13 Oct: 1 domain is landing in spam, stop using it",
                    "None of them is sending right now. Don't add them to campaigns; retire or replace them at their next bill."]


def test_read_errors_are_said_plainly():
    head, parts = kp.format_report(_build(), ["Smartlead (Bettrdata): timeout"], TODAY)
    assert any("Could not read Smartlead (Bettrdata)" in h for h in head)


def test_bill_dates_by_domain_takes_the_earliest():
    bills = [{"bills_on": "2026-11-08", "provider": "Zapmail", "label": "21 Google inboxes", "price": 68.25,
              "inboxes": ["a@one.com", "b@one.com"]},
             {"bills_on": "2026-10-27", "provider": "Zapmail", "label": "3 Google", "price": 9, "inboxes": ["c@one.com"]},
             {"bills_on": "2026-10-14", "provider": "Zapmail", "label": "12 Outlook pre-warmed", "price": 84, "inboxes": []}]
    out = kp.bill_dates(bills)
    assert out == {"one.com": {"bills_on": "2026-10-27", "provider": "Zapmail", "label": "3 Google", "price": 9}}


def test_bill_dates_from_order_domains_without_inbox_lists():
    bills = [{"bills_on": "2026-11-12", "provider": "ScaledMail", "label": "21 × Google", "price": 73.5,
              "inboxes": [], "domains": ["exactclients.com", "SolidClients.com"]}]
    assert kp.bill_dates(bills) == {d: {"bills_on": "2026-11-12", "provider": "ScaledMail", "label": "21 × Google", "price": 73.5}
                                    for d in ("exactclients.com", "solidclients.com")}


def test_bill_read_errors_are_not_called_missing_inboxes():
    head, parts = kp.format_report(_build(), [], TODAY, BILLS, bill_errors=["ScaledMail: 429"])
    assert "Bill dates may be missing (could not read ScaledMail: 429)" in head
    assert not any("inboxes there are missing" in h for h in head)
