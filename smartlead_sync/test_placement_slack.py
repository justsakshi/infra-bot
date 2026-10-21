"""Deliverability test results post: headline in the channel, details in the thread
(spec 2026-10-13). Fixtures follow the real 13 Oct posts. No network."""
import re

from smartlead.placement_report import build_report, domain_verdict, format_slack

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PROV = {"g.com": "Google", "o.com": "Outlook"}


def precise_leads():
    """Precise Leads, 13 Oct: all 30 mailboxes land in the inbox."""
    senders = {f"m{i}@pl{i // 3}.com": [{"seed": "x@g.com", "folder": "Inbox"},
                                         {"seed": "y@o.com", "folder": "Inbox"}] for i in range(30)}
    return build_report(senders, {e: "GMAIL" for e in senders}, lambda d: PROV.get(d, "other"))


def _inbox(email, spam, campaigns=()):
    return {"email": email, "domain": email.split("@")[1], "sender": "Google",
            "seeds": 10, "spam": 6 if spam else 0, "pct": 40.0 if spam else 100.0,
            "status": "bad" if spam else "good", "campaigns": list(campaigns), "by_receiver": {}}


def bettrdata():
    """Bettrdata, 13 Oct: 11 of 71 mailboxes in spam, 3 domains in spam, 3 to keep
    out of new campaigns, the four routes as posted that day."""
    layout = {"s1.com": (2, 2), "s2.com": (3, 2), "s3.com": (3, 3),     # spam domains
              "a1.com": (3, 1), "a2.com": (3, 1), "a3.com": (5, 2)}     # keep out of new
    layout.update({f"ok{i}.com": (2, 0) for i in range(26)})
    inboxes, n = {}, 0
    for dom, (total, spam) in layout.items():
        for j in range(total):
            email = f"box{j}@{dom}"
            camps = ["Ingest Q4 - Retail"] if spam and j == 0 and n % 2 == 0 else []
            inboxes[email] = _inbox(email, j < spam, camps)
        n += 1
    domains = {}
    for dom, (total, spam) in layout.items():
        v = domain_verdict(total, spam)
        domains[dom] = {"domain": dom, "verdict": v, "inboxes": total, "mailboxes_spam": spam,
                        "status": {"good": "good", "avoid_new": "watch", "spam": "bad"}[v]}
    pairs = {("Google", "Google"): {"seeds": 268, "spam": 43, "inboxes_hit": 9},
             ("Google", "Outlook"): {"seeds": 247, "spam": 0, "inboxes_hit": 0},
             ("Outlook", "Outlook"): {"seeds": 119, "spam": 0, "inboxes_hit": 0},
             ("Outlook", "Google"): {"seeds": 113, "spam": 16, "inboxes_hit": 2}}
    return {"inboxes": inboxes, "domains": domains, "pairs": pairs}


def test_all_clear_headline_is_one_line():
    head, parts = format_slack("Precise Leads", precise_leads(), ["t"], "2026-10-13")
    assert head == ["✅ Precise Leads, 13 Oct: all 30 mailboxes land in the inbox"]
    assert len(parts) == 1 and "Where the test emails landed" in parts[0]
    assert "Google → Google: 0 of 30 in spam (0%)" in parts[0]


def test_spam_headline_counts_and_routes_over_5_percent():
    head, _ = format_slack("Bettrdata", bettrdata(), ["t"], "2026-10-13")
    # spam mailboxes still in live campaigns: act today (same rule as kill warnings)
    assert head[0] == "\U0001f6a8 Bettrdata, 13 Oct: 11 of 71 mailboxes in spam (3 domains in spam)"
    assert head[1] == "3 still in live campaigns"
    assert head[2] == "Google→Google 16% spam · Outlook→Google 14%"
    assert len(head) <= 4
    assert not any(EMAIL.search(l) for l in head), "no mailbox address in the channel message"


def test_every_spam_mailbox_is_in_the_thread_with_its_action():
    rep = bettrdata()
    _, parts = format_slack("Bettrdata", rep, ["t"], "2026-10-13")
    thread = "\n".join(parts)
    for i in rep["inboxes"].values():
        if i["status"] == "bad":
            todo = f"remove from {i['campaigns'][0]}" if i["campaigns"] else "not in a live campaign"
            assert re.search(re.escape(i["email"]) + r" · " + re.escape(todo), thread), i["email"]
    assert "box0@ok0.com" not in thread, "inbox mailboxes are not listed"


def test_thread_order_mailboxes_domains_routes():
    _, parts = format_slack("Bettrdata", bettrdata(), ["t"], "2026-10-13")
    assert len(parts) == 3
    assert parts[0].startswith("*Mailboxes in spam (11)*")
    assert "a1.com, a2.com, a3.com" in parts[1] and "s1.com, s2.com, s3.com" in parts[1]
    assert parts[1].index("out of new campaigns") < parts[1].index("in spam")
    assert "Google → Google: 43 of 268 in spam (16%)" in parts[2]
    assert "Outlook → Google: 16 of 113 in spam (14%)" in parts[2]


def test_rule_text_said_once():
    _, parts = format_slack("Bettrdata", bettrdata(), ["t"], "2026-10-13")
    assert "\n".join(parts).count("half or more") == 1


def test_spam_but_none_in_a_live_campaign_is_a_warning():
    rep = bettrdata()
    for i in rep["inboxes"].values():
        i["campaigns"] = []
    for i in list(rep["inboxes"].values())[:40]:
        i.update(status="bad", pct=10.0)
    head, _ = format_slack("Bettrdata", rep, ["t"], "2026-10-13")
    assert head[0].startswith("⚠️ Bettrdata, 13 Oct:") and "live campaigns" not in "\n".join(head)


def test_no_route_line_when_all_routes_under_5_percent():
    rep = bettrdata()
    rep["pairs"][("Google", "Google")]["spam"] = 5
    rep["pairs"][("Outlook", "Google")]["spam"] = 2
    head, _ = format_slack("Bettrdata", rep, ["t"], "2026-10-13")
    assert not any("\u2192" in l for l in head), "no route line"
