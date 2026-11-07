"""The reply check judges a sending domain over 14 pooled days against the other domains in its campaigns
(audit 7 Nov 2026, critic fix 12). Pure functions, fakes only."""
from datetime import date, timedelta

import pytest

from smartlead import reply_stats as rs
from smartlead.reply_stats import aggregate_domain_stats

TODAY = date(2026, 11, 9)


def _d(days_ago: int) -> str:
    return (TODAY - timedelta(days=days_ago)).isoformat()


def snap(dom, days_ago, campaigns=None, sent=None, replies=None, client="Precise Leads"):
    """A stored daily snapshot. campaigns = {id: (sent, replies)} lifetime totals; without it, a pre-8-Nov
    domain-total snapshot."""
    doc = {"domain": dom, "date": _d(days_ago), "client": client}
    if campaigns is not None:
        doc["campaigns"] = {cid: {"sent": s, "replies": r, "client": client} for cid, (s, r) in campaigns.items()}
        doc["sent"] = sum(s for s, _ in campaigns.values())
        doc["replies"] = sum(r for _, r in campaigns.values())
    else:
        doc["sent"], doc["replies"] = sent, replies
    return doc


def steady(dom, cid, per_day, replies_every, days=15, start_sent=1000, client="Precise Leads"):
    """Daily snapshots of one domain in one campaign: per_day sends a day, one reply every `replies_every` sends."""
    out = []
    for i in range(days, -1, -1):
        sent = start_sent + per_day * (days - i)
        out.append(snap(dom, i, {cid: (sent, sent // replies_every)}, client=client))
    return out


# ── counting ─────────────────────────────────────────────────────────────────

def test_aggregate_sums_per_domain_and_keeps_bounces():
    rows = [
        {"email": "a@dom1.com", "sent_count": 100, "reply_count": 5, "bounce_count": 2, "sender_bounce_count": 1},
        {"email": "b@dom1.com", "sent_count": 50, "reply_count": 1, "bounce_count": 1},
        {"from_email": "c@dom2.com", "sent_count": "30", "reply_count": 2, "sender_bounce_count": None},
    ]
    out = aggregate_domain_stats(rows)
    assert out["dom1.com"] == {"sent": 150, "replies": 6, "bounce_count": 3, "sender_bounce_count": 1}
    assert out["dom2.com"] == {"sent": 30, "replies": 2, "bounce_count": 0, "sender_bounce_count": 0}


def test_merge_campaign_takes_the_client_from_the_campaign_not_the_account():
    fleet = {}
    rs.merge_campaign(fleet, 11, "Melior", "PRECISE_LEADS", {"m.com": {"sent": 40, "replies": 1, "bounce_count": 2}})
    rs.merge_campaign(fleet, 12, "Precise Leads", "PRECISE_LEADS", {"m.com": {"sent": 10, "replies": 0},
                                                                    "p.com": {"sent": 5, "replies": 1}})
    m = fleet["m.com"]
    assert m["client"] == "Melior" and m["account"] == "PRECISE_LEADS"       # most sends were Melior's
    assert (m["sent"], m["replies"], m["bounce_count"], m["sender_bounce_count"]) == (50, 1, 2, 0)
    assert m["campaigns"]["11"] == {"sent": 40, "replies": 1, "bounce_count": 2, "sender_bounce_count": 0,
                                    "client": "Melior"}
    assert fleet["p.com"]["client"] == "Precise Leads"


# ── the 14-day window ────────────────────────────────────────────────────────

def test_window_counts_only_sends_made_inside_the_window():
    docs = steady("a.com", "1", per_day=20, replies_every=50, days=20)
    w = rs.window_stats(docs, TODAY)["a.com"]
    assert w["sent"] == 14 * 20 and w["by_campaign"] == {"1": [280, w["replies"]]}
    assert w["unattributed"] == [0, 0]


def test_a_campaign_that_pauses_and_resumes_is_no_fake_day_of_sends():
    # REPLIES-6: campaign 2 (lifetime 5,000 sends) pauses for 4 days, then comes back having sent 30 more
    docs = []
    for i in range(10, -1, -1):
        camps = {"1": (1000 + 10 * (10 - i), 20)}
        if not 3 <= i <= 6:
            camps["2"] = (5000 + (30 if i < 3 else 0), 100)
        docs.append(snap("a.com", i, camps))
    w = rs.window_stats(docs, TODAY)["a.com"]
    assert w["by_campaign"]["2"] == [30, 0]
    assert w["sent"] == 100 + 30


def test_a_campaign_seen_for_the_first_time_adds_nothing():
    docs = [snap("a.com", 2, {"1": (100, 1)}), snap("a.com", 1, {"1": (110, 1), "9": (4000, 80)})]
    assert rs.window_stats(docs, TODAY)["a.com"]["sent"] == 10


def test_totals_that_go_down_restart_the_count():
    docs = [snap("a.com", 3, {"1": (500, 5)}), snap("a.com", 2, {"1": (100, 1)}), snap("a.com", 1, {"1": (120, 2)})]
    w = rs.window_stats(docs, TODAY)["a.com"]
    assert w["sent"] == 20 and w["replies"] == 1


def test_old_domain_total_snapshots_drop_campaign_status_jumps():
    docs = [snap("a.com", 5, sent=1000, replies=10), snap("a.com", 4, sent=1030, replies=11),
            snap("a.com", 3, sent=600, replies=5),        # a campaign left ACTIVE: fall, dropped
            snap("a.com", 2, sent=1100, replies=12),      # it came back: +500 in a day, dropped
            snap("a.com", 1, sent=1125, replies=12)]
    w = rs.window_stats(docs, TODAY)["a.com"]
    assert w["unattributed"] == [30 + 25, 1] and w["by_campaign"] == {}


def test_the_switch_from_domain_totals_to_campaign_snapshots():
    docs = [snap("a.com", 3, sent=1000, replies=10), snap("a.com", 2, {"1": (1020, 11)}),
            snap("a.com", 1, {"1": (1050, 12)})]
    w = rs.window_stats(docs, TODAY)["a.com"]
    assert w["unattributed"] == [20, 1] and w["by_campaign"] == {"1": [30, 1]}


# ── judging ──────────────────────────────────────────────────────────────────

def _fleet_docs(bad_replies_every=304, others=3, client="Precise Leads"):
    """bad.com and `others` healthy domains share campaign 1; the healthy ones reply at 1 in 44 (2.3%)."""
    docs = steady("bad.com", "1", per_day=22, replies_every=bad_replies_every, start_sent=0, client=client)
    for k in range(others):
        docs += steady(f"ok{k}.com", "1", per_day=25, replies_every=44, client=client)
    return docs


def test_the_audit_case_one_reply_on_300_sends_is_an_alert():
    window = rs.window_stats(_fleet_docs(), TODAY)
    v = rs.judge(window, ["bad.com", "ok0.com"])
    bad = v["bad.com"]
    assert bad["judged_sent"] == 308 and bad["observed"] == 1 and bad["reference"] == "campaigns"
    assert 6.5 < bad["expected"] < 8 and bad["p"] <= rs.ALERT_P and bad["verdict"] == rs.ALERT
    assert v["ok0.com"]["verdict"] == rs.OK


def test_an_ordinary_quiet_day_is_not_an_alert():
    # the old rule's noise: 0 replies on a 28-send day. Pooled over 14 days at the normal rate it is fine.
    docs = _fleet_docs(bad_replies_every=44)
    last = docs[15]
    docs[15] = {**last, "campaigns": {"1": {**last["campaigns"]["1"], "replies": docs[14]["campaigns"]["1"]["replies"]}}}
    v = rs.judge(rs.window_stats(docs, TODAY), ["bad.com"])["bad.com"]
    assert v["verdict"] == rs.OK


def test_a_domain_under_50_sends_is_not_judged():
    docs = _fleet_docs()
    docs = [d for d in docs if d["domain"] != "bad.com"] + steady("bad.com", "1", per_day=3, replies_every=10 ** 6,
                                                                start_sent=0)
    v = rs.judge(rs.window_stats(docs, TODAY), ["bad.com"])["bad.com"]
    assert v["sent"] == 42 and v["verdict"] == rs.TOO_FEW


def test_watch_needs_the_drop_to_be_unlikely_by_chance():
    # 60 sends with 0 replies against 2.3%: 1.4 expected, 0.0x but a 1-in-4 chance -> not a watch
    docs = [d for d in _fleet_docs() if d["domain"] != "bad.com"]
    docs += [snap("thin.com", 15, {"1": (0, 0)}), snap("thin.com", 0, {"1": (60, 0)})]
    assert rs.judge(rs.window_stats(docs, TODAY), ["thin.com"])["thin.com"]["verdict"] == rs.OK
    # 8 replies on 616 sends against 14 expected: under 0.7x and under 1 in 10 -> watch, not alert
    docs = _fleet_docs(bad_replies_every=77)
    docs = [d for d in docs if d["domain"] != "bad.com"] + steady("bad.com", "1", per_day=44, replies_every=77,
                                                                start_sent=0)
    v = rs.judge(rs.window_stats(docs, TODAY), ["bad.com"])["bad.com"]
    assert v["verdict"] == rs.WATCH and v["ratio"] < rs.WATCH_RATIO and rs.ALERT_P < v["p"] <= rs.WATCH_P


def test_melior_is_judged_against_melior_not_against_precise_leads():
    # PL and Melior share one Smartlead account. Made-up rates: Melior 1.2%, PL 4.5%. On domain-total snapshots
    # (no campaign split) a Melior domain at Melior's rate is fine; pooled with PL it would look like an alert.
    docs = []
    for k in range(3):
        docs += [snap(f"pl{k}.com", i, sent=1000 + 30 * (15 - i), replies=10 + (30 * (15 - i)) // 22,
                      client="Precise Leads") for i in range(15, -1, -1)]
    for k in range(3):
        docs += [snap(f"mel{k}.com", i, sent=1000 + 40 * (15 - i), replies=10 + (40 * (15 - i)) // 83,
                      client="Melior") for i in range(15, -1, -1)]
    window = rs.window_stats(docs, TODAY)
    v = rs.judge(window, ["mel0.com"])["mel0.com"]
    assert v["client"] == "Melior" and v["reference"] == "client" and v["verdict"] == rs.OK
    pooled = {d: {**w, "client": "PRECISE_LEADS"} for d, w in window.items()}
    assert rs.judge(pooled, ["mel0.com"])["mel0.com"]["verdict"] == rs.ALERT


def test_a_thin_campaign_falls_back_to_the_clients_other_domains():
    # campaign 7 has no other domain, so its sends are judged at the client's rate on its other domains
    docs = [d for d in _fleet_docs() if d["domain"] != "bad.com"]
    docs += steady("solo.com", "7", per_day=22, replies_every=304, start_sent=0)
    v = rs.judge(rs.window_stats(docs, TODAY), ["solo.com"])["solo.com"]
    assert v["reference"] == "client" and v["verdict"] == rs.ALERT
    # and with no other domain for the client at all there is nothing to compare with
    alone = steady("solo.com", "7", per_day=22, replies_every=304, start_sent=0, client="Bettrdata")
    assert rs.judge(rs.window_stats(alone, TODAY), ["solo.com"])["solo.com"]["verdict"] == rs.NO_REFERENCE


@pytest.mark.parametrize("prior,expected", [({}, ["bad.com"]), ({"bad.com": rs.WATCH}, ["bad.com"]),
                                            ({"bad.com": rs.ALERT}, [])])
def test_an_alert_is_new_only_on_its_first_day(prior, expected):
    verdicts = {"bad.com": {"verdict": rs.ALERT}, "ok.com": {"verdict": rs.OK}}
    assert rs.new_alerts(verdicts, prior) == expected
