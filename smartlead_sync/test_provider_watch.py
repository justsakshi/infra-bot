"""Microsoft / Proofpoint reach on real sends. No network."""
from datetime import date

from smartlead.provider_watch import (ALERT, OK, TOO_FEW, WATCH, client_lines, judge, lead_events,
                                      poisson_le, provider_from_mx, report_text)

TODAY = date(2026, 11, 1)
PROVIDER = {"g.com": "google", "m.com": "microsoft", "p.com": "proofpoint", "o.com": "other"}


def _of(domain):
    return PROVIDER[domain]


def _leads(domain, n, events, start=0):
    return {f"u{start + i}@{domain}": {"event": i < events} for i in range(n)}


def _campaign(g=(100, 10), m=(50, 5), p=(0, 0)):
    out = {}
    out.update(_leads("g.com", *g))
    out.update(_leads("m.com", *m))
    out.update(_leads("p.com", *p))
    return out


def test_provider_from_mx():
    assert provider_from_mx(["aspmx.l.google.com."]) == "google"
    assert provider_from_mx(["acme-com.mail.protection.outlook.com"]) == "microsoft"
    assert provider_from_mx(["mx0a-0012.pphosted.com", "acme-com.mail.protection.outlook.com"]) == "proofpoint"
    assert provider_from_mx(["mx1.mimecast.com"]) == "other"
    assert provider_from_mx([]) == "no_mx"
    assert provider_from_mx(["notgoogle.com.evil.net"]) == "other"


def test_poisson_matches_the_alert_rule():
    assert abs(poisson_le(3, 10.0) - 0.0103) < 0.0005   # 10 expected, 3 seen: the rule's edge
    assert poisson_le(0, 0) == 1.0


def test_lead_events_window_reply_on_earlier_step_and_bounces():
    rows = [
        # step 1 before the window carries the reply; step 2 inside the window brings the lead in
        {"lead_email": "A@g.com", "sent_time": "2026-09-01T10:00:00Z", "reply_time": "2026-10-20T10:00:00Z",
         "lead_category": "Interested"},
        {"lead_email": "a@g.com", "sent_time": "2026-10-15T10:00:00Z"},
        {"lead_email": "old@g.com", "sent_time": "2026-09-01T10:00:00Z", "reply_time": "2026-09-02T10:00:00Z"},
        {"lead_email": "ooo@m.com", "sent_time": "2026-10-30T10:00:00Z", "reply_time": "2026-10-30T11:00:00Z",
         "lead_category": "Out Of Office"},
        {"lead_email": "wrong@m.com", "sent_time": "2026-10-30T10:00:00Z", "reply_time": "2026-10-30T11:00:00Z",
         "lead_category": "Wrong Person"},
        {"lead_email": "b@m.com", "sent_time": "2026-10-30T10:00:00Z", "is_bounced": True},
        {"lead_email": "future@g.com", "sent_time": "2026-11-05T10:00:00Z"},
        {"lead_email": "", "sent_time": "2026-10-30T10:00:00Z"},
    ]
    got = lead_events(rows, TODAY)
    assert got == {"a@g.com": {"event": True}, "ooo@m.com": {"event": True}, "wrong@m.com": {"event": False}}


def test_junked_microsoft_alerts_and_healthy_proofpoint_is_ok():
    # BettrData-like: Microsoft at ~0.1x of the same campaigns' Google rate
    camps = {str(i): _campaign(g=(300, 9), m=(100, 0 if i else 1), p=(40, 1)) for i in range(4)}
    r = judge(camps, _of)
    assert r["microsoft"]["expected"] == 12.0 and r["microsoft"]["observed"] == 1
    assert r["microsoft"]["verdict"] == ALERT
    assert r["proofpoint"]["verdict"] == TOO_FEW          # 4.8 expected: too few to act
    assert r["google"] == {"sent": 1200, "events": 36}


def test_ok_watch_and_skipped_campaigns():
    same = judge({"1": _campaign(g=(400, 40), m=(200, 19))}, _of)
    assert same["microsoft"]["verdict"] == OK
    low = judge({"1": _campaign(g=(400, 40), m=(200, 12))}, _of)   # 0.6x, p ~0.04: not proof yet
    assert low["microsoft"]["verdict"] == WATCH
    thin = judge({"1": _campaign(g=(29, 10), m=(200, 0))}, _of)    # under 30 Google sends: no control
    assert thin["microsoft"]["campaigns"] == 0 and thin["microsoft"]["verdict"] == TOO_FEW


def test_text_says_report_only_and_names_the_alert():
    camps = {str(i): _campaign(g=(300, 9), m=(100, 0)) for i in range(4)}
    lines = client_lines("BettrData", judge(camps, _of))
    assert lines[0] == "*BettrData*: Google recipients 1200 sent, 36 replied or out-of-office (3.0%)"
    assert lines[1].startswith("• Microsoft: ALERT. 0 replied or out-of-office vs 12.0 expected")
    assert lines[2] == "• Proofpoint: no campaign with enough sends to compare"
    text = report_text([lines, client_lines("Precise Leads", None, "HTTP 401 Plan expired!")], TODAY)
    assert "*Precise Leads*: not read (HTTP 401 Plan expired!)" in text
    assert "most likely going to junk" in text and text.endswith("nothing was paused or changed.")
    quiet = report_text([client_lines("Melior", judge({}, _of))], TODAY)
    assert "junk" not in quiet
