"""Headline in the channel, details in the thread (spec 2026-10-13) for the
infra audit, copy-sync summary and the short alerts. Pure formatters, no network."""
from datetime import date

from smartlead import infra_audit as ia

TODAY = date(2026, 10, 13)
NOTE = "Not checkable by the bot"


def _audit_findings():
    fs = [ia.finding("main_domain", "P0", "Bettrdata", "bettrdata.com", "sending from the real site", "Move the inboxes off it")]
    fs += [ia.finding("signature_missing", "P2", "Precise Leads", f"anna{i}@pipelinecal.com", "no signature",
                      "Add the client signature in Smartlead") for i in range(7)]
    fs += [ia.finding("warmup_off", "P1", "Melior", "jo@meliorhq.co", "warmup off, 20 sends/day", "Turn warmup on"),
           ia.finding("warmup_off", "P1", "Melior", "al@meliorhq.co", "warmup off, 18 sends/day", "Turn warmup on")]
    return fs


def _headline_ok(head):
    assert 1 <= len(head) <= 4
    assert not any("@" in line for line in head)


def test_infra_audit_headline_counts_and_thread_lists_every_problem():
    fs = _audit_findings()
    head, thread = ia.summary_parts(fs, {"domains": 43, "inboxes": 120}, today=TODAY)
    _headline_ok(head)
    assert head[0] == "🚨 Infra audit, 13 Oct: 10 problems (P0 1 · P1 2 · P2 7)"   # a P0: act today

    body = "\n".join(thread)
    for f in fs:
        assert f["subject"] in body
    # one reply per problem type, worst first, each with its fix; the note last, once
    assert len(thread) == 4
    assert "Sending from the main domain" in thread[0] and "Fix: Move the inboxes off it" in thread[0]
    assert "No signature" in thread[2] and thread[2].count("Fix:") == 1
    assert NOTE in thread[-1] and body.count(NOTE) == 1
    assert NOTE not in "\n".join(head)

def test_infra_audit_without_p0_is_a_warning():
    fs = [f for f in _audit_findings() if f["severity"] != "P0"]
    head, _ = ia.summary_parts(fs, {"domains": 43, "inboxes": 120}, today=TODAY)
    assert head[0].startswith("⚠️ Infra audit, 13 Oct: 9 problems")


def test_infra_audit_no_problems():
    head, thread = ia.summary_parts([], {"domains": 43, "inboxes": 120}, today=TODAY)
    assert head[0] == "✅ Infra audit, 13 Oct: no problems"
    _headline_ok(head)
    assert len(thread) == 1 and NOTE in thread[0]


def test_infra_audit_summarize_text_has_both_parts():
    fs = _audit_findings()
    txt = ia.summarize(fs, {"domains": 2, "inboxes": 2})
    assert "Infra audit" in txt and "anna6@pipelinecal.com" in txt and txt.count(NOTE) == 1


def test_copy_sync_summary_parts():
    from copy_sync import _summary_parts
    head, thread = _summary_parts(4, [])
    assert head == ["✅ Message sequences refreshed"] and thread == []
    probs = ["• DT PL #3: no source campaign", "• 4085678: Smartlead 500"]
    head, thread = _summary_parts(4, probs)
    assert head == ["⚠️ Message sequences refreshed: 2 problems"]
    assert all(p in "\n".join(thread) for p in probs)
    head, _ = _summary_parts(0, probs)
    assert head == ["⚠️ Message sequences not refreshed: 2 problems"]


def test_key_health_alert_advice_once_in_thread():
    from key_health_monitor import _alert_parts
    dead = [{"client": "BETTRDATA", "key_tail": "a1b2"}, {"client": "PRECISE_LEADS", "key_tail": "c3d4"}]
    unreach = [{"client": "MELIOR", "detail": "ConnectTimeout: timed out"}]
    head, thread = _alert_parts(dead, unreach)
    _headline_ok(head)
    assert head[0].startswith("🚨") and "BETTRDATA" in "\n".join(head) and "MELIOR" in "\n".join(head)
    advice = "Every job is blind"
    assert advice not in "\n".join(head) and "\n".join(thread).count(advice) == 1
    head, thread = _alert_parts([], unreach)
    assert head[0].startswith("⚠️") and "ConnectTimeout" in "\n".join(head)


def test_blacklist_alert_advice_in_thread_and_long_lists_moved():
    from blacklist_monitor import _alert_parts
    serious = [{"domain": f"d{i}.com", "clients": ["Bettrdata"], "listed_on": ["Spamhaus DBL"]} for i in range(8)]
    head, thread = _alert_parts(serious, surbl_only=3)
    _headline_ok(head)
    assert head[0].startswith("🚨") and "8 domains" in head[0]
    body = "\n".join(thread)
    assert all(f"d{i}.com" in body for i in range(8))
    assert "Pause campaigns" not in "\n".join(head) and body.count("Pause campaigns") == 1
    assert "SURBL" in "\n".join(head + thread)
    # a short list stays in the headline
    head, thread = _alert_parts(serious[:2], surbl_only=0)
    _headline_ok(head)
    assert "d0.com" in "\n".join(head) and "d1.com" in "\n".join(head)
    assert "Pause campaigns" in "\n".join(thread)


def test_reply_alert_long_list_in_thread():
    from reply_monitor import _alert_parts
    lines = [f"   • *d{i}.com* (Melior): reply rate 0.1% vs 1.2% baseline" for i in range(30)]
    head, thread = _alert_parts(lines)
    _headline_ok(head)
    assert head[0].startswith("⚠️") and "30 drops" in head[0]
    body = "\n".join(thread)
    assert all(f"d{i}.com" in body for i in range(30))
    head, thread = _alert_parts(lines[:3])
    assert len(head) == 4 and "d2.com" in head[3] and thread == []
