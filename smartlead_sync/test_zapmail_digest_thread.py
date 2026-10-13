"""Domains & inboxes daily + Inboxes ready: headline in the channel, details
in the thread (spec 2026-10-13). Pure, no network.

    .venv\\Scripts\\python.exe -m pytest test_zapmail_digest_thread.py -q
"""
from __future__ import annotations

from datetime import date

from zapmail_digest import format_digest

TODAY = date(2026, 10, 13)
INBOXKIT = "Inboxkit is in the Daily Renewal Check"
RETIRE = "To stop paying for some inboxes"

STATUS = {
    "PRECISE_LEADS": {"ok": True, "wallet_balance": 64, "auto_recharge": True,
                      "active_mailboxes": {"GOOGLE": 54, "MICROSOFT": 15}, "placement_credits": 31},
    "Belardi Wong": {"ok": True, "wallet_balance": 12, "auto_recharge": False,
                     "assigned_mailboxes": 48},
}
PAST = [f"pastclient{i}.com" for i in range(34)]
RENEWALS = ([{"domain": "gomelior.com", "expire_on": "2026-10-20", "account": "PRECISE_LEADS",
              "client": "Melior", "assigned_mailboxes": 3}]
            + [{"domain": d, "expire_on": "2026-10-18", "account": "PRECISE_LEADS", "client": None}
               for d in PAST])
BATCHES = [{"batch_id": "due1", "status": "planned", "earliest_date": "2026-10-13",
            "domains": ["a-leads.com", "b-leads.com"], "client": "Precise Leads", "estimated_usd": 25.98},
           {"batch_id": "stuck", "status": "unknown", "domains": ["c.com"]}]
ZAP_BILLING = [
    {"bills_on": "2026-10-14", "mailboxes": 15, "provider": "MICROSOFT", "kind": "inboxes",
     "price": 48.75, "clients": ["Bettrdata"]},
    {"bills_on": "2026-10-15", "mailboxes": 21, "provider": "GOOGLE", "kind": "inboxes",
     "price": 68.25, "clients": ["Belardi Wong"]},
]
SCALEDMAIL = {
    "lines": [
        "Setting up, business day 1 of 2-3, order o9: 2 domain(s): x1.com, x2.com",
        "Late, business day 4 (support says 2-3), order o7: y1.com. Ask support.",
        "✅ Ready: 20 SMTP inboxes (5 domains, plan p1) - signature/client on 20, tags PL + SMTP.",
        "Orders billing in 3 days: 10×Google $35.00 on 2026-10-15 (Precise Leads)",
        "No client on 2 domain(s) — set the order tag: z1.com, z2.com",
    ],
    "billing": [{"amount": 35.0, "billing_day": "2026-10-15", "clients": ["Precise Leads"],
                 "description": "10*Google"}],
    "setting_up": 1, "late": 1, "live": 0, "ready": 1, "unassigned": ["z1.com", "z2.com"],
    "monthly_total": 662.75,
}


def _busy():
    return format_digest(STATUS, RENEWALS, BATCHES, today=TODAY, ledger_ok=True,
                         zap_billing=ZAP_BILLING, scaledmail=SCALEDMAIL)


def test_headline_is_short_and_flags_action():
    head, thread, actions = _busy()
    assert 1 <= len(head) <= 4
    assert head[0] == "⚠️ Domains & inboxes, 13 Oct"
    text = "\n".join(head)
    # the money that bills soon: Zapmail 48.75 + 68.25 + ScaledMail 35.00, 3 bills
    assert "$152.00" in text and "3 bills" in text
    assert "late" in text.lower()
    # no lists in the parent
    for d in PAST + ["gomelior.com", "a-leads.com", "x1.com", "z1.com", "y1.com"]:
        assert d not in text
    assert len(text) < 400
    assert actions > 0


def test_every_item_is_in_the_thread():
    _, thread, _ = _busy()
    body = "\n".join(thread)
    for item in (["gomelior.com", "a-leads.com", "b-leads.com", "stuck", "x1.com", "y1.com", "z1.com",
                  "z2.com", "Belardi Wong", "PRECISE_LEADS", "54 Google + 15 Outlook", "$48.75",
                  "$68.25", "20 SMTP inboxes", "$35.00"] + PAST):
        assert item in body, item
    assert "*Zapmail*" in body and "*ScaledMail*" in body
    assert body.index("*Zapmail*") < body.index("*ScaledMail*")


def test_past_client_domains_are_one_line_with_the_full_list():
    _, thread, _ = _busy()
    past_lines = [l for p in thread for l in p.split("\n") if "pastclient0.com" in l]
    assert len(past_lines) == 1
    assert all(d in past_lines[0] for d in PAST) and "34" in past_lines[0]


def test_boilerplate_once_in_the_last_reply():
    _, thread, _ = _busy()
    body = "\n".join(thread)
    assert body.count(INBOXKIT) == 1 and INBOXKIT in thread[-1]
    assert body.count(RETIRE) == 1 and RETIRE in thread[-1]


def test_quiet_day_is_a_green_headline():
    head, thread, actions = format_digest(
        {"PRECISE_LEADS": {**STATUS["PRECISE_LEADS"]}}, [], [], today=TODAY, ledger_ok=True)
    assert head[0] == "✅ Domains & inboxes, 13 Oct" and actions == 0
    assert "Nothing needs action" in "\n".join(head)
    assert RETIRE not in "\n".join(thread)
    assert "\n".join(thread).count(INBOXKIT) == 1


def test_billing_alone_is_not_a_warning():
    head, _, _ = format_digest({"PRECISE_LEADS": {**STATUS["PRECISE_LEADS"]}}, [], [], today=TODAY,
                               ledger_ok=True, zap_billing=ZAP_BILLING[:1])
    assert head[0].startswith("✅") and "$48.75" in "\n".join(head) and "1 bill" in "\n".join(head)


def test_failed_payment_and_read_errors_are_warnings():
    rows = [{**ZAP_BILLING[0], "payment_failure": "card declined"}]
    head, thread, _ = format_digest({"PRECISE_LEADS": {**STATUS["PRECISE_LEADS"]}}, [], [], today=TODAY,
                                    ledger_ok=True, zap_billing=rows,
                                    errors=["ScaledMail could not be read: 500"])
    assert head[0].startswith("⚠️")
    assert "payment failed" in "\n".join(head).lower()
    assert "card declined" in "\n".join(thread) and "ScaledMail could not be read" in "\n".join(thread)


def test_scaledmail_placement_flags_and_stuck_setup_are_warnings():
    """Review 2026-10-13: the headline said ✅ while the thread listed flagged domains."""
    sm = {"lines": ["Placement week 2026-10-05: x.com 40%", "Still being set up: y.com (New Order)"],
          "billing": [], "unassigned": [], "setting_up": 0, "late": 0, "live": 0, "ready": 0,
          "flagged": 1, "stuck": 1}
    head, _, _ = format_digest({"PRECISE_LEADS": {**STATUS["PRECISE_LEADS"]}}, [], [], today=TODAY,
                               ledger_ok=True, scaledmail=sm)
    h = "\n".join(head)
    assert head[0].startswith("⚠️") and "placement" in h and "stuck" in h


# ── Inboxes ready in Smartlead ───────────────────────────────────────────────

READY = ["✅ Ready: 20 SMTP inboxes (5 domains, plan p1) - signature/client on 20, tags PL + SMTP.",
         "⏳ 3 of 10 Google inboxes in Smartlead for 10 Google inboxes (2 domains, plan p2) - waiting for the rest."]


def test_ready_post_headline_sums_ready_orders():
    from scaledmail_cli import ready_message
    plans = {"p1": {"client": "Precise Leads", "domains": ["r1.com", "r2.com"]}}
    head, thread = ready_message(READY, plans)
    assert head[0] == "✅ 20 SMTP inboxes ready in Smartlead (Precise Leads)"
    assert len(head) <= 4 and "r1.com" not in "\n".join(head)
    body = "\n".join(thread)
    assert "plan p1" in body and "r1.com" in body and "r2.com" in body and "plan p2" in body


def test_ready_post_mixed_orders():
    from scaledmail_cli import ready_message
    lines = [READY[0], "✅ Ready: 10 Google inboxes (2 domains, plan p3) - signature/client on 10, tags ML."]
    head, thread = ready_message(lines, {"p1": {"client": "Precise Leads"}, "p3": {"client": "Melior"}})
    assert head[0] == "✅ 30 inboxes ready in Smartlead (Precise Leads, Melior)"
    assert len([l for l in "\n".join(thread).split("\n") if "plan p" in l]) == 2


def test_post_lines_posts_headline_and_thread(monkeypatch):
    import scaledmail_cli
    from smartlead import notify
    sent = []
    monkeypatch.setenv("SCALEDMAIL_NOTIFY_CHANNEL", "C1")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "t")
    monkeypatch.setattr(notify, "_post", lambda tok, ch, text, thread_ts=None:
                        sent.append((text, thread_ts)) or "1.1")
    monkeypatch.setattr(scaledmail_cli, "_plans_by_id", lambda: {"p1": {"client": "Precise Leads"}})
    assert scaledmail_cli.post_lines("*Inboxes ready in Smartlead*", READY[:1]) is True
    assert sent[0] == ("✅ 20 SMTP inboxes ready in Smartlead (Precise Leads)", None)
    assert sent[1][1] == "1.1" and "plan p1" in sent[1][0]


def test_ready_headline_says_the_inbox_kind_in_plain_words():
    import scaledmail_cli as cli
    head, _ = cli.ready_message(["✅ Ready: 20 ScaledMail-SMTP inboxes (5 domains, plan p1) - signature/client on 20.",
                                 "✅ Ready: 50 ScaledMail-Microsoft inboxes (2 domains, plan p2) - signature/client on 50."])
    h = "\n".join(head)
    assert "ScaledMail-" not in h and "70" in h
    head, _ = cli.ready_message(["✅ Ready: 20 ScaledMail-SMTP inboxes (5 domains, plan p1) - x."])
    assert "20 SMTP inboxes" in head[0]


def test_the_digest_never_applies_readiness_itself(monkeypatch):
    """The 2-hourly job applies it; the 9:40 digest only shows what would happen."""
    import scaledmail_cli as cli
    calls = []
    monkeypatch.setattr(cli, "ready_lines", lambda apply: calls.append(apply) or {"lines": ["x"], "ready": []})
    assert cli.digest_ready_lines(enabled=True) == [] and calls == []
    assert cli.digest_ready_lines(enabled=False) == ["x"] and calls == [False]
