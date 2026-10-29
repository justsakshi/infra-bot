"""Text of the daily campaign stats post. No network."""
from smartlead.stats_post import build_post, not_ready_text, weekday_day


def _c(client, channel, name, change, totals=None, running=True, status="ACTIVE"):
    return {"client": client, "channel": channel, "platform": "x", "campaign": name,
            "status": status, "running": running, "change": change,
            "totals": totals or change}


EMAIL = dict(sent=165, replies=3, positive=2, not_interested=1, wrong_person=0, bounced=0,
             invites=None, accepted=None)
LI = dict(sent=None, bounced=None, invites=12, accepted=1, replies=0, positive=0,
          not_interested=None, wrong_person=None)


def _post(changes, base="2026-10-29", not_counted=()):
    return build_post(changes, "2026-10-30", base, "https://sheet", list(not_counted))


def test_headline_has_each_clients_totals_in_order():
    head, _ = _post([
        _c("Precise Leads", "Email", "Campaign D", dict(EMAIL, sent=20, replies=1)),
        _c("BettrData", "Email", "Campaign A", EMAIL),
        _c("BettrData", "Email", "Campaign B", dict(EMAIL, sent=35, replies=0, positive=0)),
        _c("BettrData", "LinkedIn", "LinkedIn A", LI),
    ])
    text = head
    assert head.split("\n")[0] == "📊 Campaign stats, Fri 30 Oct"
    assert "Since Thu 29 Oct, 9:00 IST." in text
    assert text.index("*BettrData*") < text.index("*Precise Leads*")
    assert ("Email: 200 sent, 3 replies (2 positive/neutral, 2 not interested, "
            "0 wrong person), 0 bounced") in text
    assert "LinkedIn: 12 invites, 1 accepted, 0 replies, 0 interested" in text
    assert "Email: 20 sent, 1 reply (2 positive/neutral, 1 not interested" in text   # singular


def test_thread_lists_each_campaign_with_change_and_running_total():
    totals = dict(EMAIL, sent=284, replies=5)
    _, thread = _post([_c("BettrData", "Email", "Campaign A", EMAIL, totals)])
    assert thread == ["*BettrData · Email*\n"
                      "• Campaign A: 165 sent, 3 replies (2 positive/neutral, 1 not interested,"
                      " 0 wrong person), 0 bounced"
                      " · to date 284 sent, 5 replies (2 positive/neutral, 1 not interested,"
                      " 2 other)"]


def test_quiet_paused_campaign_is_left_out_but_a_moving_one_stays():
    zero = {k: (0 if v is not None else None) for k, v in EMAIL.items()}
    _, thread = _post([_c("Melior", "Email", "Old", zero, EMAIL, running=False),
                       _c("Melior", "Email", "Finishing", EMAIL, running=False)])
    assert "Old" not in thread[0] and "Finishing" in thread[0]


def test_unknown_change_says_so_instead_of_a_number():
    head, thread = _post([_c("Melior", "Email", "Resumed", None, EMAIL)])
    assert "no earlier numbers to compare" in head
    assert "not in the 29 Oct numbers" in thread[0]


def test_first_day_shows_totals_to_date():
    head, thread = _post([_c("BettrData", "Email", "Campaign A", None, EMAIL)], base=None)
    text = head
    assert "Totals to date." in text and "Email: 165 sent" in text
    assert thread == ["*BettrData · Email*\n• Campaign A: 165 sent, 3 replies "
                      "(2 positive/neutral, 1 not interested)"]


def test_not_counted_line_and_sheet_link_close_the_post():
    head, _ = _post([_c("BettrData", "Email", "Campaign A", EMAIL)],
                    not_counted=["Precise Leads LinkedIn (Waalaxy has no API)"])
    assert head.endswith("Not counted: Precise Leads LinkedIn (Waalaxy has no API).\n\n"
                         "<https://sheet|Campaign History sheet>")


def test_spacing_survives_the_real_posting_path(monkeypatch):
    # Checks the text Slack receives, not build_post's output: post_with_thread
    # drops empty headline items, which once took out every blank line.
    from smartlead import notify
    sent = []
    monkeypatch.setattr(notify, "_post", lambda tok, ch, text, thread_ts=None:
                        sent.append(notify.tidy(text, headline=not thread_ts)) or "1.0")
    monkeypatch.setattr(notify, "_REPLY_PAUSE_S", 0)
    head, thread = _post([_c("BettrData", "Email", "Campaign A", EMAIL),
                          _c("BettrData", "LinkedIn", "LinkedIn A", LI),
                          _c("Melior", "Email", "Campaign C", EMAIL)])
    notify.post_with_thread("t", "C1", [head], thread)
    parent = sent[0]
    assert parent.startswith("📊 *Campaign stats, Fri 30 Oct*\n\nSince Thu 29 Oct")
    assert "\n\n\n" not in parent
    assert "\n\n*BettrData*\nEmail:" in parent and "\n\n*Melior*\nEmail:" in parent
    assert "\n\n<https://sheet|" in parent
    assert "\n\n*BettrData · LinkedIn*\n" in sent[1]       # thread keeps its blank line


def test_not_ready_notice():
    text = not_ready_text("2026-10-30", "2026-10-29")
    assert text.startswith("⚠️ Campaign stats, Fri 30 Oct: not ready\n\n")
    assert "latest numbers are from Thu 29 Oct" in text
    assert weekday_day("2026-11-02") == "Mon 2 Nov"


def test_no_replies_means_no_breakdown_and_wrong_person_is_counted():
    _, thread = _post([_c("Melior", "Email", "Quiet", dict(EMAIL, replies=0, positive=0,
                                                          not_interested=0)),
                       _c("Melior", "Email", "Mixed", dict(EMAIL, replies=4, positive=1,
                                                          not_interested=1, wrong_person=2))])
    assert ("• Mixed: 165 sent, 4 replies (1 positive/neutral, 1 not interested, "
            "2 wrong person)") in thread[0]
    assert "• Quiet: 165 sent, 0 replies, 0 bounced" in thread[0]
