"""Text of the daily campaign stats post. No network."""
import json

from smartlead.stats_post import build_post, not_ready_text, part_as_text, weekday_day


def _c(client, channel, name, change, totals=None, running=True, status="ACTIVE",
       cid="11", platform="Smartlead", open_tracking=""):
    return {"client": client, "channel": channel, "platform": platform, "campaign": name,
            "status": status, "running": running, "change": change,
            "totals": totals or change, "campaign_id": cid, "open_tracking": open_tracking}


def _texts(part):
    return [b["text"]["text"] for b in part["blocks"] if b["type"] in ("section", "header")]


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
    assert len(thread) == 1
    assert _texts(thread[0]) == [
        "BettrData · Email",
        "*Campaign A*\n165 sent · 3 replies (2 positive/neutral, 1 not interested)"
        "\n_To date: 284 sent · 5 replies (2 positive/neutral, 1 not interested, 2 other)_"]


def test_a_campaign_with_new_replies_gets_a_button_carrying_the_window():
    _, thread = _post([_c("BettrData", "Email", "Campaign A", EMAIL, cid="4251027")])
    btn = thread[0]["blocks"][1]["accessory"]
    assert btn["action_id"] == "show_replies" and btn["text"]["text"] == "Show 3 replies"
    v = json.loads(btn["value"])
    assert v["id"] == "4251027" and v["platform"] == "Smartlead" and v["n"] == 3
    assert v["from"] == "2026-10-29T03:30:00+00:00" and v["to"] == "2026-10-30T03:30:00+00:00"
    assert len(btn["value"]) < 2000


def test_no_button_without_new_replies():
    _, thread = _post([_c("Melior", "Email", "Quiet", dict(EMAIL, replies=0, positive=0,
                                                          not_interested=0))])
    assert "accessory" not in thread[0]["blocks"][1]


def test_long_names_are_shortened_and_quiet_running_campaigns_share_one_line():
    zero = {k: (0 if v is not None else None) for k, v in EMAIL.items()}
    _, thread = _post([
        _c("BettrData", "Email", "Roundtable | AI in the Data Business | Laurie", EMAIL),
        _c("BettrData", "Email", "Roundtable | AI in the Data Business | Aaron", zero, EMAIL),
        _c("BettrData", "Email", "Marketplace | Hard to Find audiences", zero, EMAIL)])
    text = part_as_text(thread[0])
    assert "*Roundtable · Laurie*" in text
    assert "No change: Marketplace · Hard to Find audiences, Roundtable · Aaron" in text
    assert "0 sent" not in text


def test_open_rate_only_where_opens_are_tracked():
    on = dict(EMAIL, reached=132, opened=106)
    head, thread = _post([
        _c("BettrData", "Email", "Tracked", EMAIL, on, open_tracking="on"),
        _c("BettrData", "Email", "Untracked", EMAIL, dict(EMAIL, reached=500, opened=0),
           open_tracking="off")])
    text = part_as_text(thread[0])
    assert "80% opened" in text and text.count("opened") == 1
    assert "· 80% opened to date (1 of 2 campaigns track opens)" in head


def test_linkedin_shows_the_accept_rate():
    _, thread = _post([_c("BettrData", "LinkedIn", "LinkedIn A", LI,
                          dict(LI, invites=25, accepted=1), platform="Expandi")])
    assert "_To date: 25 invites · 1 accepted · 4% accepted_" in part_as_text(thread[0])


def test_quiet_paused_campaign_is_left_out_but_a_moving_one_stays():
    zero = {k: (0 if v is not None else None) for k, v in EMAIL.items()}
    _, thread = _post([_c("Melior", "Email", "Old", zero, EMAIL, running=False),
                       _c("Melior", "Email", "Finishing", EMAIL, running=False)])
    text = part_as_text(thread[0])
    assert "Old" not in text and "Finishing" in text


def test_unknown_change_says_so_instead_of_a_number():
    head, thread = _post([_c("Melior", "Email", "Resumed", None, EMAIL)])
    assert "no earlier numbers to compare" in head
    assert "Not in the 29 Oct numbers" in part_as_text(thread[0])


def test_a_stopped_campaign_with_nothing_to_compare_is_left_out():
    _, thread = _post([_c("Melior", "LinkedIn", "Finished long ago", None, LI, running=False,
                          status="FINISHED", platform="Heyreach"),
                       _c("Melior", "LinkedIn", "Running", LI, platform="Heyreach")])
    text = part_as_text(thread[0])
    assert "Finished long ago" not in text and "Running" in text


def test_first_day_shows_totals_to_date():
    head, thread = _post([_c("BettrData", "Email", "Campaign A", None, EMAIL)], base=None)
    assert "Totals to date." in head and "Email: 165 sent" in head
    assert _texts(thread[0])[1] == "*Campaign A*\n165 sent · 3 replies (2 positive/neutral, 1 not interested)"
    assert "accessory" not in thread[0]["blocks"][1]


def test_many_campaigns_stay_under_slacks_block_cap():
    many = [_c("BettrData", "Email", f"Campaign {i:02}", EMAIL) for i in range(60)]
    _, thread = _post(many)
    assert len(thread) == 2 and all(len(p["blocks"]) <= 50 for p in thread)
    assert sum(len(p["blocks"]) for p in thread) == 61          # 60 + one header


def test_not_counted_line_and_sheet_link_close_the_post():
    head, _ = _post([_c("BettrData", "Email", "Campaign A", EMAIL)],
                    not_counted=["Precise Leads LinkedIn (Waalaxy has no API)"])
    assert head.endswith("Not counted: Precise Leads LinkedIn (Waalaxy has no API).\n\n"
                         "<https://sheet|Campaign History sheet>")


def test_spacing_survives_the_real_posting_path(monkeypatch):
    # Checks what Slack receives: post_with_thread drops empty headline items,
    # which once took out every blank line; thread parts go as blocks.
    from smartlead import notify
    sent, blocks = [], []
    monkeypatch.setattr(notify, "_post", lambda tok, ch, text, thread_ts=None:
                        sent.append(notify.tidy(text, headline=not thread_ts)) or "1.0")
    monkeypatch.setattr(notify, "_post_blocks", lambda tok, ch, text, b, thread_ts=None:
                        blocks.append((text, b, thread_ts)) or "2.0")
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
    assert [t for t, _, _ in blocks] == ["BettrData · Email", "BettrData · LinkedIn",
                                         "Melior · Email"]
    assert all(ts == "1.0" for _, _, ts in blocks)


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
    text = part_as_text(thread[0])
    assert "4 replies (1 positive/neutral, 1 not interested, 2 wrong person)" in text
    assert "*Quiet*\n165 sent\n" in text


def test_only_shown_numbers_make_a_campaign_count_as_moved():
    still = {k: (0 if v is not None else None) for k, v in EMAIL.items()}
    _, thread = _post([_c("BettrData", "Email", "Opens moved", dict(still, reached=5, opened=3),
                          EMAIL)])
    assert "No change: Opens moved" in part_as_text(thread[0])
