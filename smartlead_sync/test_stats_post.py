"""Text of the 10:30 stats post (smartlead/stats_post.py). No network."""
import json
from datetime import date

from smartlead.daily_stats import Period
from smartlead.notify import post_with_thread
from smartlead.stats_post import build_post, part_as_text, weekday_day

DAY = Period("day", date(2026, 10, 30), date(2026, 10, 30))
WEEK = Period("week", date(2026, 10, 30), date(2026, 11, 5))
MONTH = Period("month", date(2026, 10, 1), date(2026, 10, 31))
CLIENTS = ["BettrData", "Melior", "Precise Leads"]


def _email(**kw):
    c = dict(reached=0, sent=0, replies=0, positive=0, not_interested=0, wrong_person=0,
             out_of_office=0, bounced=0)
    c.update(kw)
    return c


def _li(**kw):
    c = dict(invites=0, accepted=0, replies=0, positive=0)
    c.update(kw)
    return c


def _r(client, channel, name, platform="Smartlead", cid="11", **counts):
    return {"client": client, "channel": channel, "platform": platform, "campaign": name,
            "campaign_id": cid, "counts": counts}


def _post(rows, periods=(DAY,), **kw):
    return build_post(rows, list(periods), CLIENTS, **kw)


def test_headline_names_the_day_and_each_clients_numbers_in_order():
    head, _ = _post([
        _r("Precise Leads", "Email", "D", day=_email(sent=20, replies=1, positive=1)),
        _r("BettrData", "Email", "A", day=_email(reached=100, sent=120, replies=3, positive=2,
                                                 not_interested=1, out_of_office=1, bounced=2)),
        _r("BettrData", "Email", "B", day=_email(sent=30)),
    ])
    lines = head.split("\n")
    # Manveen, 4 Nov 2026: "for bettrdata would you say no replies ?" then "go ahead with 5:30":
    # the day stays 05:30-05:30 IST, said in India time only.
    assert lines[0] == "📊 Campaign stats for Fri 30 Oct"
    assert ("Fri 30 Oct = 5:30 AM Fri to 5:30 AM Sat 31 Oct, India time. "
            "Anything before 5:30 AM counts for the day before.") in head
    assert "US" not in head
    assert head.index("*BettrData*") < head.index("*Melior*") < head.index("*Precise Leads*")
    assert ("Email: 100 new people reached · 150 emails sent · 3 people replied "
            "(2 positive/neutral, 1 not interested) · 1 out-of-office · 2 bounced") in lines


def test_a_quiet_client_is_still_listed():
    head, thread = _post([])
    assert head.count("Email: nothing sent, no replies") == 3
    assert [p["text"] for p in thread] == ["Client summaries"]
    assert "No replies on Fri 30 Oct: BettrData, Melior, Precise Leads." in part_as_text(thread[0])


def test_linkedin_line_for_clients_with_a_linkedin_source():
    head, _ = _post([_r("Melior", "LinkedIn", "ES", "Heyreach", day=_li(invites=25, accepted=11,
                                                                     replies=1, positive=1))],
                    linkedin_clients=("BettrData", "Melior"))
    assert "LinkedIn: 25 invites · 11 accepted · 1 person replied (1 interested)" in head
    assert "LinkedIn: no activity" in head                             # BettrData, quiet
    assert head.count("LinkedIn:") == 2                                 # not Precise Leads


def test_waalaxy_shows_only_what_it_tracks():
    head, _ = _post([_r("Precise Leads", "LinkedIn", "kochi", "Waalaxy",
                        day=_li(invites=None, accepted=None, replies=2))])
    assert "LinkedIn: 2 people replied" in head


def test_a_period_before_tracking_started_says_so():
    since = {"Precise Leads": date(2026, 10, 30)}
    before = Period("day", date(2026, 10, 29), date(2026, 10, 29))
    head, _ = build_post([], [before, Period("week", date(2026, 10, 23), date(2026, 10, 29))],
                         CLIENTS, linkedin_clients=("Precise Leads",), linkedin_from=since)
    assert "LinkedIn: not tracked before Fri 30 Oct" in head
    assert "Precise Leads LinkedIn: not tracked before Fri 30 Oct" in head
    row = _r("Precise Leads", "LinkedIn", "kochi", "Waalaxy",
             day=_li(invites=None, accepted=None, replies=2),
             month=_li(invites=None, accepted=None, replies=2))
    head, _ = build_post([row], [DAY, MONTH], CLIENTS, linkedin_from=since)
    assert "Precise Leads LinkedIn: 2 people replied (tracked from Fri 30 Oct)" in head
    assert "LinkedIn: 2 people replied (tracked from Fri 30 Oct)" in head.split("*October")[0]
    later = Period("day", date(2026, 10, 31), date(2026, 10, 31))
    head, _ = build_post([], [later], CLIENTS, linkedin_clients=("Precise Leads",), linkedin_from=since)
    assert "LinkedIn: no activity" in head and "tracked" not in head


def test_replies_to_earlier_emails_do_not_show_zero_sent():
    head, _ = _post([_r("Melior", "Email", "C", day=_email(replies=1, positive=1))])
    assert "Email: 1 person replied (1 positive/neutral)" in head


def test_other_replies_are_the_uncategorised_rest():
    head, _ = _post([_r("Melior", "Email", "C", day=_email(sent=5, replies=3, positive=1))])
    assert "3 people replied (1 positive/neutral, 2 other)" in head


def test_thread_lists_the_days_campaigns_with_a_button_for_the_day():
    _, thread = _post([
        _r("BettrData", "Email", "Roundtable | Aaron", day=_email(sent=258, replies=4, positive=1),
           week=_email(sent=900)),
        _r("BettrData", "Email", "Roundtable | Quiet", cid="12", day=_email(),
           week=_email(sent=5)),
    ], periods=(DAY, WEEK))
    part, _summary = thread
    text = part_as_text(part)
    assert "Roundtable · Aaron" in text and "Quiet" not in text
    button = part["blocks"][1]["accessory"]
    v = json.loads(button["value"])
    assert v["from"] == "2026-10-30T00:00:00+00:00" and v["to"] == "2026-10-31T00:00:00+00:00"
    assert v["n"] == 4 and button["text"]["text"] == "1 interested, 3 other"


def test_campaigns_without_replies_stay_out_of_the_thread():
    # Manveen, 3 Nov 2026: "just put campaign with replies".
    head, thread = _post([_r("BettrData", "Email", "A", day=_email(sent=10)),
                          _r("BettrData", "Email", "B", cid="12", day=_email(sent=5, replies=1))])
    part, _summary = thread
    assert "*B*" in part_as_text(part) and "*A*" not in part_as_text(part)
    assert "15 emails sent" in head                                   # still counted above


def test_button_names_the_kinds_of_reply_and_follow_up_replies():
    from smartlead.stats_post import button_label
    assert button_label(_email(replies=3, positive=2, not_interested=1)) == "2 interested, 1 not interested"
    assert button_label(_email(replies=2, wrong_person=1, later_touch=1)) == "1 wrong person, 1 other · 1 on 2nd+ touch"
    assert button_label(_li(replies=2), "LinkedIn") == "2 replies"
    assert button_label(_li(replies=3, positive=1), "LinkedIn") == "1 interested, 2 more"
    assert len(button_label(_email(replies=9999, positive=1000, not_interested=1000,
                                   wrong_person=1000, later_touch=1000))) <= 75


def test_friday_adds_the_week_and_first_working_day_adds_the_month():
    rows = [_r("BettrData", "Email", "A", day=_email(sent=10), week=_email(sent=70),
               month=_email(sent=300, replies=2, positive=2))]
    head, _ = _post(rows, periods=(DAY, WEEK, MONTH))
    assert "*This week, Fri 30 Oct – Thu 5 Nov*" in head
    assert "BettrData email: 70 emails sent · 0 people replied" in head
    assert "*October 2026*" in head
    assert "BettrData email: 300 emails sent · 2 people replied (2 positive/neutral)" in head
    assert head.index("This week") < head.index("October 2026")


def test_a_platform_that_could_not_be_read_is_named():
    head, _ = _post([], failed=["Smartlead PRECISE_LEADS"])
    assert "⚠️ Not fully read, so these numbers may be off: Smartlead PRECISE_LEADS." in head


def test_expandi_still_loading_is_said():
    row = _r("BettrData", "LinkedIn", "DP", "Expandi", day=_li(invites=3))
    row["partial"] = True
    head, _ = _post([row])
    assert "may be low (Expandi's lead history is still loading)" in head


def test_not_counted_line():
    head, _ = _post([], not_counted=["Precise Leads LinkedIn invites (no count)"])
    assert head.endswith("Not counted: Precise Leads LinkedIn invites (no count).")


def test_many_campaigns_stay_under_slacks_block_cap():
    rows = [_r("Melior", "Email", f"C{i}", cid=str(i), day=_email(sent=1, replies=1)) for i in range(60)]
    _, thread = _post(rows)
    assert len(thread) == 3 and all(len(p["blocks"]) <= 50 for p in thread)


def test_spacing_survives_the_real_posting_path(monkeypatch):
    sent = []
    from smartlead import notify
    monkeypatch.setattr(notify, "_post", lambda t, c, text, thread_ts=None: sent.append(text) or "1.0")
    monkeypatch.setattr(notify, "_post_blocks", lambda *a, **k: True)
    monkeypatch.setattr(notify.time, "sleep", lambda s: None)
    head, thread = _post([_r("BettrData", "Email", "A", day=_email(sent=1))])
    post_with_thread("x", "C1", [head], thread)
    assert "\n\n*BettrData*\n" in sent[0]


def test_weekday_day():
    assert weekday_day(date(2026, 10, 30)) == "Fri 30 Oct" == weekday_day("2026-10-30")


def test_the_thread_ends_with_a_summary_button_per_client():
    # Manveen, 4 Nov 2026: "what about bettrdata and i want summary per client".
    _, thread = _post([_r("Melior", "Email", "A", day=_email(sent=10, replies=1))])
    last = thread[-1]
    text = part_as_text(last)
    assert "No replies on Fri 30 Oct: BettrData, Precise Leads." in text
    buttons = last["blocks"][-1]["elements"]
    assert [b["text"]["text"] for b in buttons] == ["BettrData summary", "Melior summary",
                                                    "Precise Leads summary"]
    assert len({b["action_id"] for b in buttons}) == 3
    assert all(b["action_id"].startswith("show_campaign_summary_") for b in buttons)
    assert json.loads(buttons[1]["value"]) == {"day": "2026-10-30", "client": "Melior"}


def test_each_client_gets_its_own_summary_with_every_campaign_that_moved():
    from smartlead.stats_post import summary_messages
    rows = [_r("Melior", "Email", "Quiet", day=_email()),
            _r("BettrData", "Email", "Sent only", cid="12", day=_email(sent=225, reached=118)),
            _r("BettrData", "LinkedIn", "DP", "Expandi", day=_li(invites=15, accepted=4))]
    msgs = summary_messages(rows, DAY, CLIENTS)
    assert [m["client"] for m in msgs] == ["BettrData", "Melior", "Precise Leads"]
    bettr = part_as_text(msgs[0])
    assert bettr.startswith("*BettrData, Fri 30 Oct*")
    assert "*Sent only*\n118 new people reached · 225 emails sent\n" in bettr + "\n"   # no "0 replied"
    assert "15 invites · 4 accepted" in bettr and bettr.index("Email") < bettr.index("LinkedIn")
    assert "Quiet" not in part_as_text(msgs[1])
    assert "No campaign sent or got a reply this day." in part_as_text(msgs[1])


def test_thread_shows_the_campaign_name_and_the_button_only():
    _, thread = _post([_r("BettrData", "Email", "A", day=_email(sent=225, replies=1, positive=1))])
    block = thread[0]["blocks"][1]
    assert block["text"]["text"] == "*A*" and block["accessory"]["text"]["text"] == "1 interested"
