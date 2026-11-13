"""The weekly real-recipient check: sender per lead, estimates, the fair same-campaign comparison and the post."""
from datetime import date

import pytest

from smartlead import real_recipients as rr

T = date(2026, 11, 15)


def cell(client, recipient, quiet, mix, events, campaign="1"):
    return {"client": client, "campaign": campaign, "recipient": recipient, "quiet": quiet, "mix": mix,
            "events_by_sender": events}


def test_read_history_names_the_newest_sender_and_the_first_reply():
    hist = {"history": [{"type": "SENT", "from": "a@g.com"}, {"type": "SENT", "from": "B@g.com"},
                        {"type": "REPLY", "email_body": "<p>Out of office until Monday</p>"}]}
    assert rr.read_history(hist) == ("b@g.com", " Out of office until Monday ")
    assert rr.read_history([]) == (None, "")
    assert rr.is_bounce("Delivery Status Notification (Failure): address not found")
    assert not rr.is_bounce("Thanks, sounds good") and not rr.is_bounce("")


def test_estimates_scale_the_sampled_mix_and_keep_events_exact():
    cells = [cell("Melior", "Gmail", 90, {"Google": 15, "Outlook": 5, "unknown": 2}, {"Google": 6, "Outlook": 1})]
    est = rr.estimates(cells)
    assert est[("Melior", "Google", "Gmail")] == {"leads": 90 * 15 / 20 + 6, "events": 6, "sampled": 15}
    assert est[("Melior", "Outlook", "Gmail")] == {"leads": 90 * 5 / 20 + 1, "events": 1, "sampled": 5}
    assert ("Melior", "unknown", "Gmail") not in est             # an unknown sender is not a type


def test_the_fair_comparison_uses_only_campaigns_that_used_both_types():
    cells = [cell("Bettrdata", "Gmail", 100, {"Google": 10, "Outlook": 10}, {"Google": 8, "Outlook": 4}, "1"),
             cell("Bettrdata", "Gmail", 500, {"Google": 25}, {"Google": 1}, "2")]          # Google only: left out
    p = rr.paired(cells)[("Bettrdata", "Gmail")]
    assert p["Google"] == {"leads": 58.0, "events": 8, "sampled": 10}
    assert p["Outlook"] == {"leads": 54.0, "events": 4, "sampled": 10}
    assert rr.rate(p["Google"]) == pytest.approx(1000 * 8 / 58)


def test_the_post_compares_inside_campaigns_and_says_when_there_are_too_few_replies():
    cells = [cell("Melior", "Gmail", 200, {"Google": 25, "Outlook": 25}, {"Google": 20, "Outlook": 9}),
             cell("Melior", "Microsoft", 300, {"Google": 5, "Outlook": 20}, {"Google": 2, "Outlook": 4}),
             cell("Precise Leads", "Gmail", 100, {"Outlook": 25}, {"Outlook": 5}, "9")]
    head, parts = rr.format_post(cells, T, 28, {"message history HTTPStatusError": 3})
    assert head[0].startswith("📬 Real replies, 28 days to 15 Nov")
    melior = next(h for h in head if h.startswith("Melior."))
    assert "Gmail recipients: Google inboxes 167, Outlook inboxes 83 (2.0x)" in melior
    assert "Microsoft recipients: Google inboxes 32, Outlook inboxes 16 (too few replies to compare)" in melior
    pl = next(h for h in head if h.startswith("Precise Leads."))
    assert "Outlook inboxes 48 (no campaign used both types, so no fair comparison)" in pl
    assert any(p.startswith("*Microsoft against Gmail recipients") for p in parts)
    assert "Not read: message history HTTPStatusError 3." in parts[-1]
    assert "@" not in "\n".join(head + parts)                     # no addresses in the post


def test_cli_prints_without_posting_and_fails_when_no_account_was_read(monkeypatch, capsys):
    import real_recipients as cli
    monkeypatch.setattr(cli, "gather", lambda today, days, kinds=None: _done(([cell("Melior", "Gmail", 10, {"Google": 5}, {"Google": 1})], {}, 1)))
    monkeypatch.setattr("sys.argv", ["real_recipients.py", "--today", "2026-11-15"])
    assert cli.main() == 0 and "Real replies" in capsys.readouterr().out
    monkeypatch.setattr(cli, "gather", lambda today, days, kinds=None: _done(([], {"account X HTTPStatusError": 1}, 0)))
    assert cli.main() == 1


async def _done(value):
    return value


def test_the_bot_schedules_it_on_sunday_morning():
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1]
    assert "{ job: 'real-recipients', cron: '0 7 * * 0'" in (root / "bot_health.js").read_text(encoding="utf-8")
    assert "runJob('real-recipients', ['real_recipients.py', '--post', '--save']" in (root / "index.js").read_text(encoding="utf-8")


def test_a_ratio_on_a_handful_of_looked_up_leads_is_not_shown():
    """First live run (13 Nov 2026): 7 looked-up Outlook leads made a 1.5x read as 0.5x."""
    cells = [cell("Bettrdata", "Gmail", 1500, {"Google": 113, "Outlook": 7}, {"Google": 40, "Outlook": 5})]
    head, _ = rr.format_post(cells, T)
    assert "(too few leads looked up to compare)" in next(h for h in head if h.startswith("Bettrdata."))


# --- does Smartlead's test agree with real recipients? (13 Nov 2026) --------------------------------------------

def _google_world(g_events, o_events):
    """Gmail recipients in one campaign using both types: 25 looked-up leads a side, 100 quiet leads each."""
    return [cell("Melior", "Gmail", 200, {"Google": 25, "Outlook": 25}, {"Google": g_events, "Outlook": o_events})]


def test_google_becomes_untrusted_when_v2_fails_most_but_real_gmail_recipients_answer_as_well():
    new, changes, lines = rr.trust_decision(_google_world(20, 8), {"GMAIL": (54, 60)}, set())
    assert "GMAIL" in new and changes and "now NOT trusted" in changes[0]
    assert any("fails 54 of 60 Google inboxes at Gmail" in x for x in lines)


def test_google_is_trusted_again_when_real_recipients_agree_it_does_worse():
    new, changes, _ = rr.trust_decision(_google_world(5, 20), {"GMAIL": (54, 60)}, {"GMAIL"})
    assert "GMAIL" not in new and "now trusted" in changes[0]


def test_a_type_v2_passes_is_trusted_and_one_noisy_week_in_between_changes_nothing():
    assert rr.trust_decision(_google_world(20, 8), {"GMAIL": (5, 60)}, {"GMAIL"})[0] == set()   # it fails few
    new, changes, lines = rr.trust_decision(_google_world(9, 10), {"GMAIL": (54, 60)}, {"GMAIL"})
    assert new == {"GMAIL"} and changes == [] and any("in between" in x for x in lines)


def test_too_little_data_keeps_the_current_view():
    thin = [cell("Melior", "Gmail", 30, {"Google": 6, "Outlook": 6}, {"Google": 2, "Outlook": 1})]
    new, changes, lines = rr.trust_decision(thin, {"GMAIL": (54, 60)}, {"GMAIL"})
    assert new == {"GMAIL"} and changes == [] and "Not enough to judge" in lines[0]
    assert rr.trust_decision(_google_world(20, 8), {"GMAIL": (5, 6)}, set())[0] == set()        # 6 judged: too few


def test_a_campaign_with_almost_no_replies_anywhere_is_named_as_the_offer_or_list():
    cells = [dict(cell("Precise Leads", "Microsoft", 240, {"Outlook": 25}, {}), name="Exhibitors"),
             dict(cell("Precise Leads", "Gmail", 138, {"Gmail": 25}, {"Outlook": 1}), name="Exhibitors"),
             dict(cell("Precise Leads", "Gmail", 400, {"Outlook": 25}, {"Outlook": 30}, "2"), name="Clay in Kochi")]
    assert rr.quiet_campaigns(cells) == ["• Precise Leads · Exhibitors: 1 from 379 leads"]
    head, parts = rr.format_post(cells, T, trust=(["Smartlead's test for Google inboxes is now trusted: x"], ["line"]))
    assert any("almost no replies from any provider" in h for h in head)
    assert "Smartlead's test for Google inboxes is now trusted: x" in head
    assert any(p.startswith("*Does Smartlead's test agree") for p in parts)
