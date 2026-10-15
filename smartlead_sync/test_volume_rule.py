"""Weekly volume review, judged per domain (spec 2026-10-15, revised after research). Pure, no network."""
from datetime import date

from smartlead import volume_rule as vr

TODAY = date(2026, 10, 21)


def ptest(day, seeds=20, spam=0):
    return {"date": day, "seeds": seeds, "spam": spam, "by": {}}


OK = [ptest("2026-10-20")]
EMAILS = ["a@d.com", "b@d.com", "c@d.com"]


def all_tests(t=OK):
    return {e: t for e in EMAILS}


# Signals -------------------------------------------------------------------

def test_placement_good_when_every_tested_inbox_passed_since_the_last_change():
    assert vr.placement_signal(all_tests(), TODAY, since=None)[0] == vr.GOOD


def test_placement_one_failing_inbox_holds_two_step_back():
    t = all_tests()
    t["a@d.com"] = [ptest("2026-10-20", spam=8)]
    state, why = vr.placement_signal(t, TODAY, since=None)
    assert state == vr.SLIP and "1 of 3" in why
    t["b@d.com"] = [ptest("2026-10-20", spam=8)]
    assert vr.placement_signal(t, TODAY, since=None)[0] == vr.BAD


def test_placement_waits_for_tests_newer_than_the_last_change_and_half_the_inboxes():
    assert vr.placement_signal(all_tests(), TODAY, since=date(2026, 10, 20))[0] == vr.WAIT
    assert vr.placement_signal({"a@d.com": OK, "b@d.com": [], "c@d.com": []}, TODAY, since=None)[0] == vr.WAIT
    assert vr.placement_signal(all_tests([ptest("2026-09-20")]), TODAY, since=None)[0] == vr.WAIT


def test_placement_dense_domain_one_fail_in_many_is_a_slip_but_a_poor_pool_is_bad():
    many = {f"x{i}@d.com": OK for i in range(20)}
    many["x0@d.com"] = [ptest("2026-10-20", spam=8)]
    assert vr.placement_signal(many, TODAY, since=None)[0] == vr.SLIP
    poor = {f"x{i}@d.com": [ptest("2026-10-20", spam=5)] for i in range(4)}     # 75% each, pooled 75% on 80 seeds
    assert vr.placement_signal(poor, TODAY, since=None)[0] == vr.BAD


def test_bounces_pooled_over_the_domain():
    assert vr.bounce_signal(300, 3)[0] == vr.GOOD
    assert vr.bounce_signal(300, 7)[0] == vr.SLIP           # 2.3%
    state, why = vr.bounce_signal(300, 12)
    assert state == vr.BAD and "4%" in why
    assert vr.bounce_signal(90, 9)[0] == vr.WAIT            # under 100 sends: not enough to judge


def test_reputation_is_an_alarm_never_a_step_back():
    assert vr.reputation_signal([100, 98], "google")[0] == vr.GOOD
    assert vr.reputation_signal([100, 85], "google")[0] == vr.SLIP
    assert vr.reputation_signal([75], "smtp")[0] == vr.GOOD


def test_usage_needs_the_limit_to_be_used():
    assert vr.usage_signal([[15, 15, 4, 15, 14]] * 3, 15)[0] == vr.GOOD      # at the limit 3 of 5 days
    state, why = vr.usage_signal([[7, 6, 2, 9, 5]] * 3, 15)
    assert state == vr.SLIP and "about 6 of 15" in why
    assert vr.usage_signal([], 15)[0] == vr.WAIT


def sig(**over):
    s = {"placement": (vr.GOOD, "p"), "bounces": (vr.GOOD, "b"), "reputation": (vr.GOOD, "r"),
         "kill": (vr.GOOD, ""), "usage": (vr.GOOD, "u")}
    s.update(over)
    return s


# Decision --------------------------------------------------------------------

def test_raise_by_scaledmail_step_up_to_the_maximum():
    d = vr.decide("google", [15, 15, 15], sig(), last_change=None, today=TODAY)
    assert (d["action"], d["to"]) == (vr.RAISE, [17, 17, 17])
    assert vr.decide("google", [19, 20], sig(), last_change=None, today=TODAY)["to"] == [20, 20]
    assert vr.decide("outlook_dense", [5], sig(), last_change=None, today=TODAY)["to"] == [6]
    assert vr.decide("smtp", [7], sig(), last_change=None, today=TODAY)["to"] == [8]
    d = vr.decide("outlook", [10, 10], sig(), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "maximum" in d["why"]


def test_raise_waits_a_week_and_needs_the_limit_used():
    d = vr.decide("google", [17], sig(), last_change=date(2026, 10, 16), today=TODAY)
    assert d["action"] == vr.HOLD and "changed 5 days ago" in d["why"]
    d = vr.decide("google", [15], sig(usage=(vr.SLIP, "sends about 6 of 15")), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "about 6 of 15" in d["why"]


def test_a_slip_or_missing_data_holds():
    d = vr.decide("google", [17], sig(bounces=(vr.SLIP, "bounced 2.3% of 300")), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "2.3%" in d["why"]
    assert vr.decide("google", [17], sig(placement=(vr.WAIT, "w")), last_change=None, today=TODAY)["action"] == vr.HOLD


def test_any_bad_signal_steps_the_domain_back_without_cooldown_never_below_start():
    bad = sig(bounces=(vr.BAD, "bounced 4% of 300"))
    d = vr.decide("google", [19, 17], bad, last_change=date(2026, 10, 20), today=TODAY)
    assert (d["action"], d["to"]) == (vr.STEP_BACK, [17, 15])
    d = vr.decide("google", [15], bad, last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "already at the starting limit" in d["why"]
    d = vr.decide("google", [17], sig(kill=(vr.BAD, "1 inbox on the kill list")), last_change=None, today=TODAY)
    assert d["action"] == vr.STEP_BACK


def test_usage_or_cooldown_never_block_a_step_back():
    d = vr.decide("google", [17], sig(placement=(vr.BAD, "x"), usage=(vr.SLIP, "y")), last_change=date(2026, 10, 20),
                  today=TODAY)
    assert d["action"] == vr.STEP_BACK


# Which inboxes ----------------------------------------------------------------

def acct(limit=15, tags=("Bot-Setup",), warmup="2026-10-01T10:00:00Z", type_="GMAIL"):
    return {"from_email": "a@x.com", "type": type_, "message_per_day": limit,
            "tags": [{"tag_name": t} for t in tags], "warmup_details": {"warmup_created_at": warmup}}


def test_eligible_needs_setup_tag_or_pilot_ready_date_campaign_and_untouched_limit():
    assert vr.eligible(acct(), "google", TODAY, sending=True, approved=None) is None
    assert vr.eligible(acct(tags=()), "google", TODAY, sending=True, approved=None) == vr.NOT_OURS
    assert vr.eligible(acct(tags=(), limit=20), "google", TODAY, sending=True, approved=20, managed=True) is None
    assert "warming until 4 Nov" in vr.eligible(acct(warmup="2026-10-21T10:00:00Z"), "google", TODAY, sending=True,
                                                approved=None)
    assert "no campaign" in vr.eligible(acct(), "google", TODAY, sending=False, approved=None)
    assert "changed by hand to 12" in vr.eligible(acct(limit=12), "google", TODAY, sending=True, approved=None)


# Message ------------------------------------------------------------------------

def row(domain, action, kind="google", frm=(15, 15, 15), to=(17, 17, 17), pilot=False, why="all good", client="Melior"):
    return {"domain": domain, "client": client, "kind": kind, "action": action, "from": list(frm), "to": list(to),
            "why": why, "pilot": pilot, "inboxes": [{"id": i, "email": f"x{i}@{domain}"} for i in range(len(frm))]}


def test_review_text_separates_pilot_changes_from_suggestions():
    rows = [row("askmelior.com", vr.RAISE, pilot=True),
            row("meliorbase.com", vr.HOLD, kind="outlook_dense", frm=(5,), to=(5,), why="1 of 21 inboxes failed its test"),
            row("other.com", vr.RAISE)]
    t = vr.review_text(rows, {"warming": 75, "first_ready": "2026-10-27"}, TODAY, applied={"askmelior.com": True})
    lines = t.splitlines()
    assert lines[0] == "Volume review, 21 Oct (by domain, ScaledMail's weekly steps)"
    assert "Pilot, changed now: askmelior.com Google 15 -> 17 (3 inboxes)" in t
    assert "Suggested only (not in the pilot): raise other.com Google 15 -> 17 (3 inboxes)" in t
    assert "Hold: meliorbase.com (1 of 21 inboxes failed its test)" in t
    assert "75 inboxes are still warming (first ready 27 Oct)" in t


def test_review_text_reports_a_pilot_write_that_failed():
    rows = [row("askmelior.com", vr.RAISE, pilot=True)]
    t = vr.review_text(rows, {}, TODAY, applied={"askmelior.com": False})
    assert "Pilot, NOT changed (see below): askmelior.com" in t


def test_ip_rotation_note():
    t = vr.review_text([], {"rotate_ip": ["smtpdomain.com (45%)"]}, TODAY, applied={})
    assert "ask ScaledMail to rotate the IP: smtpdomain.com (45%)" in t


def test_dry_run_says_what_the_pilot_would_change():
    t = vr.review_text([row("askmelior.com", vr.RAISE, pilot=True)], {}, TODAY, applied={})
    assert "Pilot, would change (dry run): askmelior.com Google 15 -> 17 (3 inboxes)" in t
