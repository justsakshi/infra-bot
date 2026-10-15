"""Weekly volume review rule (spec 2026-10-15). Pure, no network."""
from datetime import date

from smartlead import volume_rule as vr

TODAY = date(2026, 11, 3)


def ptest(day, seeds=20, spam=0):
    return {"date": day, "seeds": seeds, "spam": spam, "by": {}}


PASS2 = [ptest("2026-11-02"), ptest("2026-10-26")]


def signals(tests=PASS2, sent=100, bounced=0, rep="100%"):
    return vr.signals(tests, sent, bounced, rep, TODAY)


# Signals -------------------------------------------------------------------

def test_placement_good_needs_two_passes_a_week_apart():
    assert vr.placement_signal(PASS2, TODAY)[0] == vr.GOOD
    assert vr.placement_signal([ptest("2026-11-02"), ptest("2026-10-30")], TODAY)[0] == vr.WAIT
    assert vr.placement_signal([ptest("2026-11-02")], TODAY)[0] == vr.WAIT
    assert vr.placement_signal([], TODAY)[0] == vr.WAIT


def test_placement_uses_an_older_pass_when_the_one_before_is_too_close():
    tests = [ptest("2026-11-02"), ptest("2026-10-31"), ptest("2026-10-25")]
    assert vr.placement_signal(tests, TODAY)[0] == vr.GOOD


def test_placement_newest_failed_is_a_slip_and_recovery_is_wait():
    state, why = vr.placement_signal([ptest("2026-11-02", spam=8), ptest("2026-10-26")], TODAY)
    assert state == vr.SLIP and "60%" in why
    assert vr.placement_signal([ptest("2026-11-02"), ptest("2026-10-26", spam=8)], TODAY)[0] == vr.WAIT


def test_placement_stale_or_small_tests_do_not_count():
    assert vr.placement_signal([ptest("2026-10-01"), ptest("2026-09-20")], TODAY)[0] == vr.WAIT
    assert vr.placement_signal([ptest("2026-11-02", seeds=5, spam=5), ptest("2026-10-26")], TODAY)[0] == vr.WAIT


def test_bounce_signal():
    assert vr.bounce_signal(100, 1)[0] == vr.GOOD
    state, why = vr.bounce_signal(100, 2)
    assert state == vr.SLIP and "2%" in why
    assert vr.bounce_signal(19, 5)[0] == vr.WAIT


def test_reputation_signal():
    assert vr.reputation_signal("90%")[0] == vr.GOOD
    assert vr.reputation_signal("89%")[0] == vr.SLIP
    assert vr.reputation_signal(None)[0] == vr.WAIT


# Decision --------------------------------------------------------------------

def test_all_good_raises_by_the_type_step():
    d = vr.decide("google", 15, signals(), last_change=None, today=TODAY)
    assert (d["action"], d["to"]) == (vr.RAISE, 17)
    d = vr.decide("outlook", 5, signals(), last_change=None, today=TODAY)
    assert (d["action"], d["to"]) == (vr.RAISE, 6)


def test_raise_never_passes_the_maximum():
    assert vr.decide("google", 19, signals(), last_change=None, today=TODAY)["to"] == 20
    d = vr.decide("outlook_dense", 8, signals(), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "maximum" in d["why"]


def test_raise_waits_seven_days_after_the_last_change():
    d = vr.decide("google", 17, signals(), last_change=date(2026, 10, 29), today=TODAY)
    assert d["action"] == vr.HOLD and "changed 5 days ago" in d["why"]
    assert vr.decide("google", 17, signals(), last_change=date(2026, 10, 27), today=TODAY)["action"] == vr.RAISE


def test_one_slip_or_missing_data_holds():
    d = vr.decide("google", 17, signals(bounced=3), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "bounced 3%" in d["why"]
    d = vr.decide("google", 17, signals(sent=10), last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "only 10 sent" in d["why"]


def test_two_slips_step_back_but_never_below_the_start():
    bad = signals(bounced=3, rep="80%")
    d = vr.decide("google", 19, bad, last_change=date(2026, 11, 1), today=TODAY)   # no cooldown on a step back
    assert (d["action"], d["to"]) == (vr.STEP_BACK, 17)
    assert vr.decide("google", 16, bad, last_change=None, today=TODAY)["to"] == 15
    d = vr.decide("google", 15, bad, last_change=None, today=TODAY)
    assert d["action"] == vr.HOLD and "starting limit" in d["why"]


def test_types_with_no_step_and_kill_list_inboxes_are_skipped():
    d = vr.decide("google", 15, signals(), last_change=None, today=TODAY, verdict="SHOULD KILL")
    assert d["action"] == vr.SKIP and "Kill warnings" in d["why"]


# Which inboxes ----------------------------------------------------------------

def acct(limit=15, tags=("Bot-Setup",), warmup="2026-10-13T10:00:00Z", type_="GMAIL"):
    return {"from_email": "a@x.com", "type": type_, "message_per_day": limit,
            "tags": [{"tag_name": t} for t in tags], "warmup_details": {"warmup_created_at": warmup}}


def test_eligible_needs_setup_tag_ready_date_campaign_and_untouched_limit():
    assert vr.eligible(acct(), "google", TODAY, sending=True, approved=None) is None
    assert vr.eligible(acct(tags=()), "google", TODAY, sending=True, approved=None) == vr.NOT_OURS
    assert "warming until 27 Oct" in vr.eligible(acct(), "google", date(2026, 10, 20), sending=True, approved=None)
    assert "no campaign" in vr.eligible(acct(), "google", TODAY, sending=False, approved=None)
    assert "changed by hand to 12" in vr.eligible(acct(limit=12), "google", TODAY, sending=True, approved=None)
    assert vr.eligible(acct(limit=17), "google", TODAY, sending=True, approved=17) is None


# Message ------------------------------------------------------------------------

def row(client, action, kind="google", frm=15, to=17, email="a@x.com", why=""):
    return {"client": client, "email": email, "kind": kind, "action": action, "from": frm, "to": to, "why": why}


def test_review_text_counts_per_client_and_explains_holds():
    rows = [row("Precise Leads", vr.RAISE, email=f"p{i}@pl.com") for i in range(3)]
    rows += [row("Bettrdata", vr.STEP_BACK, kind="outlook", frm=6, to=5, email="b@bd.com"),
             row("Bettrdata", vr.HOLD, email="h@bd.com", frm=15, to=15, why="bounced 3% of 120 sent")]
    t = vr.review_text(rows, {"warming": 75, "first_ready": "2026-10-27"}, TODAY)
    assert t.splitlines()[0] == "Volume review, 3 Nov: raise 3 inboxes, hold 1, step back 1"
    assert "Precise Leads: raise 3 (Google 15 -> 17)" in t
    assert "Bettrdata: step back 1 (bd.com Outlook 6 -> 5) · hold 1" in t
    assert "bounced 3% of 120 sent" in t
    assert "75 inboxes are still warming (first ready 27 Oct)" in t


def test_review_text_with_nothing_to_change():
    t = vr.review_text([], {"warming": 75, "first_ready": "2026-10-27"}, TODAY)
    assert "nothing to change" in t.lower()


def test_blocks_have_one_apply_button_per_client_with_changes():
    rows = [row("Precise Leads", vr.RAISE), row("Bettrdata", vr.HOLD, why="x")]
    blocks = vr.review_blocks(rows, {}, TODAY, review_id="2026-11-03")
    buttons = [e for b in blocks for e in (b.get("accessory"),) if e and e.get("type") == "button"]
    assert len(buttons) == 1
    assert buttons[0]["value"] == '{"r": "2026-11-03", "c": "Precise Leads"}'
    assert buttons[0]["confirm"]


def test_smtp_steps_by_one_to_ten_and_judges_reputation_from_70():
    smtp = vr.signals(PASS2, 100, 0, "75%", TODAY, kind="smtp")
    d = vr.decide("smtp", 7, smtp, last_change=None, today=TODAY)
    assert (d["action"], d["to"]) == (vr.RAISE, 8)
    assert vr.decide("smtp", 10, smtp, last_change=None, today=TODAY)["action"] == vr.HOLD
    assert vr.reputation_signal("75%", "google")[0] == vr.SLIP


def test_ip_rotation_note_for_low_smtp_reputation():
    t = vr.review_text([], {"rotate_ip": ["smtpdomain.com (45%)"]}, TODAY)
    assert "ask ScaledMail to rotate the IP: smtpdomain.com (45%)" in t
