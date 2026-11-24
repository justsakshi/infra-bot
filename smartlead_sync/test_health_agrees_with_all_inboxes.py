"""The Inbox Health grade never shows a better state than All Inboxes (t396, 24 Nov 2026).

On 22-23 Nov three Google inboxes graded A / 90 with placement "inbox" while All Inboxes said do not use (tagged,
v2 FAIL, recovering). Their weekly v2 test of 21 Nov put Google seeds 4 of 4 in spam and Microsoft seeds 4 of 4 in the
inbox. The grid writes a Google domain's own G suite cell as the "signal only" label (t269), which the reader takes for
no verdict; it then fell back to the Outlook column's "Inbox" and called the inbox tested and passing.

Fixtures are made-up example.com inboxes.  python -m pytest test_health_agrees_with_all_inboxes.py -q"""
from datetime import date

from smartlead import own_provider as op
from smartlead.capacity import compute_client_capacity
from smartlead.health import build_health_rows, compute_health_score, health_records_for_store, resolve_action
from smartlead.rotation_planner import select_swaps
from smartlead.sheets import own_test_status

TODAY = date(2026, 11, 23)


def grid(gs, ol, day="2026-11-21"):
    return {"status": "inbox", "date": day, "gsuite": gs.lower(), "gsuite_date": day,
            "outlook": ol.lower(), "outlook_date": day}


# --- the grid reader -----------------------------------------------------------------------------------------------

def test_google_signal_cell_is_not_replaced_by_the_outlook_inbox():
    """Google seeds in spam (labelled signal only) + Microsoft seeds in the inbox: no verdict, never "inbox"."""
    status, when, note = own_test_status(grid(op.GOOGLE_SIGNAL_CELL, "Inbox"), "GMAIL")
    assert status == "Unknown" and when == "2026-11-21"      # tested that day; the result is no verdict
    assert note == "Google seeds in spam on 2026-11-21 (Google seed result: signal only)"


def test_google_signal_cell_is_not_turned_into_a_fail_either():
    assert own_test_status(grid(op.GOOGLE_SIGNAL_CELL, "spam"), "GMAIL")[0] == "Unknown"
    assert own_test_status(grid(op.GOOGLE_SIGNAL_CELL, ""), "GMAIL")[0] == "Unknown"


def test_unchanged_cases_around_the_fix():
    assert own_test_status(grid("Inbox", "spam"), "GMAIL")[0] == "inbox"         # its own pass
    assert own_test_status(grid("", "Inbox"), "GMAIL")[0] == "inbox"             # empty own column: as before
    assert own_test_status(grid("spam", "Inbox"), "OUTLOOK")[0] == "inbox"       # Outlook judged on Outlook
    assert own_test_status(grid("Inbox", "spam"), "OUTLOOK")[0] == "fail"


# --- the grade -----------------------------------------------------------------------------------------------------

def snap(**kw):
    base = dict(email="ann@example.com", client="Example", provider="Gmail", warmup_rep_pct="55%",
                warmup_state="on", connection_ok=True, test_sheet_status="inbox", test_date="2026-11-21",
                busy_reason="", bounce_rate=0.0, campaign_name="", max_per_day=15, sent_today=0,
                tags=[], dns_spf_ok=True, dns_dkim_ok=True, dns_dmarc_ok=True)
    base.update(kw)
    return base


def test_the_23_nov_case_grades_d_not_a():
    """The live rows: placement read as inbox (the old reader), warm-up 55%, tagged, v2 FAIL set aside for Google."""
    s = snap(busy_reason="no_capacity,do_not_use")
    assert compute_health_score(s, TODAY)["grade"] == "D"
    # and with the fixed reader the placement part is neutral, not full
    fixed = compute_health_score({**s, "test_sheet_status": "Unknown", "test_date": ""}, TODAY)
    assert fixed["drivers"]["placement"] == 22 and fixed["grade"] == "D"


def test_grade_is_capped_by_the_all_inboxes_answer():
    healthy = dict(warmup_rep_pct="100%", provider="Outlook")
    assert compute_health_score(snap(**healthy), TODAY)["grade"] == "A"                         # send
    for busy in ("do_not_use", "failed_v2", "disconnected", "dns_error", "warmup_blocked"):    # never / cannot
        assert compute_health_score(snap(busy_reason=busy, **healthy), TODAY)["score"] <= 25, busy
    for busy in ("no_v2_pass", "blocklisted"):                                                   # hold
        g = compute_health_score(snap(busy_reason=busy, **healthy), TODAY)
        assert g["score"] <= 69 and g["grade"] in ("C", "D"), busy
    # codes that are about a NEW campaign, not about sending, leave the grade alone
    assert compute_health_score(snap(busy_reason="no_capacity", **healthy), TODAY)["grade"] == "A"
    # Google holds on quiet / low_rep (the Google rule); an Outlook inbox does not hold on low_rep
    assert compute_health_score(snap(busy_reason="quiet", warmup_rep_pct="100%"), TODAY)["score"] <= 69
    assert compute_health_score(snap(busy_reason="low_rep", **healthy), TODAY)["grade"] == "A"


def test_held_inbox_is_not_called_healthy():
    """No other problem found: the row says what All Inboxes says instead of 'healthy', with no priority (decided
    items do not go to the Inbox Health post)."""
    # a Google seed result labelled signal only is a test on record (dated): no "No placement test on record" retest
    signal = own_test_status(grid(op.GOOGLE_SIGNAL_CELL, "Inbox"), "GMAIL")
    g = resolve_action(snap(warmup_rep_pct="100%", busy_reason="no_capacity,do_not_use", test_sheet_status=signal[0],
                            test_date=signal[1], test_signal=signal[2]), 25)
    assert g["top_problem"] == "All Inboxes: do not use (do_not_use)" and g["status"] == "held"
    # an inbox with no test at all, or an "Unknown" with a date and no such note, still gets its test (unchanged)
    for date_ in ("", "2026-11-21"):
        assert resolve_action(snap(test_sheet_status="Unknown", test_date=date_), 50)["top_problem"] == \
            "No placement test on record"
    a = resolve_action(snap(warmup_rep_pct="100%", busy_reason="do_not_use", provider="Outlook"), 25)
    assert a["priority"] == "" and a["status"] == "held"
    assert a["top_problem"] == "All Inboxes: do not use (do_not_use)"
    ok = resolve_action(snap(warmup_rep_pct="100%", provider="Outlook"), 100)
    assert ok["status"] == "healthy" and ok["top_problem"] == ""


def test_history_record_carries_the_all_inboxes_answer():
    class Store:
        def prior_score(self, *a):
            return None
    rows = build_health_rows([snap(busy_reason="no_capacity,do_not_use", test_sheet_status="Unknown", test_date="")],
                             TODAY, Store(), lambda c: {"name": ""})
    rec = health_records_for_store(rows, TODAY)[0]
    assert rec["placement_status"] == "Unknown" and rec["grade"] == "D"
    assert rec["send_verdict"] == "never"


# --- the planners that read the same field -------------------------------------------------------------------------

def test_capacity_does_not_count_an_inbox_all_inboxes_holds_back():
    good = dict(connection_ok=True, warmup_state="on", warmup_rep_pct="95%", test_sheet_status="inbox",
                campaign_status="ACTIVE", message_per_day=30, provider="Outlook")
    rows = [dict(good, email="a@example.com"), dict(good, email="b@example.com", busy_reason="do_not_use"),
            dict(good, email="c@example.com", busy_reason="failed_v2"),
            dict(good, email="d@example.com", busy_reason="no_capacity")]
    assert compute_client_capacity(rows, 10, 0)["sendable_inboxes"] == 2      # a and d


def test_rotation_never_benches_an_inbox_all_inboxes_holds_back():
    def row(email, name, camp="", status="", busy="", rep="95%"):
        return {"email": email, "name": name, "client": "Example", "campaign_name": camp, "campaign_status": status,
                "test_sheet_status": "fail" if status else "inbox", "warmup_rep_pct": rep, "connection_ok": True,
                "capacity_left": 10, "busy_reason": busy or ("failed_test" if status else ""), "account_id": email,
                "warmup_state": "on", "provider": "Outlook"}
    rows = [row("sam@bad.example.com", "Sam A", camp="C1", status="ACTIVE"),
            row("sam@tagged.example.com", "Sam A", busy="do_not_use", rep="99%"),
            row("amy@ok.example.com", "Amy B")]
    swaps = select_swaps(rows, 2)
    assert [s["replacement_email"] for s in swaps] == ["amy@ok.example.com"]
