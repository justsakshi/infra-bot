"""The evidence gate on FREE / BUSY (smartlead/usable.py), found 10 Nov 2026: the "All Inboxes" tab that every
tool picks inboxes from ignored DO NOT USE tags, v2 verdicts and listed domains."""
from datetime import date

from smartlead import usable
from smartlead.processing import process_inbox_availability

import pytest

T = date(2026, 11, 10)
PASS = {"verdict": "PASS", "night": "2026-11-05"}


@pytest.fixture(autouse=True)
def _no_date_rule(monkeypatch):
    """The dated rule (a v2 pass is required from 13 Nov 2026) is off here unless a test turns it on, so these
    tests read the same on any day."""
    from smartlead import config
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS_FROM", "off")
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS", False)
    monkeypatch.setattr(config, "AVAILABILITY_BLOCK_LISTED", False)


def test_a_v2_pass_is_required_from_the_start_date_or_when_the_switch_is_on(monkeypatch):
    from smartlead import config
    assert usable.require_v2_pass(date(2030, 1, 1)) is False                       # "off"
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS_FROM", "2026-11-13")
    assert usable.require_v2_pass(date(2026, 11, 12)) is False
    assert usable.require_v2_pass(date(2026, 11, 13)) is True
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS_FROM", "not a date")
    assert usable.require_v2_pass(date(2030, 1, 1)) is False
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS", True)
    assert usable.require_v2_pass(date(2020, 1, 1)) is True


def test_the_shipped_start_date_is_13_nov_2026():
    from smartlead import config
    src = open(config.__file__, encoding="utf-8").read()
    assert 'os.getenv("AVAILABILITY_REQUIRE_V2_PASS_FROM", "2026-11-13")' in src


def test_a_tagged_inbox_is_never_free_whatever_else_is_known():
    for tags in (["DO NOT USE - Google spam (Nov 2026)"], ["Melior", "do not use - failed v2 (auto)"]):
        assert usable.gate(tags, PASS, False, T) == (["do_not_use"], [])
        assert usable.gate(tags, None, None, T, checked=False) == (["do_not_use"], [])      # Mongo down: still held
    assert usable.gate(["Bot-Setup", "ScaledMail-Google"], PASS, False, T) == ([], [])


def test_a_failed_v2_test_blocks_and_a_fresh_pass_is_clean():
    assert usable.gate([], {"verdict": "FAIL", "night": "2026-11-09"}, False, T) == (["failed_v2"], [])
    assert usable.gate([], PASS, False, T) == ([], [])


def test_no_pass_is_a_note_until_the_switch_makes_it_a_block():
    cases = [(None, "no v2 result"), ({"verdict": "WATCH", "night": "2026-11-09"}, "v2 watch on 9 Nov"),
             ({"verdict": "PASS", "night": "2026-10-27"}, "last v2 pass 27 Oct, older than 10 days")]
    for v2, note in cases:
        assert usable.gate([], v2, False, T) == ([], [note])
        assert usable.gate([], v2, False, T, require_v2_pass=True) == (["no_v2_pass"], [note])
    assert usable.gate([], {"verdict": "PASS", "night": "2026-10-31"}, False, T, require_v2_pass=True) == ([], [])   # day 10
    assert usable.gate([], {"verdict": "PASS", "night": "2026-10-30"}, False, T, require_v2_pass=True)[0] == ["no_v2_pass"]


def test_a_listed_domain_is_a_note_until_the_switch_makes_it_a_block():
    note = "domain on Spamhaus DBL or URIBL"
    assert usable.gate([], PASS, True, T) == ([], [note])
    assert usable.gate([], PASS, True, T, block_listed=True) == (["blocklisted"], [note])
    assert usable.gate([], PASS, None, T, block_listed=True) == ([], [])          # no blocklist row: no claim


def test_without_mongo_nothing_is_claimed_about_v2_or_blocklists():
    assert usable.gate([], None, None, T, checked=False, require_v2_pass=True, block_listed=True) == ([], [])


def test_newest_judged_skips_incomplete_rows_and_listed_domains_reads_the_newest_row():
    rows = [{"email": "A@x.com", "night": "2026-11-05", "verdict": "FAIL"},
            {"email": "a@x.com", "night": "2026-11-09", "verdict": "INCOMPLETE"},       # not a verdict: the fail stands
            {"email": "b@x.com", "night": "2026-11-05", "verdict": "FAIL"},
            {"email": "b@x.com", "night": "2026-11-09", "verdict": "PASS"}]
    assert usable.newest_judged(rows) == {"a@x.com": {"verdict": "FAIL", "night": "2026-11-05", "overridden": False},
                                          "b@x.com": {"verdict": "PASS", "night": "2026-11-09", "overridden": False}}
    bl = [{"domain": "old.com", "date": "2026-10-26", "listed_on": ["Spamhaus DBL (127.0.1.2)"]},
          {"domain": "old.com", "date": "2026-11-09", "listed_on": []},                # delisted since
          {"domain": "now.com", "date": "2026-11-09", "listed_on": ["SURBL (ABUSE)", "Spamhaus DBL (127.0.1.2)"]},
          {"domain": "links.com", "date": "2026-11-09", "listed_on": ["SURBL (ABUSE)"]}]   # a link list, not the sender
    assert usable.listed_domains(bl) == {"now.com"}


def test_load_gate_data_never_raises(monkeypatch):
    def boom():
        raise RuntimeError("mongo down")
    monkeypatch.setattr(usable, "_db", boom)
    assert usable.load_gate_data() == {"v2": {}, "listed": set(), "checked": False, "error": "mongo down"}

    class Col:
        def __init__(self, rows):
            self.rows = rows

        def find(self, *a):
            return self.rows
    db = {"v2_verdicts": Col([{"email": "a@x.com", "night": "2026-11-09", "verdict": "FAIL"}]),
          "blacklist_checks": Col([{"domain": "x.com", "date": "2026-11-09", "listed_on": ["Spamhaus DBL (127.0.1.2)"]}])}
    monkeypatch.setattr(usable, "_db", lambda: db)
    got = usable.load_gate_data()
    assert got["checked"] and got["listed"] == {"x.com"} and got["v2"]["a@x.com"]["verdict"] == "FAIL"


# The sheet rule itself -------------------------------------------------------

REP = {"rep": "99%", "warmup_state": "on", "warmup_max_count": 20, "last_active_date": "", "warmup_spam_count": 0}


def _row(email, **over):
    r = {"email": email, "connection_ok": True, "message_per_day": 15, "test_sheet_status": "inbox",
         "dns_spf_ok": True, "dns_dkim_ok": True, "dns_dmarc_ok": True, "tags": []}
    r.update(over)
    return r


def _run(rows, data, monkeypatch=None):
    process_inbox_availability(rows, {r["email"]: REP for r in rows}, {}, gate_data=data)
    return {r["email"]: (r["availability"], r["busy_reason"], r["use_notes"]) for r in rows}


def test_free_needs_more_than_capacity_reputation_and_a_seed_test():
    """Every row below was FREE before: connected, capacity, 99% reputation, a fresh 'inbox' in the grid."""
    data = {"checked": True, "error": "", "listed": {"listed.com"},
            "v2": {"pass@ok.com": {"verdict": "PASS", "night": "2026-11-09"},
                   "fail@bad.com": {"verdict": "FAIL", "night": "2026-11-09"},
                   "p@listed.com": {"verdict": "PASS", "night": "2026-11-09"}}}
    rows = [_row("pass@ok.com"), _row("fail@bad.com"), _row("tag@pulled.com", tags=["DO NOT USE - Google spam (Nov 2026)"]),
            _row("new@untested.com"), _row("p@listed.com")]
    got = _run(rows, data)
    assert got["pass@ok.com"][:2] == ("FREE", "")
    assert got["fail@bad.com"][:2] == ("BUSY", "failed_v2")
    assert got["tag@pulled.com"][0] == "BUSY" and "do_not_use" in got["tag@pulled.com"][1]
    assert got["new@untested.com"] == ("FREE", "", "no v2 result")                    # a warning until the switch
    assert got["p@listed.com"] == ("FREE", "", "domain on Spamhaus DBL or URIBL")
    assert rows[1]["v2_status"] == "FAIL" and rows[1]["v2_date"] == "2026-11-09" and rows[3]["v2_status"] == ""


def test_the_two_switches_turn_the_warnings_into_blocks(monkeypatch):
    from smartlead import config
    monkeypatch.setattr(config, "AVAILABILITY_BLOCK_LISTED", True)
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS", True)
    data = {"checked": True, "error": "", "listed": {"listed.com"},
            "v2": {"p@listed.com": {"verdict": "PASS", "night": "2026-11-09"}}}
    got = _run([_row("new@untested.com"), _row("p@listed.com")], data)
    assert got["new@untested.com"][:2] == ("BUSY", "no_v2_pass")
    assert got["p@listed.com"][:2] == ("BUSY", "blocklisted")


def test_when_mongo_is_down_the_tag_still_blocks_and_the_sheet_says_what_was_not_read():
    data = {"checked": False, "error": "mongo down", "listed": set(), "v2": {}}
    got = _run([_row("ok@x.com"), _row("tag@x.com", tags=["do not use - failed v2 (auto)"])], data)
    assert got["ok@x.com"] == ("FREE", "", usable.NOT_READ)
    assert got["tag@x.com"][:2] == ("BUSY", "do_not_use")


def test_old_callers_without_gate_data_keep_working_and_still_honour_the_tag():
    rows = [_row("ok@x.com"), _row("tag@x.com", tags=["DO NOT USE"])]
    process_inbox_availability(rows, {r["email"]: REP for r in rows}, {})
    assert (rows[0]["availability"], rows[0]["use_notes"]) == ("FREE", "")
    assert rows[1]["availability"] == "BUSY"


def test_the_all_inboxes_tab_carries_the_new_columns_last():
    from smartlead.sheets import SheetsWriter
    assert SheetsWriter.MASTER_COLUMNS[-5:] == ["v2_status", "v2_date", "use_notes", "evidence", "evidence_label"]
    assert SheetsWriter.MASTER_COLUMNS[:6] == ["client", "email", "name", "provider", "account_id", "availability"]


def test_tags_come_from_the_list_call_because_the_detail_call_has_none():
    """11 Nov 2026: 63 inboxes were tagged DO NOT USE in Smartlead and no row on the sheet said do_not_use.
    The daily sync builds rows from GET /email-accounts/{id}, which returns no tags."""
    listed = [{"id": 1, "from_email": "a@x.com", "tags": [{"id": 9, "name": "DO NOT USE - Google spam (Nov 2026)"}]},
              {"id": 2, "from_email": "b@x.com", "tags": []}, {"id": 3, "from_email": "c@x.com"}]
    details = [{"id": 1, "from_email": "a@x.com", "message_per_day": 15}, {"id": "2", "from_email": "b@x.com"},
               {"id": 3, "from_email": "c@x.com", "tags": [{"name": "kept"}]}, None]
    out = usable.carry_tags(listed, details)
    assert usable.is_do_not_use(usable.tag_names(out[0])) and out[0]["message_per_day"] == 15
    assert "tags" not in details[0]                        # the input is not changed
    assert usable.tag_names(out[1]) == [] and usable.tag_names(out[2]) == ["kept"] and out[3] is None
    assert usable.carry_tags(None, None) == []
    # and the row the gate reads gets them
    from smartlead.processing import _build_inbox_row
    row = _build_inbox_row(out[0], "a@x.com", "N/A", "N/A",
                           {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0}, {})
    assert usable.gate(row["tags"], None, None, date(2026, 11, 11))[0] == ["do_not_use"]


def test_every_inbox_row_value_fits_one_sheet_cell():
    """11 Nov 2026: the row's ``tags`` list made Google refuse the whole "<account> - Inboxes" tab
    ("Invalid values[1][3]: list_value"), and the tab had been cleared first. Whatever a row carries,
    what is sent to the sheet must be a plain value."""
    from smartlead.processing import _build_inbox_row
    from smartlead.sheets import _serialize
    load = {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0}
    for tags in ([], [{"name": "ScaledMail"}, {"name": "DO NOT USE - Google spam (Nov 2026)"}]):
        row = _build_inbox_row({"id": 1, "from_email": "a@x.com", "tags": tags}, "a@x.com", "N/A", "N/A", load, {})
        process_inbox_availability([row], {}, {}, gate_data=None)
        cells = {k: _serialize(v) for k, v in row.items()}
        bad = {k: type(v).__name__ for k, v in cells.items() if not isinstance(v, (str, int, float, type(None)))}
        assert bad == {}, bad
    assert cells["tags"] == "ScaledMail, DO NOT USE - Google spam (Nov 2026)"
    assert _serialize([]) == "" and _serialize(("a", 2)) == "a, 2" and _serialize({"b", "a"}) == "a, b"
    assert _serialize({"k": [1]}) == '{"k": [1]}' and _serialize(True) == "Yes" and _serialize(None) is None


def test_a_google_fail_or_an_overridden_fail_is_a_note_not_a_hold_2026_11_13():
    from datetime import date
    today = date(2026, 11, 13)
    fail = {"verdict": "FAIL", "night": "2026-11-09"}
    assert usable.gate([], fail, None, today, kind="Outlook") == (["failed_v2"], [])
    reasons, notes = usable.gate([], fail, None, today, kind="Gmail", require_v2_pass=True)
    # since 17 Nov (t268) no v2 pass is asked of a Google inbox either: real sends and warm-up decide
    assert reasons == [] and notes == ["failed v2 on 9 Nov, not counted: the v2 test is not used for Google inboxes"]
    assert usable.gate([], fail, None, today, kind="GMAIL")[0] == []           # no pass required yet: no hold at all
    back = {**fail, "overridden": True}
    assert usable.fail_set_aside(back, "OUTLOOK") == "a person put it back after this result"
    assert not usable.holds_back(back, "OUTLOOK") and usable.holds_back(fail, "OUTLOOK")
    assert not usable.holds_back({"verdict": "PASS"}, "OUTLOOK") and usable.fail_set_aside({"verdict": "PASS"}, "GMAIL") == ""
    assert usable.kind_of("Gmail") == "GMAIL" and usable.kind_of("Outlook") == "OUTLOOK" and usable.kind_of("smtp") == "SMTP"
    assert usable.gate(["DO NOT USE - x"], fail, None, today, kind="Gmail")[0] == ["do_not_use"]   # a tag still holds


def test_the_v2_job_and_the_readers_share_one_list_of_untrusted_types():
    import v2_weekly
    assert v2_weekly.REPORT_ONLY_TYPES is usable.V2_NOT_TRUSTED



def test_trust_comes_from_the_env_then_the_weekly_check_then_the_default():
    assert usable.trust_from("", None) == ({"GMAIL": "Google inboxes"}, "default")
    doc = {"untrusted": {"OUTLOOK": {"since": "2026-11-15"}}, "decided": "2026-11-15"}
    assert usable.trust_from("", doc) == ({"OUTLOOK": "Outlook inboxes"}, "real-recipient check of 2026-11-15")
    assert usable.trust_from("none", doc) == ({}, "env V2_UNTRUSTED_TYPES")
    assert usable.trust_from("gmail,OUTLOOK", doc)[0] == {"GMAIL": "Google inboxes", "OUTLOOK": "Outlook inboxes"}
    assert usable.trust_from("", {"untrusted": {}})[0] == {}             # the check decided: everything trusted


def test_refresh_changes_the_shared_dict_every_reader_holds(monkeypatch):
    import v2_weekly

    class Col:
        def __init__(self, doc): self.doc = doc
        def find_one(self, q): return self.doc
    monkeypatch.delenv("V2_UNTRUSTED_TYPES", raising=False)
    try:
        assert usable.refresh_trust({"v2_trust": Col({"untrusted": {}, "decided": "2026-11-15"})}).startswith("real-recipient")
        assert v2_weekly.REPORT_ONLY_TYPES == {} and not usable.fail_set_aside({"verdict": "FAIL"}, "Outlook")
        # Google stays report-only whatever v2_trust says (t268): the readers and the job both
        assert usable.fail_set_aside({"verdict": "FAIL"}, "Gmail")
        assert "GMAIL" in v2_weekly.report_only_types(None) and "OUTLOOK" not in v2_weekly.report_only_types(None)
        assert usable.refresh_trust({"v2_trust": Col(None)}) == "default" and "GMAIL" in v2_weekly.REPORT_ONLY_TYPES
        class Broken:
            def __getitem__(self, k): raise RuntimeError("down")
        assert usable.refresh_trust(Broken()) == "unread" and "GMAIL" in v2_weekly.REPORT_ONLY_TYPES   # down: default
    finally:
        usable.V2_NOT_TRUSTED.clear(); usable.V2_NOT_TRUSTED.update(usable.DEFAULT_NOT_TRUSTED)


def test_high_bounce_alone_on_an_inbox_that_passed_v2_is_a_note_not_busy_2026_11_16():
    """Manveen, 16 Nov 2026: high bounce on its own does not mean do not use ("so just say so"). Outlook inboxes that
    passed v2 again, with warm-up 91-100%, were BUSY only for bounces from past campaigns' lead lists."""
    from datetime import datetime, timedelta, timezone
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()      # the day process_inbox_availability uses
    recent, old = (today - timedelta(days=2)).isoformat(), (today - timedelta(days=20)).isoformat()
    data = {"checked": True, "error": "", "listed": set(),
            "v2": {"passed@a.com": {"verdict": "PASS", "night": recent},
                   "old@a.com": {"verdict": "PASS", "night": old},
                   "failed@a.com": {"verdict": "FAIL", "night": recent}}}
    rows = [_row(e, bounce_rate=6.5, provider="Outlook", type="OUTLOOK")
            for e in ("passed@a.com", "old@a.com", "failed@a.com", "none@a.com")]
    rows.append(_row("full@a.com", bounce_rate=6.5, provider="Outlook", type="OUTLOOK", connection_ok=False))
    data["v2"]["full@a.com"] = {"verdict": "PASS", "night": recent}
    got = _run(rows, data)
    assert got["passed@a.com"] == ("FREE", "", usable.BOUNCE_NOTE)
    assert got["old@a.com"][:2] == ("BUSY", "high_bounce")             # a pass older than 10 days does not clear it
    assert got["failed@a.com"][:2] == ("BUSY", "high_bounce,failed_v2")
    assert got["none@a.com"][:2] == ("BUSY", "high_bounce")
    assert got["full@a.com"][1] == "disconnected" and usable.BOUNCE_NOTE in got["full@a.com"][2]   # other reasons stay
    assert usable.BOUNCE_NOTE == "bounces come from the lead list"


def test_a_tagged_inbox_stays_busy_even_when_its_bounce_is_set_aside():
    from datetime import datetime, timedelta, timezone
    recent = (datetime.now(timezone(timedelta(hours=5, minutes=30))).date() - timedelta(days=2)).isoformat()
    data = {"checked": True, "error": "", "listed": set(), "v2": {"t@a.com": {"verdict": "PASS", "night": recent}}}
    got = _run([_row("t@a.com", bounce_rate=6.5, tags=["DO NOT USE"])], data)
    assert got["t@a.com"][:2] == ("BUSY", "do_not_use")


def test_the_weeks_real_send_evidence_fills_two_columns_and_changes_no_availability():
    from datetime import date
    item = {"email": "A@x.com", "tags": [], "type": "OUTLOOK"}
    data = {"v2": {}, "listed": set(), "checked": True, "error": "",
            "evidence": {"a@x.com": {"evidence": "28d: Microsoft 40 sent, 0 replies, 0 OOO · 7d: 9 sent",
                                     "label": "not enough data"}}, "evidence_week": "2026-11-15"}
    reasons = usable.apply(item, data, date(2026, 11, 17), block_listed=False, require_v2_pass=False)
    assert reasons == [] and item["evidence_label"] == "not enough data (week of 15 Nov)"
    assert item["evidence"].startswith("28d: Microsoft 40 sent")
    other = {"email": "b@x.com", "tags": []}
    usable.apply(other, data, date(2026, 11, 17), block_listed=False, require_v2_pass=False)
    assert other["evidence"] == "" and other["evidence_label"] == "no real sends in the window"


# The Google rule (t268, Manveen 17 Nov 2026) --------------------------------------------------------------------

def _g(email, rep, **over):
    return _row(email, provider="Gmail", **over), {"rep": rep, "warmup_state": "on", "warmup_max_count": 20,
                                                   "last_active_date": "", "warmup_spam_count": 0}


def _ev(**labels):
    """Week's evidence: {email: own-provider label}; a label "x|Microsoft" puts it at Microsoft instead."""
    out = {}
    for email, lab in labels.items():
        lab, _, rp = lab.partition("|")
        out[email.replace("_", "@") + ".com"] = {"evidence": "28d: ...", "label": f"{lab} at {rp or 'Gmail'}",
                                                 "providers": {rp or "Gmail": lab}}
    return out


def _grun(rows_reps, evidence, v2=None, require=True, monkeypatch=None):
    from smartlead import config
    rows = [r for r, _ in rows_reps]
    data = {"checked": True, "error": "", "listed": set(), "v2": v2 or {}, "evidence": evidence,
            "evidence_week": "2026-11-17"}
    old = config.AVAILABILITY_REQUIRE_V2_PASS
    config.AVAILABILITY_REQUIRE_V2_PASS = require
    try:
        process_inbox_availability(rows, {r["email"]: rep for r, rep in rows_reps}, {}, gate_data=data)
    finally:
        config.AVAILABILITY_REQUIRE_V2_PASS = old
    return {r["email"]: (r["availability"], r["busy_reason"], r["use_notes"]) for r in rows}


def test_google_reached_with_low_reputation_is_usable_and_says_repairing():
    got = _grun([_g("a@g.com", "60%", test_sheet_status="fail", test_date="2026-11-09")],
                _ev(a_g="reached the inbox"))
    assert got["a@g.com"][:2] == ("FREE", "")
    assert "repairing but usable: warm-up 60%" in got["a@g.com"][2]
    assert "seed test spam 9 Nov: report only for Google inboxes" in got["a@g.com"][2]


def test_google_high_reputation_without_enough_data_is_usable_and_low_reputation_is_not():
    got = _grun([_g("hi@g.com", "92%"), _g("lo@g.com", "70%")], _ev(hi_g="not enough data", lo_g="not enough data"))
    assert got["hi@g.com"][:2] == ("FREE", "")
    assert got["lo@g.com"][:2] == ("BUSY", "low_rep")


def test_google_missing_evidence_is_not_enough_data_never_quiet():
    got = _grun([_g("hi@g.com", "92%"), _g("lo@g.com", "70%")], {})
    assert got["hi@g.com"][:2] == ("FREE", "") and got["lo@g.com"][:2] == ("BUSY", "low_rep")
    for evidence in (None, {}):        # evidence not read at all (Mongo down / old callers): reputation decides
        r, _ = _g("x@g.com", "95%")
        process_inbox_availability([r], {"x@g.com": REP}, {}, gate_data=None if evidence is None else
                                   {"checked": False, "error": "down", "listed": set(), "v2": {}})
        assert r["availability"] == "FREE", r["busy_reason"]


def test_google_quiet_is_never_usable():
    got = _grun([_g("q@g.com", "99%")], _ev(q_g="quiet (0 of ~4.0 expected)"))
    assert got["q@g.com"][:2] == ("BUSY", "quiet")


def test_google_quiet_or_reached_at_another_provider_is_a_signal_only():
    got = _grun([_g("q@g.com", "99%"), _g("r@g.com", "60%")],
                _ev(q_g="quiet (0 of ~4.0 expected)|Microsoft", r_g="reached the inbox|Microsoft"))
    assert got["q@g.com"][:2] == ("FREE", "")
    assert "quiet (0 of ~4.0 expected) at Microsoft (not its provider: a signal, not a verdict)" in got["q@g.com"][2]
    assert got["r@g.com"][:2] == ("BUSY", "low_rep")          # reached only at Microsoft does not repair a Google inbox


def test_google_do_not_use_tag_wins_whatever_else():
    got = _grun([_g("t@g.com", "99%", tags=["DO NOT USE - x"])], _ev(t_g="reached the inbox"))
    assert got["t@g.com"][0] == "BUSY" and got["t@g.com"][1].split(",") == ["do_not_use"]


def test_google_disconnected_is_not_usable():
    got = _grun([_g("d@g.com", "99%", connection_ok=False)], _ev(d_g="reached the inbox"))
    assert got["d@g.com"][:2] == ("BUSY", "disconnected")


def test_google_failed_seed_or_v2_never_blocks():
    fail = {"verdict": "FAIL", "night": "2026-11-15"}
    rows = [_g("s@g.com", "95%", test_sheet_status="fail"), _g("st@g.com", "95%", test_sheet_status="stale"),
            _g("u@g.com", "95%", test_sheet_status="Unknown"), _g("v@g.com", "95%")]
    got = _grun(rows, {}, v2={"v@g.com": fail}, require=True)
    assert {e: v[:2] for e, v in got.items()} == {e: ("FREE", "") for e in got}
    assert "failed v2 on 15 Nov, not counted" in got["v@g.com"][2]
    # even when v2_trust trusts Google, a v2 fail is a note for a Google inbox
    saved = dict(usable.V2_NOT_TRUSTED)
    usable.V2_NOT_TRUSTED.clear()
    try:
        got = _grun([_g("v@g.com", "95%")], {}, v2={"v@g.com": fail})
        assert got["v@g.com"][:2] == ("FREE", "")
    finally:
        usable.V2_NOT_TRUSTED.update(saved)


def test_outlook_keeps_the_seed_v2_and_reputation_rules():
    fail = {"verdict": "FAIL", "night": "2026-11-15"}
    rows = [(_row("f@o.com", provider="Outlook", test_sheet_status="fail"), REP),
            (_row("l@o.com", provider="Outlook"), {**REP, "rep": "60%"}),
            (_row("v@o.com", provider="Outlook"), REP),
            (_row("n@o.com", provider="Outlook"), REP)]
    got = _grun(rows, _ev(l_o="reached the inbox|Microsoft"), v2={"v@o.com": fail})
    assert got["f@o.com"][:2] == ("BUSY", "failed_test,no_v2_pass")
    assert got["l@o.com"][:2] == ("BUSY", "low_rep,no_v2_pass")       # real replies do not repair an Outlook inbox
    assert got["v@o.com"][:2] == ("BUSY", "failed_v2")
    assert got["n@o.com"][:2] == ("BUSY", "no_v2_pass")


def test_google_gate_is_pure_and_reads_only_its_own_provider():
    assert usable.google_gate(95, "") == ([], [])
    assert usable.google_gate(89.9, "not enough data") == (["low_rep"], [])
    assert usable.google_gate(60, "reached the inbox")[0] == []
    assert usable.google_gate(99, "quiet (0 of ~3.1 expected)")[0] == ["quiet"]
    assert usable.google_gate(60, "quiet (0 of ~3.1 expected)")[0] == ["quiet", "low_rep"]
    assert usable.own_evidence("A@g.com", "GMAIL", {"evidence": {"a@g.com": {"providers": {"Microsoft": "reached the inbox"}}}}) \
        == ("not enough data", {"Microsoft": "reached the inbox"})
    assert usable.real_send_rule("Gmail") and usable.real_send_rule("GMAIL") and not usable.real_send_rule("Outlook")
    assert not usable.real_send_rule("SMTP")
