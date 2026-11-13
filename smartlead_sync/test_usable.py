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
    assert SheetsWriter.MASTER_COLUMNS[-3:] == ["v2_status", "v2_date", "use_notes"]
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
    assert reasons == ["no_v2_pass"] and notes == ["failed v2 on 9 Nov, not counted: the v2 test is not used for Google inboxes"]
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
        assert v2_weekly.REPORT_ONLY_TYPES == {} and not usable.fail_set_aside({"verdict": "FAIL"}, "Gmail")
        assert usable.refresh_trust({"v2_trust": Col(None)}) == "default" and "GMAIL" in v2_weekly.REPORT_ONLY_TYPES
        class Broken:
            def __getitem__(self, k): raise RuntimeError("down")
        assert usable.refresh_trust(Broken()) == "unread" and "GMAIL" in v2_weekly.REPORT_ONLY_TYPES   # down: default
    finally:
        usable.V2_NOT_TRUSTED.clear(); usable.V2_NOT_TRUSTED.update(usable.DEFAULT_NOT_TRUSTED)
