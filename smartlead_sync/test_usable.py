"""The evidence gate on FREE / BUSY (smartlead/usable.py), found 10 Nov 2026: the "All Inboxes" tab that every
tool picks inboxes from ignored DO NOT USE tags, v2 verdicts and listed domains."""
from datetime import date

from smartlead import usable
from smartlead.processing import process_inbox_availability

T = date(2026, 11, 10)
PASS = {"verdict": "PASS", "night": "2026-11-05"}


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
             ({"verdict": "PASS", "night": "2026-10-27"}, "last v2 pass 27 Oct, older than 8 days")]
    for v2, note in cases:
        assert usable.gate([], v2, False, T) == ([], [note])
        assert usable.gate([], v2, False, T, require_v2_pass=True) == (["no_v2_pass"], [note])
    assert usable.gate([], {"verdict": "PASS", "night": "2026-11-02"}, False, T, require_v2_pass=True) == ([], [])   # day 8


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
    assert usable.newest_judged(rows) == {"a@x.com": {"verdict": "FAIL", "night": "2026-11-05"},
                                          "b@x.com": {"verdict": "PASS", "night": "2026-11-09"}}
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
