"""The gate self-test: tags, v2 verdicts, the published list and live campaigns must agree."""
from datetime import datetime, timedelta, timezone

from smartlead import gate_selftest as gs

NOW = datetime(2026, 11, 16, 11, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))     # a Monday
AUTO = "DO NOT USE - failed v2 (auto)"
HB = {"_id": "pc", "last_run": "2026-11-16T04:10:00+00:00", "mode": "act"}


def _row(email, availability="BUSY", reason="no_capacity"):
    return {"Email": email, "Availability": availability, "Busy Reason": reason}


def _world():
    inboxes = [{"email": "tag@x.com", "tags": ["DO NOT USE - Google spam (Nov 2026)"]},
               {"email": "auto@x.com", "tags": ["ScaledMail", AUTO]},
               {"email": "fail@x.com", "tags": []}, {"email": "good@x.com", "tags": []},
               {"email": "past@old.com", "tags": ["DO NOT USE - old client"]}]          # not on the list: not our fleet
    rows = [_row("tag@x.com", reason="do_not_use"), _row("auto@x.com", reason="failed_test,do_not_use,failed_v2"),
            _row("fail@x.com", reason="failed_v2"), _row("good@x.com", "FREE", "")]
    v2 = {"auto@x.com": {"verdict": "FAIL", "night": "2026-11-09"}, "fail@x.com": {"verdict": "FAIL", "night": "2026-11-09"},
          "good@x.com": {"verdict": "PASS", "night": "2026-11-15"}}
    campaigns = [{"client": "Precise Leads", "campaign": "PL | A", "emails": ["good@x.com"]}]
    return inboxes, campaigns, rows, v2


def _run(inboxes, campaigns, rows, v2, hb=HB, require=True, errors=None, tracker=None):
    return {r["name"]: r for r in gs.checks(inboxes, campaigns, rows, v2, hb, NOW, require, errors, tracker=tracker)}


def test_all_agree_when_every_source_says_the_same():
    res = _run(*_world())
    assert all(r["ok"] for r in res.values()) and len(res) == 5
    assert res["list_marks_tagged"]["checked"] == 2 and res["list_marks_failed_v2"]["checked"] == 2
    head, thread = gs.format_post(list(res.values()), NOW.date())
    assert head == ["✅ Gate self-test 16 Nov: all 5 checks agree"]
    assert thread[0].count("✅") == 5


def test_a_tag_that_never_reached_the_list_is_caught_2026_11_11():
    """The defect this job was built on: 63 tagged inboxes, none marked do_not_use on the tab."""
    inboxes, campaigns, rows, v2 = _world()
    rows[0] = _row("tag@x.com", reason="failed_test,failed_v2")           # BUSY, but not for the tag
    res = _run(inboxes, campaigns, rows, v2)
    assert not res["list_marks_tagged"]["ok"] and res["list_marks_tagged"]["bad"] == ["tag@x.com"]
    assert res["list_marks_tagged"]["says"].startswith("1 of 2 inboxes tagged DO NOT USE are not marked do_not_use")
    rows[0] = _row("tag@x.com", "FREE", "")                               # worse: offered to campaigns
    assert _run(inboxes, campaigns, rows, v2)["list_marks_tagged"]["bad"] == ["tag@x.com"]


def test_a_failed_inbox_that_is_free_or_unmarked_is_caught():
    inboxes, campaigns, rows, v2 = _world()
    rows[2] = _row("fail@x.com", "FREE", "")
    res = _run(inboxes, campaigns, rows, v2)
    assert res["list_marks_failed_v2"]["bad"] == ["fail@x.com"]
    assert res["free_means_v2_pass"]["bad"] == ["fail@x.com"]            # and FREE without a pass


def test_free_needs_a_fresh_pass_only_once_the_rule_is_on():
    inboxes, campaigns, rows, v2 = _world()
    v2["good@x.com"] = {"verdict": "PASS", "night": "2026-11-01"}       # 15 days old
    assert _run(inboxes, campaigns, rows, v2)["free_means_v2_pass"]["bad"] == ["good@x.com"]
    off = _run(inboxes, campaigns, rows, v2, require=False)["free_means_v2_pass"]
    assert off["ok"] and off["says"] == "a v2 pass is not required for FREE yet (0 of 1 FREE inboxes have one)"


def test_a_held_back_inbox_in_a_sending_campaign_is_caught_and_report_only_clients_are_a_note(monkeypatch):
    monkeypatch.setattr(gs, "REPORT_ONLY", {"melior": "Melior"})     # the stage itself; none is report-only now
    inboxes, campaigns, rows, v2 = _world()
    campaigns = [{"client": "Precise Leads", "campaign": "PL | A", "emails": ["good@x.com", "fail@x.com", "tag@x.com"]},
                 {"client": "Melior", "campaign": "Melior | Legal", "emails": ["auto@x.com"]}]
    r = _run(inboxes, campaigns, rows, v2)["held_back_not_sending"]
    assert not r["ok"] and r["bad"] == ["fail@x.com", "tag@x.com"]
    assert r["says"] == "2 inboxes tagged or failed are still attached to sending campaigns: Precise Leads · PL | A (2)"
    assert r["note"] == "1 inbox held back still sends in 1 campaign of a report-only client (Melior · Melior | Legal (1))"
    only_melior = _run(inboxes, campaigns[1:], rows, v2)["held_back_not_sending"]
    assert only_melior["ok"] and only_melior["note"]                      # report-only: listed, not failed
    head, _ = gs.format_post([only_melior], NOW.date())
    assert head[0].startswith("✅") and head[1].startswith("• Note: 1 inbox held back")


def test_the_v2_job_must_be_alive_and_acting():
    w = _world()
    assert _run(*w)["v2_job_alive"]["says"] == "the v2 job on the PC ran 2 hours ago in act mode (1 inbox carries its tag)"
    stale = _run(*w, hb={**HB, "last_run": "2026-11-14T04:00:00+00:00"})["v2_job_alive"]
    assert not stale["ok"] and stale["says"] == "the v2 job on the PC last ran 50 hours ago"
    warn = _run(*w, hb={**HB, "mode": "warn"})["v2_job_alive"]
    assert not warn["ok"] and "warn mode" in warn["says"]
    assert _run(*w, hb=None)["v2_job_alive"]["says"] == "the v2 job on the PC has never reported"


def test_a_source_that_was_not_read_never_passes_as_agreement():
    res = _run(*_world(), errors=["the list (All Inboxes): quota exceeded"])
    assert [n for n, r in res.items() if not r["ok"]] == ["list_marks_tagged", "list_marks_failed_v2", "free_means_v2_pass"]
    assert res["list_marks_tagged"]["says"] == "not checked: the list (All Inboxes): quota exceeded"
    res = _run(*_world(), errors=["Smartlead PRECISE_LEADS: HTTP 429"])
    assert not res["list_marks_tagged"]["ok"] and not res["held_back_not_sending"]["ok"] and res["list_marks_failed_v2"]["ok"]
    head, thread = gs.format_post(list(res.values()), NOW.date(), ["Smartlead PRECISE_LEADS: HTTP 429"])
    assert head[0] == "⚠️ Gate self-test 16 Nov: 2 of 5 checks disagree"
    assert thread[-1] == "*Could not read*\n• Smartlead PRECISE_LEADS: HTTP 429"


def test_cli_posts_on_a_disagreement_or_a_monday_and_stays_quiet_otherwise(monkeypatch, capsys):
    import gate_selftest as cli
    sent = []

    async def side(errors):
        w = _world()
        return w[0], w[1]
    monkeypatch.setattr(cli, "smartlead_side", side)
    monkeypatch.setattr(cli, "list_rows", lambda errors: _world()[2])
    monkeypatch.setattr(cli, "mongo_side", lambda errors: (_world()[3], {**HB, "last_run": "2026-11-16T05:30:00+00:00"}, None))
    monkeypatch.setattr(cli, "tracker_names", lambda errors: {"good@x.com"})
    monkeypatch.setattr(cli.usable, "require_v2_pass", lambda today: False)
    monkeypatch.setattr(cli.notify, "post_with_thread", lambda token, ch, head, thread: sent.append(head) or "ts")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")

    class Tue(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 17, 11, 30, tzinfo=tz)

    class Mon(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 16, 11, 30, tzinfo=tz)
    monkeypatch.setattr(cli, "datetime", Tue)
    monkeypatch.setattr("sys.argv", ["gate_selftest.py", "--post"])
    assert cli.main() == 0 and sent == []                                  # all agree on a Tuesday: quiet
    monkeypatch.setattr(cli, "datetime", Mon)
    assert cli.main() == 0 and len(sent) == 1 and sent[0][0].startswith("✅ Gate self-test 16 Nov")
    monkeypatch.setattr(cli, "datetime", Tue)
    monkeypatch.setattr(cli, "list_rows", lambda errors: [_row("tag@x.com", "FREE", "")] + _world()[2][1:])
    assert cli.main() == 0 and len(sent) == 2 and sent[1][0].startswith("⚠️ Gate self-test 17 Nov: 1 of 6")


def test_a_working_inbox_the_tracker_does_not_know_is_caught_2026_11_11():
    """Twelve sending inboxes of a current client sat on domains with no brand in the name, so the tracker sync
    never added them and nothing said so."""
    inboxes, campaigns, rows, v2 = _world()
    inboxes = inboxes + [
        {"email": "known@x.com", "tags": [], "connected": True, "client": "Bettrdata"},
        {"email": "unknown@nobrand.co", "tags": [], "connected": True, "client": "Bettrdata"},
        {"email": "dead@nobrand.co", "tags": [], "connected": False, "client": "Bettrdata"},      # not working: not asked for
        {"email": "old@past.com", "tags": [], "connected": True, "client": "OSC"}]                # a past client's
    r = _run(inboxes, campaigns, rows, v2, tracker={"known@x.com", "x.com"})["tracker_has_every_inbox"]
    assert not r["ok"] and r["checked"] == 2 and r["bad"] == ["unknown@nobrand.co"]
    assert r["says"].startswith("1 of 2 connected inboxes of current clients have no row in the asset tracker")
    r = _run(inboxes, campaigns, rows, v2, tracker={"known@x.com", "unknown@nobrand.co"})["tracker_has_every_inbox"]
    assert r["ok"] and r["says"].startswith("all 2 connected inboxes")


def test_the_tracker_check_fails_when_the_tracker_was_not_read_and_is_absent_when_not_asked_for():
    inboxes, campaigns, rows, v2 = _world()
    r = _run(inboxes, campaigns, rows, v2, tracker=set(), errors=["the tracker (Mongo assets): not read"])
    assert not r["tracker_has_every_inbox"]["ok"] and r["tracker_has_every_inbox"]["says"].startswith("not checked")
    assert "tracker_has_every_inbox" not in _run(inboxes, campaigns, rows, v2)


def test_a_google_inbox_that_failed_v2_or_one_a_person_put_back_is_not_held_back_2026_11_13():
    """The v2 job does not act on Google inboxes (PR #69) or on an inbox whose tag a person removed; the self-test
    must not call them held back either, or the bot contradicts the decision in the channel every morning."""
    inboxes = [{"email": "g@b.co", "tags": [], "type": "GMAIL"}, {"email": "back@x.com", "tags": [], "type": "OUTLOOK"},
               {"email": "o@x.com", "tags": [], "type": "OUTLOOK"}]
    rows = [_row("g@b.co", reason="no_v2_pass"), _row("back@x.com", reason="no_v2_pass"), _row("o@x.com", reason="failed_v2")]
    v2 = {"g@b.co": {"verdict": "FAIL", "night": "2026-11-09"},
          "back@x.com": {"verdict": "FAIL", "night": "2026-11-09", "overridden": True},
          "o@x.com": {"verdict": "FAIL", "night": "2026-11-09"}}
    campaigns = [{"client": "Bettrdata", "campaign": "BD | Roundtable", "emails": ["g@b.co", "back@x.com", "o@x.com"]}]
    r = _run(inboxes, campaigns, rows, v2)
    assert r["list_marks_failed_v2"]["ok"] and r["list_marks_failed_v2"]["checked"] == 1      # only o@x.com counts
    assert r["held_back_not_sending"]["bad"] == ["o@x.com"]


# --- the daily digest (16 Nov 2026) ---------------------------------------------------------

def _cli(monkeypatch, day, rows=None):
    import gate_selftest as cli
    sent = []

    async def side(errors):
        w = _world()
        return w[0], w[1]
    monkeypatch.setattr(cli, "smartlead_side", side)
    monkeypatch.setattr(cli, "list_rows", lambda errors: rows if rows is not None else _world()[2])
    monkeypatch.setattr(cli, "mongo_side", lambda errors: (_world()[3], {**HB, "last_run": "2026-11-16T05:30:00+00:00"}, None))
    monkeypatch.setattr(cli, "tracker_names", lambda errors: {"good@x.com"})
    monkeypatch.setattr(cli.usable, "require_v2_pass", lambda today: False)
    monkeypatch.setattr(cli.notify, "post_with_thread", lambda token, ch, head, thread: sent.append(head) or "ts")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")

    class Day(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, day, 11, 30, tzinfo=tz)
    monkeypatch.setattr(cli, "datetime", Day)
    monkeypatch.setattr("sys.argv", ["gate_selftest.py", "--post"])
    return cli, sent


def test_with_the_digest_on_all_agree_is_silent_even_on_monday(monkeypatch):
    from smartlead import digest as dg
    cli, sent = _cli(monkeypatch, 16)                                   # a Monday
    got = []
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None: got.append((src, asks, done, detail)) or True)
    assert cli.main() == 0 and sent == []
    (src, asks, done, detail), = got
    assert src == "gate-selftest" and asks == [] and done == []          # an empty list clears yesterday's lines
    assert detail[0].startswith("✅ Gate self-test 16 Nov: all 6 checks agree")


def test_with_the_digest_on_a_disagreeing_check_is_one_broken_line_with_its_step(monkeypatch):
    from smartlead import digest as dg
    cli, sent = _cli(monkeypatch, 17, rows=[_row("tag@x.com", "FREE", "")] + _world()[2][1:])
    got = []
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None: got.append(asks) or True)
    assert cli.main() == 0 and sent == []
    (a,), = got
    assert a.key == "gate-selftest:list_marks_tagged" and a.kind == "broken" and not a.urgent
    assert a.text == ("Gate self-test: 1 of 2 inboxes tagged DO NOT USE are not marked do_not_use on the list, so "
                      "Campaign Desk and gtm would not hold them back for the tag.")
    assert a.step == gs.STEPS["list_marks_tagged"]


def test_the_old_post_goes_out_when_the_digest_is_not_written_or_off(monkeypatch):
    from smartlead import digest as dg
    cli, sent = _cli(monkeypatch, 17, rows=[_row("tag@x.com", "FREE", "")] + _world()[2][1:])
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(dg, "report", lambda *a, **k: False)            # Mongo down
    assert cli.main() == 0 and len(sent) == 1 and sent[0][0].startswith("⚠️ Gate self-test 17 Nov: 1 of 6")
    monkeypatch.setenv("DIGEST_MODE", "off")
    monkeypatch.setattr(dg, "report", lambda *a, **k: (_ for _ in ()).throw(AssertionError("digest is off")))
    assert cli.main() == 0 and len(sent) == 2


def test_a_check_that_could_not_run_says_so_and_errors_alone_still_make_a_line():
    res = list(_run(*_world(), errors=["Smartlead PRECISE_LEADS: HTTP 429"]).values())
    asks = gs.digest_asks(res, ["Smartlead PRECISE_LEADS: HTTP 429"])
    assert [a.key for a in asks] == ["gate-selftest:list_marks_tagged", "gate-selftest:held_back_not_sending"]
    assert all(a.step == gs.NOT_READ_STEP and a.kind == "broken" for a in asks)
    ok = [{"name": "x", "ok": True, "says": "fine"}]
    assert [a.key for a in gs.digest_asks(ok, ["the list: boom"])] == ["gate-selftest:read"]
    assert gs.digest_asks(ok, []) == []


def test_held_back_inboxes_still_sending_is_an_ask_for_a_person():
    """Lead review 16 Nov: the check works; a person must take the inboxes out, so it is an ask, not 'broken'."""
    asks = gs.digest_asks([{"name": "held_back_not_sending", "ok": False, "says": "2 held-back inboxes still in sending campaigns"},
                           {"name": "list_marks_tagged", "ok": False, "says": "1 tagged inbox not marked"}])
    assert [(a.key, a.kind) for a in asks] == [("gate-selftest:held_back_not_sending", "ask"),
                                               ("gate-selftest:list_marks_tagged", "broken")]
