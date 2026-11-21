"""One status per inbox, its all-time history and the daily "needs you" post (smartlead/inbox_status.py)."""
from datetime import date

from smartlead import inbox_status as st

T = date(2026, 11, 11)
AUTO = "DO NOT USE - failed v2 (auto)"


def _row(email, campaign="N/A (No active campaign)", status="N/A", client="PRECISE_LEADS", sub="Precise Leads", **kw):
    base = {"email": email, "client": client, "sub_client": sub, "campaign_name": campaign, "campaign_status": status,
            "connection_ok": True, "availability": "BUSY", "busy_reason": "no_capacity", "tags": [], "v2_status": "",
            "v2_date": "", "warmup_state": "ready", "provider": "Outlook", "account_id": "1"}
    return {**base, **kw}


def _states(rows):
    return {r["email"]: r for r in st.states(rows, T)}


def test_every_inbox_gets_exactly_one_state_first_match_wins():
    rows = [
        _row("person@x.com", "Camp A", "ACTIVE", tags=["ScaledMail", "DO NOT USE - Google spam (Nov 2026)"], v2_status="FAIL"),
        _row("pulled@x.com", tags=[AUTO], v2_status="FAIL", v2_date="2026-11-09"),
        _row("failed@x.com", "Camp A", "ACTIVE", v2_status="FAIL", v2_date="2026-11-05"),
        _row("off@x.com", "Camp A", "ACTIVE", connection_ok=False),
        _row("proven@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09"),
        _row("proven@x.com", "Camp B", "PAUSED", v2_status="PASS", v2_date="2026-11-09"),
        _row("old@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-10-20"),     # 22 days: not proof
        _row("watch@x.com", "Camp A", "ACTIVE", v2_status="WATCH", v2_date="2026-11-09"),
        _row("free@x.com", availability="FREE", busy_reason=""),
        _row("warm@x.com", warmup_state="warming"),
        _row("idle@x.com"),
        _row("test@x.com", "DT PL #1", "ACTIVE"),                 # a placement-test campaign is not sending
        _row("paused@x.com", "Camp B", "PAUSED"),
    ]
    got = _states(rows)
    assert {e: r["state"] for e, r in got.items()} == {
        "person@x.com": "do_not_use", "pulled@x.com": "pulled", "failed@x.com": "failed", "off@x.com": "disconnected",
        "proven@x.com": "sending", "old@x.com": "sending_unproven", "watch@x.com": "sending_unproven",
        "free@x.com": "free", "warm@x.com": "warming", "idle@x.com": "idle", "test@x.com": "idle", "paused@x.com": "idle"}
    assert got["proven@x.com"]["campaigns"] == ["Camp A"] and got["proven@x.com"]["client"] == "Precise Leads"
    assert got["failed@x.com"]["domain"] == "x.com" and got["failed@x.com"]["date"] == "2026-11-11"
    assert len(got) == 12                                       # two rows of one inbox are one record


def test_no_client_is_report_only_since_16_nov():
    assert st.REPORT_ONLY == {}


def test_needs_you_names_what_a_person_must_do_and_is_empty_when_all_is_well(monkeypatch):
    monkeypatch.setattr(st, "REPORT_ONLY", {"melior": "Melior"})     # the stage itself, as it worked to 16 Nov
    good = [_row(f"g{i}@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09") for i in range(4)]
    assert st.needs_you(st.states(good, T), T, require_pass=True) == []
    rows = good + [
        _row("failed@x.com", "Camp A", "ACTIVE", v2_status="FAIL", v2_date="2026-11-05"),
        _row("tagged@x.com", "Camp A", "ACTIVE", tags=["DO NOT USE - x"]),
        _row("m1@m.com", "Melior Legal", "ACTIVE", sub="Melior", tags=[AUTO]),
        _row("m2@m.com", "Melior Legal", "ACTIVE", sub="Melior", v2_status="PASS", v2_date="2026-11-09"),
        _row("off1@b.co", "BD Roundtable", "ACTIVE", client="BETTRDATA", sub="Bettrdata", connection_ok=False),
        _row("off2@b.co", "BD Roundtable", "ACTIVE", client="BETTRDATA", sub="Bettrdata", connection_ok=False),
        _row("ok@b.co", "BD Roundtable", "ACTIVE", client="BETTRDATA", sub="Bettrdata", v2_status="PASS", v2_date="2026-11-09"),
        _row("ok2@b.co", "BD Roundtable", "ACTIVE", client="BETTRDATA", sub="Bettrdata", v2_status="PASS", v2_date="2026-11-09"),
        _row("ok3@b.co", "BD Roundtable", "ACTIVE", client="BETTRDATA", sub="Bettrdata", v2_status="PASS", v2_date="2026-11-09"),
        _row("bench@b.co", client="BETTRDATA", sub="Bettrdata", connection_ok=False),      # idle: listed, not a job
        _row("u@x.com", "Camp C", "ACTIVE"),
    ]
    items = st.needs_you(st.states(rows, T), T, require_pass=True)
    assert [i["key"] for i in items] == ["held_but_sending", "held_but_sending_report_only", "campaign_short",
                                         "disconnected", "sending_unproven"]
    assert items[0]["text"] == ("2 inboxes that failed the weekly test or are tagged DO NOT USE are still in 1 live "
                                "campaign. Take them out, or the weekly job takes out the failed ones when this week's "
                                "results are in")
    assert items[0]["detail"] == ["• Precise Leads · Camp A: tagged@x.com (tagged DO NOT USE); failed@x.com (failed the "
                                  "weekly test)"]
    assert items[1]["text"] == ("1 inbox of Melior that failed the weekly test or is tagged DO NOT USE still sends in 1 "
                                "live campaign. The weekly job does not act on Melior yet while it is being proven, so "
                                "it stays until someone takes it out")
    assert "Ryan" not in items[1]["text"]                         # a safety stage, nobody's call (11 Nov 2026)
    assert items[2]["text"] == ("1 live campaign has lost half or more of its inboxes. Add inboxes that passed the "
                                "weekly test, or lower its daily sending")
    assert items[2]["detail"] == ["• Melior · Melior Legal: 1 of 2 inboxes can send (1 taken out by the weekly job)",
                                  "• Melior has no free inbox with a weekly-test pass in the last 10 days, so lower "
                                  "the daily sending"]
    assert items[3]["text"] == "2 inboxes in live campaigns are disconnected in Smartlead. Reconnect them"
    assert items[3]["detail"] == ["• Bettrdata · BD Roundtable: off1@b.co, off2@b.co"]                    # bench@b.co is not a job
    head, parts = st.format_post(st.states(rows, T), [], items, T)
    assert ("*Not connected and in no sending campaign (the renewal review asks about these at their bill)*\n"
            "• Bettrdata: 1 inbox on b.co") in parts
    assert items[4]["text"] == ("1 inbox is sending with no weekly test result in the last 10 days and is on no test "
                                "campaign, so no weekly test will check it")
    # before the v2 pass is required, sending without one is not a job for a person
    assert "sending_unproven" not in [i["key"] for i in st.needs_you(st.states(rows, T), T, require_pass=False)]


def test_a_campaign_with_no_usable_inbox_is_named():
    rows = [_row("a@x.com", "Camp Z", "ACTIVE", tags=[AUTO]), _row("b@x.com", "Camp Z", "ACTIVE", connection_ok=False)]
    items = {i["key"]: i for i in st.needs_you(st.states(rows, T), T, require_pass=False)}
    assert items["campaign_short"]["detail"] == [
        "• Precise Leads · Camp Z: 0 of 2 inboxes can send (1 disconnected, 1 taken out by the weekly job)",
        "• Precise Leads has no free inbox with a weekly-test pass in the last 10 days, so lower the daily sending"]


def test_history_records_each_change_once_and_gone_only_on_a_full_read():
    recs = st.states([_row("a@x.com", availability="FREE", busy_reason=""), _row("b@x.com")], T)
    previous = {"a@x.com": {"email": "a@x.com", "state": "idle", "client": "Precise Leads"},
                "b@x.com": {"email": "b@x.com", "state": "idle"},
                "left@x.com": {"email": "left@x.com", "state": "free", "client": "Precise Leads"},
                "old@x.com": {"email": "old@x.com", "state": "gone"}}
    ch = st.changes_since(previous, recs, T, complete=True)
    assert [(c["email"], c["from"], c["to"]) for c in ch] == [("a@x.com", "idle", "free"), ("left@x.com", "free", "gone")]
    assert ch[0]["date"] == "2026-11-11"
    # an account that could not be read must not make its inboxes look gone
    assert [(c["email"], c["to"]) for c in st.changes_since(previous, recs, T, complete=False)] == [("a@x.com", "free")]
    # first sight: from is None
    assert st.changes_since({}, recs, T, True)[0]["from"] is None


class FakeCol:
    def __init__(self, docs=()):
        self.docs = {d.get("email") or d.get("_id"): dict(d) for d in docs}
        self.writes = 0

    def find(self, q=None, fields=None):
        return [dict(d) for d in self.docs.values()]

    def find_one(self, q):
        return self.docs.get(q.get("_id"))

    def replace_one(self, q, doc, upsert=False):
        self.docs[q["_id"]] = dict(doc)

    def bulk_write(self, ops, ordered=False):
        for op in ops:
            key = tuple(sorted(op._filter.items()))
            k = op._filter.get("email") if len(op._filter) == 1 else key
            doc = self.docs.get(k)
            if doc is None:
                if not op._upsert:
                    continue
                doc = dict(op._filter)
                doc.update(op._doc.get("$setOnInsert") or {})
                self.docs[k] = doc
            doc.update(op._doc.get("$set") or {})
            self.writes += 1


class FakeDb(dict):
    def __getitem__(self, name):
        if name not in self:
            self[name] = FakeCol()
        return dict.__getitem__(self, name)


def test_save_keeps_since_until_the_state_changes_and_history_is_never_rewritten():
    db = FakeDb()
    day1, day2, day3 = date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13)
    r1 = st.save(st.states([_row("a@x.com"), _row("b@x.com")], day1), day1, True, db)
    assert r1["saved"] == 2 and len(r1["changes"]) == 2 and r1["error"] == ""
    assert db["inbox_status"].docs["a@x.com"]["since"] == "2026-11-11"
    assert db["inbox_status"].docs["a@x.com"]["first_seen"] == "2026-11-11"
    r2 = st.save(st.states([_row("a@x.com"), _row("b@x.com", availability="FREE", busy_reason="")], day2), day2, True, db)
    assert [(c["email"], c["from"], c["to"]) for c in r2["changes"]] == [("b@x.com", "idle", "free")]
    assert db["inbox_status"].docs["a@x.com"]["since"] == "2026-11-11"            # unchanged state keeps its start
    assert db["inbox_status"].docs["b@x.com"]["since"] == "2026-11-12"
    r3 = st.save(st.states([_row("a@x.com")], day3), day3, True, db)               # b left Smartlead
    assert [(c["email"], c["to"]) for c in r3["changes"]] == [("b@x.com", "gone")]
    assert db["inbox_status"].docs["b@x.com"]["state"] == "gone" and db["inbox_status"].docs["b@x.com"]["first_seen"] == "2026-11-11"
    assert len(db["inbox_status_history"].docs) == 4                               # 2 first sights, 1 change, 1 gone


def test_save_never_raises():
    class Broken:
        def __getitem__(self, name):
            raise RuntimeError("server selection timed out")
    res = st.save(st.states([_row("a@x.com")], T), T, True, Broken())
    assert res == {"changes": [], "saved": 0, "error": "server selection timed out"}


def test_post_says_nothing_needs_you_or_lists_it_and_goes_out_once_a_day(monkeypatch):
    sent = []
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.delenv("FLEET_STATUS_POST", raising=False)
    from smartlead import notify
    monkeypatch.setattr(notify, "post_with_thread", lambda token, ch, head, parts: sent.append((ch, head, parts)) or "ts")
    db = FakeDb()
    good = [_row(f"g{i}@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09") for i in range(3)]
    res = st.run(good, T, True, post=True, db=db)
    assert res["posted"] and res["needs_you"] == [] and len(sent) == 1
    assert sent[0][1] == ["✅ Inboxes, 11 Nov: nothing needs you",
                          "3 inboxes in Smartlead, 3 in live campaigns. The list is in the thread."]
    assert not st.run(good, T, True, post=True, db=db)["posted"] and len(sent) == 1          # same day: once
    nxt = date(2026, 11, 12)
    bad = good[:2] + [_row("g2@x.com", "Camp A", "ACTIVE", v2_status="FAIL", v2_date="2026-11-11")]
    res = st.run(bad, nxt, True, post=True, db=db)
    assert res["posted"] and res["needs_you"] == ["held_but_sending"]
    head = sent[1][1]
    assert head[0] == "⚠️ Inboxes, 12 Nov: 1 thing needs you"
    assert head[1].startswith("1. 1 inbox that failed the weekly test or is tagged DO NOT USE is still in 1 live campaign.")
    assert head[2] == "3 inboxes in Smartlead, 3 in live campaigns. The list is in the thread."
    assert len(head) == 3                                         # the counts and changes are in the thread
    assert ("*Every inbox, by where it stands*\n• sending, passed the weekly test: 2\n• failed the weekly test: 1"
            in sent[1][2])
    assert any("• 1: sending, passed the weekly test → failed the weekly test" in p for p in sent[1][2])
    assert any(p.startswith("*By client*") for p in sent[1][2])
    monkeypatch.setenv("FLEET_STATUS_POST", "off")
    assert not st.run(bad, date(2026, 11, 13), True, post=True, db=db)["posted"]


def test_a_google_inbox_that_failed_v2_is_not_held_back_and_nothing_asks_to_take_it_out():
    """13 Nov 2026: the daily post called 24 BettrData Google inboxes "must not send" after they were put back on
    purpose, contradicting the v2 job (Google inboxes report-only, PR #69). One rule now: usable.fail_set_aside."""
    rows = [_row(f"g{i}@b.co", "BD Roundtable", "ACTIVE", provider="Gmail", v2_status="FAIL", v2_date="2026-11-09")
            for i in range(3)]
    rows += [_row("o@b.co", "BD Roundtable", "ACTIVE", v2_status="PASS", v2_date="2026-11-09")]
    recs = _states(rows)
    assert {e: r["state"] for e, r in recs.items()} == {"g0@b.co": "set_aside", "g1@b.co": "set_aside",
                                                         "g2@b.co": "set_aside", "o@b.co": "sending"}
    assert recs["g0@b.co"]["set_aside"] == "the v2 test is not used for Google inboxes"
    assert st.needs_you(list(recs.values()), T, require_pass=True) == []        # not held, not short, not unproven


def test_a_fail_a_person_overrode_is_set_aside_but_an_outlook_fail_still_holds():
    rows = [_row("back@x.com", "Camp A", "ACTIVE", v2_status="FAIL", v2_date="2026-11-09",
                 v2_set_aside="a person put it back after this result"),
            _row("bad@x.com", "Camp A", "ACTIVE", v2_status="FAIL", v2_date="2026-11-09", v2_set_aside=""),
            _row("ok@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09"),
            _row("ok2@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09")]
    recs = _states(rows)
    assert recs["back@x.com"]["state"] == "set_aside" and recs["bad@x.com"]["state"] == "failed"
    items = st.needs_you(list(recs.values()), T, require_pass=True)
    assert [i["key"] for i in items] == ["held_but_sending"] and "bad@x.com" in items[0]["detail"][0]
    assert "back@x.com" not in items[0]["detail"][0]


def test_with_the_digest_on_the_day_goes_to_the_digest_with_steps_not_to_slack(monkeypatch):
    from smartlead import digest as dg
    from smartlead import notify
    sent, got = [], []
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(notify, "post_with_thread", lambda *a: sent.append(a) or "ts")
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None, numbers=None: got.append((src, asks, detail)) or True)
    rows = [_row(f"g{i}@x.com", "Camp A", "ACTIVE", v2_status="PASS", v2_date="2026-11-09") for i in range(2)]
    rows += [_row("off@x.com", "Camp A", "ACTIVE", connection_ok=False),
             _row("off2@x.com", "Camp A", "ACTIVE", connection_ok=False)]
    res = st.run(rows, T, True, post=True, db=FakeDb())
    assert res["digest"] and sent == []
    (src, asks, detail), = got
    assert src == "inbox-status"
    assert [(a.key, a.urgent) for a in asks] == [("inbox-status:campaign_short", True), ("inbox-status:disconnected", False)]
    assert asks[0].text == ("1 live campaign has lost half or more of its inboxes, so the rest carry its sending alone "
                            "and may go to spam or stall.")
    assert asks[1].text == ("2 inboxes in live campaigns are disconnected in Smartlead, so their share of those "
                            "campaigns is not being sent.")
    assert asks[1].step.startswith("In Smartlead, Email Accounts, filter Disconnected and reconnect")
    assert detail[0].startswith("⚠️ Inboxes, 11 Nov: 2 things need you")
    monkeypatch.setattr(dg, "report", lambda *a, **k: False)              # Mongo down: the old post goes out
    assert st.run(rows, T, True, post=True, db=FakeDb())["posted"] and len(sent) == 1


def test_a_passed_inbox_with_high_bounce_is_info_never_an_ask_2026_11_16():
    """Manveen, 16 Nov 2026: high bounce alone does not mean do not use. The list marks such an inbox FREE with the
    note "bounces come from the lead list" (smartlead/usable.py); the daily post names it, the digest asks nothing."""
    rows = [_row("idle@x.com", availability="FREE", busy_reason="", v2_status="PASS", v2_date="2026-11-09",
                 use_notes="bounces come from the lead list"),
            _row("live@x.com", "Camp A", "ACTIVE", availability="FREE", busy_reason="", v2_status="PASS",
                 v2_date="2026-11-09", use_notes="bounces come from the lead list")]
    recs = st.states(rows, T)
    assert {r["email"]: r["state"] for r in recs} == {"idle@x.com": "free", "live@x.com": "sending"}
    for require_pass in (True, False):
        assert st.digest_asks(st.needs_you(recs, T, require_pass=require_pass)) == []


def test_every_digest_ask_carries_its_list_and_no_step_points_at_a_thread():
    """17 Nov 2026, Manveen on the 12:29 urgent post: "This does not even tell us which campaign do we need to add
    inboxes". The ask carries the campaigns, and the step names where the free tested inboxes are."""
    rows = [_row("a@b.co", "Camp One", "ACTIVE", client="BETTRDATA", sub="Bettrdata", v2_status="PASS",
                 v2_date="2026-11-09"),
            _row("b@b.co", "Camp One", "ACTIVE", client="BETTRDATA", sub="Bettrdata", connection_ok=False),
            _row("c@b.co", "Camp One", "ACTIVE", client="BETTRDATA", sub="Bettrdata", connection_ok=False),
            _row("f1@b.co", client="BETTRDATA", sub="Bettrdata", availability="FREE", busy_reason="",
                 v2_status="PASS", v2_date="2026-11-09"),
            _row("f2@b.co", client="BETTRDATA", sub="Bettrdata", availability="FREE", busy_reason=""),   # untested
            _row("p1@x.com", "Camp P", "ACTIVE", connection_ok=False),
            _row("p2@x.com", "Camp P", "ACTIVE", connection_ok=False)]
    items = st.needs_you(st.states(rows, T), T, require_pass=False)
    asks = {a.key: a for a in st.digest_asks(items)}
    short = asks["inbox-status:campaign_short"]
    assert short.urgent
    assert "• Bettrdata · Camp One: 1 of 3 inboxes can send (2 disconnected)" in short.detail
    assert "• Precise Leads · Camp P: 0 of 2 inboxes can send (2 disconnected)" in short.detail
    assert ("• Bettrdata has 1 free inbox with a weekly-test pass in the last 10 days (All Inboxes tab, "
            "availability FREE)") in short.detail
    assert ("• Precise Leads has no free inbox with a weekly-test pass in the last 10 days, so lower the daily "
            "sending") in short.detail
    assert asks["inbox-status:disconnected"].detail
    for a in asks.values():
        assert a.detail, a.key
        assert "thread" not in a.step, a.step
    from smartlead import digest as dg
    text = dg.urgent_text("inbox-status", short.doc("inbox-status"))
    assert "Camp One" in text and "Camp P" in text and "thread" not in text


def test_an_untested_inbox_already_on_a_test_campaign_is_not_an_ask():
    """19 Nov 2026: 13 inboxes sending in a live campaign and already on a DT test campaign (retest booked) were
    asked about every day, with a step to add them to the deliverability sheet. The next weekly test covers them."""
    rows = [_row("w1@x.com", "Camp C", "ACTIVE"), _row("w1@x.com", "DT PL #1", "ACTIVE"),
            _row("w2@x.com", "Camp C", "ACTIVE"), _row("w2@x.com", "DT PL #1", "PAUSED"),
            _row("lone@y.com", "Camp C", "ACTIVE")]
    recs = st.states(rows, T)
    assert {r["email"]: r["in_test"] for r in recs} == {"lone@y.com": False, "w1@x.com": True, "w2@x.com": True}
    items = st.needs_you(recs, T, require_pass=True)
    (it,) = [i for i in items if i["key"] == "sending_unproven"]
    assert it["text"].startswith("1 inbox is sending") and it["detail"] == ["• Precise Leads · y.com: 1 inbox, in Camp C; never tested"]
    (ask,) = [a for a in st.digest_asks(items) if a.key == "inbox-status:sending_unproven"]
    assert "dt_senders" not in ask.step and "deliverability sheet" in ask.step
    head, parts = st.format_post(recs, [], items, T)
    # 20 Nov 2026: they are one info line, never a thing to do (not counted in the headline)
    assert head[0].endswith("1 thing needs you")
    # no assumed Saturday (21 Nov 2026): without the tests' schedules, no date at all
    assert "For info: 2 inboxes in a weekly test have no result yet; the next run checks them." in head
    (info,) = [a for a in st.digest_asks(items) if a.key == "inbox-status:sending_test_short"]
    assert info.kind == "info" and info.step == "" and info.doc("inbox-status")["kind"] == "info"


def test_the_inbox_list_hands_the_digest_its_numbers_per_client():
    rows = [_row("a@x.com", availability="FREE", busy_reason=""),
            _row("b@x.com", busy_reason="no_v2_pass"),
            _row("c@x.com", busy_reason="do_not_use", tags=["DO NOT USE - x"]),
            _row("d@m.com", sub="Melior", busy_reason="low_rep")]
    assert st.numbers(st.states(rows, T)) == {"Melior": {"Recovering": 1},
                                              "Precise Leads": {"Safe to send": 1, "Not proven yet": 1, "Do not use": 1}}


def _unproven_on_test(email, test="DT PL #1"):
    return [_row(email, "Camp C", "ACTIVE"), _row(email, test, "PAUSED"),
            _row(email, "PL Deliverability Test - all PL inboxes", "PAUSED")]      # the copy check: not named


def test_untested_inboxes_are_split_by_why_they_have_no_result():
    """20 Nov 2026 ("this is still not fixed"): 25 inboxes already in an active weekly test, whose last runs came back
    short, were told to go on the deliverability sheet. Now: on no test campaign = the sheet step; on a test campaign
    its weekly test leaves out = add them to that test; in the weekly test with short runs = info, no step."""
    rows = (_unproven_on_test("s1@short.example") + _unproven_on_test("s2@short.example")
            + _unproven_on_test("gap@left.example", "DT PL #2") + [_row("lone@none.example", "Camp C", "ACTIVE")])
    v2 = [{"email": "s1@short.example", "night": "2026-11-09", "verdict": "INCOMPLETE",
           "google": {"sent": 1}, "microsoft": {"sent": 1}},
          {"email": "s1@short.example", "night": "2026-11-07", "verdict": "INCOMPLETE",
           "google": {"sent": 1}, "microsoft": {"sent": 1}},
          {"email": "s1@short.example", "night": "2026-10-20", "verdict": "INCOMPLETE",      # too old to count
           "google": {"sent": 1}, "microsoft": {"sent": 1}},
          {"email": "s2@short.example", "night": "2026-11-09", "verdict": "INCOMPLETE",
           "google": {"sent": 1}, "microsoft": {"sent": 4}},
          {"email": "s2@short.example", "night": "2026-10-28", "verdict": "PASS"}]
    recs = st.add_test_facts(st.states(rows, T), {"gap@left.example"}, v2, T)
    by = {r["email"]: r for r in recs}
    assert by["gap@left.example"]["weekly_test"] == "missing" and by["s1@short.example"]["weekly_test"] == "in"
    assert [x["night"] for x in by["s1@short.example"]["short_runs"]] == ["2026-11-07", "2026-11-09"]
    items = {i["key"]: i for i in st.needs_you(recs, T, require_pass=True)}
    assert set(items) == {"sending_unproven", "sending_not_in_test", "sending_test_short"}

    assert items["sending_unproven"]["detail"] == ["• Precise Leads · none.example: 1 inbox, in Camp C; never tested"]
    gap = items["sending_not_in_test"]
    assert gap["text"].startswith("1 inbox sending with no weekly test result in the last 10 days sits on a test campaign")
    assert gap["detail"] == ["• Precise Leads · left.example: 1 inbox, on DT PL #2"]

    info = items["sending_test_short"]
    assert info["info"] is True
    assert info["text"] == ("2 inboxes in a weekly test have no result in the last 10 days because their last test "
                            "runs sent too few emails to judge")          # no schedule known: no "next run" date
    assert info["detail"] == ["• Precise Leads · short.example: 2 inboxes, in the weekly test of DT PL #1; last runs "
                              "7 Nov and 9 Nov sent 1 email per provider (4 needed)"]

    asks = {a.key: a for a in st.digest_asks(list(items.values()))}
    assert "deliverability sheet" in asks["inbox-status:sending_unproven"].step
    left = asks["inbox-status:sending_not_in_test"].step
    assert "Smart Delivery" in left and "add those inboxes" in left and "deliverability sheet" not in left
    short = asks["inbox-status:sending_test_short"]
    assert short.kind == "info" and short.step == "" and "deliverability" not in short.text


def test_without_a_coverage_record_an_inbox_on_a_test_campaign_is_never_told_to_go_on_the_sheet():
    recs = st.add_test_facts(st.states(_unproven_on_test("a@x.example"), T), None, [], T)
    assert recs[0]["weekly_test"] is None
    (it,) = st.needs_you(recs, T, require_pass=True)
    assert it["key"] == "sending_test_short" and it["info"]
    assert it["detail"] == ["• Precise Leads · x.example: 1 inbox, on DT PL #1; the next weekly run checks them"]


# --- 21 Nov 2026 (Manveen: "i think this still is not readable") -------------------------------------------------
# The weekly tests' own schedules decide: only a sender of an ACTIVE scheduled test is "in the weekly test", the next
# run is that test's nextRunAt, and on the test day the line says today's tests are being read.

SAT = date(2026, 11, 21)
WEEKLY = [{"name": "PL weekly 1", "campaign": "DT PL #1", "next_run": "2026-11-28T00:30:00.000Z", "weekdays": [6],
           "time": "06:00", "senders": ["s1@short.example", "s2@short.example"]},
          {"name": "PL weekly 2", "campaign": "DT PL #2", "next_run": "2026-11-28T05:30:00.000Z", "weekdays": [6],
           "time": "11:00", "senders": []}]


def _items(rows, today, weekly, v2=()):
    recs = st.add_test_facts(st.states(rows, today), set(), list(v2), today, weekly=weekly)
    return {i["key"]: i for i in st.needs_you(recs, today, require_pass=True)}, {r["email"]: r for r in recs}


def test_a_campaign_with_only_a_one_off_test_is_an_ask_to_create_a_weekly_test():
    """21 Nov 2026: 24 inboxes on a campaign with only a finished one-off test were told as "in the weekly test"."""
    rows = (_unproven_on_test("a@oneoff.example", "Client | One-off deliverability test")
            + _unproven_on_test("b@oneoff.example", "Client | One-off deliverability test"))
    items, by = _items(rows, SAT, WEEKLY)
    assert by["a@oneoff.example"]["weekly_test"] == "missing" and by["a@oneoff.example"]["no_weekly_test"]
    assert "sending_test_short" not in items and "sending_not_in_test" not in items
    it = items["test_campaign_no_weekly_test"]
    assert it["detail"] == ["• Precise Leads · oneoff.example: 2 inboxes, on Client | One-off deliverability test, "
                            "PL Deliverability Test - all PL inboxes"]
    (ask,) = [a for a in st.digest_asks(list(items.values())) if a.key == "inbox-status:test_campaign_no_weekly_test"]
    assert ask.kind == "ask" and ask.text.startswith("2 inboxes are in no weekly test")
    assert ask.step == ("In Smart Delivery, create a weekly test on each campaign listed now (Saturday, after 11:00 "
                        "IST, 4 emails per provider, text-only ticked); its first run is the next such Saturday.")


def test_one_campaign_named_in_the_step():
    rows = [_row("a@oneoff.example", "Camp C", "ACTIVE"), _row("a@oneoff.example", "Vendor test DT", "PAUSED")]
    items, _ = _items(rows, SAT, WEEKLY)
    assert items["test_campaign_no_weekly_test"]["step"].startswith(
        "In Smart Delivery, create a weekly test on campaign Vendor test DT now (Saturday, after 11:00 IST")


def test_on_the_test_day_the_line_says_results_come_tomorrow_never_a_run_of_today():
    """21 Nov 2026 at 11:00 IST the digest said "next run Sat 21 Nov" after those runs had gone out."""
    rows = _unproven_on_test("s1@short.example") + _unproven_on_test("s2@short.example")
    v2 = [{"email": "s1@short.example", "night": "2026-11-14", "verdict": "INCOMPLETE", "google": {"sent": 1},
           "microsoft": {"sent": 1}}]
    for weekly in (WEEKLY,                                                          # read after the run: next week
                   [{**WEEKLY[0], "next_run": "2026-11-21T00:30:00.000Z"}]):        # read before it: today
        items, by = _items(rows, SAT, weekly, v2)
        assert by["s1@short.example"]["runs_today"]
        text = items["sending_test_short"]["text"]
        assert text == ("Today's weekly tests are being read (2 inboxes with no recent pass are in them); results in "
                        "tomorrow's digest")
        assert "21 Nov" not in text and "next run" not in text
        assert all("next run" not in x for x in items["sending_test_short"]["detail"])


def test_other_days_name_the_next_run_from_the_tests_schedule():
    rows = _unproven_on_test("s1@short.example") + _unproven_on_test("s2@short.example")
    v2 = [{"email": "s1@short.example", "night": "2026-11-21", "verdict": "INCOMPLETE", "google": {"sent": 1},
           "microsoft": {"sent": 1}},
          {"email": "s2@short.example", "night": "2026-11-21", "verdict": "INCOMPLETE", "google": {"sent": 2},
           "microsoft": {"sent": 2}}]
    tue = date(2026, 11, 24)
    items, by = _items(rows, tue, WEEKLY, v2)
    assert not by["s1@short.example"]["runs_today"] and by["s1@short.example"]["next_run"] == "2026-11-28"
    assert items["sending_test_short"]["text"] == (
        "2 inboxes in a weekly test have no result in the last 10 days because their last test runs sent too few "
        "emails to judge; next run Sat 28 Nov")
    # a schedule that says today (Saturday) but a run of today: never printed as "next run"
    items, _ = _items(rows, SAT, [{**WEEKLY[0], "next_run": "2026-11-21T00:30:00.000Z"}], ())
    assert "next run" not in items["sending_test_short"]["text"]


def test_weekly_tests_are_read_from_a_fresh_coverage_record_only():
    class Db:
        def __init__(self, docs):
            self.docs = docs

        def __getitem__(self, k):
            return _Col(self.docs)
    fresh = {"_id": "2026-11-21", "weekly": {"PRECISE_LEADS": WEEKLY}}
    assert st.weekly_tests(Db([fresh]), SAT) == WEEKLY
    assert st.weekly_tests(Db([{"_id": "2026-11-18", "weekly": {"PRECISE_LEADS": WEEKLY}}]), SAT) is None   # old
    assert st.weekly_tests(Db([{"_id": "2026-11-21", "weekly": {}}]), SAT) is None          # nothing read
    assert st.weekly_tests(Db([{"_id": "2026-11-21"}]), SAT) is None                        # before 21 Nov


class _Col:
    def __init__(self, docs):
        self.docs = docs

    def find(self, q=None, proj=None):
        q = q or {}
        out = self.docs
        if "_id" in q:
            out = [d for d in out if d["_id"] >= q["_id"]["$gte"]]
        if "email" in q:
            out = [d for d in out if d["email"] in q["email"]["$in"]]
        return _Cur(out)


class _Cur(list):
    def sort(self, key, way):
        return _Cur(sorted(self, key=lambda d: d[key], reverse=way < 0))

    def limit(self, n):
        return _Cur(self[:n])


def test_test_facts_reads_todays_coverage_only_when_every_account_was_read():
    cov = {"_id": "2026-11-11", "tests_read": {"A": True, "B": True},
           "clients": {"Precise Leads": {"missing": [{"email": "Gap@x.example"}]}}}
    db = {"test_coverage": _Col([cov]), "v2_verdicts": _Col([{"email": "a@x.example", "night": "2026-11-09"}])}
    missing, rows = st.test_facts(db, T, {"a@x.example"})
    assert missing == {"gap@x.example"} and rows == [{"email": "a@x.example", "night": "2026-11-09"}]
    db["test_coverage"] = _Col([{**cov, "tests_read": {"A": True, "B": False}}])
    assert st.test_facts(db, T, set())[0] is None
    db["test_coverage"] = _Col([{**cov, "_id": "2026-11-08"}])                # older than yesterday: not known
    assert st.test_facts(db, T, set())[0] is None
    assert st.test_facts({}, T, {"a@x.example"}) == (None, [])                 # a read that fails: nothing known
