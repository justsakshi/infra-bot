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


def test_needs_you_names_what_a_person_must_do_and_is_empty_when_all_is_well():
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
    assert items[2]["detail"] == ["• Melior · Melior Legal: 1 of 2 inboxes can send (1 taken out by the weekly job)"]
    assert items[3]["text"] == "2 inboxes in live campaigns are disconnected in Smartlead. Reconnect them"
    assert items[3]["detail"] == ["• Bettrdata: off1@b.co, off2@b.co"]                      # bench@b.co is not a job
    head, parts = st.format_post(st.states(rows, T), [], items, T)
    assert ("*Not connected and in no sending campaign (the renewal review asks about these at their bill)*\n"
            "• Bettrdata: 1 inbox on b.co") in parts
    assert items[4]["text"] == ("1 inbox is sending with no weekly test result in the last 10 days. Download this "
                                "week's test result files")
    # before the v2 pass is required, sending without one is not a job for a person
    assert "sending_unproven" not in [i["key"] for i in st.needs_you(st.states(rows, T), T, require_pass=False)]


def test_a_campaign_with_no_usable_inbox_is_named():
    rows = [_row("a@x.com", "Camp Z", "ACTIVE", tags=[AUTO]), _row("b@x.com", "Camp Z", "ACTIVE", connection_ok=False)]
    items = {i["key"]: i for i in st.needs_you(st.states(rows, T), T, require_pass=False)}
    assert items["campaign_short"]["detail"] == [
        "• Precise Leads · Camp Z: 0 of 2 inboxes can send (1 disconnected, 1 taken out by the weekly job)"]


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
