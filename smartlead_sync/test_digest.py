"""The daily digest (smartlead/digest.py + digest_post.py): asks replace a job's previous ones and keep their
'since', done items are listed once, urgent asks post at once one time, nothing to say posts nothing."""
from datetime import datetime, timedelta, timezone

import digest_post as dp
from smartlead import digest as dg

T0 = datetime(2026, 11, 17, 5, 30, tzinfo=timezone.utc)        # Tue 17 Nov 11:00 IST


class Col:
    def __init__(self):
        self.docs = {}

    def find_one(self, q):
        return dict(self.docs[q["_id"]]) if q.get("_id") in self.docs else None

    def replace_one(self, q, doc, upsert=False):
        self.docs[q["_id"]] = dict(doc)

    def update_one(self, q, upd):
        self.docs[q["_id"]].update(upd["$set"])

    def delete_one(self, q):
        self.docs.pop(q["_id"], None)

    def find(self, q=None):
        q = q or {}
        return [dict(d) for d in self.docs.values()
                if all(d.get(k) == v for k, v in q.items() if not isinstance(v, dict))]


class DB(dict):
    def __getitem__(self, k):
        return self.setdefault(k, Col())


def test_asks_replace_the_jobs_previous_ones_and_keep_their_since():
    db, posted = DB(), []
    a1 = dg.Ask("7 inboxes in live campaigns are disconnected.", "Reconnect them in Smartlead.", key="disc")
    assert dg.report("inbox-status", [a1, dg.Ask("Two campaigns lost half.", key="half")], db=db, now=T0,
                     post_urgent=posted.append)
    later = T0 + timedelta(days=1)
    a1b = dg.Ask("6 inboxes in live campaigns are disconnected.", "Reconnect them in Smartlead.", key="disc")
    dg.report("inbox-status", [a1b], db=db, now=later)
    doc = db[dg.ASKS].find_one({"_id": "inbox-status"})
    assert [a["text"] for a in doc["asks"]] == ["6 inboxes in live campaigns are disconnected."]   # 'half' fixed
    assert doc["asks"][0]["since"] == T0.isoformat(timespec="seconds")
    assert posted == []


def test_the_default_key_ignores_numbers():
    assert dg.default_key("x", "7 inboxes are down") == dg.default_key("x", "6 inboxes are down")


def test_done_is_kept_once_and_urgent_posts_once():
    db, posted = DB(), []
    pay = dg.Ask("Zapmail declined the card for 12 Outlook pre-warmed (Bettrdata).", "Pay the invoice in Zapmail.",
                 key="bill:inv-1", urgent=True)
    dg.report("billing-watch", [pay], done=[dg.Done("Tagged 2 inboxes.", key="t1")], db=db, now=T0,
              post_urgent=lambda t: posted.append(t) or True)
    dg.report("billing-watch", [pay], done=[dg.Done("Tagged 2 inboxes.", key="t1")], db=db, now=T0,
              post_urgent=lambda t: posted.append(t) or True)
    assert len(posted) == 1 and "Pay the invoice in Zapmail." in posted[0] and f"<@{dg.OWNER}>" in posted[0]
    assert list(db[dg.DONE].docs) == ["t1"]


def test_mongo_down_makes_the_caller_post_the_old_way():
    class Down:
        def __getitem__(self, k):
            raise RuntimeError("no mongo")
    assert dg.report("blacklist", [dg.Ask("x")], db=Down(), now=T0) is False


def test_off_switch(monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "off")
    assert not dg.on()
    monkeypatch.delenv("DIGEST_MODE")
    assert dg.on()                        # on unless switched off


def _src(sid, asks, at=T0, detail=None):
    return {"_id": sid, "at": at.isoformat(), "asks": [dg.Ask(**{k: v for k, v in a.items() if k != "since"}).doc(sid)
                                                    | {"since": a.get("since", at.isoformat())} for a in asks], "detail": detail or []}


def test_the_digest_lists_asks_with_steps_done_items_and_broken_checks():
    sources = [_src("billing-watch", [{"text": "Zapmail declined the card.", "step": "Pay the invoice.",
                                       "urgent": True}]),
               _src("inbox-status", [{"text": "7 inboxes are disconnected.", "step": "Reconnect them.",
                                      "since": (T0 - timedelta(days=2)).isoformat()}],
                    detail=["full inbox list"]),
               _src("campaign-metrics", [{"text": "Campaign stats job failed (exit 2).", "kind": "broken"}])]
    done = [{"_id": "d1", "text": "Took 3 inboxes out of 2 live campaigns (failed the v2 test)."}]
    head, parts = dp.build(sources, done, [], T0 + timedelta(hours=1))
    text, thread = "\n".join(head), "\n".join(parts)
    # t409: neither ask has an approved sentence, so both are in the thread in the closest shape; the post counts them
    assert head[0] == "Inbox digest · Tue 17 Nov · 2 things to do"
    assert "2 more in the thread." in text and "Zapmail declined" not in text
    assert "1. 7 inboxes are disconnected. Reconnect them. (wording not approved yet)" in thread   # oldest first
    assert "2. Zapmail declined the card. Pay the invoice. (wording not approved yet)" in thread
    assert "Done by the bot since the last digest\n• Took 3 inboxes out of 2 live campaigns (failed the weekly test)." in thread
    assert "Campaign stats" not in text + thread                          # a job's failure is a dev-channel line
    assert "Campaign stats job failed (exit 2)." in _dev(sources, done, now=T0 + timedelta(hours=1))
    assert "full inbox list" not in thread               # a job's own report is no longer copied into the thread


def test_asks_of_a_job_silent_for_over_8_days_are_dropped_and_old_ones_say_when_checked():
    old = _src("kill-report", [{"text": "Kill 2 inboxes."}], at=T0 - timedelta(days=9))
    tue = _src("reply-monitor", [{"text": "Replies fell on one domain."}], at=T0 - timedelta(days=2))
    head, parts = dp.build([old, tue], [], [], T0)
    text = "\n".join(head + parts)
    assert "Kill 2 inboxes" not in text and "Replies fell on one domain." in text


def test_pc_and_render_checks():
    assert dp.pc_check({"last_run": (T0 - timedelta(hours=1)).isoformat()}, T0) == []
    stale = dp.pc_check({"last_run": (T0 - timedelta(hours=5)).isoformat()}, T0)
    assert stale[0]["kind"] == "broken" and "06:00 IST" in stale[0]["text"]
    assert dp.render_check(None, None) == [] and dp.render_check("abc1234def", {"error": "x"}) == []
    assert dp.render_check("abc1234def", {"behind": 0}) == []
    r = dp.render_check("abc1234def", {"behind": 6})
    assert "Render runs abc1234, 6 commits behind main" in r[0]["text"] and "Manual Deploy" in r[0]["step"]


def test_a_sheet_sync_that_has_not_finished_since_10_is_a_broken_check():
    """17 Nov 2026: Render's 10:00 IST sync never finished, and the 11:30 digest said nothing; All Inboxes still
    showed the 16 Nov sync. run.py reports to the digest at its very end, so an older report = an older sheet."""
    yesterday = {"_id": "smartlead-sync", "asks": [], "at": "2026-11-16T05:06:45+00:00"}   # Mon 16 Nov 10:36 IST
    late = dp.sync_check([yesterday], T0 + timedelta(minutes=30))                           # 11:30 IST
    assert len(late) == 1 and late[0]["kind"] == "broken" and late[0]["key"] == "digest:sync-not-finished"
    assert "All Inboxes" in late[0]["text"] and "Mon 16 Nov 10:36 IST" in late[0]["text"]
    assert "run.py" in late[0]["step"]
    today = {**yesterday, "at": "2026-11-17T05:07:00+00:00"}                                # Tue 17 Nov 10:37 IST
    assert dp.sync_check([today], T0) == []
    # Before 10:00 IST the last due run is yesterday's, which finished.
    assert dp.sync_check([yesterday], datetime(2026, 11, 17, 3, 30, tzinfo=timezone.utc)) == []
    never = dp.sync_check([], T0)
    assert len(never) == 1 and "no finished sync on record" in never[0]["text"]


def test_the_digest_waits_for_a_running_morning_job():
    class Runs:
        def __init__(self):
            self.calls = 0

        def find(self, q):
            self.calls += 1
            return [{"job": "smartlead-sync"}] if self.calls < 3 else []
    db = type("D", (), {"cron_runs": Runs()})()
    slept = []
    dp.wait_for_jobs(db, now_fn=lambda: T0, sleep=slept.append)
    assert slept == [60, 60]


def test_the_digest_does_not_wait_for_a_run_a_restart_killed():
    """17 and 18 Nov 2026: a restart killed the 10:00 sync; the digest waited 30 minutes for a run that was gone.
    index.js marks such runs lost when the bot starts again."""
    class Runs:
        def find(self, q):
            return [{"job": "smartlead-sync", "lost": True}]
    db = type("D", (), {"cron_runs": Runs()})()
    slept = []
    dp.wait_for_jobs(db, now_fn=lambda: T0, sleep=slept.append)
    assert slept == []


def test_an_urgent_ask_that_was_fixed_and_comes_back_posts_again():
    """Lead review 16 Nov: an urgent key posted once ever, so a key that died again next month stayed quiet."""
    db, posted = DB(), []
    dead = dg.Ask("Smartlead rejects the X API key.", "Set a new key.", key="key-health:dead:X", urgent=True)
    say = lambda t: posted.append(t) or True  # noqa: E731
    dg.report("key-health", [dead], db=db, now=T0, post_urgent=say)
    dg.report("key-health", [dead], db=db, now=T0, post_urgent=say)          # still open: no second post
    assert len(posted) == 1
    dg.report("key-health", [], db=db, now=T0, post_urgent=say)              # fixed: the record is cleared
    assert "key-health:dead:X" not in db[dg.URGENT].docs
    dg.report("key-health", [dead], db=db, now=T0 + timedelta(days=30), post_urgent=say)
    assert len(posted) == 2


def test_every_source_has_a_plain_label():
    for src in ("zapmail-digest", "fleet-scorecard", "inbox-intake", "inbox-ready", "smartlead-sync",
                "kill-report", "real-recipients", "placement-report", "campaign-metrics", "volume-review"):
        assert src in dp.LABELS


# 17 Nov 2026 (Manveen, on the 12:29 urgent post "2 live campaigns have lost half or more of their inboxes"): "This
# does not even tell us which campaign ... what was the point of this message if it's not gonna give me the complete
# info". An ask carries its own list, and the urgent post and the digest show it.

SHORT = ["• Client A · Campaign One: 2 of 6 inboxes can send (4 disconnected)",
         "• Client A · Campaign Two: 2 of 5 inboxes can send (3 disconnected)"]


def test_the_urgent_post_names_every_item_it_is_about():
    db, posted = DB(), []
    a = dg.Ask("2 live campaigns have lost half or more of their inboxes.", "Add inboxes to each campaign listed.",
               key="inbox-status:campaign_short", urgent=True, detail=SHORT)
    dg.report("inbox-status", [a], db=db, now=T0, post_urgent=lambda t: posted.append(t) or True)
    assert len(posted) == 1
    text = posted[0]
    assert "Campaign One: 2 of 6 inboxes can send (4 disconnected)" in text
    assert "Campaign Two: 2 of 5 inboxes can send (3 disconnected)" in text
    assert "in the thread" not in text                    # an urgent post has no thread
    # what, then the list, then the step, each part apart
    assert text.index("lost half") < text.index("Campaign One") < text.index("Add inboxes")
    assert "\n\n" in text.split("Campaign Two")[1]


def test_a_long_list_is_capped_with_and_n_more():
    lines = [f"• inbox{i}@example.com" for i in range(25)]
    text = dg.urgent_text("x", dg.Ask("25 inboxes are down.", "Fix them.", key="k", detail=lines).doc("x"))
    assert "inbox9@example.com" in text and "inbox10@example.com" not in text
    assert "• and 15 more" in text


def test_detail_lines_get_a_bullet_and_blank_ones_are_dropped():
    d = dg.Ask("t", detail=["one", "", "• two", "  "]).doc("x")
    assert d["detail"] == ["• one", "• two"]
    assert dg.Ask("t").doc("x")["detail"] == []


def test_the_digest_shows_each_asks_list_under_it():
    sources = [_src("inbox-status", [{"text": "2 live campaigns have lost half or more of their inboxes.",
                                      "step": "Add inboxes to each campaign listed.", "detail": SHORT,
                                      "key": "inbox-status:campaign_short"}])]
    head, parts = dp.build(sources, [], [], T0 + timedelta(hours=1))
    assert "1. 2 live campaigns have lost half or more of their inboxes. Add inboxes to each campaign listed. " \
           "(wording not approved yet)" in parts[0]
    assert "\n".join(SHORT) in parts[0]                  # the full list, in the thread


def test_the_digest_caps_each_list_and_says_the_rest_is_in_the_thread():
    lines = [f"• inbox{i}@example.com" for i in range(15)]
    head, parts = dp.build([_src("inbox-intake", [{"text": "15 inboxes drifted.", "step": "Fix them.",
                                                   "detail": lines}])], [], [], T0)
    assert "inbox" not in "\n".join(head[1:])                              # never the list in the post
    assert "inbox10@example.com" in parts[0] and "inbox11@example.com" not in parts[0]
    assert "• and 4 more (the inbox sheet)" in parts[0]


def test_asks_stored_before_the_list_existed_still_render():
    src = {"_id": "inbox-status", "at": T0.isoformat(),
           "asks": [{"text": "7 inboxes are disconnected.", "step": "Reconnect them.", "key": "k", "kind": "ask",
                     "urgent": False, "since": T0.isoformat()}]}
    head, parts = dp.build([src], [], [], T0)
    assert "1. 7 inboxes are disconnected. Reconnect them." in parts[0]


NOW = datetime(2026, 11, 18, 5, 30, tzinfo=timezone.utc)        # Wed 18 Nov 11:00 IST


def _kill_sources():
    fleet = _src("fleet-watch", [{"text": "5 inboxes are on the kill list.", "key": "fleet-watch:kill",
                                  "step": "Take it out of campaigns, do not renew for each one listed.",
                                  "detail": ["a1@alpha.test (open since 11 Nov)", "a2@alpha.test (open since 11 Nov)",
                                             "b1@bravo.test (open since 14 Nov)", "b2@bravo.test (open since 14 Nov)",
                                             "b3@bravo.test (open since 14 Nov)"]}], at=NOW - timedelta(minutes=30))
    kill = _src("kill-report", [
        {"text": "2 inboxes on the kill list are still sending.", "key": "kill-report:sending",
         "step": "Take each inbox listed out of the campaigns named today.",
         "detail": ["a1@alpha.test (Client A): sending in Camp One", "a2@alpha.test (Client A): sending in Camp One"]},
        {"text": "1 inbox on the kill list sits in switched-on campaigns that are not sending.",
         "key": "kill-report:idle", "step": "Pause the campaigns named.",
         "detail": ["c1@charlie.test (Client C): in Camp Two"]},
        {"text": "3 domains are landing in spam on the classic tests and should not be used.",
         "key": "kill-report:domains", "step": "Don't add inboxes on the domains listed to campaigns.",
         "detail": ["Client A · alpha.test (2 inboxes): 70% reached the inbox",
                    "Client B · bravo.test (3 inboxes): 57% reached the inbox",
                    "Client C · charlie.test (1 inbox): 67% reached the inbox"]}], at=NOW - timedelta(days=1))
    return [fleet, kill]


def _text(built):
    head, parts = built
    return "\n".join(head), parts


def test_the_kill_list_from_several_jobs_is_one_item_with_the_job_count():
    """t409: the kill list is one item per domain. A domain whose latest weekly test failed is sentence 1 with that
    test's numbers; a domain on the kill list without a failed weekly test goes to the thread (wording parked)."""
    ctx = {"v2": _v2_fail("alpha.test", "Client A")}
    text, parts = _text(dp.build(_kill_sources(), [], [], NOW, ctx=ctx))
    thread = "\n".join(parts)
    assert text.startswith("Inbox digest · Wed 18 Nov · 3 things to do\n\n1. Client A: alpha.test failed its weekly "
                           "test (0 of 8 reached the inbox at Outlook). Keep its 2 inboxes out of campaigns and don't "
                           "renew the domain.")
    assert "\n2." not in text and "2 more in the thread." in text
    assert "Fleet watch" not in text and "reported by" not in text           # no job names in the post
    assert "3 inboxes on bravo.test are on the kill list from older placement tests" in thread
    assert "1 inbox on charlie.test is on the kill list" in thread
    assert "1 above:\nWeekly test of Sat 14 Nov: 2 of 2 inboxes failed; 0 of 8 test emails reached the inbox at " \
           "Outlook, their own provider.\n• alpha.test: 2 inboxes still sending (a1@, a2@)" in thread


def _holds():
    return [{"_id": "alpha", "domains": ["alpha.test"], "reason": "Another teammate handles these.",
             "owner": "Teammate", "until": None},
            {"_id": "bravo", "domains": ["bravo.test"], "when": "kill-list", "reason": "Retest booked Sat 21 Nov.",
             "owner": "Manveen", "until": "2026-11-22"},
            {"_id": "dead", "domains": ["delta.test", "echo.test"], "when": "disconnected",
             "reason": "Mailboxes gone; on Manveen's delete list.", "owner": "Manveen", "until": None}]


def test_a_recorded_decision_holds_items_back_and_says_so_in_one_line():
    v2 = _src("v2-weekly", [{"text": "6 inboxes did not send in the test, and Smartlead can't log in to them "
                                     "(delta.test 3, echo.test 3). Reconnect them, or delete them in Smartlead.",
                             "key": "v2-weekly:cant-log-in"},
                            {"text": "2 test runs sent fewer than 4 emails per provider: run-1.csv, run-2.csv.",
                             "key": "v2-weekly:short"}])
    text, parts = _text(dp.build(_kill_sources() + [v2], [], [], NOW, holds=_holds()))
    thread = "\n".join(parts)
    assert text == "Inbox digest · Wed 18 Nov · 2 things to do\n\n2 more in the thread."
    assert "charlie.test" in thread                                       # only charlie left of the kill list
    assert "alpha.test" not in thread.split("Left out on purpose")[0] and "delta.test" not in thread.split("Left out")[0]
    assert "2 test runs sent fewer" in thread                                 # not decided: shown
    held = next(p for p in parts if p.startswith("Left out on purpose (decided earlier)"))
    assert "• alpha.test: Another teammate handles these. Owner Teammate, no end date." in held
    assert "• bravo.test: Retest booked Sat 21 Nov. Owner Manveen, until Sun 22 Nov." in held
    assert "• delta.test, echo.test: Mailboxes gone; on Manveen's delete list. Owner Manveen, no end date." in held
    assert len(held.split("\n")) == 4                    # one short line per decision


def test_a_hold_ends_on_its_date_and_the_item_comes_back():
    later = datetime(2026, 11, 22, 5, 30, tzinfo=timezone.utc)                  # Sun 22 Nov 11:00 IST
    text, parts = _text(dp.build(_kill_sources(), [], [], later, holds=_holds()))
    thread = "\n".join(parts)
    assert "bravo.test" in thread.split("Left out on purpose")[0] and "alpha.test: 2" not in thread


def test_a_conditional_hold_only_holds_asks_of_its_kind():
    """The dead-mailbox decision covers 'Smartlead can't log in' asks only: the same domains landing in spam on a
    test (a different problem) still show."""
    other = _src("kill-report", [{"text": "1 domain is landing in spam on the classic tests and should not be used.",
                                  "key": "kill-report:domains", "step": "Don't add inboxes on the domains listed.",
                                  "detail": ["Client D · delta.test (3 inboxes): 50% reached the inbox"]}])
    text, parts = _text(dp.build([other], [], [], NOW, holds=_holds()))
    assert "delta.test" in "\n".join(parts) and "Left out on purpose" not in "\n".join(parts)


def test_a_partly_held_ask_shows_only_what_is_left_and_counts_the_rest():
    src = _src("inbox-status", [{"text": "3 inboxes are disconnected.", "step": "Reconnect each one listed.",
                                 "key": "inbox-status:disc",
                                 "detail": ["x1@alpha.test", "y1@golf.test", "y2@golf.test"]}])
    text, parts = _text(dp.build([src], [], [], NOW, holds=_holds()))
    items = parts[0]
    assert "y1@golf.test" in items and "x1@alpha.test" not in items     # the list, in the thread
    assert "(1 of them held back, decided)" in items


def test_an_ask_that_says_listed_but_carries_no_list_is_left_out_and_counted():
    """17 Nov 2026: 'Look each domain in the thread up ...' with nothing under it: Manveen could not act on it."""
    src = _src("reply-monitor", [{"text": "1 sending domain gets far fewer replies than expected over 14 days.",
                                  "key": "reply-monitor:alert",
                                  "step": "Look each domain in the thread up in this week's v2 test results."}])
    ok = _src("inbox-status", [{"text": "13 inboxes are sending with no weekly test.", "step": "Add each listed.",
                                "key": "inbox-status:sending_unproven",
                                "detail": ["Client E · foxtrot.test: 13 inboxes"]}])
    text, parts = _text(dp.build([src, ok], [], [], NOW))
    assert text.startswith("Inbox digest · Wed 18 Nov · 1 thing to do\n\n1. Client E: 13 inboxes have no weekly test. "
                           "Add them to a test in Smart Delivery (steps in thread).")
    assert "far fewer replies" not in text + "\n".join(parts)
    assert "1 ask from Reply check came without its list, so it was left out of the digest." in _dev([src, ok], now=NOW)


def test_an_ask_naming_its_domains_in_its_own_text_is_not_listless():
    src = _src("v2-weekly", [{"text": "3 inboxes did not send, and Smartlead can't log in to them (golf.test 3)."}])
    assert "golf.test 3" in "\n".join(dp.build([src], [], [], NOW)[1])


def test_items_of_reads_inboxes_and_domains_but_not_file_names():
    a = {"text": "2 runs short: run-1.csv, x.json; see hotel.test", "detail": ["• a@india.test: in Camp", "• juliet.test: b@"]}
    inboxes, domains = dg.items_of(a)
    assert inboxes == {"a@india.test"} and domains == {"hotel.test", "juliet.test"}


def test_a_shortened_inbox_name_is_not_read_as_a_domain():
    # 23 Nov 2026: a kill-list line listing inboxes as "a-b@, a.b@, a_b@" put "a.b" in the digest as a domain.
    a = {"text": "25 inboxes on the kill list", "detail": ["• kilo.test: 25 inboxes (m-ryan@, m.ryan@, m_ryan@ and 22 more)"]}
    inboxes, domains = dg.items_of(a)
    assert inboxes == set() and domains == {"kilo.test"}


def test_holds_are_read_from_mongo_and_an_urgent_held_ask_is_not_posted():
    db, posted = DB(), []
    db[dg.HOLDS].replace_one({"_id": "alpha"}, _holds()[0], upsert=True)
    a = dg.Ask("2 inboxes on the kill list are still sending.", "Take them out.", key="kill-report:sending",
               urgent=True, detail=["a1@alpha.test (Client A): sending in Camp One"])
    dg.report("kill-report", [a], db=db, now=NOW, post_urgent=lambda t: posted.append(t) or True)
    assert posted == []
    assert [h["_id"] for h in dg.load_holds(db)] == ["alpha"]


# 19 Nov 2026 (Manveen, on the 11:00 digest: "i got this and it's long spammy i can not unserstand anything"): a top
# post of ~35 lines and 15 thread replies, the same 24 inboxes told by two jobs, three runs of the day before that
# "never finished" although they had run fine since, and the bot's own words. Synthetic names only (public repo).

def _lines(head):
    return [x for x in "\n".join(head).split("\n") if x.strip()]


def test_the_digest_goes_out_once_a_day():
    """No duplicate headline: a second --post run the same IST day does not post it again."""
    meta = {"_id": "last", "at": "2026-11-19T05:30:09+00:00", "posted": True, "ts": "1.2"}
    assert dp.already_posted(meta, datetime(2026, 11, 19, 6, 0, tzinfo=timezone.utc))          # 11:30 IST, same day
    assert dp.already_posted(meta, datetime(2026, 11, 19, 18, 0, tzinfo=timezone.utc))         # 23:30 IST, same day
    assert not dp.already_posted(meta, datetime(2026, 11, 19, 18, 31, tzinfo=timezone.utc))    # 00:01 IST next day
    assert not dp.already_posted({**meta, "posted": False}, datetime(2026, 11, 19, 6, 0, tzinfo=timezone.utc))
    assert not dp.already_posted(None, NOW)


def test_main_does_not_post_twice(monkeypatch):
    db = DB()
    db[dg.META].replace_one({"_id": "last"}, {"_id": "last", "at": datetime.now(timezone.utc).isoformat(),
                                              "posted": True}, upsert=True)
    monkeypatch.setattr(dg, "_db", lambda: db)
    built = []
    monkeypatch.setattr(dp, "plan", lambda *a, **k: built.append(1))
    assert dp.main(["--post"]) == 0 and built == []


def _busy_day():
    many = [{"text": f"Thing {i} on host{i}.test needs a look.", "key": f"x:{i}", "step": "Look at it.",
             "detail": [f"h{i}-{k}@host{i}.test" for k in range(30)]} for i in range(9)]
    broken = [{"text": f"Job {i} could not read its source.", "kind": "broken", "key": f"y:{i}",
               "detail": [f"error line {k}" for k in range(9)]} for i in range(6)]
    done = [{"_id": f"d{i}", "text": f"Took inbox{i}@host.test out of a campaign."} for i in range(60)]
    srcs = [_src("inbox-intake", many), _src("key-health", broken),
            {**_src("inbox-status", []), "numbers": {"Client A": {"Safe to send": 30, "Recovering": 4},
                                                     "Client B": {"Safe to send": 5, "Do not use": 7}}}]
    return srcs, done


def test_the_top_post_is_at_most_8_lines():
    """t409: the post is the header, at most 5 items, at most 2 Watching lines and one "more" line: within
    guard.TOP_LINES on the busiest day; bot problems and counts are never in it."""
    srcs, done = _busy_day()
    head, _ = dp.build(srcs, done, [], NOW, holds=_holds())
    lines = _lines(head)
    from smartlead import digest_guard as guard
    assert len(lines) <= guard.TOP_LINES, lines
    assert lines[0] == "Inbox digest · Wed 18 Nov · 9 things to do" and lines[-1] == "9 more in the thread."
    assert "API keys" not in "\n".join(head) and "Client A" not in "\n".join(head)


def test_the_thread_is_at_most_3_short_replies():
    from smartlead import notify
    srcs, done = _busy_day()
    srcs.append(_src("fleet-watch", [], detail=["Fleet watch, 19 Nov\n" + "\n".join(f"• kill{i}@a.test" for i in range(40))]))
    _, parts = dp.build(srcs, done, [], NOW, holds=_holds())
    assert 1 <= len(parts) <= 3
    for p in parts:
        assert len(p.split("\n")) <= notify._THREAD_CHUNK            # one part is one Slack reply
    assert "kill7@a.test" not in "\n".join(parts)                    # a job's own report is not copied


def test_asks_about_the_same_inboxes_are_one_item():
    """19 Nov: 8 domains missing from the deliverability sheet (test campaigns job) and their 24 inboxes missing
    from the asset tracker (gate self-test, a broken check) were told twice in the post and twice in the thread.
    t409: the bot's own check goes to the dev channel, so the domains are named once in the digest."""
    doms = ["kilo.test", "lima.test", "mike.test"]
    sheet = _src("dt-senders", [{"text": "3 current-client domains are in Smartlead but not in the deliverability sheet.",
                                 "key": "dt-senders:untracked", "step": "Add each domain listed to the sheet.",
                                 "detail": [f"{d} (Client A, 3 inboxes)" for d in doms]}])
    gate = _src("gate-selftest", [{"text": "Gate self-test: 9 inboxes have no row in the asset tracker.",
                                   "kind": "broken", "key": "gate-selftest:tracker",
                                   "step": "Add the inboxes listed to the asset tracker.",
                                   "detail": dg.by_domain([f"{u}@{d}" for d in doms for u in ("a", "b", "c")])}],
                at=NOW)                          # today's self-test (t403: one from the day before is not used)
    other = _src("reply-monitor", [{"text": "1 domain gets far fewer replies than expected.", "key": "r:1",
                                    "step": "Check it.", "detail": ["november.test (Client B): 0 replies on 200 sends"]}])
    head, parts = dp.build([sheet, gate, other], [], [], NOW)
    text = "\n".join(head + parts)
    assert head[0] == "Inbox digest · Wed 18 Nov · 2 things to do"
    assert text.count("kilo.test") == 1 and "asset tracker" not in text
    assert "november.test (Client B): 0 replies on 200 sends" in text       # an unread reply line is never lost
    assert "9 inboxes have no row in the asset tracker" in _dev([sheet, gate, other], now=NOW)


def test_a_job_that_ran_fine_since_its_failure_is_not_reported():
    """19 Nov: 'Smartlead sync started Wed 18 Nov 10:00 and never finished' although the 19 Nov run had finished."""
    d = lambda h: (NOW - timedelta(hours=h)).replace(tzinfo=None)                  # noqa: E731 - Mongo dates are naive UTC
    runs = [{"job": "smartlead-sync", "started": d(25), "lost": True},
            {"job": "smartlead-sync", "started": d(1), "ended": d(0.5), "exit_code": 0},
            {"job": "campaign-stats-post", "started": d(25)},
            {"job": "campaign-stats-post", "started": d(1.2), "ended": d(1), "exit_code": 137},
            {"job": "infra-audit", "started": d(25)},
            {"job": "infra-audit", "started": d(0.5)}]                                  # still going: says nothing yet
    bh = _src("bot-health", [
        {"text": "Smartlead sync started Wed 18 Nov 10:00 and never finished.", "kind": "broken",
         "key": "bot-health:unfinished:smartlead-sync", "step": "It runs again at its next time."},
        {"text": "Campaign stats post failed, last Thu 19 Nov 10:15 (stopped by SIGKILL).", "kind": "broken",
         "key": "bot-health:failed:campaign-stats-post", "step": "The error is in the Render log."},
        {"text": "Campaign stats post started Wed 18 Nov 10:15 and never finished.", "kind": "broken",
         "key": "bot-health:unfinished:campaign-stats-post", "step": "It runs again at its next time."},
        {"text": "Infra audit started Wed 18 Nov 10:20 and never finished.", "kind": "broken",
         "key": "bot-health:unfinished:infra-audit", "step": "It runs again at its next time."}])
    head, parts = dp.build([bh], [], [], NOW, runs=runs)
    assert head == ["Inbox digest · Wed 18 Nov · nothing to do today"] and parts == []
    dev = _dev([bh], now=NOW, runs=runs)
    assert dev.startswith("Bot jobs · Wed 18 Nov · 2 problems")
    assert "Smartlead sync" not in dev
    # one line per job: its failure today, not also yesterday's unfinished run
    assert "Campaign stats post failed" in dev and "Campaign stats post started" not in dev
    assert "Infra audit started" in dev
    assert dp.recovered("bot-health:unfinished:smartlead-sync", runs, NOW)
    assert not dp.recovered("bot-health:failed:campaign-stats-post", runs, NOW)
    assert not dp.recovered("bot-health:unfinished:infra-audit", runs, NOW)
    assert not dp.recovered("digest:pc-job", runs, NOW)


def test_plain_words_never_touch_a_domain_or_a_name():
    t = dg.plain("dt_senders puts it in the next v2 test; BUSY on failed v2; no v2 pass in 10 days; DT campaign")
    assert t == ("the test-campaign job puts it in the next weekly test; held back on failed the weekly test; no "
                 "weekly test pass in 10 days; test campaign")
    assert dg.plain("The PC's v2 weekly job ran") == "The PC's weekly test job ran"
    kept = "x-v2.test, ops@v2.test, Client | Roundtable | V2, file-v2.csv"
    assert dg.plain(kept) == kept
    assert "seed test" not in dg.plain("(Google inboxes: report-only until real replies agree with the seed test)")


def test_an_info_item_is_one_line_not_a_thing_to_do():
    """20 Nov 2026 ("this is still not fixed"): a fact with no step (kind "info") is never a thing to do. t409: it is
    off the post entirely, under "For info" in the thread."""
    info = _src("inbox-status", [{"text": "3 inboxes are in the weekly test, but the last runs sent too few emails "
                                          "to judge them; next run Sat 21 Nov.", "kind": "info",
                                  "key": "inbox-status:sending_test_short",
                                  "detail": ["Client A · kilo.test: 2 inboxes, in the weekly test of DT A #1",
                                             "Client A · lima.test: 1 inbox, in the weekly test of DT A #1"]}])
    ask = _src("dt-senders", [{"text": "1 domain is not in the deliverability sheet.", "key": "dt-senders:untracked",
                               "step": "Add it to the sheet.", "detail": ["kilo.test (Client A, 3 inboxes)"]}])
    head, parts = dp.build([info, ask], [], [], NOW)
    assert head[0] == "Inbox digest · Wed 18 Nov · 1 thing to do"
    assert "For info" not in "\n".join(head) and "weekly test" not in "\n".join(head)
    thread = "\n".join(parts)
    assert "For info, nothing to do\n3 inboxes are in the weekly test" in thread and "lima.test: 1 inbox" in thread


def test_an_info_item_alone_still_posts_and_says_nothing_needs_you():
    info = _src("inbox-status", [{"text": "1 inbox is in the weekly test; next run Sat 21 Nov.", "kind": "info",
                                  "key": "inbox-status:sending_test_short", "detail": ["Client A · kilo.test: 1 inbox"]}])
    head, parts = dp.build([info], [], [], NOW)
    assert head == ["Inbox digest · Wed 18 Nov · nothing to do today"]
    assert "1 inbox is in the weekly test" in parts[0]


def test_tracker_keys_never_reach_the_reader_2026_11_21():
    """21 Nov 2026: a person's Smartlead tag carried "(t235)" into the bill item."""
    assert dg.plain('tagged "DO NOT USE - pulled by a teammate 16 Nov (t235)"') == 'tagged "DO NOT USE - pulled by a teammate 16 Nov"'
    assert dg.plain("see (t268/t269) here") == "see here"
    assert dg.plain("a weekly v2 test") == "a weekly test"


def test_the_numbers_reply_says_what_each_word_means_2026_11_21():
    """t409: the numbers reply is gone (the counts live on the All Inboxes tab)."""
    src = {**_src("inbox-status", [{"text": "x.", "step": "y.", "detail": ["a.example: 1"]}]),
           "numbers": {"Client A": {"Safe to send": 30, "Not proven yet": 2}}}
    head, parts = dp.build([src], [], [], T0)
    assert not any(p.startswith("Where the inboxes stand") for p in parts)


def test_every_for_info_line_fits_in_the_thread_2026_11_21():
    """21 Nov 2026: the reply's line cap cut off the second "For info" item the post pointed to."""
    long_info = _src("inbox-status", [{"text": "39 inboxes are being read.", "kind": "info", "key": "inbox-status:i",
                                       "detail": [f"Client A · d{i}.example: 3 inboxes" for i in range(13)]}])
    other_info = _src("v2-weekly", [{"text": "1 result had not come in.", "kind": "info", "key": "v2-weekly:i"}])
    big = _src("dt-senders", [{"text": "Many inboxes.", "step": "Do it.", "key": "dt-senders:x",
                               "detail": [f"e{i}.example: 1 inbox" for i in range(12)]}])
    more = [_src(f"job{k}", [{"text": f"Item {k}.", "step": "Do it.", "key": f"job{k}:x",
                              "detail": [f"f{k}-{i}.example: 1 inbox" for i in range(20)]}]) for k in range(3)]
    head, parts = dp.build([long_info, other_info, big] + more, [], [], NOW)
    assert all(len(p.split("\n")) <= dp.PART_LINES + 1 for p in parts) and len(parts) <= dp.MAX_PARTS
    thread = "\n".join(parts)
    assert "1 result had not come in." in thread
    assert "• and 5 more" in thread                                    # t386: 8 lines of a "For info" list


def _dev(sources, done=(), checks=(), now=T0, **kw):
    """The dev-channel message for these sources (t409: bot and job problems go there, never in the digest)."""
    p = dp.plan(sources, list(done), list(checks), now, **kw)
    return dp.dev_post(p, now) or ""


def test_nothing_to_say_posts_one_line():
    """t409: a day with nothing to do posts its header alone, so a missing post means the bot did not run."""
    assert dp.build([_src("gate-selftest", [])], [], [], T0) == (["Inbox digest · Tue 17 Nov · nothing to do today"], [])


def test_the_unfinished_sync_line_is_in_the_dev_message():
    yesterday = {"_id": "smartlead-sync", "asks": [], "at": "2026-11-16T05:06:45+00:00"}
    head, parts = dp.build([yesterday], [], dp.sync_check([yesterday], T0), T0)
    assert head == ["Inbox digest · Tue 17 Nov · nothing to do today"] and parts == []
    dev = _dev([yesterday], checks=dp.sync_check([yesterday], T0))
    assert dev.startswith("Bot jobs · Tue 17 Nov · 1 problem\n") and "daily sheet sync has not finished" in dev


def _v2_fail(dom, client, n=2, at="microsoft"):
    """Weekly test rows of a domain that failed at Outlook: 0 of 4 test emails in the inbox per inbox."""
    other = "google" if at == "microsoft" else "microsoft"
    return [{"email": f"u{i}@{dom}", "domain": dom, "client": client, "night": "2026-11-14", "verdict": "FAIL",
             "type": "OUTLOOK" if at == "microsoft" else "GMAIL", "weekly": True,
             at: {"inbox": 0, "sent": 4}, other: {"inbox": 4, "sent": 4}} for i in range(n)]


def test_held_items_alone_post_nothing_to_do():
    """A day with only decided items has nothing to do: the one-line post, the decisions in the thread."""
    fleet = _kill_sources()[0]
    head, parts = dp.build([fleet], [], [], NOW, holds=_holds())
    assert head == ["Inbox digest · Wed 18 Nov · nothing to do today"]
    assert parts and parts[0].startswith("Left out on purpose (decided earlier)")


def test_a_broken_check_with_a_step_for_manveen_goes_to_the_dev_channel():
    """t409 decision 3: deploy Render, switch the PC job on: bot set-up, posted with its step in the dev channel."""
    r = dp.render_check("abc1234def", {"behind": 3})
    head, _ = dp.build([], [], r, NOW)
    assert head == ["Inbox digest · Wed 18 Nov · nothing to do today"]
    assert "Manveen: Render, infra-bot-1, Manual Deploy, Deploy latest commit." in _dev([], checks=r, now=NOW)


def test_the_inbox_counts_stay_on_the_sheet():
    """t409 decision 3: where the inboxes stand lives on the All Inboxes tab, not in the digest."""
    src = {**_src("inbox-status", []), "numbers": {"Client A": {"Safe to send": 30, "Recovering": 4, "Not proven yet": 2},
                                                   "Client B": {"Safe to send": 5, "Do not use": 7}}}
    head, parts = dp.build([src], [], [], NOW)
    assert head == ["Inbox digest · Wed 18 Nov · nothing to do today"] and parts == []
    assert "<@" not in "\n".join(head)
