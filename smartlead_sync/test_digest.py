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
    text = "\n".join(head)
    assert head[0] == "Inbox digest, Tue 17 Nov"
    assert f"<@{dg.OWNER}>, 2 things need you today:" in text
    assert "\n1. Zapmail declined the card.\nDo: Pay the invoice." in text            # urgent first
    assert "\n2. 7 inboxes are disconnected.\nDo: Reconnect them. Open since Sun 15 Nov." in text
    assert "Done by the bot since the last digest:\n• Took 3 inboxes" in text
    assert "Checks that broke" in text and "Campaign stats job failed (exit 2)." in text
    assert parts == ["Inboxes, as of Tue 17 Nov 11:00\n\nfull inbox list"]


def test_nothing_to_say_posts_nothing():
    assert dp.build([_src("gate-selftest", [])], [], [], T0) is None


def test_asks_of_a_job_silent_for_over_8_days_are_dropped_and_old_ones_say_when_checked():
    old = _src("kill-report", [{"text": "Kill 2 inboxes."}], at=T0 - timedelta(days=9))
    tue = _src("reply-monitor", [{"text": "Replies fell on one domain."}], at=T0 - timedelta(days=2))
    head, _ = dp.build([old, tue], [], [], T0)
    text = "\n".join(head)
    assert "Kill 2 inboxes" not in text and "(last checked Sun 15 Nov)" in text


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


def test_the_unfinished_sync_line_is_in_the_posted_digest():
    yesterday = {"_id": "smartlead-sync", "asks": [], "at": "2026-11-16T05:06:45+00:00"}
    head, _ = dp.build([yesterday], [], dp.sync_check([yesterday], T0), T0)
    text = "\n".join(head)
    assert "Checks that broke" in text and "daily sheet sync has not finished" in text


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
                                      "step": "Add inboxes to each campaign listed.", "detail": SHORT}])]
    head, _ = dp.build(sources, [], [], T0 + timedelta(hours=1))
    text = "\n".join(head)
    assert ("\n1. 2 live campaigns have lost half or more of their inboxes.\n" + "\n".join(SHORT)
            + "\nDo: Add inboxes to each campaign listed.") in text


def test_the_digest_caps_each_list_and_says_the_rest_is_in_the_thread():
    lines = [f"• inbox{i}@example.com" for i in range(9)]
    head, _ = dp.build([_src("inbox-intake", [{"text": "9 inboxes drifted.", "step": "Fix them.", "detail": lines}])],
                       [], [], T0)
    text = "\n".join(head)
    assert "inbox4@example.com" in text and "inbox5@example.com" not in text
    assert "• and 4 more in the thread" in text


def test_asks_stored_before_the_list_existed_still_render():
    src = {"_id": "inbox-status", "at": T0.isoformat(),
           "asks": [{"text": "7 inboxes are disconnected.", "step": "Reconnect them.", "key": "k", "kind": "ask",
                     "urgent": False, "since": T0.isoformat()}]}
    head, _ = dp.build([src], [], [], T0)
    assert "\n1. 7 inboxes are disconnected.\nDo: Reconnect them." in "\n".join(head)


# 18 Nov 2026 (Manveen, on the 11:30 digest: "why did i get these 13 messages"): the same kill-list inboxes came from
# two jobs as four asks, and items already decided (another person handles them, a retest is booked, a delete list
# he does by hand) came back every day. Synthetic names only (public repo).

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
    text, _ = _text(dp.build(_kill_sources(), [], [], NOW))
    assert f"<@{dg.OWNER}>, 1 thing needs you today:" in text
    assert "\n2." not in text
    assert "Kill list: 6 inboxes and 3 domains, reported by 2 jobs (Fleet watch, Kill warnings)." in text
    # one line per domain, saying what each job found there
    assert "• alpha.test: 2 inboxes still sending (a1@, a2@); domain landing in spam on the classic tests" in text
    assert "• charlie.test: 1 inbox in switched-on campaigns that are not sending (c1@)" in text
    assert "Do: " in text and "Email Accounts tab" in text and "don't renew" in text


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
    assert f"<@{dg.OWNER}>, 2 things need you today:" in text
    assert "Kill list: 1 inbox and 1 domain, reported by 1 job (Kill warnings)." in text   # only charlie is left
    assert "alpha.test" not in text and "bravo.test" not in text and "delta.test" not in text
    assert "2 test runs sent fewer" in text                                                   # not decided: shown
    assert "\nHeld back: 4 (decided). What and why: thread." in text
    held = next(p for p in parts if p.startswith("Held back (decided)"))
    assert "alpha.test" in held and "Owner Teammate, no end date" in held and "Another teammate" in held
    assert "Owner Manveen, until Sun 22 Nov" in held and "Retest booked" in held
    assert "delete list" in held
    # the fleet-watch kill list is split between two decisions: each says its share
    assert "5 inboxes are on the kill list (Fleet watch, 2 of its list)" in held
    assert "5 inboxes are on the kill list (Fleet watch, 3 of its list)" in held
    assert ". (" not in held


def test_a_hold_ends_on_its_date_and_the_item_comes_back():
    later = datetime(2026, 11, 22, 5, 30, tzinfo=timezone.utc)                  # Sun 22 Nov 11:00 IST
    text, _ = _text(dp.build(_kill_sources(), [], [], later, holds=_holds()))
    assert "bravo.test: 3 inboxes on the kill list" in text and "alpha.test" not in text


def test_a_conditional_hold_only_holds_asks_of_its_kind():
    """The dead-mailbox decision covers 'Smartlead can't log in' asks only: the same domains landing in spam on a
    test (a different problem) still show."""
    other = _src("kill-report", [{"text": "1 domain is landing in spam on the classic tests and should not be used.",
                                  "key": "kill-report:domains", "step": "Don't add inboxes on the domains listed.",
                                  "detail": ["Client D · delta.test (3 inboxes): 50% reached the inbox"]}])
    text, _ = _text(dp.build([other], [], [], NOW, holds=_holds()))
    assert "delta.test" in text and "Held back" not in text


def test_a_partly_held_ask_shows_only_what_is_left_and_counts_the_rest():
    src = _src("inbox-status", [{"text": "3 inboxes are disconnected.", "step": "Reconnect each one listed.",
                                 "key": "inbox-status:disc",
                                 "detail": ["x1@alpha.test", "y1@golf.test", "y2@golf.test"]}])
    text, _ = _text(dp.build([src], [], [], NOW, holds=_holds()))
    assert "y1@golf.test" in text and "x1@alpha.test" not in text
    assert "(1 of them held back, decided)" in text


def test_an_ask_that_says_listed_but_carries_no_list_is_left_out_and_counted():
    """17 Nov 2026: 'Look each domain in the thread up ...' with nothing under it: Manveen could not act on it."""
    src = _src("reply-monitor", [{"text": "1 sending domain gets far fewer replies than expected over 14 days.",
                                  "key": "reply-monitor:alert",
                                  "step": "Look each domain in the thread up in this week's v2 test results."}])
    ok = _src("inbox-status", [{"text": "13 inboxes are sending with no weekly test.", "step": "Add each listed.",
                                "detail": ["Client E · foxtrot.test: 13 inboxes"]}])
    text, _ = _text(dp.build([src, ok], [], [], NOW))
    assert f"<@{dg.OWNER}>, 1 thing needs you today:" in text and "far fewer replies" not in text.split("Checks")[0]
    assert "1 ask came without its list, so it was left out (Reply check)" in text


def test_an_ask_naming_its_domains_in_its_own_text_is_not_listless():
    src = _src("v2-weekly", [{"text": "3 inboxes did not send, and Smartlead can't log in to them (golf.test 3)."}])
    assert "golf.test 3" in _text(dp.build([src], [], [], NOW))[0]


def test_items_of_reads_inboxes_and_domains_but_not_file_names():
    a = {"text": "2 runs short: run-1.csv, x.json; see hotel.test", "detail": ["• a@india.test: in Camp", "• juliet.test: b@"]}
    inboxes, domains = dg.items_of(a)
    assert inboxes == {"a@india.test"} and domains == {"hotel.test", "juliet.test"}


def test_holds_are_read_from_mongo_and_an_urgent_held_ask_is_not_posted():
    db, posted = DB(), []
    db[dg.HOLDS].replace_one({"_id": "alpha"}, _holds()[0], upsert=True)
    a = dg.Ask("2 inboxes on the kill list are still sending.", "Take them out.", key="kill-report:sending",
               urgent=True, detail=["a1@alpha.test (Client A): sending in Camp One"])
    dg.report("kill-report", [a], db=db, now=NOW, post_urgent=lambda t: posted.append(t) or True)
    assert posted == []
    assert [h["_id"] for h in dg.load_holds(db)] == ["alpha"]


def test_held_items_alone_post_nothing():
    """A day with only decided items is a quiet day: no post just to say 'held back'."""
    fleet = _kill_sources()[0]
    assert dp.build([fleet], [], [], NOW, holds=_holds()) is None
