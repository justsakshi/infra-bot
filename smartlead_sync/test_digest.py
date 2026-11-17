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
                "kill-report", "real-recipients", "placement-report", "campaign-metrics"):
        assert src in dp.LABELS
