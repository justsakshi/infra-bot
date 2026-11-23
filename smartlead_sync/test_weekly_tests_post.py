"""The Monday weekly test results post (t420): smartlead/weekly_results.py and weekly_tests_post.py.

Fake domains only (the repo is public). Fixtures: a normal week, a week with a one-domain fail, a week with no results,
a client with only not-judged inboxes, and a bad day (many domains, every state) that must stay within the limits.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone

import pytest

import weekly_tests_post as wtp
from smartlead import weekly_results as wr

TODAY = date(2026, 11, 23)                     # a Monday
SAT = "2026-11-21"
NOW = datetime(2026, 11, 23, 4, 30, tzinfo=timezone.utc)   # 10:00 IST


def row(email, verdict, typ="OUTLOOK", client="Precise Leads", night=SAT, g=(4, 4), m=(4, 4), **kw):
    return {"email": email, "domain": email.split("@")[1], "client": client, "night": night, "verdict": verdict,
            "type": typ, "google": {"inbox": g[0], "sent": g[1]}, "microsoft": {"inbox": m[0], "sent": m[1]},
            "weekly": True, "history": False, **kw}


def inboxes(domain, k, verdict, **kw):
    return [row(f"u{i}@{domain}", verdict, **kw) for i in range(k)]


def st(email, client="Precise Leads", busy="", provider="Outlook", campaigns=(), state="sending"):
    return {"email": email, "client": client, "busy_reason": busy, "provider": provider, "campaigns": list(campaigns),
            "state": state}


def status_of(rows, **kw):
    return {r["email"]: st(r["email"], client=r["client"], **kw) for r in rows}


def build(rows, status=None, removed=None, marks=None, tests=None, ever=None, did=None):
    rows_, nights = wr.week_rows(rows, TODAY)
    return wr.build(rows_, nights, status or {}, removed or {}, marks or {}, tests, ever or set(), did, TODAY)


def text(head):
    return "\n".join(head)


# --- a normal week ----------------------------------------------------------------------------------------------------

def normal_week():
    pl_pass = inboxes("alpha-pl.test", 25, "PASS") + inboxes("beta-pl.test", 25, "PASS")
    pl_gone = inboxes("gone-pl.test", 4, "NOT_SENT", typ="SMTP", g=(0, 0), m=(0, 0))
    bd_pass = inboxes("ok-bd.test", 3, "PASS", client="Bettrdata", typ="GMAIL")
    bd_fail = [r for d in ("rec1-bd.test", "rec2-bd.test")
               for r in inboxes(d, 3, "FAIL", client="Bettrdata", typ="GMAIL", g=(0, 4))]
    bd_gone = inboxes("zap-bd.test", 3, "NOT_SENT", client="Bettrdata", typ="GMAIL", g=(0, 0), m=(0, 0))
    rows = pl_pass + pl_gone + bd_pass + bd_fail + bd_gone
    status = {**status_of(pl_pass + bd_pass, campaigns=["Live one"]),
              **status_of(pl_gone, busy="do_not_use", provider="SMTP", state="do_not_use"),
              **status_of(bd_fail, busy="do_not_use", provider="Google", state="recovering"),
              **status_of(bd_gone, busy="disconnected", provider="Google", state="disconnected")}
    removed = {**{r["email"]: {"provider": "ScaledMail", "why": "order cancelled, no inboxes left"} for r in pl_gone},
               **{r["email"]: {"provider": "Zapmail", "why": "every inbox on the domain deleted there"} for r in bd_gone}}
    return rows, status, removed


def test_normal_week_reads_per_client_with_results_only():
    rows, status, removed = normal_week()
    did = wr.parse_did("Took 0 inboxes out of 0 live campaigns · tagged 6 in no campaign · returned 0")
    head, parts = build(rows, status, removed, did=did, ever={r["email"] for r in rows})
    t = text(head)
    assert head[0] == "Weekly test results · tests of Sat 21 Nov"
    assert "Precise Leads: 50 passed. 4 did not send: the cancelled ScaledMail inboxes, already on the delete list." in t
    assert ("BettrData: 3 passed. 6 failed at Google: the 2 domains already tagged and recovering. 3 did not send: "
            "removed by Zapmail, on the delete list.") in t
    assert "What the bot did: tagged 6 failed inboxes that were in no campaign; no live campaign lost an inbox." in t
    assert t.index("Precise Leads:") < t.index("BettrData:") < t.index("What the bot did")
    assert "Not judged yet" not in t
    assert "Do:" not in t and "Bettrdata" not in t
    assert len(parts) == 1 and parts[0].startswith("Smartlead's own %")
    assert "rec1-bd.test 0 of 12" in parts[0] and "zap-bd.test 3" in parts[0]
    assert wr.problems(head, parts) == []


def test_google_seeds_for_outlook_inboxes_are_never_a_fail():
    """An Outlook inbox the v2 job passed at Microsoft stays passed whatever Google's seeds said; a failed Outlook
    inbox's score is its Microsoft seeds only."""
    ok = inboxes("out-pl.test", 3, "PASS", g=(0, 4), m=(4, 4))
    bad = inboxes("bad-pl.test", 2, "FAIL", g=(4, 4), m=(0, 4))
    head, _ = build(ok + bad, status_of(ok + bad, busy="failed_v2", state="pulled"))
    t = text(head)
    assert "Precise Leads: 3 passed." in t
    assert "bad-pl.test failed: 2 inboxes, 0 of 8 reached the inbox at Outlook. Kept out of campaigns.\n" in t + "\n"


# --- a one-domain fail -------------------------------------------------------------------------------------------------

def test_one_domain_fail_names_the_domain_its_score_and_state():
    good = inboxes("good-me.test", 25, "PASS", client="Melior")
    bad = inboxes("bad-me.test", 25, "FAIL", client="Melior", g=(3, 4), m=(0, 4))
    for r in bad[:15]:
        r["pulled_on"] = SAT                                 # the 15 the job tagged this week
    status = {**status_of(good, campaigns=["Melior live"]),
              **status_of(bad[:15], busy="do_not_use,failed_v2", state="pulled"),
              **status_of(bad[15:], busy="no_v2_pass", state="warming")}
    did = wr.parse_did("Took 0 inboxes out of 0 live campaigns · tagged 15 in no campaign · returned 0")
    head, parts = build(good + bad, status, did=did)
    t = text(head)
    assert ("Melior: 25 passed. bad-me.test failed: 25 inboxes, 0 of 100 reached the inbox at Outlook. "
            "Kept out of campaigns: 15 tagged DO NOT USE, 10 held back with no passing test yet.") in t
    assert ("What the bot did: tagged 15 failed inboxes that were in no campaign (Melior, bad-me.test); no live "
            "campaign lost an inbox.") in t                  # readers could not tell which 15
    assert head[-1] == wr.NO_ACTIONS
    assert wr.problems(head, parts) == []


def test_a_failed_inbox_still_in_a_campaign_says_so():
    bad = inboxes("live-pl.test", 3, "FAIL", typ="GMAIL", g=(0, 4))
    head, _ = build(bad, status_of(bad, campaigns=["PL live"], provider="Google"))
    assert "live-pl.test failed: 3 inboxes, 0 of 12 reached the inbox at Google. Still in campaigns." in text(head)


def test_did_not_send_with_several_reasons_is_one_sentence():
    gone = inboxes("gone-pl.test", 2, "NOT_SENT", g=(0, 0), m=(0, 0))
    login = inboxes("login-pl.test", 3, "NOT_SENT", g=(0, 0), m=(0, 0))
    short = inboxes("short-pl.test", 1, "NOT_SENT", g=(0, 0), m=(0, 0))
    status = {**status_of(login, busy="disconnected", state="disconnected"), **status_of(gone + short)}
    removed = {r["email"]: {"provider": "ScaledMail", "why": "order cancelled"} for r in gone}
    head, _ = build(gone + login + short, status, removed)
    assert ("6 did not send: 2 cancelled at ScaledMail; 3 Smartlead can't log in; 1 the test came back short "
            "(the removed ones are already on the delete list).") in text(head)


# --- no results --------------------------------------------------------------------------------------------------------

def test_no_results_is_one_line_and_tells_the_bot_channel_why():
    old = inboxes("old-pl.test", 3, "PASS", night="2026-11-14")
    rows, nights = wr.week_rows(old, TODAY)
    assert rows == [] and nights == []
    head, parts, dev = wtp.compose({"rows": [], "nights": [], "today": TODAY, "newest": "2026-11-14",
                                    "heartbeat": {"last_run": "2026-11-22T09:30:00+00:00"}})
    assert parts == []
    assert head[0] == "Weekly test results · week of Mon 23 Nov"
    assert [x for x in head[1:] if x] == ["No weekly test results came in this week (none stored since Sat 14 Nov). "
                                          "The bot team has been told why."]
    assert "InfraBot v2 weekly" in dev and "Sat 14 Nov" in dev and "Sun 22 Nov 15:00 IST" in dev
    assert wr.problems(head, parts) == []


# --- a client with only not-judged inboxes --------------------------------------------------------------------------

def test_client_with_only_not_judged_inboxes_is_in_the_not_judged_line_only():
    pl = inboxes("alpha-pl.test", 3, "PASS")
    new = [f"n{i}@prewarm-bo.test" for i in range(24)]
    part = inboxes("half-me.test", 2, "INCOMPLETE", client="Melior")
    status = {**status_of(pl), **status_of(part, campaigns=["x"]),
              **{e: st(e, client="Bodine & Co.", state="sending_unproven") for e in new}}
    tests = [{"name": "prewarmed", "campaign": "Pre-warmed test", "next_run": "2026-11-28T06:00:00.000Z",
              "senders": new},
             {"name": "melior", "campaign": "DT Melior #1", "next_run": "2026-11-28T02:30:00.000Z",
              "senders": [r["email"] for r in part]},
             {"name": "copy", "campaign": "DT Melior copy check", "copy": True, "next_run": "2026-11-28T05:00:00Z",
              "senders": ["c@copy-me.test"]}]
    marks = {"prewarm-bo.test": "InboxKit"}
    head, parts = build(pl + part, status, marks=marks, tests=tests, ever={r["email"] for r in pl + part})
    t = text(head)
    assert "Bodine & Co.:" not in t and "Melior:" not in t
    assert ("Not judged yet: Melior's 2 inboxes with no full result (next test Sat 28 Nov); Bodine & Co.'s 24 "
            "pre-warmed InboxKit inboxes (first weekly test Sat 28 Nov).") in t
    assert "copy-me.test" not in t + parts[0]
    assert wr.problems(head, parts) == []


def test_an_inbox_with_only_a_one_off_test_gets_its_first_weekly_test():
    e = "x@new-pl.test"
    tests = [{"campaign": "DT PL #1", "next_run": "2026-11-28T00:30:00.000Z", "senders": [e]}]
    head, _ = build(inboxes("alpha-pl.test", 1, "PASS"), {e: st(e)}, tests=tests, ever=set())
    assert "Precise Leads' 1 inbox (first weekly test Sat 28 Nov)" in text(head)
    head, _ = build(inboxes("alpha-pl.test", 1, "PASS"), {e: st(e)}, tests=tests, ever={e})
    assert "Precise Leads' 1 inbox not in this week's results (next test Sat 28 Nov)" in text(head)


# --- which rows are this week's ---------------------------------------------------------------------------------------

def test_week_rows_take_the_newest_weekend_only():
    rows = (inboxes("a-pl.test", 2, "FAIL", night="2026-11-16") + inboxes("a-pl.test", 2, "PASS", night=SAT)
            + inboxes("b-pl.test", 2, "PASS", night="2026-11-22") + inboxes("c-pl.test", 2, "FAIL", night="2026-11-16")
            + [{**row("h@hist-pl.test", "FAIL"), "history": True}, {**row("o@once-pl.test", "FAIL"), "weekly": False}])
    got, nights = wr.week_rows(rows, TODAY)
    assert nights == [SAT, "2026-11-22"]
    assert {r["email"]: r["verdict"] for r in got} == {"u0@a-pl.test": "PASS", "u1@a-pl.test": "PASS",
                                                      "u0@b-pl.test": "PASS", "u1@b-pl.test": "PASS"}
    assert wr.nights_title(nights) == "Sat 21 and Sun 22 Nov"


def test_parse_did_and_find_did_need_this_weeks_tests():
    assert wr.parse_did("Took 2 inboxes out of 3 live campaigns · tagged 1 left in paused/draft campaigns and 4 in no "
                        "campaign · returned 5") == {"took": 2, "campaigns": 3, "tagged_paused": 1, "tagged_none": 4,
                                                     "returned": 5}
    assert wr.parse_did("nothing here") is None
    texts = ["v2 placement results, tests of Sat 14 Nov\nTook 9 inboxes out of 9 live campaigns",
             "v2 weekly test (tests of Sat 21 Nov): took 0 inboxes out of 0 live campaigns · tagged 15 in no campaign"]
    assert wr.find_did(texts, "Sat 21 Nov")["tagged_none"] == 15
    assert wr.find_did(texts[:1], "Sat 21 Nov") is None


def test_did_line_words():
    d = wr.parse_did("Took 2 inboxes out of 1 live campaigns · tagged 0 in no campaign · returned 1")
    assert wr.did_line(d, [], SAT) == ("What the bot did: took 2 failed inboxes out of 1 live campaign; returned 1 "
                                       "inbox that passed again to its campaigns.")
    nothing = wr.parse_did("Took 0 inboxes out of 0 live campaigns · tagged 0 in no campaign · returned 0")
    assert wr.did_line(nothing, [], SAT) == "What the bot did: nothing changed in Smartlead."
    rows = [row("a@x-pl.test", "FAIL", pulled_on=SAT), row("b@x-pl.test", "FAIL", pulled_on="2026-11-11")]
    assert wr.did_line(None, rows, SAT) == ("What the bot did: took 1 failed inbox out of use (tagged DO NOT USE; "
                                            "Precise Leads, x-pl.test).")


# --- a bad day: every limit holds ---------------------------------------------------------------------------------------

def test_bad_day_stays_within_the_limits():
    rows, status, removed = [], {}, {}
    for c, typ, prov in (("Precise Leads", "OUTLOOK", "Outlook"), ("Melior", "GMAIL", "Google"),
                         ("Bettrdata", "GMAIL", "Google"), ("Bodine & Co.", "OUTLOOK", "Outlook"),
                         ("Northwind", "SMTP", "SMTP")):
        tag = re.sub(r"[^a-z]", "", c.lower())[:6]
        for k in range(20):
            rs = inboxes(f"p{k}-{tag}.test", 3, "PASS", client=c, typ=typ)
            rows += rs
            status.update(status_of(rs, campaigns=["live"], provider=prov))
        for k, (state, busy, camps) in enumerate([("recovering", "do_not_use", ()), ("pulled", "failed_v2", ()),
                                                  ("sending", "", ("live",)), ("free", "", ())] * 4):
            rs = inboxes(f"f{k}-{tag}.test", 3, "FAIL", client=c, typ=typ, g=(0, 4), m=(1, 4))
            rows += rs
            status.update(status_of(rs, busy=busy, state=state, campaigns=camps, provider=prov))
        for k in range(25):
            rs = inboxes(f"g{k}-{tag}.test", 4, "NOT_SENT", client=c, typ=typ, g=(0, 0), m=(0, 0))
            rows += rs
            status.update(status_of(rs, busy="disconnected" if k % 3 == 0 else "", provider=prov))
            if k % 3 == 1:
                removed.update({r["email"]: {"provider": "Zapmail", "why": "domain expired"} for r in rs})
        rows += inboxes(f"w-{tag}.test", 3, "WATCH", client=c, typ=typ)
        rows += inboxes(f"i-{tag}.test", 3, "INCOMPLETE", client=c, typ=typ)
    did = wr.parse_did("Took 40 inboxes out of 12 live campaigns · tagged 3 left in paused/draft campaigns and 30 in "
                       "no campaign · returned 7")
    head, parts = build(rows, status, removed, did=did, ever={r["email"] for r in rows})
    assert wr.problems(head, parts) == []
    t = text(head)
    assert len([x for x in head if x]) <= wr.TOP_LINES and len(t) <= wr.TOP_CHARS
    assert "…" not in t                                     # no sentence cut in half: long paragraphs go short
    assert "Melior: 60 passed, 3 borderline, 48 failed, 100 did not send (details in the thread)." in t
    assert len(parts) == 1 and "*Precise Leads*" in parts[0] and "• Passed" not in parts[0]
    assert re.search(r"\(inboxes per domain\): .* and \d+ more domains\n", parts[0])   # not read as inboxes


def test_problems_catch_a_broken_post():
    head = ["Weekly test results · tests of Sat 21 Nov"] + ["x"] * 20 + ["Do: delete them", "pl-test-run-1.csv"]
    found = wr.problems(head, ["a\nb", "c"])
    assert any("lines" in x for x in found) and any("action" in x for x in found)
    assert any("internal name" in x for x in found) and any("thread replies" in x for x in found)


# --- the CLI on a fake Mongo -------------------------------------------------------------------------------------------

class Cursor(list):
    def sort(self, key, direction=1):
        return Cursor(sorted(self, key=lambda d: str(d.get(key) or ""), reverse=direction < 0))

    def limit(self, n):
        return Cursor(self[:n])


def _match(doc, q):
    for k, v in (q or {}).items():
        x = doc.get(k)
        if isinstance(v, dict):
            for op, y in v.items():
                if op == "$gte" and not (x is not None and str(x) >= str(y)):
                    return False
                if op == "$ne" and x == y:
                    return False
                if op == "$in" and x not in y:
                    return False
        elif x != v:
            return False
    return True


class Col:
    def __init__(self, docs=()):
        self.docs = [dict(d) for d in docs]

    def find(self, q=None, proj=None):
        return Cursor(dict(d) for d in self.docs if _match(d, q))

    def find_one(self, q=None, proj=None):
        return next(iter(self.find(q)), None)

    def distinct(self, key, q=None):
        return sorted({d.get(key) for d in self.find(q) if d.get(key) is not None})

    def replace_one(self, q, doc, upsert=False):
        self.docs = [d for d in self.docs if not _match(d, q)] + [dict(doc)]


class DB(dict):
    def __getitem__(self, k):
        return self.setdefault(k, Col())

    def __getattr__(self, k):
        return self[k]


@pytest.fixture
def fake(monkeypatch):
    rows, status, _ = normal_week()
    db = DB()
    db["v2_verdicts"] = Col(rows)
    db["inbox_status"] = Col(status.values())
    db["digest_asks"] = Col([{"_id": "v2-weekly", "detail": [
        "v2 placement results, tests of Sat 21 Nov\n327 inboxes\nTook 0 inboxes out of 0 live campaigns · tagged 6 in "
        "no campaign · returned 0"]}])
    posts = []
    monkeypatch.setattr(wtp.dg, "_db", lambda: db)
    monkeypatch.setattr(wtp, "datetime", type("D", (), {"now": staticmethod(lambda tz=None: NOW)}))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    import smartlead.notify as notify
    monkeypatch.setattr(notify, "post_with_thread", lambda tok, ch, head, parts: posts.append((ch, head, parts)) or "1.1")
    monkeypatch.setattr(notify, "_post", lambda tok, ch, text, thread_ts=None: posts.append((ch, text)) or "1.2")
    return db, posts


def test_the_no_actions_line_follows_the_digest_switch(fake, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "off")
    ctx = wtp.load(fake[0], NOW)
    assert wtp.compose(ctx)[0][-1] == wr.NO_ACTIONS_OFF
    monkeypatch.delenv("DIGEST_MODE")
    assert wtp.compose(ctx)[0][-1] == wr.NO_ACTIONS


def test_cli_dry_run_posts_nothing(fake, capsys):
    db, posts = fake
    assert wtp.main([]) == 0
    out = capsys.readouterr().out
    assert "Weekly test results · tests of Sat 21 Nov" in out and "tagged 6 failed inboxes" in out
    assert posts == [] and not db["weekly_tests_post"].docs


def test_cli_posts_once_a_week_to_the_digest_channel(fake, monkeypatch):
    db, posts = fake
    monkeypatch.delenv("DIGEST_CHANNEL", raising=False)
    assert wtp.main(["--post"]) == 0
    assert len(posts) == 1 and posts[0][0] == "C0AGVSUNEFP"
    assert wtp.main(["--post"]) == 0
    assert len(posts) == 1                                   # the same week is not posted twice
    assert wtp.main(["--post", "--again"]) == 0 and len(posts) == 2


def test_cli_no_results_posts_one_line_and_the_reason_to_the_bot_channel(fake, monkeypatch):
    db, posts = fake
    db["v2_verdicts"] = Col(inboxes("old-pl.test", 2, "PASS", night="2026-11-07"))
    db["v2_heartbeat"] = Col([{"_id": "pc", "last_run": "2026-11-23T03:00:00+00:00"}])
    monkeypatch.setenv("BOT_HEALTH_CHANNEL", "CDEV")
    assert wtp.main(["--post"]) == 0
    (ch, head, parts), (dev_ch, dev) = posts
    assert ch == "C0AGVSUNEFP" and parts == [] and "No weekly test results came in this week" in "\n".join(head)
    assert dev_ch == "CDEV" and "InfraBot v2 weekly" in dev
