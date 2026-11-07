"""reply_monitor: reads every account (and says which it could not), labels Melior by the campaign's client_id,
posts only new ALERTs on weekdays and one summary on Mondays, and only when REPLY_MONITOR_POST=on. Fakes only."""
import asyncio
from datetime import date, timedelta

import httpx
import pytest

import reply_monitor as rm
from smartlead import notify
from smartlead import reply_stats as rs
from smartlead.config import AccountConfig

MON, TUE = date(2026, 11, 9), date(2026, 11, 10)
KEY = "sl-secret-key-123"


def _status_error(code, text):
    req = httpx.Request("GET", f"https://server.smartlead.ai/api/v1/campaigns?api_key={KEY}")
    resp = httpx.Response(code, text=text, request=req)
    return httpx.HTTPStatusError(f"{code} on /api/v1/campaigns: {text}", request=req, response=resp)


class FakeSmartlead:
    campaigns = {
        "PRECISE_LEADS": [
            {"id": 1, "name": "PL | Trade shows", "status": "ACTIVE", "client_id": None},
            {"id": 2, "name": "Melior - Agencies", "status": "ACTIVE", "client_id": 12256},
            {"id": 3, "name": "OSC old", "status": "ACTIVE", "client_id": 145916},       # past client: skipped
            {"id": 4, "name": "PL | Paused", "status": "PAUSED", "client_id": None},
        ],
        "BETTRDATA": [{"id": 9, "name": "Ingest", "status": "ACTIVE", "client_id": None}],
    }
    rows = {
        "1": [{"from_email": "a@pl.com", "sent_count": 300, "reply_count": 9, "bounce_count": 3,
               "sender_bounce_count": 1}],
        "2": [{"from_email": "b@mel.com", "sent_count": 200, "reply_count": 2}],
        "3": [{"from_email": "c@osc.com", "sent_count": 50, "reply_count": 0}],
        "4": [{"from_email": "d@pl.com", "sent_count": 70, "reply_count": 0}],
    }
    fail: dict = {}
    calls: list = []

    def __init__(self, key, name):
        self.name = name

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def list_campaigns(self):
        if self.fail.get(self.name) == "list":
            raise _status_error(401, '{"message":"Plan expired!"}')
        return self.campaigns[self.name]

    async def get_campaign_mailbox_statistics(self, cid):
        self.calls.append(cid)
        if self.fail.get(self.name) == cid:
            raise _status_error(500, "boom")
        return self.rows.get(cid, [])


@pytest.fixture
def sl(monkeypatch):
    FakeSmartlead.fail, FakeSmartlead.calls = {}, []
    monkeypatch.setattr(rm, "SmartleadClient", FakeSmartlead)
    monkeypatch.setattr(rm, "discover_accounts", lambda: [
        AccountConfig(name="PRECISE_LEADS", api_key=KEY, sheet_id="s"),
        AccountConfig(name="BETTRDATA", api_key="k2", sheet_id="s")])
    FakeSmartlead.rows["9"] = [{"from_email": "e@bd.com", "sent_count": 80, "reply_count": 1}]
    return FakeSmartlead


# ── reading Smartlead ────────────────────────────────────────────────────────

def test_collect_labels_melior_by_client_id_and_skips_past_clients_and_paused(sl):
    fleet, unread = asyncio.run(rm.collect())
    assert unread == {}
    assert set(fleet) == {"pl.com", "mel.com", "bd.com"} and "3" not in sl.calls and "4" not in sl.calls
    assert fleet["mel.com"]["client"] == "Melior" and fleet["mel.com"]["account"] == "PRECISE_LEADS"
    assert fleet["pl.com"]["client"] == "Precise Leads" and fleet["bd.com"]["client"] == "Bettrdata"
    assert fleet["pl.com"]["bounce_count"] == 3 and fleet["pl.com"]["sender_bounce_count"] == 1
    assert fleet["pl.com"]["campaigns"]["1"]["sent"] == 300


def test_an_account_it_cannot_read_is_named_and_never_shows_the_key(sl, capsys):
    sl.fail = {"PRECISE_LEADS": "list"}
    fleet, unread = asyncio.run(rm.collect())
    assert set(fleet) == {"bd.com"}
    assert unread == {"PRECISE_LEADS": "HTTP 401 Plan expired!"}
    out = capsys.readouterr().out
    assert "Not checked today: Precise Leads and Melior" in out and KEY not in out


def test_a_half_read_account_is_left_out_whole(sl):
    sl.fail = {"PRECISE_LEADS": "2"}                     # campaign 1 read, then campaign 2 failed
    fleet, unread = asyncio.run(rm.collect())
    assert set(fleet) == {"bd.com"} and unread["PRECISE_LEADS"].startswith("HTTP 500")


# ── history ──────────────────────────────────────────────────────────────────

def test_old_snapshots_get_the_client_label_and_keep_the_account():
    old = rm.legacy_client({"domain": "x.com", "date": "2026-11-01", "client": "PRECISE_LEADS", "sent": 5})
    assert old["client"] == "Precise Leads" and old["account"] == "PRECISE_LEADS"
    new = {"domain": "x.com", "client": "Melior", "account": "PRECISE_LEADS"}
    assert rm.legacy_client(new) is new


def test_prior_verdict_is_the_last_judged_day():
    hist = [{"domain": "a.com", "date": "2026-11-05", "verdict": "ALERT"},
            {"domain": "a.com", "date": "2026-11-07", "verdict": "OK"},
            {"domain": "a.com", "date": "2026-11-08"},                     # an old-format day, no verdict
            {"domain": "b.com", "date": "2026-11-06", "verdict": "WATCH"}]
    assert rm.prior_verdicts(hist) == {"a.com": "OK", "b.com": "WATCH"}


# ── what it says ─────────────────────────────────────────────────────────────

def _history(today, bad_every=304, client="Precise Leads", verdict=None):
    """15 days before `today`: bad.com (22 a day) and three healthy domains (25 a day, 2.3%) in campaign 1."""
    docs = []
    for i in range(15, 0, -1):
        day = (today - timedelta(days=i)).isoformat()
        for dom, per_day, every, start in (("bad.com", 22, bad_every, 0), ("ok0.com", 25, 44, 1000),
                                           ("ok1.com", 25, 44, 1000), ("ok2.com", 25, 44, 1000)):
            sent = start + per_day * (15 - i)
            doc = {"domain": dom, "date": day, "client": client, "account": "PRECISE_LEADS", "sent": sent,
                   "replies": sent // every, "campaigns": {"1": {"sent": sent, "replies": sent // every,
                                                                 "client": client}}}
            if verdict and i == 1 and dom == "bad.com":
                doc["verdict"] = verdict
            docs.append(doc)
    return docs


def _fleet(today, bad_every=304, client="Precise Leads"):
    out = {}
    for dom, per_day, every, start in (("bad.com", 22, bad_every, 0), ("ok0.com", 25, 44, 1000),
                                       ("ok1.com", 25, 44, 1000), ("ok2.com", 25, 44, 1000)):
        sent = start + per_day * 15
        out[dom] = {"domain": dom, "client": client, "account": "PRECISE_LEADS", "sent": sent,
                    "replies": sent // every, "bounce_count": 0, "sender_bounce_count": 0,
                    "campaigns": {"1": {"sent": sent, "replies": sent // every, "client": client}}}
    return out


def _headline_ok(head):
    assert 1 <= len(head) <= 4 and not any("@" in line for line in head)


def test_a_new_alert_is_posted_once():
    verdicts, posts = rm.run(TUE, _fleet(TUE), {}, _history(TUE), weekly=False)
    assert verdicts["bad.com"]["verdict"] == rs.ALERT
    assert len(posts) == 1
    head, thread = posts[0]
    _headline_ok(head)
    assert head[0] == ("⚠️ Reply-rate alert, 10 Nov: 1 drop, a sending domain with far fewer replies than "
                       "expected over 14 days")
    assert head[1].startswith("• *bad.com* (Precise Leads): 1 reply on 308 sends in 14 days, ")
    assert "expected at the reply rate of the other domains in its campaigns (0.1" in head[1]
    assert thread == [rm.HOW] and "nothing was paused or changed" in rm.HOW
    # the next day it is still an ALERT, and nothing is posted again
    _, posts = rm.run(TUE, _fleet(TUE), {}, _history(TUE, verdict=rs.ALERT), weekly=False)
    assert posts == []


def test_a_quiet_weekday_posts_nothing():
    verdicts, posts = rm.run(TUE, _fleet(TUE, bad_every=44), {}, _history(TUE, bad_every=44), weekly=False)
    assert all(v["verdict"] == rs.OK for v in verdicts.values()) and posts == []


def test_an_unread_account_is_posted_even_without_alerts():
    _, posts = rm.run(TUE, {}, {"PRECISE_LEADS": "HTTP 401 Plan expired!"}, [], weekly=False)
    head, thread = posts[0]
    assert head == ["⚠️ Reply check, 10 Nov: 1 Smartlead account not read",
                    "Not checked today: Precise Leads and Melior (HTTP 401 Plan expired!)"] and thread == []


def test_an_alert_and_an_unread_account_share_one_post():
    _, posts = rm.run(TUE, _fleet(TUE), {"BETTRDATA": "HTTP 500"}, _history(TUE), weekly=False)
    head, _ = posts[0]
    _headline_ok(head)
    assert head[1] == "Not checked today: Bettrdata (HTTP 500)" and head[2].startswith("• *bad.com*")


def test_a_long_alert_list_goes_to_the_thread():
    lines = [rm._alert_line(f"d{i}.com", "Melior", "x") for i in range(3)]
    head, thread = rm._alert_parts(lines, TUE, note="Not checked today: Bettrdata (HTTP 500)")
    _headline_ok(head)
    assert head[-1] == "The list is in the thread." and all(f"d{i}.com" in thread[0] for i in range(3))


def test_monday_summary_counts_add_up_and_names_every_flagged_domain():
    fleet = _fleet(MON)
    fleet["new.com"] = {**fleet["ok0.com"], "domain": "new.com"}           # first day: nothing to judge yet
    hist = _history(MON)
    for i in range(15, -1, -1):                                             # few.com: 3 sends a day, under 50
        doc = {**fleet["ok0.com"], "domain": "few.com", "sent": 1000 - 3 * i, "replies": 0,
               "campaigns": {"1": {"sent": 1000 - 3 * i, "replies": 0, "client": "Precise Leads"}}}
        if i:
            hist.append({**doc, "date": (MON - timedelta(days=i)).isoformat()})
        else:
            fleet["few.com"] = doc
    verdicts, posts = rm.run(MON, fleet, {"BETTRDATA": "HTTP 401 Plan expired!"}, hist, weekly=True)
    assert len(posts) == 1
    head, thread = posts[0]
    _headline_ok(head)
    assert head[:2] == ["⚠️ Reply check, 14 days to 9 Nov: 5 sending domains, 1 alert, 0 to watch",
                        "Not checked today: Bettrdata (HTTP 401 Plan expired!)"]
    assert head[2].startswith("• *bad.com* (Precise Leads): new ALERT today, 1 reply on 308 sends") and len(head) == 3
    body = thread[0]
    sending = [v for v in verdicts.values() if v["verdict"] not in (rs.NO_SENDS, rs.NOT_COUNTED)]
    sent, replies = sum(v["sent"] for v in sending), sum(v["replies"] for v in sending)
    assert (sent, replies) == (308 + 3 * 350 + 42, 1 + 3 * 8)              # the sends shown are these domains'
    assert body.startswith(f"*Precise Leads*: {sent:,} sends, {replies} replies (1.8%) in 14 days from "
                           "5 sending domains\n")
    assert "• *bad.com*: ALERT, 1 reply on 308 sends in 14 days" in body
    assert "• 3 in line with the other domains, 1 under 50 sends (not judged)" in body
    assert "• Also listed in active campaigns: 1 new or moved with no sends counted yet" in body
    assert 1 + 3 + 1 == len(sending) == 5 and len(verdicts) == 6            # alert + fine + under 50, + new
    assert verdicts["new.com"]["verdict"] == rs.NOT_COUNTED
    assert thread[1] == rm.HOW


def test_a_domain_whose_totals_never_move_is_not_counted_as_sending():
    # review 7 Nov: idle domains keep their rows in ACTIVE campaigns' stats; "1 domain sending today" next to
    # "0 sends" contradicted itself. idle.com (PL, campaign 1) and bd.com (Bettrdata) stay at 40 lifetime sends.
    fleet, hist = _fleet(MON), _history(MON)
    for dom, client, cid in (("idle.com", "Precise Leads", "1"), ("bd.com", "Bettrdata", "9")):
        doc = {"domain": dom, "client": client, "account": "X", "sent": 40, "replies": 0, "bounce_count": 0,
               "sender_bounce_count": 0, "campaigns": {cid: {"sent": 40, "replies": 0, "client": client}}}
        fleet[dom] = doc
        hist += [{**doc, "date": (MON - timedelta(days=i)).isoformat()} for i in range(15, 0, -1)]
    verdicts, posts = rm.run(MON, fleet, {}, hist, weekly=True)
    assert verdicts["idle.com"]["verdict"] == verdicts["bd.com"]["verdict"] == rs.NO_SENDS
    head, thread = posts[0]
    assert head[0] == "⚠️ Reply check, 14 days to 9 Nov: 4 sending domains, 1 alert, 0 to watch"
    bd, pl = thread[0].split("\n\n")
    assert bd == ("*Bettrdata*: no sends counted in 14 days\n"
                  "• Also listed in active campaigns: 1 with no sends in 14 days")
    assert pl.startswith("*Precise Leads*: 1,358 sends, 25 replies (1.8%) in 14 days from 4 sending domains\n")
    assert pl.endswith("• 3 in line with the other domains\n"
                       "• Also listed in active campaigns: 1 with no sends in 14 days")
    assert "sending today" not in thread[0]


def test_without_history_it_says_nothing_was_judged_instead_of_a_clean_week():
    verdicts, posts = rm.run(MON, _fleet(MON), {"BETTRDATA": "HTTP 500"}, None, weekly=True)
    assert verdicts == {}
    assert posts == [(["⚠️ Reply check, 9 Nov: nothing judged today",
                       "No stored history to compare with (Mongo not reachable).",
                       "Not checked today: Bettrdata (HTTP 500)"], [])]


def test_monday_summary_does_not_repeat_an_old_alert_in_the_headline():
    _, posts = rm.run(MON, _fleet(MON), {}, _history(MON, verdict=rs.ALERT), weekly=True)
    head, thread = posts[0]
    assert head == ["⚠️ Reply check, 14 days to 9 Nov: 4 sending domains, 1 alert, 0 to watch"]
    assert "• *bad.com*: ALERT, " in thread[0]


def test_monday_summary_without_alerts_has_no_warning_sign():
    _, posts = rm.run(MON, _fleet(MON, bad_every=44), {}, _history(MON, bad_every=44), weekly=True)
    head, thread = posts[0]
    assert head == ["Reply check, 14 days to 9 Nov: 4 sending domains, 0 alerts, 0 to watch"]
    assert "• 4 in line with the other domains" in thread[0]


def test_every_message_has_blank_lines_between_parts(monkeypatch):
    # what Slack receives: the title, a blank line, then the body; thread paragraphs apart
    _, posts = rm.run(MON, _fleet(MON), {"BETTRDATA": "HTTP 500"}, _history(MON), weekly=True)
    head, thread = posts[0]
    assert notify.tidy("\n".join(head)).split("\n")[1] == ""
    assert "\n\n" in rm.HOW and all(p.strip() for p in rm.HOW.split("\n\n"))


# ── posting ──────────────────────────────────────────────────────────────────

class FakeSlack:
    def __init__(self, ok=True):
        self.ok, self.calls = ok, []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(dict(json))
        body = {"ok": True, "ts": f"{len(self.calls)}.0"} if self.ok else {"ok": False, "error": "ratelimited"}
        return type("R", (), {"json": lambda self_: body})()


@pytest.fixture
def slack(monkeypatch):
    def install(ok=True):
        fake = FakeSlack(ok)
        monkeypatch.setattr(notify.requests, "post", fake.post)
        monkeypatch.setattr(notify.time, "sleep", lambda s: None)
        notify._REROUTED.clear()
        return fake
    return install


def test_nothing_is_posted_unless_switched_on(monkeypatch, slack):
    fake = slack()
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.delenv("REPLY_MONITOR_POST", raising=False)
    assert rm._post(["Reply check"], []) is None and fake.calls == []


def test_switched_on_it_posts_to_the_infra_channel(monkeypatch, slack):
    fake = slack()
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.setenv("REPLY_MONITOR_POST", "on")
    monkeypatch.delenv("REPLY_MONITOR_CHANNEL", raising=False)
    monkeypatch.delenv("HEALTH_NOTIFY_CHANNEL", raising=False)
    assert rm._post(["Reply check", "line"], ["detail"]) is True
    assert [c["channel"] for c in fake.calls] == ["C0AGVSUNEFP", "C0AGVSUNEFP"]


def test_a_failed_post_makes_the_job_exit_non_zero(monkeypatch, slack, capsys):
    slack(ok=False)
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.setenv("REPLY_MONITOR_POST", "on")

    async def collect():
        return {}, {"PRECISE_LEADS": "HTTP 401 Plan expired!"}
    monkeypatch.setattr(rm, "collect", collect)
    monkeypatch.setattr(rm, "_col", lambda: None)
    assert asyncio.run(rm.main()) == 1
    out = capsys.readouterr().out
    assert "Not checked today: Precise Leads and Melior (HTTP 401 Plan expired!)" in out
    assert "Slack post failed" in out


class FakeCol:
    def __init__(self, docs=()):
        self.ops, self.docs, self.queries = [], list(docs), []

    def find(self, query, projection=None):
        self.queries.append(query)
        return [dict(d) for d in self.docs if query["date"]["$gte"] <= d["date"] < query["date"]["$lt"]]

    def bulk_write(self, ops, ordered=False):
        self.ops += ops


def test_the_job_end_to_end_with_fakes(monkeypatch, capsys):
    # old-format history (account name as client, no campaigns) for bad.com and three healthy domains
    hist = [{**{k: v for k, v in d.items() if k not in ("campaigns", "account")}, "client": "PRECISE_LEADS"}
            for d in _history(TUE)]
    col = FakeCol(hist)
    monkeypatch.setattr(rm, "_col", lambda: col)
    monkeypatch.setattr(rm, "datetime", type("D", (), {"now": staticmethod(lambda tz=None: _at(TUE))}))
    monkeypatch.delenv("REPLY_MONITOR_POST", raising=False)

    async def collect():
        return _fleet(TUE), {}
    monkeypatch.setattr(rm, "collect", collect)
    assert asyncio.run(rm.main()) == 0
    out = capsys.readouterr().out
    assert "[ALERT] bad.com (Precise Leads): 1 reply on 308 sends in 14 days" in out
    assert "the client's other domains" in out                     # old snapshots: judged at the client's rate
    assert ("[ReplyMon] 4 domain(s): ALERT 1, WATCH 0, OK 3, TOO FEW 0, NO REFERENCE 0, NO SENDS 0, "
            "NOT COUNTED YET 0") in out
    assert col.queries[0]["date"]["$lt"] == "2026-11-10"
    assert {op._doc["$set"]["verdict"] for op in col.ops} == {rs.ALERT, rs.OK} and len(col.ops) == 4


def _at(d):
    from datetime import datetime as real
    return real(d.year, d.month, d.day, 13, 0)


def test_saved_snapshot_keeps_bounces_campaigns_and_the_verdict():
    col = FakeCol()
    fleet = _fleet(TUE)
    verdicts, _ = rm.run(TUE, fleet, {}, _history(TUE), weekly=False)
    rm._save(col, TUE, fleet, verdicts)
    saved = {op._filter["domain"]: op._doc["$set"] for op in col.ops}
    bad = saved["bad.com"]
    assert bad["date"] == "2026-11-10" and bad["verdict"] == rs.ALERT and bad["client"] == "Precise Leads"
    assert {"bounce_count", "sender_bounce_count", "campaigns", "account", "window_sent", "expected", "p"} <= set(bad)
