"""v2_weekly.run() end to end (audit PLACEMENT-11): a temp Downloads folder, a fake fleet, a fake Smartlead,
a fake Slack post and a fake Mongo, driven hour by hour with a fake clock. No network."""
import asyncio
import os
from datetime import datetime
from types import SimpleNamespace

import pytest

import v2_weekly as wk
from smartlead import v2_results as v2
from smartlead.config import AccountConfig

HEAD = "Mailbox,Provider,Landed,Sent at,Send status\n"
MON9 = "2026-11-09T14:31:00.000Z"            # Monday 9 Nov, 20:01 IST

# email: (Smartlead id, client). mel-ok sits on its own domain: next to mel-bad it would make y.com 1 of 2 in
# spam, a failed domain (11 Nov 2026 rule), and these tests are about single inboxes.
INBOXES = {"pl-ok1@x.com": (10, "Precise Leads"), "pl-ok2@x.com": (11, "Precise Leads"),
           "pl-bad@x.com": (20, "Precise Leads"), "mel-bad@y.com": (30, "Melior"), "mel-ok@w.com": (31, "Melior"),
           "pl-other@z.com": (40, "Precise Leads")}
DT_PL_1 = {"pl-ok1@x.com": (4, 4), "pl-ok2@x.com": (4, 4), "pl-bad@x.com": (0, 4)}
DT_MELIOR_1 = {"mel-bad@y.com": (4, 0), "mel-ok@w.com": (4, 4)}
TYPES = {}          # email: Smartlead inbox type (GMAIL / OUTLOOK), for the tests that need one; else unknown
EXTRA_DT = {}       # more "DT ..." campaigns for one test, e.g. a copy-check campaign
WED11 = "2026-11-11T05:00:00.000Z"           # Wednesday 11 Nov, 10:30 IST


def at(day, hour, minute=0):
    return datetime(2026, 11, day, hour, minute, tzinfo=v2.IST)


def write_csv(folder, name, inboxes, sent=MON9, header=HEAD, when=None):
    """inboxes: {email: (google_inbox_of_4, microsoft_inbox_of_4)}"""
    lines = [header]
    for mb, (g, m) in inboxes.items():
        lines += [f"{mb},GOOGLE_WORKSPACE,{'Inbox' if i < g else 'Spam'},{sent},SENT\n" for i in range(4)]
        lines += [f"{mb},MICROSOFT_365,{'Inbox' if i < m else 'Spam'},{sent},SENT\n" for i in range(4)]
    p = folder / name
    p.write_text("".join(lines), encoding="utf-8")
    t = (when or at(10, 8, 50)).timestamp()
    os.utime(p, (t, t))
    return p


class FakeSL:
    """Smartlead for the whole run: campaign 1 'PL | A', campaign 2 'Melior | B'. Records every call."""

    def __init__(self):
        self.senders = {1: {10, 11, 20}, 2: {30, 31}}
        self.names = {1: "PL | A", 2: "Melior | B"}
        self.tagged = set()
        self.tags = []
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def list_tags(self):
        return list(self.tags)

    async def _request_json(self, method, endpoint, body):
        self.calls.append((method, endpoint, body))
        if endpoint == "/tags":
            self.tags.append({"id": 77, "name": body["name"]})
        return {"leads_reallocated": 0} if "reallocate" in endpoint else {"ok": True}

    async def _get(self, endpoint, params=None):
        return [{"id": 1}]

    async def assign_tags(self, ids, tag_ids):
        self.calls.append(("tag", sorted(ids), list(tag_ids)))
        self.tagged |= set(ids)

    async def get_campaign_leads(self, cid):
        return []

    async def remove_campaign_email_accounts(self, cid, ids):
        self.calls.append(("remove", int(cid), sorted(ids)))
        self.senders[int(cid)] -= set(ids)

    async def get_campaign_email_accounts(self, cid):
        return [{"id": i} for i in self.senders[int(cid)]]

    async def resume_lead(self, cid, lid):
        self.calls.append(("resume", int(cid), lid))

    async def add_campaign_email_accounts(self, cid, ids):
        self.calls.append(("add", int(cid), list(ids)))
        self.senders[int(cid)] |= set(ids)


def fleet_from(sl):
    inboxes = {e: {"id": i, "client": c, "tags": [wk.TAG] if i in sl.tagged else [], "type": TYPES.get(e, "")}
               for e, (i, c) in INBOXES.items()}
    return {"PRECISE_LEADS": {"key": "k", "inboxes": inboxes,
                              "camps": {cid: {"name": sl.names[cid], "status": "ACTIVE", "senders": set(ids)}
                                        for cid, ids in sl.senders.items()},
                              "dt": {9: {"name": "DT PL #1", "status": "ACTIVE", "senders": {10, 11, 20}},
                                     8: {"name": "DT Melior #1", "status": "ACTIVE", "senders": {30, 31}},
                                     7: {"name": "DT PL #2", "status": "ACTIVE", "senders": {40}}, **EXTRA_DT}}}


class FakeCol:
    def __init__(self):
        self.docs, self.fail = {}, None

    def bulk_write(self, ops, ordered=True):
        if self.fail:
            raise self.fail
        for op in ops:
            self.docs[op._doc["_id"]] = dict(op._doc)

    def replace_one(self, flt, doc, upsert=False):
        if self.fail:
            raise self.fail
        self.docs[flt["_id"]] = dict(doc)


class FakeDB:
    name = "infrabot"

    def __init__(self):
        self.cols = {}

    def __getitem__(self, k):
        return self.cols.setdefault(k, FakeCol())


@pytest.fixture
def job(tmp_path, monkeypatch):
    h = SimpleNamespace(clock=at(10, 9), posts=[], post_ok=True, db=FakeDB(), db_down=None, sl=FakeSL(),
                        fleet_error=None, fleet_calls=0, downloads=tmp_path / "Downloads", state_dir=tmp_path / "state")
    h.downloads.mkdir()
    st = wk.load_state(h.state_dir)
    st["since"] = "2026-11-07"                                    # the job's first day, as on the PC
    wk.save_state(h.state_dir, st)
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.setattr(wk, "now_ist", lambda: h.clock)

    def fake_post(head, parts):
        h.posts.append((head, parts))
        return h.post_ok
    monkeypatch.setattr(wk, "post", fake_post)

    async def fake_load_fleet():
        h.fleet_calls += 1
        if h.fleet_error:
            raise h.fleet_error
        return fleet_from(h.sl)
    monkeypatch.setattr(wk, "load_fleet", fake_load_fleet)
    monkeypatch.setattr(wk, "_client", lambda account, key: h.sl)

    def fake_db():
        if h.db_down:
            raise h.db_down
        return h.db
    monkeypatch.setattr(wk, "_mongo_db", fake_db)
    monkeypatch.setattr(wk, "git_facts", lambda: ("abc1234", 2))

    def go(**kw):
        args = SimpleNamespace(folder=str(h.downloads), state_dir=str(h.state_dir), apply=True, warn=False,
                               act_from=None, post=True, history_only=False, replan=False, act_clients="",
                               env_dir=None, log=None, self_check=False)
        for k, v in kw.items():
            setattr(args, k, v)
        return asyncio.run(wk.run(args))
    h.run = go
    h.state = lambda: wk.load_state(h.state_dir)
    h.heartbeat = lambda: h.db.cols.get(wk.HEARTBEAT_COLLECTION, FakeCol()).docs.get("pc")
    h.verdicts = lambda: h.db.cols.get(wk.VERDICTS_COLLECTION, FakeCol()).docs
    return h


def download_monday(h):
    write_csv(h.downloads, "dt-pl-1.csv", DT_PL_1)
    write_csv(h.downloads, "dt-melior-1.csv", DT_MELIOR_1)


# --- the week ------------------------------------------------------------------------

def test_watch_week_waits_for_the_night_to_settle_then_reports_without_changing_anything(job, capsys):
    download_monday(job)
    assert job.run(act_from="2026-11-16") == 0
    out = capsys.readouterr().out
    assert "waiting: files for 2026-11-09 arrived" in out and job.posts == [] and job.fleet_calls == 0
    hb = job.heartbeat()
    assert hb["newest_night"] == "2026-11-09" and hb["files_read"] == 2 and hb["mode"] == "warn"
    assert hb["commit"] == "abc1234" and hb["behind_main"] == 2 and hb["errors"] == []
    assert job.verdicts() == {}                                   # nothing sent before the night settles

    job.clock = at(10, 10, 31)                                   # 91 minutes after the files came in
    assert job.run(act_from="2026-11-16") == 0
    assert job.sl.calls == []                                     # watch week: Smartlead untouched
    (head, parts), = job.posts
    assert head[0] == "v2 placement results, tests of Mon 9 Nov"
    assert head[1] == "5 inboxes · 3 pass · 0 watch · 2 fail"
    assert head[2] == "Would take 1 inbox out of 1 live campaign · would tag 0 in no campaign · would return 0"
    assert head[3] == ("Watch week: nothing is changed in Smartlead until 2026-11-16, then the job acts on its own "
                       "(Melior stays report-only).")
    text = "\n".join(parts)
    assert ("• fail · x.com · pl-bad@x.com · Google 0/4, Microsoft 4/4 → would be taken out of PL | A; "
            "would be tagged DO NOT USE") in text
    assert ("• fail · y.com · mel-bad@y.com · Google 4/4, Microsoft 0/4 → would be taken out of Melior | B "
            "(Melior: report-only until the job is proven on the other clients); nothing changed in Smartlead") in text
    assert "No result this week from DT PL #2 (1 inbox): download its CSV" in parts[0]
    assert "2 commits behind main" in text
    v = job.verdicts()
    assert set(v) == {f"{e}|2026-11-09" for e in DT_PL_1} | {f"{e}|2026-11-09" for e in DT_MELIOR_1}
    bad = v["pl-bad@x.com|2026-11-09"]
    assert bad["verdict"] == v2.FAIL and bad["client"] == "Precise Leads" and bad["weekly"] is True
    assert bad["history"] is False and bad["pulled_on"] is None and bad["domain"] == "x.com"
    assert bad["google"] == {"inbox": 0, "reported": 4, "sent": 4} and bad["updated_at"].endswith("+00:00")
    assert job.heartbeat()["last_post_ok"] is True


def test_the_act_from_date_switches_it_from_reporting_to_acting(job):
    download_monday(job)
    job.run(act_from="2026-11-11")
    job.clock = at(10, 10, 31)
    job.run(act_from="2026-11-11")
    assert job.sl.calls == [] and job.posts[-1][0][2].startswith("Would take 1 inbox")

    job.clock = at(11, 10)
    assert job.run(act_from="2026-11-11", replan=True) == 0
    assert ("tag", [20], [77]) in job.sl.calls and ("remove", 1, [20]) in job.sl.calls
    assert not any(30 in (c[2] if isinstance(c[2], list) else []) for c in job.sl.calls)     # Melior untouched
    head, parts = job.posts[-1]
    assert head[2] == "Took 1 inbox out of 1 live campaign · tagged 0 in no campaign · returned 0"
    assert "pl-bad@x.com · Google 0/4, Microsoft 4/4 → taken out of PL | A; tagged DO NOT USE" in "\n".join(parts)
    st = job.state()
    assert set(st["pulled"]) == {"pl-bad@x.com"} and st["pulled"]["pl-bad@x.com"]["on"] == "2026-11-11"
    assert job.verdicts()["pl-bad@x.com|2026-11-09"]["pulled_on"] == "2026-11-11"
    assert job.verdicts()["mel-bad@y.com|2026-11-09"]["pulled_on"] is None
    assert job.heartbeat()["mode"] == "act"


def test_melior_is_acted_on_only_when_named_in_act_clients(job):
    download_monday(job)
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run(act_clients="Melior") == 0
    assert ("remove", 1, [20]) in job.sl.calls and ("remove", 2, [30]) in job.sl.calls
    assert set(job.state()["pulled"]) == {"pl-bad@x.com", "mel-bad@y.com"}
    assert "report-only" not in "\n".join(job.posts[-1][1])


def test_report_only_melior_is_reported_by_what_would_happen_and_never_touched(job, monkeypatch):
    # review V2-S2: an acting run with one failing Melior inbox of each kind: in a campaign with a good sender
    # left, the last good sender of a live campaign, and in no campaign. PL is acted on; Melior only reported.
    monkeypatch.setitem(INBOXES, "mel-solo@y.com", (32, "Melior"))
    monkeypatch.setitem(INBOXES, "mel-idle@y.com", (33, "Melior"))
    job.sl.senders[3], job.sl.names[3] = {32}, "Melior | Solo"
    write_csv(job.downloads, "dt-pl-1.csv", DT_PL_1)
    write_csv(job.downloads, "dt-melior-1.csv", {**DT_MELIOR_1, "mel-solo@y.com": (0, 4), "mel-idle@y.com": (4, 1)})
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    assert [c for c in job.sl.calls if c[0] in ("tag", "remove")] == [("tag", [20], [77]), ("remove", 1, [20])]
    assert not any("/campaigns/2/" in str(c) or "/campaigns/3/" in str(c) for c in job.sl.calls)
    assert job.sl.senders[2] == {30, 31} and job.sl.senders[3] == {32} and job.sl.tagged == {20}
    assert set(job.state()["pulled"]) == {"pl-bad@x.com"}
    head, parts = job.posts[-1]
    assert head[1] == "7 inboxes · 3 pass · 0 watch · 4 fail"
    assert head[2] == "Took 1 inbox out of 1 live campaign · tagged 0 in no campaign · returned 0"
    assert head[-1] == "\n*Needs you: 3*"
    assert parts[0].split("\n")[2:] == [
        "• Melior is report-only until the job is proven on the other clients, so nothing changed in Smartlead for it. The job would take 1 inbox "
        "out of campaigns, leave 1 inbox in a live campaign where it is the last good sender and tag 1 inbox that is "
        "in no campaign. List under *Melior* below.",
        "• Melior | Solo (3) is live with only failed inboxes left (mel-solo@y.com). Not changed, as Melior is "
        "report-only until the job is proven on the other clients: ask Ryan to pause it or add a good sender.",
        "• No result this week from DT PL #2 (1 inbox): download its CSV into Downloads."]
    ro = " (Melior: report-only until the job is proven on the other clients); nothing changed in Smartlead"
    melior = next(p for p in parts if p.startswith("*Melior*"))
    assert melior.split("\n")[2:] == [
        "• fail · y.com · mel-bad@y.com · Google 4/4, Microsoft 0/4 → would be taken out of Melior | B" + ro,
        "• fail · y.com · mel-solo@y.com · Google 0/4, Microsoft 4/4 → last good sender of live Melior | Solo, so it "
        "would stay there" + ro,
        "• fail · y.com · mel-idle@y.com · Google 4/4, Microsoft 1/4 → in no campaign; would be tagged DO NOT USE" + ro]
    assert job.verdicts()["mel-solo@y.com|2026-11-09"]["pulled_on"] is None


def test_a_refused_post_exits_non_zero_is_logged_and_queued_then_sent(job, capsys):
    download_monday(job)
    job.run()
    job.clock = at(10, 10, 31)
    job.post_ok = False
    assert job.run(act_from="2026-11-16") == 3
    assert "UNPOSTED: v2 placement results, tests of Mon 9 Nov" in capsys.readouterr().out
    assert len(job.state()["unposted"]) == 1
    hb = job.heartbeat()
    assert hb["last_post_ok"] is False and any("queued" in e for e in hb["errors"])

    job.clock = at(10, 11, 31)
    job.post_ok = True
    assert job.run(act_from="2026-11-16") == 0
    assert job.posts[-1][0][0] == "v2 placement results, tests of Mon 9 Nov" and job.state()["unposted"] == []
    assert job.heartbeat()["last_post_ok"] is True and job.heartbeat()["errors"] == []


def test_the_tuesday_reminder_names_every_dt_test_with_no_result_once(job):
    job.clock = at(7, 13)
    job.run()                                                     # the job's first day
    st = job.state()
    st["dt_tests"] = {"DT PL #1": ["pl-ok1@x.com"], "DT PL #2": ["pl-other@z.com"], "DT Melior #4": []}
    wk.save_state(job.state_dir, st)
    job.clock = at(10, 11)
    job.run()
    assert job.posts == []                                        # not before Tuesday 12:00
    job.clock = at(10, 12)
    assert job.run() == 0
    (head, _), = job.posts
    assert head[0] == "v2 results for the week of Mon 9 Nov are not in yet"
    assert head[1] == "\nMissing: DT PL #1, DT PL #2 (2 tests)."
    job.clock = at(10, 13)
    job.run()
    assert len(job.posts) == 1                                    # once per Monday


def test_the_reminder_names_the_dt_test_still_missing_after_others_came_in(job):
    download_monday(job)                                          # DT PL #1 and DT Melior #1; DT PL #2 missing
    job.run(act_from="2026-11-16")
    job.clock = at(10, 10, 31)
    job.run(act_from="2026-11-16")                                # the report already names DT PL #2
    job.clock = at(10, 12)
    assert job.run(act_from="2026-11-16") == 0
    head, _ = job.posts[-1]
    assert head[0] == "v2 results for the week of Mon 9 Nov: 1 DT test not in yet" and head[1] == "\nMissing: DT PL #2."
    job.clock = at(10, 14)
    job.run(act_from="2026-11-16")
    assert len(job.posts) == 2


def test_a_crash_alerts_once_a_day_and_a_refused_alert_is_tried_again(job):
    download_monday(job)
    job.run()
    job.fleet_error = RuntimeError("Smartlead said no ?api_key=abc123")
    job.clock = at(10, 10, 31)
    job.post_ok = False
    assert job.run() == 2
    assert job.posts[-1][0][0] == "v2 weekly job failed" and job.state()["failed_on"] is None
    job.post_ok = True
    job.clock = at(10, 11, 31)
    assert job.run() == 2
    head, _ = job.posts[-1]
    assert "abc123" not in head[1] and "api_key=<hidden>" in head[1]
    assert job.state()["failed_on"] == "2026-11-10"
    n = len(job.posts)
    job.clock = at(10, 12, 31)
    assert job.run() == 2 and len(job.posts) == n                 # still failing, already said today
    assert job.heartbeat()["errors"][0].startswith("crashed: RuntimeError")
    assert all("abc123" not in e for e in job.heartbeat()["errors"])


def test_mongo_trouble_never_breaks_the_run(job, capsys):
    download_monday(job)
    job.run()
    job.clock = at(10, 10, 31)
    job.db.cols[wk.VERDICTS_COLLECTION] = FakeCol()
    job.db.cols[wk.VERDICTS_COLLECTION].fail = RuntimeError("write refused")
    assert job.run(act_from="2026-11-16") == 0 and len(job.posts) == 1
    assert any(e.startswith("v2_verdicts not written: RuntimeError") for e in job.heartbeat()["errors"])
    job.db_down = RuntimeError("no route to mongodb+srv://user:pw@cluster.example.net")
    job.clock = at(10, 11, 31)
    assert job.run(act_from="2026-11-16") == 0
    out = capsys.readouterr().out
    assert "Mongo not reached" in out and "user:pw" not in out


def test_an_unrecognised_v2_csv_is_posted_once(job):
    write_csv(job.downloads, "dt-new-format.csv", {"a@x.com": (4, 4)},
              header="Mailbox,Provider,Landed,Sent at,Send status,Seed\n")
    job.clock = at(10, 10)
    assert job.run() == 0
    (head, _), = job.posts
    assert head[0] == "unrecognised v2 CSV format: dt-new-format.csv"
    job.clock = at(10, 11)
    job.run()
    assert len(job.posts) == 1


def test_a_pending_seed_never_pulls_a_working_inbox(job):
    # audit PLACEMENT-10: 3 Inbox + 1 not answered yet was a WATCH, and a second WATCH pulled the inbox
    lines = [HEAD]
    for mb in ("pl-ok1@x.com", "pl-ok2@x.com"):
        lines += [f"{mb},{p},Inbox,{MON9},SENT\n" for p in ("GOOGLE_WORKSPACE", "MICROSOFT_365") for _ in range(4)]
    lines += [f"pl-bad@x.com,GOOGLE_WORKSPACE,{x},{MON9},SENT\n" for x in ("Inbox", "Inbox", "Inbox", "")]
    lines += [f"pl-bad@x.com,MICROSOFT_365,Inbox,{MON9},SENT\n"] * 4
    p = job.downloads / "dt-pl-1.csv"
    p.write_text("".join(lines), encoding="utf-8")
    t = at(10, 8, 50).timestamp()
    os.utime(p, (t, t))
    st = wk.load_state(job.state_dir)
    st["since"] = "2026-11-01"
    st["results"]["pl-bad@x.com"] = [{"night": "2026-11-02", "verdict": v2.WATCH, "history": False, "file": "old"}]
    wk.save_state(job.state_dir, st)
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    assert job.sl.calls == [] and job.state()["pulled"] == {}
    assert "1 seed result had not come in" in job.posts[-1][1][0]


# --- the copy check (Manveen 11 Nov 2026): a result about the email, never about an inbox ---------

def know_fleet(h):
    """The state as the PC has it after any weekly run: each inbox's client and type, each DT campaign's inboxes."""
    st = h.state()
    wk.remember_fleet(st, fleet_from(h.sl))
    wk.save_state(h.state_dir, st)


COPY_FAIL_LINE = ("• copy check · Google inboxes · 0 of 8 emails in the Google inbox, from 2 inboxes that pass the "
                  "plain note → the email is the problem: rewrite it before it sends from Google inboxes")


def test_a_copy_check_in_spam_blames_the_email_and_never_an_inbox(job, monkeypatch):
    for e in ("pl-ok1@x.com", "pl-ok2@x.com"):
        monkeypatch.setitem(TYPES, e, "GMAIL")
    know_fleet(job)
    download_monday(job)
    # the same two inboxes sent the campaign email: every one of them in spam, at both providers
    write_csv(job.downloads, "pl-copy-check-run-1.csv", {"pl-ok1@x.com": (0, 0), "pl-ok2@x.com": (0, 0)})
    job.run()
    assert job.posts == []                                        # waits for the night's files like the others
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    # Smartlead: only the inbox that failed its own inbox test is tagged and taken out
    assert [c for c in job.sl.calls if c[0] in ("tag", "remove", "add")] == [("tag", [20], [77]), ("remove", 1, [20])]
    assert job.sl.senders[1] == {10, 11} and job.sl.tagged == {20}
    st = job.state()
    assert set(st["pulled"]) == {"pl-bad@x.com"} and st["holds"] == {"2026-11-09": []}
    for e in ("pl-ok1@x.com", "pl-ok2@x.com"):                    # their inbox-test result stands, alone
        assert [(x["night"], x["verdict"]) for x in st["results"][e]] == [("2026-11-09", v2.PASS)]
    v = job.verdicts()
    assert len(v) == 5 and v["pl-ok1@x.com|2026-11-09"]["verdict"] == v2.PASS
    assert v["pl-ok2@x.com|2026-11-09"]["google"] == {"inbox": 4, "reported": 4, "sent": 4}
    (head, parts), = job.posts
    assert head[1] == "5 inboxes · 3 pass · 0 watch · 2 fail"
    assert head[2] == "Took 1 inbox out of 1 live campaign · tagged 0 in no campaign · returned 0"
    assert head[3] == "Copy check · Precise Leads · Google inboxes fail"
    assert ("• Precise Leads: the copy check failed from Google inboxes (0 of 8 emails in the Google inbox, from "
            "inboxes that pass the plain note). The email is the problem: rewrite it before it sends from Google "
            "inboxes.") in parts[0].split("\n")
    assert COPY_FAIL_LINE in next(p for p in parts if p.startswith("*Precise Leads*")).split("\n")
    doc = job.db.cols[wk.COPY_CHECKS_COLLECTION].docs["Precise Leads|2026-11-09"]
    assert doc["client"] == "Precise Leads" and doc["night"] == "2026-11-09" and doc["file"] == "pl-copy-check-run-1.csv"
    assert doc["google"] == {"inbox": 0, "sent": 8, "senders": 2, "left_out": 0, "verdict": v2.FAIL}
    assert job.heartbeat()["files_read"] == 2 and job.heartbeat()["errors"] == []
    job.clock = at(10, 11, 31)
    assert job.run() == 0 and len(job.posts) == 1                 # told once


def test_a_copy_check_on_its_own_is_posted_and_smartlead_is_not_even_read(job, monkeypatch):
    for e in ("pl-ok1@x.com", "pl-ok2@x.com"):
        monkeypatch.setitem(TYPES, e, "OUTLOOK")
    know_fleet(job)
    download_monday(job)
    write_csv(job.downloads, "dt-pl-2.csv", {"pl-other@z.com": (4, 4)})
    job.run()
    job.clock = at(10, 10, 31)
    job.run()
    posts, fleet_calls, calls = len(job.posts), job.fleet_calls, list(job.sl.calls)
    # Wednesday: a drafted campaign's email, loaded with --source and tested before launch
    job.clock = at(11, 16)
    write_csv(job.downloads, "pl draft copy_check.csv", {"pl-ok1@x.com": (0, 4), "pl-ok2@x.com": (0, 3)},
              sent=WED11, when=at(11, 15, 50))
    assert job.run() == 0 and len(job.posts) == posts             # more files may follow: not yet
    job.clock = at(11, 17, 31)
    assert job.run() == 0
    head, parts = job.posts[-1]
    assert head == ["v2 copy check, test of Wed 11 Nov", "Copy check · Precise Leads · Outlook inboxes pass",
                    "\nNothing needs you."]
    assert parts == ["*Precise Leads*\n\n• copy check · Outlook inboxes · 7 of 8 emails in the Microsoft inbox, from 2 "
                     "inboxes that pass the plain note → the email is fine from Outlook inboxes"]
    assert job.fleet_calls == fleet_calls and job.sl.calls == calls
    st = job.state()
    assert [x["night"] for x in st["results"]["pl-ok1@x.com"]] == ["2026-11-09"] and len(job.verdicts()) == 6
    assert set(st["pulled"]) == {"pl-bad@x.com"}
    assert job.db.cols[wk.COPY_CHECKS_COLLECTION].docs["Precise Leads|2026-11-11"]["outlook"]["verdict"] == v2.PASS
    job.clock = at(11, 18, 31)
    assert job.run() == 0 and len(job.posts) == posts + 1         # told once


def test_a_copy_check_campaign_is_named_missing_until_its_csv_arrives(job, monkeypatch):
    monkeypatch.setitem(EXTRA_DT, 6, {"name": "DT PL copy check", "status": "PAUSED", "senders": {10, 11}})
    for e in ("pl-ok1@x.com", "pl-ok2@x.com"):
        monkeypatch.setitem(TYPES, e, "GMAIL")
    know_fleet(job)
    download_monday(job)
    write_csv(job.downloads, "dt-pl-2.csv", {"pl-other@z.com": (4, 4)})
    job.run()
    job.clock = at(10, 10, 31)
    job.run()
    assert "• No result this week from DT PL copy check (2 inboxes): download its CSV into Downloads." in \
        job.posts[-1][1][0].split("\n")                           # its senders' inbox tests are not its result
    job.clock = at(10, 12)
    assert job.run() == 0
    head, _ = job.posts[-1]
    assert head[0] == "v2 results for the week of Mon 9 Nov: 1 DT test not in yet"
    assert head[1] == "\nMissing: DT PL copy check."
    # its CSV, under a name that says nothing: known by its mailboxes, all senders of the copy-check campaign
    write_csv(job.downloads, "export (3).csv", {"pl-ok1@x.com": (4, 0), "pl-ok2@x.com": (4, 0)}, when=at(10, 12, 30))
    job.clock = at(10, 13)
    job.run()
    job.clock = at(10, 14, 31)
    assert job.run() == 0
    head, parts = job.posts[-1]
    assert head[:2] == ["v2 copy check, test of Mon 9 Nov", "Copy check · Precise Leads · Google inboxes pass"]
    st = job.state()
    assert sorted(f["status"] for f in st["files"].values()) == ["copy_check", "ingested", "ingested", "ingested"]
    assert set(st["pulled"]) == {"pl-bad@x.com"} and len(job.verdicts()) == 6


def test_a_copy_check_campaign_whose_csv_arrived_is_not_called_missing(job, monkeypatch):
    monkeypatch.setitem(EXTRA_DT, 6, {"name": "DT PL copy check", "status": "PAUSED", "senders": {10, 11}})
    know_fleet(job)
    download_monday(job)
    write_csv(job.downloads, "dt-pl-2.csv", {"pl-other@z.com": (4, 4)})
    write_csv(job.downloads, "pl-copy-check-run-1.csv", {"pl-ok1@x.com": (4, 4), "pl-ok2@x.com": (4, 4)})
    job.run()
    job.clock = at(10, 10, 31)
    job.run()
    (head, parts), = job.posts
    assert "No result this week" not in "\n".join(parts)
    job.clock = at(10, 12)
    assert job.run() == 0 and len(job.posts) == 1                 # nothing is missing: no Tuesday reminder


def test_a_mongo_problem_with_copy_checks_never_stops_the_run(job, monkeypatch):
    for e in ("pl-ok1@x.com", "pl-ok2@x.com"):
        monkeypatch.setitem(TYPES, e, "GMAIL")
    know_fleet(job)
    download_monday(job)
    write_csv(job.downloads, "pl-copy-check-run-1.csv", {"pl-ok1@x.com": (0, 0), "pl-ok2@x.com": (0, 0)})
    job.run()
    job.clock = at(10, 10, 31)
    job.db.cols[wk.COPY_CHECKS_COLLECTION] = FakeCol()
    job.db.cols[wk.COPY_CHECKS_COLLECTION].fail = RuntimeError("write refused")
    assert job.run() == 0 and len(job.posts) == 1 and len(job.verdicts()) == 5
    assert any(e.startswith("copy_checks not written: RuntimeError") for e in job.heartbeat()["errors"])
    job.db.cols[wk.COPY_CHECKS_COLLECTION].fail = None
    job.clock = at(10, 11, 31)
    assert job.run() == 0                                         # written on the next run
    assert job.db.cols[wk.COPY_CHECKS_COLLECTION].docs["Precise Leads|2026-11-09"]["google"]["verdict"] == v2.FAIL


def test_a_dry_run_prints_the_copy_check_and_saves_nothing(job, capsys, monkeypatch):
    write_csv(job.downloads, "pl-copy-check-run-1.csv", {"pl-ok1@x.com": (0, 0), "pl-ok2@x.com": (0, 0)})
    assert job.run(apply=False) == 0
    out = capsys.readouterr().out
    assert "v2 copy check, test of Mon 9 Nov" in out and "copy check" in out.split("v2 copy check", 1)[1]
    assert job.posts == [] and job.state()["copy_checks"] == {} and job.state()["files"] == {}


# --- failed domains (Manveen 11 Nov 2026) ---------------------------------------------

def test_a_failed_domain_takes_its_other_inboxes_out_too(job, monkeypatch):
    for n in range(5):
        monkeypatch.setitem(INBOXES, f"q{n + 1}@q.com", (50 + n, "Precise Leads"))
    job.sl.senders[1] |= {50, 51, 52, 53, 54}
    write_csv(job.downloads, "dt-pl-1.csv", {**DT_PL_1, "q1@q.com": (0, 4), "q2@q.com": (4, 0), "q3@q.com": (0, 0),
                                             "q4@q.com": (4, 4)})                    # q5 is in no test
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    everyone = [20, 50, 51, 52, 53, 54]                           # 3 of q.com's 5 in spam: all five go, with pl-bad
    assert ("tag", everyone, [77]) in job.sl.calls and ("remove", 1, everyone) in job.sl.calls
    assert job.sl.senders[1] == {10, 11}
    assert set(job.state()["pulled"]) == {"pl-bad@x.com"} | {f"q{n}@q.com" for n in range(1, 6)}
    head, parts = job.posts[-1]
    assert head[2] == "Took 6 inboxes out of 1 live campaign · tagged 0 in no campaign · returned 0"
    text = "\n".join(parts)
    assert "• domain failed · q.com · 3 of 5 inboxes in spam → 2 more inboxes taken out of PL | A; tagged DO NOT USE" in text
    assert "• pass" not in text and "Pulled earlier" not in text


def test_a_failed_domain_of_a_report_only_client_changes_nothing(job, monkeypatch):
    monkeypatch.setitem(INBOXES, "mel-ok2@y.com", (32, "Melior"))
    job.sl.senders[2] |= {32}
    write_csv(job.downloads, "dt-melior-1.csv", {**DT_MELIOR_1, "mel-ok2@y.com": (4, 4)})
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    assert job.sl.calls == [] and job.state()["pulled"] == {} and job.sl.senders[2] == {30, 31, 32}
    head, parts = job.posts[-1]
    assert head[2] == "Took 0 inboxes out of 0 live campaigns · tagged 0 in no campaign · returned 0"
    assert "The job would take 2 inboxes out of campaigns. List under *Melior* below." in parts[0]
    melior = next(p for p in parts if p.startswith("*Melior*"))
    assert ("• domain failed · y.com · 1 of 2 inboxes in spam → 1 more inbox would be taken out of Melior | B "
            "(Melior: report-only until the job is proven on the other clients); nothing changed in Smartlead") in melior



# --- Google inboxes are report-only (12 Nov 2026: real replies disagree with the seed test) ---------------------

def test_a_failing_google_inbox_is_reported_and_never_moved(job, monkeypatch):
    monkeypatch.setitem(TYPES, "pl-bad@x.com", "GMAIL")
    download_monday(job)
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    assert job.sl.calls == [] and job.state()["pulled"] == {} and job.sl.tagged == set()
    head, parts = job.posts[-1]
    assert head[2] == "Took 0 inboxes out of 0 live campaigns · tagged 0 in no campaign · returned 0"
    assert ("• Google inboxes are report-only until real replies agree with the seed test, so nothing changed in "
            "Smartlead for them. The job would take 1 inbox out of campaigns. Listed under each client below.") in parts[0]
    text = "\n".join(parts)
    assert ("pl-bad@x.com · Google 0/4, Microsoft 4/4 → would be taken out of PL | A (Google inboxes: report-only until "
            "real replies agree with the seed test); nothing changed in Smartlead") in text


def test_google_inboxes_are_acted_on_only_when_named_in_act_types(job, monkeypatch):
    monkeypatch.setitem(TYPES, "pl-bad@x.com", "GMAIL")
    download_monday(job)
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run(act_types="gmail") == 0
    assert ("tag", [20], [77]) in job.sl.calls and ("remove", 1, [20]) in job.sl.calls
    assert set(job.state()["pulled"]) == {"pl-bad@x.com"}
    assert "Google inboxes: report-only" not in "\n".join(job.posts[-1][1])


def test_an_outlook_inbox_is_still_acted_on(job, monkeypatch):
    monkeypatch.setitem(TYPES, "pl-bad@x.com", "OUTLOOK")
    write_csv(job.downloads, "dt-pl-1.csv", {**DT_PL_1, "pl-bad@x.com": (4, 0)})       # fails at Microsoft, its own provider
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    assert ("remove", 1, [20]) in job.sl.calls and set(job.state()["pulled"]) == {"pl-bad@x.com"}


def test_a_report_only_clients_google_inbox_is_named_by_the_client(job, monkeypatch):
    monkeypatch.setitem(TYPES, "mel-bad@y.com", "GMAIL")
    write_csv(job.downloads, "dt-melior-1.csv", {**DT_MELIOR_1, "mel-bad@y.com": (0, 4)})
    job.run()
    job.clock = at(10, 10, 31)
    assert job.run() == 0
    text = "\n".join(job.posts[-1][1])
    assert "(Melior: report-only until the job is proven on the other clients)" in text
    assert "Google inboxes: report-only" not in text


def test_the_reason_names_the_client_stage_first():
    assert wk.ro_reason("Melior", "GMAIL", {"Melior"}, {"GMAIL"})["name"] == "Melior"
    r = wk.ro_reason("Precise Leads", "gmail", {"Melior"}, {"GMAIL"})
    assert r == {"name": "Google inboxes", "why": "until real replies agree with the seed test", "client": None}
    assert wk.ro_reason("Precise Leads", "OUTLOOK", {"Melior"}, {"GMAIL"}) is None
    assert wk.ro_reason("Precise Leads", "GMAIL", {"Melior"}, set()) is None
    assert wk.report_only_types(SimpleNamespace(act_types="GMAIL")) == set()
    assert wk.report_only_types(SimpleNamespace()) == {"GMAIL"}


# --- self-check ---------------------------------------------------------------------

@pytest.fixture
def checkable(job, monkeypatch):
    import smartlead.accounts as accs
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setattr(accs, "discover_accounts", lambda: [AccountConfig(name="PRECISE_LEADS", api_key="k1"),
                                                            AccountConfig(name="BETTRDATA", api_key="k2")])
    job.slack = {"ok": True, "channel": {"name": "domain-inboxes-expiry-reminder", "is_member": True}}
    monkeypatch.setattr(wk, "_slack_api", lambda method, token, **p: dict(job.slack, asked=(method, p)))
    job.check = lambda: asyncio.run(wk.self_check(SimpleNamespace(folder=str(job.downloads))))
    return job


def test_self_check_passes_when_everything_is_reachable(checkable, capsys):
    assert checkable.check() == 0
    out = capsys.readouterr().out
    assert "PASS  Slack channel C0AGVSUNEFP: #domain-inboxes-expiry-reminder, the bot is a member" in out
    assert "PASS  Smartlead PRECISE_LEADS: key works" in out and "PASS  Smartlead BETTRDATA: key works" in out
    assert "PASS  Mongo: reachable, database infrabot" in out and "5 passed, 0 failed" in out
    assert checkable.posts == [] and checkable.sl.calls == []     # posts nothing, changes nothing


def test_self_check_fails_when_the_bot_cannot_post_or_a_part_is_missing(checkable, capsys, monkeypatch):
    import smartlead.accounts as accs
    checkable.slack = {"ok": True, "channel": {"name": "domain-inboxes-expiry-reminder", "is_member": False}}
    monkeypatch.setattr(accs, "discover_accounts", lambda: [AccountConfig(name="PRECISE_LEADS", api_key="k1")])
    checkable.db_down = RuntimeError("timed out")
    assert checkable.check() == 1
    out = capsys.readouterr().out
    assert "FAIL  Slack channel C0AGVSUNEFP: the bot is not a member of #domain-inboxes-expiry-reminder" in out
    assert "FAIL  Smartlead BETTRDATA: no key loaded" in out and "FAIL  Mongo: RuntimeError: timed out" in out
    checkable.slack = {"ok": False, "error": "channel_not_found"}
    assert checkable.check() == 1
    assert "Slack says channel_not_found" in capsys.readouterr().out
    checkable.slack = {"ok": False, "error": "missing_scope", "needed": "channels:read"}
    assert checkable.check() == 1
    assert "the bot token lacks the channels:read scope" in capsys.readouterr().out
