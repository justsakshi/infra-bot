"""v2_weekly.py: files into state, decisions, the Smartlead write path (fake client), the report. No network."""
import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import v2_weekly as wk
from smartlead import v2_results as v2

HEAD = "Mailbox,Provider,Landed,Sent at,Send status\n"
NOW = datetime(2026, 11, 10, 11, 0, tzinfo=v2.IST)


def write_csv(folder: Path, name: str, inboxes: dict, sent="2026-11-09T14:31:00.000Z"):
    """inboxes: {email: (google_inbox_of_4, microsoft_inbox_of_4)}"""
    lines = [HEAD]
    for mb, (g, m) in inboxes.items():
        lines += [f"{mb},GOOGLE_WORKSPACE,{'Inbox' if i < g else 'Spam'},{sent},SENT\n" for i in range(4)]
        lines += [f"{mb},MICROSOFT_365,{'Inbox' if i < m else 'Spam'},{sent},SENT\n" for i in range(4)]
    p = folder / name
    p.write_text("".join(lines), encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))
    return p


def fresh_state(tmp):
    return wk.load_state(tmp / "state")


def read(tmp, st, history=False, now=NOW):
    return wk.ingest(wk.scan(tmp, st, now), st, tmp / "s", history=history, now=now, persist=False)


# --- files -------------------------------------------------------------------------

def test_scan_reads_only_new_v2_files(tmp_path):
    write_csv(tmp_path, "dt-pl-1-run-1.csv", {"a@x.com": (4, 4)})
    (tmp_path / "leads.csv").write_text("email,name\na@b.com,A\n", encoding="utf-8")
    st = fresh_state(tmp_path)
    found = wk.scan(tmp_path, st, NOW)
    assert [p.name for p, _, _ in found] == ["dt-pl-1-run-1.csv"]
    wk.ingest(found, st, tmp_path / "state", history=False, now=NOW, persist=True)
    assert wk.scan(tmp_path, st, NOW) == []                       # same content is never read twice
    assert (tmp_path / "state" / "runs").exists()                 # a copy is kept
    assert st["needs_action"] is True


def test_old_files_are_ignored(tmp_path):
    p = write_csv(tmp_path, "old.csv", {"a@x.com": (4, 4)})
    t = NOW.timestamp() - 30 * 86400
    os.utime(p, (t, t))
    assert wk.scan(tmp_path, fresh_state(tmp_path), NOW) == []


def test_a_night_is_acted_on_only_once_its_files_stop_arriving(tmp_path):
    write_csv(tmp_path, "a.csv", {"a@x.com": (4, 4)})
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    assert wk.unsettled(st, NOW) == ["2026-11-09"]
    assert wk.unsettled(st, NOW + timedelta(minutes=wk.SETTLE_MINUTES + 1)) == []


def test_a_test_from_before_the_jobs_first_day_is_history(tmp_path):
    write_csv(tmp_path, "nov5.csv", {"bad@x.com": (0, 4)}, sent="2026-11-05T02:00:00.000Z")
    st = fresh_state(tmp_path)
    st["since"] = "2026-11-07"
    got = read(tmp_path, st)                                       # not --history-only, still history
    assert st["results"]["bad@x.com"][0]["history"] is True and st["needs_action"] is False and not got["nights"]


def test_history_files_are_kept_but_never_acted_on(tmp_path):
    write_csv(tmp_path, "nov5.csv", {"bad@x.com": (0, 4)}, sent="2026-11-05T02:00:00.000Z")
    st = fresh_state(tmp_path)
    read(tmp_path, st, history=True)
    assert st["results"]["bad@x.com"][0]["history"] is True and st["needs_action"] is False
    wk.compute_holds(st, None)
    assert wk.plan(st)["bad@x.com"]["action"] is None


# --- decisions ---------------------------------------------------------------------

def test_a_suspect_provider_is_held_and_nothing_is_pulled(tmp_path):
    write_csv(tmp_path, "a.csv", {f"g{n}@x.com": (0, 4) for n in range(12)})
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    wk.compute_holds(st, None)
    assert st["holds"]["2026-11-09"] == ["google"]
    assert all(d["action"] is None for d in wk.plan(st).values())


def test_one_broken_test_file_is_held_on_its_own(tmp_path):
    # audit PLACEMENT-10: the hold worked per night only, so one broken DT test among good ones pulled its inboxes
    write_csv(tmp_path, "dt-pl-1.csv", {f"g{n}@x.com": (0, 4) for n in range(12)})
    good = {f"ok{n}@y.com": (4, 4) for n in range(40)}
    good["real@y.com"] = (0, 4)
    write_csv(tmp_path, "dt-pl-2.csv", good)
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    wk.compute_holds(st, None)
    assert st["holds"]["2026-11-09"] == []                       # 13 of 53 below 4/4: the night is fine
    assert list(st["file_holds"].values()) == [["google"]]       # but dt-pl-1 alone is held
    d = wk.plan(st)
    assert {e for e, x in d.items() if x["action"]} == {"real@y.com"}
    head, parts = wk.build_report(st, GOT, d, None, acted(), False, set())
    assert any("Google results in dt-pl-1.csv (2026-11-09) held" in p for p in parts)


def test_an_unrecognised_v2_csv_is_found_once(tmp_path):
    (tmp_path / "v2-new.csv").write_text("Mailbox,Provider,Landed,Sent at,Send status,Seed\na@x.com,G,Inbox,,SENT,1\n",
                                         encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(tmp_path / "v2-new.csv", (t, t))
    (tmp_path / "leads.csv").write_text("email,name\na@b.com,A\n", encoding="utf-8")
    st, odd = fresh_state(tmp_path), []
    assert wk.scan(tmp_path, st, NOW, odd) == [] and [p.name for p, _ in odd] == ["v2-new.csv"]
    st["files"][odd[0][1]] = {"name": "v2-new.csv", "status": "unrecognised"}
    again = []
    assert wk.scan(tmp_path, st, NOW, again) == [] and again == []          # reported once, not every hour
    head, parts = wk.build_report(st, {**GOT, "unrecognised": ["v2-new.csv"]}, {}, None, acted(), False, set())
    assert "unrecognised v2 CSV format: v2-new.csv" in parts[0]


def test_results_stored_under_the_old_rules_are_judged_again(tmp_path):
    lines = [HEAD] + [f"a@x.com,GOOGLE_WORKSPACE,Inbox,2026-11-09T14:31:00.000Z,SENT\n"] * 4
    lines += [f"a@x.com,MICROSOFT_365,{x},2026-11-09T14:31:00.000Z,SENT\n" for x in ("Inbox", "Inbox", "Inbox", "BOUNCED")]
    p = tmp_path / "dl" / "dt.csv"
    p.parent.mkdir()
    p.write_text("".join(lines), encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))
    st = fresh_state(tmp_path)
    wk.ingest(wk.scan(p.parent, st, NOW), st, tmp_path / "state", history=False, now=NOW, persist=True)
    assert st["results"]["a@x.com"][0]["verdict"] == v2.INCOMPLETE
    st["results"]["a@x.com"][0]["verdict"] = v2.WATCH            # what the rules before 7 Nov said
    st["rules"] = None
    assert wk.rejudge(st, tmp_path / "state") == 1
    assert st["results"]["a@x.com"][0]["verdict"] == v2.INCOMPLETE and st["rules"] == v2.RULES
    assert wk.rejudge(st, tmp_path / "state") == 0                # once per rules version


def test_report_says_why_an_inbox_was_not_judged(tmp_path):
    st = fresh_state(tmp_path)
    pend = v2.provider_verdict(["Inbox"] * 3 + ["Pending"])
    bounce = v2.provider_verdict(["Inbox"] * 3 + ["BOUNCED"])
    full = v2.provider_verdict(["Inbox"] * 4)
    st["results"]["p@x.com"] = [{"night": "2026-11-09", "verdict": v2.INCOMPLETE, "history": False, "google": pend,
                                 "microsoft": full, "file": "f1" + "0" * 10}]
    st["results"]["b@y.com"] = [{"night": "2026-11-09", "verdict": v2.INCOMPLETE, "history": False, "google": full,
                                 "microsoft": bounce, "file": "f1" + "0" * 10}]
    st["files"]["f1" + "0" * 62] = {"name": "dt.csv", "night": "2026-11-09", "status": "ingested", "pending_seeds": 1}
    _, parts = wk.build_report(st, GOT, wk.plan(st), None, acted(), False, set())
    text = "\n".join(parts)
    assert "x.com · p@x.com · Google 3/4, Microsoft 4/4 → a seed had not answered when the file was downloaded" in text
    assert "y.com · b@y.com · Google 4/4, Microsoft 3/4 → a seed bounced (a seed-side problem), so it was not judged" in text
    assert "dt.csv: 1 seed result had not come in when it was downloaded" in parts[0]
    assert all(d["action"] is None for d in wk.plan(st).values())


def test_a_real_failure_among_passes_is_pulled(tmp_path):
    inboxes = {f"ok{n}@x.com": (4, 4) for n in range(10)}
    inboxes["bad@x.com"] = (0, 4)
    write_csv(tmp_path, "a.csv", inboxes)
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    wk.compute_holds(st, None)
    d = wk.plan(st)
    assert {e for e, x in d.items() if x["action"]} == {"bad@x.com"} and d["bad@x.com"]["action"] == v2.PULL


def fleet(tags=None, senders=None, status="ACTIVE"):
    return {"PRECISE_LEADS": {"key": "k", "inboxes": {
        "bad@x.com": {"id": 10, "client": "Precise Leads", "tags": list(tags or [])},
        "ok@x.com": {"id": 11, "client": "Precise Leads", "tags": []}},
        "camps": {1: {"name": "PL | A", "status": status, "senders": set(senders if senders is not None else {10, 11})}},
        "dt": {}}}


def test_known_bad_inboxes_do_not_cause_a_hold(tmp_path):
    inboxes = {f"old{n}@bad.com": (0, 4) for n in range(20)}
    inboxes.update({f"ok{n}@x.com": (4, 4) for n in range(4)})
    inboxes["new@x.com"] = (0, 4)
    write_csv(tmp_path, "a.csv", inboxes)
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    f = fleet()
    for n in range(20):
        f["PRECISE_LEADS"]["inboxes"][f"old{n}@bad.com"] = {"id": 100 + n, "client": "Precise Leads",
                                                             "tags": ["DO NOT USE - Google spam (Nov 2026)"]}
    wk.compute_holds(st, None)
    assert st["holds"]["2026-11-09"] == ["google"]               # 21 of 25 below 4/4
    wk.compute_holds(st, f)
    assert st["holds"]["2026-11-09"] == []                       # the 20 a person tagged are left out
    assert wk.plan(st)["new@x.com"]["action"] == v2.PULL


def test_a_person_removing_the_tag_wins_until_the_next_result(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["bad@x.com"] = [{"night": "2026-11-09", "verdict": v2.FAIL, "history": False}]
    st["pulled"]["bad@x.com"] = {"on": "2026-11-10", "campaigns": [1]}
    notes = wk.reconcile(st, fleet(tags=[]), "2026-11-12")
    assert "bad@x.com" not in st["pulled"] and notes
    assert wk.plan(st)["bad@x.com"]["action"] is None              # same old result: left alone
    st["results"]["bad@x.com"].append({"night": "2026-11-16", "verdict": v2.FAIL, "history": False})
    assert wk.plan(st)["bad@x.com"]["action"] == v2.PULL          # a new fail: pulled again


def test_tagged_pulled_inbox_still_in_a_campaign_is_pulled_again(tmp_path):
    st = fresh_state(tmp_path)
    st["pulled"]["bad@x.com"] = {"on": "2026-11-10", "campaigns": []}
    f = fleet(tags=[wk.TAG])
    assert wk.reconcile(st, f, "2026-11-17") == []
    assert wk.still_attached(st, f) == {"PRECISE_LEADS": ["bad@x.com"]}
    assert wk.still_attached(st, fleet(tags=[wk.TAG], senders={11})) == {}


# --- the write path, against a fake Smartlead ---------------------------------------

class FakeSL:
    """Records every call; campaign 1 holds the given inboxes and two leads in progress."""

    def __init__(self, senders=(10, 11), stuck_after_remove=(501,), fail_remove=False, fail_after_apply=False):
        self.calls = []
        self.senders = {1: set(senders)}
        self.leads = {500: "INPROGRESS", 501: "INPROGRESS"}
        self.stuck_after_remove = stuck_after_remove
        self.fail_remove = fail_remove
        self.fail_after_apply = fail_after_apply
        self.tags = []

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
        return {"leads_reallocated": 1} if "reallocate" in endpoint else {"ok": True}

    async def assign_tags(self, ids, tag_ids):
        self.calls.append(("tag", list(ids), list(tag_ids)))

    async def get_campaign_leads(self, cid):
        return [{"lead": {"id": k}, "status": v} for k, v in self.leads.items()]

    async def remove_campaign_email_accounts(self, cid, ids):
        self.calls.append(("remove", int(cid), list(ids)))
        if self.fail_remove:
            raise RuntimeError("boom")
        self.senders[int(cid)] -= set(ids)
        for lid in self.stuck_after_remove:
            self.leads[lid] = "PAUSED"
        if self.fail_after_apply:
            raise TimeoutError("applied, but the answer never came")

    async def get_campaign_email_accounts(self, cid):
        return [{"id": i} for i in self.senders[int(cid)]]

    async def resume_lead(self, cid, lid):
        self.calls.append(("resume", int(cid), lid))
        self.leads[lid] = "INPROGRESS"

    async def add_campaign_email_accounts(self, cid, ids):
        self.calls.append(("add", int(cid), list(ids)))
        self.senders[int(cid)] |= set(ids)


@pytest.fixture
def fake(monkeypatch):
    box = {}

    def install(**kw):
        box["sl"] = FakeSL(**kw)
        monkeypatch.setattr(wk, "_client", lambda account, key: box["sl"])
        return box["sl"]
    return install


def test_pull_tags_removes_moves_leads_resumes_and_records(tmp_path, fake):
    sl = fake()
    st, saves = fresh_state(tmp_path), []
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet()["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-10", st,
                                  lambda: saves.append(1)))
    kinds = [c[0] for c in sl.calls]
    assert kinds == ["POST", "tag", "remove", "POST", "resume"]          # create tag, tag, remove, reallocate, resume
    assert sl.calls[3][1] == "/campaigns/1/reallocate-mailboxes"
    assert sl.calls[4] == ("resume", 1, 501)                              # only the lead the removal stalled
    assert dict(rep["removed"]) == {"bad@x.com": [1]} and rep["leads"]["resumed"] == 1
    assert st["pulled"]["bad@x.com"]["campaigns"] == [1] and st["repair"] == []
    assert len(saves) >= 3                                                 # saved after tagging and after the move


def test_pull_never_removes_the_last_good_sender(tmp_path, fake):
    sl = fake(senders=(10,))
    st = fresh_state(tmp_path)
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet(senders={10})["PRECISE_LEADS"], ["bad@x.com"], True,
                                  "2026-11-10", st, lambda: None))
    assert "remove" not in [c[0] for c in sl.calls]
    assert dict(rep["blocked"]) == {"bad@x.com": [1]} and st["pulled"]["bad@x.com"]["blocked"] == [1]


def test_a_paused_campaign_is_parked_not_flagged(tmp_path, fake):
    fake(senders=(10,))
    st = fresh_state(tmp_path)
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet(senders={10}, status="PAUSED")["PRECISE_LEADS"],
                                  ["bad@x.com"], True, "2026-11-10", st, lambda: None))
    assert dict(rep["parked"]) == {"bad@x.com": [1]} and not rep["blocked"]


def test_a_failed_removal_is_reported_and_retried_not_recorded(tmp_path, fake):
    fake(fail_remove=True)
    st = fresh_state(tmp_path)
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet()["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-10", st,
                                  lambda: None))
    assert rep["errors"] and rep["failed"] == ["bad@x.com"] and not rep["removed"] and rep["tagged"] == []
    assert st["pulled"]["bad@x.com"]["campaigns"] == []                    # still tagged + attached: pulled again next run
    assert wk.still_attached(st, fleet(tags=[wk.TAG])) == {"PRECISE_LEADS": ["bad@x.com"]}


def test_a_lead_move_in_a_paused_campaign_waits_for_it_to_be_active(tmp_path, fake):
    sl = fake()
    st = fresh_state(tmp_path)
    f = fleet(status="PAUSED")
    asyncio.run(wk.do_pulls("PRECISE_LEADS", f["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-10", st, lambda: None))
    assert "resume" not in [c[0] for c in sl.calls] and len(st["repair"]) == 1
    f["PRECISE_LEADS"]["camps"][1]["status"] = "ACTIVE"
    asyncio.run(wk.run_repairs(f, st, "2026-11-12", lambda: None))
    assert ("resume", 1, 501) in sl.calls and st["repair"] == []


def test_a_later_repair_resumes_only_the_leads_the_removal_stalled(tmp_path, fake):
    sl = fake()
    st = fresh_state(tmp_path)
    f = fleet(status="PAUSED")
    asyncio.run(wk.do_pulls("PRECISE_LEADS", f["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-10", st, lambda: None))
    assert st["repair"][0]["stalled"] == [501]
    sl.leads[500] = "PAUSED"                                  # paused later by someone else (a LinkedIn reply, say)
    f["PRECISE_LEADS"]["camps"][1]["status"] = "ACTIVE"
    asyncio.run(wk.run_repairs(f, st, "2026-11-13", lambda: None))
    assert [c for c in sl.calls if c[0] == "resume"] == [("resume", 1, 501)]


def test_a_removal_smartlead_applied_is_recorded_even_if_the_call_errored(tmp_path, fake):
    fake(fail_after_apply=True)
    st = fresh_state(tmp_path)
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet()["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-10", st,
                                  lambda: None))
    assert dict(rep["removed"]) == {"bad@x.com": [1]} and st["pulled"]["bad@x.com"]["campaigns"] == [1]
    assert rep["leads"]["moved"] == 1 and rep["leads"]["resumed"] == 1      # leads still moved and resumed


def test_return_re_adds_and_untags(tmp_path, fake):
    sl = fake(senders=(11,))
    sl.tags = [{"id": 77, "name": wk.TAG}]
    st = fresh_state(tmp_path)
    st["pulled"]["bad@x.com"] = {"on": "2026-11-10", "campaigns": [1]}
    f = fleet(tags=[wk.TAG], senders={11})
    rep = asyncio.run(wk.do_returns("PRECISE_LEADS", f["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-24", st, lambda: None))
    assert ("add", 1, [10]) in sl.calls
    assert ("DELETE", "/email-accounts/tag-mapping", {"email_account_ids": [10], "tag_ids": [77]}) in sl.calls
    assert dict(rep["added"]) == {"bad@x.com": [1]} and "bad@x.com" not in st["pulled"]


def test_return_skips_an_inbox_a_person_also_marked_do_not_use(tmp_path, fake):
    sl = fake(senders=(11,))
    st = fresh_state(tmp_path)
    st["pulled"]["bad@x.com"] = {"on": "2026-11-10", "campaigns": [1]}
    f = fleet(tags=[wk.TAG, "DO NOT USE - Google spam (Nov 2026)"], senders={11})
    rep = asyncio.run(wk.do_returns("PRECISE_LEADS", f["PRECISE_LEADS"], ["bad@x.com"], True, "2026-11-24", st, lambda: None))
    assert sl.calls == [] and rep["skipped"] and "bad@x.com" in st["pulled"]


def test_dry_run_plans_without_touching_smartlead(tmp_path, fake):
    sl = fake()
    st = fresh_state(tmp_path)
    rep = asyncio.run(wk.do_pulls("PRECISE_LEADS", fleet()["PRECISE_LEADS"], ["bad@x.com"], False, "2026-11-10", st,
                                  lambda: None))
    assert dict(rep["removed"]) == {"bad@x.com": [1]} and sl.calls == [] and st["pulled"] == {}


# --- report ------------------------------------------------------------------------

def acted(**kw):
    a = wk._new_rep()
    for k, v in kw.items():
        if isinstance(a[k], list):
            a[k] += v
        else:
            a[k].update(v)
    return a


def result(verdict, g, m, night="2026-11-09"):
    pv = lambda n: v2.provider_verdict(["Inbox"] * n + ["Spam"] * (4 - n))      # noqa: E731
    return {"night": night, "verdict": verdict, "history": False, "google": pv(g), "microsoft": pv(m)}


GOT = {"ingested": ["a.csv"], "pending": [], "nights": {"2026-11-09"}}


def test_report_says_what_was_done_and_what_needs_a_person(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["bad@x.com"] = [result(v2.FAIL, 0, 4)]
    a = acted(blocked={"bad@x.com": [1]}, tagged=["bad@x.com"])
    head, parts = wk.build_report(st, GOT, wk.plan(st), fleet(), a, True, set())
    assert head[0] == "v2 placement results, tests of Mon 9 Nov"
    assert "1 fail" in head[1] and head[-1] == "\n*Needs you: 2*"        # the campaign + ok@x.com untested
    assert "PL | A (1) is live with only failed inboxes left (bad@x.com)" in parts[0]
    assert any("• fail · x.com · bad@x.com · Google 0/4, Microsoft 4/4 → last good sender of live PL | A" in p for p in parts)


def test_report_never_claims_a_held_inbox_was_tagged(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["bad@x.com"] = [result(v2.FAIL, 4, 0)]
    st["holds"]["2026-11-09"] = ["microsoft"]
    head, parts = wk.build_report(st, GOT, wk.plan(st), fleet(), acted(), True, set())
    line = next(p for p in parts if "bad@x.com" in p and p.startswith("*Precise"))
    assert "held as a test problem; still in its campaigns" in line and "tagged" not in line
    assert "Microsoft results for 2026-11-09 held" in parts[0]


def test_report_splits_watch_groups_by_what_happened(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["f@w.com"] = [result(v2.WATCH, 3, 4, "2026-11-02"), result(v2.WATCH, 3, 4)]
    st["results"]["g@w.com"] = [result(v2.WATCH, 4, 3)]
    a = acted(removed={"f@w.com": [1]})
    f = fleet()
    f["PRECISE_LEADS"]["inboxes"].update({"f@w.com": {"id": 20, "client": "Precise Leads", "tags": []},
                                          "g@w.com": {"id": 21, "client": "Precise Leads", "tags": []}})
    _, parts = wk.build_report(st, GOT, wk.plan(st), f, a, True, set())
    text = "\n".join(parts)
    assert "• watch · w.com · f@w.com · Google 3/4, Microsoft 4/4 → below 4/4 two weeks running: taken out of PL | A" in text
    assert "• watch · w.com · g@w.com · Google 4/4, Microsoft 3/4 → 2-3 of 4 seeds in the inbox. Stays in campaigns" in text


def test_report_counts_and_collapses_inboxes_already_out(tmp_path):
    st = fresh_state(tmp_path)
    for n in range(3):
        st["results"][f"u{n}@bad.com"] = [result(v2.FAIL, n % 2, 4)]
    st["results"]["old@gone.com"] = [result(v2.FAIL, 0, 4)]
    a = acted(removed={f"u{n}@bad.com": [1] for n in range(3)})
    head, parts = wk.build_report(st, GOT, wk.plan(st), fleet(), a, True, {"old@gone.com"})
    assert head[2] == "Took 3 inboxes out of 1 live campaign · tagged 0 in no campaign · returned 0"
    text = "\n".join(parts)
    assert "• fail · bad.com · 3 of 3 inboxes · Google 0-1/4, Microsoft 4/4 → taken out of PL | A; tagged DO NOT USE" in text
    assert "Still failing, already out from an earlier run: 1 inbox on 1 domain" in text


def test_report_warn_week_and_untested_and_spacing(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    f = fleet()
    f["PRECISE_LEADS"]["dt"] = {9: {"name": "DT Melior #3", "status": "ACTIVE", "senders": {55}}}
    head, parts = wk.build_report(st, GOT, wk.plan(st), f, acted(), False, set(), act_from="2026-11-16")
    assert head[2].startswith("Would take 0 inboxes") and "until 2026-11-16" in head[3]
    assert head[-1].startswith("\n*Needs you: ")                          # a blank line before it
    assert "No result this week from DT Melior #3" in parts[0]
    assert any("x.com (Precise Leads): 1 inbox, last v2 never tested" in p for p in parts)   # bad@x.com sends, untested
    assert all("\n\n" in p for p in parts)                                 # title, blank line, lines


def test_inboxes_whose_dt_csv_is_missing_are_not_called_untested(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    f = fleet()
    f["PRECISE_LEADS"]["dt"] = {9: {"name": "DT PL #2", "status": "ACTIVE", "senders": {10}}}   # bad@x.com, no CSV
    _, parts = wk.build_report(st, GOT, wk.plan(st), f, acted(), True, set())
    assert "No result this week from DT PL #2 (1 inbox): download its CSV" in parts[0]
    assert not any("in no DT test" in p for p in parts)


def test_an_inbox_in_a_tested_dt_campaign_but_missing_from_the_results_is_called_out(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    f = fleet()
    f["PRECISE_LEADS"]["dt"] = {9: {"name": "DT PL #2", "status": "ACTIVE", "senders": {10, 11}}}
    _, parts = wk.build_report(st, GOT, wk.plan(st), f, acted(), True, set())
    assert "sit in a DT campaign but were missing from its v2 test results" in parts[0]


def test_a_pull_from_an_older_result_is_still_named(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    st["results"]["bad@x.com"] = [result(v2.FAIL, 0, 4, "2026-11-02")]
    head, parts = wk.build_report(st, GOT, wk.plan(st), fleet(), acted(removed={"bad@x.com": [1]}), True, set())
    assert "1 inboxes · 1 pass" in head[1]                                   # counts stay this night's
    assert any("bad@x.com · Google 0/4, Microsoft 4/4 → taken out of PL | A" in p for p in parts)


def test_a_post_slack_refused_is_queued(tmp_path, monkeypatch):
    st = fresh_state(tmp_path)
    monkeypatch.setattr(wk, "post", lambda head, parts: False)
    assert wk.post_or_queue(st, ["t"], ["p"]) is False and st["unposted"] == [{"head": ["t"], "parts": ["p"]}]


# --- state + run guard ---------------------------------------------------------------

def test_state_round_trips(tmp_path):
    st = fresh_state(tmp_path)
    st["pulled"]["a@x.com"] = {"on": "2026-11-10"}
    wk.save_state(tmp_path / "state", st)
    assert wk.load_state(tmp_path / "state")["pulled"] == {"a@x.com": {"on": "2026-11-10"}}


def test_a_recent_lock_skips_the_run(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "run.lock").write_text("x")
    args = type("A", (), {"state_dir": str(tmp_path / "state"), "apply": True, "warn": False, "history_only": False})()
    assert asyncio.run(wk.run(args)) == 0
    assert "another run" in capsys.readouterr().out


def test_load_fleet_refuses_a_missing_account(monkeypatch):
    import smartlead.accounts as accs
    monkeypatch.setattr(accs, "discover_accounts", lambda: [])
    with pytest.raises(wk.FleetIncomplete):
        asyncio.run(wk.load_fleet())


def test_a_one_email_test_is_reported_not_judged(tmp_path):
    lines = [HEAD] + [f"{mb},{p},{l},2026-11-09T14:31:00.000Z,SENT\n"
                      for mb, l in (("a@x.com", "Spam"), ("b@x.com", "Inbox"))
                      for p in ("GOOGLE_WORKSPACE", "MICROSOFT_365")]
    p = tmp_path / "bd-run-1.csv"
    p.write_text("".join(lines), encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    assert st["results"]["a@x.com"][0]["verdict"] == v2.INCOMPLETE         # never pulled on one email
    assert wk.plan(st)["a@x.com"]["action"] is None
    f = fleet()
    f["PRECISE_LEADS"]["inboxes"]["a@x.com"] = {"id": 30, "client": "Bettrdata", "tags": []}
    head, parts = wk.build_report(st, got, wk.plan(st), f, acted(), False, set())
    assert "bd-run-1.csv: this test sends 1 email per provider" in parts[0]
    assert any("the test sent 1 email per provider, too few to judge; nothing done" in x for x in parts)


def test_a_test_thin_at_one_provider_asks_for_a_person(tmp_path):
    """1 Google and 4 Microsoft emails per inbox: not judged, and the post says how to fix the test."""
    lines = [HEAD] + [f"{mb},{p},Inbox,2026-11-09T14:31:00.000Z,SENT\n"
                      for mb in ("a@x.com", "b@x.com", "c@x.com")
                      for p in ["GOOGLE_WORKSPACE"] + ["MICROSOFT_365"] * 4]
    p = tmp_path / "bd-run-1.csv"
    p.write_text("".join(lines), encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    assert st["results"]["a@x.com"][0]["verdict"] == v2.INCOMPLETE
    assert wk.plan(st)["a@x.com"]["action"] is None
    f = fleet()
    f["PRECISE_LEADS"]["inboxes"]["a@x.com"] = {"id": 30, "client": "Bettrdata", "tags": []}
    head, parts = wk.build_report(st, got, wk.plan(st), f, acted(), False, set())
    assert ("bd-run-1.csv: this test sends 1 Google and 4 Microsoft emails per inbox, so the job can't judge its "
            "inboxes (it needs 4 of 4 at each)") in parts[0]


def test_a_midweek_test_skips_the_weekly_checks(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4, "2026-11-05")]              # a Thursday: a one-off test
    f = fleet()
    f["PRECISE_LEADS"]["dt"] = {9: {"name": "DT Melior #3", "status": "ACTIVE", "senders": {55}}}
    got = {"ingested": ["a.csv"], "pending": [], "nights": {"2026-11-05"}}
    head, parts = wk.build_report(st, got, wk.plan(st), f, acted(), True, set())
    text = "\n".join(parts)
    assert "No result this week" not in text and "in no DT test" not in text
    assert "Not a weekly test (those start on a Saturday, Sunday or Monday)" in text and head[-1] == "\nNothing needs you."


def test_the_summary_says_live_only_when_every_campaign_sends(tmp_path):
    """10 Nov 2026: the post said "6 inboxes out of 12 live campaigns" when 3 were sending and 9 were paused."""
    st = fresh_state(tmp_path)
    st["results"]["bad@x.com"] = [result(v2.FAIL, 0, 4, "2026-11-09")]
    f = fleet()
    f["PRECISE_LEADS"]["camps"][2] = {"name": "PL | B", "status": "PAUSED", "senders": {10, 11}}
    f["PRECISE_LEADS"]["camps"][3] = {"name": "PL | C", "status": "PAUSED", "senders": {10, 11}}
    got = {"ingested": ["a.csv"], "pending": [], "nights": {"2026-11-09"}}
    head, _ = wk.build_report(st, got, wk.plan(st), f, acted(removed={"bad@x.com": [1, 2, 3]}), True, set())
    assert head[2].startswith("Took 1 inbox out of 3 campaigns (1 sending, 2 paused or draft) · ")
    head, _ = wk.build_report(st, got, wk.plan(st), f, acted(removed={"bad@x.com": [1]}), True, set())
    assert head[2].startswith("Took 1 inbox out of 1 live campaign · ")


def write_rows(folder: Path, name: str, rows: list[tuple[str, str, str]]):
    """rows: (mailbox, provider, landed), all sent Monday 9 Nov 20:01 IST."""
    p = folder / name
    p.write_text(HEAD + "".join(f"{mb},{prov},{landed},2026-11-09T14:31:00.000Z,SENT\n" for mb, prov, landed in rows),
                 encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))


def test_a_bounced_seed_needs_a_person_and_never_says_nothing_needs_you(tmp_path):
    # review V2-S1: 12 inboxes with one Microsoft seed BOUNCED each said "12 no full result" and "Nothing needs you."
    rows = []
    for n in range(12):
        rows += [(f"b{n}@x.com", "GOOGLE_WORKSPACE", "Inbox")] * 4
        rows += [(f"b{n}@x.com", "MICROSOFT_365", x) for x in ("Inbox", "Inbox", "Inbox", "BOUNCED")]
    write_rows(tmp_path, "dt-pl-1.csv", rows)
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    wk.compute_holds(st, None)
    assert all(d["action"] is None for d in wk.plan(st).values())
    head, parts = wk.build_report(st, got, wk.plan(st), None, acted(), False, set())
    assert head[1] == "12 inboxes · 0 pass · 0 watch · 0 fail · 12 no full result"
    assert head[-1] == "\n*Needs you: 1*" and "Nothing needs you." not in "\n".join(head + parts)
    assert ("• dt-pl-1.csv: a seed bounced for 12 inboxes, so they were not judged. Check the test's seed list in "
            "Smart Delivery and re-run it.") in parts[0]


def test_a_provider_with_no_seed_rows_needs_a_person(tmp_path):
    rows = [(f"g{n}@x.com", "GOOGLE_WORKSPACE", "Inbox") for n in range(3) for _ in range(4)]
    rows += [("g0@x.com", "MICROSOFT_365", "Inbox")] * 4
    write_rows(tmp_path, "dt-pl-2.csv", rows)
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    wk.compute_holds(st, None)
    head, parts = wk.build_report(st, got, wk.plan(st), None, acted(), False, set())
    assert head[1] == "3 inboxes · 1 pass · 0 watch · 0 fail · 2 no full result"
    assert ("• dt-pl-2.csv: no Microsoft seed results for 2 inboxes, so they were not judged. Check the test's seed "
            "list in Smart Delivery (it needs 4 Google and 4 Microsoft seeds) and re-run it.") in parts[0]
    assert "a provider has no result, so it was not judged; nothing done" in "\n".join(parts)


def melior_fleet():
    return {"PRECISE_LEADS": {"key": "k", "inboxes": {
        "idle@y.com": {"id": 30, "client": "Melior", "tags": []},
        "last@y.com": {"id": 31, "client": "Melior", "tags": []}},
        "camps": {5: {"name": "Melior | Solo", "status": "ACTIVE", "senders": {31}}}, "dt": {}}}


def test_report_only_lines_say_what_would_happen_to_each_inbox(tmp_path):
    # review V2-S2: two Melior fails on one domain, one in no campaign and one the last good sender of a live
    # campaign, read "would take 2 of its inboxes out of campaigns" and "2 of 2 inboxes ... would stay there".
    st = fresh_state(tmp_path)
    st["results"]["idle@y.com"] = [result(v2.FAIL, 4, 1)]
    st["results"]["last@y.com"] = [result(v2.FAIL, 0, 4)]
    a = acted(report_only={"idle@y.com": {"removed": [], "blocked": [], "parked": []},
                           "last@y.com": {"removed": [], "blocked": [5], "parked": []}})
    head, parts = wk.build_report(st, GOT, wk.plan(st), melior_fleet(), a, False, set(), report_only_clients={"Melior"})
    text = "\n".join(parts)
    assert "out of campaigns" not in parts[0] and "2 of 2" not in text
    assert ("• Melior is report-only until Ryan agrees, so nothing changed in Smartlead for it. The job would leave 1 "
            "inbox in a live campaign where it is the last good sender and tag 1 inbox that is in no campaign. "
            "List under *Melior* below.") in parts[0]
    assert ("• Melior | Solo (5) is live with only failed inboxes left (last@y.com). Not changed, as Melior is "
            "report-only until Ryan agrees: ask Ryan to pause it or add a good sender.") in parts[0]
    melior = next(p for p in parts if p.startswith("*Melior*"))
    assert ("• fail · y.com · last@y.com · Google 0/4, Microsoft 4/4 → last good sender of live Melior | Solo, so it "
            "would stay there (Melior: report-only until Ryan agrees); nothing changed in Smartlead") in melior
    assert ("• fail · y.com · idle@y.com · Google 4/4, Microsoft 1/4 → in no campaign; would be tagged DO NOT USE "
            "(Melior: report-only until Ryan agrees); nothing changed in Smartlead") in melior
    assert head[2] == "Would take 0 inboxes out of 0 live campaigns · would tag 0 in no campaign · would return 0"


def test_report_only_counts_add_up_by_kind():
    kinds = wk.Counter(removed=2, blocked=2, parked=1, tagged=3)
    assert wk._ro_would(kinds) == ("take 2 inboxes out of campaigns, leave 2 inboxes in live campaigns where they "
                                   "are the last good senders, leave 1 inbox in a paused/draft campaign that has no "
                                   "good sender and tag 3 inboxes that are in no campaign")
    assert wk._ro_kind({"removed": [1], "blocked": [2]}) == "removed"
    assert wk._ro_kind({"removed": [], "blocked": [], "parked": [3]}) == "parked"
    assert wk._ro_kind({}) == "tagged"


def test_posts_go_to_the_infra_channel_not_the_archived_fallback(monkeypatch):
    import smartlead.notify as notify
    seen = {}
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.delenv("V2_WEEKLY_CHANNEL", raising=False)
    monkeypatch.setattr(notify, "post_with_thread", lambda tok, ch, head, parts: seen.setdefault("ch", ch) or "1.0")
    assert wk.post(["t"], []) and seen["ch"] == wk.INFRA_CHANNEL == "C0AGVSUNEFP"



def test_an_inbox_pulled_by_hand_with_no_v2_result_is_still_named(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    a = acted(removed={"hand@x.com": [1]})
    f = fleet()
    f["PRECISE_LEADS"]["inboxes"]["hand@x.com"] = {"id": 40, "client": "Bettrdata", "tags": [wk.TAG]}
    head, parts = wk.build_report(st, GOT, wk.plan(st), f, a, False, {"hand@x.com"})
    assert "Would take 1 inbox out of 1 live campaign" in head[2]
    text = "\n".join(parts)
    assert "*Pulled earlier and still in campaigns*" in text
    assert "• hand@x.com (Bettrdata) → would be taken out of PL | A" in text
