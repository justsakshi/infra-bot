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
    assert ("• Melior is report-only until the job is proven on the other clients, so nothing changed in Smartlead for it. The job would leave 1 "
            "inbox in a live campaign where it is the last good sender and tag 1 inbox that is in no campaign. "
            "List under *Melior* below.") in parts[0]
    assert ("• Melior | Solo (5) is live with only failed inboxes left (last@y.com). Not changed, as Melior is "
            "report-only until the job is proven on the other clients: pause it or add a good sender.") in parts[0]
    melior = next(p for p in parts if p.startswith("*Melior*"))
    assert ("• fail · y.com · last@y.com · Google 0/4, Microsoft 4/4 → last good sender of live Melior | Solo, so it "
            "would stay there (Melior: report-only until the job is proven on the other clients); nothing changed in Smartlead") in melior
    assert ("• fail · y.com · idle@y.com · Google 4/4, Microsoft 1/4 → in no campaign; would be tagged DO NOT USE "
            "(Melior: report-only until the job is proven on the other clients); nothing changed in Smartlead") in melior
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


def test_a_later_short_test_does_not_hide_the_fail_the_job_acts_on(tmp_path):
    """Mon: a 4-email test fails the inbox. Tue: a one-off test sends it 1 Google email (not judged). Both files
    arrive together. The job pulls on Monday's fail, so the post shows that result, not Tuesday's 'incomplete'
    (seen 11 Nov 2026: 'incomplete ... Google 0/4 -> would be taken out', and the headline counted no fail)."""
    write_csv(tmp_path, "weekly.csv", {"bad@x.com": (0, 4), "ok@x.com": (4, 4)})
    lines = [HEAD] + [f"bad@x.com,{p},Inbox,2026-11-10T11:51:00.000Z,SENT\n"
                      for p in ["GOOGLE_WORKSPACE"] + ["MICROSOFT_365"] * 4]
    p = tmp_path / "one-off.csv"
    p.write_text("".join(lines), encoding="utf-8")
    t = NOW.timestamp() - 3600
    os.utime(p, (t, t))
    st = fresh_state(tmp_path)
    got = read(tmp_path, st, now=NOW + timedelta(days=1))
    assert [x["verdict"] for x in st["results"]["bad@x.com"]] == [v2.FAIL, v2.INCOMPLETE]
    decisions = wk.plan(st)
    assert decisions["bad@x.com"]["action"] == v2.PULL
    head, parts = wk.build_report(st, got, decisions, fleet(), acted(removed={"bad@x.com": [1]}), True, set())
    text = "\n".join(parts)
    assert "• fail · x.com · bad@x.com · Google 0/4, Microsoft 4/4 → taken out of PL | A; tagged DO NOT USE" in text
    assert "incomplete" not in text
    assert head[1] == "2 inboxes · 1 pass · 0 watch · 1 fail"


# --- own provider, failed domains (Manveen 11 Nov 2026) ------------------------------

BAD = {"a@bad.com": (0, 4), "b@bad.com": (0, 4), "c@bad.com": (4, 4), "d@bad.com": (4, 4)}      # 2 of 4 in spam
# 3 in spam, 2 pass: with untested@bad.com in the fleet the domain has 6 inboxes, so 3 of 6 fails it
WIDE = {"a@bad.com": (0, 4), "b@bad.com": (0, 4), "c@bad.com": (0, 4), "d@bad.com": (4, 4), "e@bad.com": (4, 4)}
FINE = {"a@fine.com": (0, 4), "b@fine.com": (4, 4), "c@fine.com": (4, 4), "d@fine.com": (4, 4)}  # 1 of 4


def planned(tmp_path, inboxes, types=None, clients=None):
    """One night's CSV read into a fresh state, with the inbox types and clients the fleet would give."""
    write_csv(tmp_path, "a.csv", inboxes)
    st = fresh_state(tmp_path)
    read(tmp_path, st)
    st["types"] = dict(types or {})
    st["clients"] = dict(clients or {})
    wk.compute_holds(st, None)
    return st


def pulls(decisions):
    return {e for e, d in decisions.items() if d["action"] == v2.PULL}


def test_an_inbox_that_fails_only_at_the_other_provider_is_not_pulled(tmp_path):
    st = planned(tmp_path, {"g@one.com": (4, 0), "o@two.com": (0, 4)}, {"g@one.com": "GMAIL", "o@two.com": "OUTLOOK"})
    assert pulls(wk.plan(st)) == set()


def test_an_inbox_that_fails_at_its_own_provider_is_pulled(tmp_path):
    st = planned(tmp_path, {"g@one.com": (0, 4), "o@two.com": (4, 0)}, {"g@one.com": "GMAIL", "o@two.com": "OUTLOOK"})
    d = wk.plan(st)
    assert pulls(d) == {"g@one.com", "o@two.com"} and d["g@one.com"]["why"] == "failed 2026-11-09"


def test_an_smtp_or_untyped_inbox_is_still_judged_on_both_providers(tmp_path):
    st = planned(tmp_path, {"s@one.com": (0, 4), "u@two.com": (0, 4), "s@three.com": (4, 0)},
                 {"s@one.com": "SMTP", "s@three.com": "SMTP"})
    assert pulls(wk.plan(st)) == {"s@one.com", "u@two.com", "s@three.com"}


def test_a_held_own_provider_is_not_judged_and_the_other_one_still_is(tmp_path):
    inboxes = {f"g{n}@d{n}.com": (0, 4) for n in range(12)}
    inboxes["o@two.com"] = (0, 0)
    st = planned(tmp_path, inboxes, {**{e: "GMAIL" for e in inboxes}, "o@two.com": "OUTLOOK"})
    assert st["holds"]["2026-11-09"] == ["google"]               # 13 of 13 below 4/4 at Google: a test problem
    assert pulls(wk.plan(st)) == {"o@two.com"}                   # its Microsoft result is not held, and failed


def test_watch_and_return_use_the_own_provider_verdict(tmp_path):
    st = fresh_state(tmp_path)
    st["types"] = {"g@one.com": "GMAIL", "o@two.com": "OUTLOOK", "back@three.com": "GMAIL"}
    for e in ("g@one.com", "o@two.com"):                         # Google 3/4 two weeks running
        st["results"][e] = [result(v2.WATCH, 3, 4, "2026-11-02"), result(v2.WATCH, 3, 4)]
    st["results"]["back@three.com"] = [result(v2.FAIL, 4, 0), result(v2.FAIL, 4, 0, "2026-11-16")]
    st["pulled"]["back@three.com"] = {"on": "2026-11-03", "campaigns": [1]}
    d = wk.plan(st)
    assert d["g@one.com"]["action"] == v2.PULL and "two weeks running" in d["g@one.com"]["why"]
    assert d["o@two.com"]["action"] is None                      # 4/4 at Microsoft both weeks
    assert d["back@three.com"]["action"] == v2.RETURN            # 4/4 at Google twice since the pull


def test_a_domain_with_half_its_inboxes_in_spam_rests_all_of_them(tmp_path):
    clients = {e: "Precise Leads" for e in {**WIDE, **FINE}}
    clients["untested@bad.com"] = "Precise Leads"                # in the fleet, in no test: a sixth inbox
    st = planned(tmp_path, {**WIDE, **FINE}, clients=clients)
    d = wk.plan(st, {"2026-11-09"})
    assert pulls(d) == set(WIDE) | {"untested@bad.com", "a@fine.com"}
    assert d["a@bad.com"]["why"] == "failed 2026-11-09" and "domain" not in d["a@bad.com"]
    for e in ("d@bad.com", "e@bad.com", "untested@bad.com"):     # passed, or never tested: out with the domain
        assert d[e]["why"] == "its domain failed (3 of 6 inboxes in spam)"
        assert d[e]["domain"] == {"name": "bad.com", "failed": 3, "inboxes": 6}
    assert d["b@fine.com"]["action"] is None                     # 1 of 4: only the inbox that failed
    assert wk.plan(st) == d                                      # no nights given: the newest night with new results


def test_an_untested_inbox_counts_toward_the_size_of_its_domain(tmp_path):
    st = planned(tmp_path, BAD, clients={e: "Precise Leads" for e in BAD})
    assert pulls(wk.plan(st)) == set(BAD)                        # 2 of 4 inboxes: the domain failed
    st["clients"]["untested@bad.com"] = "Precise Leads"          # a fifth inbox, in no test: 2 of 5
    assert pulls(wk.plan(st)) == {"a@bad.com", "b@bad.com"}      # under half: only the two that failed


def test_a_domain_fails_on_own_provider_verdicts_only(tmp_path):
    spam_at_microsoft = {"a@bad.com": (4, 0), "b@bad.com": (4, 0), "c@bad.com": (4, 4), "d@bad.com": (4, 4)}
    st = planned(tmp_path, spam_at_microsoft, {e: "GMAIL" for e in BAD})
    assert pulls(wk.plan(st)) == set()                           # Microsoft spam does not fail a Google domain
    st["types"] = {e: "OUTLOOK" for e in BAD}
    assert pulls(wk.plan(st)) == set(BAD)


def test_a_domain_pulled_inbox_returns_only_once_its_domain_no_longer_fails(tmp_path):
    st = fresh_state(tmp_path)
    nights = ("2026-11-09", "2026-11-16", "2026-11-23")
    for e in ("a@bad.com", "b@bad.com"):
        st["results"][e] = [result(v2.FAIL, 0, 4, n) for n in nights]
    for e in ("c@bad.com", "d@bad.com"):
        st["results"][e] = [result(v2.PASS, 4, 4, n) for n in nights]
    for e in BAD:
        st["pulled"][e] = {"on": "2026-11-10", "campaigns": [1]}
    d = wk.plan(st, {"2026-11-23"})
    assert all(x["action"] is None for x in d.values())          # c and d passed twice, but 2 of 4 still fail
    assert d["c@bad.com"]["why"] == "passed 2026-11-16 and 2026-11-23, but its domain failed (2 of 4 inboxes in spam)"
    _, parts = wk.build_report(st, {**GOT, "nights": {"2026-11-23"}}, d, None, acted(), True, set(BAD))
    assert ("• bad.com still fails (2 of 4 inboxes in spam), so 2 inboxes that passed twice stay out of campaigns "
            "until it no longer does.") in "\n".join(parts)

    st["results"]["a@bad.com"].append(result(v2.FAIL, 0, 4, "2026-11-30"))
    for e in ("b@bad.com", "c@bad.com", "d@bad.com"):
        st["results"][e].append(result(v2.PASS, 4, 4, "2026-11-30"))
    d = wk.plan(st, {"2026-11-30"})                              # 1 of 4 now: the domain no longer fails
    assert {e for e, x in d.items() if x["action"] == v2.RETURN} == {"c@bad.com", "d@bad.com"}
    assert d["b@bad.com"]["action"] is None                      # one pass so far


def test_history_results_never_fail_a_domain(tmp_path):
    write_csv(tmp_path, "nov5.csv", BAD, sent="2026-11-05T02:00:00.000Z")
    st = fresh_state(tmp_path)
    read(tmp_path, st, history=True)
    wk.compute_holds(st, None)
    assert pulls(wk.plan(st)) == set() and pulls(wk.plan(st, {"2026-11-05"})) == set()


def test_a_person_removing_the_tag_wins_over_a_domain_pull_until_the_next_result(tmp_path):
    clients = {e: "Precise Leads" for e in WIDE}
    clients["untested@bad.com"] = "Precise Leads"                # 3 of 6 inboxes in spam
    st = planned(tmp_path, WIDE, clients=clients)
    f = fleet()
    for n, e in enumerate(("d@bad.com", "untested@bad.com")):    # the job pulled both; a person took the tag off
        st["pulled"][e] = {"on": "2026-11-10", "campaigns": [1]}
        f["PRECISE_LEADS"]["inboxes"][e] = {"id": 50 + n, "client": "Precise Leads", "tags": []}
    assert len(wk.reconcile(st, f, "2026-11-12")) == 2
    d = wk.plan(st)
    assert d["d@bad.com"] == {"action": None, "why": "override"}
    assert d["untested@bad.com"] == {"action": None, "why": "override"}
    assert d["e@bad.com"]["action"] == v2.PULL                   # nobody overrode this one
    for e, (g, m) in WIDE.items():
        st["results"][e].append(result(v2.PASS if g else v2.FAIL, g, m, "2026-11-16"))
    d = wk.plan(st)                                              # the domain fails again a week later
    assert d["d@bad.com"]["action"] == v2.PULL and d["untested@bad.com"]["action"] == v2.PULL


def test_a_state_file_from_before_inbox_types_still_loads_and_plans(tmp_path):
    st = planned(tmp_path, {"bad@x.com": (0, 4), "ok@y.com": (4, 4)})
    del st["types"]
    wk.save_state(tmp_path / "state", st)
    old = wk.load_state(tmp_path / "state")
    assert old["types"] == {}
    assert pulls(wk.plan(old)) == {"bad@x.com"}                  # no type known: both providers, as before


def test_the_fleet_carries_each_inbox_type_and_the_state_remembers_it(monkeypatch, tmp_path):
    import smartlead.accounts as accs
    from smartlead.config import AccountConfig

    class SL:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def list_email_accounts(self):
            return [{"id": 1, "from_email": "G@x.com", "type": "Gmail"}, {"id": 2, "from_email": "o@x.com", "type": "OUTLOOK"},
                    {"id": 3, "from_email": "s@x.com", "type": "SMTP"}, {"id": 4, "from_email": "n@x.com"}]

        async def list_campaigns(self):
            return []

    monkeypatch.setattr(accs, "discover_accounts", lambda: [AccountConfig(name=a, api_key="k") for a in wk.ACCOUNTS])
    monkeypatch.setattr(wk, "_client", lambda account, key: SL())
    f = asyncio.run(wk.load_fleet())
    assert {e: i["type"] for e, i in f["PRECISE_LEADS"]["inboxes"].items()} == {
        "g@x.com": "GMAIL", "o@x.com": "OUTLOOK", "s@x.com": "SMTP", "n@x.com": ""}
    st = fresh_state(tmp_path)
    wk.remember_fleet(st, f)
    assert st["types"] == {"g@x.com": "GMAIL", "o@x.com": "OUTLOOK", "s@x.com": "SMTP", "n@x.com": ""}
    wk.remember_fleet(st, fleet())                               # a fleet read with no type: both providers
    assert st["types"] == {"bad@x.com": "", "ok@x.com": ""}


def bad_fleet():
    """The fleet with bad.com's six inboxes: the five in WIDE and one that is in no test."""
    f = fleet()
    for n, e in enumerate(sorted(WIDE) + ["untested@bad.com"]):
        f["PRECISE_LEADS"]["inboxes"][e] = {"id": 50 + n, "client": "Precise Leads", "tags": [], "type": "GMAIL"}
    return f


def test_report_gives_a_failed_domain_its_own_line(tmp_path):
    f = bad_fleet()
    st = fresh_state(tmp_path)
    for e, (g, m) in WIDE.items():
        st["results"][e] = [result(v2.PASS if g else v2.FAIL, g, m)]
    wk.remember_fleet(st, f)
    d = wk.plan(st, GOT["nights"])
    assert pulls(d) == set(WIDE) | {"untested@bad.com"}
    a = acted(removed={e: [1] for e in WIDE}, tagged=["untested@bad.com"])
    head, parts = wk.build_report(st, GOT, d, f, a, True, set())
    text = "\n".join(parts)
    # the fail line counts the 5 tested inboxes; the domain line counts all 6 the job knows on the domain
    assert "• fail · bad.com · 3 of 5 inboxes · Google 0/4, Microsoft 4/4 → taken out of PL | A; tagged DO NOT USE" in text
    assert ("• domain failed · bad.com · 3 of 6 inboxes in spam → 3 more inboxes: 2 taken out of PL | A, 1 in no "
            "campaign; tagged DO NOT USE") in text
    assert "• pass" not in text and "Pulled earlier" not in text  # the passing inboxes are in the domain line only
    assert head[1] == "5 inboxes · 2 pass · 0 watch · 3 fail"

    _, parts = wk.build_report(st, GOT, d, f, acted(removed={e: [1] for e in WIDE}), False, set())    # a dry run
    assert ("• domain failed · bad.com · 3 of 6 inboxes in spam → 2 more inboxes would be taken out of PL | A; "
            "would be tagged DO NOT USE") in "\n".join(parts)


def test_an_inbox_already_out_with_its_domain_is_not_pulled_twice_and_stays_in_the_domain_line(tmp_path):
    f = bad_fleet()
    st = fresh_state(tmp_path)
    for e, (g, m) in WIDE.items():
        st["results"][e] = [result(v2.PASS if g else v2.FAIL, g, m)]
    wk.remember_fleet(st, f)
    for e in list(WIDE) + ["untested@bad.com"]:                  # the run before this one took all six out
        st["pulled"][e] = {"on": "2026-11-10", "campaigns": []}
    d = wk.plan(st, GOT["nights"])
    assert pulls(d) == set()
    assert d["d@bad.com"]["domain"]["name"] == "bad.com" and d["untested@bad.com"]["action"] is None
    # d is still the last good sender of a live campaign, so every run tries it again and says so
    a = acted(blocked={"d@bad.com": [1]}, tagged=["d@bad.com"])
    _, parts = wk.build_report(st, GOT, d, f, a, True, set(st["pulled"]))
    text = "\n".join(parts)
    assert ("• domain failed · bad.com · 3 of 6 inboxes in spam → 1 more inbox last good sender of live PL | A: not "
            "removed (see Needs you); tagged DO NOT USE") in text
    assert "• pass" not in text


def test_the_domain_line_counts_its_inboxes_by_what_happened():
    names = {1: "PL | A", 2: "PL | B"}
    a = acted(removed={"c@bad.com": [1]}, parked={"d@bad.com": [2]}, failed=["e@bad.com"], tagged=["d@bad.com", "f@bad.com"])
    assert wk._domain_tail("Precise Leads", ["c@bad.com", "d@bad.com", "e@bad.com", "f@bad.com"], a, names, True) == (
        "4 more inboxes: 1 removal failed (see Needs you), retried next run, 1 taken out of PL | A, 1 left in "
        "paused/draft PL | B, 1 in no campaign; tagged DO NOT USE")
    ro = acted(report_only={"c@y.com": {"removed": [], "blocked": [1], "parked": []}})
    assert wk._domain_tail("Melior", ["c@y.com"], ro, names, True) == (
        "1 more inbox last good sender of live PL | A, so it would stay there (Melior: report-only until the job "
        "is proven on the other clients); nothing changed in Smartlead")


def test_report_counts_an_inbox_by_its_own_provider_verdict(tmp_path):
    st = fresh_state(tmp_path)
    st["types"] = {"g@one.com": "GMAIL", "o@two.com": "OUTLOOK"}
    st["results"]["g@one.com"] = [result(v2.FAIL, 4, 0)]         # Microsoft spam: a Google inbox passes
    st["results"]["o@two.com"] = [result(v2.FAIL, 4, 0)]         # and an Outlook inbox fails
    d = wk.plan(st)
    assert pulls(d) == {"o@two.com"}
    head, parts = wk.build_report(st, GOT, d, None, acted(tagged=["o@two.com"]), True, set())
    text = "\n".join(parts)
    assert head[1] == "2 inboxes · 1 pass · 0 watch · 1 fail"
    assert "g@one.com" not in text and "• fail · two.com · o@two.com · Google 4/4, Microsoft 0/4 →" in text


def test_a_small_test_cannot_rest_a_big_domain(tmp_path):
    """One inbox of a four-inbox domain is tested and fails: only that one is pulled."""
    write_csv(tmp_path, "one-off.csv", {"a@big.com": (0, 4)})
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    st["clients"] = {f"{n}@big.com": "Precise Leads" for n in "abcd"}
    st["types"] = {f"{n}@big.com": "GMAIL" for n in "abcd"}
    d = wk.plan(st, got["nights"])
    assert d["a@big.com"]["action"] == v2.PULL
    assert all((d.get(f"{n}@big.com") or {}).get("action") is None for n in "bcd")


def test_published_verdicts_follow_the_own_provider_rule(tmp_path):
    """The verdict other tools read (v2_verdicts) is the one the job acts on, so the list, the self-test and the
    bill review agree with what was pulled."""
    write_csv(tmp_path, "w.csv", {"g@x.com": (4, 0), "o@x.com": (0, 4), "s@x.com": (0, 4)})
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    st["types"] = {"g@x.com": "GMAIL", "o@x.com": "OUTLOOK", "s@x.com": "SMTP"}
    for n in got["nights"]:
        st["holds"].setdefault(n, [])
    docs = {d["email"]: d["verdict"] for d in wk.verdict_docs(st, NOW + timedelta(days=1)).values()}
    assert docs == {"g@x.com": v2.PASS, "o@x.com": v2.PASS, "s@x.com": v2.FAIL}


def test_a_published_fail_a_person_overrode_says_so(tmp_path):
    """13 Nov 2026: a person removed the job's tag after this result, so the job leaves the inbox alone; the readers
    (list, daily post, self-test) must not hold it back for that fail either."""
    write_csv(tmp_path, "w.csv", {"s@x.com": (0, 4), "t@x.com": (0, 4)})
    st = fresh_state(tmp_path)
    got = read(tmp_path, st)
    st["types"] = {"s@x.com": "SMTP", "t@x.com": "SMTP"}
    for n in got["nights"]:
        st["holds"].setdefault(n, [])
    st["overrides"]["s@x.com"] = max(got["nights"])
    docs = {d["email"]: (d["verdict"], d["overridden"]) for d in wk.verdict_docs(st, NOW + timedelta(days=1)).values()}
    assert docs == {"s@x.com": (v2.FAIL, True), "t@x.com": (v2.FAIL, False)}


# --- the copy check: a file that grades the email and never an inbox (11 Nov 2026) --------------------

def test_a_state_file_without_copy_checks_still_loads(tmp_path):
    st = fresh_state(tmp_path)
    del st["copy_checks"]
    wk.save_state(tmp_path / "state", st)
    assert wk.load_state(tmp_path / "state")["copy_checks"] == {}


def test_a_copy_check_file_is_read_once_and_never_becomes_an_inbox_result(tmp_path):
    st = fresh_state(tmp_path)
    st["clients"] = {"a@x.com": "Melior", "b@x.com": "Melior"}
    write_csv(tmp_path, "DT Melior copy check.csv", {"a@x.com": (0, 4), "b@x.com": (1, 4)})
    got = wk.ingest(wk.scan(tmp_path, st, NOW), st, tmp_path / "state", history=False, now=NOW, persist=True)
    assert st["results"] == {} and st["needs_action"] is False and not got["nights"] and got["ingested"] == []
    assert got["copy_checks"] == ["Melior|2026-11-09"]
    (f,) = st["files"].values()
    assert f["status"] == "copy_check" and f["client"] == "Melior" and f["night"] == "2026-11-09"
    rec = st["copy_checks"]["Melior|2026-11-09"]
    assert (rec["client"], rec["night"], rec["file"]) == ("Melior", "2026-11-09", "DT Melior copy check.csv")
    assert rec["senders"]["b@x.com"] == {"google": [1, 4], "microsoft": [4, 4]} and rec["history"] is False
    assert wk.scan(tmp_path, st, NOW) == []                       # by content hash, like any other file
    assert len(list((tmp_path / "state" / "runs").iterdir())) == 1          # a copy is kept
    st["holds"]["2026-11-09"] = []
    assert wk.unsettled(st, NOW) == [] and wk.verdict_docs(st, NOW + timedelta(days=1)) == {}
    wk.compute_holds(st, None)
    assert wk.plan(st) == {} and st["file_holds"] == {}           # no verdict, no pull, no hold from it


def test_a_file_of_copy_check_senders_is_a_copy_check_whatever_its_name(tmp_path):
    st = fresh_state(tmp_path)
    st["dt_tests"] = {"DT PL #1": ["a@x.com", "b@x.com", "c@x.com"], "DT PL copy check": ["a@x.com", "b@x.com"]}
    st["clients"] = {e: "Precise Leads" for e in st["dt_tests"]["DT PL #1"]}
    write_csv(tmp_path, "export.csv", {"a@x.com": (0, 0), "b@x.com": (0, 0)})
    write_csv(tmp_path, "dt-pl-1.csv", {"a@x.com": (4, 4), "b@x.com": (4, 4), "c@x.com": (4, 4)})
    got = read(tmp_path, st)
    assert got["ingested"] == ["dt-pl-1.csv"] and got["copy_checks"] == ["Precise Leads|2026-11-09"]
    assert {mb: [x["verdict"] for x in rs] for mb, rs in st["results"].items()} == {
        "a@x.com": [v2.PASS], "b@x.com": [v2.PASS], "c@x.com": [v2.PASS]}


def test_a_copy_check_from_before_the_jobs_first_day_is_kept_and_never_reported(tmp_path):
    st = fresh_state(tmp_path)
    st["since"] = "2026-11-10"
    write_csv(tmp_path, "copy-check.csv", {"a@x.com": (0, 0)})
    read(tmp_path, st)
    assert st["copy_checks"]["Unknown|2026-11-09"]["history"] is True
    assert wk.copy_checks_due(st, NOW + timedelta(days=1)) == []


def test_a_copy_check_waits_for_its_night_to_settle_before_it_is_reported_alone(tmp_path):
    st = fresh_state(tmp_path)
    write_csv(tmp_path, "copy-check.csv", {"a@x.com": (0, 0)})
    read(tmp_path, st)
    assert wk.copy_checks_due(st, NOW) == []                      # the inbox files of the same download may follow
    assert wk.copy_checks_due(st, NOW, settled=False) == ["Unknown|2026-11-09"]
    later = NOW + timedelta(minutes=wk.SETTLE_MINUTES + 1)
    assert wk.copy_checks_due(st, later) == ["Unknown|2026-11-09"]
    st["copy_checks"]["Unknown|2026-11-09"]["reported"] = "2026-11-10T13:00"
    assert wk.copy_checks_due(st, later) == []


def copy_checked(tmp_path, copy, results, types, client="Precise Leads"):
    """A state with stored inbox results, then one copy-check file read and judged: (state, got)."""
    st = fresh_state(tmp_path)
    st["results"], st["types"] = results, types
    st["clients"] = {e: client for e in types}
    write_csv(tmp_path, "copy check.csv", copy)
    got = read(tmp_path, st)
    wk.judge_copy_checks(st, got["copy_checks"])
    return st, got


GOOGLE5 = {f"g{n}@x.com": "GMAIL" for n in range(5)}
OUTLOOK5 = {f"o{n}@y.com": "OUTLOOK" for n in range(5)}


def test_the_report_gives_the_email_its_own_verdict_per_sender_type(tmp_path):
    types = {**GOOGLE5, **OUTLOOK5}
    results = {e: [result(v2.PASS, 4, 4, "2026-11-02")] for e in types}
    copy = {e: ((3, 0) if e == "g0@x.com" else (0, 0)) for e in GOOGLE5}            # 3 of 20 at Google
    copy.update({e: ((0, 3) if e == "o0@y.com" else (0, 4)) for e in OUTLOOK5})     # 19 of 20 at Microsoft
    st, got = copy_checked(tmp_path, copy, results, types)
    head, parts = wk.build_report(st, got, {}, None, acted(), True, set())
    assert head == ["v2 copy check, test of Mon 9 Nov",
                    "Copy check · Precise Leads · Google inboxes fail · Outlook inboxes pass",
                    "\n*Needs you: 1*"]
    assert parts[0] == ("*Needs you*\n\n• Precise Leads: the copy check failed from Google inboxes (3 of 20 emails in "
                        "the Google inbox, from inboxes that pass the plain note). The email is the problem: rewrite it "
                        "before it sends from Google inboxes.")
    assert parts[1].split("\n") == [
        "*Precise Leads*", "",
        "• copy check · Google inboxes · 3 of 20 emails in the Google inbox, from 5 inboxes that pass the plain note → "
        "the email is the problem: rewrite it before it sends from Google inboxes",
        "• copy check · Outlook inboxes · 19 of 20 emails in the Microsoft inbox, from 5 inboxes that pass the plain "
        "note → the email is fine from Outlook inboxes"]
    assert st["results"].keys() == results.keys() and all(len(rs) == 1 for rs in st["results"].values())


def test_senders_that_do_not_pass_the_plain_note_are_left_out_of_the_copy_verdict(tmp_path):
    results = {"g0@x.com": [result(v2.PASS, 4, 4, "2026-11-02")],
               "g1@x.com": [result(v2.PASS, 4, 0, "2026-11-02")],             # a Google inbox: Microsoft is not its test
               "g2@x.com": [result(v2.FAIL, 0, 4, "2026-11-02")],
               "g3@x.com": [result(v2.PASS, 4, 4, "2026-10-26"), result(v2.FAIL, 0, 4, "2026-11-02")]}   # g4: never tested
    st, got = copy_checked(tmp_path, {e: (2, 0) for e in GOOGLE5}, results, GOOGLE5)
    assert wk.known_good(st) == {"g0@x.com", "g1@x.com"}
    rec = st["copy_checks"]["Precise Leads|2026-11-09"]["types"]
    assert rec["google"] == {"inbox": 4, "sent": 8, "senders": 2, "left_out": 3, "verdict": v2.FAIL}
    head, parts = wk.build_report(st, got, {}, None, acted(), True, set())
    assert ("• copy check · Google inboxes · 4 of 8 emails in the Google inbox, from 2 inboxes that pass the plain note "
            "(3 left out) → the email is the problem") in parts[1]
    assert "Outlook" not in "\n".join(head + parts)                # no Outlook sender in the file: no Outlook line


def test_too_few_known_good_senders_is_reported_as_not_judged_and_needs_nobody(tmp_path):
    results = {"g0@x.com": [result(v2.PASS, 4, 4, "2026-11-02")]}
    st, got = copy_checked(tmp_path, {e: (0, 0) for e in GOOGLE5}, results, GOOGLE5)
    head, parts = wk.build_report(st, got, {}, None, acted(), True, set())
    assert head[1:] == ["Copy check · Precise Leads · Google inboxes not judged", "\nNothing needs you."]
    assert parts == ["*Precise Leads*\n\n• copy check · Google inboxes · too few inboxes that pass the plain note to "
                     "judge the email (1 counted, 4 left out)"]


def test_a_borderline_copy_check_says_so_and_a_file_of_unknown_inboxes_is_not_judged(tmp_path):
    results = {e: [result(v2.PASS, 4, 4, "2026-11-02")] for e in GOOGLE5}
    copy = dict(zip(GOOGLE5, [(4, 0), (4, 0), (3, 0), (1, 0), (0, 0)]))            # 12 of 20
    st, got = copy_checked(tmp_path, copy, results, GOOGLE5, client="Melior")
    head, parts = wk.build_report(st, got, {}, None, acted(), True, set())
    assert head[1] == "Copy check · Melior · Google inboxes borderline" and head[-1] == "\nNothing needs you."
    assert parts == ["*Melior*\n\n• copy check · Google inboxes · 12 of 20 emails in the Google inbox, from 5 inboxes "
                     "that pass the plain note → borderline: test the email again before it sends from Google inboxes"]
    st["types"] = {}
    wk.judge_copy_checks(st, got["copy_checks"])
    head, parts = wk.build_report(st, got, {}, None, acted(), True, set())
    assert head[1] == "Copy check · Melior · not judged"
    assert parts == ["*Melior*\n\n• copy check · none of its 5 inboxes is a Google or Outlook inbox the job knows, "
                     "so the email was not judged"]


def test_the_headline_counts_never_include_copy_check_mailboxes(tmp_path):
    types = {"ok@x.com": "GMAIL", "bad@x.com": "GMAIL"}
    st = fresh_state(tmp_path)
    st["types"], st["clients"] = types, {e: "Precise Leads" for e in types}
    write_csv(tmp_path, "dt-pl-1.csv", {"ok@x.com": (4, 4)})
    write_csv(tmp_path, "dt-pl-copy-check.csv", {"ok@x.com": (0, 0), "bad@x.com": (0, 0)})
    got = read(tmp_path, st)
    wk.compute_holds(st, None)
    wk.judge_copy_checks(st, got["copy_checks"])
    head, parts = wk.build_report(st, got, wk.plan(st), fleet(), acted(), True, set())
    assert head[0] == "v2 placement results, tests of Mon 9 Nov"
    assert head[1] == "1 inboxes · 1 pass · 0 watch · 0 fail"       # bad@x.com is only in the copy check
    assert head[2] == "Took 0 inboxes out of 0 live campaigns · tagged 0 in no campaign · returned 0"
    assert head[3] == "Copy check · Precise Leads · Google inboxes not judged"
    assert set(st["results"]) == {"ok@x.com"} and wk.plan(st)["ok@x.com"]["action"] is None
    section = next(p for p in parts if p.startswith("*Precise Leads*"))
    assert "(1 counted, 1 left out)" in section


def test_a_copy_check_campaign_has_a_result_once_its_clients_copy_check_arrived(tmp_path):
    st = fresh_state(tmp_path)
    st["results"]["ok@x.com"] = [result(v2.PASS, 4, 4)]
    f = fleet()
    f["PRECISE_LEADS"]["dt"] = {9: {"name": "DT PL #1", "status": "PAUSED", "senders": {10, 11}},
                                8: {"name": "DT PL copy check", "status": "PAUSED", "senders": {11}}}
    report = lambda: "\n".join(wk.build_report(st, GOT, wk.plan(st), f, acted(), True, set())[1])   # noqa: E731
    # ok@x.com's inbox-test result is not the copy check's result
    assert "No result this week from DT PL copy check (1 inbox): download its CSV into Downloads." in report()
    st["copy_checks"]["Precise Leads|2026-11-01"] = {"client": "Precise Leads", "night": "2026-11-01"}
    assert "No result this week from DT PL copy check" in report()            # last week's does not count
    st["copy_checks"]["Melior|2026-11-08"] = {"client": "Melior", "night": "2026-11-08"}
    assert "No result this week from DT PL copy check" in report()            # nor another client's
    st["copy_checks"]["Precise Leads|2026-11-08"] = {"client": "Precise Leads", "night": "2026-11-08"}
    assert "No result this week" not in report()                               # started on Sunday: this week's


def test_copy_checks_go_to_mongo_as_one_document_per_client_and_night(tmp_path):
    results = {e: [result(v2.PASS, 4, 4, "2026-11-02")] for e in GOOGLE5}
    st, got = copy_checked(tmp_path, {e: (4, 0) for e in GOOGLE5}, results, GOOGLE5)
    st["copy_checks"]["Melior|2026-11-09"] = {"client": "Melior", "night": "2026-11-09", "file": "m.csv", "senders": {}}
    docs = wk.copy_check_docs(st, NOW)
    assert set(docs) == {"Precise Leads|2026-11-09"}               # one not judged yet is not sent
    d = docs["Precise Leads|2026-11-09"]
    assert (d["_id"], d["client"], d["night"], d["file"]) == ("Precise Leads|2026-11-09", "Precise Leads", "2026-11-09",
                                                              "copy check.csv")
    assert d["google"] == {"inbox": 20, "sent": 20, "senders": 5, "left_out": 0, "verdict": v2.PASS}
    assert d["outlook"]["verdict"] == v2.NOT_JUDGED and d["updated_at"].endswith("+00:00")



def test_d8_says_which_agreed_conditions_hold_for_a_report_only_client():
    st = {"files": {"a": {"status": "ingested", "night": "2026-11-11"}}, "overrides": {"x@y.com": "2026-11-09"},
          "returned": []}
    ok, waiting = wk.d8_status(st, {"Melior"})["Melior"]
    assert len(ok) == 2 and waiting == ["one weekly test on the new rules",
                                        "one taken-out inbox coming back on its own after two passes"]
    st["files"]["b"] = {"status": "ingested", "night": "2026-11-15"}       # a Sunday: a weekly night
    st["returned"] = [{"email": "r@y.com"}]
    assert wk.d8_status(st, {"Melior"})["Melior"][1] == []
    st["overrides"]["z@y.com"] = "2026-11-15"                              # a person undid a pull after the switch
    assert wk.d8_status(st, {"Melior"})["Melior"][1][0].startswith("no inbox the job took out put back by a person (1")
    assert wk.d8_status(st, set()) == {}
