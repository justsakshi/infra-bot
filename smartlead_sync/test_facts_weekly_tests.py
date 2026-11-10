"""The inbox facts read the weekly scheduled classic tests, dated by the day they last ran.

Found 10 Nov 2026: smartlead_facts read only COMPLETED tests and dated them by created_at. A weekly
scheduled test stays ACTIVE and keeps the created_at of the day its schedule was made, so every weekly
result was left out. Fleet watch then said "no placement test in 21 days" about domains tested the day
before, and the kill warnings and the renewal note never saw a weekly fail.
"""
from types import SimpleNamespace

import placement_report as pr
import renewal_review as cli


def once(i, created, status="COMPLETED"):
    return {"spam_test_id": i, "test_name": f"one-off {i}", "test_type": "manual", "status": status,
            "created_at": created, "every_days": None, "schedule_start_time": None, "current_test_run_no": 0}


def weekly(i, created, start, run, status="ACTIVE"):
    return {"spam_test_id": i, "test_name": f"weekly {i}", "test_type": "auto", "status": status,
            "created_at": created, "every_days": 7, "schedule_start_time": start, "current_test_run_no": run}


def test_a_weekly_test_is_read_and_dated_by_its_last_run():
    listing = [once(3, "2026-10-12T09:00:00.000Z"),
               weekly(2, "2026-10-08T13:50:44.154Z", "2026-10-12T00:30:00.000Z", 5)]     # 12 Oct + 4 weeks
    got = cli.tests_to_read(listing)
    assert [(t["spam_test_id"], t["created_at"]) for t in got] == [(2, "2026-11-09"), (3, "2026-10-12")]


def test_a_scheduled_test_that_has_not_run_yet_and_an_unfinished_one_off_are_skipped():
    listing = [weekly(1, "2026-11-03T10:00:00.000Z", "2026-11-10T01:00:00.000Z", 0),
               once(2, "2026-11-09T05:00:00.000Z", status="RUNNING")]
    assert cli.tests_to_read(listing) == []


def test_a_stopped_weekly_test_still_counts_for_its_last_run():
    got = cli.tests_to_read([weekly(9, "2026-10-08T00:00:00.000Z", "2026-10-13T00:30:00.000Z", 4, status="STOPPED")])
    assert [(t["spam_test_id"], t["created_at"]) for t in got] == [(9, "2026-11-03")]


def test_daily_one_off_tests_never_push_a_weekly_test_out():
    """The auto placement test makes one one-off test a day; the cap is on those, not on the weekly ones."""
    listing = [once(100 + d, f"2026-11-{d:02d}T05:30:00.000Z") for d in range(1, 15)]
    listing += [weekly(i, "2026-11-03T10:00:00.000Z", "2026-11-03T01:00:00.000Z", 1) for i in (1, 2, 3)]
    got = cli.tests_to_read(listing, limit=10)
    ids = [t["spam_test_id"] for t in got]
    assert {1, 2, 3} <= set(ids)
    assert len([i for i in ids if i >= 100]) == 10 and 101 not in ids          # the 10 newest one-off tests
    assert [t["created_at"] for t in got] == sorted((t["created_at"] for t in got), reverse=True)


def test_one_off_tests_made_on_the_same_day_keep_their_order_and_a_repeat_is_read_once():
    listing = [once(1, "2026-11-02T08:00:00.000Z"), once(2, "2026-11-02T15:00:00.000Z"), once(2, "2026-11-02T15:00:00.000Z")]
    assert [t["spam_test_id"] for t in cli.tests_to_read(listing)] == [2, 1]


def _fake(monkeypatch, listing, reports, older=None):
    def get(url, key, **params):
        if url.endswith("/email-accounts/"):
            return [{"from_email": "a@x.test", "client_id": 12256, "is_smtp_success": True,
                     "is_imap_success": True, "warmup_details": {"warmup_reputation": "100%"}, "tags": []}]
        for test_id, rep in reports.items():
            if url.endswith(f"/spam-test/report/{test_id}/sender-account-wise"):
                return rep
        raise AssertionError(url)
    monkeypatch.setattr(cli, "_get", get)
    monkeypatch.setattr(cli.httpx, "post", lambda *a, **k: SimpleNamespace(json=lambda: listing))
    monkeypatch.setattr(cli, "discover_accounts", lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(cli, "_resolve_seed_domains", lambda domains: None)
    monkeypatch.setattr(cli, "seed_provider", lambda domain: "Google")
    monkeypatch.setattr(pr, "attachments", lambda key, sending_days=None: {})
    monkeypatch.setattr(cli, "_older_scheduled", lambda rows, key, account: list(older or []))


def _report(email, folders):
    return [{"email": email, "details": [{"email": f"s{i}@seed.test", "reply": {"mail_folder": f}}
                                         for i, f in enumerate(folders)]}]


def test_facts_date_an_inbox_by_the_weekly_run_not_by_the_day_the_schedule_was_made(monkeypatch):
    listing = [once(553145, "2026-10-12T06:00:00.000Z"),
               weekly(550493, "2026-10-08T13:50:44.154Z", "2026-10-12T00:30:00.000Z", 5)]
    _fake(monkeypatch, listing, {553145: _report("a@x.test", ["inbox"] * 4),
                                 550493: _report("a@x.test", ["inbox", "spam", "spam", "spam"])})
    errors = []
    f = cli.smartlead_facts(errors)["a@x.test"]
    assert errors == []
    assert f["tested_on"] == "2026-11-09" and f["pct"] == 25.0            # the weekly run, and its fail
    assert [(t["test_id"], t["date"], t["spam"]) for t in f["tests"]] == [(550493, "2026-11-09", 3), (553145, "2026-10-12", 0)]


def test_a_weekly_test_that_left_the_newest_50_is_still_read(monkeypatch):
    old = weekly(550493, "2026-10-08T13:50:44.154Z", "2026-10-12T00:30:00.000Z", 5)
    _fake(monkeypatch, [], {550493: _report("a@x.test", ["inbox"] * 4)}, older=[old])
    f = cli.smartlead_facts([])["a@x.test"]
    assert f["tested_on"] == "2026-11-09" and f["pct"] == 100.0


def test_older_scheduled_reads_the_tests_the_report_job_remembers(monkeypatch):
    old = weekly(550493, "2026-10-08T13:50:44.154Z", "2026-10-12T00:30:00.000Z", 5)

    class Col:
        def find(self, query, fields):
            assert query == {"account": "PRECISE_LEADS", "status": "ACTIVE"}
            return [{"test_id": 550493, "created_at": "2026-10-08T13:50:44.154Z"}]
    asked = []
    monkeypatch.setattr(pr, "_collection", lambda name: asked.append(name) or Col())
    monkeypatch.setattr(pr, "_list_tests", lambda key, limit, offset=0: [old] if offset == 1 else [])
    listing = [once(900, "2026-11-09T05:00:00.000Z")]
    assert cli._older_scheduled(listing, "k", "PRECISE_LEADS") == [old]
    assert asked == [pr.SCHEDULED]


def test_older_scheduled_never_breaks_the_read_without_mongo(monkeypatch):
    monkeypatch.setattr(pr, "_collection", lambda name: None)
    assert cli._older_scheduled([once(1, "2026-11-09T05:00:00.000Z")], "k", "PRECISE_LEADS") == []

    def boom(name):
        raise RuntimeError("mongo down")
    monkeypatch.setattr(pr, "_collection", boom)
    assert cli._older_scheduled([], "k", "PRECISE_LEADS") == []
