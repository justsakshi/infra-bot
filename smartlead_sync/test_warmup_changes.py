"""Warm-up learning (t16, 17 Nov 2026). Manveen: "mae sure we save thta on db as well so we know what works".
Every warm-up settings change is an event in Mongo warmup_changes; its 1/2/3-week outcome is filled from what the
bot already keeps (inbox_daily, v2_verdicts, inbox_evidence); a monthly line says which settings worked. No network."""
from datetime import date, timedelta

import fleet_watch as fw
from smartlead import warmup_changes as wc
from test_fleet_watch import FakeCol, rec

ON = "2026-11-17"

CHANGES = {"Changed@Example.com": {
    "id": 1, "type": "GMAIL", "rep_before": 71.0, "on": ON, "ok": True, "post_status": 200,
    "before": {"reply_rate": 20, "warmup_min_count": 5, "warmup_max_count": 20, "max_email_per_day": 20,
               "is_rampup_enabled": True, "rampup_value": 5, "daily_reply_limit": 20, "status": "ACTIVE"},
    "after": {"reply_rate": 35, "warmup_min_count": 5, "warmup_max_count": 10, "max_email_per_day": 10,
              "is_rampup_enabled": True, "rampup_value": 5, "daily_reply_limit": 15, "status": "ACTIVE"}},
    "failed@example.com": {"id": 2, "type": "OUTLOOK", "rep_before": 80.0, "on": ON, "ok": False,
                           "before": {"warmup_max_count": 20}, "after": {"warmup_max_count": 10}}}
BEFORE = {"inboxes": [
    {"account": "BETTRDATA", "client": "BettrData", "id": 1, "email": "Changed@Example.com", "type": "GMAIL",
     "warmup_reputation": 71.0},
    {"account": "BETTRDATA", "client": "BettrData", "id": 2, "email": "failed@example.com", "type": "OUTLOOK",
     "warmup_reputation": 80.0}]}


def event(email="a@example.com", on=ON, rep=71, kind="google", vendor="Zapmail", **over):
    e = {"key": f"{email}|{on}", "email": email, "account": "BETTRDATA", "client": "Bettrdata", "kind": kind,
         "type": "GMAIL", "vendor": vendor, "date": on, "rep_before": rep,
         "before": {"per_day": 20, "min": 5, "ramp": 5, "reply_rate": 20, "reply_limit": 20},
         "after": {"per_day": 10, "min": 5, "ramp": 5, "reply_rate": 35, "reply_limit": 15},
         "why": "repair", "who": "lead", "outcomes": {}, "recovered_on": None, "days_to_recover": None}
    e.update(over)
    return e


def state(email="a@example.com", day="2026-11-24", rep=78, **over):
    return {"email": email, "date": day, "rep": rep, "warm_sent_7d": 60, "warm_spam_7d": 3, **over}


# --- the events --------------------------------------------------------------------------------------------------

def test_settings_are_summed_up_as_per_day_min_ramp_reply_rate_and_reply_limit():
    assert wc.summary(CHANGES["Changed@Example.com"]["after"]) == {
        "per_day": 10, "min": 5, "ramp": 5, "reply_rate": 35, "reply_limit": 15}
    assert wc.summary({"warmup_max_count": 8, "is_rampup_enabled": False, "rampup_value": 5})["ramp"] == 0
    assert wc.summary(None) is None


def test_backfill_makes_one_event_per_applied_change_with_before_after_why_and_who():
    evs, skipped = wc.events_from_changes(CHANGES, BEFORE, who="t16 lead, Manveen's yes 16 Nov",
                                          why="repair: reputation under 90", vendors={"changed@example.com": "Zapmail"})
    assert skipped == ["failed@example.com"]                           # the change did not apply: no event
    (e,) = evs
    assert (e["key"], e["email"], e["account"], e["client"], e["kind"], e["type"], e["vendor"], e["date"]) == (
        "changed@example.com|2026-11-17", "changed@example.com", "BETTRDATA", "Bettrdata", "google", "GMAIL",
        "Zapmail", "2026-11-17")
    assert e["before"] == {"per_day": 20, "min": 5, "ramp": 5, "reply_rate": 20, "reply_limit": 20}
    assert e["after"] == {"per_day": 10, "min": 5, "ramp": 5, "reply_rate": 35, "reply_limit": 15}
    assert e["raw_before"]["max_email_per_day"] == 20 and e["raw_after"]["max_email_per_day"] == 10
    assert (e["rep_before"], e["why"], e["who"], e["outcomes"], e["recovered_on"]) == (
        71, "repair: reputation under 90", "t16 lead, Manveen's yes 16 Nov", {}, None)
    assert wc.events_from_changes(CHANGES, BEFORE, who="x", why="y")[0][0]["vendor"] is None   # unknown, not guessed


def test_appending_twice_keeps_the_first_event_and_its_outcomes():
    col = FakeCol()
    store = wc.ChangeStore(col)
    assert store.append([event()]) == 1
    col.docs[0]["outcomes"] = {"week1": {"rep": 80}}
    assert store.append([event(), event("b@example.com")]) == 1
    assert len(col.docs) == 2 and col.docs[0]["outcomes"] == {"week1": {"rep": 80}}


# --- the outcomes ---------------------------------------------------------------------------------------------------

EVIDENCE = [{"_id": "2026-11-22", "inboxes": {"a@example.com": {
    "sent": 40, "label": "reached the inbox at google",
    "providers": {"google": {"sent": 30, "replies": 1, "ooo": 2}, "microsoft": {"sent": 10, "replies": 0, "ooo": 0}}}}}]


def test_week_one_outcome_has_reputation_spam_v2_and_real_replies():
    hist = [state(day="2026-11-23", rep=76), state(day="2026-11-24", rep=78)]
    v2 = [{"verdict": "FAIL", "night": "2026-11-14"}, {"verdict": "PASS", "night": "2026-11-21"},
          {"verdict": "PASS", "night": "2026-11-28"}]                               # after the checkpoint: not used
    e = wc.fill(event(), hist, v2, EVIDENCE, date(2026, 11, 24))
    w1 = e["outcomes"]["week1"]
    assert (w1["on"], w1["rep"], w1["warm_sent_7d"], w1["warm_spam_7d"], w1["warm_spam_pct"]) == (
        "2026-11-24", 78, 60, 3, 5.0)
    assert w1["v2"] == {"verdict": "PASS", "night": "2026-11-21"}
    assert w1["real"] == {"week": "2026-11-22", "sent": 40, "replies": 1, "ooo": 2,
                          "label": "reached the inbox at google"}
    assert "week2" not in e["outcomes"]


def test_without_inbox_evidence_the_real_field_is_null_and_says_why():
    e = wc.fill(event(), [state()], [], [], date(2026, 11, 24))
    w1 = e["outcomes"]["week1"]
    assert w1["real"] is None and "inbox_evidence" in w1["real_why"]
    assert w1["v2"] is None


def test_a_filled_week_is_never_overwritten_and_later_weeks_fill_on_their_day():
    e = wc.fill(event(), [state(rep=78)], [], [], date(2026, 11, 24))
    e = wc.fill(e, [state(rep=99)], [], [], date(2026, 11, 25))
    assert e["outcomes"]["week1"]["rep"] == 78
    hist = [state(day="2026-11-24", rep=78), state(day="2026-12-01", rep=84), state(day="2026-12-08", rep=91)]
    e = wc.fill(e, hist, [], [], date(2026, 12, 8))
    assert {k: v["rep"] for k, v in e["outcomes"].items()} == {"week1": 78, "week2": 84, "week3": 91}


def test_a_late_run_reads_the_reading_nearest_the_checkpoint_and_a_gap_is_marked_missing():
    hist = [state(day="2026-11-22", rep=74), state(day="2026-11-26", rep=80)]   # nothing on 23-25 Nov
    e = wc.fill(event(), hist, [], [], date(2026, 11, 27))
    assert (e["outcomes"]["week1"]["on"], e["outcomes"]["week1"]["rep"]) == ("2026-11-22", 74)  # 2 days before wins
    e = wc.fill(event(), [state(day="2026-11-17", rep=71)], [], [], date(2026, 11, 28))
    assert e["outcomes"]["week1"] == {"missing": "no reading within 3 days of 24 Nov"}
    e = wc.fill(event(), [state(day="2026-11-17", rep=71)], [], [], date(2026, 11, 25))
    assert "week1" not in e["outcomes"]                                          # may still come in


def test_recovery_is_the_first_reading_at_90_after_the_change():
    hist = [state(day="2026-11-20", rep=85), state(day="2026-11-29", rep=90), state(day="2026-12-01", rep=88)]
    e = wc.fill(event(), hist, [], [], date(2026, 12, 1))
    assert (e["recovered_on"], e["days_to_recover"]) == ("2026-11-29", 12)
    blocked = wc.fill(event(), [state(day="2026-11-29", rep=95, warmup_blocked=True)], [], [], date(2026, 12, 1))
    assert blocked["recovered_on"] is None


# --- what worked (monthly) ------------------------------------------------------------------------------------------

def _three_weeks(email, rep_at_3w, rec_on=None, **over):
    e = event(email, **over)
    e["outcomes"] = {"week3": {"on": "2026-12-08", "rep": rep_at_3w}}
    if rec_on:
        e["recovered_on"] = rec_on
        e["days_to_recover"] = (date.fromisoformat(rec_on) - date.fromisoformat(e["date"])).days
    return e


def test_what_worked_counts_per_settings_type_and_vendor():
    evs = [_three_weeks("a@example.com", 92, "2026-11-29"),
           _three_weeks("b@example.com", 91, "2026-12-03"),
           _three_weeks("c@example.com", 66),                                    # 71 -> 66: worse
           _three_weeks("d@example.com", 75),
           _three_weeks("o@example.com", 88, kind="outlook", vendor=None, rep=80),
           event("new@example.com", on="2026-12-01")]                            # under 3 weeks: not judged yet
    rows = wc.what_worked(evs, date(2026, 12, 8))
    assert rows == [
        {"settings": "10/day, min 5, ramp 5, 35% reply", "kind": "google", "vendor": "Zapmail", "n": 4,
         "recovered": 2, "median_days": 14.0, "worse": 1, "still_low": 1},
        {"settings": "10/day, min 5, ramp 5, 35% reply", "kind": "outlook", "vendor": "vendor unknown", "n": 1,
         "recovered": 0, "median_days": None, "worse": 0, "still_low": 1}]
    done, detail = wc.month_items(evs, date(2026, 12, 7))                     # Monday 7 Dec: first Monday
    assert [d.text for d in done] == [
        "Warm-up changes, what worked: 5 inboxes 3+ weeks after a settings change, 2 recovered (median 14 days), "
        "1 worse."]
    assert done[0].key == "warmup-repair:what-worked:2026-12"
    assert "10/day, min 5, ramp 5, 35% reply · google · Zapmail: 4 inboxes, 2 recovered, median 14 days, 1 worse, " \
           "1 still under 90" in detail[0]
    assert "@" not in detail[0]                                                # numbers only
    assert wc.month_items(evs, date(2026, 12, 14)) == ([], [])                # second Monday: nothing
    assert wc.month_items([event()], date(2026, 12, 7)) == ([], [])           # nothing judged yet: silent


def test_first_monday_of_the_month():
    assert wc.first_monday(date(2026, 12, 7)) and wc.first_monday(date(2027, 1, 4))
    assert not wc.first_monday(date(2026, 12, 14)) and not wc.first_monday(date(2026, 12, 8))


# --- fleet watch fills the outcomes every day ------------------------------------------------------------------------

def test_fleet_watch_fills_outcomes_and_adds_the_monthly_line(monkeypatch):
    from smartlead import digest as dg
    reported = {}
    monkeypatch.setattr(dg, "on", lambda: True)
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, **k: reported.setdefault(src, (asks, done, detail)) or True)
    daily, changes, verdicts, evidence = FakeCol(), FakeCol(), FakeCol(), FakeCol()
    store = fw.FleetStore(daily, FakeCol(), FakeCol(), changes=changes, verdicts=verdicts, evidence=evidence)
    daily.docs = [state(day="2026-11-24", rep=93, account="BETTRDATA")]
    verdicts.docs = [{"email": "a@example.com", "verdict": "PASS", "night": "2026-11-21"}]
    changes.docs = [event()]
    today = date(2026, 12, 7)
    out = fw.daily(store, [rec("a@example.com", date=today.isoformat(), rep=93)], {}, {"PRECISE_LEADS"}, {}, today, [],
                   post=True)
    assert out.get("digest")
    c = changes.docs[0]
    assert c["outcomes"]["week1"]["rep"] == 93 and c["outcomes"]["week1"]["v2"]["verdict"] == "PASS"
    assert c["outcomes"]["week2"] == {"missing": "no reading within 3 days of 1 Dec"}
    assert c["recovered_on"] == "2026-11-24"
    asks, done, detail = reported["warmup-repair"]
    assert any(d.text.startswith("Warm-up changes, what worked") for d in done) is False   # under 3 weeks yet


def test_fleet_watch_without_the_new_collections_works_as_before():
    store = fw.FleetStore(FakeCol(), FakeCol(), FakeCol())
    assert store.changes is None
    today = date(2026, 11, 24)
    out = fw.daily(store, [rec("a@example.com", date=today.isoformat())], {}, {"PRECISE_LEADS"}, {}, today, [])
    assert out["records"] == 1 and not out["errors"]


def test_a_broken_outcome_fill_is_a_read_error_not_a_stop():
    class Broken(FakeCol):
        def find(self, q, proj=None):
            raise RuntimeError("down")
    store = fw.FleetStore(FakeCol(), FakeCol(), FakeCol(), changes=Broken())
    errors = []
    today = date(2026, 11, 24)
    out = fw.daily(store, [rec("a@example.com", date=today.isoformat())], {}, {"PRECISE_LEADS"}, {}, today, errors)
    assert "warm-up changes (RuntimeError)" in out["errors"]


# --- the backfill command --------------------------------------------------------------------------------------------

def _cli_db(monkeypatch, tmp_path):
    import json
    from smartlead import zapmail_asset_sync
    db = {"warmup_changes": FakeCol(), "inbox_daily": FakeCol(), "v2_verdicts": FakeCol(), "inbox_evidence": FakeCol(),
          "warmup_repair": FakeCol()}
    db["inbox_daily"].docs = [{"email": "changed@example.com", "date": (date.today() - timedelta(days=1)).isoformat(),
                               "rep": 72, "vendor": "Zapmail"}]
    monkeypatch.setattr(zapmail_asset_sync, "_db", lambda: db)
    (tmp_path / "c.json").write_text(json.dumps(CHANGES))
    (tmp_path / "b.json").write_text(json.dumps(BEFORE))
    return db, ["--changes", str(tmp_path / "c.json"), "--before", str(tmp_path / "b.json"),
                "--who", "lead", "--why", "repair"]


def test_backfill_dry_run_writes_nothing_and_the_real_run_reads_back_the_count(monkeypatch, tmp_path, capsys):
    import warmup_repair_cli as cli
    db, args = _cli_db(monkeypatch, tmp_path)
    assert cli.main(["changes", *args, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "1 events, 1 new, 1 skipped (change did not apply)" in out and "Bettrdata · google · Zapmail: 1" in out
    assert "(dry run: nothing written)" in out and db["warmup_changes"].docs == []
    assert cli.main(["changes", *args]) == 0
    out = capsys.readouterr().out
    assert "written: 1" in out and "read back: 1 events in warmup_changes" in out
    assert db["warmup_changes"].docs[0]["vendor"] == "Zapmail"
    assert cli.main(["changes", *args]) == 0
    assert "written: 0" in capsys.readouterr().out and len(db["warmup_changes"].docs) == 1


def test_outcomes_preview_reads_only(monkeypatch, tmp_path, capsys):
    import warmup_repair_cli as cli
    db, _ = _cli_db(monkeypatch, tmp_path)
    db["warmup_changes"].docs = [event("changed@example.com", on=(date.today() - timedelta(days=8)).isoformat())]
    before = [dict(d) for d in db["warmup_changes"].docs]
    assert cli.main(["outcomes"]) == 0
    out = capsys.readouterr().out
    assert "events: 1" in out and "week1 filled: 1" in out
    assert db["warmup_changes"].docs == before


def test_vendor_comes_from_the_daily_reading_else_the_asset_tracker_by_domain_with_one_spelling():
    assets = [{"name": "example.com", "provider": "Scaled Mail", "status": "Inactive"},
              {"name": "example.com", "provider": "Zapmail", "status": "Active"},
              {"name": "kit.com", "provider": "inboxkit", "status": "Active"},
              {"name": "none.com", "provider": None, "status": "Active"}]
    v = wc.vendors_by_email(["a@example.com", "b@kit.com", "c@none.com", "d@other.com", "e@example.com"],
                            {"e@example.com": "ScaledMail"}, assets)
    assert v == {"a@example.com": "Zapmail", "b@kit.com": "InboxKit", "e@example.com": "ScaledMail"}
    assert wc.vendor_name("Scaledmail") == wc.vendor_name("Scaled Mail") == "ScaledMail"
