"""Warm-up repair tracker (t16, 17 Nov 2026): inboxes under the warm-up reputation cutoff are tracked until they
recover, stop improving, or leave. Report-only: nothing here changes a warm-up setting, cancels or deletes. No network."""
from datetime import date, timedelta

import fleet_watch as fw
from smartlead import warmup_repair as wr
from test_fleet_watch import FakeCol, Poster, rec

MON = date(2026, 11, 23)          # a Monday
START = "2026-11-17"


def doc(email="a@example.com", **over):
    d = {"email": email, "account": "BETTRDATA", "client": "Bettrdata", "type": "GMAIL", "kind": "google",
         "started": START, "rep_at_start": 71, "settings_before": {"warmup_max_count": 20},
         "settings_after": {"warmup_max_count": 10}, "why_not_changed": None, "blocked": False,
         "status": "repairing", "enrolled_by": "seed", "recovered_on": None, "latest_rep": 71, "latest_on": None,
         "gone_on": None}
    d.update(over)
    return d


def reading(email="a@example.com", rep=71, day="2026-11-23", **over):
    return rec(email, **{"rep": rep, "date": day, "account": "BETTRDATA", "client": "Bettrdata", **over})


# --- seeding from the lead's two local files --------------------------------------------------------------------

CHANGES = {"Changed@Example.com": {"id": 1, "type": "GMAIL", "rep_before": 71.0, "on": "2026-11-17",
                                   "before": {"warmup_max_count": 20, "reply_rate": 20},
                                   "after": {"warmup_max_count": 10, "reply_rate": 35}, "ok": True}}
BEFORE = {"min_rep": 90.0, "inboxes": [
    {"account": "BETTRDATA", "client": "BettrData", "id": 1, "email": "Changed@Example.com", "type": "GMAIL",
     "warmup_reputation": 71.0, "warmup_details": {"warmup_max_count": 20, "reply_rate": 20, "is_warmup_blocked": False}},
    {"account": "PRECISE_LEADS", "client": "PL", "id": 2, "email": "smtp@example.com", "type": "SMTP",
     "warmup_reputation": 88.0, "warmup_details": {"warmup_max_count": 20, "reply_rate": 30, "is_warmup_blocked": True,
                                                   "blocked_reason": "Account blocked: authentication failures"}},
    {"account": "PRECISE_LEADS", "client": "Melior", "id": 3, "email": "m@example.com", "type": "OUTLOOK",
     "warmup_reputation": 86.0, "warmup_details": {"warmup_max_count": 10, "reply_rate": 65,
                                                   "is_warmup_blocked": False}}]}


def test_seed_makes_one_repairing_doc_per_inbox_with_before_and_after_or_why_not():
    docs = {d["email"]: d for d in wr.seed_docs(CHANGES, BEFORE, date(2026, 11, 17))}
    assert set(docs) == {"changed@example.com", "smtp@example.com", "m@example.com"}
    c = docs["changed@example.com"]
    assert (c["client"], c["kind"], c["started"], c["rep_at_start"], c["status"]) == (
        "Bettrdata", "google", "2026-11-17", 71, "repairing")
    assert c["settings_before"] == {"warmup_max_count": 20, "reply_rate": 20}
    assert c["settings_after"] == {"warmup_max_count": 10, "reply_rate": 35} and c["why_not_changed"] is None
    s = docs["smtp@example.com"]
    assert (s["client"], s["kind"], s["blocked"], s["settings_after"]) == ("Precise Leads", "smtp", True, None)
    assert s["why_not_changed"].startswith("warm-up blocked: credentials")
    m = docs["m@example.com"]
    assert (m["client"], m["kind"], m["settings_after"], m["blocked"]) == ("Melior", "outlook", None, False)
    assert "10/day" in m["why_not_changed"] and "65% reply" in m["why_not_changed"]


def test_a_change_that_failed_is_recorded_as_not_changed():
    changes = {"changed@example.com": {**CHANGES["Changed@Example.com"], "ok": False}}
    c = {d["email"]: d for d in wr.seed_docs(changes, BEFORE, date(2026, 11, 17))}["changed@example.com"]
    assert c["settings_after"] is None and "did not apply" in c["why_not_changed"]


def test_seeding_twice_keeps_the_first_docs():
    col = FakeCol()
    store = wr.RepairStore(col)
    assert store.seed(wr.seed_docs(CHANGES, BEFORE, date(2026, 11, 17))) == 3
    col.docs[0]["status"] = "recovered"
    assert store.seed(wr.seed_docs(CHANGES, BEFORE, date(2026, 11, 20))) == 0
    assert col.docs[0]["status"] == "recovered" and len(col.docs) == 3


# --- the rules ----------------------------------------------------------------------------------------------------

def test_recovered_at_the_types_good_reputation_on_the_latest_reading():
    d = wr.update(doc(), reading(rep=90), MON)
    assert (d["status"], d["recovered_on"], d["latest_rep"], d["latest_on"]) == ("recovered", "2026-11-23", 90, "2026-11-23")
    assert wr.update(doc(), reading(rep=89), MON)["status"] == "repairing"


def test_one_recovery_cutoff_the_all_inboxes_one_for_every_type():
    """Parent, 17 Nov: one cutoff, the All Inboxes one (config.MIN_WARMUP_REP_PCT, 90), for every inbox type."""
    from smartlead import config
    for kind in ("google", "outlook", "outlook_dense", "smtp", None):
        assert wr.level(kind) == int(config.MIN_WARMUP_REP_PCT) == 90
    smtp = doc(kind="smtp", type="SMTP", rep_at_start=60)
    assert wr.update(smtp, reading(rep=70, kind="smtp"), MON)["status"] == "repairing"   # no SMTP 70 any more
    assert wr.update(smtp, reading(rep=90, kind="smtp"), MON)["status"] == "recovered"
    new = wr.run([], [reading("s@example.com", rep=80, kind="smtp")], MON, read=set(), roster=None)[1]
    assert [d["email"] for d in new] == ["s@example.com"]                                 # SMTP 80 is enrolled


def test_a_blocked_warm_up_never_counts_as_recovered_until_a_reading_says_it_is_unblocked():
    blocked = doc(kind="smtp", type="SMTP", rep_at_start=88, blocked=True)
    d = wr.update(blocked, reading(rep=91, kind="smtp"), MON)               # the list says nothing about blocking
    assert d["status"] == "repairing" and d["blocked"] is True
    d = wr.update(d, reading(rep=91, kind="smtp", warmup_blocked=False), MON)
    assert d["status"] == "recovered" and d["blocked"] is False
    d = wr.update(doc(), reading(rep=95, warmup_blocked=True), MON)
    assert d["status"] == "repairing" and d["blocked"] is True


def test_worse_is_three_points_below_the_start():
    assert wr.worse(doc(latest_rep=68)) and not wr.worse(doc(latest_rep=69))
    assert not wr.worse(doc(latest_rep=None))


def test_no_improvement_after_three_weeks_unless_three_points_better():
    day21 = date(2026, 12, 8)
    assert wr.update(doc(), reading(rep=74, day="2026-12-08"), day21)["status"] == "no_improvement"
    assert wr.update(doc(), reading(rep=75, day="2026-12-08"), day21)["status"] == "repairing"
    assert wr.update(doc(), reading(rep=60, day="2026-12-07"), day21 - timedelta(days=1))["status"] == "repairing"
    assert wr.update(doc(blocked=True), reading(rep=71, day="2026-12-08"), day21)["status"] == "repairing"
    # a recovered inbox that drops below the line again is back in repair
    back = wr.update(doc(status="recovered", recovered_on="2026-11-20"), reading(rep=80), MON)
    assert back["status"] == "repairing" and back["recovered_on"] is None


def test_a_day_without_a_reading_keeps_the_last_one():
    d = wr.update(doc(latest_rep=80, latest_on="2026-11-20"), None, MON)
    assert (d["latest_rep"], d["latest_on"], d["status"]) == (80, "2026-11-20", "repairing")


# --- the daily run: readings, auto-enrolment, gone ------------------------------------------------------------------

def test_auto_enrol_takes_connected_low_inboxes_of_current_clients_only():
    recs = [reading("low@example.com", rep=80),
            reading("ok@example.com", rep=95),
            reading("off@example.com", rep=50, connected=False),
            reading("osc@example.com", rep=50, client="OSC", account="PRECISE_LEADS"),
            reading("none@example.com", rep=None),
            reading("smtp@example.com", rep=90, kind="smtp"),                 # at the one cutoff: healthy
            reading("smtp2@example.com", rep=60, kind="smtp", client="Precise Leads", account="PRECISE_LEADS"),
            reading("a@example.com", rep=60)]                                 # already in repair
    docs, new = wr.run([doc()], recs, MON, read={"BETTRDATA", "PRECISE_LEADS"}, roster=None)
    assert sorted(d["email"] for d in new) == ["low@example.com", "smtp2@example.com"]
    n = {d["email"]: d for d in new}["low@example.com"]
    assert (n["status"], n["started"], n["rep_at_start"], n["settings_after"], n["enrolled_by"]) == (
        "repairing", "2026-11-23", 80, None, "auto")
    assert "report-only" in n["why_not_changed"]
    assert {d["email"]: d for d in docs}["a@example.com"]["latest_rep"] == 60


def test_gone_only_with_evidence_the_inbox_left():
    roster = {"tracked": None, "present": {"b@example.com"}, "dropped": set()}
    docs, _ = wr.run([doc(), doc("b@example.com")], [], MON, read={"BETTRDATA"}, roster=roster)
    by = {d["email"]: d for d in docs}
    assert by["a@example.com"]["status"] == "gone" and by["a@example.com"]["gone_on"] == "2026-11-23"
    assert by["b@example.com"]["status"] == "repairing"               # still listed: missing from one read only
    docs, _ = wr.run([doc()], [], MON, read=set(), roster=roster)      # account not read in full: no verdict
    assert docs[0]["status"] == "repairing"


def test_the_tracker_never_changes_a_setting_cancels_or_deletes():
    import inspect
    src = inspect.getsource(wr)
    for word in ("SmartleadClient", "httpx", "requests", "delete_one", "delete_many", "update_warmup", "/warmup"):
        assert word not in src, word


# --- the digest ---------------------------------------------------------------------------------------------------

def test_monday_weekly_line_per_client_is_a_done_line():
    docs = [doc("a@example.com"), doc("b@example.com"), doc("c@example.com", latest_rep=60),
            doc("d@example.com", status="recovered", recovered_on="2026-11-21", latest_rep=92),
            doc("e@example.com", status="recovered", recovered_on="2026-11-10", latest_rep=92),
            doc("p@example.com", client="Precise Leads", account="PRECISE_LEADS", kind="smtp", blocked=True,
                rep_at_start=88, latest_rep=88)]
    asks, done, detail = wr.digest_items(docs, MON)
    assert asks == []
    assert [d.text for d in done] == ["Warm-up repair, PL: 1 repairing (1 warm-up blocked).",
                                      "Warm-up repair, BettrData: 3 repairing, 1 recovered this week, 1 worse."]
    assert all(d.key.startswith("warmup-repair:week:") and d.key.endswith("2026-11-23") for d in done)
    text = "\n".join(detail)
    assert "c@example.com" in text and "71% → 60%" in text and "worse" in text
    assert wr.digest_items(docs, MON + timedelta(days=1))[1] == []          # Tuesday: no weekly line


def test_three_weeks_without_improvement_is_the_one_ask():
    docs = [doc("a@example.com", status="no_improvement"), doc("b@example.com", status="no_improvement"),
            doc("c@example.com")]
    asks, _, detail = wr.digest_items(docs, MON + timedelta(days=2))
    (a,) = asks
    assert a.text == ("2 inboxes did not improve in 3 weeks of warm-up repair: cancel candidates, your call.")
    assert "Say which to cancel" in a.step and a.key == "warmup-repair:cancel-candidates" and a.kind == "ask"
    assert not a.urgent and "a@example.com" in "\n".join(detail)


def test_nothing_in_repair_says_nothing():
    assert wr.digest_items([doc(status="gone", gone_on="2026-11-01")], MON) == ([], [], [])


# --- wired into fleet watch's daily run ------------------------------------------------------------------------------

def test_fleet_watch_daily_tracks_repair_and_reports_to_its_own_digest_source(monkeypatch):
    from smartlead import digest as dg
    got = []
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail: got.append((src, asks, done, detail)) or True)
    repair = FakeCol()
    repair.docs = [doc()]
    s = fw.FleetStore(FakeCol(), FakeCol(), repair=repair)
    recs = [reading("a@example.com", rep=91), reading("n@example.com", rep=70)]
    fw.daily(s, recs, {}, {"BETTRDATA"}, {}, MON, [], post=True, poster=Poster())
    by = {d["email"]: d for d in repair.docs}
    assert by["a@example.com"]["status"] == "recovered" and by["n@example.com"]["status"] == "repairing"
    srcs = [g[0] for g in got]
    assert srcs == ["warmup-repair", "fleet-watch"]
    assert [d.text for d in got[0][2]] == ["Warm-up repair, BettrData: 1 repairing, 1 recovered this week."]


def test_a_repair_failure_never_stops_fleet_watch(monkeypatch):
    class Broken(FakeCol):
        def find(self, *a, **k): raise RuntimeError("no Mongo")
    s = fw.FleetStore(FakeCol(), FakeCol(), repair=Broken())
    out = fw.daily(s, [reading(rep=50)], {}, {"BETTRDATA"}, {}, MON, [], post=True, poster=Poster())
    assert out["records"] == 1 and "warm-up repair" in " ".join(out["errors"])


def test_the_record_keeps_whether_warm_up_is_blocked_when_the_list_says():
    a = {"id": 1, "from_email": "a@example.com", "type": "SMTP", "is_smtp_success": True, "is_imap_success": True,
         "warmup_details": {"status": "ACTIVE", "warmup_reputation": 88, "blocked_reason": "Account blocked: x"}}
    r = fw.record("PRECISE_LEADS", a, {}, {}, {"example.com": 1}, {}, {}, MON)
    assert r["warmup_blocked"] is True
    a["warmup_details"]["blocked_reason"] = None
    assert fw.record("PRECISE_LEADS", a, {}, {}, {"example.com": 1}, {}, {}, MON)["warmup_blocked"] is False
    del a["warmup_details"]["blocked_reason"]
    assert fw.record("PRECISE_LEADS", a, {}, {}, {"example.com": 1}, {}, {}, MON)["warmup_blocked"] is None


# --- a recovered inbox is free again by itself ------------------------------------------------------------------------

def test_a_recovered_inbox_with_a_v2_pass_is_free_in_all_inboxes_and_nothing_here_holds_it_back(monkeypatch):
    """All Inboxes frees an inbox once low_rep clears and it has a v2 pass (processing.py + usable.py); the tracker
    adds no reason of its own, even with the v2-pass switch on."""
    import inspect
    from smartlead import config, processing, usable
    monkeypatch.setattr(config, "AVAILABILITY_REQUIRE_V2_PASS", True)
    row = lambda: {"email": "a@example.com", "connection_ok": True, "message_per_day": 15,  # noqa: E731
                   "test_sheet_status": "inbox", "dns_spf_ok": True, "dns_dkim_ok": True, "dns_dmarc_ok": True,
                   "tags": []}
    gate = {"checked": True, "error": "", "listed": set(),
            "v2": {"a@example.com": {"verdict": "PASS", "night": "2026-11-21"}}}
    rep = lambda pct: {"a@example.com": {"rep": f"{pct}%", "warmup_state": "ramped", "warmup_max_count": 10,  # noqa: E731
                                         "last_active_date": "", "warmup_spam_count": 0}}
    low, back = row(), row()
    processing.process_inbox_availability([low], rep(85), {}, gate_data=gate)
    processing.process_inbox_availability([back], rep(92), {}, gate_data=gate)
    assert low["availability"] == "BUSY" and "low_rep" in low["busy_reason"]
    assert (back["availability"], back["busy_reason"]) == ("FREE", "")
    for mod in (processing, usable):
        assert "warmup_repair" not in inspect.getsource(mod) and "warmup-repair" not in inspect.getsource(mod)


# --- the seed / preview command ----------------------------------------------------------------------------------------

def _cli_db(monkeypatch, tmp_path):
    import json
    from smartlead import zapmail_asset_sync
    db = {"warmup_repair": FakeCol(), "inbox_daily": FakeCol()}
    db["inbox_daily"].docs = [
        {"email": "changed@example.com", "date": (date.today() - timedelta(days=2)).isoformat(), "rep": 75,
         "kind": "google", "client": "Bettrdata", "account": "BETTRDATA", "connected": True},
        {"email": "changed@example.com", "date": (date.today() - timedelta(days=1)).isoformat(), "rep": 78,
         "kind": "google", "client": "Bettrdata", "account": "BETTRDATA", "connected": True},
        {"email": "changed@example.com", "date": date.today().isoformat(), "sent": 3}]       # metrics only: no state
    monkeypatch.setattr(zapmail_asset_sync, "_db", lambda: db)
    (tmp_path / "c.json").write_text(json.dumps(CHANGES))
    (tmp_path / "b.json").write_text(json.dumps(BEFORE))
    return db, ["--changes", str(tmp_path / "c.json"), "--before", str(tmp_path / "b.json")]


def test_latest_readings_takes_the_newest_state_doc_per_inbox():
    import warmup_repair_cli as cli
    col = FakeCol()
    col.docs = [{"email": "a@example.com", "date": "2026-11-21", "rep": 70},
                {"email": "a@example.com", "date": "2026-11-22", "rep": 72},
                {"email": "a@example.com", "date": "2026-11-23", "sent": 4},
                {"email": "b@example.com", "date": "2026-11-01", "rep": 50}]          # older than 8 days
    assert [(r["email"], r["rep"]) for r in cli.latest_readings(col, MON)] == [("a@example.com", 72)]


def test_seed_dry_run_writes_nothing_and_prints_the_lines(monkeypatch, tmp_path, capsys):
    import warmup_repair_cli as cli
    db, files = _cli_db(monkeypatch, tmp_path)
    assert cli.main(["seed", *files, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert db["warmup_repair"].docs == [] and "3 to write" in out and "(dry run: nothing written)" in out
    assert "Warm-up repair, BettrData: 1 repairing." in out and "71% → 78%" in out
    assert cli.main(["seed", *files]) == 0 and len(db["warmup_repair"].docs) == 3
    assert cli.main(["seed", *files]) == 0 and "0 to write" in capsys.readouterr().out


def test_the_digest_names_the_source():
    import digest_post
    assert digest_post.LABELS["warmup-repair"] == "Warm-up repair"
