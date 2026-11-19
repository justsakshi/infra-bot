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
    assert [d.text for d in done] == ["Warm-up repair, PL: 1 recovering (1 warm-up blocked).",
                                      "Warm-up repair, BettrData: 3 recovering, 1 recovered this week, 1 worse."]
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
    assert "Say which of those listed to cancel" in a.step and "thread" not in a.step
    assert [x.split(" · ")[0] for x in a.detail] == ["• a@example.com", "• b@example.com"]   # 17 Nov: named in the ask
    assert a.key == "warmup-repair:cancel-candidates" and a.kind == "ask"
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
    assert [d.text for d in got[0][2]] == ["Warm-up repair, BettrData: 1 recovering, 1 recovered this week."]


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
    assert "Warm-up repair, BettrData: 1 recovering." in out and "71% → 78%" in out
    assert cli.main(["seed", *files]) == 0 and len(db["warmup_repair"].docs) == 3
    assert cli.main(["seed", *files]) == 0 and "0 to write" in capsys.readouterr().out


def test_the_digest_names_the_source():
    import digest_post
    assert digest_post.LABELS["warmup-repair"] == "Warm-up repair"


# --- every bad inbox, not only low warm-up (t325, 19 Nov 2026) ---------------------------------------------------------

HAND = "DO NOT USE - pulled by hand"
AUTO = "DO NOT USE - failed v2 (auto)"
FAIL = {"verdict": "FAIL", "night": "2026-11-21", "overridden": False}


def test_reasons_are_the_gate_codes_a_person_or_the_bot_can_repair():
    r = reading("a@example.com", rep=80, kind="outlook", tags=[HAND, AUTO], warmup_blocked=True, domain="example.com")
    assert wr.reasons(r, {"a@example.com": FAIL}, {"example.com"}) == [
        "low_rep", "warmup_blocked", "do_not_use", "v2_pulled", "failed_v2", "blocklisted"]
    assert wr.reasons(reading("ok@example.com", rep=95, kind="outlook"), {}, set()) == []


def test_a_google_v2_fail_and_a_fail_a_person_overrode_are_no_reason():
    g = reading("g@example.com", rep=100, kind="google")
    o = reading("o@example.com", rep=100, kind="outlook")
    assert wr.reasons(g, {"g@example.com": FAIL}, set()) == []
    assert wr.reasons(o, {"o@example.com": {**FAIL, "overridden": True}}, set()) == []


def test_an_inbox_its_vendor_warms_is_never_judged_on_smartlead_warm_up():
    r = reading("x@example.com", rep=40, warmup_blocked=True, warmup_by="InboxKit", tags=[HAND])
    assert wr.reasons(r, {}, set()) == ["do_not_use"]


def test_v2_or_blocklist_not_read_today_keeps_the_last_answer():
    r = reading("a@example.com", rep=100, kind="outlook")
    assert wr.reasons(r, None, None, prev=("failed_v2", "blocklisted")) == ["failed_v2", "blocklisted"]
    assert wr.reasons(r, {}, set(), prev=("failed_v2", "blocklisted")) == []


def test_every_bad_connected_inbox_of_the_three_clients_is_enrolled_whatever_the_reason():
    recs = [reading("v2@example.com", rep=100, kind="outlook"),
            reading("listed@listed.example", rep=97, kind="outlook", domain="listed.example"),
            reading("tag@example.com", rep=100, tags=[HAND]),
            reading("blocked@example.com", rep=100, warmup_blocked=True),
            reading("clean@example.com", rep=100),
            reading("dead@example.com", rep=100, tags=[HAND], connected=False),
            reading("osc@example.com", rep=100, tags=[HAND], client="OSC")]
    _, new = wr.run([], recs, MON, read={"BETTRDATA"}, roster=None, v2={"v2@example.com": FAIL}, listed={"listed.example"})
    by = {d["email"]: d for d in new}
    assert sorted(by) == ["blocked@example.com", "listed@listed.example", "tag@example.com", "v2@example.com"]
    assert by["v2@example.com"]["reasons"] == by["v2@example.com"]["reasons_at_start"] == ["failed_v2"]
    assert all(d["status"] == "repairing" and d["enrolled_by"] == "auto" for d in new)


def test_a_v2_fail_with_good_warm_up_recovers_on_its_retest_with_warm_up_left_alone():
    r = reading("v2@example.com", rep=100, kind="outlook")
    docs, _ = wr.run([], [r], MON, read=set(), roster=None, v2={"v2@example.com": FAIL}, listed=set())
    assert docs[0]["status"] == "repairing" and docs[0]["settings_after"] is None
    passed = {"v2@example.com": {"verdict": "PASS", "night": "2026-11-28", "overridden": False}}
    docs, _ = wr.run(docs, [r], MON + timedelta(days=6), read=set(), roster=None, v2=passed, listed=set())
    assert (docs[0]["status"], docs[0]["recovered_on"], docs[0]["reasons"]) == ("recovered", "2026-11-29", [])


def test_warm_up_repaired_but_still_tagged_by_a_person_is_ready_and_asks_for_the_untag():
    old = doc("a@example.com")                                   # a doc from before t325: no reasons
    docs, _ = wr.run([old], [reading("a@example.com", rep=92, tags=[HAND])], MON, read=set(), roster=None,
                     v2={}, listed=set())
    assert docs[0]["status"] == "ready" and docs[0]["reasons_at_start"] == ["low_rep", "do_not_use"]
    asks, _, detail = wr.digest_items(docs, MON + timedelta(days=1))
    (a,) = asks
    assert a.key == "warmup-repair:ready-to-untag" and "untag is your call" in a.text and "never removes" in a.step
    assert "a@example.com" in a.detail[0] and detail
    docs, _ = wr.run(docs, [reading("a@example.com", rep=92)], MON, read=set(), roster=None, v2={}, listed=set())
    assert docs[0]["status"] == "recovered"


def test_a_tag_only_inbox_with_good_warm_up_is_not_called_repaired():
    docs, _ = wr.run([], [reading("t@example.com", rep=100, tags=[HAND])], MON, read=set(), roster=None,
                     v2={}, listed=set())
    assert docs[0]["status"] == "repairing"                      # never had a warm-up problem: its path is the tag


def test_under_ninety_and_not_on_the_repair_settings_is_an_ask():
    def low(e, per_day, reply):
        return reading(e, rep=80, kind="outlook", warmup_per_day=per_day, reply_rate=reply)
    recs = [low("hi@example.com", 16, 65), low("ok@example.com", 10, 35), low("lower@example.com", 5, 30),
            low("unread@example.com", None, None), reading("off@example.com", rep=80, warmup_on=False,
                                                           warmup_per_day=20, reply_rate=20)]
    docs, _ = wr.run([], recs, MON, read=set(), roster=None, v2={}, listed=set())
    asks, _, _ = wr.digest_items(docs, MON + timedelta(days=1))
    (a,) = asks
    assert a.key == "warmup-repair:not-on-repair-settings" and [x.split(" · ")[0] for x in a.detail] == ["• hi@example.com"]
    assert "16/day, 65% reply" in a.detail[0] and "never higher than now" in a.step


def test_the_weekly_line_and_thread_show_other_recovery_per_domain():
    recs = [reading(f"m{i}@listed.example", rep=97, kind="outlook", domain="listed.example", client="Melior",
                    account="PRECISE_LEADS", live_campaigns=["Live 1"] if i < 2 else []) for i in range(3)]
    recs += [reading("w@example.com", rep=70, client="Melior", account="PRECISE_LEADS")]
    docs, _ = wr.run([], recs, MON, read=set(), roster=None, v2={}, listed={"listed.example"})
    _, done, detail = wr.digest_items(docs, MON)
    assert [d.text for d in done] == ["Warm-up repair, Melior: 1 recovering. Other recovery: 3 listed domain."]
    text = "\n".join(detail)
    assert "• listed.example · 3 inboxes outlook · listed domain · back when its domain is off Spamhaus DBL / URIBL" in text
    assert "2 still in live campaigns" in text and "w@example.com" in text


def test_fleet_watch_hands_the_tracker_the_v2_verdicts_and_the_blocklist(monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "off")
    repair, verdicts = FakeCol(), FakeCol()
    verdicts.docs = [{"email": "v2@example.com", "night": "2026-11-21", "verdict": "FAIL"}]
    s = fw.FleetStore(FakeCol(), FakeCol(), repair=repair, verdicts=verdicts)
    recs = [reading("v2@example.com", rep=100, kind="outlook"),
            reading("l@listed.example", rep=100, kind="outlook", domain="listed.example")]
    blocklist = {"listed.example": {"domain": "listed.example", "date": "2026-11-22", "listed_on": ["spamhaus dbl"]}}
    fw.daily(s, recs, {}, {"BETTRDATA"}, blocklist, MON, [], post=False, poster=Poster())
    assert {d["email"]: d["reasons"] for d in repair.docs} == {"v2@example.com": ["failed_v2"],
                                                               "l@listed.example": ["blocklisted"]}


def test_preview_judges_with_the_v2_verdicts_and_blocklist_fleet_watch_reads(monkeypatch, capsys):
    import warmup_repair_cli as cli
    from smartlead import zapmail_asset_sync
    from smartlead.config import BLACKLIST_COLLECTION
    db = {"warmup_repair": FakeCol(), "inbox_daily": FakeCol(), "v2_verdicts": FakeCol(), BLACKLIST_COLLECTION: FakeCol()}
    db["inbox_daily"].docs = [{"email": "v2@example.com", "domain": "example.com", "date": date.today().isoformat(),
                               "rep": 100, "kind": "outlook", "client": "Melior", "account": "PRECISE_LEADS",
                               "connected": True}]
    db["v2_verdicts"].docs = [{"email": "v2@example.com", "night": date.today().isoformat(), "verdict": "FAIL"}]
    monkeypatch.setattr(zapmail_asset_sync, "_db", lambda: db)
    assert cli.main(["preview"]) == 0
    out = capsys.readouterr().out
    assert "enrolled by the bot today (report-only): 1" in out and "Other recovery: 1 v2 fail." in out
    assert db["warmup_repair"].docs == []                        # preview writes nothing


def test_an_inbox_the_gate_calls_dead_is_never_enrolled():
    status = FakeCol()
    status.docs = [{"email": "dns@example.com", "busy_reason": "warmup_blocked,dns_error"},
                   {"email": "bad@example.com", "busy_reason": "no_capacity,do_not_use"}]
    s = fw.FleetStore(FakeCol(), FakeCol(), repair=FakeCol(), status=status)
    recs = [reading("dns@example.com", rep=100, warmup_blocked=True), reading("bad@example.com", rep=100, tags=[HAND])]
    fw.daily(s, recs, {}, {"BETTRDATA"}, {}, MON, [], post=False, poster=Poster())
    assert [d["email"] for d in s.repair.docs] == ["bad@example.com"]
    assert fw._repair_inputs(fw.FleetStore(FakeCol(), FakeCol()), None) == (None, None, set())   # nothing read
