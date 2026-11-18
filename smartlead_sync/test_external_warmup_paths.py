"""Every path that reads or changes Smartlead warm-up leaves a vendor-warmed inbox alone (t248, 18 Nov 2026, Manveen:
"never turn off warmup for them inboxxkit handles warmup"). Synthetic inboxes only; no network."""
from datetime import date

import pytest

from smartlead import external_warmup as xw

EXT = xw.STATE


# the warm-up planner (warmup_executor.py): never enables, boosts or retunes a vendor-warmed inbox ------------------

@pytest.mark.parametrize("always_on", [True, False])
def test_the_warm_up_planner_never_touches_a_vendor_warmed_inbox(monkeypatch, always_on):
    import smartlead.warmup_planner as wp
    monkeypatch.setattr(wp, "WARMUP_ALWAYS_ON", always_on)
    rows = [{"email": "k@kit.test", "warmup_state": EXT, "campaign_status": "", "campaign_is_stale": False,
             "account_id": "1", "client": "C", "warmup_rep_pct": "0%"},
            {"email": "s@self.test", "warmup_state": "off", "campaign_status": "", "campaign_is_stale": False,
             "account_id": "2", "client": "C", "warmup_rep_pct": "0%"}]
    got = {c["email"] for c in wp.plan_warmup_changes(rows)}
    assert "k@kit.test" not in got and "s@self.test" in got


# fleet counts: a vendor-warmed inbox is not "unhealthy" or off the bench for its Smartlead warm-up -----------------

def test_capacity_counts_a_vendor_warmed_inbox_on_its_other_checks():
    from smartlead.capacity import _healthy
    row = {"warmup_rep_pct": "0%", "warmup_state": EXT, "test_sheet_status": "inbox", "connection_ok": True}
    assert _healthy(row)
    assert not _healthy({**row, "connection_ok": False}) and not _healthy({**row, "test_sheet_status": "fail"})


def test_rotation_can_bench_a_vendor_warmed_inbox():
    from smartlead.rotation_planner import _is_bench
    row = {"test_sheet_status": "inbox", "warmup_rep_pct": "0%", "connection_ok": True, "warmup_state": EXT,
           "available_capacity": 10}
    assert _is_bench(row) and not _is_bench({**row, "warmup_state": "off"})


# fleet watch: no warm-up case, no digest ask, no warm-up repair entry ---------------------------------------------

FLEET_DAY = date(2026, 11, 18)


def _acct(email="k@kit.test", **over):
    a = {"id": 7, "from_email": email, "type": "GMAIL", "smtp_host": None, "message_per_day": 10, "client_id": None,
         "is_smtp_success": True, "is_imap_success": True, "tags": [],
         "warmup_details": {"status": "INACTIVE", "warmup_reputation": "0%", "blocked_reason": None,
                            "warmup_created_at": "2026-11-18T05:00:00Z"}}
    a.update(over)
    return a


def _rec(email="k@kit.test", marks=None, **over):
    import fleet_watch as fw
    facts = {email: {"client": "Precise Leads", "connected": True, "campaigns": ["C 1"], "sending": ["C 1"]}}
    return fw.record("PRECISE_LEADS", _acct(email, **over), facts, verdicts={}, per_domain={email.split("@")[1]: 3},
                     approved={}, health={}, today=FLEET_DAY, warm={email: (40, 30)}, external=marks)


def test_the_fleet_record_says_who_warms_it_and_has_no_ready_date_of_its_own():
    r = _rec(marks={"kit.test": "InboxKit"})
    assert r["warmup_by"] == "InboxKit" and r["ready_on"] is None
    assert _rec()["warmup_by"] is None


def test_fleet_checks_raise_no_warm_up_case_for_a_vendor_warmed_inbox():
    from smartlead import fleet as fl
    marked, plain = _rec(marks={"kit.test": "InboxKit"}), _rec("s@self.test")
    hist = {r["email"]: [{**r, "rep": 0}] * 7 for r in (marked, plain)}
    kinds = {(c["kind"], c["target"]) for c in fl.checks([marked, plain], hist, {}, FLEET_DAY)}
    assert ("warmup_off", "s@self.test") in kinds and ("low_rep", "s@self.test") in kinds \
        and ("warmup_spam", "s@self.test") in kinds
    assert not [k for k in kinds if k[1] == "k@kit.test"]
    def opened(rs):
        return [{**c, "opened_on": "2026-11-18"} for c in fl.checks(rs, hist, {}, FLEET_DAY)]
    asks, _ = fl.digest_items(opened([marked]), FLEET_DAY)
    assert asks == []                                      # the 17 Nov "warm-up off on N inboxes" ask is not raised
    asks, _ = fl.digest_items(opened([plain]), FLEET_DAY)
    assert any("warm-up" in str(getattr(a, "text", a)).lower() for a in asks)


def test_fleet_still_judges_a_vendor_warmed_inbox_on_connection():
    from smartlead import fleet as fl
    r = _rec(marks={"kit.test": "InboxKit"}, is_smtp_success=False)
    r["connected"] = False
    kinds = {c["kind"] for c in fl.checks([r], {}, {}, FLEET_DAY)}
    assert "disconnected" in kinds and not kinds & set(fl.WARMUP_KINDS)


def test_an_open_warm_up_case_on_an_inbox_marked_later_closes_as_cleared():
    from smartlead import fleet as fl
    r = _rec(marks={"kit.test": "InboxKit"})
    for kind in ("warmup_off", "low_rep", "warmup_spam"):
        case = {"kind": kind, "target": r["email"], "domain": r["domain"], "scope": "inbox", "key": f"{kind}:x"}
        assert fl.evidence(case, [r], True, set(), {}) == "cleared"


def test_warm_up_repair_never_enrols_a_vendor_warmed_inbox():
    from smartlead import warmup_repair as wr
    marked = {**_rec(marks={"kit.test": "InboxKit"}), "rep": 0}
    plain = {**_rec("s@self.test"), "rep": 50}
    _, new = wr.run([], [marked, plain], FLEET_DAY, read={"PRECISE_LEADS"}, roster=None)
    assert [d["email"] for d in new] == ["s@self.test"]


def test_fleet_watch_gather_reads_the_marks_once_and_hands_them_to_every_record():
    import fleet_watch as fw                     # gather needs live Smartlead; its wiring is read from the source
    src = open(fw.__file__, encoding="utf-8").read()
    body = src[src.index("def gather("):src.index("def warm_counts(")]
    assert body.count("xw.load_live()") == 1 and "external=external" in body


# inbox intake (live every 2 hours): never asks whose they are, never writes warm-up, limits or tags ---------------

def test_intake_leaves_a_vendor_warmed_inbox_alone_with_or_without_a_client():
    import asyncio
    from smartlead import inbox_intake as ii
    from test_inbox_intake import FakeCol, FakeSL, PROF, SINCE, TODAY, acct
    kit = [acct(i, f"k{i}@kit.test") for i in (1, 2, 3)]
    for a in kit:
        a["warmup_details"].update(status="INACTIVE", warmup_reputation="0%")
    melior = [acct(4, "m@kit2.test", client_id=12256)]                         # a client intake knows
    for marks, accounts in (({"kit.test": "InboxKit"}, kit), ({"m@kit2.test": "InboxKit"}, melior)):
        sl = FakeSL(accounts)
        store = ii.IntakeStore(FakeCol())
        store.col.docs["kit.test"] = {"email": "kit.test", "answer": "Precise Leads"}   # even if someone answered
        out = asyncio.run(ii.run_account(sl, "PRECISE_LEADS", approve=True, since=SINCE, today=TODAY, by_domain={},
                                         tracker={}, store=store, external=marks,
                                         profile=lambda c: {**PROF, "smartlead_client_id": 12256} if c == "Melior" else PROF))
        assert not sl.writes and not sl.tagged and not out["ask"] and not out["set_up"] and not out["would"]
    sl = FakeSL(kit)                                                           # unmarked: intake asks, as before
    out = asyncio.run(ii.run_account(sl, "PRECISE_LEADS", approve=True, since=SINCE, today=TODAY, by_domain={},
                                     tracker={}, store=ii.IntakeStore(FakeCol()), profile=lambda c: PROF))
    assert [a["domain"] for a in out["ask"]] == ["kit.test"]


def test_intake_drift_sweep_never_reports_a_vendor_warmed_inbox():
    from smartlead import inbox_intake as ii
    from test_inbox_intake import acct
    a = acct(1, "k@kit.test", tags=[(9, "Bot-Setup")])
    a["warmup_details"].update(status="INACTIVE")
    assert ii._drift_sweep([a], {"kit.test": 3})                               # unmarked: "warmup off" drift
    assert ii._drift_sweep([a], {"kit.test": 3}, external={"kit.test": "InboxKit"}) == []


def test_the_intake_job_reads_the_marks():
    src = open("inbox_intake_cli.py", encoding="utf-8").read()
    assert "load_live()" in src and "external=external" in src


# retest executor (RETEST_DISABLE_WARMUP): never switches a vendor-warmed inbox's warm-up off, so never back on ----

def test_the_retest_executor_never_toggles_warm_up_on_a_vendor_warmed_sender():
    import retest_executor as rx
    ids = rx.warmup_toggle_ids(["1", "2"], ["k@kit.test", "s@self.test"], {"kit.test": "InboxKit"})
    assert ids == ["2"]
    assert rx.warmup_toggle_ids(["1"], ["k@kit.test"], {}) == ["1"]
    src = open(rx.__file__, encoding="utf-8").read()
    assert "for aid in warmup_toggle_ids(sender_ids, senders, external)" in src and "load_live()" in src


# infra audit: no "warm-up off while sending", "warm-up below cold" or "young inbox in campaign" finding -----------

def test_the_infra_audit_raises_no_warm_up_finding_for_a_vendor_warmed_inbox():
    from datetime import datetime, timezone
    from smartlead import infra_audit as ia

    def box(email):
        return {"from_email": email, "type": "GMAIL", "message_per_day": 10, "signature": "x", "from_name": "A B",
                "tags": [], "warmup_details": {"status": "INACTIVE", "max_email_per_day": 2,
                                               "warmup_created_at": "2026-11-18T05:00:00Z"}}
    now = datetime(2026, 11, 20, tzinfo=timezone.utc)
    accounts = [box("k@kit.test"), box("s@self.test")]
    active = {"k@kit.test": ["C 1"], "s@self.test": ["C 1"]}
    got = ia.check_mailboxes(accounts, {}, active, now, external={"kit.test": "InboxKit"})
    warm = {"warmup_off", "warmup_below_cold", "young_inbox_in_campaign"}
    by = {(f["check"], f["subject"]) for f in got}
    assert {k for k, t in by if t == "s@self.test"} >= warm
    assert not {k for k, t in by if t == "k@kit.test"} & warm


# renewal review: a vendor-warmed inbox is never "CHECK" for its Smartlead warm-up reputation -----------------------

def test_the_renewal_review_never_judges_a_vendor_warmed_inbox_on_smartlead_reputation():
    from smartlead import renewal_review as rr
    facts = {"in_smartlead": True, "connected": True, "reputation": "0%", "tags": []}
    v, why = rr.judge_inbox("s@self.test", facts, None, FLEET_DAY, v2=[])
    assert v == "CHECK" and "warmup reputation 0%" in why
    v, why = rr.judge_inbox("k@kit.test", {**facts, "warmup_by": "InboxKit"}, None, FLEET_DAY, v2=[])
    assert "reputation" not in why
    src = open("renewal_review.py", encoding="utf-8").read()
    assert '"warmup_by": warm_vendor(' in src
