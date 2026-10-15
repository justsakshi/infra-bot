"""Inbox intake: new inboxes set up once, proven by read-back (spec 2026-10-13). No network."""
import asyncio
import copy
from datetime import date

from smartlead import inbox_intake as ii

TODAY, SINCE = date(2026, 10, 13), date(2026, 10, 12)
PROF = {"signature_lines": ["{full_name}", "Precise Leads"], "company": "Precise Leads", "website": "preciseleads.in"}
MELIOR = {**PROF, "smartlead_client_id": 12256}
TAGS = [{"id": 3212, "name": "PreciseLeads"}, {"id": 266037, "name": "Melior"}, {"id": 153335, "name": "ScaledMail-Google"},
        {"id": 154156, "name": "ScaledMail-Microsoft"}, {"id": 9, "name": "Bot-Setup"}]


def acct(i, email, *, created="2026-10-13T10:00:00Z", type_="GMAIL", tags=(), client_id=None):
    return {"id": i, "from_email": email, "from_name": "Avinash Haridas", "type": type_, "smtp_host": None,
            "created_at": created, "client_id": client_id, "signature": "", "message_per_day": 10, "minTimeToWaitInMins": None,
            "custom_tracking_domain": None, "tags": [{"tag_id": t, "tag_name": n} for t, n in tags],
            "warmup_details": {"status": "ACTIVE", "max_email_per_day": 30, "reply_rate": 65,
                               "warmup_created_at": "2026-10-13T10:49:25Z", "warmup_reputation": "100%"}}


class FakeSL:
    """Applies writes to its accounts, so read-back sees them; `ignore` drops a field."""
    def __init__(self, accounts, ignore=(), fail=()):
        self.accounts = {a["id"]: copy.deepcopy(a) for a in accounts}   # never mutate the shared fixtures
        self.ignore, self.fail, self.tagged, self.writes = set(ignore), set(fail), [], []

    async def list_email_accounts(self): return list(self.accounts.values())
    async def list_tags(self): return TAGS

    async def update_email_account(self, i, fields):
        if int(i) in self.fail:
            raise RuntimeError("429 for url 'https://server.smartlead.ai/x?api_key=SECRET'")
        self.writes.append((int(i), dict(fields)))
        a = self.accounts[int(i)]
        if "max_email_per_day" in fields and "limit" not in self.ignore:
            a["message_per_day"] = fields["max_email_per_day"]
        if "time_to_wait_in_mins" in fields and "gap" not in self.ignore:
            a["minTimeToWaitInMins"] = fields["time_to_wait_in_mins"]
        for k in ("signature", "client_id"):
            if k in fields:
                a[k] = fields[k]

    async def set_warmup(self, i, enabled, total_per_day, daily_rampup, reply_rate, auto_adjust, weekdays_only):
        w = self.accounts[int(i)]["warmup_details"]
        w["max_email_per_day"] = total_per_day
        w["reply_rate"] = reply_rate if "reply" not in self.ignore else w["reply_rate"]

    async def get_email_account(self, i):   # the single read has no minTimeToWaitInMins (live 15 Oct)
        return {k: v for k, v in self.accounts[int(i)].items() if k != "minTimeToWaitInMins"}
    async def assign_tags(self, ids, tag_ids): self.tagged.append((sorted(ids), sorted(tag_ids)))


class FakeCol:
    def __init__(self): self.docs = {}
    def find_one(self, q, proj=None): return self.docs.get(q["email"])
    def update_one(self, q, upd, upsert=False):
        d = self.docs.setdefault(q["email"], {"email": q["email"], "tries": 0, "history": []})
        d.update(upd.get("$set", {}))
        for k, v in upd.get("$inc", {}).items(): d[k] = d.get(k, 0) + v
        for k, v in upd.get("$push", {}).items(): d[k].append(v)


PLAN = {"plan_id": "13845d", "status": "placed", "client": "Precise Leads", "provider": "google",
        "domains": ["warmmeetings.com"], "mailboxes": 3}


def run(sl, store=None, approve=True, plans=(PLAN,), jobs=(), tracker=None, account="PRECISE_LEADS"):
    store = store or ii.IntakeStore(FakeCol())
    out = asyncio.run(ii.run_account(sl, account, approve=approve, since=SINCE, today=TODAY,
                                     by_domain=ii.deliveries(list(plans), list(jobs)), tracker=tracker or {},
                                     store=store, profile=lambda c: MELIOR if c == "Melior" else PROF))
    return out, store


THREE = [acct(i, f"a{i}@warmmeetings.com") for i in (1, 2, 3)]


def test_new_scaledmail_inboxes_are_set_up_proven_and_tagged():
    sl = FakeSL(THREE)
    out, store = run(sl)
    assert [r["email"] for r in out["set_up"]] == ["a1@warmmeetings.com", "a2@warmmeetings.com", "a3@warmmeetings.com"]
    a = sl.accounts[1]
    assert a["message_per_day"] == 15 and a["warmup_details"]["max_email_per_day"] == 25 and a["warmup_details"]["reply_rate"] == 30
    assert "Avinash Haridas" in a["signature"] and ("custom_tracking_url", "") in sl.writes[0][1].items()
    assert sl.tagged == [([1, 2, 3], [9, 3212, 153335])]
    assert store.done("a1@warmmeetings.com") and out["complete"] == ["13845d"]
    assert out["set_up"][0]["ready_on"] == date(2026, 10, 27)


def test_preview_writes_nothing():
    sl = FakeSL(THREE)
    out, store = run(sl, approve=False)
    assert len(out["would"]) == 3 and not out["set_up"] and not sl.writes and not sl.tagged
    assert not store.done("a1@warmmeetings.com")


def test_old_or_already_done_inboxes_are_never_written():           # Review Focus 1
    old = acct(7, "old@warmmeetings.com", created="2026-09-01T00:00:00Z")
    tagged = acct(8, "t@warmmeetings.com", tags=[(9, "Bot-Setup")])
    sl = FakeSL([old, tagged])
    out, _ = run(sl, plans=())
    assert not sl.writes and not out["set_up"]
    assert out["problems"] == ["Changed since setup on 1 inbox (t@warmmeetings.com): daily limit 10, want 15; "
                               "warmup 30/day, want 25; reply rate 65%, want 30%; minimum gap empty, want 20 min"]


def test_unknown_client_is_reported_not_guessed():                  # Review Focus 2
    sl = FakeSL([acct(5, "x@mystery.com")])
    out, _ = run(sl, plans=())
    assert not sl.writes and any("mystery.com" in p and "no client" in p for p in out["problems"])


def test_melior_from_tracker_gets_melior_client():
    sl = FakeSL([acct(6, "r@meliorbase.com")])
    out, _ = run(sl, plans=(), tracker={"meliorbase.com": {"client": "Melior", "provider": "Scaledmail"}})
    assert sl.accounts[6]["client_id"] == 12256 and out["set_up"][0]["client"] == "Melior"


def test_readback_mismatch_is_retried_then_stuck():                 # Review Focus 3
    store = ii.IntakeStore(FakeCol())
    for n in range(3):
        sl = FakeSL([acct(1, "a1@warmmeetings.com")], ignore={"reply"})
        out, _ = run(sl, store=store, plans=({**PLAN, "mailboxes": 1},))
        assert not out["set_up"] and not sl.tagged
        assert any("reply rate 65%, want 30%" in p for p in out["problems"])
    sl = FakeSL([acct(1, "a1@warmmeetings.com")], ignore={"reply"})
    out, _ = run(sl, store=store, plans=({**PLAN, "mailboxes": 1},))
    assert not sl.writes and any("stuck after 3 tries" in p for p in out["problems"])


def test_partial_delivery_waits():                                  # Review Focus 4
    sl = FakeSL(THREE[:2])
    out, _ = run(sl)
    assert not sl.writes and out["waiting"] == ["⏳ 2 of 3 inboxes in Smartlead for ScaledMail order 13845d (warmmeetings.com), waiting for the rest"]


def test_one_failing_inbox_does_not_stop_the_rest_and_hides_keys():  # Review Focus 5
    sl = FakeSL(THREE, fail={2})
    out, _ = run(sl)
    assert [r["email"] for r in out["set_up"]] == ["a1@warmmeetings.com", "a3@warmmeetings.com"]
    msg = " ".join(out["problems"])
    assert "a2@warmmeetings.com" in msg and "SECRET" not in msg and out["complete"] == []


def test_dense_outlook_gets_five_and_five():
    boxes = [acct(i, f"o{i}@qualifiedcalendar.com", type_="OUTLOOK") for i in range(1, 26)]
    sl = FakeSL(boxes)
    plan = {"plan_id": "85b1cd", "status": "placed", "client": "Precise Leads", "provider": "outlook",
            "domains": ["qualifiedcalendar.com"], "mailboxes": 25}
    out, _ = run(sl, plans=(plan,))
    assert len(out["set_up"]) == 25 and sl.accounts[1]["message_per_day"] == 5
    assert sl.accounts[1]["warmup_details"]["max_email_per_day"] == 5 and out["set_up"][0]["kind"] == "outlook_dense"
    assert [len(ids) for ids, _ in sl.tagged] == [25]


def test_intake_message_headline_and_ready_dates():
    from inbox_intake_cli import intake_message
    res = {"PRECISE_LEADS": {
        "set_up": [{"email": f"a{i}@warmmeetings.com", "client": "Precise Leads", "kind": "google", "vendor": "scaledmail",
                    "domain": "warmmeetings.com", "ready_on": date(2026, 10, 27)} for i in range(3)],
        "would": [], "waiting": ["⏳ 2 of 21 inboxes in Smartlead for ScaledMail order 2cb59b (x.com), waiting for the rest"],
        "problems": ["`x@mystery.com`: no client known for mystery.com (no order, job or tracker row), not set up"],
        "complete": ["13845d"]}}
    head, parts = intake_message(res, TODAY)
    assert head[0] == "⚠️ Set up 3 new inboxes in Smartlead, 1 needs a look"
    assert head[1] == "Precise Leads 3 · ready to use from 27 Oct"
    body = "\n".join(parts)
    assert "warmmeetings.com · Google · 3 inboxes · limit 15, warmup 25/day, reply 30%, weekdays only · ready from 27 Oct" in body
    assert "⏳ 2 of 21" in body and "mystery.com" in body
    assert "Smartlead does not show ramp, auto-adjust or weekdays-only, so those are set but not re-read." in body
    preview = intake_message({"PRECISE_LEADS": {**res["PRECISE_LEADS"], "set_up": [], "would": res["PRECISE_LEADS"]["set_up"]}}, TODAY)[0]
    assert preview[0].startswith("👀 Would set up 3 new inboxes")


def test_readiness_lines_group_by_domain():
    def a(e, t="GMAIL", rep="100%", start="2026-10-13T10:00:00Z"):
        return {"from_email": e, "type": t, "tags": [{"tag_name": "Bot-Setup"}],
                "warmup_details": {"warmup_created_at": start, "warmup_reputation": rep}}
    boxes = [a("x1@warmmeetings.com"), a("x2@warmmeetings.com"), a("y@low.com", rep="80%"),
             a("z@notyet.com", t="OUTLOOK"), {**a("n@untracked.com"), "tags": []}]
    r = ii.readiness_lines(boxes, date(2026, 10, 27))
    assert r["ready_today"] == ["2 Google inboxes on warmmeetings.com"]
    assert r["low"] == ["1 Google inbox on low.com: warmup reputation 80% (needs 90%)"]


# ── final-review fixes ─────────────────────────────────────────────────────

NO_SETUP_TAG = [t for t in TAGS if t["name"] != "Bot-Setup"]


class FakeSLNoTag(FakeSL):
    async def list_tags(self): return NO_SETUP_TAG


def test_missing_setup_tag_blocks_writes_but_preview_still_lists():      # review 1
    sl = FakeSLNoTag(THREE)
    out, store = run(sl)
    assert not sl.writes and not out["set_up"] and not store.done("a1@warmmeetings.com")
    assert any("no tag `Bot-Setup`" in p and "nothing was set up" in p for p in out["problems"])
    prev, _ = run(FakeSLNoTag(THREE), approve=False)
    assert len(prev["would"]) == 3 and any("no tag `Bot-Setup`" in p for p in prev["problems"])


def test_missing_tag_is_quiet_when_nothing_is_new():                     # review 1 / executor finding
    old = acct(7, "old@x.com", created="2026-09-01T00:00:00Z")
    out, _ = run(FakeSLNoTag([old]), plans=(), account="BETTRDATA")
    assert out["problems"] == []


class FakeSLTagFails(FakeSL):
    async def assign_tags(self, ids, tag_ids): raise RuntimeError("429 on tag-mapping")


def test_tag_failure_leaves_inboxes_pending_and_retried():              # review 2
    store = ii.IntakeStore(FakeCol())
    out, _ = run(FakeSLTagFails(THREE), store=store)
    assert not out["set_up"] and not store.done("a1@warmmeetings.com") and out["complete"] == []
    assert any("tags not added" in p for p in out["problems"])
    sl = FakeSL(THREE)
    out, _ = run(sl, store=store)
    assert len(out["set_up"]) == 3 and sl.tagged and out["complete"] == ["13845d"]


def test_order_completes_across_runs():                                  # review 3
    store = ii.IntakeStore(FakeCol())
    out, _ = run(FakeSL(THREE, fail={2}), store=store)
    assert out["complete"] == []
    out, _ = run(FakeSL(THREE), store=store)
    assert [r["email"] for r in out["set_up"]] == ["a2@warmmeetings.com"] and out["complete"] == ["13845d"]


def test_old_inboxes_on_an_owned_domain_do_not_open_the_gate():          # review 5
    old = [acct(i, f"old{i}@warmmeetings.com", created="2026-09-01T00:00:00Z") for i in (7, 8)]
    sl = FakeSL(old + THREE[:1])
    out, _ = run(sl)
    assert not sl.writes and out["waiting"][0].startswith("⏳ 1 of 3 inboxes")


def test_only_limits_the_run_to_one_domain_or_inbox():                  # review 6
    other = {**PLAN, "plan_id": "x1", "domains": ["other.com"], "mailboxes": 1}
    sl = FakeSL(THREE + [acct(4, "b@other.com")])
    store = ii.IntakeStore(FakeCol())
    out = asyncio.run(ii.run_account(sl, "PRECISE_LEADS", approve=True, since=SINCE, today=TODAY,
                                     by_domain=ii.deliveries([PLAN, other], []), tracker={}, store=store,
                                     profile=lambda c: PROF, only="b@other.com"))
    assert [r["email"] for r in out["set_up"]] == ["b@other.com"] and {i for i, _ in sl.writes} == {4}


def test_before_values_are_recorded():                                   # review 10 (spec: before/after)
    col = FakeCol()
    run(FakeSL(THREE), store=ii.IntakeStore(col))
    assert col.docs["a1@warmmeetings.com"]["before"] == {"message_per_day": 10, "warmup_per_day": 30,
                                                         "reply_rate": 65, "tracking": None}


def test_smartlead_outages_do_not_use_up_tries():                         # review 11
    store = ii.IntakeStore(FakeCol())
    one = {**PLAN, "mailboxes": 1}
    for _ in range(4):
        out, _ = run(FakeSL([acct(1, "a1@warmmeetings.com")], fail={1}), store=store, plans=(one,))
        assert not any("stuck" in p for p in out["problems"])
    out, _ = run(FakeSL([acct(1, "a1@warmmeetings.com")]), store=store, plans=(one,))
    assert len(out["set_up"]) == 1


def test_channel_post_only_for_new_news():                                # review 4
    from inbox_intake_cli import should_post
    res = {"A": {"set_up": [], "problems": ["p1"]}}
    assert should_post(res, previous=[]) is True
    assert should_post(res, previous=["p1"]) is False
    assert should_post({"A": {"set_up": [{"email": "x"}], "problems": ["p1"]}}, previous=["p1"]) is True
    col = FakeCol()
    s = ii.IntakeStore(col)
    s.save_posted_problems(["p1", "p2"])
    assert s.posted_problems() == ["p1", "p2"]


def test_old_readiness_preview_is_off_when_intake_runs(monkeypatch):     # review 3 (digest)
    import scaledmail_cli as smc
    monkeypatch.delenv("INBOX_READY_ENABLED", raising=False)
    monkeypatch.setenv("INBOX_INTAKE_ENABLED", "true")
    assert smc._new_inboxes_handled() is True
    monkeypatch.setenv("INBOX_INTAKE_ENABLED", "false")
    assert smc._new_inboxes_handled() is False


def test_gap_that_does_not_stick_keeps_the_inbox_pending():                # 15 Oct: gap was left empty
    sl = FakeSL(THREE, ignore={"gap"})
    out, store = run(sl)
    assert not out["set_up"] and not sl.tagged and not store.done("a1@warmmeetings.com")
    assert any("minimum gap empty, want 20 min" in p for p in out["problems"])
    assert all(("time_to_wait_in_mins", 20) in w.items() for _, w in sl.writes)


def test_drift_on_set_up_inboxes_is_reported_grouped():
    done = {"tags": [(9, "Bot-Setup"), (153335, "ScaledMail-Google")]}
    good = acct(1, "ok@d1.com", **done)
    good.update({"message_per_day": 15, "minTimeToWaitInMins": 20})
    good["warmup_details"].update({"max_email_per_day": 25, "reply_rate": 30})
    gap = [dict(good, id=i, from_email=f"g{i}@d2.com", minTimeToWaitInMins=None) for i in (2, 3)]
    out, _ = run(FakeSL([good] + gap), plans=())
    assert out["problems"] == ["Changed since setup on 2 inboxes (d2.com): minimum gap empty, want 20 min"]
