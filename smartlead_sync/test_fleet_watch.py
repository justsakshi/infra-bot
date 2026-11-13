"""fleet_watch.py: building each day's record and keeping it. No network."""
from datetime import date

import fleet_watch as fw

TODAY = date(2026, 10, 20)


def acct(**over):
    a = {"id": 1, "from_email": "A@d.com", "type": "GMAIL", "smtp_host": None, "message_per_day": 15, "client_id": None,
         "is_smtp_success": True, "is_imap_success": True,
         "tags": [{"tag_name": "Bot-Setup"}, {"tag_name": "ScaledMail-Google"}],
         "warmup_details": {"status": "ACTIVE", "warmup_reputation": "98%", "warmup_created_at": "2026-10-13T10:00:00Z"}}
    a.update(over)
    return a


FACTS = {"a@d.com": {"client": "Precise Leads", "connected": True, "campaigns": ["PL 1"], "sending": ["PL 1"],
                     "tested_on": "2026-10-13", "pct": 95.0}}


def test_record_holds_the_inbox_state_for_the_day():
    r = fw.record("PRECISE_LEADS", acct(), FACTS, verdicts={"a@d.com": "GOOD"}, per_domain={"d.com": 3},
                  approved={"a@d.com": (15, "2026-10-14")}, health={"a@d.com": (120, 1)}, today=TODAY,
                  live={"a@d.com": ["PL 1", "PL 2", "PL 1"]}, warm={"a@d.com": (90, 12)})
    assert r == {"email": "a@d.com", "domain": "d.com", "date": "2026-10-20", "account": "PRECISE_LEADS",
                 "client": "Precise Leads", "kind": "google", "vendor": "ScaledMail", "setup": True, "limit": 15,
                 "approved_limit": 15, "warmup_on": True, "rep": 98, "connected": True, "campaigns": 1, "sending": True,
                 "ready_on": "2026-10-27", "warmup_started": "2026-10-13", "test_date": "2026-10-13", "test_pct": 95.0,
                 "verdict": "GOOD", "sent_14d": 120, "bounced_14d": 1, "warm_sent_7d": 90, "warm_spam_7d": 12,
                 "tags": ["Bot-Setup", "ScaledMail-Google"], "live_campaigns": ["PL 1", "PL 2"]}
    unread = fw.record("PRECISE_LEADS", acct(), FACTS, verdicts={}, per_domain={"d.com": 3}, approved={}, health={}, today=TODAY)
    assert (unread["warm_sent_7d"], unread["warm_spam_7d"]) == (None, None)       # warm-up stats not read: no numbers


def test_warm_counts_reads_the_seven_day_numbers_or_says_nothing():
    assert fw.warm_counts({"id": 1, "sent_count": 88, "spam_count": 43, "stats_by_date": []}) == (88, 43)
    assert fw.warm_counts([{"sent_count": "30", "spam_count": None}]) == (30, 0)
    assert fw.warm_counts(None) is None and fw.warm_counts([]) is None and fw.warm_counts({"id": 1}) is None
    assert fw.warm_counts({"sent_count": "n/a"}) is None


def test_old_inboxes_get_no_ready_date_and_missing_facts_are_empty():
    r = fw.record("BETTRDATA", acct(tags=[], is_imap_success=False, warmup_details={"status": "INACTIVE"}), {}, {},
                  {"d.com": 1}, {}, {}, TODAY)
    assert r["ready_on"] is None and r["setup"] is False and r["warmup_on"] is False and r["connected"] is False
    assert r["client"] == "Bettrdata" and r["campaigns"] == 0 and r["test_date"] is None and r["vendor"] is None


class FakeCol:
    def __init__(self): self.docs = []
    def _match(self, d, q):
        for k, v in q.items():
            if isinstance(v, dict) and ("$gte" in v or "$lt" in v):
                if not d.get(k) or ("$gte" in v and d[k] < v["$gte"]) or ("$lt" in v and d[k] >= v["$lt"]):
                    return False
            elif isinstance(v, dict) and "$in" in v:
                if d.get(k) not in v["$in"]:
                    return False
            elif d.get(k) != v:
                return False
        return True
    def update_one(self, q, upd, upsert=False):
        for d in self.docs:
            if self._match(d, q):
                d.update(upd["$set"]); return
        self.docs.append({**q, **upd["$set"]})
    def find(self, q, proj=None):
        return [dict(d) for d in self.docs if self._match(d, q)]


def test_store_upserts_by_inbox_and_day_and_returns_history_oldest_first():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_state([{"email": "a@d.com", "date": "2026-10-19", "rep": 90}])
    s.save_metrics({"a@d.com": {"2026-10-13": {"sent": 15, "bounced": 0}}})
    s.save_metrics({"a@d.com": {"2026-10-13": {"sent": 14, "bounced": 1}}})           # a later read overwrites
    s.save_state([{"email": "a@d.com", "date": "2026-10-20", "rep": 80}])
    h = s.history(["a@d.com"], since="2026-10-14")
    assert [d["rep"] for d in h["a@d.com"]] == [90, 80]
    assert [d for d in s.daily.docs if d["date"] == "2026-10-13"][0]["sent"] == 14


def test_store_keeps_cases_by_key():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_cases([{"key": "warmup_off:a@d.com", "kind": "warmup_off", "closed_on": None}])
    assert list(s.open_cases()) == ["warmup_off:a@d.com"]
    s.save_cases([{"key": "warmup_off:a@d.com", "kind": "warmup_off", "closed_on": "2026-10-21"}])
    assert s.open_cases() == {} and len(s.all_cases()) == 1



# Audit 7 Nov 2026: nothing closes on missing data, nothing is lost when Slack refuses -------------

def test_blocklist_read_keeps_the_newest_row_per_domain_however_old():
    col = FakeCol()
    col.docs = [{"domain": "d.com", "date": "2026-09-01", "listed_on": ["SURBL (ABUSE)"]},
                {"domain": "d.com", "date": "2026-10-26", "listed_on": ["Spamhaus DBL (127.0.1.2)"]},
                {"domain": "e.com", "date": "2026-10-01", "listed_on": []}]
    got = fw._blocklisted(col)
    assert got["d.com"]["listed_on"] == ["Spamhaus DBL (127.0.1.2)"] and got["e.com"]["date"] == "2026-10-01"

    class Broken:
        def find(self, *a, **k): raise RuntimeError("no Mongo")
    assert fw._blocklisted(Broken()) is None                    # unknown, never "clean"


def test_an_account_with_a_read_error_is_not_counted_as_read():
    errors = ["Smartlead BETTRDATA: 402 plan expired", "deliverability sheet (judging every inbox today)"]
    assert fw._read_ok(["PRECISE_LEADS", "BETTRDATA"], errors) == {"PRECISE_LEADS"}


def rec(email="a@d.com", **over):
    r = {"email": email, "domain": email.split("@")[1], "date": "2026-10-20", "account": "PRECISE_LEADS",
         "client": "Precise Leads", "kind": "google", "limit": 15, "approved_limit": None, "setup": True,
         "warmup_on": True, "rep": 100, "connected": True, "campaigns": 1, "sending": False, "ready_on": "2026-10-01",
         "warmup_started": "2026-09-17", "test_date": "2026-10-13", "verdict": "GOOD", "sent_14d": 0, "bounced_14d": 0}
    r.update(over)
    return r


class Poster:
    def __init__(self, *results):
        self.results, self.calls = list(results), []

    def __call__(self, head, thread=()):
        self.calls.append((list(head), list(thread)))
        return self.results.pop(0) if self.results else True


def day(store, records, today, poster, read=("PRECISE_LEADS",), blocklist=None, errors=(), roster=None):
    return fw.daily(store, records, {}, set(read), blocklist if blocklist is not None else {}, today, list(errors),
                    post=True, poster=poster, roster=roster)


def test_a_refused_post_leaves_cases_unannounced_and_the_next_run_sends_them_again():
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster(False, True)
    out = day(s, [rec(warmup_on=False, date="2026-10-20")], date(2026, 10, 20), p)
    assert out["posted"] is False and out["failed"] is True
    assert [c["announced"] for c in s.cases.docs] == [False]
    out = day(s, [rec(warmup_on=False, date="2026-10-21")], date(2026, 10, 21), p)
    head, thread = p.calls[1]
    assert head[0].startswith("⚠️ New inbox problem, 21 Oct: 1 ")
    assert "1 of them was found on 20 Oct, but that post did not go out." in head
    assert out["posted"] is True and [c["announced"] for c in s.cases.docs] == [True]
    day(s, [rec(warmup_on=False, date="2026-10-22")], date(2026, 10, 22), p)
    assert len(p.calls) == 2                                      # announced: not repeated


def test_cases_from_before_this_change_are_posted_once_as_a_backlog():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_cases([{"key": "warmup_off:b@d.com", "kind": "warmup_off", "scope": "inbox", "target": "b@d.com",
                   "domain": "d.com", "client": "Precise Leads", "action": "Turn warmup back on",
                   "line": "b@d.com", "opened_on": "2026-11-03", "acted_on": None, "closed_on": None,
                   "at_open": {}, "baseline": False},
                  {"key": "blocklist:old.com", "kind": "blocklist", "scope": "domain", "target": "old.com",
                   "domain": "old.com", "client": "Bettrdata", "action": "x", "line": "old.com (SURBL (ABUSE))",
                   "opened_on": "2026-10-15", "acted_on": None, "closed_on": None, "at_open": {}, "baseline": True}])
    p = Poster()
    out = day(s, [rec(), rec("b@d.com", warmup_on=False)], date(2026, 11, 8), p)
    assert out["backlog"] == 1 and len(p.calls) == 1               # nothing new today: only the backlog post
    head, thread = p.calls[0]
    assert head == ["📋 Fleet watch: 1 problem found since 3 Nov is still open (list in the thread, shown once; "
                    "suggestions only, nothing was changed)"]
    assert "• b@d.com, open since 3 Nov" in thread[0] and "old.com" not in "\n".join(thread)    # baseline stays quiet
    day(s, [rec(), rec("b@d.com", warmup_on=False)], date(2026, 11, 9), p)
    assert len(p.calls) == 1


def test_a_day_without_the_account_closes_nothing():
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster()
    day(s, [rec(warmup_on=False)], date(2026, 10, 20), p)
    out = day(s, [], date(2026, 10, 21), p, read=(), errors=["Smartlead PRECISE_LEADS: 402"])
    assert out["closed"] == 0 and s.cases.docs[0]["closed_on"] is None and s.cases.docs[0]["unknown"] is True
    # nothing new to say: no post for the read error alone; the run fails, so the bot-health post lists it
    assert len(p.calls) == 1 and out["failed"] is True and out["errors"] == ["Smartlead PRECISE_LEADS: 402"]


def test_the_store_marks_announced_cases_by_key_and_open_day():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_cases([{"key": "k", "opened_on": "2026-10-20", "announced": False, "closed_on": None}])
    s.mark_announced([{"key": "k", "opened_on": "2026-10-20"}], date(2026, 10, 21))
    assert s.cases.docs[0]["announced"] is True and s.cases.docs[0]["announced_on"] == "2026-10-21"



def test_gather_puts_live_campaigns_and_tags_on_the_record_and_leaves_a_failed_account_out(monkeypatch):
    from types import SimpleNamespace

    import renewal_review
    import smartlead.inbox_intake as ii
    import smartlead.tracked
    import volume_review
    accounts = {"PRECISE_LEADS": SimpleNamespace(name="PRECISE_LEADS", api_key="k1"),
                "BETTRDATA": SimpleNamespace(name="BETTRDATA", api_key="k2")}
    monkeypatch.setattr(renewal_review, "smartlead_facts", lambda errors: {})
    monkeypatch.setattr(volume_review, "_clients", lambda: accounts)

    async def listing(acc):
        return [acct(tags=[{"tag_name": "DO NOT USE - failed v2 (auto)"}])]

    async def live(acc):
        if acc.name == "BETTRDATA":
            raise RuntimeError("402 plan expired")
        return {"a@d.com": ["PL | Outbound"]}
    monkeypatch.setattr(volume_review, "_list", listing)
    monkeypatch.setattr(volume_review, "_health", lambda key, start, end: {})
    monkeypatch.setattr(fw, "_live_campaigns", live)
    monkeypatch.setattr(fw, "_day_metrics", lambda key, today: {})
    monkeypatch.setattr(smartlead.tracked, "tracked_domains", lambda: {"d.com"})
    monkeypatch.setattr(ii.IntakeStore, "__init__", lambda self, collection=None: None)
    monkeypatch.setattr(ii.IntakeStore, "approved_limits", lambda self: {})
    errors = []
    records, _, read, _ = fw.gather(TODAY, errors)
    assert read == {"PRECISE_LEADS"} and any(e.startswith("Smartlead BETTRDATA:") for e in errors)
    assert [(r["account"], r["live_campaigns"], r["tags"]) for r in records] == \
        [("PRECISE_LEADS", ["PL | Outbound"], ["DO NOT USE - failed v2 (auto)"])]


def test_gather_lists_what_smartlead_holds_and_what_was_left_out_on_purpose(monkeypatch):
    from types import SimpleNamespace

    import renewal_review
    import smartlead.inbox_intake as ii
    import smartlead.tracked
    import volume_review
    monkeypatch.setattr(renewal_review, "smartlead_facts", lambda errors: {"f@d.com": {"client": "Precise Leads"}})
    monkeypatch.setattr(volume_review, "_clients",
                        lambda: {"PRECISE_LEADS": SimpleNamespace(name="PRECISE_LEADS", api_key="k1")})

    async def listing(acc):
        return [acct(), acct(from_email="u@other.com")]          # other.com is not in the sheet

    async def live(acc):
        return {}
    monkeypatch.setattr(volume_review, "_list", listing)
    monkeypatch.setattr(volume_review, "_health", lambda key, start, end: {})
    monkeypatch.setattr(fw, "_live_campaigns", live)
    monkeypatch.setattr(fw, "_day_metrics", lambda key, today: {})
    monkeypatch.setattr(smartlead.tracked, "tracked_domains", lambda: {"d.com"})
    monkeypatch.setattr(ii.IntakeStore, "__init__", lambda self, collection=None: None)
    monkeypatch.setattr(ii.IntakeStore, "approved_limits", lambda self: {})
    records, _, read, roster = fw.gather(TODAY, [])
    assert [r["email"] for r in records] == ["a@d.com"] and read == {"PRECISE_LEADS"}
    assert roster == {"tracked": {"d.com"}, "present": {"a@d.com", "u@other.com", "f@d.com"},
                      "dropped": {"u@other.com"}}


def test_an_inbox_missing_from_one_days_list_stays_open_and_is_not_announced_twice():
    """Audit 7 Nov (F1): the inbox list can stop early without an error. An inbox still in Smartlead
    (the facts read has it) and still in the sheet is not 'gone': its case stays open, and when the inbox
    is back the next day nothing is posted again as new."""
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster()
    fleet = [rec(f"i{n}@d.com", date="2026-10-20") for n in range(9)]
    day(s, fleet + [rec(warmup_on=False, date="2026-10-20")], date(2026, 10, 20), p)
    assert len(p.calls) == 1
    roster = {"tracked": {"d.com"}, "present": {"a@d.com"} | {r["email"] for r in fleet}, "dropped": set()}
    out = day(s, [dict(r, date="2026-10-21") for r in fleet], date(2026, 10, 21), p, roster=roster)
    assert out["closed"] == 0 and s.cases.docs[0]["closed_on"] is None and s.cases.docs[0]["unknown"] is True
    out = day(s, [dict(r, date="2026-10-22") for r in fleet] + [rec(warmup_on=False, date="2026-10-22")],
              date(2026, 10, 22), p, roster=roster)
    assert out["opened"] == 0 and len(p.calls) == 1 and len(s.cases.docs) == 1      # no second "new problem"


def test_an_account_whose_inbox_count_drops_closes_nothing_that_day():
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster()
    fleet = [rec(f"i{n}@d.com", date="2026-10-20") for n in range(9)]
    day(s, fleet + [rec(warmup_on=False, date="2026-10-20")], date(2026, 10, 20), p)
    left = {"tracked": {"d.com"}, "present": set(), "dropped": set()}       # the roster even says it left
    half = [dict(r, date="2026-10-21") for r in fleet[:5]]
    out = day(s, half, date(2026, 10, 21), p, roster=left)
    assert out["closed"] == 0 and s.cases.docs[0]["closed_on"] is None
    assert len(p.calls) == 1 and out["failed"] is True
    assert out["errors"] == ["Smartlead PRECISE_LEADS in full: 5 inboxes today, 10 at the last run, "
                             "so none of its cases close today"]
    out = day(s, [dict(r, date="2026-10-22") for r in fleet[:5]], date(2026, 10, 22), p, roster=left)
    assert out["closed"] == 1 and s.cases.docs[0]["closed_why"] == "gone"       # same count two runs running


def test_the_store_counts_inboxes_per_account_on_the_last_day_recorded():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_state([rec("a@d.com", date="2026-10-18"), rec("b@d.com", date="2026-10-18"),
                  rec("c@d.com", date="2026-10-19"), rec("x@x.com", date="2026-10-19", account="BETTRDATA"),
                  rec("t@d.com", date="2026-10-20")])
    s.save_metrics({"z@d.com": {"2026-10-19": {"sent": 3}}})                  # metric-only: not an inbox record
    assert s.last_counts("2026-10-20", "2026-10-12") == {"PRECISE_LEADS": 1, "BETTRDATA": 1}
    assert s.last_counts("2026-10-18", "2026-10-12") == {}


def test_kinds_nobody_acts_on_daily_are_recorded_but_not_posted():
    """13 Nov 2026: the channel was "spammy, not understandable". A disconnected inbox is already in the daily inbox
    post; it is still a case (Monday's scorecard counts it) but the daily fleet post stays quiet about it."""
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster()
    out = day(s, [rec(connected=False)], date(2026, 11, 13), p)
    assert out["opened"] == 1 and out["quiet"] == 1 and p.calls == []
    assert all(c["announced"] is True for c in s.cases.docs)
    day(s, [rec(connected=False)], date(2026, 11, 14), p)
    assert p.calls == []                                           # not resent, not a backlog either


def test_a_post_that_goes_out_says_what_could_not_be_read():
    s = fw.FleetStore(FakeCol(), FakeCol())
    p = Poster()
    out = day(s, [rec(warmup_on=False)], date(2026, 11, 13), p, errors=["blocklist history (Mongo)"])
    assert out["failed"] is True and len(p.calls) == 1
    assert "Could not read today, so some problems may be missing: blocklist history (Mongo)" in p.calls[0][0]
