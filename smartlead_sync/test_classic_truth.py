"""Classic placement tests tell the truth (audit 7 Nov 2026: PLACEMENT-5/-6/-9/-13,
REACH-5/-14). Fakes only, no network.

    python -m pytest test_classic_truth.py -q
"""
import sys
from datetime import date

import placement_report as pr
from smartlead import classic_blocklist as cb
from smartlead.placement_report import (EXPECTED_TESTS, LABEL, build_report, find_scheduled, format_slack,
                                        missing_test_text, missing_tests)
from smartlead.report_sheet import merge_status_rows

TODAY = date(2026, 11, 10)
PROV = {"gs.example": "Google", "os.example": "Outlook"}


def _report(n=3, spam_at=()):
    senders = {f"m{i}@a{i}.example": [{"seed": "s@gs.example", "folder": "Spam" if i in spam_at else "Inbox"},
                                      {"seed": "t@os.example", "folder": "Inbox"}] for i in range(n)}
    return build_report(senders, {e: "GMAIL" for e in senders}, lambda d: PROV.get(d, "other"))


def _doc(dom, day="2026-11-09", listed=(), errors=(), inconclusive=()):
    return {"domain": dom, "date": day, "listed_on": list(listed), "lookup_errors": list(errors),
            "inconclusive_zones": list(inconclusive)}


# ── blocklist summary ────────────────────────────────────────────────────────

def test_newest_check_per_domain_wins():
    latest = cb.latest_by_domain([_doc("x.example", "2026-11-02", ["Spamhaus DBL (127.0.1.2)"]),
                                  _doc("x.example", "2026-11-09")])
    assert latest["x.example"]["date"] == "2026-11-09"
    s = cb.summarise({"x.example"}, latest, TODAY)
    assert s["listed"] == {} and s["checked_dates"] == ["2026-11-09"]


def test_only_dbl_and_uribl_count_and_unsure_checks_are_unknown():
    latest = cb.latest_by_domain([
        _doc("a.example", listed=["Spamhaus DBL (127.0.1.2)"]),
        _doc("b.example", listed=["URIBL (127.0.0.2)"]),
        _doc("c.example", listed=["SURBL (ABUSE)"]),                       # links, not the sender
        _doc("d.example", errors=["Spamhaus DBL"]),                        # could not read DBL
        _doc("e.example", inconclusive=["URIBL"]),
        _doc("f.example", "2026-10-01"),                                  # older than STALE_DAYS
    ])
    s = cb.summarise({"a.example", "b.example", "c.example", "d.example", "e.example", "f.example", "g.example"},
                     latest, TODAY)
    assert s["listed"] == {"a.example": ["Spamhaus DBL"], "b.example": ["URIBL"]}
    assert s["unknown"] == ["d.example", "e.example", "f.example", "g.example"]
    assert s["domains"] == 7 and s["read"]


def test_headline_lines():
    two = cb.summarise({"a.example", "b.example", "c.example"},
                       cb.latest_by_domain([_doc("a.example", "2026-11-02", ["Spamhaus DBL (x)"]),
                                            _doc("b.example", "2026-11-09", ["URIBL (x)"]),
                                            _doc("c.example")]), TODAY)
    assert cb.headline_line(two) == "2 of its 3 domains on Spamhaus DBL or URIBL (newest check 9 Nov)"
    one = cb.summarise({"a.example"}, cb.latest_by_domain([_doc("a.example", listed=["Spamhaus DBL (x)"])]), TODAY)
    assert cb.headline_line(one) == "Its only domain is on Spamhaus DBL or URIBL (check of 9 Nov)"
    assert cb.headline_line(cb.summarise({"a.example"}, None, TODAY)) == \
        "Blocklist status unknown: could not read the blocklist checks."
    assert cb.headline_line(cb.summarise({"a.example"}, {}, TODAY)) == \
        "Blocklist status unknown: no check of its domains in the last 21 days."
    assert cb.headline_line(cb.summarise({"c.example"}, cb.latest_by_domain([_doc("c.example")]), TODAY)) is None


def test_read_latest_is_defensive():
    class Broken:
        def find(self, *a, **k):
            raise RuntimeError("timed out")

    class Fake:
        def find(self, query, projection=None):
            assert query["domain"] == {"$in": ["a.example"]} and query["date"] == {"$gte": "2026-10-20"}
            return [_doc("a.example", "2026-11-02"), _doc("a.example", "2026-11-09", ["URIBL (x)"])]
    assert cb.read_latest({"a.example"}, TODAY, Broken()) is None
    assert cb.read_latest({"A.example"}, TODAY, Fake())["a.example"]["date"] == "2026-11-09"


# ── the classic post ─────────────────────────────────────────────────────────

def _listed(day="2026-11-09"):
    s = cb.summarise({"a0.example", "a1.example", "a2.example"},
                     cb.latest_by_domain([_doc("a0.example", day, ["Spamhaus DBL (127.0.1.2)"]),
                                          _doc("a1.example", day), _doc("a2.example", day)]), TODAY)
    s["notes"]["a0.example"] = "25 inboxes, 12 in live campaigns"
    return s


def test_no_all_clear_when_a_domain_is_listed():
    head, parts = format_slack("Melior", _report(), ["t"], "2026-11-10", blocklist=_listed())
    assert head == [f"⚠️ {LABEL}, Melior, 10 Nov: no mailbox flagged (3 tested)",
                    "1 of its 3 domains on Spamhaus DBL or URIBL (check of 9 Nov)"]
    assert "*On Spamhaus DBL or URIBL* (check of 9 Nov)\n• a0.example · Spamhaus DBL · 25 inboxes, 12 in live campaigns" \
        in parts
    assert "Blocklists: none" not in "\n".join(parts)


def test_clean_blocklist_is_a_footer_note_only():
    clean = cb.summarise({"a0.example"}, cb.latest_by_domain([_doc("a0.example")]), TODAY)
    head, parts = format_slack("Melior", _report(1), ["t"], "2026-11-10", blocklist=clean)
    assert head == [f"{LABEL}, Melior, 10 Nov: no mailbox flagged (1 tested)"]
    assert parts[-1].endswith("Blocklists: none of its 1 checked domain on Spamhaus DBL or URIBL (check of 9 Nov)._")


def test_unknown_blocklist_is_said():
    head, parts = format_slack("Melior", _report(1), ["t"], "2026-11-10",
                               blocklist=cb.summarise({"a0.example"}, None, TODAY))
    assert head[1] == "Blocklist status unknown: could not read the blocklist checks."
    assert "says nothing about Spamhaus DBL or URIBL" in parts[-1]


def test_senders_not_in_smartlead_are_named_and_not_counted():
    head, parts = format_slack("Precise Leads", _report(2, spam_at=(1,)), ["t"], "2026-11-10",
                               gone=["z@gone.example", "y@gone.example"])
    assert head[0] == f"⚠️ {LABEL}, Precise Leads, 10 Nov: 1 of 2 mailboxes flagged (1 domain in spam)"
    gone = next(p for p in parts if p.startswith("*Not in Smartlead now (2)*"))
    assert gone.endswith("• y@gone.example\n• z@gone.example") and "left out of the" in gone


def test_a_test_whose_senders_are_all_gone():
    head, _ = format_slack("Precise Leads", build_report({}, {}, lambda d: "other"), ["t"], "2026-11-10",
                           gone=[f"g{i}@gone.example" for i in range(18)])
    assert head == [f"⚠️ {LABEL}, Precise Leads, 10 Nov: none of the 18 test senders is in Smartlead now, "
                    "nothing to grade"]


# ── expected weekly tests ────────────────────────────────────────────────────

def test_expected_days_match_the_schedule():
    assert missing_tests("PRECISE_LEADS", "2026-11-09", set()) == [("Melior", "DT Melior #1 and #2")]   # Monday
    assert missing_tests("PRECISE_LEADS", "2026-11-10", set()) == [("Melior", "DT Melior #3"),
                                                                  ("Precise Leads", "DT PL #1, #2 and #3")]
    assert missing_tests("PRECISE_LEADS", "2026-11-10", {"Melior"}) == [("Precise Leads", "DT PL #1, #2 and #3")]
    assert missing_tests("BETTRDATA", "2026-11-10", set()) == [("Bettrdata", "DT BettrData #1 and #2")]
    assert missing_tests("BETTRDATA", "2026-11-09", set()) == []
    assert missing_tests("PRECISE_LEADS", "2026-11-11", set()) == []                                    # Wednesday
    assert set(EXPECTED_TESTS) == {"PRECISE_LEADS", "BETTRDATA"}


def test_missing_test_warning_text():
    head, thread = missing_test_text("Bettrdata", "DT BettrData #1 and #2", "2026-11-10")
    assert head == [f"⚠️ {LABEL}, Bettrdata, 10 Nov: no result today",
                    "Expected the scheduled test on DT BettrData #1 and #2."]
    assert thread[0].startswith("Nothing was measured, so this says nothing about the Bettrdata inboxes")


# ── scheduled tests beyond the newest 50 ─────────────────────────────────────

def _manual(i, day):
    return {"spam_test_id": i, "test_name": f"t{i}", "test_type": "manual", "status": "COMPLETED",
            "created_at": f"{day}T05:00:00.000Z"}


def _auto(i, created, start, run):
    return {"spam_test_id": i, "test_name": f"t{i}", "test_type": "auto", "status": "ACTIVE", "every_days": 7,
            "created_at": created, "schedule_start_time": start, "current_test_run_no": run}


def _days_back(n, start=7):
    """n distinct days, newest first, from 2026-11-<start> back."""
    from datetime import timedelta
    d0 = date(2026, 11, start)
    return [(d0 - timedelta(days=k)).isoformat() for k in range(n)]


def _full_listing():
    """60 newer manual tests, then the remembered weekly test, then 30 older ones."""
    newer = [_manual(700000 + k, d) for k, d in enumerate(_days_back(60))]
    weekly = _auto(900001, "2026-09-08T13:50:44.154Z", "2026-09-08T00:30:00.000Z", 10)
    older = [_manual(600000 + k, "2026-09-01") for k in range(30)]
    return newer + [weekly] + older


def test_a_remembered_weekly_test_is_found_past_the_window():
    full = _full_listing()
    calls = []

    def page(offset):
        calls.append(offset)
        return full[offset:offset + 100]
    found, lost = find_scheduled(full[:50], {900001: "2026-09-08T13:50:44.154Z"}, page)
    assert [t["spam_test_id"] for t in found] == [900001] and lost == [] and calls == [50]


def test_paging_stops_once_past_a_missing_tests_creation():
    full = [_manual(700000 + k, d) for k, d in enumerate(_days_back(400))]     # one test a day back from 7 Nov
    calls = []

    def page(offset):
        calls.append(offset)
        return full[offset:offset + 100]
    # created 20 Oct, inside the first 50 rows' span, yet not listed: deleted, so no paging at all
    assert find_scheduled(full[:50], {900009: "2026-10-20T00:00:00.000Z"}, page) == ([], [900009]) and calls == []
    # created 1 Aug: one page back (offset 50) reaches past 1 Aug without it, so stop there
    assert find_scheduled(full[:50], {900010: "2026-08-01T00:00:00.000Z"}, page) == ([], [900010]) and calls == [50]


def test_nothing_remembered_means_no_extra_calls():
    assert find_scheduled([_manual(1, "2026-11-01")], {}, lambda o: 1 / 0) == ([], [])


# ── Inbox Status tab: replaced by email ──────────────────────────────────────

HDR = ["Email", "Domain", "Client"]


def test_two_runs_keep_both_sets_of_rows():
    monday = [["a@m.example", "m.example", "Melior", "YES", "2026-11-09"],
              ["b@m.example", "m.example", "Melior", "YES", "2026-11-09"]]
    tuesday = [["c@m.example", "m.example", "Melior", "NO", "2026-11-10"]]
    body, kept = merge_status_rows([HDR] + monday, tuesday)
    assert body == monday + tuesday and kept == 2


def test_a_retested_inbox_replaces_only_its_own_row():
    old = [["a@m.example", "m.example", "Melior", "YES", "2026-11-09"],
           ["b@m.example", "m.example", "Melior", "YES", "2026-11-09"]]
    retest = [["A@m.example", "m.example", "Melior", "NO", "2026-11-10"]]
    body, _ = merge_status_rows([HDR] + old, retest)
    assert body == [old[1]] + retest


def test_inboxes_no_longer_in_the_account_leave_the_tab():
    old = [["a@m.example", "m.example", "Melior", "YES", "2026-11-09"],
           ["gone@p.example", "p.example", "PRECISE_LEADS", "YES", "2026-11-03"],      # old fallback label
           ["old@m.example", "m.example", "Melior", "YES", "2026-11-03"],              # this run's gone sender
           ["x@b.example", "b.example", "Bettrdata", "YES", "2026-11-03"]]             # another account
    body, kept = merge_status_rows([HDR] + old, [], account_name="PRECISE_LEADS",
                                   account_emails={"a@m.example"}, gone={"OLD@m.example"})
    assert body == [old[0], old[3]] and kept == 2
    # an empty or failed account read prunes nothing
    assert merge_status_rows([HDR] + old, [], account_name="PRECISE_LEADS", account_emails=set())[0] == old


def test_a_shared_client_label_never_prunes_the_other_accounts_rows():
    # Review CLASSIC-1: the PRECISE_LEADS account can label an inbox "Bettrdata" (client 456214),
    # and so can the BETTRDATA account. Neither account's run may delete the other's rows.
    from smartlead.report_sheet import client_label
    assert client_label("PRECISE_LEADS", 456214) == client_label("BETTRDATA", None) == "Bettrdata"
    tab = [["b1@bd.example", "bd.example", "Bettrdata", "YES", "2026-11-03"],          # BETTRDATA account
           ["b2@bd.example", "bd.example", "Bettrdata", "YES", "2026-11-03"],          # BETTRDATA account
           ["p1@pl.example", "pl.example", "Bettrdata", "YES", "2026-11-03"],          # PL account, client 456214
           ["m1@mel.example", "mel.example", "Melior", "YES", "2026-11-03"]]           # PL account
    pl_run = [["m1@mel.example", "mel.example", "Melior", "NO", "2026-11-10"]]
    body, kept = merge_status_rows([HDR] + tab, pl_run, account_name="PRECISE_LEADS",
                                   account_emails={"p1@pl.example", "m1@mel.example"}, gone=set())
    assert body == tab[:3] + pl_run and kept == 3
    bd_run = [["b1@bd.example", "bd.example", "Bettrdata", "NO", "2026-11-10"]]
    body, kept = merge_status_rows([HDR] + tab, bd_run, account_name="BETTRDATA",
                                   account_emails={"b1@bd.example", "b2@bd.example"}, gone=set())
    assert body == [tab[1], tab[2], tab[3]] + bd_run and kept == 3


# ── placement_report.py end to end (fakes) ───────────────────────────────────

class _Col:
    """Just enough of a Mongo collection: equality, $or, $set, $addToSet, upsert."""

    def __init__(self, docs=()):
        self.docs = [dict(d) for d in docs]

    @staticmethod
    def _match(d, q):
        for k, v in q.items():
            if k == "$or":
                if not any(_Col._match(d, x) for x in v):
                    return False
            elif d.get(k) != v:
                return False
        return True

    def find(self, q=None, projection=None):
        return [dict(d) for d in self.docs if self._match(d, q or {})]

    def update_one(self, flt, upd, upsert=False):
        doc = next((d for d in self.docs if self._match(d, flt)), None)
        if doc is None:
            doc = {k: v for k, v in flt.items() if not k.startswith("$")}
            self.docs.append(doc)
        doc.update(upd.get("$set", {}))
        for k, v in upd.get("$addToSet", {}).items():
            doc.setdefault(k, [])
            if v not in doc[k]:
                doc[k].append(v)


class _Acc:
    name, api_key = "PRECISE_LEADS", "k"


def _wire(monkeypatch, cols, posts, status_calls, today_day):
    weekly = _auto(900001, "2026-09-08T13:50:44.154Z", "2026-09-08T00:30:00.000Z", 10)   # run 10 = 10 Nov
    full = [_manual(700000 + k, d) for k, d in enumerate(_days_back(60))] + [weekly]
    monkeypatch.setattr(pr, "discover_accounts", lambda: [_Acc()])
    monkeypatch.setattr(pr, "_list_tests", lambda key, limit, offset=0: full[offset:offset + limit])
    monkeypatch.setattr(pr, "_collection", lambda name="placement_reports_posted": cols.get(name))
    seeds = [{"email": "s@gs.example", "reply": {"mail_folder": "Inbox"}},
             {"email": "t@os.example", "reply": {"mail_folder": "Inbox"}}]
    monkeypatch.setattr(pr, "_get", lambda url, key, **p: [
        {"email": "M1@mel.example", "details": seeds}, {"email": "m2@mel.example", "details": seeds},
        {"email": "old@gone.example", "details": seeds}])
    monkeypatch.setattr(pr, "accounts_by_email", lambda key: {
        "m1@mel.example": {"type": "GMAIL", "client_id": 12256},
        "m2@mel.example": {"type": "GMAIL", "client_id": 12256},
        "p1@pl.example": {"type": "OUTLOOK", "client_id": None}})
    monkeypatch.setattr(pr, "active_attachments", lambda key: {"m1@mel.example": ["Camp A"]})
    monkeypatch.setattr(pr, "mx_provider", lambda d: PROV.get(d, "other"))
    monkeypatch.setattr(pr, "grid_writes", lambda *a, **k: [])
    monkeypatch.setattr(pr, "write_status_tab", lambda report, cmap, tested_on, apply, **k:
                        status_calls.append((sorted(report["inboxes"]), k)) or {"rows": len(report["inboxes"])})
    monkeypatch.setattr(cb, "read_latest", lambda domains, today, collection=None: {
        "mel.example": {"domain": "mel.example", "date": today.isoformat(), "listed_on": ["Spamhaus DBL (127.0.1.2)"]}})
    import smartlead.notify as notify
    monkeypatch.setattr(notify, "post_with_thread", lambda token, ch, head, thread: posts.append((head, thread)) or "ts")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "x")
    monkeypatch.setattr(sys, "argv", ["placement_report.py", "--account", "PRECISE_LEADS", "--since", today_day,
                                      "--only-new", "--final", "--post", "--status-tab"])


def test_final_run_reports_the_weekly_test_and_warns_for_the_missing_client(monkeypatch):
    cols = {"placement_reports_posted": _Col(),
            "classic_scheduled_tests": _Col([{"_id": "PRECISE_LEADS|900001", "account": "PRECISE_LEADS",
                                              "test_id": 900001, "created_at": "2026-09-08T13:50:44.154Z",
                                              "status": "ACTIVE"}])}
    posts, status_calls = [], []
    _wire(monkeypatch, cols, posts, status_calls, "2026-11-10")
    assert pr.main() == 0

    (mel_head, mel_thread), (warn_head, _) = posts
    assert mel_head[0] == f"⚠️ {LABEL}, Melior, 10 Nov: no mailbox flagged (2 tested)"
    assert mel_head[1].startswith("Its only domain is on Spamhaus DBL or URIBL (check of ")
    assert any(p.startswith("*Not in Smartlead now (1)*") and "old@gone.example" in p for p in mel_thread)
    assert any("mel.example · Spamhaus DBL · 2 inboxes, 1 in live campaigns" in p for p in mel_thread)
    assert warn_head == [f"⚠️ {LABEL}, Precise Leads, 10 Nov: no result today",
                         "Expected the scheduled test on DT PL #1, #2 and #3."]

    # the gone sender is not in the tab, and its old row is dropped; nothing is pruned by label
    [(inboxes, kw)] = status_calls
    assert inboxes == ["m1@mel.example", "m2@mel.example"]
    assert kw["account_emails"] == {"m1@mel.example", "m2@mel.example", "p1@pl.example"}
    assert kw["account_name"] == "PRECISE_LEADS" and kw["gone"] == {"old@gone.example"}
    assert "account_clients" not in kw                  # never prune by a shared client label

    [rec] = cols["placement_reports_posted"].docs
    assert (rec["test_id"], rec["run_no"], rec["run_day"], rec["test_type"], rec["clients"]) == \
        (900001, 10, "2026-11-10", "auto", ["Melior"])
    assert cols["classic_scheduled_tests"].docs[0]["last_seen"]

    # A second final run: nothing new, Melior is covered from Mongo, PL is still missing.
    posts.clear()
    assert pr.main() == 0
    assert [h[0] for h, _ in posts] == [f"⚠️ {LABEL}, Precise Leads, 10 Nov: no result today"]


def test_hourly_run_never_warns(monkeypatch):
    cols = {"placement_reports_posted": _Col(), "classic_scheduled_tests": _Col()}
    posts, status_calls = [], []
    _wire(monkeypatch, cols, posts, status_calls, "2026-11-10")
    sys.argv.remove("--final")
    assert pr.main() == 0
    assert posts == [] and status_calls == []          # weekly test outside the window, nothing remembered yet


def test_scheduled_tests_are_remembered_only_on_a_writing_run(monkeypatch):
    weekly = _auto(900002, "2026-11-03T09:09:09.857Z", "2026-11-10T01:00:00.000Z", 1)
    monkeypatch.setattr(pr, "_list_tests", lambda key, limit, offset=0: [weekly, _manual(1, "2026-11-02")][offset:])
    col = _Col()
    picked = pr.pick_tests("k", "2026-11-10", [], "PRECISE_LEADS", col, remember=False)
    assert [t["spam_test_id"] for t in picked] == [900002] and col.docs == []
    pr.pick_tests("k", "2026-11-10", [], "PRECISE_LEADS", col, remember=True)
    assert [(d["_id"], d["status"]) for d in col.docs] == [("PRECISE_LEADS|900002", "ACTIVE")]


def test_an_empty_inbox_list_still_marks_the_test_reported(monkeypatch):
    cols = {"placement_reports_posted": _Col(),
            "classic_scheduled_tests": _Col([{"_id": "PRECISE_LEADS|900001", "account": "PRECISE_LEADS",
                                              "test_id": 900001, "created_at": "2026-09-08T13:50:44.154Z",
                                              "status": "ACTIVE"}])}
    posts, status_calls = [], []
    _wire(monkeypatch, cols, posts, status_calls, "2026-11-10")
    monkeypatch.setattr(pr, "accounts_by_email", lambda key: {})      # an API glitch, not a real empty account
    assert pr.main() == 0
    assert posts[0][0][0].startswith(f"{LABEL}, Precise Leads, 10 Nov: no mailbox flagged (3 tested)")
    assert [d["test_id"] for d in cols["placement_reports_posted"].docs] == [900001]   # not re-posted next hour
    assert status_calls[0][1]["account_emails"] == set()                                 # nothing pruned
    assert status_calls[0][1]["gone"] == set()
