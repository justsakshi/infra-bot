"""Every current-client inbox sits on its client's deliverability test campaigns (paused only). No network."""
import asyncio

import pytest

import dt_senders as ts


@pytest.fixture(autouse=True)
def _no_live_tracker(monkeypatch):
    """The coverage rule reads the asset tracker (Mongo) for cancelled ScaledMail orders: never the live one here."""
    import smartlead.zapmail_asset_sync as zas
    monkeypatch.setattr(zas, "read_tracker", lambda: {"live.com": {"provider": "Scaledmail", "status": "Active"}})


def camp(cid, name, client, role, attached=(), status="PAUSED"):
    return {"id": cid, "name": name, "client": client, "role": role, "status": status, "attached": set(attached)}


CAMPS = [camp(1, "PL Deliverability Test - all PL inboxes", "Precise Leads", "main", attached={10}),
         camp(2, "DT PL #1", "Precise Leads", "dt", attached={10}),
         camp(3, "Melior - Deliverability Test Campaign", "Melior", "main"),
         camp(4, "DT Melior #1", "Melior", "dt", attached={20, 21}),
         camp(5, "DT Melior #2", "Melior", "dt", attached={22})]


def box(i, client):
    return {"id": i, "email": f"x{i}@d.com", "client": client}


def test_each_inbox_joins_its_clients_main_and_the_first_dt_campaign_with_room():
    p = ts.plan([box(10, "Precise Leads"), box(11, "Precise Leads"), box(30, "Melior"), box(31, "Melior")], CAMPS)
    assert p["adds"] == {1: [11], 2: [11], 3: [30, 31], 4: [22, 30, 31]}
    assert p["removes"] == {5: [22]} and p["create"] == []        # fill DT #1 first: #2's one inbox moves up


def dts(client, prefix, counts, start_id=1000):
    out, i = [], start_id
    for n, cnt in enumerate(counts, 1):
        out.append(camp(100 + n, f"{prefix} #{n}", client, "dt", attached=set(range(i, i + cnt))))
        i += cnt
    return out


def boxes(ids, client):
    return [box(i, client) for i in ids]


def test_two_half_full_campaigns_become_50_and_the_rest():
    camps = dts("Bettrdata", "DT BettrData", [36, 36])
    p = ts.plan(boxes(range(1000, 1072), "Bettrdata"), camps)
    assert p["adds"] == {101: list(range(1058, 1072))} and p["removes"] == {102: list(range(1058, 1072))}   # #2 keeps its lowest 22
    assert p["create"] == []


def test_overfull_campaign_spills_into_new_campaigns_named_after_the_last():
    camps = dts("Melior", "DT Melior", [78, 77])
    p = ts.plan(boxes(range(1000, 1155), "Melior"), camps)
    assert [c["name"] for c in p["create"]] == ["DT Melior #3", "DT Melior #4"]
    assert all(c["clone_of"] == 101 for c in p["create"])
    assert len(p["adds"]["DT Melior #3"]) == 50 and len(p["adds"]["DT Melior #4"]) == 5
    assert len(p["removes"][101]) == 28 and len(p["removes"][102]) == 27
    assert 101 not in p["adds"] and 102 not in p["adds"]


def test_a_new_inbox_goes_to_the_first_campaign_with_room():
    p = ts.plan(boxes(range(1000, 1053), "Melior") + [box(5000, "Melior")], dts("Melior", "DT Melior", [50, 3]))
    assert p["adds"] == {102: [5000]} and p["removes"] == {}
    p = ts.plan(boxes(range(1000, 1049), "Melior") + [box(5000, "Melior")], dts("Melior", "DT Melior", [49]))
    assert p["adds"] == {101: [5000]}


def test_already_attached_inboxes_and_past_clients_are_left_alone():
    p = ts.plan([box(10, "Precise Leads"), box(40, "OSC")], CAMPS[:2])
    assert p["adds"] == {} and p["removes"] == {}


def test_an_active_campaign_is_never_touched():
    camps = [dict(CAMPS[0], status="ACTIVE"), CAMPS[1]]
    p = ts.plan([box(11, "Precise Leads")], camps)
    assert 1 not in p["adds"] and p["adds"] == {2: [11]}
    assert any("is ACTIVE" in x for x in p["problems"])


def test_a_client_without_a_test_campaign_is_reported():
    p = ts.plan([box(50, "Bettrdata")], CAMPS)
    assert p["adds"] == {} and any("Bettrdata" in x and "no test campaign" in x for x in p["problems"])


class FakeSL:
    def __init__(self, attached, drop=()):
        self.attached, self.drop, self.calls = attached, set(drop), []

    async def add_campaign_email_accounts(self, cid, ids):
        self.calls.append(("add", int(cid), list(ids)))
        self.attached.setdefault(int(cid), set()).update(i for i in ids if i not in self.drop)

    async def remove_campaign_email_accounts(self, cid, ids):
        self.calls.append(("remove", int(cid), list(ids)))
        self.attached[int(cid)] -= set(ids)

    async def get_campaign_email_accounts(self, cid):
        return [{"id": i} for i in self.attached.get(int(cid), set())]


def run_apply(sl, p, monkeypatch=None):
    return asyncio.run(ts.apply(sl, {"create": [], "removes": {}, "drops": {}, **p}, batch=50))


def test_apply_adds_in_batches_and_reads_back():
    sl = FakeSL({})
    r = run_apply(sl, {"adds": {1: list(range(120))}})
    assert [len(c[2]) for c in sl.calls] == [50, 50, 20]
    assert r["added"] == 120 and r["missing"] == []


def test_apply_reports_inboxes_missing_after_the_write():
    r = run_apply(FakeSL({}, drop={7}), {"adds": {1: [6, 7]}})
    assert r["added"] == 1 and r["missing"] == ["7 not on campaign 1 after the add"]


def test_a_move_removes_only_after_the_add_is_confirmed():
    sl = FakeSL({5: {22, 23}}, drop={23})
    r = run_apply(sl, {"adds": {4: [22, 23]}, "removes": {5: [22, 23]}})
    assert [c[0] for c in sl.calls] == ["add", "remove"]
    assert sl.calls[1] == ("remove", 5, [22])            # 23 never reached #4, so it stays on #5
    assert sl.attached == {5: {23}, 4: {22}} and r["removed"] == 1


def test_new_campaigns_are_created_first_and_named_targets_resolved(monkeypatch):
    made = []

    async def fake_clone(sl, src, name):
        made.append((src, name))
        return 900
    monkeypatch.setattr(ts, "_clone", fake_clone)
    sl = FakeSL({101: {1, 2}})
    r = run_apply(sl, {"create": [{"name": "DT Melior #3", "client": "Melior", "clone_of": 101}],
                       "adds": {"DT Melior #3": [2]}, "removes": {101: [2]}})
    assert made == [(101, "DT Melior #3")] and sl.attached == {101: {1}, 900: {2}}
    assert r["created"] == ["DT Melior #3"]


# Which scheduled tests need updating in Smart Delivery (the API cannot edit them) ----

DTC = [{"id": 1, "name": "DT PL #1", "emails": {"a@x.com", "b@x.com"}},
       {"id": 2, "name": "DT PL #2", "emails": {"c@y.com"}}]


def test_campaign_fully_covered_by_its_scheduled_test_needs_nothing():
    tests = [{"id": 9, "name": "PL 1", "campaign_id": 1, "senders": {"a@x.com", "b@x.com"}},
             {"id": 8, "name": "PL 2", "campaign_id": 2, "senders": {"c@y.com"}}]
    assert ts.test_gaps(DTC, tests) == []


def test_missing_inboxes_and_campaigns_without_a_test_are_listed():
    tests = [{"id": 9, "name": "PL 1", "campaign_id": 1, "senders": {"a@x.com"}}]
    gaps = ts.test_gaps(DTC, tests)
    assert gaps == [{"campaign": "DT PL #1", "test": "PL 1", "add": ["b@x.com"], "count": 2},
                    {"campaign": "DT PL #2", "test": None, "add": ["c@y.com"], "count": 1}]
    t = ts.gap_text(gaps)
    assert "DT PL #1: add 1 inbox to the scheduled test \"PL 1\": b@x.com" in t
    assert "DT PL #2: no scheduled test yet. Create a weekly test with all 1 inbox on it." in t


def test_inboxes_we_do_not_use_come_off_the_dt_campaigns():
    # Manveen 15 Oct: only inboxes in the deliverability sheet are tracked; the rest are client retainers.
    camps = dts("Melior", "DT Melior", [3])                       # 1000, 1001, 1002 attached
    p = ts.plan(boxes([1000, 1001], "Melior"), camps, drop_others=True)
    assert p["drops"] == {101: [1002]} and p["adds"] == {} and p["removes"] == {}
    assert ts.plan(boxes([1000, 1001], "Melior"), camps)["drops"] == {}     # sheet unreadable: never drop


def test_apply_drops_with_read_back():
    sl = FakeSL({101: {1000, 1002}})
    r = run_apply(sl, {"adds": {}, "drops": {101: [1002]}})
    assert sl.attached == {101: {1000}} and r["dropped"] == 1 and r["dropped_ids"] == {101: [1002]}


def test_dm_names_only_the_inboxes_added_today_and_new_campaigns():
    # Manveen 15 Oct: "why am I getting this again and again" - tell him about NEW inboxes only.
    gaps = [{"campaign": "DT PL #1", "test": "PL spam test 1", "add": ["old@x.com", "new@x.com"], "count": 50},
            {"campaign": "DT PL #3", "test": None, "add": ["n2@y.com"], "count": 25},
            {"campaign": "DT Melior #1", "test": "Melior spam test 1", "add": ["stale@m.com"], "count": 50}]
    t = ts.new_inbox_text(gaps, added={"new@x.com", "n2@y.com"}, created=["DT PL #3"])
    assert "DT PL #1: add 1 new inbox to the scheduled test \"PL spam test 1\": new@x.com" in t
    assert "DT PL #3 (new campaign): create a weekly test with all 25 inboxes on it." in t
    assert "old@x.com" not in t and "Melior" not in t
    assert ts.new_inbox_text(gaps, added=set(), created=[]) == ""



# Audit 7 Nov 2026: the DM is kept until Slack accepts it; drops and domains outside the sheet are told --------

from datetime import date

GAPS = [{"campaign": "DT PL #1", "test": "PL spam test 1", "add": ["new@x.com", "old@x.com"], "count": 50}]


def test_the_dm_is_kept_until_slack_accepts_it():
    sent, keep = ts.deliver_dm(GAPS, {"new@x.com"}, [], {}, complete=True, send=lambda t: False)
    assert sent is False and keep == {"added": ["new@x.com"], "created": []}
    texts = []
    sent, keep = ts.deliver_dm(GAPS, set(), [], keep, complete=True, send=lambda t: texts.append(t) or True)
    assert sent is True and "new@x.com" in texts[0] and "old@x.com" not in texts[0]
    assert keep == {"added": [], "created": []}


def test_a_kept_inbox_that_is_on_its_test_now_is_dropped_but_only_when_every_account_was_read():
    pending = {"added": ["done@x.com"], "created": []}
    assert ts.deliver_dm(GAPS, set(), [], pending, complete=True, send=lambda t: True) == (None, {"added": [], "created": []})
    assert ts.deliver_dm(GAPS, set(), [], pending, complete=False, send=lambda t: True) == (None, pending)


RESULTS = {"PRECISE_LEADS": {"dropped": ["a@x.com (DT PL #1)"],
                             "untracked": {"x.com": {"client": "Precise Leads", "inboxes": 3},
                                           "old.com": {"client": "Melior", "inboxes": 2}}},
           "BETTRDATA": {"error": "402 plan expired"}}


def test_infra_report_names_drops_new_domains_outside_the_sheet_and_errors():
    head, thread, now = ts.infra_report(RESULTS, known={"old.com"}, errors=["BETTRDATA: 402 plan expired"],
                                        dm_failed=True, today=date(2026, 11, 8))
    assert head[0] == "🧪 Test campaigns, 8 Nov: 1 inbox taken off, 1 domain not in the deliverability sheet"
    assert "Could not finish: BETTRDATA (details in the thread)" in head
    assert any("DM about new inboxes did not go out" in l for l in head)
    text = "\n".join(thread)
    assert "• a@x.com (DT PL #1)" in text and "• x.com (Precise Leads, 3 inboxes)" in text and "old.com" not in text
    assert "• BETTRDATA: 402 plan expired" in text
    assert now == {"x.com", "old.com"}


def test_infra_report_is_empty_when_nothing_changed():
    quiet = {"PRECISE_LEADS": {"untracked": {"old.com": {"client": "Melior", "inboxes": 2}}}}
    head, thread, now = ts.infra_report(quiet, known={"old.com"}, errors=[], dm_failed=False, today=date(2026, 11, 8))
    assert head == [] and thread == [] and now == {"old.com"}
    head, _, now = ts.infra_report({"PRECISE_LEADS": {}}, known=None, errors=[], dm_failed=False,
                                   today=date(2026, 11, 8))
    assert head == [] and now is None                            # sheet unread: nothing known about it



class FakeState:
    def __init__(self): self.docs = {}
    def find_one(self, q): return self.docs.get(q["_id"])
    def update_one(self, q, upd, upsert=False): self.docs[q["_id"]] = {"_id": q["_id"], **upd["$set"]}
    def delete_one(self, q): self.docs.pop(q["_id"], None)


class FakeClient:
    """One account: t.com is in the sheet, u.com is not and sits on DT PL #1."""
    attached = {}

    def __init__(self, key, name): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def list_email_accounts(self):
        return [{"id": 1, "from_email": "a@t.com", "client_id": None, "tags": []},
                {"id": 2, "from_email": "b@u.com", "client_id": None, "tags": []}]
    async def list_campaigns(self): return [{"id": 101, "name": "DT PL #1", "status": "PAUSED"}]
    async def get_campaign_email_accounts(self, cid): return [{"id": i} for i in sorted(self.attached.get(int(cid), set()))]
    async def add_campaign_email_accounts(self, cid, ids): self.attached.setdefault(int(cid), set()).update(ids)
    async def remove_campaign_email_accounts(self, cid, ids): self.attached[int(cid)] -= set(ids)


def test_the_daily_run_keeps_a_failed_dm_and_tells_the_infra_channel(monkeypatch, capsys):
    import sys
    from types import SimpleNamespace

    import smartlead.accounts
    import smartlead.api
    import smartlead.tracked
    FakeClient.attached = {101: {2}}
    state, dms, posts = FakeState(), [], []
    monkeypatch.setattr(smartlead.accounts, "discover_accounts",
                        lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(smartlead.api, "SmartleadClient", FakeClient)
    monkeypatch.setattr(smartlead.tracked, "tracked_domains", lambda: {"t.com"})

    monkeypatch.setattr(ts, "_weekly_tests", lambda key: [{"id": 9, "name": "PL spam test 1", "campaign_id": 101,
                                                          "senders": set(), "sender_ids": set()}])
    monkeypatch.setattr(ts, "_state_col", lambda: state)
    monkeypatch.setattr(ts, "_post_infra", lambda head, thread: posts.append((head, thread)) or True)
    monkeypatch.setattr(sys, "argv", ["dt_senders.py", "--apply"])

    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) and False)        # Slack refuses the DM
    assert ts.main() == 1
    assert state.docs["pending_dm"]["added"] == ["a@t.com"] and state.docs["untracked"]["domains"] == ["u.com"]
    head, thread = posts[0]
    assert head[0].endswith(": 1 inbox taken off, 1 domain not in the deliverability sheet")
    assert "• b@u.com (DT PL #1)" in "\n".join(thread) and "• u.com (Precise Leads, 1 inbox)" in "\n".join(thread)
    assert any("did not go out" in l for l in head)

    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) or True)          # next day: accepted
    assert ts.main() == 0
    assert "a@t.com" in dms[-1] and "pending_dm" not in state.docs
    assert len(posts) == 1                                                       # nothing new for the channel
    capsys.readouterr()


# --- the daily digest (16 Nov 2026) ---------------------------------------------------------

def _daily(monkeypatch, report_ok=True):
    """The daily --apply run of the test above, with the digest on and dg.report faked."""
    import sys
    from types import SimpleNamespace

    import smartlead.accounts
    import smartlead.api
    import smartlead.tracked
    from smartlead import digest as dg
    FakeClient.attached = {101: {2}}
    state, dms, posts, reports = FakeState(), [], [], []
    monkeypatch.setenv("DIGEST_MODE", "on")
    monkeypatch.setattr(smartlead.accounts, "discover_accounts",
                        lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(smartlead.api, "SmartleadClient", FakeClient)
    monkeypatch.setattr(smartlead.tracked, "tracked_domains", lambda: {"t.com"})

    monkeypatch.setattr(ts, "_weekly_tests", lambda key: [{"id": 9, "name": "PL spam test 1", "campaign_id": 101,
                                                          "senders": set(), "sender_ids": set()}])
    monkeypatch.setattr(ts, "_state_col", lambda: state)
    monkeypatch.setattr(ts, "_post_infra", lambda head, thread: posts.append((head, thread)) or True)
    monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None:
                        reports.append((src, asks, done, detail)) or report_ok)
    monkeypatch.setattr(sys, "argv", ["dt_senders.py", "--apply"])
    return state, dms, posts, reports


def test_with_the_digest_on_changes_are_done_lines_and_a_domain_outside_the_sheet_is_an_ask(monkeypatch, capsys):
    state, dms, posts, reports = _daily(monkeypatch)
    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) and False)        # Slack refuses the DM
    assert ts.main() == 1                                   # the refused DM still fails the run, as before
    assert posts == [] and len(dms) == 1                    # the DM itself is unchanged
    (src, asks, done, detail), = reports
    t = date.today().isoformat()
    assert src == "dt-senders"
    assert [(d.key, d.text) for d in done] == [
        (f"dt-senders:{t}:DT PL #1:added", "Put 1 inbox on DT PL #1 for the weekly placement test."),
        (f"dt-senders:{t}:DT PL #1:off", "Took 1 inbox off DT PL #1: their domain is not in the deliverability sheet.")]
    assert [(a.key, a.kind) for a in asks] == [("dt-senders:untracked", "ask"), ("dt-senders:dm", "broken")]
    assert asks[0].step == ("If we use it, add u.com to the deliverability sheet, tab Precise Leads, column A; the next "
                            "run puts its inboxes on the test campaigns.")
    assert "• b@u.com (DT PL #1)" in "\n".join(detail)
    assert state.docs["untracked"]["domains"] == ["u.com"]               # told once, as before
    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) or True)          # next day: nothing new
    assert ts.main() == 0
    assert reports[-1][1] == [] and reports[-1][2] == [] and posts == []
    capsys.readouterr()


def test_when_the_digest_cannot_be_written_the_infra_post_goes_out(monkeypatch, capsys):
    state, dms, posts, reports = _daily(monkeypatch, report_ok=False)
    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) or True)
    assert ts.main() == 0
    assert reports and len(posts) == 1 and posts[0][0][0].endswith(": 1 inbox taken off, 1 domain not in the "
                                                                   "deliverability sheet")
    assert state.docs["untracked"]["domains"] == ["u.com"]
    capsys.readouterr()


def test_digest_items_for_errors_moves_and_new_campaigns():
    res = {"PRECISE_LEADS": {"created": ["DT PL #3"], "added_to": {"DT PL #3": 2}, "moved_off": {"DT PL #2": 2},
                             "untracked": {"x.com": {"client": "Melior", "inboxes": 2},
                                           "y.com": {"client": "Precise Leads", "inboxes": 1}}},
           "BETTRDATA": {"error": "402 plan expired"}}
    asks, done = ts.digest_items(res, set(), ["BETTRDATA: 402 plan expired"], False, date(2026, 11, 16))
    assert [d.key for d in done] == ["dt-senders:2026-11-16:DT PL #3:created", "dt-senders:2026-11-16:DT PL #3:added",
                                     "dt-senders:2026-11-16:DT PL #2:moved"]
    assert asks[0].text.startswith("2 current-client domains are in Smartlead") and "tab Melior, Precise Leads" in asks[0].step
    assert asks[1].kind == "broken" and asks[1].text == "The test-campaign job could not finish: BETTRDATA."
    assert ts.digest_items(res, None, [], False, date(2026, 11, 16))[0] == []     # sheet unread: no domain ask


# --- pulled inboxes stay in a weekly test (Manveen, 16 Nov 2026: "go ahead") ----------------------------------
# An inbox the v2 job took out after a failed v2 test (tag "DO NOT USE - failed v2 (auto)") stays on a DT campaign,
# so the weekly test keeps checking it, until it is back in use (tag off) or cancelled (gone, or disconnected).

def test_a_pulled_inbox_outside_the_sheet_stays_on_its_dt_campaign():
    camps = dts("Melior", "DT Melior", [3])                       # 1000, 1001, 1002 attached; 1002 not in the sheet
    p = ts.plan(boxes([1000, 1001], "Melior"), camps, drop_others=True, retest=[box(1002, "Melior")])
    assert p["drops"] == {} and p["adds"] == {} and p["retest"] == {} and p["no_room"] == {}


def test_a_pulled_inbox_on_no_dt_campaign_goes_on_the_first_with_room_and_not_on_the_main_campaign():
    camps = [camp(1, "Melior - Deliverability Test Campaign", "Melior", "main", attached=range(1000, 1053))]
    camps += dts("Melior", "DT Melior", [50, 3])
    p = ts.plan(boxes(range(1000, 1053), "Melior"), camps, drop_others=True, retest=[box(5000, "Melior")])
    assert p["adds"] == {102: [5000]} and p["retest"] == {102: [5000]} and p["drops"] == {} and p["create"] == []


def test_pulled_inboxes_never_overfill_a_dt_campaign_and_the_rest_have_no_room():
    camps = dts("Melior", "DT Melior", [50, 49])
    p = ts.plan(boxes(range(1000, 1099), "Melior"), camps, drop_others=True,
                retest=[box(5001, "Melior"), box(5000, "Melior")])
    assert p["adds"] == {102: [5000]} and p["retest"] == {102: [5000]}
    assert p["no_room"] == {"Melior": [5001]} and p["create"] == []            # never a new campaign for them
    # one already on a DT campaign keeps its place before one that is on none
    camps = dts("Melior", "DT Melior", [50, 50])                               # 1099 (pulled, untracked) on #2
    p = ts.plan(boxes(range(1000, 1099), "Melior"), camps, drop_others=True,
                retest=[box(1099, "Melior"), box(900, "Melior")])
    assert p["adds"] == {} and p["drops"] == {} and p["no_room"] == {"Melior": [900]}


def test_with_the_sheet_unreadable_a_pulled_inbox_only_takes_free_room():
    camps = dts("Melior", "DT Melior", [49])
    p = ts.plan(boxes([1000], "Melior"), camps, retest=[box(5000, "Melior"), box(5001, "Melior")])
    assert p["adds"] == {101: [5000]} and p["no_room"] == {"Melior": [5001]} and p["create"] == []


def test_a_pulled_inbox_of_a_past_client_is_not_retested():
    p = ts.plan([], dts("Melior", "DT Melior", [3]), drop_others=True, retest=[box(5000, "OSC")])
    assert p["retest"] == {} and p["no_room"] == {}


class FakeRetest(FakeClient):
    """t.com is in the sheet. b@u.com (on DT PL #1) and c@v.com (on none) were pulled by the v2 job; d@w.com was
    pulled too but Smartlead can no longer log in to it."""
    async def list_email_accounts(self):
        tag = [{"tag_name": "DO NOT USE - failed v2 (auto)"}]
        ok = {"is_smtp_success": True, "is_imap_success": True}
        return [{"id": 1, "from_email": "a@t.com", "client_id": None, "tags": [], **ok},
                {"id": 2, "from_email": "b@u.com", "client_id": None, "tags": tag, **ok},
                {"id": 3, "from_email": "c@v.com", "client_id": None, "tags": tag, **ok},
                {"id": 4, "from_email": "d@w.com", "client_id": None, "tags": tag, "is_smtp_success": False,
                 "is_imap_success": True}]


def test_the_daily_run_keeps_pulled_inboxes_on_a_dt_campaign_with_a_done_line(monkeypatch, capsys):
    import smartlead.api
    state, dms, posts, reports = _daily(monkeypatch)
    monkeypatch.setattr(smartlead.api, "SmartleadClient", FakeRetest)
    FakeRetest.attached = {101: {2, 4}}
    monkeypatch.setattr(ts, "_dm", lambda text: dms.append(text) or True)
    assert ts.main() == 0
    assert FakeRetest.attached == {101: {1, 2, 3}}                 # d@w.com (disconnected) left the retest
    (src, asks, done, detail), = reports
    t = date.today().isoformat()
    assert [(d.key, d.text) for d in done] == [
        (f"dt-senders:{t}:DT PL #1:added", "Put 1 inbox on DT PL #1 for the weekly placement test."),
        (f"dt-senders:{t}:DT PL #1:retest", "Put 1 inbox taken out after a failed v2 test on DT PL #1, so the weekly "
                                            "test keeps checking it until it is back in use."),
        (f"dt-senders:{t}:DT PL #1:off", "Took 1 inbox off DT PL #1: their domain is not in the deliverability sheet.")]
    assert state.docs["untracked"]["domains"] == ["w.com"]        # a retested inbox is not "kept off the tests"
    capsys.readouterr()


def test_pulled_inboxes_with_no_room_are_an_ask():
    res = {"PRECISE_LEADS": {"no_room": {"Melior": 3}, "retest_to": {"DT PL #2": 2}}}
    asks, done = ts.digest_items(res, set(), [], False, date(2026, 11, 17))
    assert [(d.key, d.text) for d in done] == [
        ("dt-senders:2026-11-17:DT PL #2:retest", "Put 2 inboxes taken out after a failed v2 test on DT PL #2, so the "
                                                  "weekly test keeps checking them until they are back in use.")]
    (a,) = asks
    assert a.key == "dt-senders:no-room:Melior" and a.kind == "ask"
    assert a.text == ("3 Melior inboxes taken out after a failed v2 test are on no DT campaign: every DT campaign "
                      "of the client holds 50, so the weekly test does not check them.")
    assert a.step.startswith("Make room on a DT campaign")
    head, thread, _ = ts.infra_report(res, set(), [], False, date(2026, 11, 17))
    assert head[0].endswith(": 3 pulled inboxes with no room on a DT campaign")


# --- coverage rule (Manveen 16 Nov 2026: "make sure we have all inboxes in test except scaled mail canceld orders") --
# Every connected inbox we use sits in exactly one ACTIVE weekly v2 test. A v2 test keeps the senders it was created
# with, so an inbox that is a sender of an active test is never moved off that test's DT campaign; an inbox in no
# active test, or in two, is a digest ask.

def test_a_test_sender_stays_on_its_tests_campaign_even_when_an_earlier_campaign_has_room():
    camps = dts("Melior", "DT Melior", [30, 0, 25])               # #1 = 1000-1029, #3 = 1030-1054 (its own test)
    pinned = {i: 103 for i in range(1030, 1055)}
    p = ts.plan(boxes(range(1000, 1055), "Melior"), camps, pinned=pinned)
    assert p["removes"] == {} and p["adds"] == {}                  # fill-first alone would move 20 to #1


def test_unpinned_inboxes_still_fill_first_around_the_pinned_ones():
    camps = dts("Melior", "DT Melior", [30, 0, 25])
    pinned = {i: 103 for i in range(1030, 1040)}                   # only 10 of #3 are in its test
    p = ts.plan(boxes(range(1000, 1055), "Melior"), camps, pinned=pinned)
    moved = set(p["removes"].get(103, []))
    assert moved and not moved & set(pinned) and set(p["adds"][101]) == moved and len(moved) == 15


def test_a_test_sender_outside_the_sheet_is_not_taken_off_its_campaign():
    camps = dts("Melior", "DT Melior", [3])                        # 1002 is not in the sheet but in the test
    p = ts.plan(boxes([1000, 1001], "Melior"), camps, drop_others=True, pinned={1002: 101})
    assert p["drops"] == {}


def test_pins_come_from_active_tests_on_dt_campaigns_only():
    tests = [{"name": "t1", "campaign_id": 101, "sender_ids": {1, 2}},
             {"name": "t2", "campaign_id": 999, "sender_ids": {3}}]   # 999 is not a DT campaign
    assert ts.pins(tests, [camp(101, "DT PL #1", "Precise Leads", "dt")]) == {1: 101, 2: 101}


def test_coverage_lists_inboxes_in_no_test_and_in_two_per_client():
    members = {"a@x.com": "Melior", "b@x.com": "Melior", "c@y.com": "Precise Leads", "d@y.com": "Precise Leads"}
    tests = [{"name": "M1", "senders": {"a@x.com"}}, {"name": "P1", "senders": {"c@y.com", "d@y.com"}},
             {"name": "P2", "senders": {"d@y.com", "z@gone.com"}}]
    cov = ts.coverage(members, tests)
    assert cov == {"Melior": {"inboxes": 2, "none": ["b@x.com"], "dupes": {}},
                   "Precise Leads": {"inboxes": 2, "none": [], "dupes": {"d@y.com": ["P1", "P2"]}}}


def test_coverage_gaps_and_duplicates_are_digest_asks_and_full_coverage_is_quiet():
    from datetime import date
    res = {"PRECISE_LEADS": {"coverage": {
        "Melior": {"inboxes": 50, "none": ["b@x.com"], "dupes": {}, "on": {"b@x.com": "DT Melior #3"}},
        "Precise Leads": {"inboxes": 9, "none": [], "dupes": {"d@y.com": ["P1", "P2"]}},
        "Bettrdata": {"inboxes": 5, "none": [], "dupes": {}}}}}
    asks, _ = ts.digest_items(res, set(), [], False, date(2026, 11, 17))
    keys = {a.key: a for a in asks}
    gap = keys["dt-senders:coverage:Melior"]
    assert gap.text.startswith("1 Melior inbox is in no active weekly v2 test")
    assert "DT Melior #3" in gap.step and "05:30 IST" in gap.step and "50" in gap.step
    assert "2 or more" in keys["dt-senders:coverage-dupes:Precise Leads"].text
    assert not any("Bettrdata" in k for k in keys)


def test_coverage_is_not_judged_when_the_tests_could_not_be_read():
    from datetime import date
    res = {"BETTRDATA": {"coverage": None, "coverage_error": "tests (HTTP 500)"}}
    asks, _ = ts.digest_items(res, set(), [], False, date(2026, 11, 17))
    assert [a.key for a in asks] == ["dt-senders:coverage-unread"] and asks[0].kind == "broken"


def test_when_the_v2_tests_cannot_be_read_nobody_on_a_dt_campaign_moves(monkeypatch, capsys):
    import json
    import sys
    from types import SimpleNamespace

    import smartlead.accounts
    import smartlead.api
    import smartlead.tracked
    from smartlead import v2_api
    FakeClient.attached = {101: {2}}                     # b@u.com is outside the sheet: normally taken off
    monkeypatch.setattr(smartlead.accounts, "discover_accounts",
                        lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(smartlead.api, "SmartleadClient", FakeClient)
    monkeypatch.setattr(smartlead.tracked, "tracked_domains", lambda: {"t.com"})

    def down(key):
        raise v2_api.V2ApiError("tests", 500)
    monkeypatch.setattr(ts, "_weekly_tests", down)
    monkeypatch.setattr(sys, "argv", ["dt_senders.py"])
    assert ts.main() == 0
    out = json.loads(capsys.readouterr().out)["PRECISE_LEADS"]
    assert out["would_drop"] == {} and out["would_remove"] == {}
    assert out["coverage"] is None and out["coverage_error"] == "tests (HTTP 500)"


def test_the_copy_check_test_reuses_inboxes_on_purpose_so_it_is_no_duplicate():
    members = {"a@x.com": "Melior"}
    tests = [{"name": "Melior weekly 2", "senders": {"a@x.com"}}, {"name": "Melior weekly synced copy", "senders": {"a@x.com"}}]
    assert ts.coverage(members, tests)["Melior"]["dupes"] == {}


def test_an_inbox_whose_only_test_is_the_copy_check_is_in_no_test():
    members = {"a@x.com": "Melior", "b@x.com": "Melior"}
    tests = [{"name": "Melior weekly 2", "senders": {"a@x.com"}},
             {"name": "Melior weekly synced copy", "senders": {"a@x.com", "b@x.com"}}]
    assert ts.coverage(members, tests) == {"Melior": {"inboxes": 2, "none": ["b@x.com"], "dupes": {}}}


def test_two_active_tests_with_one_name_are_an_ask():
    from datetime import date
    tests = [{"id": "1", "name": "B copy"}, {"id": "2", "name": "B copy"}, {"id": "3", "name": "B 2"}]
    assert ts.twins(tests) == {"B copy": 2}
    asks, _ = ts.digest_items({"BETTRDATA": {"coverage": {}, "twins": {"B copy": 2}}}, set(), [], False,
                              date(2026, 11, 17))
    assert [a.key for a in asks] == ["dt-senders:coverage-twins:BETTRDATA:B copy"]
    assert "stop the older one" in asks[0].step


def test_cancelled_scaledmail_orders_come_from_the_asset_tracker():
    tracker = {"gone.com": {"provider": "Scaledmail", "status": "Inactive", "type": "DOMAIN"},
               "live.com": {"provider": "Scaled Mail", "status": "Active", "type": "DOMAIN"},
               "zap.com": {"provider": "Zapmail", "status": "Inactive", "type": "DOMAIN"},
               "a@gone.com": {"provider": "Scaledmail", "status": "Inactive", "type": "INBOX"}}
    assert ts.cancelled_domains(tracker) == {"gone.com"}
