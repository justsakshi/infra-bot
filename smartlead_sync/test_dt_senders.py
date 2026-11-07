"""Every current-client inbox sits on its client's deliverability test campaigns (paused only). No network."""
import asyncio

import dt_senders as ts


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

    async def no_tests(key):
        return [{"id": 9, "name": "PL spam test 1", "campaign_id": 101, "senders": set()}]
    monkeypatch.setattr(ts, "_scheduled_tests", no_tests)
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
