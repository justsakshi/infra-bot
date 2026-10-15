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
    assert sl.attached == {101: {1000}} and r["dropped"] == 1


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
