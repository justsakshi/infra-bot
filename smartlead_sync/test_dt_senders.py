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


def test_each_inbox_joins_its_clients_main_and_the_emptier_dt_campaign():
    p = ts.plan([box(10, "Precise Leads"), box(11, "Precise Leads"), box(30, "Melior"), box(31, "Melior")], CAMPS)
    assert p["adds"] == {1: [11], 2: [11], 3: [30, 31], 5: [30], 4: [31]}     # emptier DT first, ties to the lower id
    assert not p["problems"]


def test_dt_campaigns_stay_balanced():
    p = ts.plan([box(i, "Melior") for i in (30, 31, 32)], CAMPS)
    on4, on5 = len(p["adds"].get(4, [])) + 2, len(p["adds"].get(5, [])) + 1
    assert abs(on4 - on5) <= 1 and on4 + on5 == 6


def test_already_attached_inboxes_and_past_clients_are_left_alone():
    p = ts.plan([box(10, "Precise Leads"), box(40, "OSC")], CAMPS)
    assert p["adds"] == {}


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
        self.calls.append((int(cid), list(ids)))
        self.attached.setdefault(int(cid), set()).update(i for i in ids if i not in self.drop)

    async def get_campaign_email_accounts(self, cid):
        return [{"id": i} for i in self.attached.get(int(cid), set())]


def test_apply_adds_in_batches_and_reads_back():
    sl = FakeSL({})
    r = asyncio.run(ts.apply(sl, {1: list(range(120))}, batch=50))
    assert [len(ids) for _, ids in sl.calls] == [50, 50, 20]
    assert r == {"added": 120, "missing": []}


def test_apply_reports_inboxes_missing_after_the_write():
    sl = FakeSL({}, drop={7})
    r = asyncio.run(ts.apply(sl, {1: [6, 7]}, batch=50))
    assert r == {"added": 1, "missing": ["7 not on campaign 1 after the add"]}
