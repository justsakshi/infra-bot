"""Offline test for HeyReachClient paging + stat shape (fake HTTP)."""
from __future__ import annotations
import asyncio
from smartlead.heyreach import HeyReachClient


class FakeResp:
    def __init__(self, payload, status=200):
        self._p, self.status_code, self.headers = payload, status, {}
    def json(self): return self._p
    def raise_for_status(self): pass


class FakeHTTP:
    """Stands in for httpx.AsyncClient; routes by URL suffix."""
    def __init__(self):
        self.calls = []
    async def post(self, url, headers=None, json=None):
        self.calls.append((url, json))
        if url.endswith("/campaign/GetAll"):
            off = json["offset"]
            return FakeResp({"totalCount": 2, "items": [
                {"id": 1, "name": "C1", "status": "IN_PROGRESS",
                 "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}},
                {"id": 2, "name": "C2", "status": "PAUSED",
                 "progressStats": {"totalUsers": 10, "totalUsersInProgress": 3}},
            ] if off == 0 else []})
        if url.endswith("/stats/GetOverallStats"):
            return FakeResp({"overallStats": {"connectionsSent": 25, "connectionsAccepted": 4,
                     "messagesSent": 12, "totalMessageReplies": 3, "autoTaggedInterested": 1},
                     "byDayStats": {"2026-06-29T00:00:00Z": {"totalMessageReplies": 1, "autoTaggedInterested": 1}}})
        if url.endswith("/campaign/GetLeadsFromCampaign"):
            off = json["offset"]
            return FakeResp({"totalCount": 1, "items": [{"creationTime": "2026-06-29T10:00:00Z"}] if off == 0 else []})
        return FakeResp({})
    async def aclose(self): pass


def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


async def main():
    c = HeyReachClient("k", "DARLEAN")
    c._client = FakeHTTP()  # inject fake (bypass __aenter__)
    camps = await c.list_campaigns()
    ok(len(camps) == 2, f"list_campaigns paged to 2 (got {len(camps)})")
    ok(camps[0]["progressStats"]["totalUsers"] == 99, "campaign progressStats present")
    s = await c.get_overall_stats(1, start="2026-06-01T00:00:00Z", end="2026-06-30T23:59:59Z")
    ok(s["overallStats"]["connectionsSent"] == 25, "overall stats parsed")
    ok("2026-06-29T00:00:00Z" in s["byDayStats"], "byDayStats present")
    leads = await c.get_campaign_leads(1)
    ok(len(leads) == 1 and leads[0]["creationTime"].startswith("2026-06-29"), "leads paged")
    print("\nALL PASSED")

if __name__ == "__main__":
    asyncio.run(main())
