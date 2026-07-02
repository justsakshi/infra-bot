"""Runtime test for the batched/deduped fetch_account_data wiring.

Stubs only the network *leaf* methods of SmartleadClient with call counters,
so the real _gather_chunked + fetch_* helpers + processing logic execute.
Proves: dedup (#1), warmup batching (#2), analytics batching (#3).

Run: python test_sync_wiring.py
"""

from __future__ import annotations

import asyncio
from collections import Counter

from datetime import datetime, timedelta

from smartlead.api import SmartleadClient
from smartlead.processing import fetch_account_data, _apply_staleness


class FakeClient(SmartleadClient):
    """Real helpers, fake network leaves + per-endpoint call counters."""

    def __init__(self) -> None:
        super().__init__(api_key="testkey12345678", account_name="TEST")
        self.calls: Counter[str] = Counter()
        self.acct_calls: Counter[str] = Counter()  # per-campaign account fetches

    async def list_campaigns(self):
        self.calls["list_campaigns"] += 1
        return [
            {"id": 1, "name": "Active Camp", "status": "ACTIVE", "created_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "name": "Done Camp", "status": "COMPLETED", "created_at": "2025-01-01T00:00:00Z"},
        ]

    async def _get(self, endpoint, extra_params=None):
        # Only campaign-detail GETs reach here in this test
        self.calls["campaign_detail"] += 1
        cid = endpoint.rsplit("/", 1)[-1]
        return {"id": int(cid), "name": "Active Camp", "status": "ACTIVE", "max_leads_per_day": 60}

    async def get_campaign_analytics(self, campaign_id):
        self.calls["analytics"] += 1
        return {
            "sent_count": 100, "open_count": 40, "reply_count": 5, "bounce_count": 1,
            "unique_sent_count": 90,
            "campaign_lead_stats": {"total": 200, "notStarted": 50, "inprogress": 10,
                                    "paused": 0, "completed": 30, "stopped": 0},
        }

    async def get_campaign_email_accounts(self, campaign_id):
        self.acct_calls[str(campaign_id)] += 1
        return [{"id": 10}, {"id": 11}]

    async def list_email_accounts(self):
        self.calls["list_email_accounts"] += 1
        return [{"id": 10}, {"id": 11}, {"id": 12}]  # 12 = orphan (no campaign)

    async def get_email_account(self, account_id):
        self.calls["email_account_detail"] += 1
        return {
            "id": int(account_id),
            "from_email": f"box{account_id}@dom.com",
            "from_name": f"Box {account_id}",
            "type": "GMAIL",
            "message_per_day": 35,
            "daily_sent_count": 5,
            "is_smtp_success": True,
            "is_imap_success": True,
            "is_suspended": False,
            "updated_at": "2026-06-10T00:00:00Z",
            "warmup_details": {
                "status": "ACTIVE", "warmup_reputation": 95, "warmup_max_count": 40,
                "is_warmup_blocked": False, "created_at": "2026-01-01T00:00:00Z",
            },
        }

    async def get_warmup_stats(self, account_id):
        self.calls["warmup_stats"] += 1
        return [{
            "warmup_limit": 40, "sent_count": 20, "inbox_count": 19, "spam_count": 1,
            "stats_by_date": [
                {"date": "2026-06-08", "sent_count": 6},
                {"date": "2026-06-09", "sent_count": 0},
                {"date": "2026-06-11", "sent_count": 4},
            ],
        }]


def ok(cond, msg):
    print(f"  {'PASS' if cond else 'FAIL'}: {msg}")
    assert cond, msg


async def main() -> None:
    client = FakeClient()
    inbox, summary, warmup = await fetch_account_data(client, deliverability_map={}, active_only=True)

    print("\n--- assertions ---")
    # Campaign summary: 1 active (full) + 1 inactive (basic)
    ok(len(summary) == 2, f"campaign_summary == 2 rows (got {len(summary)})")
    # Inbox rows: 2 from active campaign + 1 orphan
    ok(len(inbox) == 3, f"inbox_data == 3 rows (got {len(inbox)})")
    # Warmup: one per account
    ok(len(warmup) == 3, f"warmup_data == 3 rows (got {len(warmup)})")

    # #1 DEDUP: campaign email-accounts fetched exactly ONCE per campaign
    ok(client.acct_calls["1"] == 1,
       f"get_campaign_email_accounts(1) called once, not twice (got {client.acct_calls['1']})")

    # #3 analytics fetched once for the single active campaign
    ok(client.calls["analytics"] == 1, f"analytics called once (got {client.calls['analytics']})")

    # #2 warmup batched: one call per account, all 3 ran
    ok(client.calls["warmup_stats"] == 3, f"warmup_stats called 3x (got {client.calls['warmup_stats']})")

    # data integrity spot-checks
    active_row = next(r for r in summary if r["name"] == "Active Camp")
    ok(active_row["sent"] == 100, f"active sent==100 (got {active_row['sent']})")
    ok(active_row["reach_pct"] == "45.0%", f"reach==45.0% (got {active_row['reach_pct']})")

    orphan = [r for r in inbox if r["campaign_name"].startswith("N/A")]
    ok(len(orphan) == 1, f"one orphan inbox (got {len(orphan)})")
    ok(all(w["warmup_reputation"] == "95%" for w in warmup), "warmup rep == 95% for all")

    row = inbox[0]
    ok("client" in row and row["client"] == "TEST", f"client set (got {row.get('client')!r})")
    ok(row["max_per_day"] == 35, f"max_per_day==35 (got {row['max_per_day']})")
    ok(row["sent_today"] == 5, f"sent_today==5 (got {row['sent_today']})")
    ok(row["capacity_left"] == 30, f"capacity_left==30 (got {row['capacity_left']})")
    ok(row["connection_ok"] is True, f"connection_ok True (got {row['connection_ok']})")
    # warmup created 2026-01-01 => age > 21d and rep 95 => ramped
    ok(row["warmup_state"] == "ramped", f"warmup_state ramped (got {row['warmup_state']})")
    ok(row["warmup_max_count"] == 40, f"warmup_max_count==40 (got {row['warmup_max_count']})")
    # latest sent>0 date in stats_by_date is 2026-06-11
    ok(row["last_active_date"] == "2026-06-11", f"last_active_date 2026-06-11 (got {row['last_active_date']})")
    # empty deliverability map => Unknown status, blank test_date
    ok("test_date" in row and row["test_date"] == "", f"test_date present+blank (got {row.get('test_date')!r})")

    # staleness helper: fresh inbox stays inbox, >14d inbox -> stale, fail/Unknown unchanged
    fresh = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    ok(_apply_staleness("inbox", fresh) == "inbox", "fresh inbox stays inbox")
    ok(_apply_staleness("inbox", old) == "stale", "20d-old inbox -> stale")
    ok(_apply_staleness("fail", old) == "fail", "old fail stays fail (not stale)")
    ok(_apply_staleness("inbox", "") == "inbox", "inbox with no date stays inbox")

    # availability hardening: empty deliverability map => test Unknown => BUSY w/ reason
    ok(row["availability"] == "BUSY", f"untested inbox is BUSY (got {row['availability']})")
    ok("untested" in row["busy_reason"], f"busy_reason has 'untested' (got {row['busy_reason']!r})")

    # master dedup: box10/box11 each in 1 campaign + box12 orphan => 3 unique inboxes
    from smartlead.sheets import SheetsWriter
    groups = {}
    for r in inbox:
        k = (r.get("client", ""), r.get("email", "").lower())
        g = groups.setdefault(k, 0)
        if not str(r.get("campaign_name", "")).startswith("N/A"):
            groups[k] = g + 1
    ok(len(groups) == 3, f"dedup -> 3 unique inboxes (got {len(groups)})")
    campaign_counts = sorted(groups.values())
    ok(campaign_counts == [0, 1, 1], f"campaign counts [0,1,1] (got {campaign_counts})")

    # Deliverability queue: failed/stale/untested/low-rep/high-volume rows become action items.
    from smartlead.sheets import build_deliverability_queue
    queue = build_deliverability_queue([
        {
            "client": "TEST", "email": "failed@dom.com", "provider": "Gmail", "account_id": "1",
            "availability": "BUSY", "busy_reason": "failed_test", "campaign_name": "Camp",
            "campaigns": 1, "max_per_day": 35, "sent_today": 0, "true_load": 0,
            "available_capacity": 35, "warmup_state": "ramped", "warmup_rep_pct": "95%",
            "test_sheet_status": "fail", "test_date": fresh,
        },
        {
            "client": "TEST", "email": "stale@dom.com", "provider": "Outlook", "account_id": "2",
            "availability": "BUSY", "busy_reason": "stale_test", "campaign_name": "N/A (No active campaign)",
            "campaigns": 0, "max_per_day": 35, "sent_today": 0, "true_load": 0,
            "available_capacity": 35, "warmup_state": "ramped", "warmup_rep_pct": "95%",
            "test_sheet_status": "stale", "test_date": old,
        },
        {
            "client": "TEST", "email": "healthy@dom.com", "provider": "Gmail", "account_id": "3",
            "availability": "FREE", "busy_reason": "", "campaign_name": "Camp",
            "campaigns": 1, "max_per_day": 35, "sent_today": 28, "true_load": 28,
            "available_capacity": 7, "warmup_state": "ramped", "warmup_rep_pct": "95%",
            "test_sheet_status": "inbox", "test_date": old,
        },
    ])
    ok([r["priority"] for r in queue] == ["P0", "P1", "P2"],
       f"queue priorities P0/P1/P2 (got {[r['priority'] for r in queue]})")
    ok(queue[0]["owner_skill"] == "deliverability-incident-response",
       f"failed test routes to incident response (got {queue[0]['owner_skill']})")
    ok(queue[1]["owner_skill"] == "email-deliverability-audit",
       f"stale test routes to audit (got {queue[1]['owner_skill']})")
    ok(queue[2]["owner_skill"] == "deliverability-test-public",
       f"high-volume retest routes to deliverability test (got {queue[2]['owner_skill']})")

    # Extension verification checks
    
    # 1. API key cleaning
    from smartlead.accounts import _clean_key
    ok(_clean_key("Smartlead: uuid_123_abc") == "uuid_123_abc", "clean key strips prefix")
    ok(_clean_key("  uuid_123_abc  ") == "uuid_123_abc", "clean key strips spaces")
    ok(_clean_key(None) is None, "clean key handles None")
    
    # 2. Date parsing
    from smartlead.sheets import _parse_test_date
    ok(_parse_test_date("2026-06-22") == "2026-06-22", "parse_test_date handles YYYY-MM-DD")
    ok(_parse_test_date("22/06/2026") == "2026-06-22", "parse_test_date handles DD/MM/YYYY")
    ok(_parse_test_date("2026/06/22") == "2026-06-22", "parse_test_date handles YYYY/MM/DD")
    
    # 3. Provider detection
    from smartlead.processing import detect_provider
    ok(detect_provider("GOOGLE_OAUTH") == "Gmail", "detect_provider handles GOOGLE_OAUTH")
    ok(detect_provider("OFFICE365_OAUTH") == "Outlook", "detect_provider handles OFFICE365_OAUTH")
    ok(detect_provider("smtp") == "Other", "detect_provider fallback")
    
    # 4. Connection check default values
    from smartlead.processing import _build_inbox_row
    dummy_acc = {"from_email": "test@dom.com", "from_name": "Test", "type": "smtp", "message_per_day": 35}
    row_with_defaults = _build_inbox_row(dummy_acc, "test@dom.com", "Cam", "ACTIVE", {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0}, {})
    ok(row_with_defaults["connection_ok"] is True, "connection_ok defaults to True when SMTP/IMAP flags are missing")

    # 5. Paused campaign staleness
    from smartlead.processing import _is_active_status
    ok(_is_active_status("PAUSED") is True, "paused campaign is active status")

    print("\nALL PASSED (INCLUDING IMPROVEMENTS)")


if __name__ == "__main__":
    asyncio.run(main())
