import asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

async def main():
    acc = next(a for a in discover_accounts() if a.name == "DARLEAN")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        cid = str((await c.list_campaigns())[0]["id"])
        leads = await c.get_campaign_leads(cid)
        ok(isinstance(leads, list), "leads is a list")
        if leads:
            ok("created_at" in leads[0], "lead has created_at")
        a = await c.get_analytics_by_date(cid, "2026-06-01", "2026-06-30")
        ok("reply_count" in a, "analytics-by-date has reply_count")
    print("\nALL PASSED")

asyncio.run(main())
