"""Live check of the stats post's numbers (read-only, no Slack).

For every Smartlead campaign that moved in the period: emails sent equal
Smartlead's own analytics-by-date figure, and the people who replied are at
most the people the Show replies button lists (the post counts a person on
their first reply only since 11 Nov; the button shows everyone who wrote).
Bounces are printed beside Smartlead's figure. Exit 1 on any mismatch.

    python check_stats_post.py 2026-10-30               one day
    python check_stats_post.py 2026-10-23 2026-10-29    a week

Run 2 Nov 2026: 27, 29, 30 Oct and the week 23-29 Oct all matched (a
mismatch seen once came from Smartlead rate limits during the run).
"""
import sys, asyncio; sys.path.insert(0,'.')
from datetime import date
from smartlead import daily_stats as ds
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead import reply_view as rv
DAY = date.fromisoformat(sys.argv[1])
END = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else DAY
P = ds.Period("day", DAY, END)
async def main():
    failed=[]
    rows = await ds._smartlead([P], failed, "[chk]")
    rows += await ds._heyreach([P], failed, "[chk]")
    bad=0; n=0
    accs = discover_accounts()
    for r in rows:
        c=r["counts"]["day"]; lo,hi=P.window
        if r["platform"]=="Smartlead":
            a=None
            for acc in accs:
                async with SmartleadClient(acc.api_key, acc.name) as s:
                    try: a=await s.get_analytics_by_date(r["campaign_id"], DAY.isoformat(), END.isoformat())
                    except Exception: continue
                    if a and a.get("name"): break
            found,_ = await rv._smartlead(r["campaign_id"], lo, hi)
            btn = rv.select_replies(found, lo, hi, 0)
            people = len({rv._who(x) for x in btn})
            checks = {"sent": (c["sent"], int(a["sent_count"])), "button people": (c["replies"], people)}
            print("INFO bounced", r["campaign"][:40], "post", c["bounced"], "smartlead day", a["bounce_count"])
        else:
            found = await rv._heyreach(r["client"], r["campaign_id"], lo, hi)
            checks = {"button people": (c["replies"], len({rv._who(x) for x in found}))}
        for k,(mine,theirs) in checks.items():
            n+=1
            ok = mine <= theirs if k == "button people" else mine == theirs
            bad += not ok
            print(("PASS" if ok else "FAIL"), r["platform"], r["campaign"][:45], k, "post", mine, "vs", theirs)
    print(f"{n-bad}/{n} pass; failed fetches: {failed}")
    return 1 if bad else 0
sys.exit(asyncio.run(main()))
