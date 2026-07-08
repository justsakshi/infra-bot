"""Detect 'stale' campaigns: ACTIVE in Smartlead but effectively dead.

A campaign is stale when it has added NO new lead in the last STALE_DAYS days
AND sent NO emails in the last STALE_DAYS days. Such a campaign's inboxes are
neither working nor being kept warm -> they silently go cold. We flag them so
(a) the inbox gets warmup turned back ON, and (b) the manager is told to feed
the campaign leads or pause it.
"""
from __future__ import annotations

from datetime import date, datetime

STALE_DAYS = 14


def _days_since(date_str: str, today: date) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
    except ValueError:
        return None
    return (today - d).days


def is_campaign_stale(newest_lead_date: str, sent_last_14d: int, today: date) -> bool:
    """Stale = no lead added in STALE_DAYS AND no sends in STALE_DAYS."""
    if int(sent_last_14d or 0) > 0:
        return False  # still sending -> alive
    age = _days_since(newest_lead_date, today)
    if age is None:
        return True   # no leads on record + not sending -> stale
    return age >= STALE_DAYS


async def stale_campaign_names(client, today) -> set[str]:
    """Names of ACTIVE campaigns that are stale (dead 14d+).

    PERF CONTRACT (2026-07-08): the full-lead pagination is the expensive
    call (~1h across the fleet). A campaign with ANY sends in the window is
    fresh by definition (is_campaign_stale requires no sends AND no new
    leads), so leads are fetched ONLY for campaigns whose 14d sent_count
    is 0 — in practice a handful instead of all of them.
    `client` is a SmartleadClient (or any object with list_campaigns /
    get_analytics_by_date / get_campaign_leads)."""
    from datetime import timedelta

    stale: set[str] = set()
    start = (today - timedelta(days=STALE_DAYS)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    for camp in await client.list_campaigns():
        if str(camp.get("status", "")).upper() != "ACTIVE":
            continue
        cid = str(camp["id"])
        try:
            an = await client.get_analytics_by_date(cid, start, end)
            sent14 = int(float(an.get("sent_count", 0) or 0))
            if sent14 > 0:
                continue  # sending -> fresh; skip the expensive lead fetch
            leads = await client.get_campaign_leads(cid)
            newest = max((l.get("created_at", "") for l in leads), default="")
        except Exception as exc:  # noqa: BLE001
            print(f"  [Freshness] check failed for {cid}: {exc}")
            continue
        if is_campaign_stale(newest, sent14, today):
            stale.add(camp.get("name", ""))
    return stale
