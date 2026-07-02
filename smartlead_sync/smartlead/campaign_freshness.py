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
