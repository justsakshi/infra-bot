"""Assemble per-campaign metric rows (Smartlead + HeyReach) with UTC date buckets."""
from __future__ import annotations

from datetime import datetime, timezone

COLUMNS = [
    "campaign", "platform", "status", "total_leads", "leads_added_month",
    "leads_added_yesterday", "leads_in_progress", "connections_sent",
    "connections_accepted", "msg_sent", "positive_responses_yesterday",
    "total_responses_month", "positive_neutral_month",
]

# numeric columns summed in the Total row
_NUMERIC = [
    "total_leads", "leads_added_month", "leads_added_yesterday", "leads_in_progress",
    "connections_sent", "connections_accepted", "msg_sent",
    "positive_responses_yesterday", "total_responses_month", "positive_neutral_month",
]


def _parse(dt: str) -> datetime | None:
    if not dt:
        return None
    try:
        d = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def month_start(today: datetime) -> datetime:
    return today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _in_month(dt: datetime | None, today: datetime) -> bool:
    return dt is not None and dt >= month_start(today) and dt <= today


def _is_yesterday(dt: datetime | None, today: datetime) -> bool:
    if dt is None:
        return False
    return dt.date().toordinal() == today.date().toordinal() - 1


def _int(v) -> int:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def smartlead_metric_row(summary: dict, leads: list[dict], month_replies: int,
                         yest_replies: int, today: datetime, positive_ids: set[int]) -> dict:
    added_month = added_yest = pos_neutral = 0
    for lead in leads:
        d = _parse(lead.get("created_at", ""))
        if _in_month(d, today):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1
        if lead.get("lead_category_id") in positive_ids:
            pos_neutral += 1
    return {
        "campaign": summary.get("name", ""),
        "platform": "Smartlead",
        "status": summary.get("status", ""),
        "total_leads": _int(summary.get("total_leads", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(summary.get("in_progress", 0)),
        "connections_sent": "-",
        "connections_accepted": "-",
        "msg_sent": _int(summary.get("sent", 0)),
        # Smartlead positive-by-date is API-limited; yesterday-positive not reliably
        # available -> "-" (HeyReach has it). positive/neutral = current category snapshot.
        "positive_responses_yesterday": "-",
        "total_responses_month": _int(month_replies),
        "positive_neutral_month": pos_neutral,
    }


def heyreach_metric_row(campaign: dict, overall_alltime: dict, overall_month: dict,
                        leads: list[dict], today: datetime) -> dict:
    ps = campaign.get("progressStats", {}) or {}
    oa = (overall_alltime or {}).get("overallStats", {}) or {}
    om = (overall_month or {}).get("overallStats", {}) or {}
    by_day = (overall_month or {}).get("byDayStats", {}) or {}

    added_month = added_yest = 0
    for lead in leads:
        d = _parse(lead.get("creationTime", ""))
        if _in_month(d, today):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1

    # yesterday's positive from byDayStats (key = UTC midnight)
    y_ord = today.date().toordinal() - 1
    pos_yest = 0
    for k, v in by_day.items():
        d = _parse(k)
        if d and d.date().toordinal() == y_ord:
            pos_yest = _int(v.get("autoTaggedInterested", 0))
            break

    return {
        "campaign": campaign.get("name", ""),
        "platform": "Heyreach",
        "status": campaign.get("status", ""),
        "total_leads": _int(ps.get("totalUsers", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(ps.get("totalUsersInProgress", 0)),
        "connections_sent": _int(oa.get("connectionsSent", 0)),
        "connections_accepted": _int(oa.get("connectionsAccepted", 0)),
        "msg_sent": _int(oa.get("messagesSent", 0)),
        "positive_responses_yesterday": pos_yest,
        "total_responses_month": _int(om.get("totalMessageReplies", 0)),
        "positive_neutral_month": _int(om.get("autoTaggedInterested", 0)),
    }


def total_row(rows: list[dict]) -> dict:
    out = {c: "" for c in COLUMNS}
    out["campaign"] = "Total"
    for col in _NUMERIC:
        out[col] = sum(r.get(col, 0) for r in rows if isinstance(r.get(col), int))
    return out
