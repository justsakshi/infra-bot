"""Assemble per-campaign metric rows (Smartlead + HeyReach) with UTC date buckets.

Feeds the "Campaign Metrics" sheet tab, which mirrors the table the team keeps
by hand. Rows come from two platforms with different capabilities, so a column
can be exact on one and unavailable on the other — "-" means the platform does
not report it, and is deliberately not 0.

Trust levels, so nobody reads a soft number as hard:

  EXACT (straight from the API)
    total_leads, leads_in_progress, leads_not_started,
    leads_added_month, leads_added_yesterday,
    connections_sent, connections_accepted (HeyReach only), status

  APPROXIMATE (platform auto-categorisation, no human check)
    positive_neutral_month  - Smartlead category ids 1/2/5, HeyReach autoTagged
    total_responses_month   - counts auto-replies, OOO and bounces too

  UNRESOLVED
    msg_sent - ours is all-time and does not match the team's manual sheet.
               See the note at the field itself before "fixing" it.
    positive_responses_yesterday - Smartlead exposes no per-day figure; blank
               rather than inferred. HeyReach reports it properly.

Dates are bucketed in UTC. "Yesterday" therefore means the UTC day, which can
differ from an IST reading day near midnight.
"""
from __future__ import annotations

from datetime import datetime, timezone

COLUMNS = [
    # `client` was added once the tab covered more than one account — without it
    # a shared tab mixes clients' campaigns with no way to tell them apart, and
    # the Total row silently sums across unrelated businesses.
    "client", "campaign", "platform", "status", "total_leads", "leads_added_month",
    "leads_added_yesterday", "leads_in_progress", "leads_not_started", "connections_sent",
    "connections_accepted", "msg_sent", "positive_responses_yesterday",
    "total_responses_month", "positive_neutral_month",
]

# numeric columns summed in the Total row
_NUMERIC = [
    "total_leads", "leads_added_month", "leads_added_yesterday", "leads_in_progress", "leads_not_started",
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


def get_reporting_range(month_arg: str | None, today: datetime) -> tuple[datetime, datetime, str]:
    """Return (start_dt, end_dt, month_name) for the reporting month.
    
    All datetimes are timezone-aware (UTC).
    """
    from datetime import timedelta
    month_arg = (month_arg or "auto").strip().lower()
    
    # Auto-detection: day <= 5 -> previous month; otherwise current month
    if month_arg == "auto":
        if today.day <= 5:
            month_arg = "previous"
        else:
            month_arg = "current"
            
    if month_arg == "current":
        start_dt = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = today
        month_name = today.strftime("%B")
        return start_dt, end_dt, month_name
        
    elif month_arg == "previous":
        if today.month == 1:
            prev_month = 12
            prev_year = today.year - 1
        else:
            prev_month = today.month - 1
            prev_year = today.year
        start_dt = datetime(prev_year, prev_month, 1, 0, 0, 0, tzinfo=timezone.utc)
        curr_month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = curr_month_start - timedelta(seconds=1)
        month_name = start_dt.strftime("%B")
        return start_dt, end_dt, month_name
        
    else:
        # Try to parse month name or number
        month_num = None
        try:
            val = int(month_arg)
            if 1 <= val <= 12:
                month_num = val
        except ValueError:
            pass
            
        if not month_num:
            months = [
                "january", "february", "march", "april", "may", "june",
                "july", "august", "september", "october", "november", "december"
            ]
            for idx, m in enumerate(months):
                if month_arg.startswith(m[:3]):
                    month_num = idx + 1
                    break
                    
        if not month_num:
            raise ValueError(f"Unknown month format: {month_arg}")
            
        start_dt = datetime(today.year, month_num, 1, 0, 0, 0, tzinfo=timezone.utc)
        if month_num == 12:
            next_month_start = datetime(today.year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        else:
            next_month_start = datetime(today.year, month_num + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_dt = next_month_start - timedelta(seconds=1)
        
        if start_dt <= today <= end_dt:
            end_dt = today
            
        month_name = start_dt.strftime("%B")
        return start_dt, end_dt, month_name


def _in_month(dt: datetime | None, start_dt: datetime, end_dt: datetime) -> bool:
    return dt is not None and dt >= start_dt and dt <= end_dt


def _is_yesterday(dt: datetime | None, today: datetime) -> bool:
    if dt is None:
        return False
    return dt.date().toordinal() == today.date().toordinal() - 1


def _int(v) -> int:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def smartlead_summary_from_analytics(analytics: dict) -> dict:
    """Map lightweight campaign analytics to the summary shape metrics uses."""
    lead_stats = analytics.get("campaign_lead_stats", {}) or {}
    return {
        "campaign_id": analytics.get("id"),
        "name": analytics.get("name", ""),
        "status": analytics.get("status", ""),
        "total_leads": _int(lead_stats.get("total", 0)),
        "in_progress": _int(lead_stats.get("inprogress", 0)),
        "not_started": _int(lead_stats.get("notStarted", 0)),
        "sent": _int(analytics.get("sent_count", 0)),
    }


_INACTIVE_STATUSES = {"DRAFTED", "DRAFT"}
_STALE_STATUSES = {"PAUSED", "COMPLETED", "COMPLETE", "STOPPED", "STOP"}
_STALE_DAYS = 7


def should_include_smartlead_campaign(summary: dict, week_sent: int, month_sent: int = 0) -> bool:
    """Exclude DRAFTs. For PAUSED/COMPLETED: include if sent this month or this week.

    Why not simply list every campaign: an account accumulates dozens of drafts
    and long-dead campaigns, and a report padded with permanent zero rows stops
    being read. A paused campaign that still sent this month is real work and
    belongs in the table; one that has sent nothing does not.
    """
    status = str(summary.get("status", "")).upper()
    if status in _INACTIVE_STATUSES or status.startswith("DRAFT"):
        return False
    if status in _STALE_STATUSES or any(status.startswith(s) for s in _STALE_STATUSES):
        return (week_sent > 0) or (month_sent > 0)
    return True


def should_include_heyreach_campaign(campaign: dict, month_by_day: dict) -> bool:
    """Exclude DRAFTs. For PAUSED/COMPLETED: include if sent this month."""
    status = str(campaign.get("status", "")).upper()
    if status in _INACTIVE_STATUSES or status.startswith("DRAFT"):
        return False
    if status in _STALE_STATUSES or any(status.startswith(s) for s in _STALE_STATUSES):
        for v in month_by_day.values():
            if _int(v.get("messagesSent", 0)) > 0 or _int(v.get("connectionsSent", 0)) > 0:
                return True
        return False
    return True


def smartlead_metric_row(summary: dict, leads: list[dict], month_replies: int,
                         yest_replies: int, today: datetime, positive_ids: set[int],
                         client: str = "",
                         month_sent: int = 0, start_dt: datetime | None = None,
                         end_dt: datetime | None = None) -> dict:
    added_month = added_yest = pos_neutral = 0
    if start_dt is None or end_dt is None:
        start_dt = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = today

    for lead in leads:
        d = _parse(lead.get("created_at", ""))
        if _in_month(d, start_dt, end_dt):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1
        if lead.get("lead_category_id") in positive_ids:
            pos_neutral += 1
    return {
        "client": client,
        "campaign": summary.get("name", ""),
        "platform": "Smartlead",
        "status": summary.get("status", ""),
        "total_leads": _int(summary.get("total_leads", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(summary.get("in_progress", 0)),
        "leads_not_started": _int(summary.get("not_started", 0)),
        # LinkedIn-only concepts; "-" rather than 0 so nobody reads a real zero.
        "connections_sent": "-",
        "connections_accepted": "-",
        # ALL-TIME sends for the campaign, falling back to the reporting month
        # when the all-time figure is missing. NOTE (2026-07-30): this does not
        # match the team's manual sheet — for "Legal Firms Roundtable" we report
        # 1068 (all-time) where the manual sheet shows 374. Smartlead offers
        # all-time 1068, July-only 694 and unique 833, none of which is 374, so
        # the manual column is measuring a different window. Confirm the
        # intended definition with whoever maintains that sheet before changing
        # this line.
        "msg_sent": _int(summary.get("sent", 0)) or month_sent,
        # Smartlead exposes no per-day positive-reply count. It could be
        # inferred from lead categories plus timestamps, but that would be a
        # guess presented as a number, so it stays blank until we can source it
        # properly. HeyReach does report this, so its rows carry a real value.
        "positive_responses_yesterday": "-",
        # Raw reply count for the month — includes auto-replies, out-of-office
        # and bounces, so it reads higher than a human counting real responses.
        "total_responses_month": _int(month_replies),
        # Smartlead's own auto-categorisation (Interested / Meeting Request /
        # Information Request). Machine-tagged, not human-verified.
        "positive_neutral_month": pos_neutral,
    }


def heyreach_metric_row(campaign: dict, overall_alltime: dict, overall_month: dict,
                        leads: list[dict], today: datetime, client: str = "",
                        start_dt: datetime | None = None,
                        end_dt: datetime | None = None) -> dict:
    ps = campaign.get("progressStats", {}) or {}
    oa = (overall_alltime or {}).get("overallStats", {}) or {}
    om = (overall_month or {}).get("overallStats", {}) or {}
    by_day = (overall_month or {}).get("byDayStats", {}) or {}

    if start_dt is None or end_dt is None:
        start_dt = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = today

    added_month = added_yest = 0
    for lead in leads:
        d = _parse(lead.get("creationTime", ""))
        if _in_month(d, start_dt, end_dt):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1

    y_ord = today.date().toordinal() - 1
    pos_yest = 0
    for k, v in by_day.items():
        d = _parse(k)
        if d and d.date().toordinal() == y_ord:
            pos_yest = _int(v.get("autoTaggedInterested", 0))
            break

    return {
        "client": client,
        "campaign": campaign.get("name", ""),
        "platform": "Heyreach",
        "status": campaign.get("status", ""),
        "total_leads": _int(ps.get("totalUsers", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(ps.get("totalUsersInProgress", 0)),
        # HeyReach has no "not started" concept — leads are either in the
        # sequence or not in the campaign at all.
        "leads_not_started": "-",
        "connections_sent": _int(oa.get("connectionsSent", 0)),
        "connections_accepted": _int(oa.get("connectionsAccepted", 0)),
        # All-time, matching the Smartlead column's basis.
        "msg_sent": _int(oa.get("messagesSent", 0)),
        # HeyReach reports this per day, so unlike Smartlead it is a real value.
        "positive_responses_yesterday": pos_yest,
        "total_responses_month": _int(om.get("totalMessageReplies", 0)),
        # HeyReach's own auto-tagging, same caveat as the Smartlead column.
        "positive_neutral_month": _int(om.get("autoTaggedInterested", 0)),
    }


def total_row(rows: list[dict], client: str = "") -> dict:
    out = {c: "" for c in COLUMNS}
    out["client"] = client
    out["campaign"] = f"Total — {client}" if client else "Total"
    for col in _NUMERIC:
        out[col] = sum(r.get(col, 0) for r in rows if isinstance(r.get(col), int))
    return out


def rows_with_totals(rows: list[dict]) -> list[dict]:
    """Group rows by client, append a per-client subtotal after each group, and
    finish with a grand total.

    A single mixed Total across unrelated clients is not a number anyone can
    use — Darlean's lead count summed with BettrData's answers no question. The
    per-client subtotal is the figure that matches how the team reports.
    """
    if not rows:
        return []
    by_client: dict[str, list[dict]] = {}
    for r in rows:
        by_client.setdefault(str(r.get("client", "")), []).append(r)

    out: list[dict] = []
    for client in sorted(by_client):
        group = by_client[client]
        out.extend(group)
        out.append(total_row(group, client))
    if len(by_client) > 1:
        out.append(total_row(rows, ""))   # grand total across all clients
    return out
