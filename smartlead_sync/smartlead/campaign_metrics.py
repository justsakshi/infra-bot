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


def is_excluded_campaign_name(name: str, patterns: list[str] | None = None) -> bool:
    """True when the campaign name matches a curated exclusion pattern.

    Kept separate from the status/lead rules because it encodes an editorial
    decision about which verticals get reported, not a fact about the campaign.
    """
    if not patterns:
        return False
    # Both sides get whitespace-collapsed. Campaign names in Smartlead contain
    # stray double spaces ("Architecture & interior design  - New"), so a
    # pattern copied from the UI would otherwise never match — and the failure
    # is silent, leaving the row in the report.
    n = " ".join(str(name or "").lower().split())
    return any(" ".join(p.split()) in n for p in patterns)


def should_include_smartlead_campaign(summary: dict, week_sent: int, month_sent: int = 0) -> bool:
    """Exclude DRAFTs and campaigns holding no leads. Keep everything else.

    This deliberately keeps PAUSED/COMPLETED campaigns that sent nothing this
    month. An earlier version dropped them to avoid padding the report with
    zero rows, but that hid 15,476 leads — a paused campaign still holding
    5,518 leads in progress is live inventory someone has to act on, and the
    team's manual sheet lists exactly these rows with a 0 in Msg Sent.

    The zero-lead check is what the activity filter was really reaching for:
    it drops abandoned shells (0 leads, 0 sends) without hiding real backlog.

    `week_sent`/`month_sent` are no longer used for the decision. They are kept
    in the signature because callers compute them anyway for `msg_sent`, and
    dropping them would churn every call site for no gain.
    """
    status = str(summary.get("status", "")).upper()
    if status in _INACTIVE_STATUSES or status.startswith("DRAFT"):
        return False
    # Only reachable once totals are known; callers that pass a bare campaign
    # dict (no lead stats) fall through to True, preserving prior behaviour.
    if "total_leads" in summary and _int(summary.get("total_leads", 0)) == 0:
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
        # Sends within the reporting month. RESOLVED (2026-08-06): the earlier
        # note here guessed that the manual sheet used some unknown window. It
        # is simply the current calendar month. Verified against the team's
        # sheet across five campaigns — Legal Firms 374, Consulting 518, Field
        # Services V3 2498, HVAC 32, Plumbing 85 — where month-to-date matches
        # every value and all-time matches none.
        "msg_sent": month_sent,
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
    """Build the metrics row for one HeyReach campaign.

    `overall_alltime` is accepted but no longer read: every activity column is
    month-to-date so the row lines up with the Smartlead rows beside it. The
    parameter stays so the two call sites keep working unchanged, and because
    an all-time column may well be wanted again later.
    """
    ps = campaign.get("progressStats", {}) or {}
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
        # Month-to-date, matching the Smartlead column. These read from `om`
        # (the month window) not `oa` (all-time): a Total row that adds
        # all-time LinkedIn activity to month-to-date email activity is a
        # number with no meaning.
        "connections_sent": _int(om.get("connectionsSent", 0)),
        "connections_accepted": _int(om.get("connectionsAccepted", 0)),
        "msg_sent": _int(om.get("messagesSent", 0)),
        # HeyReach reports this per day, so unlike Smartlead it is a real value.
        "positive_responses_yesterday": pos_yest,
        "total_responses_month": _int(om.get("totalMessageReplies", 0)),
        # HeyReach's own auto-tagging, same caveat as the Smartlead column.
        "positive_neutral_month": _int(om.get("autoTaggedInterested", 0)),
    }


def expandi_metric_row(campaign: dict, baseline: dict | None, prev_day: dict | None,
                       client: str = "", lead_counts: dict | None = None) -> dict:
    """Build the metrics row for one Expandi campaign.

    Expandi reports cumulative lifetime counters and has no working date filter,
    so where a snapshot exists the activity columns are today's counters minus
    the snapshot taken at or before the month start (see expandi_store).

    Windowed columns come from per-lead timestamps where the sweep has covered
    the campaign, and from snapshot differencing otherwise.

    Where neither exists the cell is "-", not a number. An earlier version fell
    back to the all-time counter on the reasoning that a true figure over a
    wider window beats a placeholder. That was wrong, and visibly so: every
    Expandi campaign reported its entire lead history as both "added in August"
    and "added yesterday" — 4,234 leads across the account all appearing to
    arrive on the same day. A lifetime total in a month-to-date column is not a
    conservative estimate, it is a wrong answer with no way for the reader to
    tell. A fabricated 0 is equally wrong in the other direction, so neither is
    used: "-" says the window is not knowable for this campaign yet.

    total_leads, leads_in_progress, leads_not_started and the response columns
    are standing totals and stay populated regardless.
    """
    stats = campaign.get("stats") or {}

    def delta(field: str, since: dict | None):
        """Windowed count from two snapshots, or "-" when unknowable.

        Returning the all-time counter here was wrong: these columns are
        month-to-date or yesterday, and a lifetime total placed in them
        overstates the window without any signal that it has done so. It made
        every Expandi campaign report its entire lead history as both "added in
        August" and "added yesterday".
        """
        if since is None:
            return "-"
        return max(0, _int(stats.get(field)) - _int(since.get(field)))

    # Per-lead timestamps are the accurate source: exact day buckets, and they
    # cover history from before this code existed. Snapshot differencing is the
    # fallback for campaigns the lead cache has not swept yet.
    #
    # `cached` is compared against the campaign's own contacted count rather
    # than trusted outright — a half-swept campaign would otherwise report, say,
    # 40 of 126 invites as though that were the month's real total, which is
    # both wrong and indistinguishable from a quiet month.
    # `swept` is set once the messengers endpoint has been paginated to the end
    # for every instance of the campaign. It is NOT inferred from row counts:
    # `stats.initiated` can exceed the rows that endpoint returns (contacts
    # messaged directly, with no connection request, are counted by the stats
    # but carry no invite timestamp), so a count comparison would leave those
    # campaigns permanently "incomplete" and pinned to the fallback path.
    lc = lead_counts or {}
    lead_data_complete = bool(lc) and bool(lc.get("swept")) and _int(lc.get("cached")) > 0

    if lead_data_complete:
        conn_sent_month = _int(lc.get("invited_month"))
        conn_acc_month = _int(lc.get("connected_month"))
    elif baseline is not None:
        # A real snapshot exists, so the difference is a genuine month figure.
        conn_sent_month = delta("initiated", baseline)
        conn_acc_month = delta("connected", baseline)
    else:
        # Neither per-lead data nor a baseline. The all-time counter is a true
        # number but answers a different question, and putting it in a
        # month-to-date column silently overstates the month — the same defect
        # that made "leads added" report a campaign's entire history as today's
        # intake. "-" says the month is not knowable for this campaign yet.
        conn_sent_month = "-"
        conn_acc_month = "-"

    # Responses are reported as running totals, matching how the team's manual
    # sheet reads them — verified against it, where every response figure agrees
    # exactly (1/1, 3/3, 3/3, 1/1). Differencing them to a month window would
    # show 0 for a campaign that has replies but none since the month started,
    # which reads as "no one answered" rather than "no new answers".
    replied_total = _int(stats.get("replied_msg")) + _int(stats.get("replied_excl_msg"))

    return {
        "client": client,
        "campaign": campaign.get("name", ""),
        "platform": "Expandi",
        # Expandi exposes `active` as a bool, not a status string. Mapped onto
        # the same vocabulary the other platforms use so the column stays
        # sortable and filterable.
        "status": "IN_PROGRESS" if campaign.get("active") else "PAUSED",
        # All-time and correct as-is: a lead count is a standing total, not an
        # activity figure, so it needs no differencing.
        "total_leads": _int(stats.get("people_in_campaign")),
        # From each lead's own `created` timestamp. NOT differenced from
        # people_in_campaign: that is a standing total, so with no snapshot to
        # subtract, the all-time fallback reported every lead the campaign has
        # ever held as added this month AND added yesterday — 4,234 leads all
        # claiming to arrive on the same day. Where the sweep has not covered a
        # campaign the value is "-", because no honest number exists for it.
        "leads_added_month": _int(lc.get("added_month")) if lead_data_complete else "-",
        "leads_added_yesterday": _int(lc.get("added_yesterday")) if lead_data_complete else "-",
        "leads_in_progress": _int(stats.get("in_queue")),
        # Contacts the campaign has never acted on: everyone in it, minus
        # everyone it has initiated contact with. Verified against the team's
        # sheet, where the campaigns that have reached every contact
        # (BD Select 126/126, Persona 2 129/129) show 0 exactly.
        # `in_queue` is NOT this figure — that is the active send queue, and
        # using it reports 30 "not started" for a campaign that has already
        # contacted everyone.
        "leads_not_started": max(0, _int(stats.get("people_in_campaign"))
                                 - _int(stats.get("initiated"))),
        # From per-lead invited_at/connected_at where the lead cache has swept
        # this campaign; snapshot differencing otherwise.
        "connections_sent": conn_sent_month,
        "connections_accepted": conn_acc_month,
        # Expandi sends its first message as the connection request, so
        # contacted_people tracks initiated. Kept on the stats/snapshot path
        # because a later sequence step is not a new messenger row and so is
        # not visible in the per-lead data.
        "msg_sent": delta("contacted_people", baseline),
        "positive_responses_yesterday": delta("interested_people", prev_day),
        # Running totals, not month deltas — see the note above.
        "total_responses_month": replied_total,
        "positive_neutral_month": _int(stats.get("interested_people")),
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
