"""Collect one metrics row per campaign: Smartlead, HeyReach and Expandi.

The single collector behind the Campaign Metrics tabs, Campaign History and
the daily stats post (metrics_only.py runs it at 09:00 IST). run.py used to
carry its own copy of the Smartlead block; two copies is how one of them
drifted and reported different numbers.

Only ACTIVE_CLIENTS are collected, and a campaign is dropped as early as
possible: drafts, de-scoped names and other clients' campaigns before any
analytics call, and a PAUSED / COMPLETED campaign after the one call that
shows it sent nothing this week (the rule should_include_smartlead_campaign
applies anyway). On 29 Oct the PRECISE_LEADS account held 571 non-draft
campaigns, 4 of them ACTIVE; fetching every lead of each before filtering made
the block take about 24 minutes (Last Sync tab, 29 Oct).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from smartlead import campaign_metrics as cm
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.client_filter import is_past_client_campaign
from smartlead.config import (CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_EXCLUDE,
                              SMARTLEAD_POSITIVE_CATEGORY_IDS)
from smartlead.expandi_rows import build_expandi_rows
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces


def _n(payload: dict, field: str) -> int:
    return int(float((payload or {}).get(field, 0) or 0))


async def _smartlead_rows(today: datetime, start_dt: datetime, end_dt: datetime,
                          log: str) -> list[dict]:
    rows: list[dict] = []
    ms_str, end_str = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    week_start = cm.week_start_str(today)
    yest_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    accounts = [a for a in discover_accounts()
                if cm.account_in_scope(a.name, CAMPAIGN_METRICS_CLIENTS)]
    if not accounts:
        print(f"{log} no Smartlead account holds an active client — check ACTIVE_CLIENTS")
    for acc in accounts:
        # Isolated per account: one account's outage must not cost every other
        # account's rows in the same run.
        try:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                campaigns = await slc.list_campaigns()
                if not campaigns:
                    print(f"{log} Smartlead {acc.name} returned 0 campaigns — "
                          f"check this is not silent data loss")
                kept = 0
                for camp in campaigns:
                    cid = str(camp.get("id") or "")
                    if not cid:
                        continue
                    status = str(camp.get("status", "")).upper()
                    if status in cm._INACTIVE_STATUSES or status.startswith("DRAFT"):
                        continue
                    if (cm.is_excluded_campaign_name(camp.get("name", ""),
                                                     CAMPAIGN_METRICS_EXCLUDE)
                            or is_past_client_campaign(camp)):
                        continue
                    client = cm.display_client(
                        cm.metrics_client_for(acc.name, camp.get("client_id")))
                    if not cm.row_client_wanted(client, CAMPAIGN_METRICS_CLIENTS):
                        continue
                    # A failed fetch is not "zero activity": skip the campaign
                    # and say so, rather than write a fabricated 0.
                    try:
                        week_sent = _n(await slc.get_analytics_by_date(
                            cid, week_start, today_str), "sent_count")
                        if cm.is_finished_status(status) and week_sent == 0:
                            continue
                        mo = await slc.get_analytics_by_date(cid, ms_str, end_str)
                        yd = await slc.get_analytics_by_date(cid, yest_str, yest_str)
                        analytics = await slc.get_campaign_analytics(cid)
                        leads = await slc.get_campaign_leads(cid)
                    except Exception as exc:  # noqa: BLE001
                        print(f"  [!] Smartlead {cid} ({camp.get('name', '')}) "
                              f"skipped — fetch failed: {exc}")
                        continue
                    summary = cm.smartlead_summary_from_analytics(analytics)
                    if not cm.should_include_smartlead_campaign(
                            summary, week_sent, _n(mo, "sent_count")):
                        continue
                    rows.append(cm.smartlead_metric_row(
                        summary, leads, _n(mo, "reply_count"), 0,
                        today, SMARTLEAD_POSITIVE_CATEGORY_IDS,
                        client=client,
                        month_sent=_n(mo, "sent_count"), start_dt=start_dt, end_dt=end_dt,
                        yest_sent=_n(yd, "sent_count"),
                        launch_date=str(camp.get("created_at", ""))[:10]))
                    kept += 1
                print(f"{log} Smartlead {acc.name}: {kept} campaign(s) of "
                      f"{len(campaigns)} reported")
        except Exception as exc:  # noqa: BLE001
            print(f"{log} Smartlead account {acc.name} failed to list campaigns "
                  f"— its rows are MISSING this run: {exc}")
    return rows


async def _heyreach_rows(today: datetime, start_dt: datetime, end_dt: datetime,
                         log: str) -> list[dict]:
    rows: list[dict] = []
    yest_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    for ws in discover_heyreach_workspaces():
        if not cm.row_client_wanted(ws.name, CAMPAIGN_METRICS_CLIENTS):
            continue
        try:
            async with HeyReachClient(ws.api_key, ws.name) as hrc:
                camps = await hrc.list_campaigns()
                print(f"{log} HeyReach {ws.name}: {len(camps)} campaign(s)")
                for camp in camps:
                    cid = camp["id"]
                    try:
                        oa = await hrc.get_overall_stats(cid)
                        om = await hrc.get_overall_stats(
                            cid, start=start_dt.isoformat().replace("+00:00", "Z"),
                            end=end_dt.isoformat().replace("+00:00", "Z"))
                        # HeyReach honours a date range, so yesterday is a real
                        # query rather than a difference between snapshots.
                        oy = await hrc.get_overall_stats(
                            cid, start=f"{yest_str}T00:00:00Z",
                            end=f"{yest_str}T23:59:59Z")
                        leads = await hrc.get_campaign_leads(cid)
                    except Exception as exc:  # noqa: BLE001
                        print(f"  [!] HeyReach {cid} failed: {exc}")
                        continue
                    if not cm.should_include_heyreach_campaign(
                            camp, (om or {}).get("byDayStats", {}) or {}):
                        continue
                    rows.append(cm.heyreach_metric_row(
                        camp, oa, om, leads, today, client=cm.display_client(ws.name),
                        start_dt=start_dt, end_dt=end_dt, overall_yesterday=oy))
        except Exception as exc:  # noqa: BLE001
            print(f"[!] HeyReach workspace {ws.name} failed: {exc}")
    return rows


async def collect_metric_rows(today: datetime, start_dt: datetime, end_dt: datetime,
                              log: str = "[Metrics]") -> list[dict]:
    """Every active client's campaign rows, without totals."""
    print(f"{log} clients: {', '.join(sorted(CAMPAIGN_METRICS_CLIENTS))}")
    rows = await _smartlead_rows(today, start_dt, end_dt, log)
    rows += await _heyreach_rows(today, start_dt, end_dt, log)
    rows += await build_expandi_rows(today, start_dt, log=log)
    return rows
