#!/usr/bin/env python3
"""Write the Campaign Metrics tab on its own, without the full inbox sync.

The metrics block normally runs at the very end of run.py, behind several
minutes of inbox/DNS/health work. That is fine on a schedule but far too slow
when you are checking whether the numbers are right. This entry point does the
metrics and nothing else, so a figure can be re-checked in under a minute.

Same code paths as run.py — it imports the same builders — so verifying here
verifies the scheduled run too.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead import campaign_metrics as cm
from smartlead.config import (
    CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_SHEET_ID,
    CAMPAIGN_METRICS_EXCLUDE,
    SMARTLEAD_POSITIVE_CATEGORY_IDS,
)
from smartlead.expandi_rows import build_expandi_rows
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead.sheets import SheetsWriter


async def main() -> None:
    ap = argparse.ArgumentParser(description="Write only the Campaign Metrics tab")
    ap.add_argument("--month", default="auto")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the rows instead of writing to the sheet")
    args = ap.parse_args()

    today = datetime.now(timezone.utc)
    start_dt, end_dt, month_name = cm.get_reporting_range(args.month, today)
    ms_str, end_str = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    week_start = (today.replace(day=max(1, today.day - 7))).strftime("%Y-%m-%d")
    yest_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[Metrics] range {ms_str} -> {end_str} ({month_name})")
    print(f"[Metrics] clients: {', '.join(sorted(CAMPAIGN_METRICS_CLIENTS))}")

    rows: list[dict] = []
    # Case-insensitive: Windows upper-cases env var names, Linux does not, so
    # the same SMARTLEAD_API_KEY_Darlean yields "DARLEAN" locally and "Darlean"
    # on Render. See the note in run.py.
    accounts = [a for a in discover_accounts()
                if a.name.upper() in CAMPAIGN_METRICS_CLIENTS]
    if not accounts:
        print("[Metrics] no matching accounts — check CAMPAIGN_METRICS_CLIENTS")

    for acc in accounts:
      # Isolated per account: one account's outage must not cost every other
      # account's rows in the same run.
      try:
        async with SmartleadClient(acc.api_key, acc.name) as slc:
            campaigns = await slc.list_campaigns()
            print(f"[Metrics] {acc.name}: {len(campaigns)} campaign(s) to evaluate")
            for camp in campaigns:
                cid = str(camp.get("id") or "")
                if not cid:
                    continue
                # Drafts are dropped before any network call — they can never
                # qualify, and an account carries dozens of them.
                status = str(camp.get("status", "")).upper()
                if status in cm._INACTIVE_STATUSES or status.startswith("DRAFT"):
                    continue
                if cm.is_excluded_campaign_name(camp.get("name", ""),
                                                CAMPAIGN_METRICS_EXCLUDE):
                    continue
                # A failed fetch is not "zero activity". Skip the campaign and
                # say so, rather than writing a fabricated 0 into the sheet.
                try:
                    wk = await slc.get_analytics_by_date(cid, week_start, today.strftime("%Y-%m-%d"))
                    week_sent = int(float(wk.get("sent_count", 0) or 0))
                    mo = await slc.get_analytics_by_date(cid, ms_str, end_str)
                    month_sent = int(float(mo.get("sent_count", 0) or 0))
                    month_replies = int(float(mo.get("reply_count", 0) or 0))
                    yd = await slc.get_analytics_by_date(cid, yest_str, yest_str)
                    yest_sent = int(float(yd.get("sent_count", 0) or 0))
                    analytics = await slc.get_campaign_analytics(cid)
                    leads = await slc.get_campaign_leads(cid)
                except Exception as exc:  # noqa: BLE001
                    print(f"  [!] {cid} ({camp.get('name','')}) skipped — fetch failed: {exc}")
                    continue
                # Filter after fetching, so the lead counts are available to it.
                summary = cm.smartlead_summary_from_analytics(analytics)
                if not cm.should_include_smartlead_campaign(summary, week_sent, month_sent):
                    continue
                rows.append(cm.smartlead_metric_row(
                    summary, leads, month_replies, 0,
                    today, SMARTLEAD_POSITIVE_CATEGORY_IDS, client=acc.name,
                    month_sent=month_sent, start_dt=start_dt, end_dt=end_dt,
                    yest_sent=yest_sent,
                    launch_date=str(camp.get("created_at", ""))[:10]))
      except Exception as exc:  # noqa: BLE001
        print(f"[Metrics] Smartlead account {acc.name} failed to list "
              f"campaigns — its rows are MISSING this run: {exc}")

    for ws in discover_heyreach_workspaces():
        if CAMPAIGN_METRICS_CLIENTS and ws.name.upper() not in CAMPAIGN_METRICS_CLIENTS:
            continue
        try:
            async with HeyReachClient(ws.api_key, ws.name) as hrc:
                camps = await hrc.list_campaigns()
                print(f"[Metrics] HeyReach {ws.name}: {len(camps)} campaign(s)")
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
                        oa, om, oy, leads = {}, {}, {}, []
                    if not cm.should_include_heyreach_campaign(camp, (om or {}).get("byDayStats", {}) or {}):
                        continue
                    rows.append(cm.heyreach_metric_row(
                        camp, oa, om, leads, today, client=ws.name,
                        start_dt=start_dt, end_dt=end_dt, overall_yesterday=oy))
        except Exception as exc:  # noqa: BLE001
            print(f"[!] HeyReach workspace {ws.name} failed: {exc}")

    rows.extend(await build_expandi_rows(today, start_dt))

    if not rows:
        print("[Metrics] no campaigns qualified — nothing written.")
        return

    out = cm.rows_with_totals(rows)
    print(f"\n[Metrics] {len(rows)} campaign(s) -> {len(out)} row(s)\n")
    print(f"  {'client':10} {'campaign':40} {'plat':10} {'total':>6} {'yest':>5} {'sent':>6} {'pos_y':>6}")
    for r in out:
        print(f"  {str(r.get('client','')):10} {str(r.get('campaign',''))[:40]:40} "
              f"{str(r.get('platform','')):10} {str(r.get('total_leads','')):>6} "
              f"{str(r.get('leads_added_yesterday','')):>5} {str(r.get('msg_sent','')):>6} "
              f"{str(r.get('positive_responses_yesterday','')):>6}")

    if args.dry_run:
        print("\n[Metrics] --dry-run: sheet not written.")
        return
    writer = SheetsWriter(CAMPAIGN_METRICS_SHEET_ID)
    writer.write_campaign_metrics(out, month_name=month_name)
    # `rows`, not `out`: `out` already carries subtotal/total rows, which the
    # per-client writer regenerates for each tab.
    writer.write_campaign_metrics_per_client(rows, month_name=month_name)
    # No-ops unless CAMPAIGN_METRICS_CLIENT_SHEETS names a spreadsheet.
    SheetsWriter.write_campaign_metrics_client_sheets(rows, month_name=month_name)
    print(f"\n[Metrics] written to sheet {CAMPAIGN_METRICS_SHEET_ID}")


if __name__ == "__main__":
    asyncio.run(main())
