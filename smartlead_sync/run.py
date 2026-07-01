#!/usr/bin/env python3
"""Smartlead Dashboard -> Google Sheets  (CLI entry-point).

Usage:
    python get_smartlead_inboxes.py                            # real data, all accounts
    python get_smartlead_inboxes.py --mock                     # fake data (no API calls)
    python get_smartlead_inboxes.py --mock --sheet "My Sheet"  # mock -> named sheet
"""

from __future__ import annotations

import asyncio
import argparse
import sys
import os
from datetime import datetime, timezone

# Fix Windows console encoding for Polars unicode table output
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import polars as pl

# Use ASCII tables so output works on any terminal
pl.Config.set_tbl_formatting("ASCII_FULL")

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.mock import get_mock_data
from smartlead.processing import fetch_account_data
from smartlead.config import (
    TEST_TAB_NAME, ACCOUNT_DELIVERABILITY_TABS, MASTER_TAB_NAME,
    CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_SHEET_ID, SMARTLEAD_POSITIVE_CATEGORY_IDS,
)
from smartlead.sheets import DeliverabilityReader, SheetsWriter
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead import campaign_metrics as cm


# ── Per-account pipeline ─────────────────────────────────────────────────────

async def process_account(
    api_key: str,
    sheet_id: str,
    account_name: str,
    deliverability_map: dict[str, str],
    active_only: bool = False,
) -> tuple[list[dict], list[dict]]:
    """Fetch data for one Smartlead account and push it to Google Sheets.

    Returns ``(inbox_data, campaign_summary)``.
    """
    print(f"\n{'=' * 60}")
    print(f"  Account: {account_name}")
    print(f"{'=' * 60}")

    async with SmartleadClient(api_key, account_name) as client:
        inbox_data, campaign_summary, warmup_data = await fetch_account_data(
            client, deliverability_map, active_only=active_only,
        )

    if not inbox_data and not campaign_summary:
        print(f"  [!] No data for {account_name} - skipping sheet update.")
        return [], []

    # Console preview
    _print_tables(account_name, inbox_data, campaign_summary, warmup_data)

    # Sync to Google Sheets
    try:
        writer = SheetsWriter(sheet_id, account_name)
        writer.write_all(campaign_summary, inbox_data, warmup_data)
    except Exception as exc:
        print(f"  [!] Sheet sync failed for {account_name}: {exc}")

    return inbox_data, campaign_summary


def _print_tables(
    name: str,
    inbox_data: list[dict],
    campaign_summary: list[dict],
    warmup_data: list[dict],
) -> None:
    print(f"\n  --- {name} -Campaign Summary ---")
    print(pl.DataFrame(campaign_summary).sort("name"))

    print(f"\n  --- {name} -Inboxes ---")
    print(pl.DataFrame(inbox_data).sort("campaign_name"))

    if warmup_data:
        print(f"\n  --- {name} -Warmup Reputation ---")
        print(pl.DataFrame(warmup_data).sort("email"))


# ── CLI ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Smartlead Dashboard -> Google Sheets")
    parser.add_argument("--mock", action="store_true", help="Use mock data (no API calls)")
    parser.add_argument("--active-only", action="store_true", help="Only process active/paused campaigns (default: process all)")
    parser.add_argument("--sheet", type=str, default=None, help="Google Sheet name (for mock mode)")
    parser.add_argument("--month", type=str, default="auto", help="Reporting month (e.g. 'June', 'previous', 'current', or 'auto' [defaults to previous month on days 1-5])")
    args = parser.parse_args()
    active_only = args.active_only

    if args.mock:
        inbox_data, campaign_summary, warmup_data = get_mock_data()
        _print_tables("Mock", inbox_data, campaign_summary, warmup_data)
        print("\n[mock] Done. Pass real API keys to sync to Google Sheets.")
        return

    # Discover accounts
    accounts = discover_accounts()
    if not accounts:
        print("[!] No SMARTLEAD_API_KEY* found in .env -nothing to do.")
        sys.exit(1)

    sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[*] Sync started at {sync_time}")
    print(f"[*] {len(accounts)} account(s) discovered.")

    # Group inbox rows by the sheet they belong to, so each workspace/sheet
    # gets its own consolidated master tab (a separate-workspace client on its
    # own sheet gets an isolated 'All Inboxes'; clients sharing a sheet share one).
    rows_by_sheet: dict[str, list[dict]] = {}
    # (account, campaign_summary) pairs for accounts included in the metrics tab
    smartlead_campaigns_for_metrics: list = []

    # Process each account sequentially with its own deliverability data
    for acc in accounts:
        try:
            tab_names = ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME])
            deliverability_map: dict[str, str] = {}
            for tab_name in tab_names:
                reader = DeliverabilityReader(tab_name=tab_name)
                tab_map = await reader.fetch()
                # Merge across tabs: fail wins; otherwise keep the most-recent test.
                # Each value is {"status": str, "date": "YYYY-MM-DD"|""}.
                for domain, entry in tab_map.items():
                    status, date = entry.get("status", ""), entry.get("date", "")
                    existing = deliverability_map.get(domain)
                    existing_status = existing.get("status") if existing else None
                    if status == "fail" or existing_status == "fail":
                        fail_date = date if status == "fail" else (existing or {}).get("date", "")
                        deliverability_map[domain] = {"status": "fail", "date": fail_date}
                    elif status and (not existing or date >= existing.get("date", "")):
                        deliverability_map[domain] = {"status": status, "date": date}
            rows, campaigns = await process_account(acc.api_key, acc.sheet_id, acc.name, deliverability_map, active_only)
            rows_by_sheet.setdefault(acc.sheet_id, []).extend(rows)
            if acc.name in CAMPAIGN_METRICS_CLIENTS:
                smartlead_campaigns_for_metrics.append((acc, campaigns))
        except Exception as exc:
            print(f"[!] Account {acc.name} failed: {exc}")

    # Write shared tabs once (glossary + last sync)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        shared_writer = SheetsWriter(accounts[0].sheet_id)
        shared_writer.write_glossary()
        shared_writer.write_sync_timestamp(end_time)
    except Exception as exc:
        print(f"[!] Shared tabs failed: {exc}")

    # One 'All Inboxes' master tab per distinct sheet (per workspace)
    for sheet_id, rows in rows_by_sheet.items():
        try:
            SheetsWriter(sheet_id).write_master_inboxes(rows)
            print(f"[*] Master tab '{MASTER_TAB_NAME}' written to sheet {sheet_id} from {len(rows)} campaign-rows")
        except Exception as exc:
            print(f"[!] Master inbox tab failed for sheet {sheet_id}: {exc}")

    # ── Campaign Metrics dashboard (Smartlead + HeyReach) ────────────────────
    try:
        today = datetime.now(timezone.utc)
        reporting_start, reporting_end, month_name = cm.get_reporting_range(args.month, today)
        ms_str = reporting_start.strftime("%Y-%m-%d")
        end_str = reporting_end.strftime("%Y-%m-%d")
        print(f"[*] Reporting range for metrics: {ms_str} to {end_str} ({month_name})")
        metric_rows: list[dict] = []

        # Smartlead rows (DARLEAN account(s))
        week_start_str = (today.replace(day=max(1, today.day - 7))).strftime("%Y-%m-%d")
        for acc, campaigns in smartlead_campaigns_for_metrics:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                for camp in campaigns:
                    cid = str(camp.get("campaign_id") or camp.get("id") or "")
                    if not cid:
                        continue
                    try:
                        week_an = await slc.get_analytics_by_date(cid, week_start_str, today.strftime("%Y-%m-%d"))
                        week_sent = int(float(week_an.get("sent_count", 0) or 0))
                        m_an = await slc.get_analytics_by_date(cid, ms_str, end_str)
                        month_sent = int(float(m_an.get("sent_count", 0) or 0))
                        month_replies = int(float(m_an.get("reply_count", 0) or 0))
                    except Exception as exc:
                        print(f"  [metrics] Smartlead campaign {cid} analytics failed: {exc}")
                        week_sent, month_sent, month_replies = 0, 0, 0
                    if not cm.should_include_smartlead_campaign(camp, week_sent, month_sent):
                        continue
                    try:
                        leads = await slc.get_campaign_leads(cid)
                    except Exception as exc:
                        print(f"  [metrics] Smartlead campaign {cid} leads failed: {exc}")
                        leads = []
                    metric_rows.append(cm.smartlead_metric_row(
                        camp, leads, month_replies, 0, today, SMARTLEAD_POSITIVE_CATEGORY_IDS,
                        month_sent=month_sent, start_dt=reporting_start, end_dt=reporting_end))

        # HeyReach rows (all workspaces; currently DARLEAN)
        for ws in discover_heyreach_workspaces():
            try:
                async with HeyReachClient(ws.api_key, ws.name) as hrc:
                    for camp in await hrc.list_campaigns():
                        cid = camp["id"]
                        try:
                            oa = await hrc.get_overall_stats(cid)
                            om = await hrc.get_overall_stats(
                                cid,
                                start=reporting_start.isoformat().replace("+00:00", "Z"),
                                end=reporting_end.isoformat().replace("+00:00", "Z"),
                            )
                            leads = await hrc.get_campaign_leads(cid)
                        except Exception as exc:
                            print(f"  [metrics] HeyReach campaign {cid} failed: {exc}")
                            oa, om, leads = {}, {}, []
                        by_day = (om or {}).get("byDayStats", {}) or {}
                        if not cm.should_include_heyreach_campaign(camp, by_day):
                            continue
                        metric_rows.append(cm.heyreach_metric_row(
                            camp, oa, om, leads, today, start_dt=reporting_start, end_dt=reporting_end))
            except Exception as exc:
                print(f"  [metrics] HeyReach workspace {ws.name} failed: {exc}")

        if metric_rows:
            metric_rows.append(cm.total_row(metric_rows))
            SheetsWriter(CAMPAIGN_METRICS_SHEET_ID).write_campaign_metrics(metric_rows, month_name=month_name)
            print(f"[*] Campaign Metrics tab written: {len(metric_rows) - 1} campaigns")
    except Exception as exc:
        print(f"[!] Campaign Metrics dashboard failed: {exc}")

    print(f"\n[*] All accounts processed. Finished at {end_time}")


if __name__ == "__main__":
    asyncio.run(main())
