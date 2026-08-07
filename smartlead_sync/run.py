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
from datetime import datetime, timedelta, timezone

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
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import (
    TEST_TAB_NAME, ACCOUNT_DELIVERABILITY_TABS, MASTER_TAB_NAME,
    CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_SHEET_ID, CAMPAIGN_METRICS_EXCLUDE,
    SMARTLEAD_POSITIVE_CATEGORY_IDS,
)
from smartlead.sheets import DeliverabilityReader, SheetsWriter
from smartlead.expandi_rows import build_expandi_rows
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead import campaign_metrics as cm
from datetime import date as _date
from smartlead.health import build_health_rows, health_records_for_store
from smartlead.health_store import HealthStore
from smartlead.manager_map import resolve_manager
from smartlead import notify as _notify


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

    # Drop old/inactive clients' inboxes (health, master tab, warmup rows)
    before = len(inbox_data)
    inbox_data = [r for r in inbox_data if not is_excluded_inbox(r)]
    warmup_data = [r for r in warmup_data if not is_excluded_inbox(r)]
    if before != len(inbox_data):
        print(f"  [filter] excluded {before - len(inbox_data)} old-client inbox row(s)")

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
    # Metrics fetch campaigns independently. The full inbox pipeline can take
    # several minutes or fail on one mailbox; that must not erase Smartlead
    # rows while HeyReach still reaches the shared Campaign Metrics tab.
    # Match case-insensitively. Windows upper-cases environment variable names,
    # Linux does not, so SMARTLEAD_API_KEY_Darlean yields an account called
    # "DARLEAN" locally but "Darlean" on Render. A case-sensitive comparison
    # therefore passed every local test and silently dropped Darlean from the
    # metrics tab in production — HeyReach rows appeared, Smartlead rows did not.
    smartlead_accounts_for_metrics = [
        acc for acc in accounts if acc.name.upper() in CAMPAIGN_METRICS_CLIENTS
    ]

    # Process each account sequentially with its own deliverability data
    failed_accounts: list = []
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
            # Overlay any newer API placement results (no-op until phase-3 tests run)
            from smartlead.placement_merge import apply_api_results
            deliverability_map = apply_api_results(deliverability_map)
            rows, campaigns = await process_account(acc.api_key, acc.sheet_id, acc.name, deliverability_map, active_only)
            rows_by_sheet.setdefault(acc.sheet_id, []).extend(rows)
        except Exception as exc:
            print(f"[!] Account {acc.name} failed: {exc}")
            failed_accounts.append(acc)

    # Write shared tabs once (glossary + last sync)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        shared_writer = SheetsWriter(accounts[0].sheet_id)
        shared_writer.write_glossary()
        shared_writer.write_sync_timestamp(end_time)
    except Exception as exc:
        print(f"[!] Shared tabs failed: {exc}")

    # One 'All Inboxes' master tab per distinct sheet (per workspace).
    # A failed account must NOT silently vanish from the master tab — downstream
    # tools (precise-automator / Campaign Desk) read it live, and a missing
    # client reads as "0 inboxes exist" (real incident 2026-07-10: MYTHIC 401 +
    # a transient BW failure left the tab with only Darlean + PL, and Campaign
    # Desk showed 'No eligible FREE inboxes' for Belardi Wong). Salvage the
    # failed clients' rows from the tab's previous contents: one-day-stale data
    # beats a client disappearing.
    failed_by_sheet: dict[str, list[str]] = {}
    for facc in failed_accounts:
        failed_by_sheet.setdefault(facc.sheet_id, []).append(facc.name)
    for sheet_id, rows in rows_by_sheet.items():
        try:
            writer = SheetsWriter(sheet_id)
            stale_clients = failed_by_sheet.get(sheet_id, [])
            if stale_clients:
                from smartlead.sheets import read_master_inboxes
                salvaged = read_master_inboxes(sheet_id, stale_clients)
                if salvaged:
                    rows = rows + salvaged
                    print(f"[!] Salvaged {len(salvaged)} stale row(s) for failed "
                          f"account(s) {stale_clients} from the previous master tab")
                else:
                    print(f"[!] WARNING: account(s) {stale_clients} failed and no "
                          "previous master rows found — they will be missing from "
                          f"'{MASTER_TAB_NAME}' until the next successful sync")
            writer.write_master_inboxes(rows)
            print(f"[*] Master tab '{MASTER_TAB_NAME}' written to sheet {sheet_id} from {len(rows)} campaign-rows")
        except Exception as exc:
            print(f"[!] Master inbox tab failed for sheet {sheet_id}: {exc}")

    # ── Inbox Health workbook (score + trend + action + manager) ─────────────
    try:
        today_d = _date.today()
        store = HealthStore()
        all_health_rows: list[dict] = []
        health_rows_by_sheet: dict[str, list[dict]] = {}
        for sheet_id, rows in rows_by_sheet.items():
            hrows = build_health_rows(rows, today_d, store, resolve_manager)
            for hr in hrows:
                hr["_mgr_slack"] = resolve_manager(hr.get("sub_client") or hr["client"]).get("slack", "")
            health_rows_by_sheet[sheet_id] = hrows
            all_health_rows.extend(hrows)
            try:
                SheetsWriter(sheet_id).write_inbox_health(hrows)
            except Exception as exc:
                print(f"[!] Inbox Health tab failed for sheet {sheet_id}: {exc}")
        saved = store.save_daily(
            [rec for hrows in health_rows_by_sheet.values()
             for rec in health_records_for_store(hrows, today_d)]
        )
        print(f"[*] Inbox Health: {len(all_health_rows)} inboxes scored, {saved} history records saved")
        try:
            primary = accounts[0].sheet_id
            sheet_url = f"https://docs.google.com/spreadsheets/d/{primary}"
            digest = _notify.build_digest(all_health_rows, sheet_url)
            if _notify.post_digest(digest, _notify.build_full_lists(all_health_rows)):
                print("[*] Inbox Health Slack digest posted")
        except Exception as exc:
            print(f"[!] Inbox Health notify failed: {exc}")
    except Exception as exc:
        print(f"[!] Inbox Health workbook failed: {exc}")

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
        yest_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        for acc in smartlead_accounts_for_metrics:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                campaigns = await slc.list_campaigns()
                for camp in campaigns:
                    cid = str(camp.get("id") or "")
                    if not cid:
                        continue
                    # Drafts and de-scoped verticals never qualify — skip them
                    # before spending API calls on their analytics.
                    status = str(camp.get("status", "")).upper()
                    if status in cm._INACTIVE_STATUSES or status.startswith("DRAFT"):
                        continue
                    if cm.is_excluded_campaign_name(camp.get("name", ""),
                                                    CAMPAIGN_METRICS_EXCLUDE):
                        continue
                    # A failed fetch is not "zero activity" — skip the campaign
                    # rather than writing a fabricated 0 into the sheet.
                    try:
                        week_an = await slc.get_analytics_by_date(cid, week_start_str, today.strftime("%Y-%m-%d"))
                        week_sent = int(float(week_an.get("sent_count", 0) or 0))
                        m_an = await slc.get_analytics_by_date(cid, ms_str, end_str)
                        month_sent = int(float(m_an.get("sent_count", 0) or 0))
                        month_replies = int(float(m_an.get("reply_count", 0) or 0))
                        y_an = await slc.get_analytics_by_date(cid, yest_str, yest_str)
                        yest_sent = int(float(y_an.get("sent_count", 0) or 0))
                        analytics = await slc.get_campaign_analytics(cid)
                        leads = await slc.get_campaign_leads(cid)
                    except Exception as exc:
                        print(f"  [metrics] Smartlead campaign {cid} "
                              f"({camp.get('name','')}) skipped — fetch failed: {exc}")
                        continue
                    # Filter after fetching, so lead counts are available to it.
                    summary = cm.smartlead_summary_from_analytics(analytics)
                    if not cm.should_include_smartlead_campaign(summary, week_sent, month_sent):
                        continue
                    metric_rows.append(cm.smartlead_metric_row(
                        summary, leads, month_replies, 0, today, SMARTLEAD_POSITIVE_CATEGORY_IDS,
                        client=acc.name,
                        month_sent=month_sent, start_dt=reporting_start, end_dt=reporting_end,
                        yest_sent=yest_sent,
                        launch_date=str(camp.get("created_at", ""))[:10]))

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
                            oy = await hrc.get_overall_stats(
                                cid, start=f"{yest_str}T00:00:00Z",
                                end=f"{yest_str}T23:59:59Z")
                            leads = await hrc.get_campaign_leads(cid)
                        except Exception as exc:
                            print(f"  [metrics] HeyReach campaign {cid} failed: {exc}")
                            oa, om, oy, leads = {}, {}, {}, []
                        by_day = (om or {}).get("byDayStats", {}) or {}
                        if not cm.should_include_heyreach_campaign(camp, by_day):
                            continue
                        metric_rows.append(cm.heyreach_metric_row(
                            camp, oa, om, leads, today, client=ws.name,
                            start_dt=reporting_start, end_dt=reporting_end,
                            overall_yesterday=oy))
            except Exception as exc:
                print(f"  [metrics] HeyReach workspace {ws.name} failed: {exc}")

        metric_rows.extend(await build_expandi_rows(
            today, reporting_start, log="  [metrics]"))

        if metric_rows:
            campaign_count = len(metric_rows)
            campaign_rows = metric_rows  # keep the pre-totals rows for the per-client tabs
            metric_rows = cm.rows_with_totals(metric_rows)
            writer = SheetsWriter(CAMPAIGN_METRICS_SHEET_ID)
            writer.write_campaign_metrics(metric_rows, month_name=month_name)
            writer.write_campaign_metrics_per_client(campaign_rows, month_name=month_name)
            # No-ops unless CAMPAIGN_METRICS_CLIENT_SHEETS names a spreadsheet.
            SheetsWriter.write_campaign_metrics_client_sheets(
                campaign_rows, month_name=month_name)
            clients = sorted({str(r.get("client", "")) for r in metric_rows if r.get("client")})
            print(f"[*] Campaign Metrics tab written: {campaign_count} campaigns "
                  f"across {len(clients)} client(s): {', '.join(clients)}")
    except Exception as exc:
        print(f"[!] Campaign Metrics dashboard failed: {exc}")

    print(f"\n[*] All accounts processed. Finished at {end_time}")


if __name__ == "__main__":
    asyncio.run(main())
