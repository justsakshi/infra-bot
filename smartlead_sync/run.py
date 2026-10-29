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
from datetime import datetime

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
)
from smartlead.sheets import DeliverabilityReader, SheetsWriter
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
            # A failure before any tab write (API down, bad key) never reaches
            # the tab writer, so record it here or the ledger would show the
            # account as simply absent rather than failed.
            from smartlead.sheets import _record_write
            _record_write(f"{acc.name} - (account sync)", error=exc)

    # Glossary is static; Last Sync is written at the very END of the run (see
    # the sync verdict below) so its ledger covers every tab this run touches.
    try:
        SheetsWriter(accounts[0].sheet_id).write_glossary()
    except Exception as exc:
        print(f"[!] Glossary tab failed: {exc}")

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

    # Campaign Metrics and Campaign History are written by metrics_only.py
    # (09:00 IST job in index.js), before the 10:30 stats post that reads them.
    # This run used to repeat the same ~24-minute pull at its end.

    # ── Sync verdict ─────────────────────────────────────────────────────────
    # Written last, so the Last Sync ledger covers every tab above. Until
    # 2026-09-16 the timestamp was written mid-run and unconditionally: it
    # advanced every morning while account tabs failed silently for weeks.
    # A failed tab now (1) shows on the Last Sync tab with its error, (2) is
    # printed here with a [SYNC] marker, (3) posts to Slack, and (4) makes the
    # process exit 2 so index.js logs it as a failure rather than "finished".
    from smartlead.sheets import sync_ledger_summary
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        SheetsWriter(accounts[0].sheet_id).write_sync_timestamp(end_time)
    except Exception as exc:
        print(f"[!] Last Sync tab failed: {exc}")
    ok_tabs, failed_tabs = sync_ledger_summary()
    print(f"\n[*] All accounts processed. Finished at {end_time}")
    if failed_tabs:
        total = len(ok_tabs) + len(failed_tabs)
        print(f"[SYNC] FAILED: {len(failed_tabs)} of {total} tab(s) NOT written:")
        for e in failed_tabs:
            print(f"[SYNC]   {e['tab']}: {e['error']}")
        try:
            _notify.post_digest(_notify.sync_failure_text(failed_tabs, total))
        except Exception as exc:
            print(f"[!] Sync-failure Slack alert failed: {exc}")
        sys.exit(2)
    print(f"[SYNC] OK: all {len(ok_tabs)} tab(s) written")


if __name__ == "__main__":
    asyncio.run(main())
