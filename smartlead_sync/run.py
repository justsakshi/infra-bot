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
from smartlead.config import TEST_TAB_NAME, ACCOUNT_DELIVERABILITY_TABS, MASTER_TAB_NAME
from smartlead.sheets import DeliverabilityReader, SheetsWriter


# ── Per-account pipeline ─────────────────────────────────────────────────────

async def process_account(
    api_key: str,
    sheet_id: str,
    account_name: str,
    deliverability_map: dict[str, str],
    active_only: bool = True,
) -> list[dict]:
    """Fetch data for one Smartlead account and push it to Google Sheets."""
    print(f"\n{'=' * 60}")
    print(f"  Account: {account_name}")
    print(f"{'=' * 60}")

    async with SmartleadClient(api_key, account_name) as client:
        inbox_data, campaign_summary, warmup_data = await fetch_account_data(
            client, deliverability_map, active_only=active_only,
        )

    if not inbox_data and not campaign_summary:
        print(f"  [!] No data for {account_name} - skipping sheet update.")
        return []

    # Console preview
    _print_tables(account_name, inbox_data, campaign_summary, warmup_data)

    # Sync to Google Sheets
    try:
        writer = SheetsWriter(sheet_id, account_name)
        writer.write_all(campaign_summary, inbox_data, warmup_data)
    except Exception as exc:
        print(f"  [!] Sheet sync failed for {account_name}: {exc}")

    return inbox_data


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
    parser.add_argument("--all", action="store_true", help="Include ALL campaigns (default: active/paused only)")
    parser.add_argument("--sheet", type=str, default=None, help="Google Sheet name (for mock mode)")
    args = parser.parse_args()
    active_only = not args.all

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
            rows = await process_account(acc.api_key, acc.sheet_id, acc.name, deliverability_map, active_only)
            rows_by_sheet.setdefault(acc.sheet_id, []).extend(rows)
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

    print(f"\n[*] All accounts processed. Finished at {end_time}")


if __name__ == "__main__":
    asyncio.run(main())
