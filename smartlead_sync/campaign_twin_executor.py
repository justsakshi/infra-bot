#!/usr/bin/env python3
"""Create an open-tracking twin of an existing Smartlead campaign.

The twin copies sequences, schedule, settings and sender inboxes, flips
email-open tracking, and is left UNSTARTED. Both campaigns are renamed with a
visible " · open tracking" / " · no open tracking" marker. The pair is
recorded in Mongo (campaign_twins) so the staggering uploader can route the
first leads to the tracked copy and the rest to the untracked one.

Usage:
    python3 campaign_twin_executor.py --account "Belardi Wong" --campaign 3375601
    python3 campaign_twin_executor.py --account "Belardi Wong" --campaign 3375601 --dry-run
    python3 campaign_twin_executor.py --list --account "Belardi Wong"     # campaigns + tracking
    python3 campaign_twin_executor.py --accounts                          # account names

Add --json for one machine-readable line on stdout (the dashboard uses this).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.campaign_twin import create_twin, has_open_tracking, strip_tag
from smartlead.config import HEALTH_HISTORY_DB

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None

TWINS_COLLECTION = os.getenv("CAMPAIGN_TWINS_COLLECTION", "campaign_twins")


def _find_account(name: str):
    accounts = discover_accounts()
    for a in accounts:
        if a.name.lower() == name.lower():
            return a
    known = ", ".join(a.name for a in accounts)
    raise SystemExit(f"ERROR: no Smartlead account named '{name}'. Known: {known}")


def _record(summary: dict, account: str, created_by: str) -> bool:
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [Twin] Mongo unavailable - pair not recorded.", file=sys.stderr)
        return False
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client.get_default_database()
        if db is None:
            db = client[HEALTH_HISTORY_DB]
        db[TWINS_COLLECTION].update_one(
            {"account": account, "source_id": summary["source_id"]},
            {"$set": {**summary, "account": account, "created_by": created_by,
                      "created_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [Twin] Mongo write failed: {exc}", file=sys.stderr)
        return False


async def _list(account_name: str, as_json: bool) -> int:
    acc = _find_account(account_name)
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = await c.list_campaigns()
    rows = []
    for camp in camps:
        # list_campaigns does not include track_settings; fetch per campaign
        # would cost N calls, so report name-tag state and leave the detail
        # lookup to the twin call itself.
        rows.append({
            "id": camp.get("id"), "name": camp.get("name"),
            "status": camp.get("status"),
            "tagged": strip_tag(camp.get("name") or "") != (camp.get("name") or ""),
        })
    if as_json:
        print(json.dumps({"account": acc.name, "campaigns": rows}))
    else:
        for r in rows:
            print(f"  {r['id']:>9}  {r['status']:<9} {r['name']}")
    return 0


async def _twin(account_name: str, campaign_id: str, dry_run: bool,
                keep_source_name: bool, created_by: str, as_json: bool) -> int:
    acc = _find_account(account_name)
    async with SmartleadClient(acc.api_key, acc.name) as c:
        summary = await create_twin(c, campaign_id, dry_run=dry_run,
                                    rename_source=not keep_source_name)
    summary["account"] = acc.name
    summary["recorded"] = False if dry_run else _record(summary, acc.name, created_by)

    if as_json:
        print(json.dumps(summary))
        return 0

    verb = "Would create" if dry_run else "Created"
    print(f"{verb} twin of {summary['source_id']} on {acc.name}")
    print(f"  source : {summary['source_name']}  "
          f"(open tracking {'ON' if summary['source_open_tracking'] else 'OFF'})")
    print(f"  twin   : {summary['twin_name']}  "
          f"(open tracking {'ON' if summary['twin_open_tracking'] else 'OFF'})"
          + ("" if dry_run else f"  id={summary['twin_id']}"))
    print(f"  copied : {summary['steps']} sequence step(s), "
          f"{summary['email_accounts']} sender inbox(es)")
    if not dry_run:
        print("  The twin is NOT started. bounce_autopause_threshold is not "
              "readable via API - set it by hand if the source had one.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Create an open-tracking twin campaign")
    ap.add_argument("--account", help="Smartlead account name (see --accounts)")
    ap.add_argument("--campaign", help="Source campaign id")
    ap.add_argument("--dry-run", action="store_true", help="Plan only; create nothing")
    ap.add_argument("--keep-source-name", action="store_true",
                    help="Do not add the tracking tag to the source campaign's name")
    ap.add_argument("--created-by", default="", help="Who asked (for the record)")
    ap.add_argument("--list", action="store_true", help="List campaigns on --account")
    ap.add_argument("--accounts", action="store_true", help="List account names")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.accounts:
        names = [a.name for a in discover_accounts()]
        print(json.dumps({"accounts": names}) if args.json else "\n".join(names))
        return 0
    if not args.account:
        ap.error("--account is required")
    if args.list:
        return asyncio.run(_list(args.account, args.json))
    if not args.campaign:
        ap.error("--campaign is required")
    return asyncio.run(_twin(args.account, args.campaign, args.dry_run,
                             args.keep_source_name, args.created_by, args.json))


if __name__ == "__main__":
    raise SystemExit(main())
