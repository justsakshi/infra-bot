#!/usr/bin/env python3
"""Inboxes whose warm-up a vendor runs outside Smartlead (t248, 18 Nov 2026). Logic: smartlead/external_warmup.py.

    python external_warmup_cli.py list                      # how many marks per vendor (no addresses)
    python external_warmup_cli.py mark --file <emails.txt> --vendor InboxKit --why "<why>"            # dry run
    python external_warmup_cli.py mark --file <emails.txt> --vendor InboxKit --why "<why>" --apply    # Mongo
    python external_warmup_cli.py assets --file <emails.txt> --vendor Inboxkit --client "<client>" \\
        --bought 2026-11-17 --received 2026-11-18 --bill-day 2026-12-17                              # dry run
    python external_warmup_cli.py assets ... --apply        # insert the missing asset tracker rows

``--file`` holds one inbox address (or a whole domain, for ``mark``) per line. It stays outside the repo (public):
addresses are read at run time and never printed, only counted. ``mark`` writes only Mongo external_warmup;
``assets`` reads Smartlead (one paged inbox list per account) and inserts only rows the tracker lacks, through the
syncs' own write (zapmail_asset_sync.apply). Nothing here writes to Smartlead.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import external_warmup as xw


def read_keys(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return [x.strip().lower() for x in f if x.strip() and not x.strip().startswith("#")]


def _col():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    if db is None:
        raise SystemExit("Mongo unreachable (MONGO_URI); nothing done")
    return db[xw.COLLECTION]


def cmd_list(_args) -> int:
    marks = xw.load(_col())
    by = Counter(marks.values())
    print(f"{len(marks)} marks" + "".join(f"\n  {v}: {n}" for v, n in sorted(by.items())))
    return 0


def cmd_mark(args) -> int:
    keys = sorted(set(read_keys(args.file)))
    print(f"{len(keys)} marks for {xw.vendor_label(args.vendor)} "
          f"({sum('@' in k for k in keys)} inboxes, {sum('@' not in k for k in keys)} domains)")
    if not args.apply:
        print("dry run: nothing written (add --apply)")
        return 0
    n = xw.mark(_col(), keys, vendor=xw.vendor_label(args.vendor), since=date.today().isoformat(),
                who=args.who, why=args.why)
    print(f"written: {n}; now {len(xw.load(_col()))} marks in total")
    return 0


async def _accounts(emails: set[str]) -> list[dict]:
    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    out = []
    for acc in discover_accounts():
        async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
            out += [a for a in await sl.list_email_accounts() if str(a.get("from_email") or "").lower() in emails]
    return out


def cmd_assets(args) -> int:
    from smartlead import vendor_assets as va
    from smartlead.zapmail_asset_sync import apply, read_tracker
    emails = {k for k in read_keys(args.file) if "@" in k}
    tracker = read_tracker()
    if not tracker:
        raise SystemExit("asset tracker unreachable (Mongo); nothing planned")
    accounts = asyncio.run(_accounts(emails))
    print(f"{len(emails)} inboxes in the file, {len(accounts)} found in Smartlead")
    if len(accounts) != len(emails):
        print("not every inbox was found in Smartlead: nothing written")
        return 1
    plan = va.plan_rows(accounts, tracker, vendor=args.vendor, client=args.client,
                        bought=date.fromisoformat(args.bought), received=date.fromisoformat(args.received),
                        bill_day=date.fromisoformat(args.bill_day), kind=args.kind, why=args.why)
    print("planned:", plan["counts"], "| workspaces:", dict(Counter(o["doc"]["workspace"] for o in plan["ops"])))
    if not args.apply:
        print("dry run: nothing written (add --apply)")
        return 0
    print(f"written: {apply(plan['ops'])} rows")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    m = sub.add_parser("mark")
    m.add_argument("--file", required=True)
    m.add_argument("--vendor", required=True)
    m.add_argument("--why", required=True)
    m.add_argument("--who", default="external_warmup_cli")
    m.add_argument("--apply", action="store_true")
    a = sub.add_parser("assets")
    for k in ("--file", "--vendor", "--client", "--bought", "--received", "--bill-day"):
        a.add_argument(k, required=True)
    a.add_argument("--kind", default="Prewarmed")
    a.add_argument("--why", default="external_warmup_cli")
    a.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)
    return {"list": cmd_list, "mark": cmd_mark, "assets": cmd_assets}[args.cmd](args)


if __name__ == "__main__":
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    raise SystemExit(main())
