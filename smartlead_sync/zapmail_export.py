#!/usr/bin/env python3
"""Export Zapmail mailboxes to Smartlead (dry-run by default).

Read-only:
    python zapmail_export.py --accounts

Write (dry-run unless --approve):
    python zapmail_export.py --ensure-account user@smartlead.example
    python zapmail_export.py --export a.com,b.com
    python zapmail_export.py --export a.com,b.com --approve
    python zapmail_export.py --status 10938        # poll an export id

Export costs no new money but mutates; gates on --approve. See
docs/ZAPMAIL_QUESTIONS_2026-09.md Q7 for the SMARTLEAD credential convention.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.domain_export import (
    SMARTLEAD_APP, ensure_account, export_domain, list_accounts,
)
from smartlead.zapmail_accounts import open_client


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Export Zapmail mailboxes (dry-run default)")
    ap.add_argument("--accounts", action="store_true", help="List export accounts (read-only)")
    ap.add_argument("--ensure-account", metavar="EMAIL",
                    help="Ensure a Smartlead export account exists")
    ap.add_argument("--password", default="", help="Password/API key for the account")
    ap.add_argument("--export", help="Comma-separated domains whose mailboxes to export")
    ap.add_argument("--status", help="Poll an export id (read-only)")
    ap.add_argument("--third-party-account-id", default=None)
    ap.add_argument("--app", default=SMARTLEAD_APP)
    ap.add_argument("--approve", action="store_true")
    ap.add_argument("--client", default="")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.accounts:
        res = await list_accounts(args.app, client=args.client or None)
        if args.json:
            print(json.dumps(res))
        else:
            for a in ((res or {}).get("data") or {}).get("accounts") or []:
                print(f"  {a.get('email', '?'):40} {a.get('appName', '')} id={a.get('id', '')}")
        return 0

    if args.status:
        async with open_client(args.client or None) as z:
            res = await z.get_export_status(args.status)
        print(json.dumps(res if args.json else (res or {}).get("data") or res, indent=None if args.json else 2))
        return 0

    if args.ensure_account:
        res = await ensure_account(args.ensure_account, args.password, app=args.app,
                                   client=args.client or None, approve=args.approve)
        print(json.dumps(res, indent=None if args.json else 2))
        return 0

    if args.export:
        out = []
        for d in _split(args.export):
            out.append(await export_domain(
                d, app=args.app, client=args.client or None,
                third_party_account_id=args.third_party_account_id,
                approve=args.approve))
        if args.json:
            print(json.dumps(out))
        else:
            for r in out:
                if r.get("dry_run"):
                    sel = r["selection"]
                    print(f"  {r['domain']:30} DRY RUN → {len(sel['mailboxes'])} mailbox(es) "
                          f"to account {sel['third_party_account_id']}: "
                          f"{', '.join(sel['mailboxes'])}")
                elif r.get("ok"):
                    print(f"  {r['domain']:30} ok — export_id={r.get('export_id')} "
                          f"status={r.get('status', 'triggered')}")
                else:
                    print(f"  {r['domain']:30} FAILED — {r.get('error', r.get('status'))}")
        return 0

    ap.print_help()
    return 0


def _run() -> int:
    from smartlead.zapmail import ZapmailError
    try:
        return asyncio.run(main())
    except ZapmailError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_run())