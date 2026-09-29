#!/usr/bin/env python3
"""Renewal + tag maintenance CLI (read-only preview; gated actions).

Read-only:
    python zapmail_maintenance.py --renewals

Actions (dry-run unless --approve):
    python zapmail_maintenance.py --renew id1,id2 --approve
    python zapmail_maintenance.py --auto-renew id1,id2 --enable --approve
    python zapmail_maintenance.py --ensure-tag "Q4 batch" --approve
    python zapmail_maintenance.py --assign-tag <tag_id> id1,id2 --approve
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

from smartlead.zapmail import ZapmailError, ZapmailSpendBlocked
from smartlead.zapmail_maintenance import (
    assign_tag, ensure_tag, renew_now, renewals_preview, set_auto_renew,
)


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Zapmail renewal/tag maintenance")
    ap.add_argument("--renewals", action="store_true", help="Preview expiring domains (read-only)")
    ap.add_argument("--renew", help="Comma-separated domain ids to renew")
    ap.add_argument("--auto-renew", help="Comma-separated domain ids")
    ap.add_argument("--enable", action="store_true")
    ap.add_argument("--disable", action="store_true")
    ap.add_argument("--ensure-tag", help="Tag name to find or create")
    ap.add_argument("--color", default="#625B97")
    ap.add_argument("--assign-tag", help="Tag id to assign")
    ap.add_argument("--domains", default="", help="Domain ids for --assign-tag")
    ap.add_argument("--approve", action="store_true")
    ap.add_argument("--client", default="")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.renewals:
        rows = await renewals_preview(client=args.client or None)
        if args.json:
            print(json.dumps(rows))
        else:
            print(f"  {'EXPIRE':12} {'DOMAIN':30} {'RENEW':>9}")
            for r in rows:
                price = r.get("renew_price") or "?"
                print(f"  {r['expire_on']:12} {str(r['domain']):30} {str(price):>9}")
        return 0

    if args.renew:
        try:
            res = await renew_now(_split(args.renew), approve=args.approve,
                                  client=args.client or None)
        except ZapmailSpendBlocked as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        except ZapmailError as exc:
            print(f"ERROR: {exc} — check --renewals before retrying "
                  "(a timeout may still have renewed).", file=sys.stderr)
            return 1
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.auto_renew:
        enabled = True
        if args.disable:
            enabled = False
        if not args.enable and not args.disable:
            print("ERROR: pass --enable or --disable", file=sys.stderr)
            return 2
        res = await set_auto_renew(_split(args.auto_renew), enabled,
                                   approve=args.approve, client=args.client or None)
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.ensure_tag:
        res = await ensure_tag(args.ensure_tag, color=args.color,
                               approve=args.approve, client=args.client or None)
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.assign_tag:
        res = await assign_tag(args.assign_tag, _split(args.domains),
                               approve=args.approve, client=args.client or None)
        print(json.dumps(res, indent=None if args.json else 2, default=str))
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