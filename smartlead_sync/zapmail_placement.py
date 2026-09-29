#!/usr/bin/env python3
"""Placement-test fleet CLI (read-only status; gated test creation).

Read-only:
    python zapmail_placement.py --status
    python zapmail_placement.py --eligible --limit 50
    python zapmail_placement.py --report <cart_order_id>

Create (dry-run unless --approve AND ZAPMAIL_ALLOW_SPEND=true):
    python zapmail_placement.py --run id1,id2,id3 --type ONE_TIME
    python zapmail_placement.py --run id1,id2,id3 --approve
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

from smartlead.zapmail import ZapmailSpendBlocked
from smartlead.zapmail_placement import (
    eligible_mailboxes, placement_status, run_test, test_report,
)


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Zapmail placement-test fleet")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--eligible", action="store_true")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--report", help="cart_order_id (read-only)")
    ap.add_argument("--run", help="Comma-separated mailbox ids to test")
    ap.add_argument("--name", default="Fleet placement test")
    ap.add_argument("--type", default="ONE_TIME", choices=["ONE_TIME", "MONTHLY"])
    ap.add_argument("--seeds", default="google")
    ap.add_argument("--approve", action="store_true")
    ap.add_argument("--client", default="")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.status:
        res = await placement_status(client=args.client or None)
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.eligible:
        res = await eligible_mailboxes(limit=args.limit, client=args.client or None)
        if args.json:
            print(json.dumps(res))
        else:
            boxes = ((res or {}).get("data") or {}).get("mailboxes") or []
            for m in boxes:
                print(f"  {m.get('email', '?'):40} {m.get('status', ''):12} "
                      f"warmed={m.get('isWarmedUp')} id={m.get('id', '')}")
        return 0

    if args.report:
        res = await test_report(args.report, client=args.client or None)
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.run:
        try:
            res = await run_test(
                _split(args.run), test_name=args.name,
                seeds=tuple(_split(args.seeds)) or ("google",),
                placement_type=args.type, approve=args.approve,
                client=args.client or None)
        except ZapmailSpendBlocked as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
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