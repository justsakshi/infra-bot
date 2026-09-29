#!/usr/bin/env python3
"""Register / list Zapmail webhooks, so Zapmail pushes changes to Infrabot.

    python zapmail_webhooks.py --list                          # read-only
    python zapmail_webhooks.py --register https://<host>/webhooks/zapmail/<account> \
        --client "Precise Leads" --approve                     # WRITE (free)

One endpoint per Zapmail account. Zapmail returns the signing secret ONCE:
put it in the environment as ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT> (e.g.
ZAPMAIL_WEBHOOK_SECRET_PRECISE_LEADS, or ZAPMAIL_WEBHOOK_SECRET for the plain
ZAPMAIL_API_KEY account) — zapmail_webhooks.js refuses unsigned or
wrongly-signed deliveries. The <account> in the URL is that same suffix in
lower case ("precise_leads", or "primary").
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


async def main() -> int:
    from smartlead.zapmail import ZapmailClient
    from smartlead.zapmail_accounts import discover_zapmail_accounts, open_client

    ap = argparse.ArgumentParser(description="Zapmail webhook endpoints")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--register", metavar="URL")
    ap.add_argument("--client", default="", help="Client whose Zapmail account to register on")
    ap.add_argument("--approve", action="store_true")
    args = ap.parse_args()

    if args.list:
        for acc in discover_zapmail_accounts():
            async with ZapmailClient(api_key=acc.api_key) as z:
                eps = ((await z.list_webhook_endpoints()) or {}).get("data") or []
            print(f"  {acc.name}: {len(eps)} endpoint(s)")
            for e in eps:
                print(f"    {e.get('status'):8} {e.get('url')}  events={e.get('enabled_events')}")
        return 0

    if args.register:
        url = args.register.strip()
        if not url.startswith("https://"):
            print("ERROR: webhook URL must be https://", file=sys.stderr)
            return 2
        events = list(ZapmailClient.WEBHOOK_EVENTS)
        if not args.approve:
            print(f"DRY RUN — would register {url} for {events} on {args.client!r}'s "
                  "Zapmail account. Re-run with --approve.")
            return 0
        async with open_client(args.client or None) as z:
            res = await z.create_webhook_endpoint(url, events, approve=True)
        data = (res or {}).get("data") or {}
        print(json.dumps({k: data.get(k) for k in ("id", "url", "status", "enabled_events")}))
        if data.get("secret"):
            print("\n  SIGNING SECRET (shown once — store it now as "
                  "ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT> on Render):")
            print(f"  {data['secret']}")
        return 0

    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
