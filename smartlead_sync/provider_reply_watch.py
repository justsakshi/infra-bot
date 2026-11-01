#!/usr/bin/env python3
"""Are Microsoft and Proofpoint recipients still getting our mail? Weekly, from real sends.

Seed placement tests missed this: BettrData's sender scored Office365 7/7 inbox on
2026-09-10, while in the same campaigns Microsoft recipients answered at 0.11x the
Google rate (measured 2026-11-01). Rule and thresholds: smartlead/provider_watch.py.

    python provider_reply_watch.py                  print it, post nothing (default)
    python provider_reply_watch.py --to U09TQ9D7YNM post to one person (a test)
    python provider_reply_watch.py --post           post to PROVIDER_WATCH_CHANNEL

Read-only against Smartlead (campaign list + statistics) and DNS; never pauses or
changes anything. Not scheduled: adding it to cron is a separate decision.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import httpx

from smartlead import provider_watch as pw
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.notify import _post

LIVE_STATUSES = {"ACTIVE", "PAUSED", "COMPLETED"}
DOH = "https://dns.google/resolve"
IST = timezone(timedelta(hours=5, minutes=30))


def _why(exc: Exception, key: str) -> str:
    """Status and Smartlead's message only: httpx's own text holds the URL, and the URL holds the key."""
    if isinstance(exc, httpx.HTTPStatusError):
        msg = exc.response.text[:80].replace(key, "<key>") if key else exc.response.text[:80]
        return f"HTTP {exc.response.status_code} {msg}".strip()
    return type(exc).__name__


async def read_account(acc, today: date, days: int) -> dict[str, dict[str, dict]]:
    oldest = today - timedelta(days=days + 120)   # a campaign this old that isn't ACTIVE can't have sent lately
    out: dict[str, dict[str, dict]] = {}
    async with SmartleadClient(acc.api_key, acc.name) as c:
        for camp in await c.list_campaigns() or []:
            status = str(camp.get("status") or "").upper()
            created = str(camp.get("created_at") or "")[:10]
            if status not in LIVE_STATUSES or (status != "ACTIVE" and created and created < oldest.isoformat()):
                continue
            leads = pw.lead_events(await c.get_campaign_statistics(str(camp["id"])), today, days)
            if leads:
                out[str(camp["id"])] = leads
    return out


async def mx_providers(domains: set[str]) -> dict[str, str]:
    sem = asyncio.Semaphore(12)
    out: dict[str, str] = {}

    async def one(client: httpx.AsyncClient, d: str) -> None:
        async with sem:
            try:
                r = await client.get(DOH, params={"name": d, "type": "MX"}, timeout=15)
                data = r.json() if r.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                data = {}
            if data.get("Status", 2) not in (0, 3):
                out[d] = "unknown"
                return
            hosts = [str(a.get("data", "")).split(" ", 1)[-1] for a in data.get("Answer") or []
                     if a.get("type") == 15]
            out[d] = pw.provider_from_mx(hosts)

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(one(client, d) for d in sorted(domains)))
    return out


async def build(today: date, days: int) -> str:
    blocks = []
    for acc in discover_accounts():
        try:
            camps = await read_account(acc, today, days)
        except Exception as exc:  # one dead account (e.g. "Plan expired!") must not hide the others
            blocks.append(pw.client_lines(acc.name.replace("_", " "), None, _why(exc, acc.api_key)))
            continue
        domains = {e.split("@", 1)[1] for leads in camps.values() for e in leads}
        providers = await mx_providers(domains)
        blocks.append(pw.client_lines(acc.name.replace("_", " "), pw.judge(camps, lambda d: providers.get(d, "unknown"))))
    return pw.report_text(blocks, today, days)


def main() -> int:
    ap = argparse.ArgumentParser(description="Microsoft / Proofpoint reach from real sends")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--post", action="store_true", help="post to PROVIDER_WATCH_CHANNEL")
    where.add_argument("--to", help="post to this Slack user or channel id instead")
    where.add_argument("--dry-run", action="store_true", help="print only (the default)")
    ap.add_argument("--today", help="YYYY-MM-DD (default: today in IST)")
    ap.add_argument("--days", type=int, default=pw.WINDOW_DAYS, help="window of sends, days")
    args = ap.parse_args()

    target = args.to or (os.getenv("PROVIDER_WATCH_CHANNEL", "") if args.post else "")
    if args.post and not target:
        print("[provider-watch] PROVIDER_WATCH_CHANNEL is not set - nothing posted.")
        return 1
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if target and not token:
        print("[provider-watch] SLACK_BOT_TOKEN missing - cannot post.")
        return 1

    today = date.fromisoformat(args.today) if args.today else datetime.now(IST).date()
    text = asyncio.run(build(today, args.days))
    print(text)
    if not target:
        return 0
    if not _post(token, target, text):
        print("[provider-watch] Slack post failed.")
        return 1
    print(f"[provider-watch] posted to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
