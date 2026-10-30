#!/usr/bin/env python3
"""Post the daily campaign stats to Slack: #pl_daily, 10:30 IST, Mon-Fri.

Reads the Campaign History tab that metrics_only.py fills at 09:00 IST and
posts each client's email and LinkedIn numbers since the last post (Friday's
for a Monday post), with every campaign in the thread. Decided with Manveen,
29 Oct 2026: #pl_daily, 30 minutes before the 11:00 standup, weekdays.

    python campaign_stats_post.py --dry-run          print it, post nothing
    python campaign_stats_post.py --to U09TQ9D7YNM   post to one person (a test)
    python campaign_stats_post.py --post             post to CAMPAIGN_STATS_CHANNEL

If today's numbers are missing it posts a short "not ready" notice instead
(silence would read as "no activity") and exits 1.
"""
from __future__ import annotations

import argparse
import os
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead import campaign_history as ch
from smartlead import campaign_metrics as cm
from smartlead.config import (ACTIVE_CLIENTS, CAMPAIGN_HISTORY_TAB_NAME,
                              CAMPAIGN_METRICS_SHEET_ID, CAMPAIGN_STATS_CHANNEL)
from smartlead.expandi_accounts import discover_expandi_workspaces
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead.notify import _post, post_with_thread
from smartlead.sheets import SheetsWriter
from smartlead.stats_post import build_post, not_ready_text, part_as_text


def not_counted() -> list[str]:
    """Active clients' LinkedIn numbers this server cannot read, and why."""
    active = {cm._norm_client(c) for c in ACTIVE_CLIENTS}
    out = []
    if "BETTRDATA" in active and not any(
            cm._norm_client(ws.name) == "BETTRDATA" for ws in discover_expandi_workspaces()):
        out.append("BettrData LinkedIn (no Expandi key on the server)")
    if "MELIOR" in active and not any(
            cm._norm_client(ws.name) == "MELIOR" for ws in discover_heyreach_workspaces()):
        out.append("Melior LinkedIn (no HeyReach key on the server)")
    if "PRECISE LEADS" in active:
        out.append("Precise Leads LinkedIn (Waalaxy has no API)")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Post the daily campaign stats")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--post", action="store_true", help="post to CAMPAIGN_STATS_CHANNEL")
    where.add_argument("--to", help="post to this Slack user or channel id instead")
    where.add_argument("--dry-run", action="store_true", help="print only (the default)")
    ap.add_argument("--today", help="YYYY-MM-DD to report on (default: today in IST)")
    args = ap.parse_args()

    target = args.to or (CAMPAIGN_STATS_CHANNEL if args.post else "")
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if target and not token:
        print("[stats-post] SLACK_BOT_TOKEN missing - cannot post.")
        return 1

    writer = SheetsWriter(CAMPAIGN_METRICS_SHEET_ID)
    ws = writer._get_or_create_shared_tab(CAMPAIGN_HISTORY_TAB_NAME, rows=1000,
                                          cols=len(ch.HEADER))
    history = ch.parse_history(ws.get_all_values())
    today = args.today or ch.snapshot_day()
    days = sorted({r["date"] for r in history})

    if today not in days:
        text = not_ready_text(today, days[-1] if days else None)
        print(text)
        if target:
            _post(token, target, text)
        return 1

    base = ch.baseline_day(days, today)
    url = f"https://docs.google.com/spreadsheets/d/{CAMPAIGN_METRICS_SHEET_ID}/edit#gid={ws.id}"
    head, thread = build_post(ch.campaign_changes(history, today, base), today, base,
                              url, not_counted())
    print(head)
    for part in thread:
        print("\n--- thread ---\n" + part_as_text(part))
    if not target:
        return 0
    if not post_with_thread(token, target, [head], thread):
        print("[stats-post] Slack post failed.")
        return 1
    print(f"[stats-post] posted to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
