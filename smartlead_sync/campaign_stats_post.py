#!/usr/bin/env python3
"""Post the campaign stats to Slack: #pl_daily, 10:15 IST, Mon-Fri.

The last working day's email and LinkedIn numbers per client (a Monday post
covers Friday), plus the week Friday to Thursday on Fridays and last month on
the month's first working day; each day's campaigns in the thread. Counted
from the platforms' dated records at posting time (smartlead.daily_stats),
not from the 09:00 snapshots. Decided with Manveen, 29 Oct and 2 Nov 2026.

    python campaign_stats_post.py --dry-run          print it, post nothing
    python campaign_stats_post.py --to U09TQ9D7YNM   post to one person (a test)
    python campaign_stats_post.py --post             post to CAMPAIGN_STATS_CHANNEL
    python campaign_stats_post.py --today 2026-11-06 the post that day would make
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead import campaign_history as ch
from smartlead import campaign_metrics as cm
from smartlead import daily_stats as ds
from smartlead.config import ACTIVE_CLIENTS, CAMPAIGN_STATS_CHANNEL
from smartlead.expandi_accounts import discover_expandi_workspaces
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead.notify import post_with_thread
from smartlead.stats_post import build_post, part_as_text


def not_counted(rows: list[dict] = ()) -> list[str]:
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
        accepts = any(c.get("accepted") is not None for r in rows if r.get("platform") == "Waalaxy"
                      for c in r["counts"].values())
        out.append("Precise Leads LinkedIn invites (Waalaxy has no invite count; accepts "
                   "only from campaigns with the CRM Sync step)" if accepts else
                   "Precise Leads LinkedIn invites and accepts (Waalaxy has no invite count; "
                   "accepts are logged only for campaigns with a CRM Sync step, and none has one yet)")
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

    today = date.fromisoformat(args.today or ch.snapshot_day())
    periods = ds.periods_for(today)
    rows, failed = asyncio.run(ds.collect(periods))
    clients = sorted({cm.display_client(c) for c in ACTIVE_CLIENTS})
    linkedin = tuple({cm.display_client(w.name) for w in discover_expandi_workspaces()}
                     | {cm.display_client(w.name) for w in discover_heyreach_workspaces()}
                     | ({"Precise Leads"} if "Precise Leads" in clients else set()))
    head, thread = build_post(rows, periods, clients, not_counted(rows), failed, linkedin,
                              {"Precise Leads": ds.WAALAXY_LOG_START})
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
