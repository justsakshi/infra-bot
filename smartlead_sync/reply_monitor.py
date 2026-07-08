#!/usr/bin/env python3
"""Per-domain reply-rate early warning (daily, read-only).

Pulls per-mailbox stats for every ACTIVE campaign, aggregates sent/replies
per SENDING DOMAIN, stores a daily record in Mongo, and alerts when a
domain's reply rate falls >30% below its own trailing baseline (leading
indicator — fires ~48h before opens/bounces move) or breaks the 1% rule.

Endpoint verified LIVE on 2026-07-08 (Task 4 Step 1) — see
smartlead/api.py::get_campaign_mailbox_statistics and
smartlead/reply_stats.py for the exact path + field names checked."""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import HEALTH_HISTORY_DB, REPLY_STATS_COLLECTION
from smartlead.reply_stats import aggregate_domain_stats, evaluate_alerts

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None


def _col():
    """Connect to the reply-stats collection. Returns None (graceful no-op)
    if MONGO_URI is unset, pymongo isn't installed, or the connection/ping
    fails — matches smartlead/health_store.py's fail-open pattern so a
    Mongo outage never breaks the (more important) live API collection."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [ReplyMon] Mongo unavailable (no MONGO_URI) - history disabled.")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][REPLY_STATS_COLLECTION]
        col.create_index([("domain", 1), ("date", 1)], unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [ReplyMon] Mongo connect failed ({exc}) - history disabled.")
        return None


async def _fleet_domain_stats() -> dict[str, dict]:
    """{domain: {sent, replies, client}} across all ACTIVE campaigns."""
    fleet: dict[str, dict] = {}
    for acc in discover_accounts():
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                camps = [x for x in await c.list_campaigns()
                         if str(x.get("status", "")).upper() == "ACTIVE"]
                for camp in camps:
                    rows = await c.get_campaign_mailbox_statistics(str(camp["id"]))
                    for dom, s in aggregate_domain_stats(rows).items():
                        d = fleet.setdefault(dom, {"sent": 0, "replies": 0,
                                                   "client": acc.name})
                        d["sent"] += s["sent"]
                        d["replies"] += s["replies"]
        except Exception as exc:  # noqa: BLE001
            print(f"  [ReplyMon] {acc.name} failed: {exc}")
    return fleet


async def main() -> None:
    today = date.today().isoformat()
    fleet = await _fleet_domain_stats()
    print(f"[ReplyMon] collected stats for {len(fleet)} domain(s)")

    col = _col()
    if col is not None and fleet:
        ops = [UpdateOne({"domain": d, "date": today},
                         {"$set": {"domain": d, "date": today, **s}}, upsert=True)
               for d, s in fleet.items()]
        try:
            col.bulk_write(ops, ordered=False)
        except PyMongoError as exc:
            print(f"  [ReplyMon] Mongo write failed: {exc}")

    baseline_start = (date.today() - timedelta(days=14)).isoformat()  # 14d baseline window
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    alerts_total = 0
    for dom, cur in sorted(fleet.items()):
        history: list[dict] = []
        if col is not None:
            try:
                history = list(col.find({"domain": dom,
                                         "date": {"$gte": baseline_start, "$lte": yesterday}}))
            except PyMongoError as exc:
                print(f"  [ReplyMon] Mongo read failed for {dom}: {exc}")
        for alert in evaluate_alerts(dom, cur, history):
            alerts_total += 1
            print(f"  [ALERT] {dom} ({cur.get('client', '')}): {alert}")

    if alerts_total == 0:
        print("[ReplyMon] OK - no reply-rate alerts.")
    else:
        print(f"[ReplyMon] {alerts_total} alert(s) - check the domains above "
              "(pause/investigate per OPERATOR_PLAYBOOK daily P0 flow).")


if __name__ == "__main__":
    asyncio.run(main())
