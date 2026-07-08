#!/usr/bin/env python3
"""Per-domain reply-rate early warning (daily, read-only).

Pulls per-mailbox stats for every ACTIVE campaign, aggregates sent/replies
per SENDING DOMAIN, and alerts when a domain's reply rate falls >30% below
its own trailing baseline (leading indicator — fires ~48h before opens/
bounces move) or breaks the 1% rule.

Endpoint verified LIVE on 2026-07-08 (Task 4 Step 1) — see
smartlead/api.py::get_campaign_mailbox_statistics and
smartlead/reply_stats.py for the exact path + field names checked.

IMPORTANT: Smartlead's mailbox-statistics endpoint returns CUMULATIVE
campaign-lifetime totals, not daily counts. This job stores one cumulative
snapshot per domain per day, then computes DAILY DELTAS by diffing
consecutive snapshots before handing anything to evaluate_alerts() — summing
raw cumulative snapshots across days (the original design) would n-count
the same sends and badly distort the baseline. A domain's first-ever
snapshot has no prior cumulative to diff against, so it contributes zero
delta (not a false spike) until the next run."""
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


def _yesterdays_cumulative(col, dom: str, yesterday: str) -> dict:
    """Most recent stored cumulative snapshot for `dom` on or before
    `yesterday`. {} if none on record (first-ever run for this domain)."""
    if col is None:
        return {}
    try:
        doc = col.find_one({"domain": dom, "date": {"$lte": yesterday}},
                           sort=[("date", -1)])
        return doc or {}
    except PyMongoError as exc:
        print(f"  [ReplyMon] Mongo read failed for {dom}: {exc}")
        return {}


async def main() -> None:
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    fleet = await _fleet_domain_stats()
    print(f"[ReplyMon] collected stats for {len(fleet)} domain(s)")

    col = _col()

    # evaluate_alerts needs DAILY deltas, not Smartlead's cumulative
    # campaign-lifetime totals (raw `fleet` values). Convert each domain's
    # cumulative snapshot into today's delta by diffing against the most
    # recent stored cumulative snapshot, BEFORE writing today's own
    # cumulative snapshot (order matters: read prior state first).
    alerts_total = 0
    baseline_start = (date.today() - timedelta(days=14)).isoformat()
    for dom, cur in sorted(fleet.items()):
        prior = _yesterdays_cumulative(col, dom, yesterday)
        delta_sent = max(0, cur["sent"] - int(prior.get("sent", 0) or 0))
        delta_replies = max(0, cur["replies"] - int(prior.get("replies", 0) or 0))
        # first-ever snapshot for this domain: no prior cumulative to diff
        # against, so there's no meaningful "today's activity" yet -> skip
        # alerting on it (avoids a false alert seeded by lifetime totals).
        current_delta = ({"sent": delta_sent, "replies": delta_replies}
                         if prior else {"sent": 0, "replies": 0})

        history: list[dict] = []
        if col is not None:
            try:
                history = list(col.find(
                    {"domain": dom, "date": {"$gte": baseline_start, "$lte": yesterday}},
                    sort=[("date", 1)]))
            except PyMongoError as exc:
                print(f"  [ReplyMon] Mongo read failed for {dom}: {exc}")
        # history docs are cumulative snapshots too -> diff consecutive pairs
        # into daily deltas so evaluate_alerts sees the same units as `cur`.
        daily_history: list[dict] = []
        for prev_doc, next_doc in zip(history, history[1:]):
            daily_history.append({
                "sent": max(0, int(next_doc.get("sent", 0) or 0) - int(prev_doc.get("sent", 0) or 0)),
                "replies": max(0, int(next_doc.get("replies", 0) or 0) - int(prev_doc.get("replies", 0) or 0)),
            })

        for alert in evaluate_alerts(dom, current_delta, daily_history):
            alerts_total += 1
            print(f"  [ALERT] {dom} ({cur.get('client', '')}): {alert}")

    if col is not None and fleet:
        # store today's CUMULATIVE snapshot (as Smartlead reports it) after
        # alert evaluation, so tomorrow's diff-against-prior sees today's raw
        # totals — daily_history is derived on read, not stored pre-diffed.
        ops = [UpdateOne({"domain": d, "date": today},
                         {"$set": {"domain": d, "date": today, **s}}, upsert=True)
               for d, s in fleet.items()]
        try:
            col.bulk_write(ops, ordered=False)
        except PyMongoError as exc:
            print(f"  [ReplyMon] Mongo write failed: {exc}")

    if alerts_total == 0:
        print("[ReplyMon] OK - no reply-rate alerts.")
    else:
        print(f"[ReplyMon] {alerts_total} alert(s) - check the domains above "
              "(pause/investigate per OPERATOR_PLAYBOOK daily P0 flow).")


if __name__ == "__main__":
    asyncio.run(main())
