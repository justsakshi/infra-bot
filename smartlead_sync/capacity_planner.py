#!/usr/bin/env python3
"""Capacity planner (read-only, Mondays). Demand vs healthy supply vs bench
per client -> ORDER advisories with 4-6-week lead time. Also maintains the
domain_registry (first-seen date per domain) for future age-gating.

No enable flag: this job only reads Smartlead and writes the advisory tab
+ Mongo registry — it never touches inbox or campaign settings."""
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
from smartlead.capacity import compute_client_capacity
from smartlead.campaign_freshness import stale_campaign_names
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import (
    HEALTH_HISTORY_DB, DOMAIN_REGISTRY_COLLECTION, DEFAULT_SHEET_ID,
    ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME,
)
from smartlead.processing import fetch_account_data, get_domain_from_email
from smartlead.sheets import SheetsWriter, DeliverabilityReader, _dedupe_inbox_rows

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None


async def _demand_per_day(c: SmartleadClient, today: date) -> float:
    """7-day average of actual sends across ACTIVE, non-stale campaigns."""
    start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    stale = await stale_campaign_names(c, today)
    total = 0.0
    for camp in await c.list_campaigns():
        if str(camp.get("status", "")).upper() != "ACTIVE":
            continue
        if camp.get("name", "") in stale:
            continue
        try:
            an = await c.get_analytics_by_date(str(camp["id"]), start, end)
            total += float(an.get("sent_count", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Capacity] analytics failed for {camp.get('id')}: {exc}")
    return total / 7.0


def _register_domains(rows: list[dict], today: str) -> int:
    """Upsert first-seen dates. Returns how many domains are on record."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [Capacity] Mongo unavailable - domain registry skipped.")
        return 0
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][DOMAIN_REGISTRY_COLLECTION]
        col.create_index("domain", unique=True)
        ops = []
        for r in rows:
            dom = get_domain_from_email(str(r.get("email", "")))
            if not dom:
                continue
            ops.append(UpdateOne(
                {"domain": dom},
                {"$setOnInsert": {"domain": dom, "first_seen": today},
                 "$set": {"client": r.get("client", ""), "last_seen": today}},
                upsert=True))
        if ops:
            col.bulk_write(ops, ordered=False)
        return col.count_documents({})
    except Exception as exc:  # noqa: BLE001
        print(f"  [Capacity] domain registry failed: {exc}")
        return 0


def _churn_per_month(client_rows: list[dict]) -> int:
    """V1 placeholder: proper churn needs 30d of health history; until the
    history accumulates, use count of currently-broken inboxes (failed test
    or blocked) as the replacement-rate proxy. Revisit after 30 days."""
    broken = sum(1 for r in client_rows
                 if str(r.get("test_sheet_status", "")).lower() in {"fail", "spam"}
                 or str(r.get("warmup_state", "")).lower() == "blocked")
    return broken


async def _deliverability_map(acc) -> dict[str, dict]:
    """Same pattern as retest_executor._health_rows_for: merge every tab
    configured for this client so test_sheet_status reflects real placement
    results instead of defaulting to 'Unknown' for every inbox."""
    dmap: dict[str, dict] = {}
    for tab in ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME]):
        try:
            dmap.update(await DeliverabilityReader(tab_name=tab).fetch())
        except Exception as exc:  # noqa: BLE001
            print(f"  [Capacity] deliverability read {tab} failed: {exc}")
    return dmap


async def main() -> None:
    today = date.today()
    out_rows: list[dict] = []
    all_rows: list[dict] = []
    for acc in discover_accounts():
        try:
            dmap = await _deliverability_map(acc)
            async with SmartleadClient(acc.api_key, acc.name) as c:
                inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
                demand = await _demand_per_day(c, today)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Capacity] {acc.name} failed: {exc}")
            continue
        inbox = [r for r in inbox if not is_excluded_inbox(r)]
        for r in inbox:
            r.setdefault("client", acc.name)
        deduped = _dedupe_inbox_rows(inbox)
        all_rows.extend(deduped)
        churn = _churn_per_month(deduped)
        cap = compute_client_capacity(deduped, demand, churn)
        cap["client"] = acc.name
        cap["churn_per_month"] = churn
        out_rows.append(cap)
        print(f"  [Capacity] {acc.name}: {cap['status']} — demand {cap['demand_per_day']}/d, "
              f"capacity {cap['safe_capacity']}/d, bench {cap['bench']}/{cap['bench_target']}, "
              f"order {cap['order_domains']} domain(s) by {cap['order_by'] or '—'}")

    registered = _register_domains(all_rows, today.isoformat())
    print(f"[Capacity] domain registry: {registered} domain(s) on record")

    try:
        writer = SheetsWriter(DEFAULT_SHEET_ID)
        writer.write_capacity(out_rows)
    except Exception as exc:  # noqa: BLE001
        print(f"[Capacity] Sheets write failed: {exc}")
    print(f"[Capacity] done: {len(out_rows)} client(s).")


if __name__ == "__main__":
    asyncio.run(main())
