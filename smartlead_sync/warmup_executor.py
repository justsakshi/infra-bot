#!/usr/bin/env python3
"""Auto-warmup executor (health phase 4). Separate cron; dry-run by default.

Rule (docs/DELIVERABILITY_MASTER_PLAN.md §1): warmup is ALWAYS ON; the planner
assigns each inbox a state profile (NEW 40/ramp, ACTIVE-campaign 20+auto-adjust,
IDLE 20, RECOVERING 15+reply-rate-boost) and this executor applies whatever the
change dict carries — unless WARMUP_AUTO_ENABLED is False (dry-run: log only).
"""
from __future__ import annotations

import asyncio
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from datetime import date, timedelta

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import WARMUP_AUTO_ENABLED
from smartlead.processing import fetch_account_data
from smartlead.warmup_planner import plan_warmup_changes
from smartlead.campaign_freshness import is_campaign_stale, STALE_DAYS
from smartlead.client_filter import is_excluded_inbox


async def _stale_campaign_names(c: SmartleadClient, today: date) -> set[str]:
    """Return the names of ACTIVE campaigns that are stale (dead 14d+)."""
    stale: set[str] = set()
    start = (today - timedelta(days=STALE_DAYS)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    for camp in await c.list_campaigns():
        if str(camp.get("status", "")).upper() != "ACTIVE":
            continue
        cid = str(camp["id"])
        try:
            leads = await c.get_campaign_leads(cid)
            newest = max((l.get("created_at", "") for l in leads), default="")
            an = await c.get_analytics_by_date(cid, start, end)
            sent14 = int(float(an.get("sent_count", 0) or 0))
        except Exception as exc:  # noqa: BLE001
            print(f"  [Warmup] freshness check failed for {cid}: {exc}")
            continue
        if is_campaign_stale(newest, sent14, today):
            stale.add(camp.get("name", ""))
    return stale


async def _raw_inbox_rows(acc, today: date) -> list[dict]:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, {}, active_only=False)
        stale_names = await _stale_campaign_names(c, today)
    inbox = [r for r in inbox if not is_excluded_inbox(r)]  # drop old-client inboxes
    for r in inbox:
        r.setdefault("client", acc.name)
        # stamp campaign_is_stale so the planner can 3-way decide
        r["campaign_is_stale"] = r.get("campaign_name", "") in stale_names
    if stale_names:
        print(f"  [Warmup] {acc.name}: {len(stale_names)} stale ACTIVE campaign(s): "
              f"{', '.join(list(stale_names)[:5])}")
    return inbox


async def main() -> None:
    accounts = discover_accounts()
    all_changes: list[dict] = []
    acc_by_client = {a.name: a for a in accounts}
    today = date.today()

    for acc in accounts:
        try:
            rows = await _raw_inbox_rows(acc, today)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Warmup] rows for {acc.name} failed: {exc}")
            continue
        changes = plan_warmup_changes(rows)
        # dedupe by (account_id) so multiple campaign-rows of one inbox -> one change
        seen: set = set()
        for ch in changes:
            key = (ch["client"], ch["email"])
            if key in seen:
                continue
            seen.add(key)
            all_changes.append(ch)

    counts = {a: sum(1 for c in all_changes if c["action"] == a)
              for a in ("enable", "retune", "boost", "disable", "trickle")}
    print(f"[Warmup] {len(all_changes)} change(s): {counts['enable']} enable, "
          f"{counts['retune']} retune, {counts['boost']} boost, "
          f"{counts['disable']} disable, {counts['trickle']} trickle"
          f"{' (DRY-RUN - not applying)' if not WARMUP_AUTO_ENABLED else ''}")
    for c in all_changes[:60]:
        print(f"    {c['action']:7} {c['client']:14} {c['email']:34} {c['reason']}")
    if len(all_changes) > 60:
        print(f"    ...and {len(all_changes) - 60} more")

    if not WARMUP_AUTO_ENABLED:
        print("[Warmup] WARMUP_AUTO_ENABLED=false -> dry-run complete. Set true to apply.")
        return

    applied = failed = 0
    for c in all_changes:
        acc = acc_by_client.get(c["client"])
        if not acc or not c.get("account_id"):
            failed += 1
            continue
        async with SmartleadClient(acc.api_key, acc.name) as sl:
            try:
                # every change carries its full profile from the planner
                await sl.set_warmup(
                    str(c["account_id"]),
                    enabled=(c["action"] != "disable"),
                    total_per_day=c.get("per_day", 40),
                    daily_rampup=c.get("rampup", 0),
                    reply_rate=c.get("reply_rate", 25),
                    auto_adjust=c.get("auto_adjust"),
                )
                applied += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  [Warmup] {c['action']} failed for {c['email']}: {exc}")
                failed += 1
    print(f"[Warmup] applied {applied}, failed {failed}.")


if __name__ == "__main__":
    asyncio.run(main())
