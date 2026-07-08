#!/usr/bin/env python3
"""Bounce auto-protection sweep (plan §5.4). Separate cron; dry-run by default.

Ensures every ACTIVE campaign has Smartlead's "High Bounce Rate Auto
Protection" set (bounce_autopause_threshold, default 3%): the platform then
pauses the campaign itself the moment bounces spike — no human in the loop.

DRY-RUN unless BOUNCE_PROTECT_ENABLED=true. Only sends the single
bounce_autopause_threshold field (partial update), capped per run.
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

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import (
    BOUNCE_PROTECT_ENABLED, BOUNCE_PROTECT_THRESHOLD, BOUNCE_PROTECT_PER_RUN_CAP,
)


def _threshold_missing(campaign: dict) -> bool:
    """True when the campaign has no (or a zeroed) bounce auto-pause threshold."""
    raw = campaign.get("bounce_autopause_threshold")
    if raw in (None, "", 0, "0"):
        return True
    try:
        return float(raw) <= 0
    except (TypeError, ValueError):
        return True


async def _plan_for_account(acc) -> list[dict]:
    plans: list[dict] = []
    async with SmartleadClient(acc.api_key, acc.name) as c:
        campaigns = [
            camp for camp in await c.list_campaigns()
            if str(camp.get("status", "")).upper() == "ACTIVE"
        ]
        ids = [str(camp["id"]) for camp in campaigns]
        details = await c.fetch_all_campaign_details(ids)
    by_id = {str(d.get("id")): d for d in details}
    for camp in campaigns:
        cid = str(camp["id"])
        detail = by_id.get(cid, camp)
        if _threshold_missing(detail):
            plans.append({
                "client": acc.name, "campaign_id": cid,
                "campaign_name": camp.get("name", ""),
                "current": detail.get("bounce_autopause_threshold"),
            })
    return plans


async def main() -> None:
    accounts = discover_accounts()
    plans: list[dict] = []
    for acc in accounts:
        try:
            plans.extend(await _plan_for_account(acc))
        except Exception as exc:  # noqa: BLE001
            print(f"  [BounceProtect] {acc.name}: plan failed: {exc}")

    print(f"[BounceProtect] {len(plans)} ACTIVE campaign(s) missing bounce auto-pause "
          f"(target threshold {BOUNCE_PROTECT_THRESHOLD}%)"
          f"{' (DRY-RUN - not applying)' if not BOUNCE_PROTECT_ENABLED else ''}")
    for p in plans[:40]:
        print(f"    {p['client']:14} {p['campaign_id']:>8}  {p['campaign_name'][:48]:48} "
              f"current={p['current']!r}")
    if len(plans) > 40:
        print(f"    ...and {len(plans) - 40} more")

    if not BOUNCE_PROTECT_ENABLED:
        print("[BounceProtect] BOUNCE_PROTECT_ENABLED=false -> dry-run complete. "
              "Set true to apply.")
        return

    todo = plans[:BOUNCE_PROTECT_PER_RUN_CAP]
    if len(plans) > len(todo):
        print(f"[BounceProtect] capping to {len(todo)} this run "
              f"(BOUNCE_PROTECT_PER_RUN_CAP={BOUNCE_PROTECT_PER_RUN_CAP})")
    acc_by_client = {a.name: a for a in accounts}
    applied = failed = 0
    for p in todo:
        acc = acc_by_client.get(p["client"])
        if not acc:
            failed += 1
            continue
        async with SmartleadClient(acc.api_key, acc.name) as c:
            try:
                await c.update_campaign_settings(
                    p["campaign_id"],
                    {"bounce_autopause_threshold": str(BOUNCE_PROTECT_THRESHOLD)},
                )
                applied += 1
                print(f"  [BounceProtect] set {BOUNCE_PROTECT_THRESHOLD}% on "
                      f"{p['client']} / {p['campaign_name']}")
            except Exception as exc:  # noqa: BLE001
                print(f"  [BounceProtect] failed for {p['campaign_id']}: {exc}")
                failed += 1
    print(f"[BounceProtect] applied {applied}, failed {failed}.")


if __name__ == "__main__":
    asyncio.run(main())
