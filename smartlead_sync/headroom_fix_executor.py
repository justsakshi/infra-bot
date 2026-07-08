#!/usr/bin/env python3
"""Warmup-headroom fix (found 2026-07-07). Separate cron; dry-run by default.

Problem: Smartlead's max_email_per_day is ONE shared bucket for warmup AND
campaign sends (confirmed via Smartlead's own API docs: "Maximum emails
allowed per day (including warmup and campaign emails)"). Every real inbox
across all 4 clients caps at <=30/day. Our ACTIVE-state warmup profile asks
for 20/day of warmup on top of the campaign's own volume — but there's no
room left in the shared bucket, so warmup gets silently squeezed to ~0 on
inboxes that are actually sending, even with warmup nominally "on."

Fix: for inboxes in a LIVE (ACTIVE + non-stale) campaign, raise
max_email_per_day to (current value + WARMUP_HEADROOM) so warmup has real
room. Only ACTIVE-state inboxes are touched — IDLE/NEW/RECOVERING already
have no competing campaign volume and don't have this conflict.

DRY-RUN unless HEADROOM_FIX_ENABLED=true. Capped per run.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import (
    HEADROOM_FIX_ENABLED, WARMUP_HEADROOM, HEADROOM_FIX_PER_RUN_CAP,
)
from smartlead.processing import fetch_account_data
from smartlead.campaign_freshness import stale_campaign_names
from smartlead.client_filter import is_excluded_inbox


def _in_live_campaign(row: dict) -> bool:
    status = str(row.get("campaign_status", "")).upper()
    if status != "ACTIVE":
        return False
    return not bool(row.get("campaign_is_stale", False))


async def _plan_for_account(acc, today: date) -> list[dict]:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, {}, active_only=False)
        stale_names = await stale_campaign_names(c, today)
    inbox = [r for r in inbox if not is_excluded_inbox(r)]

    seen: set = set()
    plans: list[dict] = []
    for row in inbox:
        row["campaign_is_stale"] = row.get("campaign_name", "") in stale_names
        if not _in_live_campaign(row):
            continue
        email = str(row.get("email", "")).strip()
        account_id = row.get("account_id", "")
        key = (acc.name, email)
        if not email or not account_id or key in seen:
            continue
        seen.add(key)
        current = int(row.get("message_per_day", 0) or 0)
        target = current + WARMUP_HEADROOM
        if current >= target:  # already has headroom (or field missing -> skip, unsafe to guess)
            continue
        plans.append({
            "client": acc.name, "email": email, "account_id": account_id,
            "current": current, "target": target,
            "campaign_name": row.get("campaign_name", ""),
        })
    return plans


async def main() -> None:
    today = date.today()
    accounts = discover_accounts()
    plans: list[dict] = []
    for acc in accounts:
        try:
            plans.extend(await _plan_for_account(acc, today))
        except Exception as exc:  # noqa: BLE001
            print(f"  [Headroom] {acc.name}: plan failed: {exc}")

    print(f"[Headroom] {len(plans)} ACTIVE inbox(es) need max_email_per_day raised "
          f"(+{WARMUP_HEADROOM} headroom for warmup)"
          f"{' (DRY-RUN - not applying)' if not HEADROOM_FIX_ENABLED else ''}")
    for p in plans[:40]:
        print(f"    {p['client']:14} {p['email']:34} {p['current']:3} -> {p['target']:3}  "
              f"({p['campaign_name'][:40]})")
    if len(plans) > 40:
        print(f"    ...and {len(plans) - 40} more")

    if not HEADROOM_FIX_ENABLED:
        print("[Headroom] HEADROOM_FIX_ENABLED=false -> dry-run complete. Set true to apply.")
        return

    todo = plans[:HEADROOM_FIX_PER_RUN_CAP]
    if len(plans) > len(todo):
        print(f"[Headroom] capping to {len(todo)} this run "
              f"(HEADROOM_FIX_PER_RUN_CAP={HEADROOM_FIX_PER_RUN_CAP})")
    acc_by_client = {a.name: a for a in accounts}
    applied = failed = 0
    for p in todo:
        acc = acc_by_client.get(p["client"])
        if not acc:
            failed += 1
            continue
        async with SmartleadClient(acc.api_key, acc.name) as c:
            try:
                await c.update_email_account(str(p["account_id"]),
                                             {"max_email_per_day": p["target"]})
                applied += 1
                print(f"  [Headroom] {p['client']} / {p['email']}: "
                      f"{p['current']} -> {p['target']}")
            except Exception as exc:  # noqa: BLE001
                print(f"  [Headroom] failed for {p['email']}: {exc}")
                failed += 1
    print(f"[Headroom] applied {applied}, failed {failed}.")


if __name__ == "__main__":
    asyncio.run(main())
