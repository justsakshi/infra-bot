#!/usr/bin/env python3
"""Auto-warmup executor (health phase 4). Separate cron; dry-run by default.

Rule: warmup ON unless the inbox is actively sending in a live ACTIVE campaign.
Reads raw inbox rows (which carry warmup_state / sent_today / campaign_status /
account_id), plans the changes, and applies them via the Smartlead API — unless
WARMUP_AUTO_ENABLED is False (dry-run: log only, change nothing).
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
    WARMUP_AUTO_ENABLED, WARMUP_PER_DAY, WARMUP_DAILY_RAMPUP, WARMUP_REPLY_RATE,
)
from smartlead.processing import fetch_account_data
from smartlead.warmup_planner import plan_warmup_changes


async def _raw_inbox_rows(acc) -> list[dict]:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, {}, active_only=False)
    for r in inbox:
        r.setdefault("client", acc.name)
    return inbox


async def main() -> None:
    accounts = discover_accounts()
    all_changes: list[dict] = []
    acc_by_client = {a.name: a for a in accounts}

    for acc in accounts:
        try:
            rows = await _raw_inbox_rows(acc)
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

    enable = [c for c in all_changes if c["action"] == "enable"]
    disable = [c for c in all_changes if c["action"] == "disable"]
    print(f"[Warmup] {len(all_changes)} change(s): {len(enable)} enable, {len(disable)} disable"
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
                await sl.set_warmup(
                    str(c["account_id"]),
                    enabled=(c["action"] == "enable"),
                    total_per_day=WARMUP_PER_DAY,
                    daily_rampup=WARMUP_DAILY_RAMPUP,
                    reply_rate=WARMUP_REPLY_RATE,
                )
                applied += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  [Warmup] {c['action']} failed for {c['email']}: {exc}")
                failed += 1
    print(f"[Warmup] applied {applied}, failed {failed}.")


if __name__ == "__main__":
    asyncio.run(main())
