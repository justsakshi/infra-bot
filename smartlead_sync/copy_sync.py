#!/usr/bin/env python3
"""Weekly: point each deliverability TEST campaign at what is really being sent.

The test campaigns carry the scheduled SmartDelivery tests, so the email they
test must be the email of a campaign that is running this week. This copies
the newest ACTIVE campaign's first step into each test campaign's first step.
Live campaigns are only read, never written.

    python3 copy_sync.py               # dry run: says what it would copy
    python3 copy_sync.py --apply       # write
    python3 copy_sync.py --only BETTRDATA

Targets are listed below. `source_client_id` keeps a test on its own client's
copy: None = the account's own campaigns (e.g. Precise Leads' own outbound),
an id = one Smartlead client (Melior is 12256 inside the Precise Leads account).
A target with no eligible active campaign keeps its current copy and says so.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.placement_copy import refresh_test_campaign
from smartlead.placement_store import PlacementStore

# account name -> [(standing test campaign id, source client id), ...]. Every campaign
# in the account named "DT ..." (made by dt_campaigns.py) is synced too, from the
# first entry's source rule.
TARGETS = {
    "PRECISE_LEADS": [
        (4080258, None),     # PL's own outbound, never Melior's
        (4085678, 12256),    # Melior's weekly test campaign (Anjali): newest active Melior campaign
    ],
    "BETTRDATA": [(3752513, None)],   # BettrData's campaigns carry no client id
}
# Campaigns named "DT <prefix> #n" take their copy from this client's campaigns.
DT_SOURCE_CLIENT = {"DT PL": None, "DT Melior": 12256, "DT BettrData": None}


async def first_step_id(acc, campaign_id: int) -> int | None:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        seq = await c._get(f"/campaigns/{campaign_id}/sequences")
    steps = seq if isinstance(seq, list) else seq.get("sequences", [])
    steps = sorted(steps, key=lambda s: s.get("seq_number", 0))
    return int(steps[0]["id"]) if steps else None


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--only", help="account name, e.g. BETTRDATA")
    args = ap.parse_args()
    store = PlacementStore()
    by_name = {a.name.upper(): a for a in discover_accounts()}
    failed = 0
    for name, standing in TARGETS.items():
        if args.only and args.only.upper() != name:
            continue
        acc = by_name.get(name)
        if not acc:
            print(f"[CopySync] {name}: no API key in this environment, skipped")
            continue
        async with SmartleadClient(acc.api_key, acc.name) as c:
            dt_names = [x for x in await c.list_campaigns()
                        if str(x.get("name", "")).startswith("DT ")]
        plan = dict(standing)
        for x in dt_names:
            prefix = next((p for p in DT_SOURCE_CLIENT if x["name"].startswith(p + " #")), None)
            plan.setdefault(int(x["id"]), DT_SOURCE_CLIENT.get(prefix, standing[0][1]))
        for test_id, client_id in plan.items():
            step = await first_step_id(acc, test_id)
            if step is None:
                print(f"[CopySync] {name}: test campaign {test_id} has no step to write into")
                failed += 1
                continue
            r = await refresh_test_campaign(acc, test_id, step, store, dry_run=not args.apply,
                                            source_client_id=client_id)
            state = "UPDATED" if r["written"] else "kept"
            print(f"[CopySync] {name} test {test_id}: {state} - source "
                  f"{r['source']} - {r['reason']}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
