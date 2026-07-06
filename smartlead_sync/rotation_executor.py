#!/usr/bin/env python3
"""Auto inbox rotation (spec 2026-07-03). Separate cron; DRY-RUN by default.

Swaps P0-broken senders out of live campaigns for healthy same-client bench
inboxes. Order is load-bearing: ADD replacement -> handle in-flight leads
(reassign persona-matched / pause) -> REMOVE victim. Never removes a campaign's
last sender. Everything logged to Mongo rotation_log.

NOTE: the in-flight reassignment body + lead->inbox assignment discovery are
validated on a DUMMY CAMPAIGN before ROTATION_ENABLED is ever set true (spec
safety rail). Until then this executor's enabled path performs add+remove and
logs the in-flight policy it would apply.
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
from smartlead.sheets import DeliverabilityReader
from smartlead.config import (
    ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME,
    ROTATION_ENABLED, ROTATION_PER_CLIENT_DAILY_CAP,
)
from smartlead.processing import fetch_account_data
from smartlead.client_filter import is_excluded_inbox
from smartlead.rotation_planner import select_swaps
from smartlead.rotation_store import RotationStore


async def _raw_rows(acc) -> list[dict]:
    dmap = {}
    for tab in ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME]):
        try:
            dmap.update(await DeliverabilityReader(tab_name=tab).fetch())
        except Exception as exc:  # noqa: BLE001
            print(f"  [Rotation] deliverability read {tab} failed: {exc}")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
    inbox = [r for r in inbox if not is_excluded_inbox(r)]
    for r in inbox:
        r.setdefault("client", acc.name)
    return inbox


async def _apply_swap(acc, swap: dict, store: RotationStore) -> bool:
    """Enabled path: add replacement -> verify -> remove victim. Never leaves 0 senders."""
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = await c.list_campaigns()
        camp = next((x for x in camps if x.get("name") == swap["campaign_name"]), None)
        if not camp:
            store.log_swap(swap, "failed", "campaign not found")
            return False
        cid = str(camp["id"])
        # 1. ADD replacement
        await c.add_campaign_email_accounts(cid, [swap["replacement_account_id"]])
        # verify attached + count senders
        accts = await c.get_campaign_email_accounts(cid)
        emails = [a.get("from_email", "").lower() for a in accts]
        if swap["replacement_email"].lower() not in emails:
            store.log_swap(swap, "failed", "replacement did not attach")
            return False
        if len(emails) < 2:
            store.log_swap(swap, "failed", "would leave campaign with <1 sender")
            return False
        # 2. in-flight policy — validated on the dummy campaign before real use
        print(f"  [Rotation] in-flight policy for {swap['victim_email']}: {swap['inflight_policy']}")
        # 3. REMOVE victim
        await c.remove_campaign_email_accounts(cid, [swap["victim_account_id"]])
        store.log_swap(swap, "done")
        return True


async def main() -> None:
    store = RotationStore()
    if not store.available:
        print("[Rotation] Log store unavailable - skipping (won't swap untracked).")
        return
    accounts = discover_accounts()
    acc_by_client: dict = {}
    all_rows: list[dict] = []
    for acc in accounts:
        try:
            rows = await _raw_rows(acc)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Rotation] rows for {acc.name} failed: {exc}")
            continue
        all_rows.extend(rows)
        for r in rows:
            acc_by_client[r.get("client", acc.name)] = acc

    swaps = select_swaps(all_rows, ROTATION_PER_CLIENT_DAILY_CAP, store.already_swapped())
    doable = [s for s in swaps if s["replacement_email"]]
    alerts = [s for s in swaps if not s["replacement_email"]]

    print(f"[Rotation] {len(doable)} swap(s) planned, {len(alerts)} no-bench alert(s)"
          f"{' (DRY-RUN - not applying)' if not ROTATION_ENABLED else ''}")
    for s in doable:
        print(f"    SWAP  {s['client']:14} {s['campaign_name'][:28]:28} "
              f"{s['victim_email']:32} -> {s['replacement_email']:32} "
              f"inflight={s['inflight_policy']}")
    for s in alerts:
        print(f"    ALERT {s['client']:14} {s['campaign_name'][:28]:28} "
              f"{s['victim_email']:32} -> NO HEALTHY BENCH - provision domains")

    if not ROTATION_ENABLED:
        print("[Rotation] ROTATION_ENABLED=false -> dry-run complete. "
              "Validate on the dummy campaign, then set true to apply.")
        return

    done = failed = 0
    for s in doable:
        acc = acc_by_client.get(s["client"])
        if not acc:
            failed += 1
            continue
        try:
            ok = await _apply_swap(acc, s, store)
            done += 1 if ok else 0
            failed += 0 if ok else 1
        except Exception as exc:  # noqa: BLE001
            print(f"  [Rotation] swap failed for {s['victim_email']}: {exc}")
            store.log_swap(s, "failed", str(exc)[:200])
            failed += 1
    print(f"[Rotation] applied {done}, failed {failed}.")


if __name__ == "__main__":
    asyncio.run(main())
