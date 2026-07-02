#!/usr/bin/env python3
"""Auto placement-test executor (health phase 3). Separate cron; dry-run by default.

Pass A: poll previously-created ACTIVE tests -> on completion, pull report,
        write inbox/fail result, mark done.
Pass B: select worst-first targets per client (capped) and create tests
        (unless RETEST_ENABLED is False -> dry-run: log only).
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

# Windows console: force UTF-8 so arrows/em-dashes in logs don't crash.
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
    ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME, RETEST_ENABLED,
    RETEST_PER_CLIENT_DAILY_CAP, RETEST_INBOX_THRESHOLD, RETEST_DISABLE_WARMUP,
)
from smartlead.processing import fetch_account_data
from smartlead.health import build_health_rows
from smartlead.manager_map import resolve_manager
from smartlead.retest_targets import select_targets
from smartlead.smart_delivery import SmartDeliveryClient, CreditError, SmartDeliveryError
from smartlead.placement_store import PlacementStore


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


async def _poll_pending(store: PlacementStore, key_by_client: dict[str, str]) -> int:
    """Pass A: poll ACTIVE tests, write results for completed ones."""
    completed = 0
    for t in store.pending_tests():
        client = t.get("client", "")
        api_key = key_by_client.get(client)
        if not api_key:
            continue
        async with SmartDeliveryClient(api_key) as sd:
            try:
                poll = await sd.poll_test(t["test_id"])
                if not poll["done"]:
                    continue
                rep = await sd.get_report(t["test_id"])
            except SmartDeliveryError as exc:
                print(f"  [Retest] poll/report failed for {t['test_id']}: {exc}")
                continue
        status = "inbox" if rep["inbox_pct"] >= RETEST_INBOX_THRESHOLD else "fail"
        today = date.today().strftime("%Y-%m-%d")
        for email in t.get("emails", []):
            store.save_result(email, _domain(email), status, today, "api")
        store.mark_done(t["test_id"], rep["inbox_pct"], status)
        completed += 1
        print(f"  [Retest] test {t['test_id']} ({client}) -> {status} ({rep['inbox_pct']:.0f}% inbox)")
    return completed


async def _health_rows_for(acc) -> list[dict]:
    dmap = {}
    for tab in ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME]):
        try:
            dmap.update(await DeliverabilityReader(tab_name=tab).fetch())
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] deliverability read {tab} failed: {exc}")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
    for r in inbox:
        r.setdefault("client", acc.name)
    return build_health_rows(inbox, date.today(), None, resolve_manager)


async def _campaign_launch_args(acc, campaign_name: str):
    """Resolve (campaign_id, sequence_mapping_id, sender_emails) for a campaign name."""
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = await c.list_campaigns()
        camp = next((x for x in camps if x.get("name") == campaign_name), None)
        if not camp:
            return None
        cid = camp["id"]
        seq = await c._get(f"/campaigns/{cid}/sequences")
        seqs = seq if isinstance(seq, list) else seq.get("sequences", [])
        if not seqs:
            return None
        accts = await c.get_campaign_email_accounts(str(cid))
        senders = [a.get("from_email") for a in accts if a.get("from_email")]
        # account ids of the senders we'll test — needed to toggle warmup off/on
        sender_ids = [str(a.get("id")) for a in accts if a.get("from_email") and a.get("id")]
        if not senders:
            return None
        return cid, seqs[0]["id"], senders[:100], sender_ids[:100]


async def main() -> None:
    store = PlacementStore()
    if not store.available:
        print("[Retest] State store unavailable — skipping (won't create untracked tests).")
        return

    accounts = discover_accounts()
    key_by_client = {a.name: a.api_key for a in accounts}

    print("[Retest] Pass A: polling pending tests...")
    completed = await _poll_pending(store, key_by_client)
    print(f"[Retest] Pass A done: {completed} test(s) completed.")

    print("[Retest] Pass B: selecting worst-first targets...")
    pending_emails = store.pending_emails()
    all_targets: list[dict] = []
    acc_by_client = {a.name: a for a in accounts}
    for acc in accounts:
        try:
            rows = await _health_rows_for(acc)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] health rows for {acc.name} failed: {exc}")
            continue
        all_targets.extend(select_targets(rows, RETEST_PER_CLIENT_DAILY_CAP, pending_emails))

    if not all_targets:
        print("[Retest] No targets. Done.")
        return

    print(f"[Retest] {len(all_targets)} target(s) selected"
          f"{' (DRY-RUN - not creating)' if not RETEST_ENABLED else ''}:")
    for t in all_targets:
        print(f"    {t['client']:14} {t['email']:34} {t['reason']}  camp={t['campaign_hint'][:30]}")

    if not RETEST_ENABLED:
        print("[Retest] RETEST_ENABLED=false -> dry-run complete. Set RETEST_ENABLED=true to create.")
        return

    created = skipped = 0
    drained_clients: set[str] = set()
    for t in all_targets:
        if t["client"] in drained_clients:
            skipped += 1
            continue
        acc = acc_by_client.get(t["client"])
        if not acc:
            continue
        args = await _campaign_launch_args(acc, t["campaign_hint"])
        if not args:
            print(f"  [Retest] skip {t['email']}: no campaign/senders/sequence")
            skipped += 1
            continue
        cid, seq_id, senders, sender_ids = args
        # RETEST_DISABLE_WARMUP: turn warmup OFF on the sender inboxes so the test
        # measures real send-path deliverability (faster + true placement), then
        # test with is_warmup=false. Warmup is restored right after creation.
        disabled_ids: list[str] = []
        if RETEST_DISABLE_WARMUP:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                for aid in sender_ids:
                    try:
                        await slc.set_warmup(aid, enabled=False)
                        disabled_ids.append(aid)
                    except Exception as exc:  # noqa: BLE001
                        print(f"  [Retest] warmup-off failed for acct {aid}: {exc}")
            print(f"  [Retest] warmup disabled on {len(disabled_ids)} sender(s) for test")
        async with SmartDeliveryClient(acc.api_key) as sd:
            try:
                tid = await sd.create_test(cid, seq_id, senders,
                                           f"auto-{t['client']}-{date.today().isoformat()}",
                                           is_warmup=not RETEST_DISABLE_WARMUP)
                store.record_created(tid, t["client"], cid, senders)
                created += 1
                print(f"  [Retest] created test {tid} for {t['client']} campaign {cid}"
                      f"{' (warmup-off mode)' if RETEST_DISABLE_WARMUP else ''}")
            except CreditError as exc:
                print(f"  [Retest] CREDITS exhausted on {t['client']} — skipping rest of client: {exc}")
                drained_clients.add(t["client"])
                skipped += 1
            except SmartDeliveryError as exc:
                print(f"  [Retest] create failed for {t['email']}: {exc}")
                skipped += 1
        # NOTE: warmup is intentionally NOT auto-restored here. These are active-
        # campaign senders, so warmup-off is already their correct state per the
        # warmup rule; the daily warmup executor sets the final state next run.
    print(f"[Retest] Pass B done: {created} created, {skipped} skipped.")


if __name__ == "__main__":
    asyncio.run(main())
