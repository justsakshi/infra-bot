#!/usr/bin/env python3
"""EmailGuard placement-test executor — credit-free inbox testing, unattended.

Replaces the human step in the Smartlead flow. For each target inbox:

  Pass A (poll)  existing tests -> when every seed has landed, record the
                 result to Mongo + the API Tests sheet tab, then pause and
                 clean up the sender campaign.
  Pass B (create) pick worst-first targets, create an EmailGuard test, and
                 send its seed list from the inbox's OWN Smartlead account via
                 a short-lived campaign carrying the filter phrase.

The inbox never leaves its own account (no imports — team rule), no Smartlead
SmartDelivery credits are consumed, and no human touches the loop.

DRY-RUN by default (EG_TEST_ENABLED=false): logs the plan, spends nothing.
Placement-test quota is checked before every create — it is the scarce
resource, and a trial workspace has single digits of it.
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
    EG_TEST_ENABLED, EG_PER_RUN_CAP, EG_MIN_QUOTA_RESERVE, EG_SEND_GAP_MINUTES,
    RETEST_INBOX_THRESHOLD, ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME,
)
from smartlead.emailguard import (
    EmailGuardClient, EmailGuardError, QuotaError, summarize_placement, api_key,
)
from smartlead.placement_store import PlacementStore
from smartlead.placement_sheet import append_api_result
from smartlead.processing import fetch_account_data
from smartlead.health import build_health_rows
from smartlead.manager_map import resolve_manager
from smartlead.retest_targets import select_targets
from smartlead.client_filter import is_excluded_inbox
from smartlead.sheets import DeliverabilityReader

_CAMPAIGN_PREFIX = "EG Placement Test"


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


async def _send_seeds(acc, inbox: dict, seeds: list[str], phrase: str) -> str:
    """Create + start a short-lived campaign that mails the seed list from this
    inbox. Returns the campaign id.

    Schedule constraints verified live: min_time_btw_emails must be >= 3, and
    the schedule endpoint wants the flat timezone/days_of_the_week form."""
    target = inbox["from_email"]
    body = ("<p>Hi,</p><p>Quick note ahead of this week's update — let me know if "
            "the timing works and I'll send the details.</p><p>Best,<br>"
            f"{inbox.get('from_name') or 'Sam'}</p><p>{phrase}</p>")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camp = await c.create_campaign(f"{_CAMPAIGN_PREFIX} - {target} - {date.today()}")
        cid = str(camp["id"])
        await c.save_campaign_sequences(cid, "Quick note before this week's update", body)
        await c.update_campaign_schedule(cid, {
            "timezone": "Asia/Kolkata", "days_of_the_week": [0, 1, 2, 3, 4, 5, 6],
            "start_hour": "00:00", "end_hour": "23:59",
            "min_time_btw_emails": max(3, EG_SEND_GAP_MINUTES),
            "max_new_leads_per_day": len(seeds) + 5,
        })
        await c.add_campaign_leads(cid, seeds)
        await c.add_campaign_email_accounts(cid, [int(inbox["id"])])
        await c.set_campaign_status(cid, "START")
    return cid


async def _cleanup(acc, campaign_id: str) -> None:
    """Pause the throwaway sender campaign once its test is scored."""
    try:
        async with SmartleadClient(acc.api_key, acc.name) as c:
            await c.set_campaign_status(campaign_id, "PAUSED")
    except Exception as exc:  # noqa: BLE001
        print(f"  [EG] cleanup: could not pause campaign {campaign_id}: {exc}")


async def _poll_open_tests(store: PlacementStore, acc_by_client: dict) -> int:
    """Pass A — score every finished test and write it into the pipeline."""
    done = 0
    async with EmailGuardClient() as eg:
        for t in store.pending_tests():
            if t.get("source") != "emailguard":
                continue  # Smartlead tests are handled by retest_executor
            uuid = t.get("eg_uuid") or ""
            try:
                res = summarize_placement(await eg.get_placement_test(uuid))
            except EmailGuardError as exc:
                print(f"  [EG] poll failed for {uuid}: {exc}")
                continue
            if not res["complete"]:
                print(f"  [EG] {t.get('emails', ['?'])[0]}: {res['delivered']}/{res['seeds']} "
                      "seed(s) landed — still running")
                continue
            status = "inbox" if res["inbox_pct"] >= RETEST_INBOX_THRESHOLD else "fail"
            today = date.today().strftime("%Y-%m-%d")
            for email in t.get("emails", []):
                store.save_result(email, _domain(email), status, today, "emailguard")
            store.mark_done(t["test_id"], res["inbox_pct"], status)
            append_api_result(t.get("client", "EG"), t["test_id"], t.get("emails", []),
                              res["inbox_pct"], res["spam_pct"], status)
            print(f"  [EG] {t.get('emails', ['?'])[0]} -> {status} "
                  f"({res['inbox_pct']:.0f}% inbox / {res['spam_pct']:.0f}% spam, "
                  f"{res['delivered']} seeds)")
            acc = acc_by_client.get(t.get("client", ""))
            if acc and t.get("campaign_id"):
                await _cleanup(acc, str(t["campaign_id"]))
            done += 1
    return done


async def _targets_for(acc, pending: set[str]) -> list[dict]:
    dmap: dict = {}
    for tab in ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME]):
        try:
            dmap.update(await DeliverabilityReader(tab_name=tab).fetch())
        except Exception as exc:  # noqa: BLE001
            print(f"  [EG] deliverability read {tab} failed: {exc}")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
    inbox = [r for r in inbox if not is_excluded_inbox(r)]
    for r in inbox:
        r.setdefault("client", acc.name)
    rows = build_health_rows(inbox, date.today(), None, resolve_manager)
    return select_targets(rows, EG_PER_RUN_CAP, pending)


async def main() -> None:
    if not api_key():
        print("[EG] no EmailGuard token (EMAILGUARD_API_KEY / EMAILGURAD) — skipping.")
        return
    store = PlacementStore()
    if not store.available:
        print("[EG] state store unavailable — refusing to create untracked tests.")
        return

    accounts = discover_accounts()
    acc_by_client = {a.name: a for a in accounts}

    print("[EG] Pass A: polling open EmailGuard tests...")
    print(f"[EG] Pass A done: {await _poll_open_tests(store, acc_by_client)} test(s) scored.")

    async with EmailGuardClient() as eg:
        try:
            quota = await eg.remaining_placement_tests()
        except EmailGuardError as exc:
            print(f"[EG] quota check failed: {exc}")
            return
    print(f"[EG] {quota} placement-test credit(s) remaining "
          f"(reserve {EG_MIN_QUOTA_RESERVE})")

    budget = max(0, quota - EG_MIN_QUOTA_RESERVE)
    if budget <= 0:
        print("[EG] quota at/below reserve — not creating tests. Top up the "
              "EmailGuard workspace to resume automated testing.")
        return

    pending = store.pending_emails()
    plan: list[tuple] = []
    for acc in accounts:
        try:
            for t in await _targets_for(acc, pending):
                plan.append((acc, t))
        except Exception as exc:  # noqa: BLE001
            print(f"  [EG] target selection failed for {acc.name}: {exc}")
    plan = plan[:budget]

    if not plan:
        print("[EG] no inboxes need testing. Done.")
        return

    print(f"[EG] {len(plan)} inbox(es) to test"
          f"{' (DRY-RUN — nothing will be created)' if not EG_TEST_ENABLED else ''}:")
    for acc, t in plan:
        print(f"    {acc.name:14} {t['email']:34} {t['reason']}")
    if not EG_TEST_ENABLED:
        print("[EG] EG_TEST_ENABLED=false -> dry-run complete.")
        return

    created = 0
    async with EmailGuardClient() as eg:
        for acc, t in plan:
            email = t["email"]
            try:
                async with SmartleadClient(acc.api_key, acc.name) as c:
                    accs = await c.list_email_accounts()
                inbox = next((a for a in accs if a.get("from_email") == email), None)
                if not inbox:
                    print(f"  [EG] {email}: not found in {acc.name} — skipped")
                    continue
                test = await eg.create_placement_test(f"{email}-{date.today().isoformat()}")
                seeds = [s["email"] for s in test.get("inbox_placement_test_emails", [])]
                if not seeds:
                    print(f"  [EG] {email}: test created but returned no seeds — skipped")
                    continue
                cid = await _send_seeds(acc, inbox, seeds, test["filter_phrase"])
                store.record_created(test["uuid"], acc.name, cid, [email])
                store.tag_source(test["uuid"], "emailguard", test["uuid"])
                created += 1
                print(f"  [EG] {email}: test {test['uuid'][:8]}… sending {len(seeds)} "
                      f"seed(s) via campaign {cid}")
            except QuotaError:
                print("  [EG] placement-test quota exhausted — stopping.")
                break
            except Exception as exc:  # noqa: BLE001
                print(f"  [EG] {email}: create/send failed: {exc}")
    print(f"[EG] Pass B done: {created} test(s) created.")


if __name__ == "__main__":
    asyncio.run(main())
