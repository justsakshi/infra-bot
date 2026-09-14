#!/usr/bin/env python3
"""Weekly placement-test executor. Thursday batch + result collection.

Pass A collects results for tests already in flight and writes them to Mongo
and to the client's deliverability grid. Pass B selects this week's domains and
fires one test per domain.

Two passes rather than one long job because a test takes ~75 minutes: Thursday
fires the batch, and the daily run picks the results up as they land. A test
that never completes is abandoned after 3 days by the existing retest_executor
sweep, which also restores warmup.

Every test routes through the account's standing "deliverability test" campaign.
Live campaigns are never modified and never used to send a test.

Dry-run unless PLACEMENT_ENABLED is set.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import date

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import (
    ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME, TEST_SHEET_ID,
    RETEST_INBOX_THRESHOLD, RETEST_TEST_CAMPAIGN_KEYWORD,
    PLACEMENT_WAVE_SIZE, PLACEMENT_NO_TIME_GAP, PLACEMENT_DAILY_TEST_CAP,
    PLACEMENT_MIN_COVERAGE, PLACEMENT_COPY_REFRESH,
)
from smartlead.placement_copy import refresh_test_campaign
from smartlead.placement_grid import write_result
from smartlead.placement_schedule import PlacementSchedule, select_batch
from smartlead.placement_store import PlacementStore
from smartlead.smart_delivery import SmartDeliveryClient, CreditError, SmartDeliveryError

# Domains the team has retired. Testing them spends credits to learn nothing.
SKIP_STATUSES = ("replaced", "cancel")
INTERVAL_DAYS = 7
# A test still open after this long is not going to fill in. Smartlead closes
# the send window at ~72 minutes; anything unclassified three days later is
# starved, and the domain should be re-queued rather than left waiting.
ABANDON_AFTER_DAYS = 3


def _domain_of(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


def _tab_for(client: str) -> str:
    tabs = ACCOUNT_DELIVERABILITY_TABS.get(client, [TEST_TAB_NAME])
    return tabs[0] if tabs else TEST_TAB_NAME


def read_testable_domains(tab: str) -> dict[str, str]:
    """Domain -> status, for domains the team still considers live.

    The status column is hand-maintained and has no other home, so it is the
    one field still read from the sheet.
    """
    try:
        from smartlead.sheets import _authorize
        rows = _authorize().open_by_key(TEST_SHEET_ID).worksheet(tab).get_all_values()
    except Exception as exc:  # noqa: BLE001
        print(f"  [Placement] could not read tab {tab}: {exc}")
        return {}
    out: dict[str, str] = {}
    for row in rows[2:]:
        if not row or not row[0].strip():
            continue
        domain = row[0].strip().lower()
        if "." not in domain:
            continue
        status = (row[3] if len(row) > 3 else "").strip().lower()
        if any(s in status for s in SKIP_STATUSES):
            continue
        out[domain] = status
    return out


async def _live_inboxes(acc) -> dict[str, list[str]]:
    """Domain -> sending inboxes, from Smartlead."""
    async with SmartleadClient(acc.api_key, acc.name) as c:
        accounts = await c.list_email_accounts()
    by_domain: dict[str, list[str]] = defaultdict(list)
    for a in accounts:
        email = (a.get("from_email") or "").strip()
        if not email or is_excluded_inbox({"email": email, "tags": a.get("tags", [])}):
            continue
        by_domain[_domain_of(email)].append(email)
    return dict(by_domain)


async def _test_campaign(acc) -> tuple[int, int] | None:
    """(campaign_id, sequence_mapping_id) of the standing test campaign."""
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = await c.list_campaigns()
        camp = next((x for x in camps
                     if RETEST_TEST_CAMPAIGN_KEYWORD in str(x.get("name", "")).lower()), None)
        if not camp:
            print(f"  [Placement] no '{RETEST_TEST_CAMPAIGN_KEYWORD}' campaign in {acc.name}")
            return None
        seq = await c._get(f"/campaigns/{camp['id']}/sequences")
        seqs = seq if isinstance(seq, list) else seq.get("sequences", [])
        if not seqs:
            print(f"  [Placement] test campaign {camp['id']} has no sequence")
            return None
        return camp["id"], seqs[0]["id"]


async def collect_results(acc, store: PlacementStore, schedule: PlacementSchedule,
                          dry_run: bool) -> int:
    """Pass A: write results for any completed test."""
    tab = _tab_for(acc.name)
    done = 0
    for t in store.pending_tests():
        if t.get("source") == "emailguard" or t.get("client") != acc.name:
            continue
        async with SmartDeliveryClient(acc.api_key) as sd:
            try:
                poll = await sd.poll_test(t["test_id"])
                if not poll["done"]:
                    continue
                report = await sd.get_report(t["test_id"])
            except SmartDeliveryError as exc:
                print(f"  [Placement] poll/report {t['test_id']} failed: {exc}")
                continue

        # Smartlead flips a test to COMPLETED before every seed has been
        # classified, especially under a large concurrent batch. A report with
        # no or few classified seeds is "not measured yet", not a failure —
        # scoring it would write `spam` for a healthy domain. Leave the test
        # ACTIVE so a later collector run picks up the finished data.
        if not report.get("has_data"):
            print(f"  [Placement] test {t['test_id']}: no seeds classified yet "
                  f"({report.get('classified', 0)}/{report.get('dispatched', 0)}) - leaving open")
            continue
        if report.get("coverage", 1.0) < PLACEMENT_MIN_COVERAGE:
            print(f"  [Placement] test {t['test_id']}: only "
                  f"{report.get('classified')}/{report.get('dispatched')} seeds classified "
                  f"({report.get('coverage', 0):.0%}) - below {PLACEMENT_MIN_COVERAGE:.0%}, "
                  "leaving open")
            continue

        judged = report.get("worst_provider_inbox_pct", report["inbox_pct"])
        status = "inbox" if judged >= RETEST_INBOX_THRESHOLD else "fail"
        for email in t.get("emails", []):
            domain = _domain_of(email)
            detail = " ".join(f"{p}={v['inbox']}/{v['total']}"
                              for p, v in sorted(report.get("by_provider", {}).items()))
            print(f"  [Placement] {domain} -> {status} (worst {judged:.0f}%) {detail}")
            if dry_run:
                continue
            store.save_result(email, domain, status, date.today().isoformat(), "api")
            schedule.record_result(acc.name, domain, email, status,
                                   report.get("by_provider", {}))
            plan = write_result(tab, domain, report.get("by_provider", {}),
                                threshold=RETEST_INBOX_THRESHOLD)
            if plan.skipped_reason:
                print(f"  [Placement] grid: {domain} not written ({plan.skipped_reason})")
        if not dry_run:
            store.mark_done(t["test_id"], report["inbox_pct"], status)
        done += 1

    # Only after every collectable test has been read: anything still open
    # this long is starved and will not fill in. Abandoning before collecting
    # would discard a test that crossed the coverage bar on its last day.
    for t in store.stale_tests(max_age_days=ABANDON_AFTER_DAYS):
        if t.get("client") != acc.name or t.get("source") == "emailguard":
            continue
        print(f"  [Placement] test {t['test_id']} open >{ABANDON_AFTER_DAYS}d below "
              f"coverage - abandoning; domain re-queues next run")
        if not dry_run:
            store.mark_abandoned(t["test_id"])
    return done


async def fire_batch(acc, store: PlacementStore, schedule: PlacementSchedule,
                     dry_run: bool, cap: int | None) -> int:
    """Pass B: select this week's domains and create one test each."""
    testable = read_testable_domains(_tab_for(acc.name))
    if not testable:
        print(f"  [Placement] no testable domains for {acc.name}")
        return 0

    inboxes = await _live_inboxes(acc)
    covered = {d: inboxes[d] for d in testable if inboxes.get(d)}
    if not covered:
        print(f"  [Placement] no live inboxes on testable domains for {acc.name}")
        return 0
    schedule.sync_inboxes(acc.name, covered)

    records = schedule.load(acc.name)
    for domain, emails in covered.items():
        records.setdefault(domain, {"domain": domain})["inboxes"] = emails

    batch = select_batch({d: r for d, r in records.items() if d in covered},
                         today=date.today().isoformat(),
                         interval_days=INTERVAL_DAYS,
                         pending=store.pending_emails(), cap=cap)
    if not batch:
        print(f"  [Placement] nothing due for {acc.name}")
        return 0

    # Wave throttle: only fire when the previous wave has mostly cleared. This
    # is what turns "fire all 19" into a sequence of small batches across the
    # day without a long-running process.
    open_now = sum(1 for t in store.pending_tests()
                   if t.get("client") == acc.name and t.get("source") != "emailguard")
    room = max(0, PLACEMENT_WAVE_SIZE - open_now)
    if room == 0:
        print(f"  [Placement] {open_now} test(s) still open for {acc.name} "
              f"(wave size {PLACEMENT_WAVE_SIZE}) - waiting for them to clear")
        return 0
    # Daily spend ceiling. There is no balance endpoint; this is the brake.
    spent = store.created_since(acc.name, date.today().isoformat())
    room = min(room, max(0, PLACEMENT_DAILY_TEST_CAP - spent))
    if room == 0:
        print(f"  [Placement] daily cap reached for {acc.name} "
              f"({spent}/{PLACEMENT_DAILY_TEST_CAP} created today)")
        return 0
    if len(batch) > room:
        print(f"  [Placement] {len(batch)} due, firing {room} this wave "
              f"({open_now} open, {spent}/{PLACEMENT_DAILY_TEST_CAP} today)")
        batch = batch[:room]

    print(f"  [Placement] {len(batch)} domain(s) due for {acc.name}"
          f"{' (DRY-RUN)' if dry_run else ''}:")
    for b in batch:
        print(f"      {b['domain']:26} {b['email']:36} fails={b['consecutive_fails']} "
              f"last={b['last_tested_date'] or 'never'}")
    if dry_run:
        return 0

    args = await _test_campaign(acc)
    if not args:
        return 0
    campaign_id, sequence_id = args

    copy = {"hash": "", "source": None}
    if PLACEMENT_COPY_REFRESH:
        copy = await refresh_test_campaign(acc, campaign_id, sequence_id, store)
        state = "refreshed" if copy["written"] else "kept previous copy"
        print(f"  [Placement] test campaign copy {state}: {copy['reason']}")

    # Attach any sender not already on the test campaign; create_test rejects
    # senders the campaign does not know about.
    async with SmartleadClient(acc.api_key, acc.name) as c:
        accounts = await c.list_email_accounts()
        id_by_email = {(a.get("from_email") or "").strip(): a.get("id") for a in accounts}
        attached = await c.get_campaign_email_accounts(str(campaign_id))
        already = {(a.get("from_email") or "").strip() for a in attached}
        missing = [int(id_by_email[b["email"]]) for b in batch
                   if b["email"] not in already and id_by_email.get(b["email"])]
        if missing:
            await c.add_campaign_email_accounts(str(campaign_id), missing)
            print(f"  [Placement] attached {len(missing)} sender(s) to the test campaign")

    async def fire(item: dict):
        async with SmartDeliveryClient(acc.api_key) as sd:
            try:
                tid = await sd.create_test(
                    campaign_id, sequence_id, [item["email"]],
                    f"auto-{acc.name}-{item['domain']}-{date.today().isoformat()}",
                    is_warmup=True, no_time_gap=PLACEMENT_NO_TIME_GAP)
                return item, tid, None
            except CreditError as exc:
                return item, None, f"credits: {exc}"
            except SmartDeliveryError as exc:
                return item, None, str(exc)

    created = 0
    for item, tid, err in await asyncio.gather(*[fire(b) for b in batch]):
        if tid:
            store.record_created(tid, acc.name, campaign_id, [item["email"]],
                                 extra={"copy_hash": copy["hash"],
                                        "copy_source": copy["source"]})
            created += 1
            print(f"  [Placement] test {tid} created for {item['domain']}")
        else:
            print(f"  [Placement] {item['domain']} not tested: {err}")
            if err and err.startswith("credits"):
                # Out of credits: the rest of this client's batch would fail the
                # same way, and each attempt is a wasted API call.
                print("  [Placement] credits exhausted - stopping this client")
                break
    return created


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", help="limit to one client")
    ap.add_argument("--domain", help="test one domain's next inbox")
    ap.add_argument("--inbox", help="test one specific inbox")
    ap.add_argument("--collect-only", action="store_true",
                    help="write results for in-flight tests, create none")
    ap.add_argument("--dry-run", action="store_true", help="log, change nothing")
    ap.add_argument("--cap", type=int, help="max domains per client this run")
    opts = ap.parse_args()

    store = PlacementStore()
    if not store.available:
        print("[Placement] state store unavailable - refusing to create untracked tests.")
        return
    schedule = PlacementSchedule()

    accounts = discover_accounts()
    if opts.client:
        accounts = [a for a in accounts if a.name.upper() == opts.client.upper()]
        if not accounts:
            print(f"[Placement] no such client: {opts.client}")
            return

    for acc in accounts:
        print(f"[Placement] {acc.name}: collecting results...")
        done = await collect_results(acc, store, schedule, opts.dry_run)
        print(f"[Placement] {acc.name}: {done} result(s) written.")

        if opts.collect_only:
            continue
        if opts.inbox:
            await fire_single(acc, store, opts.inbox, opts.dry_run)
            continue
        created = await fire_batch(acc, store, schedule, opts.dry_run, opts.cap)
        print(f"[Placement] {acc.name}: {created} test(s) created.")


async def fire_single(acc, store: PlacementStore, email: str, dry_run: bool) -> None:
    """On-demand path: test one named inbox through the same machinery."""
    if dry_run:
        print(f"  [Placement] would test {email}")
        return
    args = await _test_campaign(acc)
    if not args:
        return
    campaign_id, sequence_id = args
    async with SmartleadClient(acc.api_key, acc.name) as c:
        accounts = await c.list_email_accounts()
        account_id = next((a.get("id") for a in accounts
                           if (a.get("from_email") or "").strip() == email), None)
        if not account_id:
            print(f"  [Placement] {email} not found on {acc.name}")
            return
        try:
            await c.add_campaign_email_accounts(str(campaign_id), [int(account_id)])
        except Exception as exc:  # noqa: BLE001
            print(f"  [Placement] attach {email}: {exc} (continuing)")
    async with SmartDeliveryClient(acc.api_key) as sd:
        try:
            tid = await sd.create_test(campaign_id, sequence_id, [email],
                                       f"manual-{acc.name}-{email}-{date.today().isoformat()}",
                                       is_warmup=True, no_time_gap=PLACEMENT_NO_TIME_GAP)
        except (CreditError, SmartDeliveryError) as exc:
            print(f"  [Placement] create failed for {email}: {exc}")
            return
    store.record_created(tid, acc.name, campaign_id, [email])
    print(f"  [Placement] test {tid} created for {email}")


if __name__ == "__main__":
    asyncio.run(main())
