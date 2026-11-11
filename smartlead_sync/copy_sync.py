#!/usr/bin/env python3
"""Weekly: set the email each deliverability TEST campaign sends.

Two kinds of test campaign, two kinds of email:

  * INBOX tests ("DT <client> #n", made by dt_campaigns.py; the weekly Smart
    Delivery v2 tests run on them) send a fixed neutral note with tracking off
    (smartlead/placement_copy.NEUTRAL_*). A fail then means the inbox or its
    domain, never the copy (Manveen, 11 Nov 2026).
  * The standing test campaigns below send what is really being sent: the
    client's MOST ACTIVE campaign of the week (most emails sent in the last 7
    days; else the newest active one; else its last active paused/completed
    one) is copied into the first step.

Posts "Message sequence refreshed" to Slack (plus a line per test campaign it
could not refresh). Live campaigns are only read, never written.

    python3 copy_sync.py               # dry run: says what it would write
    python3 copy_sync.py --apply       # write
    python3 copy_sync.py --only BETTRDATA
    python3 copy_sync.py --apply --skip 111,222   # leave these test campaigns alone (a test is running on them)

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
from smartlead.placement_copy import refresh_test_campaign, write_neutral_note
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
# PL's account still holds old BettrData / Melior campaigns with no client id:
# PL's own tests must never copy them.
SKIP_WORDS = {("PRECISE_LEADS", None): ("bettrdata", "melior")}


def is_inbox_test(name: str) -> bool:
    """A "DT <known client> #n" campaign: an inbox test, which sends the neutral note."""
    n = str(name or "")
    return any(n.startswith(p + " #") for p in DT_SOURCE_CLIENT)


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
    ap.add_argument("--skip", default="", help="comma-separated test campaign ids to leave alone this run")
    args = ap.parse_args()
    store = PlacementStore()
    problems: list[str] = []
    written = attempted = 0
    by_name = {a.name.upper(): a for a in discover_accounts()}
    failed = 0
    skip = {int(x) for x in str(args.skip).split(",") if x.strip().isdigit()}
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
        names = {int(x["id"]): x["name"] for x in dt_names}
        for x in dt_names:
            prefix = next((p for p in DT_SOURCE_CLIENT if x["name"].startswith(p + " #")), None)
            if prefix is None:
                # Never guess a client: a wrong guess tests another client's email.
                msg = f"{name}: test campaign '{x['name']}' has no known client prefix ({', '.join(DT_SOURCE_CLIENT)}) - not refreshed"
                print(f"[CopySync] {msg}")
                problems.append(":warning: " + msg)
                failed += 1
                continue
            plan.setdefault(int(x["id"]), DT_SOURCE_CLIENT[prefix])
        for test_id, client_id in plan.items():
            if int(test_id) in skip:
                print(f"[CopySync] {name} test {test_id}: skipped (--skip)")
                continue
            step = await first_step_id(acc, test_id)
            if step is None:
                msg = f"{name}: test campaign {names.get(int(test_id), test_id)} has no step to write into"
                print(f"[CopySync] {msg}")
                problems.append(":warning: " + msg)
                failed += 1
                continue
            if is_inbox_test(names.get(int(test_id), "")):
                try:
                    async with SmartleadClient(acc.api_key, acc.name) as c:
                        r = await write_neutral_note(c, acc.name, test_id, step, store, dry_run=not args.apply)
                except Exception as exc:  # noqa: BLE001 - say so and go on; never print the exception text (URL)
                    r = {"written": False, "source": None, "reason": f"could not write the neutral note ({type(exc).__name__})"}
                    problems.append(f":warning: {name}: test campaign {names.get(int(test_id), test_id)} kept its old copy "
                                    f"({type(exc).__name__})")
                    failed += 1
            else:
                r = await refresh_test_campaign(acc, test_id, step, store, dry_run=not args.apply,
                                                source_client_id=client_id,
                                                skip_words=SKIP_WORDS.get((name, client_id), ()))
            state = "UPDATED" if r["written"] else "kept"
            print(f"[CopySync] {name} test {test_id}: {state} - source "
                  f"{r['source']} - {r['reason']}")
            attempted += 1
            written += bool(r["written"])
    if args.apply and (attempted or problems):
        _post_summary(written, problems)
    return 1 if failed else 0


def _summary_parts(written: int, problems: list[str]) -> tuple[list[str], list[str]]:
    """(headline_lines, thread_parts): one line in the channel, problems in its thread."""
    verb = "refreshed" if written else "not refreshed"
    if not problems:
        return [f"\u2705 Message sequences {verb}" if written else f"\u26a0\ufe0f Message sequences {verb}"], []
    n = len(problems)
    return ([f"\u26a0\ufe0f Message sequences {verb}: {n} problem{'s' if n != 1 else ''}"],
            ["\n".join(problems)])


def _post_summary(written: int, problems: list[str]) -> None:
    """One line for the team; problems in the thread, per-campaign detail in the logs."""
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = os.getenv("PLACEMENT_REPORT_CHANNEL") or "C0AGVSUNEFP"
    if not token:
        return
    try:
        from smartlead.notify import post_with_thread
        head, thread = _summary_parts(written, problems)
        post_with_thread(token, channel, head, thread)
    except Exception as exc:  # noqa: BLE001
        print(f"[CopySync] Slack summary failed: {exc}")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
