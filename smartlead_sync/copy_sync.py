#!/usr/bin/env python3
"""Weekly: set the email each deliverability TEST campaign sends.

Three kinds of test campaign:

  * INBOX tests ("DT <client> #n", made by dt_campaigns.py; the weekly Smart
    Delivery v2 tests run on them) send a fixed neutral note with tracking off
    (smartlead/placement_copy.NEUTRAL_*). A fail then means the inbox or its
    domain, never the copy (Manveen, 11 Nov 2026).
  * The standing test campaigns below send what is really being sent: the
    client's MOST ACTIVE campaign of the week (most emails sent in the last 7
    days; else the newest active one; else its last active paused/completed
    one) is copied into the first step.
  * COPY checks ("DT <client> copy check", one per client) grade the email, not
    the inbox: they carry that same live copy (their tracking settings are left
    as they are) and send it from a few inboxes that just passed the neutral note. Every run
    also keeps their senders in step: up to 5 Google-hosted and 5 Outlook-hosted
    inboxes of the client, 2 per domain, connected, no DO NOT USE tag, newest v2
    verdict a pass from the last 10 days (smartlead/placement_copy.
    pick_copy_check_senders, read from Mongo v2_verdicts). If nobody qualifies,
    or the verdicts cannot be read, the senders stay as they are. The PC job
    (v2_weekly.py) reads the test's CSV as a verdict on the copy and never on
    the inboxes.

Posts "Message sequence refreshed" to Slack (plus a line per test campaign it
could not refresh). Live campaigns are only read, never written.

    python3 copy_sync.py               # dry run: says what it would write
    python3 copy_sync.py --apply       # write
    python3 copy_sync.py --only BETTRDATA
    python3 copy_sync.py --apply --skip 111,222   # leave these test campaigns alone (a test is running on them)
    python3 copy_sync.py --create-copy-check "DT Melior" --apply   # make "DT Melior copy check" (once per client)
    python3 copy_sync.py --only-copy-check "DT Melior" --apply     # refresh that one copy check now
    python3 copy_sync.py --only-copy-check "DT Melior" --source 123456 --apply
                                       # before launch: load campaign 123456's first email (a draft too) into it

The last three touch one copy-check campaign and nothing else, and post nothing.
--create-copy-check clones the client's first "DT <client> #n" campaign
(dt_senders._clone: one step, same settings and schedule, placeholder lead,
left PAUSED and never started), then gives it its copy and senders; it refuses
to make a second one.

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
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.placement_copy import (COPY_CHECK_MAX_AGE_DAYS, pick_copy_check_senders, refresh_test_campaign,
                                      write_neutral_note)
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


# "DT <prefix> copy check": the client's copy-check campaign, and the client (smartlead.report_sheet.client_label,
# as v2_weekly.load_fleet labels inboxes) whose inboxes it may hold.
COPY_CHECK = "copy check"
COPY_CHECK_CLIENT = {"DT PL": "Precise Leads", "DT Melior": "Melior", "DT BettrData": "Bettrdata"}
SAFE_STATUSES = {"PAUSED", "DRAFTED"}        # senders are only changed on a campaign that sends nothing by itself


def is_inbox_test(name: str) -> bool:
    """A "DT <known client> #n" campaign: an inbox test, which sends the neutral note."""
    n = str(name or "")
    return any(n.startswith(p + " #") for p in DT_SOURCE_CLIENT)


def copy_check_prefix(name: str) -> str | None:
    """The known client prefix of a "DT <prefix> copy check" campaign (any case of "copy check"), else None."""
    n = str(name or "").strip()
    return next((p for p in DT_SOURCE_CLIENT
                 if n.startswith(p + " ") and n[len(p):].strip().lower() == COPY_CHECK), None)


def is_copy_check(name: str) -> bool:
    """A "DT <known client> copy check" campaign: it carries the client's live copy and is never an inbox test."""
    return copy_check_prefix(name) is not None


def _today() -> date:
    return date.today()


def read_verdicts(today: date) -> list[dict]:
    """Mongo v2_verdicts rows (email, night, verdict) recent enough to pick a copy-check sender on. Raises when
    Mongo cannot be read; the caller then changes no senders."""
    from renewal_review import _collection
    since = (today - timedelta(days=COPY_CHECK_MAX_AGE_DAYS)).isoformat()
    return list(_collection("v2_verdicts").find({"night": {"$gte": since}},
                                                {"_id": 0, "email": 1, "night": 1, "verdict": 1}))


async def _attached(c, campaign_id: int) -> set[int]:
    return {int(a.get("id") or a.get("email_account_id") or 0)
            for a in await c.get_campaign_email_accounts(str(campaign_id))}


async def sync_copy_check_senders(c, account: str, campaign_id: int, status: str, prefix: str, inboxes: list[dict],
                                  verdicts: list[dict], today: date, apply: bool) -> dict:
    """Keep one copy-check campaign on the client's inboxes that pass now: attach the picked ones that are
    missing, detach attached ones no longer picked, then read the list back. Nobody picked: nothing changes.
    Returns {"text": what happened, "problem": None or why it needs a person}. ``c`` is an open Smartlead client.
    Never raises, and never prints a Smartlead error's text (it can hold the request URL)."""
    from smartlead.report_sheet import client_label
    mine = [a for a in inboxes if client_label(account, a.get("client_id")) == COPY_CHECK_CLIENT[prefix]]
    picks = pick_copy_check_senders(mine, verdicts, today)
    if not picks:
        return {"text": "no inbox qualifies (connected, no DO NOT USE tag, a v2 pass in the last "
                        f"{COPY_CHECK_MAX_AGE_DAYS} days): senders left as they are", "problem": None}
    if status not in SAFE_STATUSES:
        return {"text": f"campaign is {status}: senders left as they are",
                "problem": f"is {status}, so its senders were not changed (a copy check stays paused)"}
    email = {int(a["id"]): str(a.get("from_email") or a["id"]) for a in inboxes if a.get("id") is not None}
    names = lambda ids: ", ".join(email.get(i, str(i)) for i in ids) or "none"      # noqa: E731
    try:
        before = await _attached(c, campaign_id)
        attach = [i for i in picks if i not in before]
        detach = sorted(before - set(picks))
        if not attach and not detach:
            return {"text": f"senders already in step ({len(picks)})", "problem": None}
        if not apply:
            return {"text": f"dry-run: would attach {names(attach)}; would detach {names(detach)}", "problem": None}
        if attach:
            await c.add_campaign_email_accounts(str(campaign_id), attach)
        if detach:
            await c.remove_campaign_email_accounts(str(campaign_id), detach)
        after = await _attached(c, campaign_id)
    except Exception as exc:  # noqa: BLE001 - the type only: the text can hold the URL
        return {"text": f"sender change failed ({type(exc).__name__})",
                "problem": f"its senders could not be changed ({type(exc).__name__})"}
    missing, still = [i for i in attach if i not in after], [i for i in detach if i in after]
    text = f"attached {names([i for i in attach if i in after])}; detached {names([i for i in detach if i not in after])}"
    if missing or still:
        why = "; ".join(x for x in (f"{len(missing)} inbox(es) not attached after the add" if missing else "",
                                    f"{len(still)} inbox(es) still attached after the removal" if still else "") if x)
        return {"text": f"{text}; {why}", "problem": why}
    return {"text": text, "problem": None}


async def create_copy_check(c, dt_campaigns: list[dict], prefix: str, apply: bool) -> dict:
    """Make "DT <prefix> copy check" in this account by cloning the client's first "DT <prefix> #n" campaign
    (dt_senders._clone: one step, same settings and schedule, placeholder lead, PAUSED, never started).
    Returns {"status": exists | none | would | created | failed, "text", "row": the new campaign or None}.
    Never makes a second one."""
    from dt_senders import _clone, _dt_number
    name = f"{prefix} {COPY_CHECK}"
    have = next((x for x in dt_campaigns if copy_check_prefix(x.get("name")) == prefix), None)
    if have:
        return {"status": "exists", "row": None,
                "text": f"'{have['name']}' already exists (campaign {have['id']}); not created again"}
    tests = sorted((x for x in dt_campaigns if str(x.get("name", "")).startswith(prefix + " #")),
                   key=lambda x: _dt_number(x["name"]))
    if not tests:
        return {"status": "none", "row": None, "text": ""}
    src = tests[0]
    if not apply:
        return {"status": "would", "row": None,
                "text": f"dry-run: would create '{name}' by cloning '{src['name']}' ({src['id']}), left paused"}
    try:
        new_id = await _clone(c, int(src["id"]), name)
    except Exception as exc:  # noqa: BLE001 - the type only: the text can hold the URL
        return {"status": "failed", "row": None,
                "text": f"could not create '{name}' ({type(exc).__name__}); check Smartlead for a half-made campaign "
                        f"of that name before trying again"}
    return {"status": "created", "row": {"id": new_id, "name": name, "status": "PAUSED"},
            "text": f"created '{name}' ({new_id}) by cloning '{src['name']}', left paused"}


async def first_step_id(acc, campaign_id: int) -> int | None:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        seq = await c._get(f"/campaigns/{campaign_id}/sequences")
    steps = seq if isinstance(seq, list) else seq.get("sequences", [])
    steps = sorted(steps, key=lambda s: s.get("seq_number", 0))
    return int(steps[0]["id"]) if steps else None


async def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--only", help="account name, e.g. BETTRDATA")
    ap.add_argument("--skip", default="", help="comma-separated test campaign ids to leave alone this run")
    ap.add_argument("--only-copy-check", choices=list(DT_SOURCE_CLIENT), metavar="PREFIX",
                    help='touch only this client\'s copy-check campaign, e.g. "DT Melior"')
    ap.add_argument("--source", type=int, metavar="CAMPAIGN_ID",
                    help="with --only-copy-check: take the email from this campaign's first step (a draft too) "
                         "instead of the most active campaign")
    ap.add_argument("--create-copy-check", choices=list(DT_SOURCE_CLIENT), metavar="PREFIX",
                    help='create "<PREFIX> copy check" if it does not exist (dry run unless --apply)')
    args = ap.parse_args(argv)
    if args.source is not None and not args.only_copy_check:
        ap.error('--source needs --only-copy-check "<prefix>": it loads one campaign into one copy check')
    if args.only_copy_check and args.create_copy_check and args.only_copy_check != args.create_copy_check:
        ap.error("--only-copy-check and --create-copy-check name different clients")
    one = args.only_copy_check or args.create_copy_check       # a run for one copy-check campaign, nothing else
    found_one = False
    made: list[str] = []
    verdicts: list[dict] | None = None
    verdicts_read = False
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
            if args.create_copy_check:
                new = await create_copy_check(c, dt_names, args.create_copy_check, args.apply)
                if new["text"]:
                    print(f"[CopySync] {name}: {new['text']}")
                made.append(new["status"])
                if new["status"] in ("exists", "failed"):
                    return 1
                if new["row"]:
                    dt_names.append(new["row"])
        plan = {} if one else dict(standing)
        names = {int(x["id"]): x["name"] for x in dt_names}
        status = {int(x["id"]): str(x.get("status") or "").upper() for x in dt_names}
        for x in dt_names:
            check = copy_check_prefix(x["name"])
            prefix = check or next((p for p in DT_SOURCE_CLIENT if x["name"].startswith(p + " #")), None)
            if one:
                if check != one:
                    continue
                found_one = True
            elif prefix is None:
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
                                                skip_words=SKIP_WORDS.get((name, client_id), ()),
                                                source_campaign_id=args.source if one else None)
                if args.source is not None and not r.get("hash"):
                    # The named email is not in the copy check: a test run now would grade the old one.
                    problems.append(f":warning: {name}: {names.get(int(test_id), test_id)} was not loaded from "
                                    f"campaign {args.source}")
                    failed += 1
            state = "UPDATED" if r["written"] else "kept"
            print(f"[CopySync] {name} test {test_id}: {state} - source "
                  f"{r['source']} - {r['reason']}")
            attempted += 1
            written += bool(r["written"])
        # Copy-check campaigns: keep their senders on the client's inboxes that pass the plain note now.
        checks = [(int(t), copy_check_prefix(names.get(int(t), ""))) for t in plan
                  if int(t) not in skip and copy_check_prefix(names.get(int(t), ""))]
        if checks and not verdicts_read:
            verdicts_read = True
            try:
                verdicts = read_verdicts(_today())
            except Exception as exc:  # noqa: BLE001 - the type only: the text can hold the Mongo URI
                msg = f"copy check: v2_verdicts could not be read ({type(exc).__name__}), so no copy-check senders were changed"
                print(f"[CopySync] {msg}")
                problems.append(":warning: " + msg)
                failed += 1
        if checks and verdicts is not None:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                try:
                    inboxes = await c.list_email_accounts()
                except Exception as exc:  # noqa: BLE001 - the type only: the text can hold the URL
                    inboxes = None
                    msg = f"{name}: inboxes could not be read ({type(exc).__name__}), so no copy-check senders were changed"
                    print(f"[CopySync] {msg}")
                    problems.append(":warning: " + msg)
                    failed += 1
                for test_id, prefix in checks if inboxes is not None else ():
                    r = await sync_copy_check_senders(c, acc.name, test_id, status.get(test_id, ""), prefix, inboxes,
                                                      verdicts, _today(), args.apply)
                    print(f"[CopySync] {name} {names[test_id]} senders: {r['text']}")
                    if r["problem"]:
                        problems.append(f":warning: {name}: {names[test_id]} {r['problem']}")
                        failed += 1
    if args.create_copy_check and not any(s != "none" for s in made):
        print(f"[CopySync] no '{args.create_copy_check} #n' campaign to clone in the account(s) read: nothing created")
        return 1
    if one and not found_one and "would" not in made:
        print(f"[CopySync] no '{one} {COPY_CHECK}' campaign in the account(s) read "
              f"(create it: copy_sync.py --create-copy-check \"{one}\" --apply)")
        return 1
    if args.apply and (attempted or problems) and not one:
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
