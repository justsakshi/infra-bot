#!/usr/bin/env python3
"""Weekly: what real recipients say, by sending inbox type (Google, Outlook) and recipient provider. Rules and the
post: smartlead/real_recipients.py.

    python real_recipients.py                 print it, post nothing (default)
    python real_recipients.py --to U09TQ9D7YNM post to one person (a test)
    python real_recipients.py --post --save   post to REAL_RECIPIENTS_CHANNEL (default the infra channel) and keep
                                              the week's cells in Mongo (real_recipients, one doc per day)

Read-only against Smartlead (campaign list, statistics, leads, message history) and DNS. One message-history call per
looked-up lead, paced to stay well under Smartlead's 200 calls a minute, so a run takes about an hour. Scheduled in
index.js: Sundays 07:00 IST, when no other job reads Smartlead. REAL_RECIPIENTS_POST=off stops it.

It also decides whether Smartlead's v2 test is trusted per inbox type (rr.trust_decision) and, with --save, keeps that
in Mongo v2_trust, which the list, the daily inbox post, the gate self-test and the v2 job read (usable.refresh_trust).
V2_TRUST_AUTO=off: the decision is shown but not saved. V2_UNTRUSTED_TYPES on any reader overrides it.
Exits 1 when no account could be read or the post was refused.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from provider_reply_watch import mx_providers
from smartlead import provider_watch as pw
from smartlead import real_recipients as rr
from smartlead.accounts import PAST_CLIENT_ACCOUNTS, discover_accounts
from smartlead.api import SmartleadClient
from smartlead.report_sheet import client_label
from smartlead.usable import kind_of

IST = timezone(timedelta(hours=5, minutes=30))
CHANNEL = os.getenv("REAL_RECIPIENTS_CHANNEL", "C0AGVSUNEFP")
COLLECTION = "real_recipients"
PAUSE = 0.35                      # seconds between message-history calls: under 200 a minute with other calls
LIVE = {"ACTIVE", "PAUSED", "COMPLETED", "STOPPED"}
SKIP_NAMES = ("DT ", "One-off placement test")


async def _history(c, cid: str, lid) -> tuple[str | None, str, str | None]:
    if not lid:
        return None, "", "NoLeadId"
    try:
        h = await c._get(f"/campaigns/{cid}/leads/{lid}/message-history")
    except Exception as exc:  # noqa: BLE001 - the type only: the error text can hold the URL, the URL the key
        return None, "", type(exc).__name__
    sender, body = rr.read_history(h)
    return sender, body, None


async def read_account(acc, today: date, days: int, errors: Counter, rng: random.Random,
                       all_kinds: dict | None = None) -> list[dict]:
    from smartlead.sheets import _is_test_campaign
    cells = []
    oldest = (today - timedelta(days=days + 120)).isoformat()
    async with SmartleadClient(acc.api_key, acc.name) as c:
        accounts = await c.list_email_accounts() or []
        kinds = {str(a.get("from_email") or "").strip().lower(): rr.sender_of(kind_of(a.get("type"))) for a in accounts}
        if all_kinds is not None:
            all_kinds.update({str(a.get("from_email") or "").strip().lower(): kind_of(a.get("type")) for a in accounts})
        for camp in await c.list_campaigns() or []:
            name = str(camp.get("name") or "")
            status = str(camp.get("status") or "").upper()
            if status not in LIVE or name.startswith(SKIP_NAMES) or _is_test_campaign(name):
                continue
            if status != "ACTIVE" and str(camp.get("created_at") or "")[:10] < oldest:
                continue
            cid = str(camp["id"])
            try:
                ev = pw.lead_events(await c.get_campaign_statistics(cid), today, days)
            except Exception as exc:  # noqa: BLE001
                errors["campaign statistics " + type(exc).__name__] += 1
                continue
            if not ev:
                continue
            try:
                leads = await c.get_campaign_leads(cid)
            except Exception as exc:  # noqa: BLE001
                errors["campaign leads " + type(exc).__name__] += 1
                continue
            ids = {str((x.get("lead") or {}).get("email") or "").strip().lower(): (x.get("lead") or {}).get("id")
                   for x in leads or []}
            prov = await mx_providers({e.split("@", 1)[1] for e in ev})
            by_cell: dict[str, list] = defaultdict(list)
            for e, v in ev.items():
                by_cell[rr.recipient_of(prov.get(e.split("@", 1)[1], ""))].append((e, v["event"]))
            client = str(client_label(acc.name, camp.get("client_id")))
            for rp, items in sorted(by_cell.items()):
                quiet = sorted(e for e, evt in items if not evt)
                mix = Counter()
                for e in rng.sample(quiet, min(rr.SAMPLE, len(quiet))):
                    s, _, err = await _history(c, cid, ids.get(e))
                    if err:
                        errors["message history " + err] += 1
                    else:
                        mix[kinds.get(s or "", "unknown")] += 1
                    await asyncio.sleep(PAUSE)
                ev_by = Counter()
                for e, evt in items:
                    if not evt:
                        continue
                    s, body, err = await _history(c, cid, ids.get(e))
                    if err:
                        errors["message history " + err] += 1
                    if rr.is_bounce(body):
                        await asyncio.sleep(PAUSE)
                        continue
                    ev_by["unknown" if err else kinds.get(s or "", "unknown")] += 1
                    await asyncio.sleep(PAUSE)
                cells.append({"client": client, "campaign": cid, "name": name, "recipient": rp, "leads": len(items),
                              "quiet": len(quiet), "mix": dict(mix), "events_by_sender": dict(ev_by)})
            print(f"[real-recipients] {acc.name} | {client} | {name[:50]} | leads {len(ev)}", flush=True)
    return cells


async def gather(today: date, days: int, all_kinds: dict | None = None) -> tuple[list[dict], dict, int]:
    errors: Counter = Counter()
    rng = random.Random(today.toordinal())
    cells, read = [], 0
    for acc in discover_accounts():
        if acc.name.strip().lower().replace("_", " ") in PAST_CLIENT_ACCOUNTS:
            continue
        try:
            cells += await read_account(acc, today, days, errors, rng, all_kinds)
            read += 1
        except Exception as exc:  # noqa: BLE001 - one dead account must not hide the others
            errors[f"account {acc.name} " + type(exc).__name__] += 1
    return cells, dict(errors), read


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    return MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]


def trust(today: date, cells: list[dict], all_kinds: dict, write: bool) -> tuple[list[str], list[str], str]:
    """(changes, lines, error). Reads the v2 verdicts and the current trust, decides, and with ``write`` keeps it.
    Never raises: on a Mongo problem nothing changes and the post says so."""
    from smartlead import usable
    try:
        db = _db()
        v2 = usable.newest_judged(db["v2_verdicts"].find({}, {"_id": 0, "email": 1, "night": 1, "verdict": 1}))
        share: dict[str, list] = defaultdict(lambda: [0, 0])
        for e, kind in all_kinds.items():
            if e in v2:
                share[kind][1] += 1
                share[kind][0] += v2[e]["verdict"] == "FAIL"
        usable.refresh_trust(db)
        current = set(usable.V2_NOT_TRUSTED)
        new, changes, lines = rr.trust_decision(cells, {k: tuple(v) for k, v in share.items()}, current)
        if write and os.getenv("V2_TRUST_AUTO", "").strip().lower() != "off":
            old = db[usable.TRUST_COLLECTION].find_one({"_id": "current"}) or {}
            since = (old.get("untrusted") or {})
            doc = {"_id": "current", "decided": today.isoformat(), "lines": lines,
                   "untrusted": {k: since.get(k) or {"since": today.isoformat()} for k in sorted(new)}}
            db[usable.TRUST_COLLECTION].replace_one({"_id": "current"}, doc, upsert=True)
            if changes:
                db[usable.TRUST_COLLECTION].replace_one({"_id": f"change-{today.isoformat()}"},
                    {"_id": f"change-{today.isoformat()}", "changes": changes, "lines": lines,
                     "from": sorted(current), "to": sorted(new)}, upsert=True)
        elif changes:
            changes = [c + " (not saved: V2_TRUST_AUTO=off or a dry run)" for c in changes]
        return changes, lines, ""
    except Exception as exc:  # noqa: BLE001
        return [], [], type(exc).__name__


def save(today: date, days: int, cells: list[dict], errors: dict) -> str:
    """Keep the week's cells (no lead addresses in them). Never raises; returns an error text or ""."""
    try:
        db = _db()
        db[COLLECTION].replace_one({"_id": today.isoformat()}, {
            "_id": today.isoformat(), "days": days, "cells": cells, "errors": errors,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, upsert=True)
        return ""
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__


def main() -> int:
    ap = argparse.ArgumentParser(description="Real-recipient replies by sending inbox type and recipient provider")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--post", action="store_true", help="post to REAL_RECIPIENTS_CHANNEL")
    where.add_argument("--to", help="post to this Slack user or channel id instead")
    ap.add_argument("--save", action="store_true", help="keep the week's cells in Mongo")
    ap.add_argument("--today", help="YYYY-MM-DD (default: today in IST)")
    ap.add_argument("--days", type=int, default=rr.DAYS)
    args = ap.parse_args()

    target = args.to or (CHANNEL if args.post else "")
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if target and not token:
        print("[real-recipients] SLACK_BOT_TOKEN missing - cannot post.")
        return 1
    today = date.fromisoformat(args.today) if args.today else datetime.now(IST).date()
    all_kinds: dict = {}
    cells, errors, read = asyncio.run(gather(today, args.days, all_kinds))
    if args.save:
        err = save(today, args.days, cells, errors)
        print(f"[real-recipients] Mongo: {'saved' if not err else 'not saved (' + err + ')'}")
    changes, lines, terr = trust(today, cells, all_kinds, write=bool(args.save and read))
    if terr:
        errors = {**errors, f"v2 trust ({terr}), so it was not changed": 1}
    head, parts = rr.format_post(cells, today, args.days, errors, trust=(changes, lines))
    print("\n".join(head) + "\n\n" + "\n\n".join(parts))
    if not read:
        print("[real-recipients] no Smartlead account could be read")
        return 1
    if not target:
        return 0
    from smartlead.notify import post_with_thread
    if not post_with_thread(token, target, head, parts):
        print("[real-recipients] Slack post failed.")
        return 1
    print(f"[real-recipients] posted to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
