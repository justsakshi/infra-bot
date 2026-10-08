#!/usr/bin/env python3
"""Read finished SmartDelivery tests and report, per client: spam by provider
pair, flagged inboxes, which of them sit in an ACTIVE campaign. Dry-run
(prints) unless a write flag is given.

    python3 placement_report.py --account PRECISE_LEADS --tests 547669
    python3 placement_report.py --account BETTRDATA --since 2026-10-06 --post --status-tab

  --post         post one Slack message per client (channel C0AGVSUNEFP)
  --status-tab   write the "Inbox Status" tab Campaign Desk reads
  --grid         write the domain cells into the client's grid tab
  --sheet        both sheet writes
  --save         keep per-inbox status in Mongo
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import httpx

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except Exception:
    pass

from smartlead.accounts import discover_accounts
from smartlead.config import SMARTDELIVERY_BASE_URL
from smartlead.placement_report import active_campaign, build_report, format_slack, mx_provider
from smartlead.report_sheet import client_label, grid_writes, write_status_tab

DEFAULT_CHANNEL = "C0AGVSUNEFP"   # Slack channel for the weekly deliverability report
SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}


MIN_CLASSIFIED = 0.9   # SmartDelivery marks a test COMPLETED before every seed is classified


def _posted_collection():
    """Mongo record of which tests were already reported, so hourly runs post once."""
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        from smartlead.config import HEALTH_HISTORY_DB
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client[HEALTH_HISTORY_DB]["placement_reports_posted"]
    except Exception as exc:  # noqa: BLE001
        print(f"  [Report] Mongo unavailable: {exc}")
        return None


def _get(url, key, **params):
    r = httpx.get(url, params={"api_key": key, **params}, headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


def accounts_by_email(key):
    out, off = {}, 0
    while True:
        page = _get(f"{SL}/email-accounts/", key, offset=off, limit=100)
        for a in page:
            out[a["from_email"].lower()] = a
        if len(page) < 100:
            return out
        off += 100


def active_attachments(key):
    """{inbox email: [active campaign names]}."""
    out = {}
    for c in _get(f"{SL}/campaigns", key):
        if str(c.get("status", "")).upper() != "ACTIVE":
            continue
        if not active_campaign(_get(f"{SL}/campaigns/{c['id']}/analytics", key)):
            continue
        for a in _get(f"{SL}/campaigns/{c['id']}/email-accounts", key) or []:
            out.setdefault(a["from_email"].lower(), []).append(c["name"][:40])
    return out


def pick_tests(key, since, ids):
    if ids:
        out = []
        for i in ids:
            d = _get(f"{SMARTDELIVERY_BASE_URL}/spam-test/{i}", key)
            out.append({"spam_test_id": i, "test_name": d.get("test_name", str(i)),
                        "created_at": d.get("created_at", "")})
        return out
    r = httpx.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report", params={"api_key": key},
                   json={"limit": 50}, headers=UA, timeout=60)
    r.raise_for_status()
    return [t for t in r.json() if t["status"] == "COMPLETED" and t["created_at"][:10] >= since]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", required=True)
    ap.add_argument("--tests", help="comma-separated test ids")
    ap.add_argument("--since", default=date.today().isoformat())
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--save", action="store_true")
    ap.add_argument("--grid", action="store_true")
    ap.add_argument("--status-tab", action="store_true")
    ap.add_argument("--sheet", action="store_true")
    ap.add_argument("--only-new", action="store_true",
                    help="skip tests already reported; wait until 90%% of a test's seeds are classified")
    ap.add_argument("--final", action="store_true",
                    help="with --only-new: report even if some seeds are still unclassified")
    args = ap.parse_args()
    acc = next((a for a in discover_accounts() if a.name.upper() == args.account.upper()), None)
    if not acc:
        print(f"no API key for {args.account}")
        return 1
    key = acc.api_key
    ids = [int(x) for x in args.tests.split(",")] if args.tests else []
    tests = pick_tests(key, args.since, ids)
    if not tests:
        print("no completed test to report")
        return 0

    posted = _posted_collection() if args.only_new else None
    if args.only_new and posted is not None:
        done = {d["test_id"] for d in posted.find({"account": acc.name}, {"test_id": 1})}
        tests = [t for t in tests if t["spam_test_id"] not in done]
        if not tests:
            print("nothing new to report")
            return 0
    elif args.only_new:
        print("Mongo unavailable: cannot tell which tests were already reported")
        if not args.final:
            return 0

    senders: dict[str, list] = {}
    ready = []
    for t in tests:
        url = f"{SMARTDELIVERY_BASE_URL}/spam-test/report/{t['spam_test_id']}/sender-account-wise"
        rows_t, total, classified = [], 0, 0
        for e in _get(url, key):
            for d in e.get("details") or []:
                folder = (d.get("reply") or {}).get("mail_folder", "")
                total += 1
                classified += bool(str(folder).strip())
                rows_t.append((e["email"].lower(), {"seed": d["email"], "folder": folder}))
        share = classified / total if total else 0.0
        if args.only_new and share < MIN_CLASSIFIED and not args.final:
            print(f"test {t['spam_test_id']}: only {share:.0%} of seeds classified, waiting")
            continue
        ready.append(t)
        for em, row in rows_t:
            senders.setdefault(em, []).append(row)
    tests = ready
    if not tests:
        return 0
    accts = accounts_by_email(key)
    types = {e: a["type"] for e, a in accts.items()}
    client_by_email = {e: client_label(acc.name, a.get("client_id")) for e, a in accts.items()}
    attached = active_attachments(key)
    cache: dict[str, str] = {}

    def seed_provider(d):
        if d not in cache:
            cache[d] = mx_provider(d)
        return cache[d]

    # One report per client: the Precise Leads account also holds Melior and OSC.
    by_client: dict[str, dict] = {}
    for email, rows in senders.items():
        by_client.setdefault(client_by_email.get(email, acc.name), {})[email] = rows

    tested_on = max(str(t.get("created_at", ""))[:10] for t in tests) or date.today().isoformat()
    do_grid, do_status = args.grid or args.sheet, args.status_tab or args.sheet
    names = [str(t["test_name"]) for t in tests]
    code = 0
    for client, group in sorted(by_client.items()):
        report = build_report(group, types, seed_provider, attached)
        text = format_slack(client, report, names)
        print("=" * 60)
        print(text)
        cmap = {e: client for e in group}
        grid = grid_writes(report, cmap, tested_on, apply=do_grid)
        status = write_status_tab(report, cmap, tested_on, apply=do_status)
        print(f"\n[Sheet] {client}: test date {tested_on}; grid {'WROTE' if do_grid else 'dry-run'} "
              f"({sum(g.get('writes', 0) for g in grid)} cells), "
              f"Inbox Status {'WROTE' if status.get('written') else 'dry-run'} ({status['rows']} rows)")
        if args.save:
            from smartlead.placement_store import PlacementStore
            store = PlacementStore()
            for i in report["inboxes"].values():
                store.save_result(i["email"], i["domain"], i["status"], tested_on, "weekly_report")
        if args.post:
            from smartlead.notify import _post
            token = os.getenv("SLACK_BOT_TOKEN", "")
            channel = os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL
            if not token:
                print("SLACK_BOT_TOKEN missing - not posted")
                code = 1
            else:
                ok = _post(token, channel, text)
                print("posted" if ok else "post FAILED")
                code = code or (0 if ok else 1)
                if ok and posted is not None:
                    for t in tests:
                        posted.update_one({"account": acc.name, "test_id": t["spam_test_id"]},
                                          {"$set": {"account": acc.name, "test_id": t["spam_test_id"],
                                                    "client": client, "posted_on": date.today().isoformat()}},
                                          upsert=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
