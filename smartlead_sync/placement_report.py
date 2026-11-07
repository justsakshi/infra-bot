#!/usr/bin/env python3
"""Read finished SmartDelivery tests and report, per client: spam by provider
pair, flagged inboxes, which of them sit in an ACTIVE campaign. Dry-run
(prints) unless a write flag is given.

    python3 placement_report.py --account PRECISE_LEADS --tests 547669
    python3 placement_report.py --account BETTRDATA --since 2026-10-06 --post --status-tab

  --post         post one Slack message per client, details in its thread (channel C0AGVSUNEFP)
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

from smartlead.api import raise_for_status
from smartlead.accounts import discover_accounts
from smartlead.config import SMARTDELIVERY_BASE_URL
from smartlead.placement_report import (active_campaign, build_report, find_scheduled, format_slack,
                                        missing_test_text, missing_tests, mx_provider, select_tests)
from smartlead.report_sheet import client_label, grid_writes, write_status_tab

DEFAULT_CHANNEL = "C0AGVSUNEFP"   # Slack channel for the weekly deliverability report
SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}


MIN_CLASSIFIED = 0.9   # SmartDelivery marks a test COMPLETED before every seed is classified
LISTING = 50           # newest tests read each run (ad-hoc tests and new scheduled ones)
PAGE = 100             # page size when looking further back for a remembered scheduled test
SCHEDULED = "classic_scheduled_tests"   # every scheduled test seen, so it is found after it leaves the window


def _collection(name: str = "placement_reports_posted"):
    """A Mongo collection in the health DB, or None. placement_reports_posted records which
    tests were already reported, so hourly runs post once."""
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        from smartlead.config import HEALTH_HISTORY_DB
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client[HEALTH_HISTORY_DB][name]
    except Exception as exc:  # noqa: BLE001
        print(f"  [Report] Mongo unavailable: {exc}")
        return None


def _posted_collection():
    return _collection("placement_reports_posted")


def _get(url, key, **params):
    r = httpx.get(url, params={"api_key": key, **params}, headers=UA, timeout=120)
    raise_for_status(r)  # no key in the error text
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


def _sent_lately(key, cid, days, today):
    """Did the campaign send anything in the last ``days`` days? Unreadable
    counts as yes, so a possibly live campaign is never hidden."""
    from datetime import timedelta
    try:
        r = _get(f"{SL}/campaigns/{cid}/analytics-by-date", key,
                 start_date=(today - timedelta(days=days)).isoformat(), end_date=today.isoformat())
        return int(r.get("sent_count") or 0) > 0
    except Exception:  # noqa: BLE001
        return True


def attachments(key, sending_days=None, today=None):
    """{inbox email: {campaigns: [switched-on campaigns with leads left],
    sending: [those that sent in the last ``sending_days`` days]}}. Live
    2026-10-13: ACTIVE campaigns with leads mid-sequence had sent nothing for
    two weeks, so "switched on" alone does not mean "sending"."""
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date
    today = today or date.today()

    def one(c):        # per switched-on campaign: still has leads? sent lately? its inboxes
        if not active_campaign(_get(f"{SL}/campaigns/{c['id']}/analytics", key)):
            return None
        sending = sending_days is not None and _sent_lately(key, c["id"], sending_days, today)
        return c["name"][:40], sending, _get(f"{SL}/campaigns/{c['id']}/email-accounts", key) or []

    active = [c for c in _get(f"{SL}/campaigns", key) if str(c.get("status", "")).upper() == "ACTIVE"]
    with ThreadPoolExecutor(max_workers=5) as pool:          # side by side; map keeps the campaign order
        results = list(pool.map(one, active))
    out = {}
    for res in results:
        if res is None:
            continue
        name, sending, inboxes = res
        for a in inboxes:
            row = out.setdefault(a["from_email"].lower(), {"campaigns": [], "sending": []})
            row["campaigns"].append(name)
            if sending:
                row["sending"].append(name)
    return out


def active_attachments(key):
    """{inbox email: [active campaign names]}."""
    return {e: v["campaigns"] for e, v in attachments(key).items()}


def _list_tests(key, limit, offset=0):
    """SmartDelivery's test list, newest first (offset measured to work 7 Nov 2026)."""
    body = {"limit": limit, **({"offset": offset} if offset else {})}
    r = httpx.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report", params={"api_key": key},
                   json=body, headers=UA, timeout=60)
    raise_for_status(r)  # no key in the error text
    rows = r.json()
    return rows if isinstance(rows, list) else []


def _remembered(col, account) -> dict[int, str]:
    """{test_id: created_at} of the ACTIVE scheduled tests seen in earlier runs."""
    try:
        return {int(d["test_id"]): str(d.get("created_at") or "")
                for d in col.find({"account": account, "status": "ACTIVE"}, {"test_id": 1, "created_at": 1})}
    except Exception as exc:  # noqa: BLE001 - the newest-50 window still works
        print(f"  [Report] scheduled-test memory unreadable: {exc}")
        return {}


def _remember(col, account, rows) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for t in rows:
        if t.get("test_type") != "auto":
            continue
        try:
            col.update_one({"_id": f"{account}|{t['spam_test_id']}"},
                           {"$set": {"account": account, "test_id": t["spam_test_id"],
                                     "test_name": t.get("test_name"), "created_at": t.get("created_at"),
                                     "status": t.get("status"), "every_days": t.get("every_days"),
                                     "schedule_start_time": t.get("schedule_start_time"),
                                     "test_end_date": t.get("test_end_date"), "last_seen": now}},
                           upsert=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Report] could not remember scheduled test {t.get('spam_test_id')}: {exc}")
            return


def pick_tests(key, since, ids, account="", scheduled=None, remember=False):
    """Tests whose latest run is on or after ``since``: the newest LISTING tests, plus every
    ACTIVE scheduled test remembered in ``scheduled`` (Mongo) that has left that window
    (audit PLACEMENT-9: the weekly tests drop out of the newest 50 within weeks). With
    ``remember``, the scheduled tests seen are saved for later runs (not on a dry run)."""
    if ids:
        out = []
        for i in ids:
            d = _get(f"{SMARTDELIVERY_BASE_URL}/spam-test/{i}", key)
            out.append({"spam_test_id": i, "test_name": d.get("test_name", str(i)),
                        "created_at": d.get("created_at", "")})
        return out
    listing = _list_tests(key, LISTING)
    if scheduled is not None:
        older, lost = find_scheduled(listing, _remembered(scheduled, account),
                                     lambda off: _list_tests(key, PAGE, off), page_size=PAGE)
        if older:
            print(f"  [Report] {len(older)} scheduled test(s) found beyond the newest {LISTING}")
        if lost:
            print(f"  [Report] scheduled test(s) not found in the list: {', '.join(map(str, lost))}")
        listing = listing + older
        if remember:
            _remember(scheduled, account, listing)
    return select_tests(listing, since)


def _covered(posted, account, day) -> set[str]:
    """Clients that already had a scheduled (weekly) test reported for run day ``day``.
    Old records have no run_day/test_type: they count by posted_on and their client."""
    out: set[str] = set()
    try:
        for d in posted.find({"account": account, "$or": [{"run_day": day}, {"posted_on": day}]}):
            if d.get("test_type", "auto") != "auto" or d.get("run_day", day) != day:
                continue
            out.update(d.get("clients") or ([d["client"]] if d.get("client") else []))
    except Exception as exc:  # noqa: BLE001
        print(f"  [Report] could not read earlier reports: {exc}")
    return out


def _gone_client(email, tests_of, clients_of, fallback) -> str:
    """The client a sender that is no longer in Smartlead was tested for: the most common
    client among the other senders of its tests."""
    from collections import Counter
    votes = Counter()
    for tid in tests_of.get(email, ()):
        votes.update(clients_of.get(tid, {}))
    return votes.most_common(1)[0][0] if votes else fallback


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
                    help="with --only-new: report even if some seeds are still unclassified, and warn "
                         "for each expected client with no scheduled test reported today")
    args = ap.parse_args()
    acc = next((a for a in discover_accounts() if a.name.upper() == args.account.upper()), None)
    if not acc:
        print(f"no API key for {args.account}")
        return 1
    key = acc.api_key
    ids = [int(x) for x in args.tests.split(",")] if args.tests else []
    writes = args.post or args.save or args.status_tab or args.grid or args.sheet
    tests = pick_tests(key, args.since, ids, acc.name, None if ids else _collection(SCHEDULED), remember=writes)
    posted = _collection() if args.only_new else None
    covered = _covered(posted, acc.name, args.since) if posted is not None else set()

    code = 0
    if not tests:
        print("no completed test to report")
    elif args.only_new and posted is not None:
        # A scheduled test reuses its id every week, so a report is one (test, run).
        done = {(d["test_id"], d.get("run_no", 0))
                for d in posted.find({"account": acc.name}, {"test_id": 1, "run_no": 1})}
        tests = [t for t in tests if (t["spam_test_id"], t.get("run_no", 0)) not in done]
        if not tests:
            print("nothing new to report")
    elif args.only_new:
        print("Mongo unavailable: cannot tell which tests were already reported")
        if not args.final:
            return 0
    if tests:
        code, reported = _report(args, acc, key, tests, posted)
        covered |= reported
    if args.final:
        code = _warn_missing(args, acc, covered) or code
    return code


def _warn_missing(args, acc, covered: set[str]) -> int:
    """13:00 final run: one warning per expected client with no scheduled test reported
    today (audit REACH-14: a test that did not run was silent)."""
    code = 0
    for client, expected in missing_tests(acc.name, args.since, covered):
        head, thread = missing_test_text(client, expected, args.since)
        print("=" * 60)
        print("\n".join(head))
        for part in thread:
            print("\n--- thread reply ---")
            print(part)
        if args.post:
            from smartlead.notify import post_with_thread
            token = os.getenv("SLACK_BOT_TOKEN", "")
            channel = os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL
            ok = bool(token and post_with_thread(token, channel, head, thread))
            print("posted" if ok else "post FAILED")
            code = code or (0 if ok else 1)
    return code


def _report(args, acc, key, tests, posted) -> tuple[int, set[str]]:
    """Read, report and record the given tests. Returns (exit code, clients reported from a
    scheduled test)."""
    senders: dict[str, list] = {}
    tests_of: dict[str, set] = {}
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
            tests_of.setdefault(em, set()).add(t["spam_test_id"])
    tests = ready
    if not tests:
        return 0, set()
    accts = accounts_by_email(key)
    types = {e: a["type"] for e, a in accts.items()}
    client_by_email = {e: client_label(acc.name, a.get("client_id")) for e, a in accts.items()}
    fallback = client_label(acc.name, None)
    if not accts:
        print("  [Report] the account lists no inboxes: every sender is reported under the account")
    attached = active_attachments(key)
    cache: dict[str, str] = {}

    def seed_provider(d):
        if d not in cache:
            cache[d] = mx_provider(d)
        return cache[d]

    # One report per client: the Precise Leads account also holds Melior and OSC. A sender
    # not in the account now is "not in Smartlead now": left out of the counts and the tab,
    # and named under the client the rest of its test belongs to (audit PLACEMENT-13).
    clients_of: dict[int, dict[str, int]] = {}
    for email, tids in tests_of.items():
        if email in client_by_email:
            for tid in tids:
                c = clients_of.setdefault(tid, {})
                c[client_by_email[email]] = c.get(client_by_email[email], 0) + 1
    by_client: dict[str, dict] = {}
    gone_by_client: dict[str, list] = {}
    for email, rows in senders.items():
        if email in client_by_email or not accts:
            by_client.setdefault(client_by_email.get(email, fallback), {})[email] = rows
        else:
            gone_by_client.setdefault(_gone_client(email, tests_of, clients_of, fallback), []).append(email)

    # Blocklists: each reported client's domains (all its inboxes in Smartlead now).
    from collections import Counter
    from smartlead import classic_blocklist as cb
    reported = sorted(set(by_client) | set(gone_by_client))
    doms_of = {c: {e.split("@")[1] for e, cl in client_by_email.items() if cl == c} for c in reported}
    latest = cb.read_latest(set().union(*doms_of.values()) if doms_of else set(), date.today())
    inboxes_on = Counter(e.split("@")[1] for e in client_by_email)
    live_on = Counter(e.split("@")[1] for e in client_by_email if attached.get(e))

    tested_on = max(str(t.get("created_at", ""))[:10] for t in tests) or date.today().isoformat()
    do_grid, do_status = args.grid or args.sheet, args.status_tab or args.sheet
    names = [str(t["test_name"]) for t in tests]
    gone_all = {e for es in gone_by_client.values() for e in es}
    code, covered = 0, set()
    for client in reported:
        group = by_client.get(client, {})
        report = build_report(group, types, seed_provider, attached)
        blocklist = cb.summarise(doms_of[client], latest, date.today())
        for dom in blocklist["listed"]:
            live = live_on.get(dom, 0)
            blocklist["notes"][dom] = (f"{inboxes_on.get(dom, 0)} inboxes, "
                                       + (f"{live} in live campaigns" if live else "none in a live campaign"))
        headline, thread = format_slack(client, report, names, tested_on, blocklist=blocklist,
                                        gone=gone_by_client.get(client, []))
        print("=" * 60)
        print("\n".join(headline))
        for part in thread:
            print("\n--- thread reply ---")
            print(part)
        cmap = {e: client for e in group}
        grid = grid_writes(report, cmap, tested_on, apply=do_grid)
        status = write_status_tab(report, cmap, tested_on, apply=do_status, account_name=acc.name,
                                  account_emails=set(accts), gone=gone_all)
        print(f"\n[Sheet] {client}: test date {tested_on}; grid {'WROTE' if do_grid else 'dry-run'} "
              f"({sum(g.get('writes', 0) for g in grid)} cells), "
              f"Inbox Status {'WROTE' if status.get('written') else 'dry-run'} ({status['rows']} rows)")
        if args.save:
            from smartlead.placement_store import PlacementStore
            store = PlacementStore()
            for i in report["inboxes"].values():
                store.save_result(i["email"], i["domain"], i["status"], tested_on, "weekly_report")
        # the tests this client's senders (in Smartlead or not) were in
        mine_ids = set().union(*(tests_of[e] for e in [*group, *gone_by_client.get(client, [])]))
        mine = [t for t in tests if t["spam_test_id"] in mine_ids]
        ok = True
        if args.post:
            from smartlead.notify import post_with_thread
            token = os.getenv("SLACK_BOT_TOKEN", "")
            channel = os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL
            if not token:
                print("SLACK_BOT_TOKEN missing - not posted")
                ok, code = False, 1
            else:
                ok = bool(post_with_thread(token, channel, headline, thread))
                print("posted" if ok else "post FAILED")
                code = code or (0 if ok else 1)
            if ok and posted is not None:
                for t in mine:
                    run = t.get("run_no", 0)
                    posted.update_one({"account": acc.name, "test_id": t["spam_test_id"], "run_no": run},
                                      {"$set": {"account": acc.name, "test_id": t["spam_test_id"],
                                                "run_no": run, "client": client,
                                                "run_day": str(t.get("created_at", ""))[:10],
                                                "test_type": t.get("test_type") or "manual",
                                                "posted_on": date.today().isoformat()},
                                       "$addToSet": {"clients": client}},
                                      upsert=True)
        if ok and any(t.get("test_type") == "auto" and str(t.get("created_at", ""))[:10] == args.since
                      for t in mine):
            covered.add(client)
    return code, covered


if __name__ == "__main__":
    raise SystemExit(main())
