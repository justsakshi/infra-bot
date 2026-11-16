#!/usr/bin/env python3
"""Warm-up repair tracker: seed it from the lead's local files, or preview what the digest would say.
Logic: smartlead/warmup_repair.py. Fleet watch updates it every day (10:30 IST) and hands the digest its lines.
Never changes a warm-up setting and never calls Smartlead.

    python warmup_repair_cli.py seed --changes <t16_warmup_changes.json> --before <t16_warmup_before.json> --dry-run
    python warmup_repair_cli.py seed --changes <...> --before <...>      # writes Mongo warmup_repair (the lead runs it)
    python warmup_repair_cli.py preview                                  # read-only: today's digest lines

The two files hold inbox addresses: they stay outside the repo and are read at run time. A dry run and preview read
Mongo (inbox_daily, warmup_repair) and write nothing. Both print the weekly lines whatever the weekday.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import warmup_repair as wr

READ_DAYS = 8


def latest_readings(daily_col, today: date) -> list[dict]:
    """The newest state reading (a doc with ``rep``) per inbox in inbox_daily over the last 8 days. Read-only."""
    since = (today - timedelta(days=READ_DAYS)).isoformat()
    latest: dict[str, dict] = {}
    for d in daily_col.find({"date": {"$gte": since}}, {"_id": 0}):
        if "rep" in d and (d["email"] not in latest or d["date"] > latest[d["email"]]["date"]):
            latest[d["email"]] = d
    return list(latest.values())


def preview(docs: list[dict], readings: list[dict], today: date) -> tuple[list[dict], list, list, list[str]]:
    """(docs after the readings, asks, done, detail) without saving anything. No roster: nothing turns gone."""
    docs, new = wr.run(docs, readings, today, read=set(), roster=None)
    asks, done, detail = wr.digest_items(docs, today, weekly=True)
    return new, asks, done, detail


def _print(new, asks, done, detail, quiet: bool) -> None:
    print(f"enrolled by the bot today (report-only): {len(new)}")
    print("Asks:" if asks else "Asks: none")
    for a in asks:
        print(f"  {a.text}\n  Do: {a.step}")
    print("Weekly lines (Mondays, Done):")
    for d in done:
        print(f"  {d.text}")
    if detail and not quiet:
        print("\n--- thread ---\n" + "\n\n".join(detail))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Warm-up repair tracker (report-only)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed", help="seed warmup_repair from the lead's two local JSON files")
    s.add_argument("--changes", required=True, help="t16_warmup_changes.json: {email: {on, before, after, ok}}")
    s.add_argument("--before", required=True, help="t16_warmup_before.json: {inboxes: [...]}")
    s.add_argument("--dry-run", action="store_true", help="print what would be written and the digest lines; write nothing")
    p = sub.add_parser("preview", help="read-only: what the digest would say today")
    for x in (s, p):
        x.add_argument("--quiet", action="store_true", help="no per-inbox lines")
    args = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        pass
    today = date.today()
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    if db is None:
        print("Mongo not reachable: nothing read or written")
        return 1
    store = wr.RepairStore(db["warmup_repair"])
    docs = store.all()
    if args.cmd == "seed":
        with open(args.changes, encoding="utf-8") as f:
            changes = json.load(f)
        with open(args.before, encoding="utf-8") as f:
            before = json.load(f)
        seeds = wr.seed_docs(changes, before, today)
        have = {d["email"]: d for d in docs}
        todo = [d for d in seeds if d["email"] not in have or have[d["email"]].get("enrolled_by") == "auto"]
        by = {}
        for d in seeds:
            k = (wr.CLIENTS.get(d["client"], d["client"]), "settings changed" if d["settings_after"] else "tracked only")
            by[k] = by.get(k, 0) + 1
        print(f"seed: {len(seeds)} inboxes in the files, {len(todo)} to write, "
              f"{len(seeds) - len(todo)} already tracked")
        for (client, what), n in sorted(by.items()):
            print(f"  {client}: {n} {what}")
        if not args.dry_run:
            print(f"written: {store.seed(seeds)}")
            return 0
        print("(dry run: nothing written)\n")
        merged = {d["email"]: d for d in docs}
        merged.update({d["email"]: d for d in todo})
        docs = list(merged.values())
    _print(*preview(docs, latest_readings(db["inbox_daily"], today), today), quiet=args.quiet)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
