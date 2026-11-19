#!/usr/bin/env python3
"""Warm-up repair tracker (since t325, 19 Nov 2026: every bad inbox in recovery): seed it from the lead's local files,
or preview what the digest would say.
Logic: smartlead/warmup_repair.py. Fleet watch updates it every day (10:30 IST) and hands the digest its lines.
Never changes a warm-up setting and never calls Smartlead.

    python warmup_repair_cli.py seed --changes <t16_warmup_changes.json> --before <t16_warmup_before.json> --dry-run
    python warmup_repair_cli.py seed --changes <...> --before <...>      # writes Mongo warmup_repair (the lead runs it)
    python warmup_repair_cli.py preview                                  # read-only: today's digest lines
    python warmup_repair_cli.py changes --changes <...> --before <...> --who <...> --why <...> --dry-run
    python warmup_repair_cli.py changes --changes <...> --before <...> --who <...> --why <...>
                                         # appends one event per applied change to Mongo warmup_changes
    python warmup_repair_cli.py outcomes                                 # read-only: outcomes due today + monthly lines

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

from smartlead import warmup_changes as wc
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


def preview(docs: list[dict], readings: list[dict], today: date, v2: dict | None = None,
            listed: set[str] | None = None, dead: set[str] | None = None) -> tuple[list[dict], list, list, list[str]]:
    """(docs after the readings, asks, done, detail) without saving anything. No roster: nothing turns gone.
    ``v2``/``listed``/``dead``: as fleet watch reads them (see fleet_watch._repair_inputs)."""
    docs, new = wr.run(docs, readings, today, read=set(), roster=None, v2=v2, listed=listed, dead=dead)
    asks, done, detail = wr.digest_items(docs, today, weekly=True)
    return new, asks, done, detail


def _gate_inputs(db) -> tuple[dict | None, set[str] | None, set[str]]:
    """(newest judged v2 verdict per inbox, listed sending domains, dead inboxes) from Mongo, as fleet watch reads
    them; None / empty when a collection is missing or unreadable. Read-only."""
    import fleet_watch as fw
    from smartlead.config import BLACKLIST_COLLECTION

    def col(name):
        try:
            return db[name]
        except (KeyError, TypeError):
            return None

    class _Store:
        verdicts, status = col("v2_verdicts"), col("inbox_status")
    blocklist = col(BLACKLIST_COLLECTION)
    return fw._repair_inputs(_Store, fw._blocklisted(blocklist) if blocklist is not None else None)


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


def vendors(db, emails: list[str], today: date) -> dict[str, str]:
    """{email: vendor}: the newest inbox_daily reading's vendor (last 8 days), else the asset tracker's provider for
    the domain (Mongo assets). Read-only."""
    daily = {r["email"]: r["vendor"] for r in latest_readings(db["inbox_daily"], today) if r.get("vendor")}
    domains = sorted({e.split("@")[-1] for e in emails})
    try:
        col = db["assets"]
    except KeyError:                                       # a test db without the asset tracker
        col = None
    assets = list(col.find({"name": {"$in": domains}}, {"name": 1, "provider": 1, "status": 1})) if col is not None else []
    return wc.vendors_by_email(emails, daily, assets)


def changes_cmd(db, args, today: date) -> int:
    with open(args.changes, encoding="utf-8") as f:
        changes = json.load(f)
    with open(args.before, encoding="utf-8") as f:
        before = json.load(f)
    events, skipped = wc.events_from_changes(changes, before, who=args.who, why=args.why,
                                             vendors=vendors(db, [str(k).lower() for k in changes], today))
    store = wc.ChangeStore(db["warmup_changes"])
    have = {e["key"] for e in store.all()}
    new = [e for e in events if e["key"] not in have]
    print(f"{len(events)} events, {len(new)} new, {len(skipped)} skipped (change did not apply)")
    by: dict[str, int] = {}
    for e in events:
        k = f"{e['client']} · {e['kind']} · {e['vendor'] or 'vendor unknown'}: {wc.label(e['before'])} -> {wc.label(e['after'])}"
        by[k] = by.get(k, 0) + 1
    for k, n in sorted(by.items()):
        client_kind_vendor, settings = k.split(": ", 1)
        print(f"  {client_kind_vendor}: {n} ({settings})")
    if args.dry_run:
        print("(dry run: nothing written)")
        return 0
    print(f"written: {store.append(events)}")
    print(f"read back: {len(store.all())} events in warmup_changes")
    return 0


def outcomes_cmd(db, today: date) -> int:
    """Read-only: what fleet watch would fill today, and the monthly lines whatever the day."""
    events = wc.ChangeStore(db["warmup_changes"]).all()

    class _ReadOnly:                                       # fill in memory only
        def __init__(self, docs): self.docs = docs
        def find(self, q, proj=None): return [dict(d) for d in self.docs]
        def update_one(self, *a, **k): pass

    after = wc.daily(_ReadOnly(events), db["inbox_daily"], db["v2_verdicts"], db["inbox_evidence"], today)
    print(f"events: {len(after)}")
    for w in wc.WEEKS:
        k = f"week{w}"
        filled = sum(1 for e in after if "rep" in (e.get("outcomes") or {}).get(k, {}))
        missing = sum(1 for e in after if "missing" in (e.get("outcomes") or {}).get(k, {}))
        print(f"  {k} filled: {filled}, missing: {missing}")
    print(f"  recovered: {sum(1 for e in after if e.get('recovered_on'))}")
    done, detail = wc.month_items(after, today, monthly=True)
    print("Monthly line (first Monday):", done[0].text if done else "none yet (no 3-week outcome)")
    if detail:
        print(detail[0])
    return 0


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
    c = sub.add_parser("changes", help="append warm-up settings changes to Mongo warmup_changes")
    c.add_argument("--changes", required=True, help="{email: {on, type, rep_before, before, after, ok}}")
    c.add_argument("--before", required=True, help="{inboxes: [{email, account, client, type}]}")
    c.add_argument("--who", required=True, help="who made the change (and on whose yes)")
    c.add_argument("--why", required=True, help="why the settings were changed")
    c.add_argument("--dry-run", action="store_true", help="print what would be written; write nothing")
    sub.add_parser("outcomes", help="read-only: the outcomes fleet watch would fill today, and the monthly lines")
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
    if args.cmd == "changes":
        return changes_cmd(db, args, today)
    if args.cmd == "outcomes":
        return outcomes_cmd(db, today)
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
    _print(*preview(docs, latest_readings(db["inbox_daily"], today), today, *_gate_inputs(db)), quiet=args.quiet)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
