#!/usr/bin/env python3
"""Batch/delayed domain purchase CLI — the only way domains get bought.

Stage a staggered buy plan (read-only), then buy ONE approved batch at a time.

Usage:
    python zapmail_buy.py --plan a.com,b.com,c.com,d.com --client Bettrdata
    python zapmail_buy.py --list
    python zapmail_buy.py --execute <batch_id> --approve   # + ZAPMAIL_ALLOW_SPEND=true
    python zapmail_buy.py --reconcile <batch_id>           # after a timeout/unknown

Buying refuses unless: --approve, ZAPMAIL_ALLOW_SPEND=true, a reachable
ledger, the batch's scheduled date has arrived, its client resolves to a
configured Zapmail account, and a live re-check confirms every name is still
available at (or under) the staged price and under the ceiling.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.domain_availability import DEFAULT_PRICE_CEILING_USD
from smartlead.domain_batch import (
    DEFAULT_DAY_GAP, DEFAULT_PER_BATCH, BatchStore, execute_one, reconcile_one,
    stage_batches,
)
from smartlead.zapmail import ZapmailError, ZapmailSpendBlocked


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Batch/delayed Zapmail domain purchase")
    ap.add_argument("--plan", help="Comma-separated domains to stage a buy plan for")
    ap.add_argument("--list", action="store_true", help="Show the purchase ledger")
    ap.add_argument("--execute", help="Batch id to buy (needs --approve + env)")
    ap.add_argument("--reconcile", help="Batch id to settle against Zapmail (read-only)")
    ap.add_argument("--approve", action="store_true",
                    help="Acknowledge this specific batch purchase")
    ap.add_argument("--client", default="",
                    help="Client to plan for; on --execute only a cross-check "
                         "against the client recorded on the batch")
    ap.add_argument("--registrars", default="Zapmail")
    ap.add_argument("--per-batch", type=int, default=DEFAULT_PER_BATCH)
    ap.add_argument("--day-gap", type=int, default=DEFAULT_DAY_GAP)
    ap.add_argument("--price-ceiling", type=float, default=DEFAULT_PRICE_CEILING_USD)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.reconcile:
        try:
            res = await reconcile_one(args.reconcile)
        except (RuntimeError, ZapmailError) as exc:
            print(json.dumps({"error": str(exc)}) if args.json else f"ERROR: {exc}",
                  file=sys.stdout if args.json else sys.stderr)
            return 1
        print(json.dumps(res, indent=None if args.json else 2, default=str))
        return 0

    if args.list:
        store = BatchStore()
        rows = store.load_all()
        if args.json:
            print(json.dumps({"ledger_ok": store.available, "batches": rows}, default=str))
        else:
            if not store.available:
                print("  ⚠ ledger unavailable (no Mongo) — no batches to show")
            elif not rows:
                print("  No purchase batches yet. Stage some with --plan or /domains buy.")
            for r in sorted(rows, key=lambda x: x.get("earliest_date", "")):
                print(f"  {r.get('earliest_date','?'):12} {r.get('status','?'):10} "
                      f"{r.get('batch_id','')}  {', '.join(r.get('domains', []))}")
        return 0

    if args.execute:
        try:
            result = await execute_one(args.execute, approve=args.approve,
                                       client=args.client,
                                       price_ceiling=args.price_ceiling)
        except ZapmailSpendBlocked as exc:
            if args.json:
                print(json.dumps({"error": str(exc)}))
            else:
                print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        except ZapmailError as exc:
            msg = (f"{exc} — ledger updated; if the batch is now 'unknown', run "
                   f"--reconcile {args.execute} before anything else.")
            print(json.dumps({"error": msg}) if args.json else f"ERROR: {msg}",
                  file=sys.stdout if args.json else sys.stderr)
            return 1
        if args.json:
            print(json.dumps(result, default=str))
        else:
            print(json.dumps(result, indent=2, default=str))
        return 0

    if args.plan:
        domains = _split(args.plan)
        if not domains:
            print("ERROR: --plan needs at least one domain", file=sys.stderr)
            return 2
        plan = await stage_batches(
            domains, client=args.client,
            registrars=tuple(_split(args.registrars)) or ("Zapmail",),
            per_batch=args.per_batch, day_gap=args.day_gap,
            price_ceiling=args.price_ceiling)
        if args.json:
            print(json.dumps(plan, default=str))
            return 0
        print(f"\n  Buy plan for {args.client or '(primary account)'} "
              f"(bills: {plan['spend_account'] or 'NO ACCOUNT — buying refused'}) "
              f"— staged, nothing bought:")
        for b in plan["batches"]:
            print(f"    {b['earliest_date']:12} {b['registrar']:10} "
                  f"${b['estimated_usd']:>7.2f}  {', '.join(b['domains'])}")
            print(f"        batch_id: {b['batch_id']}")
        for label, key in (("taken", "unavailable"), ("unverified", "unknown"),
                           (f"over ${plan['price_ceiling']:.0f} ceiling", "over_ceiling"),
                           ("already in ledger", "already_planned")):
            if plan[key]:
                print(f"  ⚠ {label}: {', '.join(plan[key])}")
        print(f"\n  Total est. ${plan['total_usd']}  ·  ledger_ok={plan['ledger_ok']}")
        if not plan["ledger_ok"]:
            print("  ⚠ plan NOT recorded (no Mongo) — these batches cannot be executed")
        print("  Buy a batch on/after its date: zapmail_buy.py --execute <batch_id> "
              "--approve (and ZAPMAIL_ALLOW_SPEND=true)")
        return 0

    ap.print_help()
    return 0


def _run() -> int:
    from smartlead.zapmail import ZapmailError
    try:
        return asyncio.run(main())
    except ZapmailError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_run())