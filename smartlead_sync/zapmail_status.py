#!/usr/bin/env python3
"""Read-only Zapmail fleet status + renewal cross-check (CLI).

Safest thing in the fleet: pure READ. No purchase, no wallet, no mutation.

Usage:
    python zapmail_status.py --status              # wallet/plan/mailbox snapshot, all accounts
    python zapmail_status.py --renewals            # domains expiring <=2mo, all accounts
    python zapmail_status.py --cross-check         # Zapmail renewal-soon vs /infra asset tracker
    (append --json for a single machine-readable stdout line)
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


async def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only Zapmail fleet status")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--renewals", action="store_true")
    ap.add_argument("--cross-check", action="store_true")
    ap.add_argument("--domain", help="Find a domain across every Zapmail account")
    ap.add_argument("--prewarmed", action="store_true",
                    help="Pre-warmed subscriptions, free slots, and Zapmail's stock")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.prewarmed:
        from smartlead.zapmail_fleet import prewarmed_overview
        res = await prewarmed_overview()
        if args.json:
            print(json.dumps({"prewarmed": res}, default=str))
            return 0
        stock = res.get("stock", {})
        print(f"  Zapmail stock: {stock.get('google')} Google, {stock.get('microsoft')} Outlook")
        for acc, per in res["accounts"].items():
            for p, s in per.items():
                if s.get("error"):
                    print(f"  {acc:14} {p:9} ⚠ {s['error']}")
                    continue
                subs = ", ".join(f"{x['plan']} ${x['price']}/mo × {x['mailboxes']}"
                                 for x in s["subscriptions"]) or "none"
                print(f"  {acc:14} {p:9} slots {s['assigned']}/{s['slots']} used "
                      f"({s['free']} free) · {subs}")
        return 0

    if args.domain:
        from smartlead.zapmail_fleet import locate_domain
        hits = await locate_domain(args.domain)
        if args.json:
            print(json.dumps({"domain": args.domain.strip().lower(), "hits": hits}))
            return 0
        if not hits:
            print(f"  {args.domain}: not in any Zapmail account")
        for h in hits:
            if h.get("error"):
                print(f"  {h['account']:14} ⚠ {h['error']}")
                continue
            print(f"  {h['account']:14} {h['status']:10} expires {h['expire_on']} "
                  f"auto-renew={h['auto_renew']} mailboxes={len(h['mailboxes'])}")
            for m in h["mailboxes"]:
                print(f"      {m.get('email') or '?':40} {m.get('status') or '?'}")
        return 0

    if not any((args.status, args.renewals, args.cross_check)):
        ap.print_help()
        return 0

    result: dict = {}

    if args.status:
        from smartlead.zapmail_fleet import fleet_status
        status = await fleet_status()
        result["status"] = status
        if args.json:
            print(json.dumps(result))
            return 0
        for name, s in status.items():
            if not s.get("ok"):
                print(f"  {name:14} ⚠ unreachable: {s.get('error', '')}")
                continue
            dom, act = s.get("domains") or {}, s.get("active_mailboxes") or {}
            recharge = "on" if s.get("auto_recharge") else "OFF"
            print(f"  {name:14} wallet=${s.get('wallet_balance')} (auto-recharge {recharge})"
                  f"  domains G{dom.get('GOOGLE')}/M{dom.get('MICROSOFT')}"
                  f"  active mailboxes G{act.get('GOOGLE')}/M{act.get('MICROSOFT')}"
                  f"  placement credits {s.get('placement_credits')}")

    if args.renewals:
        from smartlead.zapmail_fleet import renewals_asof
        errors: list[str] = []
        rows = await renewals_asof(errors)
        result["renewals"] = rows
        result["renewal_errors"] = errors
        if args.json:
            print(json.dumps(result))
            return 0
        print(f"\n  Domains expiring <=2mo ({len(rows)}):")
        for r in sorted(rows, key=lambda x: x["expire_on"]):
            print(f"    {r['expire_on']}  {r['domain']:30} {r['account']:14} {r['provider']}")
        for e in errors:
            print(f"  ⚠ could not check {e}")

    if args.cross_check:
        from smartlead.zapmail_renewals import renewals_cross_check
        cc = await renewals_cross_check()
        result["cross_check"] = cc
        if args.json:
            print(json.dumps(result, default=str))
            return 0
        print(f"\n  Cross-check ({cc['zapmail_domains']} Zapmail domains vs "
              f"{cc['tracker_domains']} in the /infra tracker):")
        for e in cc.get("zapmail_errors") or []:
            print(f"    ⚠ could not read {e} — 'only in tracker' is unreliable")
        # Past clients' domains, lapsed ones and other-provider tracker rows
        # are left out: nobody acts on them (29 Sep review).
        missing = [x for x in cc["only_in_zapmail"] if x.get("client")]
        print(f"    Current-client domains not in the tracker ({len(missing)}):")
        for x in missing:
            print(f"      {x['expire_on'] or '?':10}  {x['domain']:30} {x['client']}")
        print(f"    Tracked without an expiry date ({len(cc.get('no_tracker_expiry', []))}):")
        for x in cc.get("no_tracker_expiry", []):
            print(f"      {x['domain']:30} zapmail={x['expire_on'] or '?'}")
        print("    (both are filled in by zapmail_asset_sync.py --apply / Apply to tracker)")
        print(f"    Date mismatch ({len(cc['date_mismatch'])}):")
        for x in cc["date_mismatch"]:
            print(f"      {x['domain']:30} zapmail={x['zapmail_expire']} tracker={x['tracker_expire']}")

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