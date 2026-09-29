#!/usr/bin/env python3
"""Domain suggestions for one client (Zapmail AI + our generator). Read-only.

Backs Slack's "Suggest domains for <client>" button; also usable by hand.

    python domain_suggest.py --clients                  # configured clients
    python domain_suggest.py --client Bettrdata         # suggestions
    python domain_suggest.py --client Bettrdata --json  # one JSON line (Slack)

Profiles (site + keywords) live in domain_clients.json.
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
    from smartlead.domain_suggest import load_profiles, suggest

    ap = argparse.ArgumentParser(description="Suggest buyable domains for a client")
    ap.add_argument("--client")
    ap.add_argument("--clients", action="store_true", help="List configured clients")
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--no-generator", action="store_true",
                    help="Zapmail AI only (faster, uses fewer searches)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.clients:
        rows = [{"client": k, "label": p.get("label") or k,
                 "ready": bool(p.get("main_domain") or p.get("brand_stem")) and len(p.get("keywords") or []) >= 3}
                for k, p in load_profiles().items()]
        print(json.dumps({"clients": rows}) if args.json
              else "\n".join(f"  {r['label']:16} {'ready' if r['ready'] else 'needs site + keywords'}"
                             for r in rows))
        return 0
    if not args.client:
        ap.print_help()
        return 2

    try:
        res = await suggest(args.client, count=max(1, min(args.count, 20)),
                            use_generator=not args.no_generator)
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}) if args.json else f"ERROR: {exc}")
        return 0 if args.json else 1
    if args.json:
        print(json.dumps(res, default=str))
        return 0
    print(f"\n  {res['label']} ({res['main_domain']}) — keywords: {', '.join(res['keywords'])}")
    print(f"  Zapmail AI: {res['ai_usable']} usable of {res['ai_found']} · "
          f"our generator: {res['generator_usable']} usable")
    for s in res["suggestions"]:
        src = "Zapmail AI" if s["source"] == "ai" else "generator"
        words = " + ".join(s.get("words") or [])
        print(f"    {s['domain']:24} ${s['price']:.2f}  {words:26} ({src})")
    if res.get("ai_dropped"):
        print(f"  Dropped {len(res['ai_dropped'])} Zapmail AI name(s) that don't read as this business:")
        for d in res["ai_dropped"][:8]:
            print(f"    {d['domain']:24} {d['reason']}")
    for e in res["errors"]:
        print(f"  ⚠ {e}")
    if not res["estate_ok"]:
        print("  ⚠ an owned-domain source failed — dedupe may be incomplete")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
