#!/usr/bin/env python3
"""Sync Zapmail domains + mailboxes into the /infra asset tracker.

Dry run by default: prints what would change. See
smartlead/zapmail_asset_sync.py for the rules (expiry follows Zapmail, status
only downgrades, new rows only for current clients, nothing deleted).

    python zapmail_asset_sync.py                    # preview
    python zapmail_asset_sync.py --apply            # write the tracker
    python zapmail_asset_sync.py --domain x.com     # one domain (webhooks)
    python zapmail_asset_sync.py --json

The daily cron runs --apply only when ZAPMAIL_ASSET_SYNC_ENABLED=true.
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
    from smartlead.zapmail_asset_sync import sync

    ap = argparse.ArgumentParser(description="Zapmail → /infra tracker sync")
    ap.add_argument("--apply", action="store_true", help="Write changes (default: preview)")
    ap.add_argument("--domain", help="Only this domain and its mailboxes")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    res = await sync(apply_changes=args.apply, domain=args.domain)
    if args.json:
        print(json.dumps({k: res.get(k) for k in
                          ("ok", "counts", "applied", "errors", "note")}
                         | {"ops": [{k: o[k] for k in ("action", "type", "name", "reason")}
                                    for o in res.get("ops", [])],
                            "skipped": res.get("skipped", [])}, default=str))
        return 0
    c = res.get("counts", {})
    print(f"\n  Zapmail → /infra tracker ({'APPLIED' if args.apply else 'preview'})")
    print(f"    domains: {c.get('domain_insert', 0)} new, {c.get('domain_update', 0)} updated, "
          f"{c.get('domain_skipped', 0)} skipped (past client / unassigned / lapsed)")
    print(f"    inboxes: {c.get('inbox_insert', 0)} new, {c.get('inbox_update', 0)} updated, "
          f"{c.get('inbox_skipped', 0)} skipped")
    for o in res.get("ops", [])[:40]:
        print(f"      {o['action']:6} {o['type']:6} {o['name']:40} {o['reason']}")
    if len(res.get("ops", [])) > 40:
        print(f"      … and {len(res['ops']) - 40} more")
    for e in res.get("errors", []):
        print(f"  ⚠ {e}")
    if res.get("note"):
        print(f"  ⚠ {res['note']}")
    if args.apply:
        print(f"  wrote {res.get('applied', 0)} row(s)")
    else:
        print("  Preview only — re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
