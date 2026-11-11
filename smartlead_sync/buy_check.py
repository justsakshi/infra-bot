#!/usr/bin/env python3
"""May we order N inboxes of this vendor and type? Read-only.

    python buy_check.py --vendor zapmail --type google --count 30
    python buy_check.py --vendor zapmail --type outlook --count 12 --json

Rule (smartlead/buy_rule.py): no batch of a vendor + inbox type until six inboxes of that kind have passed their
placement test twice; a trial of six is always allowed. Reads the asset tracker and the v2 verdicts from Mongo.
Exit code 0 = fine to order, 1 = trial only, 2 = could not read (no verdict)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".." / ".env")
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from smartlead import buy_rule as br  # noqa: E402


def read() -> tuple[list[dict], list[dict]]:
    """(tracker INBOX rows, v2 verdict rows). Raises when either cannot be read: no verdict is better than a guess."""
    from renewal_review import _collection                      # the health-history db, where v2_weekly.py writes
    from smartlead.zapmail_asset_sync import ASSETS_COLLECTION, _db
    db = _db()
    if db is None:
        raise RuntimeError("the asset tracker is not reachable")
    assets = list(db[ASSETS_COLLECTION].find({"type": "INBOX"},
                                             {"_id": 0, "type": 1, "name": 1, "provider": 1, "workspace": 1}))
    verdicts = list(_collection("v2_verdicts").find({}, {"_id": 0, "email": 1, "night": 1, "verdict": 1}))
    return assets, verdicts


def main() -> int:
    ap = argparse.ArgumentParser(description="Buying rule check (read-only)")
    ap.add_argument("--vendor", required=True, help="zapmail | scaledmail | inboxkit")
    ap.add_argument("--type", required=True, help="google | outlook | smtp")
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    try:
        assets, verdicts = read()
    except Exception as exc:  # noqa: BLE001 - the type only: the text can carry the Mongo address
        out = {"error": f"could not read the tracker or the v2 verdicts ({type(exc).__name__})"}
        print(json.dumps(out) if args.json else f"NO VERDICT: {out['error']}")
        return 2
    r = br.check(args.vendor, args.type, args.count, assets, verdicts)
    print(json.dumps(r) if args.json else ("OK to order: " if r["ok"] else "TRIAL ONLY: ") + r["why"])
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
