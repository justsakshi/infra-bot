#!/usr/bin/env python3
"""Retry failed inboxes / retire inboxes at renewal. DRY RUN unless --approve.

    python zapmail_inboxes.py --retry-failed gomelior.com --client Melior
    python zapmail_inboxes.py --retire ann@gomelior.com,bo@gomelior.com --client Melior
    python zapmail_inboxes.py --retire ann@gomelior.com --client Melior --undo
    ... add --approve to do it, --json for Slack.

Retiring removes the inboxes at their NEXT renewal (they keep working until
then; the slot stops being billed after); it can be undone before that.
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
    except Exception:
        pass


async def main() -> int:
    from smartlead.inbox_maintenance import retire, retry_failed

    ap = argparse.ArgumentParser(description="Retry failed / retire inboxes")
    ap.add_argument("--client", required=True)
    ap.add_argument("--retry-failed", metavar="DOMAIN")
    ap.add_argument("--retire", metavar="EMAILS", help="comma-separated inbox addresses")
    ap.add_argument("--undo", action="store_true", help="cancel a scheduled retirement")
    ap.add_argument("--approve", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    try:
        if args.retry_failed:
            r = await retry_failed(args.retry_failed, client=args.client, approve=args.approve)
        elif args.retire:
            r = await retire(args.retire.split(","), client=args.client, approve=args.approve, undo=args.undo)
        else:
            ap.error("give --retry-failed DOMAIN or --retire EMAILS")
    except ValueError as exc:
        r = {"ok": False, "error": str(exc)}
    if args.json:
        print(json.dumps(r, default=str))
        return 0
    if r.get("error"):
        print(f"ERROR: {r['error']}")
        return 1
    if r.get("dry_run"):
        items = r.get("would_retry") or r.get("inboxes") or []
        print(f"  DRY RUN — would {r.get('action', 'retry')}: {', '.join(items)}\n  Re-run with --approve.")
        return 0
    done = r.get("inboxes") or r.get("retried") or []
    print(f"  ✓ {r.get('action', 'retried')}: {', '.join(done)} — {r.get('detail', '')}" if done
          else f"  ✓ {r.get('detail', 'nothing to do')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
