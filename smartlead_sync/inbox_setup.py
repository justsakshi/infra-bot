#!/usr/bin/env python3
"""Rename an inbox, set signatures, file inboxes under a Smartlead client. DRY RUN by default.

    python inbox_setup.py --client Melior --inbox ann@gomelior.com --first Ann --last Reyes
    python inbox_setup.py --client Bettrdata --domain askbettrdata.com --signature
    python inbox_setup.py --client Melior --domain gomelior.com --new       # new-inbox warmup
    ... add --approve to write, --json for Slack.

Renames keep the address (only the name changes, in Smartlead and Zapmail).
Signatures come from inbox_profiles.json. Nothing here spends money.
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


def _short(v, n=60) -> str:
    s = "" if v is None else str(v).replace("\n", " ")
    return s if len(s) <= n else s[: n - 1] + "…"


async def main() -> int:
    from smartlead.inbox_setup import apply_changes, plan_for

    ap = argparse.ArgumentParser(description="Inbox name / signature / client in Smartlead")
    ap.add_argument("--client", required=True)
    ap.add_argument("--inbox", action="append", default=[], help="Inbox address (repeatable)")
    ap.add_argument("--domain", help="Every inbox on this domain")
    ap.add_argument("--first", help="New first name (one inbox only)")
    ap.add_argument("--last", help="New last name (one inbox only)")
    ap.add_argument("--signature", action="store_true", help="Apply the client's signature template")
    ap.add_argument("--new", action="store_true", help="Also apply the new-inbox warmup standard")
    ap.add_argument("--approve", action="store_true", help="Write the changes (default: dry run)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not args.inbox and not args.domain:
        ap.error("give --inbox or --domain")
    try:
        account, changes = await plan_for(
            args.client, emails=args.inbox, domain=args.domain, first=args.first,
            last=args.last, set_signature=args.signature, new_inbox=args.new)
    except (ValueError, PermissionError) as exc:
        print(json.dumps({"error": str(exc)}) if args.json else f"ERROR: {exc}")
        return 0 if args.json else 1

    results = None
    if args.approve:
        results = await apply_changes(args.client, changes, approve=True)

    if args.json:
        print(json.dumps({
            "client": args.client, "smartlead_account": account, "applied": bool(args.approve),
            "changes": [{"email": c.email, "fields": c.fields, "before": c.before,
                         "warmup": c.warmup, "zapmail_rename": c.zapmail_rename,
                         "error": c.error} for c in changes],
            "results": results}, default=str))
        return 0

    print(f"\n  {args.client} — Smartlead account {account} — "
          f"{'APPLYING' if args.approve else 'DRY RUN'}")
    if not changes:
        print("  no inboxes matched")
    for c in changes:
        if c.error:
            print(f"  ✗ {c.email}: {c.error}")
            continue
        if c.empty:
            print(f"  = {c.email}: already set, nothing to change")
            continue
        print(f"  • {c.email}")
        for k, v in c.fields.items():
            print(f"      {k:14} {_short(c.before.get(k)) or '(empty)'}  →  {_short(v)}")
        if c.zapmail_rename:
            print(f"      zapmail name   → {c.zapmail_rename['firstName']} {c.zapmail_rename['lastName']}".rstrip())
        if c.warmup:
            w = c.warmup
            print(f"      warmup         on, {w['total_per_day']}/day ramping +{w['daily_rampup']}, "
                  f"reply {w['reply_rate']}%")
    for r in results or []:
        mark = "✓" if r["ok"] else "✗"
        extra = f" — {r['error']}" if r.get("error") else ""
        extra += f" (Zapmail: {r['zapmail_error']})" if r.get("zapmail_error") else ""
        print(f"  {mark} {r['email']}: {', '.join(r['done']) or 'nothing'}{extra}")
    if not args.approve and any(not c.empty and not c.error for c in changes):
        print("\n  DRY RUN — re-run with --approve to write.")
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
