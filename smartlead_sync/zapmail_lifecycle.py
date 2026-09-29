#!/usr/bin/env python3
"""Domain lifecycle CLI: connect + mailbox creation (dry-run by default).

Read-only:
    python zapmail_lifecycle.py --ns
    python zapmail_lifecycle.py --connect-status a.com,b.com

Write (mutations — DRY-RUN unless --approve):
    python zapmail_lifecycle.py --connect a.com,b.com
    python zapmail_lifecycle.py --connect a.com,b.com --approve
    python zapmail_lifecycle.py --mailboxes a.com,b.com --per-domain 2
    python zapmail_lifecycle.py --mailboxes a.com,b.com --per-domain 2 --approve

One step after a purchase (connect -> mailboxes [-> export]):
    python zapmail_lifecycle.py --provision a.com,b.com --client "Precise Leads" \
        --names "Jane Doe,John Roe" [--export] [--approve]

Every action runs on the client's own Zapmail account (--client is required
for anything except --ns; unmapped clients are refused). Connect and mailbox
creation cost no new money but mutate the account, so they gate only on
--approve (the money kill-switch is for purchases/renewals).
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

from smartlead.domain_lifecycle import (
    assign_mailboxes_and_wait, connect_and_wait, connect_status,
    required_nameservers,
)


def _poll(args) -> dict:
    """Only pass polling overrides the user actually set; else Zapmail's defaults."""
    kw = {}
    if args.timeout is not None:
        kw["timeout_s"] = args.timeout
    if args.interval is not None:
        kw["interval_s"] = args.interval
    return kw


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Zapmail domain lifecycle (dry-run default)")
    ap.add_argument("--ns", action="store_true", help="Print required nameservers")
    ap.add_argument("--connect", help="Comma-separated domains to connect")
    ap.add_argument("--connect-status", help="Comma-separated domains to check (read-only)")
    ap.add_argument("--mailboxes", help="Comma-separated domains to create mailboxes on")
    ap.add_argument("--provision",
                    help="Comma-separated bought domains: connect -> mailboxes [-> export]")
    ap.add_argument("--export", action="store_true",
                    help="With --provision: also export to the client's Smartlead")
    ap.add_argument("--provider", default="GOOGLE", choices=["GOOGLE", "MICROSOFT"],
                    help="For --connect/--provision of a NEW domain: which mailbox provider")
    ap.add_argument("--third-party-account-id", default=None,
                    help="With --provision --export: Zapmail's id for the Smartlead account")
    ap.add_argument("--per-domain", type=int, default=2)
    ap.add_argument("--names", default="",
                    help='Real sender names for --mailboxes, e.g. "Jane Doe,John Roe"')
    ap.add_argument("--approve", action="store_true",
                    help="Actually mutate; without it, dry-run")
    ap.add_argument("--client", default="")
    ap.add_argument("--timeout", type=float, default=None,
                    help="Seconds to wait (default: 30 min connect / 60 min mailboxes)")
    ap.add_argument("--interval", type=float, default=None,
                    help="Seconds between polls (default: Zapmail's 60s connect / 180s mailboxes)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.provision:
        from smartlead.domain_provision import provision_domain
        reports = []
        for d in _split(args.provision):
            reports.append(await provision_domain(
                d, client=args.client or None, per_domain=args.per_domain,
                provider=args.provider,
                sender_names=_split(args.names) or None, export=args.export,
                third_party_account_id=args.third_party_account_id,
                approve=args.approve, **_poll(args)))
        if args.json:
            print(json.dumps(reports, default=str))
            return 0
        for r in reports:
            head = "DRY RUN" if r["dry_run"] else ("OK" if r["ok"] else "STOPPED")
            print(f"\n  {r['domain']} ({r['account']}) — {head}")
            for step, res in r["steps"].items():
                if res.get("skipped"):
                    detail = f"skipped ({res['skipped']})"
                elif res.get("error"):
                    detail = f"FAILED — {res['error']}"
                elif res.get("would_create"):
                    detail = "would create " + ", ".join(res["would_create"])
                elif res.get("status") == "DRY_RUN":
                    detail = "would connect (NS must point at Zapmail)"
                elif res.get("dry_run"):
                    detail = f"would export {res.get('selection')}"
                else:
                    detail = json.dumps({k: v for k, v in res.items()
                                         if k in ("status", "ok", "created", "pending",
                                                  "export_id")}, default=str)
                print(f"    {step:10} {detail}")
        if not args.approve:
            print("\n  DRY RUN — re-run with --approve to do it.")
        return 0

    if args.ns:
        ns = required_nameservers()
        if args.json:
            print(json.dumps({"nameservers": list(ns)}))
        else:
            print("Point the domain's NS at Zapmail before connecting:")
            for n in ns:
                print(f"  {n}")
        return 0

    if args.connect_status:
        out = {}
        for d in _split(args.connect_status):
            out[d] = await connect_status(d, client=args.client or None)
        if args.json:
            print(json.dumps(out))
        else:
            for d, s in out.items():
                print(f"  {d:30} {s['status']:28} connected={s['connected']}")
        return 0

    if args.connect:
        domains = _split(args.connect)
        res = await connect_and_wait(
            domains, approve=args.approve, client=args.client or None,
            provider=args.provider, **_poll(args))
        if args.json:
            print(json.dumps(res))
            return 0
        for d, s in res.items():
            print(f"  {d:30} {s['status']:28} ok={s.get('ok')}")
        if not args.approve:
            print("\n  DRY RUN — re-run with --approve to connect.")
        return 0

    if args.mailboxes:
        results = []
        for d in _split(args.mailboxes):
            results.append(await assign_mailboxes_and_wait(
                d, count=args.per_domain, approve=args.approve,
                client=args.client or None,
                sender_names=_split(args.names) or None,
                **_poll(args)))
        if args.json:
            print(json.dumps(results))
            return 0
        for r in results:
            if r.get("dry_run"):
                print(f"  {r['domain']:30} DRY RUN → {', '.join(r['would_create'])}")
            elif r.get("ok"):
                print(f"  {r['domain']:30} ok — created {len(r.get('created', []))} "
                      f"(pending {len(r.get('pending', []))})")
            else:
                print(f"  {r['domain']:30} FAILED — {r.get('error', 'pending at timeout')}")
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