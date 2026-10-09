#!/usr/bin/env python3
"""GoDaddy domains from the command line.

READ (free):
    python godaddy_cli.py check a.com b.com          # availability, first-year + renewal price, name rules
    python godaddy_cli.py domains                    # everything we own: expiry, auto-renew, nameservers
    python godaddy_cli.py domain a.com
    python godaddy_cli.py quote a.com                # locked price + the agreements to accept (never charges)
    python godaddy_cli.py plans                      # staged / bought (ledger)
    python godaddy_cli.py sync [--apply]             # GoDaddy -> /infra tracker

STAGE (free, ledger only):
    python godaddy_cli.py stage --client "Precise Leads" --domains a.com,b.com [--ns ns1.x.com,ns2.x.com]

SPEND (charges the account; --approve + GODADDY_ALLOW_SPEND=true, set for that one run):
    GODADDY_ALLOW_SPEND=true python godaddy_cli.py place <plan_id> --approve --user <name>
    python godaddy_cli.py drop <plan_id>

WRITE (free; changes a live domain):
    python godaddy_cli.py ns a.com ns1.x.com,ns2.x.com --approve
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead.godaddy import GoDaddyClient, GoDaddyError, idempotency_key, spend_allowed
from smartlead import godaddy_orders as orders

SAFE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9-]{1,63})+$")


def _dom(s: str) -> str:
    s = s.strip().lower()
    if not SAFE.match(s):
        raise GoDaddyError(f"not a domain: {s!r}")
    return s


def _list(s: str | None) -> list[str]:
    return [_dom(x) for x in str(s or "").split(",") if x.strip()]


def run(args) -> dict:
    c = args.cmd
    if c == "plans":
        return {"plans": sorted(orders.PlanStore().all(), key=lambda p: p.get("staged_at", ""))}
    if c == "drop":
        return orders.drop_plan(args.plan_id, user=args.user or "")
    if c == "sync":
        from smartlead.godaddy_asset_sync import sync
        return sync(apply_changes=args.apply)
    with GoDaddyClient() as gd:
        if c == "check":
            rows = gd.check([_dom(d) for d in args.names])
            for r in rows:
                r["name_problems"] = orders.name_problems(r["domain"])
            return {"results": rows}
        if c == "domains":
            return {"domains": [{k: d.get(k) for k in ("domain", "status", "expiresAt", "autoRenew",
                                                       "nameServers", "privacy")} for d in gd.domains()]}
        if c == "domain":
            return gd.domain(_dom(args.name))
        if c == "quote":
            return gd.quote(_dom(args.name))
        if c == "stage":
            return orders.stage_plan(gd, client=args.client, domains=_list(args.domains),
                                     nameservers=_list(args.ns), user=args.user or "")
        if c == "place":
            return orders.place_plan(gd, args.plan_id, approve=args.approve, user=args.user or "")
        if c == "ns":
            dom, ns = _dom(args.name), _list(args.servers)
            if not args.approve:
                return {"dry_run": True, "domain": dom, "nameservers": ns}
            return gd.set_nameservers(dom, ns, key=idempotency_key("manual-ns", dom, ",".join(ns)),
                                      approve=True)
    raise GoDaddyError(f"unknown command {c}")


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("check"); p.add_argument("names", nargs="+")
    sub.add_parser("domains")
    p = sub.add_parser("domain"); p.add_argument("name")
    p = sub.add_parser("quote"); p.add_argument("name")
    sub.add_parser("plans")
    p = sub.add_parser("sync"); p.add_argument("--apply", action="store_true")
    p = sub.add_parser("stage"); p.add_argument("--client", required=True)
    p.add_argument("--domains", required=True); p.add_argument("--ns"); p.add_argument("--user")
    p = sub.add_parser("place"); p.add_argument("plan_id"); p.add_argument("--approve", action="store_true")
    p.add_argument("--user")
    p = sub.add_parser("drop"); p.add_argument("plan_id"); p.add_argument("--user")
    p = sub.add_parser("ns"); p.add_argument("name"); p.add_argument("servers")
    p.add_argument("--approve", action="store_true")
    return ap


def main() -> int:
    args = parser().parse_args()
    try:
        out = run(args)
    except GoDaddyError as exc:
        out = {"ok": False, "error": str(exc)}
    out = {**out, "spend_allowed": spend_allowed()} if isinstance(out, dict) else out
    print(json.dumps(out, indent=1, default=str))
    return 0 if not (isinstance(out, dict) and out.get("ok") is False) else 1


if __name__ == "__main__":
    sys.exit(main())
