#!/usr/bin/env python3
"""ScaledMail from the command line (and for Slack, with --json).

READ (free, no flag needed):
    python scaledmail_cli.py status                  # orders, monthly cost, domains + mailboxes
    python scaledmail_cli.py renewals                # registrations <=60 days, billings <=14 days
    python scaledmail_cli.py domain x.com            # one domain, mailboxes, order
    python scaledmail_cli.py orders                  # every order, its client(s) and billing day
    python scaledmail_cli.py reports                 # ScaledMail's weekly placement tests
    python scaledmail_cli.py prewarmed               # pre-warmed stock + prices
    python scaledmail_cli.py packages                # fixed bundles
    python scaledmail_cli.py quote 30000 --providers google,outlook --split 70,30 --tier low
    python scaledmail_cli.py search a.com b.com      # availability + price + blacklist
    python scaledmail_cli.py suggest bettrdata       # name ideas (available only)
    python scaledmail_cli.py sync [--apply]          # ScaledMail -> /infra tracker
    python scaledmail_cli.py plans                   # staged / placed orders (ledger)
    python scaledmail_cli.py digest                  # today's ScaledMail action list

WRITE (free; dry run unless --approve):
    python scaledmail_cli.py tag <order_id> Melior --approve
    python scaledmail_cli.py senders x.com "Jane Doe, John Roe" --approve   # name only, same addresses
    python scaledmail_cli.py redirect x.com https://client.com --approve
    python scaledmail_cli.py swap old.com new.com --approve                 # new.com from our unused inventory

CANCEL (stops every mailbox in the order; --approve + SCALEDMAIL_ALLOW_CANCEL=true):
    python scaledmail_cli.py cancel <order_id> --approve

SPEND (charges the card; --approve + SCALEDMAIL_ALLOW_SPEND=true):
    python scaledmail_cli.py stage --client Melior --provider google --domains a.com,b.com --senders "Jane Doe"
    python scaledmail_cli.py place <plan_id> --approve
    python scaledmail_cli.py reconcile <plan_id>         # after a timeout
    python scaledmail_cli.py mark-failed <plan_id>       # only after checking the ScaledMail UI
"""
from __future__ import annotations

import argparse
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
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead.scaledmail import ScaledMailClient, ScaledMailError, cancel_allowed, spend_allowed
from smartlead import scaledmail_fleet as fleet
from smartlead.zapmail_clients import CURRENT_CLIENTS, _squash

SAFE_DOMAIN = __import__("re").compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9-]{1,63})+$")


def _tracker() -> dict:
    try:
        from smartlead.zapmail_asset_sync import read_tracker
        return read_tracker()
    except Exception:  # noqa: BLE001
        return {}


def _domain(arg: str) -> str:
    d = arg.strip().lower()
    if not SAFE_DOMAIN.match(d):
        raise ScaledMailError(f"not a domain: {arg!r}")
    return d


def _client(arg: str) -> str:
    for c in CURRENT_CLIENTS:
        if _squash(c) == _squash(arg):
            return c
    raise ScaledMailError(f"{arg!r} is not a current client ({', '.join(CURRENT_CLIENTS)})")


def digest(sm) -> dict:
    snap = fleet.snapshot(sm, tracker=_tracker())
    ren = fleet.renewals(snap, domain_days=14, billing_days=3)
    stuck = [d for d in snap["domains"] if d["status"] != "Active"]
    unassigned = [d["domain"] for d in snap["domains"] if not d["client"]]
    rep = fleet.report_summary(sm.reporting())
    lines = []
    if ren["billing"]:
        lines.append("Orders billing in 3 days: " + "; ".join(
            f"{o['description'].replace('*', '×')} ${o['amount']:.2f} on {o['billing_day']} ({', '.join(o['clients'])})"
            for o in ren["billing"]))
    if ren["domains"]:
        lines.append("Domains renewing in 14 days: " + ", ".join(
            f"{d['domain']} {d['renewal_at']}" for d in ren["domains"]))
    if stuck:
        lines.append("Still being set up: " + ", ".join(f"{d['domain']} ({d['status']})" for d in stuck))
    if unassigned:
        lines.append(f"No client on {len(unassigned)} domain(s) — set the order tag: " + ", ".join(unassigned[:10]))
    if rep.get("flagged"):
        lines.append(f"Placement week {rep['weekof']}: " + ", ".join(
            f"{r['domain']} {r['score']}%" for r in rep["flagged"][:10]))
    return {"lines": lines, "monthly_total": snap["monthly_total"],
            "text": ("*ScaledMail today*\n" + "\n".join("• " + l for l in lines)) if lines else ""}


def run(args) -> dict:
    c = args.cmd
    if c == "plans":
        from smartlead.scaledmail_orders import PlanStore
        return {"plans": sorted(PlanStore().all(), key=lambda p: p.get("staged_at", ""), reverse=True)}
    if c == "mark-failed":
        from smartlead.scaledmail_orders import mark_failed
        return mark_failed(args.plan_id, user=os.getenv("USER") or os.getenv("USERNAME") or "cli")
    with ScaledMailClient() as sm:
        if c == "status":
            snap = fleet.snapshot(sm, tracker=_tracker())
            return {k: snap[k] for k in ("orders", "monthly_total", "active_orders", "by_provider",
                                         "by_client", "today")} | {"inventory": snap["inventory"],
                                                                   "domain_count": len(snap["domains"])}
        if c == "renewals":
            return fleet.renewals(fleet.snapshot(sm, tracker=_tracker()), domain_days=args.days)
        if c == "domain":
            return fleet.locate(sm, _domain(args.domain), tracker=_tracker())
        if c == "orders":
            snap = fleet.snapshot(sm, tracker=_tracker())
            return {"orders": snap["orders"], "domains": [
                {k: d[k] for k in ("domain", "provider", "status", "mailboxes", "client", "order_id", "tag")}
                for d in snap["domains"]]}
        if c == "order":
            return sm.order(args.order_id)
        if c == "reports":
            return fleet.report_summary(sm.reporting())
        if c == "prewarmed":
            return sm.prewarmed()
        if c == "packages":
            return {"packages": sm.packages()}
        if c == "quote":
            provs = [p.strip() for p in args.providers.split(",") if p.strip()]
            dist = None
            if args.split:
                parts = [int(x) for x in args.split.split(",")]
                dist = dict(zip(provs, parts))
            return {"request": {"volume": args.volume, "period": args.period, "providers": provs,
                                "split": dist, "tier": args.tier},
                    "quote": sm.calculate(args.volume, provs, period=args.period,
                                          distribution=dist, tier=args.tier)}
        if c == "search":
            return {"results": sm.search_domains([_domain(d) for d in args.names][:30])}
        if c == "suggest":
            r = sm.suggest_domains(args.keyword, [t.strip() for t in args.tlds.split(",")], limit=50)
            return {"keyword": args.keyword,
                    "available": [d for d in r.get("domains") or [] if d.get("status") == "available"]}
        if c == "sync":
            from smartlead.scaledmail_asset_sync import sync
            res = sync(apply_changes=args.apply)
            return {k: res.get(k) for k in ("ok", "counts", "applied", "errors")} | {
                "ops": [{k: o[k] for k in ("action", "type", "name", "reason")} for o in res.get("ops", [])],
                "skipped": res.get("skipped", [])}
        if c == "digest":
            out = digest(sm)
            if args.post and out["text"]:
                channel = (os.getenv("SCALEDMAIL_NOTIFY_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL") or "").strip()
                token = os.getenv("SLACK_BOT_TOKEN") or os.getenv("DOMAINS_SLACK_BOT_TOKEN") or ""
                if channel and token:
                    from smartlead.notify import _post
                    out["posted"] = _post(token, channel, out["text"]) is not None
                else:
                    out["posted"] = False
                    out["note"] = "SCALEDMAIL_NOTIFY_CHANNEL not set — printed only"
            return out
        if c == "tag":
            tag = _squash(_client(args.client)) if not args.raw else args.client
            if not args.approve:
                return {"dry_run": True, "would": f"set tag {tag!r} on order {args.order_id}"}
            return {"ok": True, "result": sm.set_order_tag(args.order_id, tag, approve=True), "tag": tag}
        if c == "senders":
            from smartlead.scaledmail_orders import parse_senders
            names = parse_senders(args.names)
            if not names:
                raise ScaledMailError('give names as "First Last, First Last"')
            dom = _domain(args.domain)
            if not args.approve:
                return {"dry_run": True, "would": f"ask ScaledMail to rename senders on {dom} to "
                        + ", ".join(" ".join(n) for n in names) + " (addresses unchanged)"}
            return {"ok": True, "result": sm.swap_sender_names(dom, names, mode="name_only", approve=True)}
        if c == "redirect":
            dom = _domain(args.domain)
            if not args.approve:
                return {"dry_run": True, "would": f"ask ScaledMail to redirect {dom} to {args.url or '(none)'}"}
            return {"ok": True, "result": sm.swap_redirect(dom, args.url, approve=True)}
        if c == "swap":
            old, new = _domain(args.old), _domain(args.new)
            if not args.approve:
                return {"dry_run": True, "would": f"ask ScaledMail to replace {old} with {new} (from our inventory)"}
            return {"ok": True, "result": sm.swap_domain(old, new, source="scaledmail", approve=True)}
        if c == "cancel":
            if not args.approve:
                return {"dry_run": True, "cancel_allowed": cancel_allowed(),
                        "would": f"cancel order {args.order_id} — every mailbox in it stops"}
            return {"ok": True, "result": sm.cancel_order(args.order_id, approve=True)}
        if c == "stage":
            from smartlead.scaledmail_orders import stage_order
            return stage_order(sm, client=_client(args.client), provider=args.provider,
                               domains=[_domain(d) for d in args.domains.split(",") if d.strip()],
                               senders_text=args.senders, per_domain=args.per_domain,
                               redirect=args.redirect or "", user=args.user or "cli")
        if c == "place":
            from smartlead.scaledmail_orders import place_order
            return place_order(sm, args.plan_id, approve=args.approve, user=args.user or "cli")
        if c == "reconcile":
            from smartlead.scaledmail_orders import reconcile
            return reconcile(sm, args.plan_id)
    raise ScaledMailError(f"unknown command {c}")


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="ScaledMail (read, gated write, ledgered spend)")
    ap.add_argument("--json", action="store_true", help="one JSON line on stdout")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("status", "orders", "reports", "prewarmed", "packages", "plans"):
        sub.add_parser(name)
    p = sub.add_parser("digest"); p.add_argument("--post", action="store_true",
                                                 help="post to SCALEDMAIL_NOTIFY_CHANNEL when set")
    p = sub.add_parser("renewals"); p.add_argument("--days", type=int, default=60)
    p = sub.add_parser("domain"); p.add_argument("domain")
    p = sub.add_parser("order"); p.add_argument("order_id")
    p = sub.add_parser("quote"); p.add_argument("volume", type=int)
    p.add_argument("--providers", default="google"); p.add_argument("--split", default="")
    p.add_argument("--tier", default="low", choices=["low", "medium", "max"])
    p.add_argument("--period", default="month", choices=["month", "day"])
    p = sub.add_parser("search"); p.add_argument("names", nargs="+")
    p = sub.add_parser("suggest"); p.add_argument("keyword"); p.add_argument("--tlds", default="com")
    p = sub.add_parser("sync"); p.add_argument("--apply", action="store_true")
    p = sub.add_parser("tag"); p.add_argument("order_id"); p.add_argument("client")
    p.add_argument("--raw", action="store_true", help="use the text as the tag, not a client name")
    p.add_argument("--approve", action="store_true")
    p = sub.add_parser("senders"); p.add_argument("domain"); p.add_argument("names")
    p.add_argument("--approve", action="store_true")
    p = sub.add_parser("redirect"); p.add_argument("domain"); p.add_argument("url", nargs="?", default="")
    p.add_argument("--approve", action="store_true")
    p = sub.add_parser("swap"); p.add_argument("old"); p.add_argument("new")
    p.add_argument("--approve", action="store_true")
    p = sub.add_parser("cancel"); p.add_argument("order_id"); p.add_argument("--approve", action="store_true")
    p = sub.add_parser("stage"); p.add_argument("--client", required=True)
    p.add_argument("--provider", required=True, choices=["google", "outlook", "smtp"])
    p.add_argument("--domains", required=True); p.add_argument("--senders", required=True)
    p.add_argument("--per-domain", type=int); p.add_argument("--redirect")
    p.add_argument("--user")
    p = sub.add_parser("place"); p.add_argument("plan_id"); p.add_argument("--approve", action="store_true")
    p.add_argument("--user")
    p = sub.add_parser("reconcile"); p.add_argument("plan_id")
    p = sub.add_parser("mark-failed"); p.add_argument("plan_id")
    return ap


def main() -> int:
    args = parser().parse_args()
    try:
        out = run(args)
        code = 0
    except ScaledMailError as exc:
        out, code = {"error": str(exc)}, 1
    out = out if isinstance(out, dict) else {"result": out}
    out.setdefault("spend_allowed", spend_allowed())
    if args.json:
        print(json.dumps(out, default=str))
        return 0   # Slack reads {"error": ...} from the line; a non-zero exit would hide it
    print(json.dumps(out, indent=1, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
