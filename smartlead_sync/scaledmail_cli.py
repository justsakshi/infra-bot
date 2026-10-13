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
    python scaledmail_cli.py setup                       # placed orders: setting up / late / LIVE
    python scaledmail_cli.py ready [--apply]             # arrived in Smartlead -> signature, client, tags
    python scaledmail_cli.py mark-failed <plan_id>       # only after checking the ScaledMail UI (--note "why")
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


def setup_lines(snap: dict) -> dict:
    """Setup watcher over the order ledger; logs ``setup_live`` the first time
    an order is seen live. Without Mongo it says so and the digest goes on."""
    from datetime import date
    from smartlead.scaledmail_orders import PlanStore, setup_watch
    try:
        store = PlanStore()
        w = setup_watch(snap, store.all(), today=date.today())
        for pid in w["newly_live"]:
            store.log(pid, "setup_live")
        return w
    except Exception as exc:  # noqa: BLE001 - the rest of the digest still posts
        return {"lines": [f"Order ledger unreadable, setup not checked: {str(exc)[:100]}"],
                "newly_live": [], "covered": set()}


def ready_lines(apply: bool) -> dict:
    """New ScaledMail inboxes in Smartlead -> signature, client, tags (inbox_ready).
    Writes only with apply=True; the schedule passes it when INBOX_READY_ENABLED=true."""
    import asyncio
    from smartlead.api import SmartleadClient
    from smartlead.inbox_ready import run_ready
    from smartlead.inbox_setup import profile_for, smartlead_key_for
    from smartlead.scaledmail_orders import PlanStore

    def sl_for(client):
        name, key = smartlead_key_for(profile_for(client)[1])
        return SmartleadClient(key, account_name=name)
    try:
        return asyncio.run(run_ready(PlanStore(), approve=apply, sl_for=sl_for,
                                     profile=lambda c: profile_for(c)[1]))
    except Exception as exc:  # noqa: BLE001 - the digest still posts
        return {"lines": [f"Inbox readiness not checked: {str(exc)[:120]}"], "ready": []}


_READY_RE = __import__("re").compile(r"^✅ Ready: (\d+) (\S+) inboxes \((\d+) domains?, plan ([^)\s]+)\)")


def _plans_by_id() -> dict:
    """plan_id -> plan from the order ledger, for client and domain names; {} without Mongo."""
    try:
        from smartlead.scaledmail_orders import PlanStore
        return {p.get("plan_id"): p for p in PlanStore().all()}
    except Exception:  # noqa: BLE001 - the post still goes out, just without names
        return {}


def ready_message(lines: list[str], plans: dict | None = None) -> tuple[list[str], list[str]]:
    """``(headline_lines, thread_parts)`` for "Inboxes ready in Smartlead". Pure.
    Headline: the inboxes made ready, summed over orders, and whose they are;
    thread: one line per order (+ its domains), then anything still waiting."""
    plans = plans or {}
    total, kinds, clients, per_order, other = 0, [], [], [], []
    for line in lines:
        m = _READY_RE.match(line)
        if not m:
            other.append("• " + line)
            continue
        n, kind, pid = int(m.group(1)), m.group(2), m.group(4)
        plan = plans.get(pid) or {}
        total += n
        if kind not in kinds:
            kinds.append(kind)
        client = plan.get("client") or ""
        if client and client not in clients:
            clients.append(client)
        doms = plan.get("domains") or []
        per_order.append("• " + (f"*{client}*: " if client else "") + line
                         + (f" Domains: {', '.join(doms)}" if doms else ""))
    kind = f"{kinds[0]} " if len(kinds) == 1 else ""
    who = f" ({', '.join(clients)})" if clients else ""
    if total:
        head = [f"✅ {total} {kind}inboxes ready in Smartlead{who}"]
    else:
        head = ["⏳ Inboxes on the way to Smartlead"]
    mismatch = sum(1 for l in per_order if "⚠️" in l)
    if mismatch:
        head.append(f"Sender name differs from the order on {mismatch} order(s), see the thread")
    waiting = sum(1 for l in other if l.startswith("• ⏳"))
    if waiting:
        head.append(f"{waiting} more order(s) still waiting for inboxes")
    parts = ["\n".join(per_order + other)] if per_order or other else []
    return head, parts


def post_lines(title: str, lines: list[str]) -> bool:
    """Post to SCALEDMAIL_NOTIFY_CHANNEL (else ZAPMAIL_NOTIFY_CHANNEL); False when not set up.
    Headline in the channel, one line per order in its thread (spec 2026-10-13)."""
    from smartlead.notify import post_with_thread
    channel = (os.getenv("SCALEDMAIL_NOTIFY_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL") or "").strip()
    token = (os.getenv("SLACK_BOT_TOKEN") or os.getenv("DOMAINS_SLACK_BOT_TOKEN") or "").strip()
    if not channel or not token:
        return False
    head, parts = ready_message(lines, _plans_by_id())
    if not any(_READY_RE.match(l) for l in lines):
        head = [title.strip("*")] + head[1:]
    return post_with_thread(token, channel, head, parts) is not None


def digest(sm) -> dict:
    """ScaledMail's part of the 09:40 "Domains & inboxes daily": the full lines
    (thread) plus the counts and sums the headline needs."""
    snap = fleet.snapshot(sm, tracker=_tracker())
    ren = fleet.renewals(snap, domain_days=14, billing_days=3)
    watch = setup_lines(snap)
    ready = ready_lines(os.getenv("INBOX_READY_ENABLED", "").lower() == "true")["lines"]
    stuck = [d for d in snap["domains"] if d["status"] != "Active" and d["domain"] not in watch["covered"]]
    unassigned = [d["domain"] for d in snap["domains"] if not d["client"]]
    rep = fleet.report_summary(sm.reporting())
    lines = list(watch["lines"]) + ready
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
        lines.append(f"No client on {len(unassigned)} domain(s) — set the order tag: " + ", ".join(unassigned))
    if rep.get("flagged"):
        lines.append(f"Placement week {rep['weekof']}: " + ", ".join(
            f"{r['domain']} {r['score']}%" for r in rep["flagged"]))
    w = watch["lines"]
    return {"lines": lines, "monthly_total": snap["monthly_total"],
            "billing": ren["billing"], "unassigned": unassigned,
            "setting_up": sum(1 for l in w if l.startswith("Setting up")),
            "late": sum(1 for l in w if l.startswith("Late")),
            "live": sum(1 for l in w if l.startswith("LIVE")),
            "ready": sum(1 for l in ready if _READY_RE.match(l)),
            "text": ("*ScaledMail today*\n" + "\n".join("• " + l for l in lines)) if lines else ""}


def run(args) -> dict:
    c = args.cmd
    if c == "plans":
        from smartlead.scaledmail_orders import PlanStore
        return {"plans": sorted(PlanStore().all(), key=lambda p: p.get("staged_at", ""), reverse=True)}
    if c == "drop":
        from smartlead.scaledmail_orders import drop_plan
        return drop_plan(args.plan_id, user=os.getenv("USER") or os.getenv("USERNAME") or "cli")
    if c == "mark-failed":
        from smartlead.scaledmail_orders import mark_failed
        return mark_failed(args.plan_id, user=os.getenv("USER") or os.getenv("USERNAME") or "cli",
                           note=args.note or "")
    with ScaledMailClient() as sm:
        if c == "status":
            snap = fleet.snapshot(sm, tracker=_tracker())
            return {k: snap[k] for k in ("orders", "monthly_total", "active_orders", "by_provider",
                                         "by_client", "today")} | {"inventory": snap["inventory"],
                                                                   "domain_count": len(snap["domains"])}
        if c == "ready":
            out = ready_lines(bool(args.apply))
            if args.post and out["ready"]:
                out["posted"] = post_lines("*Inboxes ready in Smartlead*", out["lines"])
            return out
        if c == "setup":
            w = setup_lines(fleet.snapshot(sm, tracker=_tracker()))
            return {"lines": w["lines"], "newly_live": w["newly_live"]}
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
                               redirect=args.redirect or "", user=args.user or "cli",
                               own_domains=args.own_domains)
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
    p.add_argument("--own-domains", action="store_true",
                   help="domains are already ours on GoDaddy: order mailboxes only (provider=other)")
    p.add_argument("--user")
    p = sub.add_parser("place"); p.add_argument("plan_id"); p.add_argument("--approve", action="store_true")
    p.add_argument("--user")
    p = sub.add_parser("reconcile"); p.add_argument("plan_id")
    sub.add_parser("setup", help="where placed orders are in ScaledMail's 2-3 day setup")
    p = sub.add_parser("ready", help="new inboxes in Smartlead: signature, client, tags (dry run unless --apply)")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--post", action="store_true", help="post to SCALEDMAIL_NOTIFY_CHANNEL when something became ready")
    p = sub.add_parser("mark-failed"); p.add_argument("plan_id")
    p.add_argument("--note", help="why it was not ordered (e.g. refunded to credits); kept in history")
    p = sub.add_parser("drop"); p.add_argument("plan_id")
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
