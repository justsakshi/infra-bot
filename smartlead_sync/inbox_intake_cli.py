#!/usr/bin/env python3
"""Inbox intake: new Smartlead inboxes -> the team's standard, once (spec 2026-10-13).

    python inbox_intake_cli.py                    # preview: writes nothing
    python inbox_intake_cli.py --apply --post     # write, read back, post (the cron, INBOX_INTAKE_ENABLED=true)
    python inbox_intake_cli.py --since 2026-10-12 --json
    python inbox_intake_cli.py --apply --only warmmeetings.com   # try a few inboxes first
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from datetime import date

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import inbox_standard as st


def _day(d: date | None) -> str:
    return f"{d.day} {d:%b}" if d else "?"


def _n(k: int, one: str, many: str) -> str:
    return f"{k} {one if k == 1 else many}"


def intake_message(results: dict[str, dict], today: date) -> tuple[list[str], list[str]]:
    """(headline, thread). Pure."""
    rows = [r for res in results.values() for r in res["set_up"]]
    would = [r for res in results.values() for r in res["would"]]
    waiting = [w for res in results.values() for w in res["waiting"]]
    problems = [p for res in results.values() for p in res["problems"]]
    shown = rows or would
    verb = "Set up" if rows or not would else "Would set up"
    icon = "👀" if verb == "Would set up" else ("⚠️" if problems else "✅")
    look = f", {len(problems)} {'needs' if len(problems) == 1 else 'need'} a look" if problems else ""
    head = [f"{icon} {verb} {_n(len(shown), 'new inbox', 'new inboxes')} in Smartlead{look}"]
    if shown:
        per = Counter(r["client"] for r in shown)
        first = min((r["ready_on"] for r in shown if r["ready_on"]), default=None)
        head.append(" · ".join(f"{c} {k}" for c, k in per.most_common())
                    + (f" · ready to use from {_day(first)}" if first else ""))
    parts: list[str] = []
    by_dom: dict[tuple, list[dict]] = {}
    for r in shown:
        by_dom.setdefault((r["domain"], r["kind"], r["client"]), []).append(r)
    lines = []
    for (dom, kind, client), rs in sorted(by_dom.items()):
        s = st.standard_for(kind)
        lines.append(f"• {dom} · {s['label']} · {_n(len(rs), 'inbox', 'inboxes')} · limit {s['max_email_per_day']}, "
                     f"warmup {s['total_warmup_per_day']}/day, reply {s['reply_rate']}%, weekdays only · "
                     f"ready from {_day(rs[0]['ready_on'])}")
    if lines:
        parts.append("\n".join(["*Set up*" if verb == "Set up" else "*Would set up*"] + lines))
    if waiting:
        parts.append("\n".join(["*Waiting for the rest of a delivery*"] + ["• " + w for w in waiting]))
    if problems:
        parts.append("\n".join(["*Needs a look*"] + ["• " + p for p in problems]))
    parts.append("_Signature, client, tags, daily limit and warmup are read back from Smartlead before an inbox "
                 "counts as set up. Smartlead does not show ramp, auto-adjust or weekdays-only, so those are set but "
                 "not re-read._")
    return head, parts


def with_scaledmail_clients(tracker: dict, sm_domains: list[dict]) -> dict:
    """The tracker's domain rows, plus ScaledMail's own order client for any
    domain the tracker does not know (swapped domains, orders placed outside
    the bot; 15 Oct). The tracker wins where both have it. Pure."""
    out = dict(tracker)
    for d in sm_domains:
        dom = str(d.get("domain") or "").strip().lower()
        if dom and d.get("client") and dom not in out:
            out[dom] = {"client": d["client"], "provider": "Scaledmail"}
    return out


def should_post(results: dict[str, dict], previous: list[str]) -> bool:
    """Post when something was set up or a problem is new; standing problems
    stay in the 09:40 digest instead of every 2 hours. Pure."""
    problems = {p for r in results.values() for p in r.get("problems") or []}
    return any(r.get("set_up") for r in results.values()) or bool(problems - set(previous))


def main() -> int:
    ap = argparse.ArgumentParser(description="Set new Smartlead inboxes up once")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--since", default=os.getenv("INTAKE_SINCE", "2026-10-12"))
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--only", default=None, help="one inbox or one domain")
    args = ap.parse_args()

    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    from smartlead.inbox_intake import CURRENT_ACCOUNTS, IntakeStore, deliveries, run_account
    from smartlead.inbox_setup import profile_for
    from smartlead.notify import redact

    today, since = date.today(), date.fromisoformat(args.since)
    try:
        from smartlead.scaledmail_orders import PlanStore
        plan_store = PlanStore()
        plans = plan_store.all()
    except Exception:  # noqa: BLE001
        plan_store, plans = None, []
    try:
        from smartlead.inbox_jobs import JobStore
        jobs = JobStore().list()
    except Exception:  # noqa: BLE001
        jobs = []
    try:
        from smartlead.zapmail_asset_sync import read_tracker
        tracker = {k: v for k, v in read_tracker().items() if "@" not in k}
    except Exception:  # noqa: BLE001
        tracker = {}
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if configured():
            from smartlead.scaledmail_fleet import snapshot
            with ScaledMailClient() as sm:
                tracker = with_scaledmail_clients(tracker, snapshot(sm)["domains"])
    except Exception:  # noqa: BLE001 - without it those inboxes are reported, not guessed
        pass
    store = IntakeStore()
    by_domain = deliveries(plans, jobs)
    results: dict[str, dict] = {}

    async def run_all():
        for acc in discover_accounts():
            if acc.name.upper() not in CURRENT_ACCOUNTS:
                continue
            try:
                async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
                    results[acc.name] = await run_account(
                        sl, acc.name, approve=args.apply, since=since, today=today, by_domain=by_domain,
                        tracker=tracker, store=store, profile=lambda c: profile_for(c)[1], only=args.only)
            except Exception as exc:  # noqa: BLE001 - say so, keep the other account
                results[acc.name] = {"set_up": [], "would": [], "waiting": [], "complete": [],
                                     "problems": [f"{acc.name}: could not read Smartlead ({redact(str(exc))[:120]})"]}
    asyncio.run(run_all())

    if args.apply and plan_store is not None:
        for pid in {p for r in results.values() for p in r["complete"]}:
            plan = next((p for p in plans if p.get("plan_id") == pid), None)
            if plan is not None:
                plan_store.log(pid, "in_smartlead", found=plan.get("mailboxes"), expected=plan.get("mailboxes"))
                plan_store.log(pid, "ready", by="inbox_intake")

    head, parts = intake_message(results, today)
    posted = False
    if args.post and should_post(results, store.posted_problems()):
        from smartlead.notify import post_with_thread
        channel = (os.getenv("SCALEDMAIL_NOTIFY_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL") or "").strip()
        token = (os.getenv("SLACK_BOT_TOKEN") or "").strip()
        posted = bool(channel and token and post_with_thread(token, channel, head, parts))
        if posted:
            store.save_posted_problems([p for r in results.values() for p in r["problems"]])
    out = {"headline": head, "thread": parts, "posted": posted,
           "counts": {k: sum(len(r[k]) for r in results.values()) for k in ("set_up", "would", "waiting", "problems")}}
    print(json.dumps(out, default=str) if args.json else "\n".join(head + [""] + parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
