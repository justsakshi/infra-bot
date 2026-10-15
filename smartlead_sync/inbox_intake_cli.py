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


def scaledmail_tracker(tracker: dict, problems: list[str]) -> dict:
    """``with_scaledmail_clients`` fed live; a failed ScaledMail read becomes a
    problem line (posted once) instead of silently leaving domains unplaced."""
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if not configured():
            return tracker
        from smartlead.scaledmail_fleet import snapshot
        with ScaledMailClient() as sm:
            return with_scaledmail_clients(tracker, snapshot(sm)["domains"])
    except Exception as exc:  # noqa: BLE001
        from smartlead.notify import redact
        problems.append(f"ScaledMail could not be read ({redact(str(exc))[:100]}): inboxes on ScaledMail "
                        "domains the tracker doesn't know were not set up this run")
        return tracker


READY_CHANNEL = "C0ACLJZ7JHE"   # #testing-4866 (Manveen, 15 Oct); INBOX_READY_CHANNEL overrides


def ready_message(announcements: list[dict]) -> str:
    """One post: domains ready to send now, then any past their date but still warming. Pure."""
    ready = [a["line"] for a in announcements if a["state"] == "ready"]
    low = [a["line"] for a in announcements if a["state"] != "ready"]
    parts = []
    if ready:
        n = len(ready)
        parts.append("\n".join([f"✅ Ready to send now: {n} domain{'s' if n != 1 else ''}"] + ready
                               + ["Run a Smart Delivery placement test before adding them to campaigns."]))
    if low:
        parts.append("\n".join(["⚠️ Reached the warmup date but not ready yet:"] + low))
    return "\n\n".join(parts)


def post_ready(announcements: list[dict], store, post) -> bool:
    """Post the ready message with ``post(text)`` and mark each domain
    announced only if it went out (a failed post is retried next run)."""
    if not announcements:
        return False
    if not post(ready_message(announcements)):
        return False
    for a in announcements:
        store.mark_announced(a["key"])
    return True


ASK_USER = "U09TQ9D7YNM"   # Manveen; INTAKE_ASK_USER overrides


def ask_blocks(ask: dict) -> list[dict]:
    """The DM asking whose a new domain's inboxes are, with three buttons. Pure."""
    acc = {"PRECISE_LEADS": "Precise Leads"}.get(ask["account"], ask["account"])
    n = ask["inboxes"]
    text = (f"{n} new {ask['kind']} inbox{'es' if n != 1 else ''} on *{ask['domain']}* in the {acc} Smartlead "
            "account, and the bot doesn't know whose they are. Pick one to set them up (daily limit, warmup, "
            "signature, tags); nothing changes until you do.")

    def btn(label, key):
        return {"type": "button", "action_id": f"intake_client_{key}", "value": ask["domain"],
                "text": {"type": "plain_text", "text": label}}
    return [{"type": "section", "text": {"type": "mrkdwn", "text": text}},
            {"type": "actions", "elements": [btn("Precise Leads", "pl"), btn("Melior", "melior"),
                                             btn("Not ours, skip", "skip")]}]


CHOICES = {"pl": "Precise Leads", "melior": "Melior", "skip": "skip"}


def save_answer(domain: str, choice: str, by: str, store) -> str:
    """Record Manveen's answer for a domain ("pl", "melior" or "skip")."""
    if choice not in CHOICES:
        raise ValueError(f"choice must be one of {sorted(CHOICES)}")
    store.set_answer(domain.strip().lower(), CHOICES[choice], by)
    return CHOICES[choice]


def post_asks(asks: list[dict], store, send) -> int:
    """DM each ask with ``send(ask)``; mark a domain asked only if its DM went out."""
    sent = 0
    for ask in asks:
        if send(ask):
            store.mark_asked(ask["domain"])
            sent += 1
    return sent


def _dm_ask(ask: dict) -> bool:
    import requests
    token = (os.getenv("DOMAINS_SLACK_BOT_TOKEN") or "").strip()
    user = (os.getenv("INTAKE_ASK_USER") or ASK_USER).strip()
    if not token:
        return False
    try:
        r = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {token}"},
                          json={"channel": user, "text": f"Whose are the new inboxes on {ask['domain']}?",
                                "blocks": ask_blocks(ask)}, timeout=15)
        return bool(r.json().get("ok"))
    except Exception:  # noqa: BLE001 - asked again next run
        return False


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
    ap.add_argument("--answer", nargs=2, metavar=("DOMAIN", "CHOICE"),
                    help="Manveen's answer from Slack (pl | melior | skip): save it and set that domain up now")
    ap.add_argument("--by", default="", help="who answered (Slack user id)")
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
    read_problems: list[str] = []
    tracker = scaledmail_tracker(tracker, read_problems)
    store = IntakeStore()
    answered = None
    if args.answer:
        domain, choice = args.answer
        answered = save_answer(domain, choice, args.by, store)
        if answered == "skip":
            print(json.dumps({"domain": domain.lower(), "client": "skip", "set_up": 0, "problems": []}))
            return 0
        args.apply, args.post, args.only = True, False, domain.strip().lower()
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
                results[acc.name] = {"set_up": [], "would": [], "waiting": [], "complete": [], "ready": [],
                                     "problems": [f"{acc.name}: could not read Smartlead ({redact(str(exc))[:120]})"]}
    asyncio.run(run_all())
    if read_problems:
        results.setdefault("ScaledMail", {"set_up": [], "would": [], "waiting": [], "complete": [], "problems": [], "ready": []})
        results["ScaledMail"]["problems"] += read_problems

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
    if answered:
        rows = [r for res in results.values() for r in res["set_up"]]
        ready = min((r["ready_on"] for r in rows if r["ready_on"]), default=None)
        print(json.dumps({"domain": args.only, "client": answered, "set_up": len(rows),
                          "ready_on": str(ready) if ready else None,
                          "problems": [p for res in results.values() for p in res["problems"] if args.only in p]}))
        return 0
    asks = [a for r in results.values() for a in r.get("ask") or []]
    if asks and args.apply and args.post:
        post_asks(asks, store, _dm_ask)
    elif asks:
        parts.append("*Would ask Manveen whose these are*\n" + "\n".join(f"• {a['domain']} ({a['inboxes']} {a['kind']})" for a in asks))
    announcements = [a for r in results.values() for a in r.get("ready") or []]
    ready_posted = False
    if args.apply and args.post:
        from smartlead.notify import _post
        token = (os.getenv("SLACK_BOT_TOKEN") or "").strip()
        channel = (os.getenv("INBOX_READY_CHANNEL") or READY_CHANNEL).strip()
        ready_posted = bool(token) and post_ready(announcements, store, lambda text: _post(token, channel, text))
    elif announcements:
        parts.append("*Would post to the test channel*\n" + ready_message(announcements))
    out = {"headline": head, "thread": parts, "posted": posted, "ready": announcements, "ready_posted": ready_posted,
           "counts": {k: sum(len(r[k]) for r in results.values()) for k in ("set_up", "would", "waiting", "problems")}}
    print(json.dumps(out, default=str) if args.json else "\n".join(head + [""] + parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
