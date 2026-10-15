#!/usr/bin/env python3
"""Weekly volume review, per domain: raise, hold or step back daily limits on
ScaledMail's weekly steps. Rule: smartlead/volume_rule.py; spec
docs/superpowers/specs/2026-10-15-weekly-volume-review-design.md.

Only PILOT domains are changed (Manveen, 15 Oct: test on a small patch, then
roll out to all). Every other domain is judged and reported, never changed.
Each run's decisions and inputs are saved, so the rule can be graded.

    python volume_review.py                          # dry run: print, change nothing
    python volume_review.py --post                   # change the pilot domains, save, post
                                                     #   (VOLUME_REVIEW_CHANNEL, else KILL_REPORT_CHANNEL, else C0AGVSUNEFP)
    python volume_review.py --pilot-add a.com,b.com --by U123   # their current limits become the approved start
    python volume_review.py --pilot-remove a.com
    python volume_review.py --pilot-add '*'          # roll out: every judged domain

Runs Tuesday 13:45 IST (after Mon/Tue placement tests and Kill warnings) when
VOLUME_REVIEW_ENABLED=true.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta

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

from smartlead import inbox_standard as st
from smartlead import volume_rule as vr

DEFAULT_CHANNEL = "C0AGVSUNEFP"
HEALTH_URL = "https://server.smartlead.ai/api/v1/analytics/mailbox/name-wise-health-metrics"
HEALTH_DAYS = 14
USAGE_DAYS = 5                 # recent weekdays with sending data (Smartlead's numbers run ~6 days behind)
REPUTATION_WINDOW_DAYS = 7     # Smartlead: reputation is a 7-day window; early numbers are noise


def build_rows(per_account: dict[str, list[dict]], facts: dict, health: dict, daily: dict, approved: dict,
               verdicts: dict, today: date, pilot: set[str]) -> tuple[list[dict], dict]:
    """One row per domain to decide on, and what was not reviewed (and why). Pure.
    health = {email: (sent, bounced)} over 14 days; daily = {email: [sends per day]};
    approved = {email: (limit, approved on)}; pilot = domains the review may change ("*" = all)."""
    from smartlead.report_sheet import client_label
    rows: list[dict] = []
    waiting: dict = {"warming": 0, "first_ready": None, "no_campaign": 0, "by_hand": [], "skipped": [], "rotate_ip": []}
    low_ip: dict[str, int] = {}
    for account, accounts in per_account.items():
        by_dom: dict[str, list[dict]] = defaultdict(list)
        for a in accounts:
            by_dom[str(a.get("from_email", "")).lower().split("@")[-1]].append(a)
        for dom, boxes in sorted(by_dom.items()):
            kind = st.inbox_kind(boxes[0], len(boxes))
            managed = dom in pilot
            judged: list[dict] = []
            for a in boxes:
                email = str(a.get("from_email", "")).lower()
                f = facts.get(email) or {}
                lim = approved.get(email, (None, None))[0]
                reason = vr.eligible(a, kind, today, sending=bool(f.get("sending")), approved=lim, managed=managed)
                if reason == vr.NOT_OURS:
                    continue
                rep = (a.get("warmup_details") or {}).get("warmup_reputation")
                started = st.warmup_started(a)
                if st.needs_ip_rotation(kind, rep) and started and (today - started).days >= REPUTATION_WINDOW_DAYS:
                    low_ip[dom] = min(low_ip.get(dom, 101), st._int(rep))
                if reason is None:
                    judged.append(a)
                elif reason.startswith("warming"):
                    waiting["warming"] += 1
                    ready = st.ready_date(a, kind).isoformat()
                    waiting["first_ready"] = min(filter(None, [waiting["first_ready"], ready]))
                elif reason.startswith("no campaign"):
                    waiting["no_campaign"] += 1
                elif reason.startswith("changed by hand"):
                    waiting["by_hand"].append(f"{email} {reason}")
                else:
                    waiting["skipped"].append(f"{email}: {reason}")
            if not judged:
                continue
            emails = [str(a["from_email"]).lower() for a in judged]
            limits = [st._int(a.get("message_per_day")) for a in judged]
            on = [approved.get(e, (None, None))[1] for e in emails]
            last = max((date.fromisoformat(d) for d in on if d), default=None)
            sent = sum(health.get(e, (0, 0))[0] for e in emails)
            bounced = sum(health.get(e, (0, 0))[1] for e in emails)
            sig = {"placement": vr.placement_signal({e: (facts.get(e) or {}).get("tests") or [] for e in emails},
                                                    today, since=last),
                   "bounces": vr.bounce_signal(sent, bounced),
                   "reputation": vr.reputation_signal(
                       [(a.get("warmup_details") or {}).get("warmup_reputation") for a in judged], kind),
                   "kill": vr.kill_signal([verdicts.get(e) for e in emails]),
                   "usage": vr.usage_signal([daily[e] for e in emails if e in daily],
                                            Counter(limits).most_common(1)[0][0])}
            d = vr.decide(kind, limits, sig, last_change=last, today=today)
            client = (facts.get(emails[0]) or {}).get("client") or client_label(account, judged[0].get("client_id"))
            rows.append({**d, "domain": dom, "client": client, "account": account, "pilot": "*" in pilot or managed,
                         "inboxes": [{"id": a["id"], "email": e} for a, e in zip(judged, emails)],
                         "signals": {k: list(v) for k, v in sig.items()}})
    waiting["rotate_ip"] = [f"{dom} ({rep}%)" for dom, rep in sorted(low_ip.items())]
    return rows, waiting


def _health(key: str, start: date, end: date) -> dict:
    import httpx
    r = httpx.get(HEALTH_URL, params={"api_key": key, "start_date": start.isoformat(), "end_date": end.isoformat(),
                                      "timezone": "Asia/Kolkata"}, timeout=120)
    r.raise_for_status()
    rows = ((r.json() or {}).get("data") or {}).get("email_health_metrics") or []
    return {str(x.get("from_email", "")).lower(): (int(x.get("sent") or 0), int(x.get("bounced") or 0)) for x in rows}


def _daily(key: str, today: date) -> dict[str, list[int]]:
    """Sends per inbox on the last USAGE_DAYS weekdays that have data."""
    out: dict[str, list[int]] = defaultdict(list)
    day, found = today - timedelta(days=1), 0
    while found < USAGE_DAYS and (today - day).days <= 21:
        if day.weekday() < 5:
            one = _health(key, day, day)
            if any(s for s, _ in one.values()):
                found += 1
                for e, (s, _) in one.items():
                    out[e].append(s)
        day -= timedelta(days=1)
    return dict(out)


def _clients() -> dict:
    from smartlead.accounts import discover_accounts
    from smartlead.inbox_intake import CURRENT_ACCOUNTS
    return {a.name: a for a in discover_accounts() if a.name.upper() in CURRENT_ACCOUNTS}


async def _list(acc) -> list[dict]:
    from smartlead.api import SmartleadClient
    async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
        return await sl.list_email_accounts()


def gather(today: date, store, errors: list[str]) -> tuple[list[dict], dict]:
    from renewal_review import smartlead_facts
    from smartlead import kill_rule as kr
    from smartlead.notify import redact
    facts = smartlead_facts(errors)
    verdicts = {e: v["verdict"] for e, v in kr.judge_all({e: f.get("tests") or [] for e, f in facts.items()}, today).items()}
    per_account: dict[str, list[dict]] = {}
    health: dict = {}
    daily: dict = {}
    for name, acc in _clients().items():
        try:
            per_account[name] = asyncio.run(_list(acc))
            health.update(_health(acc.api_key, today - timedelta(days=HEALTH_DAYS), today))
            daily.update(_daily(acc.api_key, today))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            errors.append(f"Smartlead {name}: {redact(str(exc))[:150]}")
    return build_rows(per_account, facts, health, daily, store.approved_limits(), verdicts, today,
                      store.pilot_domains())


async def _apply(rows: list[dict], store, today: date) -> dict:
    from smartlead.api import SmartleadClient
    sls = {}
    try:
        for name, acc in _clients().items():
            sls[name] = await SmartleadClient(acc.api_key, account_name=acc.name).__aenter__()
        return await vr.apply_rows(sls, store, rows, by="volume-review", today=today)
    finally:
        for sl in sls.values():
            await sl.__aexit__(None, None, None)


def pilot_add(domains: set[str], store, by: str) -> dict:
    """Add domains to the pilot; each inbox's current limit becomes its approved
    start, so the next review steps from where the domain is now."""
    store.set_pilot(store.pilot_domains() | domains, by=by)
    approved = store.approved_limits()
    started = []
    for acc in _clients().values():
        for a in asyncio.run(_list(acc)):
            email = str(a.get("from_email", "")).lower()
            if email.split("@")[-1] in domains and email not in approved:
                store.approve_limit(email, st._int(a.get("message_per_day")), by=by, action="pilot start", on=date.today())
                started.append(email)
    return {"pilot": sorted(store.pilot_domains()), "baselined": len(started)}


def _post(text: str) -> bool:
    import requests
    from smartlead.notify import redact
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    channel = (os.getenv("VOLUME_REVIEW_CHANNEL") or os.getenv("KILL_REPORT_CHANNEL")
               or os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL)
    if not token:
        return False
    r = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {token}"},
                      json={"channel": channel, "text": redact(text)[:3900]}, timeout=20).json()
    if not r.get("ok"):
        print(f"  [volume-review] post failed: {r.get('error')}", file=sys.stderr)
    return bool(r.get("ok"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly volume review, per domain")
    ap.add_argument("--post", action="store_true", help="change the pilot domains, save the run, post it")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--pilot-add", default=None)
    ap.add_argument("--pilot-remove", default=None)
    ap.add_argument("--by", default="")
    args = ap.parse_args()
    from smartlead.inbox_intake import IntakeStore
    today = date.today()
    store = IntakeStore()
    if args.pilot_add or args.pilot_remove:
        if args.pilot_remove:
            store.set_pilot(store.pilot_domains() - {d.strip().lower() for d in args.pilot_remove.split(",")}, by=args.by)
        out = pilot_add({d.strip().lower() for d in (args.pilot_add or "").split(",") if d.strip()}, store, args.by)
        print(json.dumps(out))
        return 0

    errors: list[str] = []
    rows, waiting = gather(today, store, errors)
    waiting["skipped"] += [f"could not read {e}" for e in errors]
    applied: dict[str, bool] = {}
    if args.post:
        todo = [r for r in rows if r["pilot"] and r["action"] in (vr.RAISE, vr.STEP_BACK)]
        if todo:
            res = asyncio.run(_apply(todo, store, today))
            for dom, r in res.items():
                applied[dom] = bool(r["changed"]) and not r["failed"] and not r["skipped"]
                waiting.setdefault("apply_problems", []).extend(r["skipped"] + r["failed"])
        store.save_review(today.isoformat(), [{**r, "applied": applied.get(r["domain"])} for r in rows])
    text = vr.review_text(rows, waiting, today, applied=applied)
    out = {"review": today.isoformat(), "domains": len(rows), "applied": applied, "text": text}
    if args.post:
        out["posted"] = _post(text)
    print(json.dumps(out, default=str) if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
