#!/usr/bin/env python3
"""Fleet watch: a daily record of every inbox we use (domains in the deliverability
sheet, smartlead/tracked.py), problems that START from now on with what to do, and a
scorecard of whether the suggestions helped. Never writes to Smartlead.

Spec: docs/superpowers/specs/2026-10-15-fleet-watch-design.md. Checks and
messages: smartlead/fleet.py.

    python fleet_watch.py                 # dry run: print what would be posted, save nothing
    python fleet_watch.py --post          # save the day's record and cases, post new problems
                                          #   (FLEET_WATCH_CHANNEL, default #test-channel C0AV3HTPP0S)
    python fleet_watch.py --save          # save the day's record only
    python fleet_watch.py --baseline      # record what is already wrong as known; never posted
    python fleet_watch.py --scorecard [--post]

Daily 10:30 IST; scorecard Mondays 10:50 IST.
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

from smartlead import fleet as fl
from smartlead import inbox_standard as st

CHANNEL = "C0AV3HTPP0S"          # #test-channel during the trial (Manveen, 15 Oct)
METRIC_DAYS = 10                 # Smartlead's per-day numbers arrive ~6 days late: re-read and fill in
HISTORY_DAYS = 8
BLOCKLIST_FRESH_DAYS = 8


def record(account: str, a: dict, facts: dict, verdicts: dict, per_domain: dict, approved: dict, health: dict,
           today: date) -> dict:
    """One inbox on one day. Pure."""
    from smartlead.report_sheet import client_label
    email = str(a.get("from_email", "")).lower()
    dom = email.split("@")[-1]
    f = facts.get(email) or {}
    kind = st.inbox_kind(a, per_domain.get(dom, 1))
    names = [str(t.get("tag_name") or t.get("name") or "") for t in a.get("tags") or []]
    setup = st.SETUP_TAG in names
    vendor = next((n.split("-")[0] for n in names if n.startswith(("ScaledMail-", "Zapmail-"))), None)
    w = a.get("warmup_details") or {}
    started = st.warmup_started(a)
    ready = st.ready_date(a, kind) if setup else None
    sent, bounced = health.get(email, (0, 0))
    connected = f.get("connected")
    if connected is None:
        connected = bool(a.get("is_smtp_success") and a.get("is_imap_success"))
    return {"email": email, "domain": dom, "date": today.isoformat(), "account": account,
            "client": f.get("client") or client_label(account, a.get("client_id")), "kind": kind, "vendor": vendor,
            "setup": setup, "limit": st._int(a.get("message_per_day")),
            "approved_limit": approved.get(email, (None, None))[0],
            "warmup_on": str(w.get("status") or "").upper() == "ACTIVE", "rep": st._int(w.get("warmup_reputation")),
            "connected": connected, "campaigns": len(f.get("campaigns") or []), "sending": bool(f.get("sending")),
            "ready_on": ready.isoformat() if ready else None,
            "warmup_started": started.isoformat() if started else None,
            "test_date": f.get("tested_on"), "test_pct": f.get("pct"), "verdict": verdicts.get(email),
            "sent_14d": int(sent), "bounced_14d": int(bounced)}


class FleetStore:
    """inbox_daily: one document per inbox per day; fleet_cases: one per case."""

    def __init__(self, daily=None, cases=None) -> None:
        if daily is None or cases is None:
            from smartlead.zapmail_asset_sync import _db
            db = _db()
            if db is None:
                raise RuntimeError("Mongo unavailable; nothing saved")
            daily, cases = db["inbox_daily"], db["fleet_cases"]
        self.daily, self.cases = daily, cases

    def save_state(self, records: list[dict]) -> None:
        for r in records:
            self.daily.update_one({"email": r["email"], "date": r["date"]}, {"$set": r}, upsert=True)

    def save_metrics(self, by_email: dict[str, dict[str, dict]]) -> None:
        for email, days in by_email.items():
            for day, m in days.items():
                self.daily.update_one({"email": email, "date": day}, {"$set": {"email": email, "date": day, **m}},
                                      upsert=True)

    def history(self, emails: list[str], since: str) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = defaultdict(list)
        for d in self.daily.find({"email": {"$in": list(emails)}, "date": {"$gte": since}}, {"_id": 0}):
            if "rep" in d:                       # state documents only (metric-only days have no state)
                out[d["email"]].append(d)
        return {e: sorted(v, key=lambda d: d["date"]) for e, v in out.items()}

    def open_cases(self) -> dict[str, dict]:
        return {c["key"]: c for c in self.cases.find({"closed_on": None}, {"_id": 0})}

    def all_cases(self) -> list[dict]:
        return [c for c in self.cases.find({}, {"_id": 0})]

    def save_cases(self, cases: list[dict]) -> None:
        for c in cases:
            self.cases.update_one({"key": c["key"], "opened_on": c.get("opened_on")} if c.get("opened_on")
                                  else {"key": c["key"]}, {"$set": c}, upsert=True)


def _day_metrics(key: str, today: date) -> dict[str, dict[str, dict]]:
    """{email: {day: {sent, bounced, replied, unsubscribed}}} for the last METRIC_DAYS days."""
    import httpx
    from volume_review import HEALTH_URL
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for n in range(1, METRIC_DAYS + 1):
        day = (today - timedelta(days=n)).isoformat()
        r = httpx.get(HEALTH_URL, params={"api_key": key, "start_date": day, "end_date": day, "timezone": "Asia/Kolkata"},
                      timeout=120)
        r.raise_for_status()
        for x in ((r.json() or {}).get("data") or {}).get("email_health_metrics") or []:
            out[str(x.get("from_email", "")).lower()][day] = {
                "sent": int(x.get("sent") or 0), "bounced": int(x.get("bounced") or 0),
                "replied": int(x.get("replied") or 0), "unsubscribed": int(x.get("unsubscribed") or 0)}
    return out


def _blocklisted(today: date) -> dict[str, list[str]]:
    try:
        from pymongo import MongoClient
        from smartlead.config import BLACKLIST_COLLECTION, HEALTH_HISTORY_DB
        col = MongoClient(os.getenv("MONGO_URI", ""), serverSelectionTimeoutMS=5000)[HEALTH_HISTORY_DB][BLACKLIST_COLLECTION]
        since = (today - timedelta(days=BLOCKLIST_FRESH_DAYS)).isoformat()
        latest: dict[str, dict] = {}
        for d in col.find({"date": {"$gte": since}}, {"_id": 0, "domain": 1, "date": 1, "listed_on": 1}):
            if d["domain"] not in latest or d["date"] > latest[d["domain"]]["date"]:
                latest[d["domain"]] = d
        return {k: v["listed_on"] for k, v in latest.items() if v.get("listed_on")}
    except Exception:  # noqa: BLE001 - no blocklist data is not a reason to skip the rest
        return {}


def gather(today: date, errors: list[str]) -> tuple[list[dict], dict]:
    from renewal_review import smartlead_facts
    from smartlead import kill_rule as kr
    from smartlead.client_filter import is_excluded_inbox
    from smartlead.inbox_intake import CURRENT_CLIENTS, IntakeStore
    from smartlead.notify import redact
    from volume_review import HEALTH_DAYS, _clients, _health, _list
    facts = smartlead_facts(errors)
    verdicts = {e: v["verdict"] for e, v in kr.judge_all({e: f.get("tests") or [] for e, f in facts.items()}, today).items()}
    from smartlead.tracked import only_tracked, tracked_domains
    approved = IntakeStore().approved_limits()
    tracked = tracked_domains()               # inboxes we use (deliverability sheet); None = sheet unreadable
    if tracked is None:
        errors.append("deliverability sheet (judging every inbox today)")
    records, metrics = [], {}
    for name, acc in _clients().items():
        try:
            accts = only_tracked([a for a in asyncio.run(_list(acc))
                                  if not is_excluded_inbox({"email": a.get("from_email") or "", "tags": a.get("tags", [])})],
                                 tracked)
            health = _health(acc.api_key, today - timedelta(days=HEALTH_DAYS), today)
            metrics.update(_day_metrics(acc.api_key, today))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            errors.append(f"Smartlead {name}: {redact(str(exc))[:150]}")
            continue
        per_domain = Counter(str(a.get("from_email", "")).lower().split("@")[-1] for a in accts)
        for a in accts:
            r = record(name, a, facts, verdicts, per_domain, approved, health, today)
            if r["client"] in CURRENT_CLIENTS:
                records.append(r)
    return records, metrics


def _post(text: str) -> bool:
    import requests
    from smartlead.notify import redact, tidy
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return False
    r = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {token}"},
                      json={"channel": os.getenv("FLEET_WATCH_CHANNEL") or CHANNEL, "text": redact(tidy(text))[:3900]},
                      timeout=20).json()
    if not r.get("ok"):
        print(f"  [fleet-watch] post failed: {r.get('error')}", file=sys.stderr)
    return bool(r.get("ok"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Fleet watch (read-only for Smartlead)")
    ap.add_argument("--post", action="store_true", help="save the day's record and cases, post new problems")
    ap.add_argument("--save", action="store_true", help="save the day's record only (no cases, no post)")
    ap.add_argument("--baseline", action="store_true",
                    help="save the record and today's problems as already known (never posted)")
    ap.add_argument("--scorecard", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    today = date.today()

    if args.scorecard:
        from smartlead.inbox_intake import IntakeStore
        ledger = IntakeStore()
        text = fl.scorecard_text(FleetStore().all_cases(), today, pilot=ledger.pilot_domains(),
                                 control=ledger.control_domains())
        posted = _post(text) if args.post else None
        print(json.dumps({"text": text, "posted": posted}) if args.json else text)
        return 0

    errors: list[str] = []
    records, metrics = gather(today, errors)
    store = FleetStore() if (args.post or args.save or args.baseline) else None
    if store:
        store.save_state(records)
        store.save_metrics(metrics)
    if args.save and not args.post:
        print(json.dumps({"records": len(records), "metric_inboxes": len(metrics), "errors": errors}))
        return 0
    if store:
        history = store.history([r["email"] for r in records], (today - timedelta(days=HISTORY_DAYS)).isoformat())
        open_before = store.open_cases()
    else:
        history = {r["email"]: [r] for r in records}
        open_before = {}
    fired = fl.checks(records, history, _blocklisted(today), today)
    opened, open_now, closed = fl.update_cases(open_before, fired, records, today, baseline=args.baseline)
    if store:
        store.save_cases(open_now + closed)
    counts = Counter(c["kind"] for c in open_now if c not in opened and not c.get("baseline"))
    text = fl.watch_text(opened, dict(counts), today)
    if errors:
        text += "\n:warning: Could not read: " + "; ".join(errors)
    out = {"records": len(records), "opened": len(opened), "open": len(open_now), "closed": len(closed), "text": text}
    if args.post and (opened or errors):
        out["posted"] = _post(text)
    print(json.dumps(out, default=str) if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
