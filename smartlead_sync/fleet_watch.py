#!/usr/bin/env python3
"""Fleet watch: a daily record of every inbox we use (domains in the deliverability
sheet, smartlead/tracked.py), problems that START from now on with what to do, and a
scorecard of whether the suggestions helped. Never writes to Smartlead.

Spec: docs/superpowers/specs/2026-10-15-fleet-watch-design.md. Checks and
messages: smartlead/fleet.py.

    python fleet_watch.py                 # dry run: print what would be posted, save nothing
    python fleet_watch.py --post          # save the day's record and cases, post new problems
                                          #   (FLEET_WATCH_CHANNEL, default the infra channel C0AGVSUNEFP);
                                          #   a case counts as told only once Slack accepts the post,
                                          #   so a refused post goes out again the next run
    python fleet_watch.py --save          # save the day's record only
    python fleet_watch.py --baseline      # record what is already wrong as known; never posted
    python fleet_watch.py --scorecard [--post]

Daily 10:30 IST; scorecard Mondays 10:50 IST. Exits 1 when a post was refused.
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

from smartlead.api import raise_for_status
from smartlead import fleet as fl
from smartlead import inbox_standard as st

CHANNEL = "C0AGVSUNEFP"          # the infra channel; #test-channel (C0AV3HTPP0S) is gone (audit 7 Nov 2026)
METRIC_DAYS = 10                 # Smartlead's per-day numbers arrive ~6 days late: re-read and fill in
HISTORY_DAYS = 8
READ_DROP = 0.15                 # an account's inbox count down more than this since the last run: closes nothing today
LIVE_STATUSES = ("ACTIVE", "SCHEDULED")   # campaigns that send, or will, without anyone touching them


def record(account: str, a: dict, facts: dict, verdicts: dict, per_domain: dict, approved: dict, health: dict,
           today: date, live: dict | None = None, warm: dict | None = None) -> dict:
    """One inbox on one day. Pure. ``live`` = {email: [live non-test campaign names]} (see _live_campaigns);
    ``warm`` = {email: (warm-up emails sent, landed in spam) over 7 days} (see _warmup_spam), None = not read."""
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
            "sent_14d": int(sent), "bounced_14d": int(bounced),
            "warm_sent_7d": (warm or {}).get(email, (None, None))[0],
            "warm_spam_7d": (warm or {}).get(email, (None, None))[1],
            "tags": names, "live_campaigns": list(dict.fromkeys((live or {}).get(email) or []))}


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

    def last_counts(self, before: str, since: str) -> dict[str, int]:
        """{account: inboxes recorded} on the latest day before ``before`` (back to ``since``)."""
        by_day: dict[str, Counter] = defaultdict(Counter)
        for d in self.daily.find({"date": {"$gte": since, "$lt": before}},
                                 {"_id": 0, "date": 1, "account": 1, "rep": 1}):
            if "rep" in d and str(d.get("date")) < before:      # state documents only
                by_day[d["date"]][d.get("account")] += 1
        return dict(by_day[max(by_day)]) if by_day else {}

    def open_cases(self) -> dict[str, dict]:
        return {c["key"]: c for c in self.cases.find({"closed_on": None}, {"_id": 0})}

    def all_cases(self) -> list[dict]:
        return [c for c in self.cases.find({}, {"_id": 0})]

    def save_cases(self, cases: list[dict]) -> None:
        for c in cases:
            self.cases.update_one({"key": c["key"], "opened_on": c.get("opened_on")} if c.get("opened_on")
                                  else {"key": c["key"]}, {"$set": c}, upsert=True)

    def mark_announced(self, cases: list[dict], today: date) -> None:
        """Slack accepted the post that named these cases."""
        for c in cases:
            self.cases.update_one({"key": c["key"], "opened_on": c.get("opened_on")},
                                  {"$set": {"announced": True, "announced_on": today.isoformat()}})


def _day_metrics(key: str, today: date) -> dict[str, dict[str, dict]]:
    """{email: {day: {sent, bounced, replied, unsubscribed}}} for the last METRIC_DAYS days."""
    import httpx
    from volume_review import HEALTH_URL
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for n in range(1, METRIC_DAYS + 1):
        day = (today - timedelta(days=n)).isoformat()
        r = httpx.get(HEALTH_URL, params={"api_key": key, "start_date": day, "end_date": day, "timezone": "Asia/Kolkata"},
                      timeout=120)
        raise_for_status(r)  # no key in the error text
        for x in ((r.json() or {}).get("data") or {}).get("email_health_metrics") or []:
            out[str(x.get("from_email", "")).lower()][day] = {
                "sent": int(x.get("sent") or 0), "bounced": int(x.get("bounced") or 0),
                "replied": int(x.get("replied") or 0), "unsubscribed": int(x.get("unsubscribed") or 0)}
    return out


def _blocklisted(col=None) -> dict[str, dict] | None:
    """The newest blacklist_checks row per domain, however old: an old listing still counts and the post
    says when it was checked (audit 7 Nov: an 8-day cut-off made listed domains look clean). None when
    Mongo can't be read: then no blocklist case opens or closes that day."""
    try:
        if col is None:
            from pymongo import MongoClient
            from smartlead.config import BLACKLIST_COLLECTION, HEALTH_HISTORY_DB
            col = MongoClient(os.getenv("MONGO_URI", ""),
                              serverSelectionTimeoutMS=5000)[HEALTH_HISTORY_DB][BLACKLIST_COLLECTION]
        latest: dict[str, dict] = {}
        for d in col.find({}, {"_id": 0, "domain": 1, "date": 1, "listed_on": 1, "lookup_errors": 1,
                               "inconclusive_zones": 1}):
            dom = str(d.get("domain") or "").lower()
            if dom and (dom not in latest or str(d.get("date") or "") > str(latest[dom].get("date") or "")):
                latest[dom] = d
        return latest
    except Exception:  # noqa: BLE001 - no blocklist data is not a reason to skip the rest
        return None


async def _live_campaigns(acc) -> dict[str, list[str]]:
    """{inbox email: [ACTIVE or SCHEDULED campaigns it is on, test campaigns left out]}. Read-only."""
    from smartlead.api import SmartleadClient
    from smartlead.v2_results import is_test_campaign
    out: dict[str, list[str]] = defaultdict(list)
    async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
        for c in await sl.list_campaigns() or []:
            if str(c.get("status") or "").upper() not in LIVE_STATUSES or is_test_campaign(c.get("name")):
                continue
            for a in await sl.get_campaign_email_accounts(str(c["id"])) or []:
                e = str(a.get("from_email") or "").lower()
                if e:
                    out[e].append(str(c.get("name") or c["id"]))
    return dict(out)


def _read_ok(names, errors: list[str]) -> set[str]:
    """The accounts read in full today: no 'Smartlead <name>: ...' error from the facts read or from gather."""
    return {n for n in names if not any(str(e).startswith(f"Smartlead {n}:") for e in errors)}


def gather(today: date, errors: list[str]) -> tuple[list[dict], dict, set[str], dict]:
    """(records, per-day metrics, the accounts read in full, roster). A case on an account not read in full
    never closes that day. roster (smartlead.fleet.left): the sheet's domains, every inbox Smartlead lists
    today (facts read + inbox list) and the ones left out on purpose; a case missing from the records
    closes as 'gone' only when the roster shows it left."""
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
    present, dropped = set(facts), set()      # facts: its own inbox read fails loudly on a bad page
    clients = _clients()
    for name, acc in clients.items():
        try:
            listed = asyncio.run(_list(acc))
            accts = only_tracked([a for a in listed
                                  if not is_excluded_inbox({"email": a.get("from_email") or "", "tags": a.get("tags", [])})],
                                 tracked)
            live = asyncio.run(_live_campaigns(acc))
            health = _health(acc.api_key, today - timedelta(days=HEALTH_DAYS), today)
            metrics.update(_day_metrics(acc.api_key, today))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            errors.append(f"Smartlead {name}: {redact(str(exc))[:150]}")
            continue
        try:
            warm = asyncio.run(_warmup_spam(acc, accts))
        except Exception as exc:  # noqa: BLE001 - only the warm-up spam check is lost; its open cases stay open
            warm = None
            errors.append(f"warm-up stats {name}: {redact(str(exc))[:150]}")
        per_domain = Counter(str(a.get("from_email", "")).lower().split("@")[-1] for a in accts)
        kept = set()
        for a in accts:
            r = record(name, a, facts, verdicts, per_domain, approved, health, today, live, warm)
            if r["client"] in CURRENT_CLIENTS:
                records.append(r)
                kept.add(r["email"])
        emails = {str(a.get("from_email") or "").lower() for a in listed} - {""}
        present |= emails
        dropped |= emails - kept
    roster = {"tracked": tracked, "present": present, "dropped": dropped}
    return records, metrics, _read_ok(list(clients), errors), roster


def warm_counts(stats) -> tuple[int, int] | None:
    """(warm-up emails sent, landed in spam) over the last 7 days from one /warmup-stats answer; None when
    the answer is missing or carries no counts. Pure."""
    s = stats[0] if isinstance(stats, list) and stats else stats
    if not isinstance(s, dict) or s.get("sent_count") is None:
        return None
    try:
        return int(s.get("sent_count") or 0), int(s.get("spam_count") or 0)
    except (TypeError, ValueError):
        return None


async def _warmup_spam(acc, accounts: list[dict]) -> dict:
    """{inbox email: (sent, spam)} for the inboxes whose warm-up stats were read. Read-only."""
    from smartlead.api import SmartleadClient
    ids = {str(a["id"]): str(a.get("from_email") or "").lower() for a in accounts if a.get("id") is not None}
    async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
        stats = await sl.fetch_all_warmup_stats(list(ids))
    out = {}
    for aid, email in ids.items():
        counts = warm_counts(stats.get(aid))
        if counts is not None:
            out[email] = counts
    return out


def _post(headline, thread=()) -> bool:
    """Headline in the channel, details in its thread. True only when Slack accepted the headline."""
    from smartlead import notify
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    if not token:
        print("  [fleet-watch] SLACK_BOT_TOKEN not set: nothing posted", file=sys.stderr)
        return False
    lines = str(headline).split("\n") if isinstance(headline, str) else list(headline)
    ok = notify.post_with_thread(token, os.getenv("FLEET_WATCH_CHANNEL") or CHANNEL, lines, list(thread))
    if not ok:
        print("  [fleet-watch] post failed", file=sys.stderr)
    return bool(ok)


def _short_reads(read: set[str], records: list[dict], last: dict[str, int]) -> dict[str, tuple[int, int]]:
    """{account: (inboxes today, at the last run)} for accounts read today whose count fell by more than
    READ_DROP: the inbox list may have stopped early, so none of their cases close today."""
    now = Counter(r.get("account") for r in records)
    return {a: (now.get(a, 0), last[a]) for a in sorted(read)
            if last.get(a) and now.get(a, 0) < last[a] * (1 - READ_DROP)}


def daily(store, records: list[dict], metrics: dict, read: set[str], blocklist: dict | None, today: date,
          errors: list[str], *, post: bool = False, baseline: bool = False, poster=None,
          roster: dict | None = None) -> dict:
    """Save the day, update the cases, and post. Cases are saved first with announced=False and marked
    announced only when Slack accepts the post, so a refused post goes out again the next run. Open cases
    from before that field existed are posted once as a backlog. ``store`` None = dry run. ``roster``: see
    gather; without it a case missing from today's records stays open."""
    poster = poster or _post
    read = set(read)
    if store:
        since = (today - timedelta(days=HISTORY_DAYS)).isoformat()
        for a, (k, was) in _short_reads(read, records, store.last_counts(today.isoformat(), since)).items():
            errors.append(f"Smartlead {a} in full: {k} inboxes today, {was} at the last run, "
                          f"so none of its cases close today")
            read.discard(a)
        store.save_state(records)
        store.save_metrics(metrics)
        history = store.history([r["email"] for r in records], since)
        open_before = store.open_cases()
    else:
        history = {r["email"]: [r] for r in records}
        open_before = {}
    fired = fl.checks(records, history, blocklist or {}, today)
    opened, open_now, closed = fl.update_cases(open_before, fired, records, today, baseline=baseline, read=read,
                                               blocklist=blocklist, roster=roster)
    if store:
        store.save_cases(open_now + closed)
    new_keys = {c["key"] for c in opened}
    resend = [c for c in open_now if c.get("announced") is False and not c.get("baseline") and c["key"] not in new_keys]
    backlog = [c for c in open_now if c.get("announced") is None and not c.get("baseline")] if store else []
    to_post = opened + resend
    posting = {c["key"] for c in to_post}
    rest = [c for c in open_now if c["key"] not in posting and not c.get("baseline")]
    head, thread = fl.watch_post(to_post, dict(Counter(c["kind"] for c in rest)), today,
                                 unknown=sum(1 for c in rest if c.get("unknown")))
    if errors:
        head.append(":warning: Could not read: " + "; ".join(errors))
    out = {"records": len(records), "opened": len(opened), "resent": len(resend), "backlog": len(backlog),
           "open": len(open_now), "closed": len(closed), "text": fl.as_text(head, thread), "failed": False}
    if post and (to_post or errors):
        out["posted"] = bool(poster(head, thread))
        if out["posted"] and store and to_post:
            store.mark_announced(to_post, today)
        out["failed"] |= not out["posted"]
    if post and backlog:
        bh, bt = fl.backlog_post(backlog, today)
        out["backlog_text"] = fl.as_text(bh, bt)
        out["backlog_posted"] = bool(poster(bh, bt))
        if out["backlog_posted"]:
            store.mark_announced(backlog, today)
        out["failed"] |= not out["backlog_posted"]
    return out


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
        head, thread = fl.scorecard_post(FleetStore().all_cases(), today, pilot=ledger.pilot_domains(),
                                         control=ledger.control_domains())
        text = fl.as_text(head, thread)
        posted = _post(head, thread) if args.post else None
        print(json.dumps({"text": text, "posted": posted}) if args.json else text)
        return 1 if posted is False else 0

    errors: list[str] = []
    records, metrics, read, roster = gather(today, errors)
    blocklist = _blocklisted()
    if blocklist is None:
        errors.append("blocklist history (Mongo)")
    store = FleetStore() if (args.post or args.save or args.baseline) else None
    if args.save and not args.post:
        store.save_state(records)
        store.save_metrics(metrics)
        print(json.dumps({"records": len(records), "metric_inboxes": len(metrics), "errors": errors}))
        return 0
    out = daily(store, records, metrics, read, blocklist, today, errors, post=args.post, baseline=args.baseline,
                roster=roster)
    print(json.dumps(out, default=str) if args.json
          else out["text"] + ("\n\n" + out["backlog_text"] if out.get("backlog_text") else ""))
    return 1 if out["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
