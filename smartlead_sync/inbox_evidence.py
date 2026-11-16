#!/usr/bin/env python3
"""Weekly per-inbox evidence from real sends (t17). Rules: smartlead/inbox_evidence.py and docs/INBOX_EVIDENCE.md.

    python inbox_evidence.py                  print the per-client lines, save nothing (default)
    python inbox_evidence.py --save           keep the week in Mongo inbox_evidence (the All Inboxes columns read it)
    python inbox_evidence.py --save --post    also the daily digest: one done line per client, the detail in its thread

Read-only against Smartlead. Every lead emailed in the last 28 days in a live campaign (DT and test campaigns
skipped) gets its message history read once; Mongo lead_senders keeps each lead's stats_id -> inbox map, so a lead is
read again only when it has a new email or a new reply. Calls are paced under Smartlead's 200 a minute and wait 60 s
after a 429 (Smartlead support, 16 Nov 2026). The team's trusted reply logs (REPLY_LOGS, JSON list of {client, sheet,
tab, email, date}) are read with the service account and checked against what Smartlead shows. Scheduled in index.js:
Sundays 09:00 IST, after the real-recipient check. INBOX_EVIDENCE=off stops it. Exits 1 when no account was read.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import inbox_evidence as ie

IST = timezone(timedelta(hours=5, minutes=30))
LIVE = {"ACTIVE", "PAUSED", "COMPLETED", "STOPPED"}
PAUSE = 0.35            # seconds between message-history calls
RATE_WAIT = 60.0        # after a 429
TRIES = 4
SOURCE = "inbox-evidence"
COLLECTION, CACHE = "inbox_evidence", "lead_senders"


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    return MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]


async def _history(http, key: str, cid: str, lid, errors: Counter):
    """One lead's message history, or None. The key travels in the query string: only the error TYPE is kept."""
    from smartlead.config import BASE_URL
    for n in range(TRIES):
        try:
            r = await http.get(f"{BASE_URL}/campaigns/{cid}/leads/{lid}/message-history", params={"api_key": key})
        except Exception as exc:  # noqa: BLE001 - the library's text holds the URL with the key
            errors["message history " + type(exc).__name__] += 1
            return None
        if r.status_code == 429:
            errors["429 (waited 60 s)"] += 1
            await asyncio.sleep(RATE_WAIT)
            continue
        if r.status_code >= 500 and n < TRIES - 1:
            await asyncio.sleep(5 * (n + 1))
            continue
        if r.status_code != 200:
            errors[f"message history HTTP {r.status_code}"] += 1
            return None
        return r.json()
    errors["message history: still 429 after 4 tries"] += 1
    return None


def _needs_read(cached: dict | None, rows: list[dict]) -> bool:
    if not cached:
        return True
    sids = set((cached.get("sids") or {}))
    if any(str(r.get("stats_id")) not in sids for r in rows if r.get("stats_id")):
        return True
    return any(r.get("reply_time") for r in rows) and not cached.get("replied")


async def read_account(acc, today: date, errors: Counter, cache, counts: Counter) -> tuple[dict, dict]:
    """(leads {lead email: Lead}, {inbox: client})."""
    import httpx
    from smartlead.api import SmartleadClient
    from smartlead.report_sheet import client_label
    from smartlead.sheets import _is_test_campaign
    start = today - timedelta(days=ie.DAYS)
    leads, emailed = {}, set()
    async with SmartleadClient(acc.api_key, acc.name) as c, httpx.AsyncClient(timeout=60) as http:
        inbox_client = {str(a.get("from_email") or "").strip().lower(): str(client_label(acc.name, a.get("client_id")))
                        for a in await c.list_email_accounts() or []}
        for camp in await c.list_campaigns() or []:
            name, status = str(camp.get("name") or ""), str(camp.get("status") or "").upper()
            if status not in LIVE or name.startswith(("DT ", "One-off placement test")) or _is_test_campaign(name):
                continue
            cid = str(camp["id"])
            try:
                stats = await c.get_campaign_statistics(cid)
            except Exception as exc:  # noqa: BLE001
                errors["campaign statistics " + type(exc).__name__] += 1
                continue
            by_lead: dict[str, list] = defaultdict(list)
            for r in stats or []:
                e = str(r.get("lead_email") or "").strip().lower()
                if "@" in e:
                    by_lead[e].append(r)
            recent = {e: rows for e, rows in by_lead.items()
                      if any((d := ie._day(r.get("sent_time"))) and start < d <= today for r in rows)}
            if not recent:
                continue
            try:
                ids = {str((x.get("lead") or {}).get("email") or "").strip().lower(): (x.get("lead") or {}).get("id")
                       for x in await c.get_campaign_leads(cid) or []}
            except Exception as exc:  # noqa: BLE001
                errors["campaign leads " + type(exc).__name__] += 1
                continue
            for e, rows in recent.items():
                emailed.add(e)
                lid = ids.get(e)
                key = f"{acc.name}:{cid}:{lid}"
                cached = cache.get(key) if cache is not None else None
                if lid and _needs_read(cached, rows):
                    h = await _history(http, acc.api_key, cid, lid, errors)
                    counts["read"] += 1
                    await asyncio.sleep(PAUSE)
                    if h is not None:
                        sids, body = ie.sent_by(h)
                        cached = {"_id": key, "sids": sids, "body": body, "replied": bool(body)}
                        if cache is not None:
                            cache.put(cached)
                elif cached:
                    counts["cached"] += 1
                else:
                    errors["lead id missing"] += 1
                hist = {"history": [{"type": "SENT", "stats_id": s, "from": f} for s, f in (cached or {}).get("sids", {}).items()]
                        + ([{"type": "REPLY", "email_body": cached["body"]}] if (cached or {}).get("body") else [])}
                prev = leads.get(e)
                if prev:       # one lead in two campaigns: keep both campaigns' emails
                    prev.rows += rows
                    prev.history["history"] += hist["history"]
                else:
                    leads[e] = ie.Lead(rows=list(rows), history=hist)
            print(f"[inbox-evidence] {acc.name} | {name[:50]} | leads {len(recent)} | read {counts['read']} "
                  f"cached {counts['cached']}", flush=True)
    return leads, inbox_client


class Cache:
    """Mongo lead_senders: {_id: account:campaign:lead id, sids: {stats_id: inbox}, body, replied}. No lead address."""

    def __init__(self, col):
        self.col = col
        self.docs = {d["_id"]: d for d in col.find({})}

    def get(self, key):
        return self.docs.get(key)

    def put(self, doc):
        self.docs[doc["_id"]] = doc
        self.col.replace_one({"_id": doc["_id"]}, doc, upsert=True)


def reply_logs() -> tuple[dict[str, list[dict]], dict[str, str]]:
    """({client: [{email, date}]}, {client: why not read}) from REPLY_LOGS. Never raises."""
    try:
        cfg = json.loads(os.getenv("REPLY_LOGS", "") or "[]")
    except ValueError:
        return {}, {"all": "REPLY_LOGS is not valid JSON"}
    out, why = {}, {}
    if not cfg:
        return {}, {}
    try:
        from smartlead.sheets import _authorize
        gc = _authorize()
    except Exception as exc:  # noqa: BLE001
        return {}, {x.get("client", "?"): f"service account ({type(exc).__name__})" for x in cfg}
    for x in cfg:
        client = x.get("client", "?")
        try:
            vals = gc.open_by_key(x["sheet"]).worksheet(x["tab"]).get_all_values()
        except Exception as exc:  # noqa: BLE001 - a sheet the service account can't open is reported, not worked around
            why[client] = type(exc).__name__
            continue
        head = [h.strip() for h in vals[0]] if vals else []
        try:
            ei, di = head.index(x.get("email", "Email")), head.index(x.get("date", "Reply date"))
        except ValueError:
            why[client] = "column not found"
            continue
        rows = []
        for v in vals[1:]:
            d = parse_day(v[di] if di < len(v) else "")
            if ei < len(v) and "@" in v[ei]:
                rows.append({"email": v[ei], "date": d.isoformat() if d else ""})
        out.setdefault(client, []).extend(rows)
        time.sleep(1.0)
    return out, why


def parse_day(s: str) -> date | None:
    """A reply-log date: ISO, US m/d/Y (the logs are kept by a US-hours team), or '12 Nov 2026'. Pure."""
    s = str(s or "").strip()
    for part in (s, s.split(" ")[0], s.split("T")[0]):
        for f in ("%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%b %d, %Y", "%d-%b-%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(part, f).date()
            except ValueError:
                continue
    return None


def build(today: date, leads: dict, inbox_client: dict, provider_of, logs: dict, why: dict) -> dict:
    """The week's record: per client summary + crosscheck, per inbox rows. Pure apart from provider_of."""
    cells, unknown = ie.tally(leads, provider_of, today)
    known_sent = sum(c["sent"] for c in cells.values())
    by_client: dict[str, dict] = defaultdict(dict)
    for (inbox, rp), c in cells.items():
        by_client[inbox_client.get(inbox, "unknown")][(inbox, rp)] = c
    replied: dict[str, str | None] = {}
    for e, lead in leads.items():
        sids, body = ie.sent_by(lead.history)
        for r in lead.rows:
            if ie.kind(r, body) in ("reply", "ooo"):
                replied[e] = sids.get(str(r.get("stats_id"))) or replied.get(e)
    emailed = set(leads)
    clients, inboxes = {}, {}
    for client, cl_cells in sorted(by_client.items()):
        rates = ie.base_rates(cl_cells)
        rows = ie.inbox_rows(cl_cells, rates)
        inboxes.update({k: {**v, "client": client} for k, v in rows.items()})
        s = {"inboxes": len(rows), "sent": sum(c["sent"] for c in cl_cells.values()),
             "replies": sum(c["replies"] for c in cl_cells.values()), "ooo": sum(c["ooo"] for c in cl_cells.values()),
             "bounces": sum(c["bounces"] for c in cl_cells.values()),
             "quiet": sum(1 for v in rows.values() if v["label"].startswith("quiet")),
             "not_enough": sum(1 for v in rows.values() if v["label"] == "not enough data"),
             "known_pct": round(100 * known_sent / (known_sent + unknown), 1) if known_sent + unknown else 100.0}
        cc = ie.crosscheck(logs[client], replied, emailed, today) if client in logs else None
        clients[client] = {"summary": s, "rates": rates, "crosscheck": cc, "log_not_read": why.get(client, "")}
    return {"_id": today.isoformat(), "days": ie.DAYS, "clients": clients, "inboxes": inboxes, "unknown_sends": unknown}


def digest_parts(rec: dict, errors: dict, read: int, total: int) -> tuple[list, list, list[str]]:
    from smartlead import digest as dg
    t = rec["_id"]
    done = [dg.Done(ie.digest_line(cl, v["summary"], v["crosscheck"]), key=f"{SOURCE}:{cl}:{t}")
            for cl, v in rec["clients"].items() if cl in ("Precise Leads", "Melior", "Bettrdata")]
    asks = []
    if read < total:
        asks.append(dg.Ask(f"The weekly inbox evidence read {read} of {total} Smartlead accounts.",
                           "Look at the inbox-evidence log on Render; it runs again next Sunday.",
                           key=f"{SOURCE}:partial", kind="broken"))
    detail = []
    for cl, v in rec["clients"].items():
        cc = v["crosscheck"]
        lines = [f"{cl}: base rate (replies + OOO per email) " +
                 ", ".join(f"{p} {r:.2%}" for p, r in sorted(v["rates"].items()))]
        if v["log_not_read"]:
            lines.append(f"Team reply log not read: {v['log_not_read']}")
        for e, why in sorted((cc or {}).get("misses", {}).items()):
            lines.append(f"• in the reply log, not matched: {e}: {why}")
        quiet = [f"• {k}: {r['label']}" for k, r in sorted(rec["inboxes"].items())
                 if r["client"] == cl and r["label"].startswith("quiet")]
        detail.append("\n".join(lines + quiet))
    if errors:
        detail.append("Errors: " + ", ".join(f"{k} x{v}" for k, v in sorted(errors.items())))
    return asks, done, detail


def main() -> int:
    ap = argparse.ArgumentParser(description="Per-inbox evidence from real sends")
    ap.add_argument("--save", action="store_true")
    ap.add_argument("--post", action="store_true", help="the daily digest (needs --save)")
    ap.add_argument("--today")
    ap.add_argument("--dump", help="also write the record to this JSON file (holds inbox and lead addresses)")
    args = ap.parse_args()
    if os.getenv("INBOX_EVIDENCE", "").strip().lower() == "off":
        print("[inbox-evidence] INBOX_EVIDENCE=off")
        return 0
    from provider_reply_watch import mx_providers
    from smartlead.accounts import PAST_CLIENT_ACCOUNTS, discover_accounts
    today = date.fromisoformat(args.today) if args.today else datetime.now(IST).date()
    errors, counts = Counter(), Counter()
    try:
        db = _db()
        cache = Cache(db[CACHE])
    except Exception as exc:  # noqa: BLE001 - without the cache every lead is read; nothing saved
        print(f"[inbox-evidence] Mongo: {type(exc).__name__}; reading every lead, nothing saved")
        db, cache = None, None
    leads, inbox_client, read, total = {}, {}, 0, 0
    for acc in discover_accounts():
        if acc.name.strip().lower().replace("_", " ") in PAST_CLIENT_ACCOUNTS:
            continue
        total += 1
        try:
            lds, ic = asyncio.run(read_account(acc, today, errors, cache, counts))
            for e, l in lds.items():          # same lead in two accounts: keep both
                if e in leads:
                    leads[e].rows += l.rows
                    leads[e].history["history"] += l.history["history"]
                else:
                    leads[e] = l
            inbox_client.update(ic)
            read += 1
        except Exception as exc:  # noqa: BLE001
            errors[f"account {acc.name} {type(exc).__name__}"] += 1
    prov = asyncio.run(mx_providers({e.split("@", 1)[1] for e in leads}))
    logs, why = reply_logs()
    rec = build(today, leads, inbox_client, lambda d: prov.get(d, ""), logs, why)
    rec.update({"errors": dict(errors), "reads": dict(counts), "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    for cl, v in rec["clients"].items():
        print(ie.digest_line(cl, v["summary"], v["crosscheck"]))
    print(f"[inbox-evidence] histories read {counts['read']}, from cache {counts['cached']}, errors {dict(errors)}")
    if args.dump:
        with open(args.dump, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=1, default=str)
    if args.save and db is not None and read:
        db[COLLECTION].replace_one({"_id": rec["_id"]}, json.loads(json.dumps(rec, default=str)), upsert=True)
        print("[inbox-evidence] Mongo: saved")
    if args.post and args.save:
        from smartlead import digest as dg
        if dg.on():
            asks, done, detail = digest_parts(rec, dict(errors), read, total)
            dg.report(SOURCE, asks, done, detail)
            print("[inbox-evidence] to the daily digest")
    return 0 if read else 1


if __name__ == "__main__":
    sys.exit(main())
