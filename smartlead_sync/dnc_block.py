#!/usr/bin/env python3
"""Do Not Contact -> global block list, in every Smartlead account (Manveen, 24 Nov 2026).

Two BettrData leads wrote "please remove me" / "unsubscribe" on 23 Nov. Smartlead tagged them
Do Not Contact, which only stops that lead in that one campaign: neither address was on either
account's global block list, so any later upload could have emailed them again. The n8n bounce
logger blocks bounces only (it is fed by the bounce webhook, which never sees a reply).

Every lead in a campaign whose category is Do Not Contact (id 4) or who used the unsubscribe
link is added to the global block list of EVERY account (PL, which also holds Melior, and
BettrData), the way the bounce logger blocks bounces. Only the address, never the domain: one
person opting out speaks for themselves, not their colleagues.

    python dnc_block.py            # dry run: counts only
    python dnc_block.py --apply    # add the missing addresses
    python dnc_block.py --all      # every campaign, not just the recent ones (the backfill)
    python dnc_block.py --report   # opt-outs by account, campaign and reason, from opt_outs (no addresses)

Every opt-out is also kept in Mongo `opt_outs` (HEALTH_HISTORY_DB, one document per address:
which account and campaigns, why, when first seen, when blocked in each account), so any agent can
count and study opt-outs by client, campaign or copy later (Manveen, 24 Nov: "keep track of all
kinds of data ... shareable by any other agent"). Addresses stay in that private database, never in
this public repo, its logs or a public page.

Daily it reads campaigns that are not drafts and were created in the last RECENT_DAYS days or are
still ACTIVE / PAUSED; --all reads every campaign once. Prints counts only, never addresses.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

import httpx

from smartlead.accounts import discover_accounts
from smartlead.api import get_json, raise_for_status
from smartlead.config import BASE_URL as SMARTLEAD_BASE_URL

DO_NOT_CONTACT = 4          # Smartlead's built-in lead category id (GET /leads/fetch-categories)
RECENT_DAYS = 120
PAGE = 100
ADD_BATCH = 500             # 500 per call worked (15 Oct 2026 backfill)


def reason(row: dict) -> str | None:
    if row.get("lead_category_id") == DO_NOT_CONTACT:
        return "do_not_contact"
    if (row.get("lead") or {}).get("is_unsubscribed"):
        return "unsubscribe_link"
    return None


def record(row: dict, account: str, campaign: dict) -> dict | None:
    """What opt_outs keeps for one opted-out lead row (None for any other row)."""
    email, why = opted_out(row), reason(row)
    if not email or not why:
        return None
    lead = row.get("lead") or {}
    return {"email": email, "domain": email.split("@", 1)[1], "account": account, "reason": why,
            "campaign_id": str(campaign.get("id")), "campaign": campaign.get("name") or "",
            "lead_status": row.get("status"), "lead_added": row.get("created_at"),
            "company": lead.get("company_name") or "", "first_name": lead.get("first_name") or "",
            "last_name": lead.get("last_name") or ""}


def opted_out(row: dict) -> str | None:
    """The lowercased address of a /campaigns/{id}/leads row that asked not to be contacted."""
    lead = row.get("lead") or {}
    email = str(lead.get("email") or "").strip().lower()
    if "@" not in email:
        return None
    if row.get("lead_category_id") == DO_NOT_CONTACT or lead.get("is_unsubscribed"):
        return email
    return None


def wanted_campaigns(campaigns: list[dict], everything: bool, now: datetime) -> list[dict]:
    if everything:
        return [c for c in campaigns if str(c.get("status") or "").upper() != "DRAFTED"]
    cutoff = now - timedelta(days=RECENT_DAYS)
    out = []
    for c in campaigns:
        status = str(c.get("status") or "").upper()
        if status == "DRAFTED":
            continue
        try:
            created = datetime.fromisoformat(str(c.get("created_at")).replace("Z", "+00:00"))
        except ValueError:
            created = None
        if status in ("ACTIVE", "PAUSED") or (created and created >= cutoff):
            out.append(c)
    return out


def missing(found: set[str], listed: set[str]) -> list[str]:
    return sorted(e for e in found if e not in listed and e.split("@", 1)[1] not in listed)


def _campaign_opt_outs(key: str, account: str, campaign: dict) -> list[dict]:
    out, offset = [], 0
    while True:
        data = get_json(f"{SMARTLEAD_BASE_URL}/campaigns/{campaign['id']}/leads", key, offset=offset, limit=PAGE)
        rows = (data.get("data") if isinstance(data, dict) else data) or []
        out += [r for r in (record(x, account, campaign) for x in rows) if r]
        if len(rows) < PAGE:
            return out
        offset += PAGE


def _block_list(key: str) -> set[str]:
    out, offset = set(), 0
    while True:
        rows = get_json(f"{SMARTLEAD_BASE_URL}/leads/get-domain-block-list", key, limit=1000, offset=offset) or []
        out |= {str(r.get("email_or_domain") or "").strip().lower() for r in rows}
        if len(rows) < 1000:
            return out
        offset += 1000


def _add(key: str, emails: list[str]) -> int:
    added = 0
    for i in range(0, len(emails), ADD_BATCH):
        resp = httpx.post(f"{SMARTLEAD_BASE_URL}/leads/add-domain-block-list", params={"api_key": key},
                          json={"domain_block_list": emails[i:i + ADD_BATCH]}, timeout=120)
        raise_for_status(resp)  # no key in the error text
        added += int((resp.json() or {}).get("uploadCount") or 0)
    return added


def save(records: list[dict], blocked: dict[str, list[str]], now: datetime) -> str | None:
    """Upsert opt_outs; returns why it could not, or None. Never fails the job: the block matters more."""
    try:
        from pymongo import UpdateOne
        from smartlead.digest import _db
        ops = []
        for r in records:
            sightings = {k: r[k] for k in ("account", "campaign_id", "campaign", "reason", "lead_status", "lead_added")}
            ops.append(UpdateOne({"_id": r["email"]}, {
                "$setOnInsert": {"first_seen": now},
                "$set": {"domain": r["domain"], "company": r["company"], "first_name": r["first_name"],
                         "last_name": r["last_name"], "last_seen": now},
                "$addToSet": {"sightings": sightings}}, upsert=True))
        for account, emails in blocked.items():
            ops += [UpdateOne({"_id": e}, {"$set": {f"blocked.{account}": now}}) for e in emails]
        if ops:
            _db()["opt_outs"].bulk_write(ops, ordered=False)
        return None
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__


def summary(docs: list[dict]) -> list[str]:
    """opt_outs counted by account, reason and campaign: no addresses, safe to paste anywhere."""
    from collections import Counter
    by_acc, by_reason, by_camp = Counter(), Counter(), Counter()
    for d in docs:
        for s in d.get("sightings") or []:
            by_acc[s.get("account")] += 1
            by_reason[s.get("reason")] += 1
            by_camp[(s.get("account"), s.get("campaign"))] += 1
    lines = [f"{len(docs)} opted-out address(es)",
             "by account: " + ", ".join(f"{k} {v}" for k, v in by_acc.most_common()),
             "by reason: " + ", ".join(f"{k} {v}" for k, v in by_reason.most_common()),
             "top campaigns:"]
    lines += [f"  {n}  {acc} · {camp}" for (acc, camp), n in by_camp.most_common(15)]
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="add the missing addresses (default: dry run)")
    ap.add_argument("--all", action="store_true", help="read every campaign (backfill)")
    ap.add_argument("--report", action="store_true", help="print opt-out counts from Mongo and stop")
    args = ap.parse_args()
    if args.report:
        from smartlead.digest import _db
        print("\n".join(summary(list(_db()["opt_outs"].find({}, {"sightings": 1})))))
        return 0

    accounts = discover_accounts()
    if not accounts:
        print("[dnc-block] no Smartlead account keys found")
        return 1
    found: set[str] = set()
    records: list[dict] = []
    blocked: dict[str, list[str]] = {}
    now = datetime.now(timezone.utc)
    failed = []
    for acc in accounts:
        try:
            camps = wanted_campaigns(get_json(f"{SMARTLEAD_BASE_URL}/campaigns", acc.api_key) or [],
                                     args.all, now)
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{acc.name} campaigns ({type(exc).__name__})")
            continue
        mine = set()
        for c in camps:
            try:
                got = _campaign_opt_outs(acc.api_key, acc.name, c)
                records += got
                mine |= {r["email"] for r in got}
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{acc.name} campaign {c['id']} ({type(exc).__name__})")
        print(f"[dnc-block] {acc.name}: {len(camps)} campaign(s) read, {len(mine)} opted-out address(es)")
        found |= mine

    for acc in accounts:
        try:
            todo = missing(found, _block_list(acc.api_key))
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{acc.name} block list ({type(exc).__name__})")
            continue
        if not todo:
            print(f"[dnc-block] {acc.name}: all {len(found)} already on the block list")
        elif args.apply:
            print(f"[dnc-block] {acc.name}: {_add(acc.api_key, todo)} of {len(todo)} missing address(es) added")
            blocked[acc.name] = todo
        else:
            print(f"[dnc-block] {acc.name}: {len(todo)} missing (dry run, --apply adds them)")
    if args.apply:
        why = save(records, blocked, now)
        print(f"[dnc-block] opt_outs: {len({r['email'] for r in records})} address(es) recorded"
              if not why else f"[dnc-block] opt_outs not saved ({why})")
    for f in failed:
        print(f"[dnc-block] not read: {f}")
    return 2 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
