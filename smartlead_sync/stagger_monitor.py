#!/usr/bin/env python3
"""Pause staggered companies that replied or bounced (daily, before release).

Polls each batch's campaigns for per-lead statistics and folds them into
company state. A reply that binds the organisation ("Do Not Contact",
"Interested", "Meeting Request") stops the company; a reply that is only that
person's answer ("Not Interested", "Wrong Person") stops the lead and leaves
colleagues queued; an out-of-office stops nothing; a bounce or block stops
the company. Smartlead classifies the replies itself (lead_category /
ignore_reply), so this makes no judgement of its own.

Polling rather than a webhook is deliberate: a missed webhook would leave a
company being emailed after it answered, and this is the check that must not
be missed. It is safe to run repeatedly - company states are terminal, so a
second pass changes nothing.

Run this BEFORE stagger_executor.py --release so the day's send already
reflects yesterday's replies.

Usage:
    python3 stagger_monitor.py --all
    python3 stagger_monitor.py --batch <id> [--dry-run] [--json]
"""
from __future__ import annotations

import argparse
import asyncio
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

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.stagger import CompanyState, LeadState
from smartlead.stagger_events import classify_rows
from smartlead.stagger_store import LEADS, StaggerStore


def _find_account(name: str):
    for a in discover_accounts():
        if a.name.lower() == name.lower():
            return a
    raise SystemExit(f"ERROR: no Smartlead account named '{name}'")


async def _check_batch(store: StaggerStore, batch: dict, dry_run: bool) -> dict:
    batch_id = batch.get("id") or str(batch["_id"])
    name = batch.get("name", batch_id)
    campaign_ids = [c for c in (batch.get("tracked_campaign_id"),
                                batch.get("untracked_campaign_id")) if c]
    if not campaign_ids:
        return {"batch_id": batch_id, "name": name, "skipped": "no campaigns"}

    acc = _find_account(batch["account"])
    rows: list[dict] = []
    async with SmartleadClient(acc.api_key, acc.name) as client:
        for cid in campaign_ids:
            try:
                rows.extend(await client.get_campaign_statistics(str(cid)))
            except Exception as exc:  # noqa: BLE001
                # An unreadable campaign means we cannot prove a company is
                # still safe to email, so say so rather than releasing blind.
                return {"batch_id": batch_id, "name": name,
                        "error": f"campaign {cid}: {exc}"}

    events = classify_rows(rows)
    if not events:
        return {"batch_id": batch_id, "name": name, "replied": 0, "bounced": 0,
                "person_only": 0, "auto_replies": 0}

    # Only leads in THIS batch matter; a campaign may hold leads from
    # elsewhere, and pausing a company we never staggered would be wrong.
    ours = {d["email"]: d for d in store.db[LEADS].find(
        {"batch_id": batch_id, "email": {"$in": list(events)}},
        {"email": 1, "company": 1, "state": 1, "_id": 0})}

    paused_reply: list[str] = []
    paused_bounce: list[str] = []
    person_only: list[str] = []
    autos = 0

    for email, event in events.items():
        doc = ours.get(email)
        if doc is None:
            continue
        if event.kind == "auto_reply":
            autos += 1
            continue

        # "Not interested" / "wrong person" is one person's answer, not the
        # company's. Stop contacting THEM and leave their colleagues queued -
        # holding several leads per company is pointless otherwise.
        if event.pauses_lead_only:
            person_only.append(email)
            if not dry_run:
                store.mark_lead(batch_id, email, LeadState.REPLIED,
                                reason=event.detail or event.category)
            continue

        state = event.company_state
        if state is None:
            continue
        if dry_run:
            (paused_reply if state is CompanyState.PAUSED_REPLY
             else paused_bounce).append(doc["company"])
            continue

        store.mark_lead(batch_id, email,
                        LeadState.REPLIED if state is CompanyState.PAUSED_REPLY
                        else LeadState.BOUNCED,
                        reason=event.detail or event.category)
        if store.set_company_state(batch_id, doc["company"], state,
                                   reason=f"{event.category} ({email})"):
            (paused_reply if state is CompanyState.PAUSED_REPLY
             else paused_bounce).append(doc["company"])

    return {"batch_id": batch_id, "name": name,
            "replied": len(set(paused_reply)), "bounced": len(set(paused_bounce)),
            "person_only": len(person_only), "auto_replies": autos,
            "dry_run": dry_run,
            "paused_companies": sorted(set(paused_reply) | set(paused_bounce))[:20]}


def main() -> int:
    ap = argparse.ArgumentParser(description="Pause staggered companies on reply/bounce")
    ap.add_argument("--batch", help="Batch id (default: every batch)")
    ap.add_argument("--all", action="store_true", help="Check every batch")
    ap.add_argument("--dry-run", action="store_true", help="Report without pausing")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    store = StaggerStore()
    if args.batch:
        batch = store.get_batch(args.batch)
        if not batch:
            print(f"ERROR: no batch {args.batch}", file=sys.stderr)
            return 2
        batch["id"] = args.batch
        batches = [batch]
    else:
        batches = store.list_batches()

    results = [asyncio.run(_check_batch(store, b, args.dry_run)) for b in batches]

    if args.json:
        print(json.dumps({"batches": results}))
        return 0

    for r in results:
        if r.get("skipped"):
            print(f"{r['name']}: skipped ({r['skipped']})")
        elif r.get("error"):
            print(f"{r['name']}: ERROR {r['error']}")
        else:
            prefix = "[dry run] " if r.get("dry_run") else ""
            print(f"{prefix}{r['name']}: {r['replied']} company(ies) paused on reply, "
                  f"{r['bounced']} on bounce, {r.get('person_only', 0)} person-only "
                  f"no (company kept), {r['auto_replies']} auto-reply ignored")
            if r.get("paused_companies"):
                print("    " + ", ".join(r["paused_companies"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
