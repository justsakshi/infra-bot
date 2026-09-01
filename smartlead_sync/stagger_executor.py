#!/usr/bin/env python3
"""Company-staggered lead upload and release.

Upload a lead CSV once; the leads are then released a few per company per
day so outreach spreads across as many companies as possible instead of
piling onto a few. See smartlead/stagger.py for why breadth beats depth.

Usage:
    # inspect a CSV without storing anything
    python3 stagger_executor.py --preview leads.csv

    # store a batch (does not send)
    python3 stagger_executor.py --upload leads.csv --name "Q3 field services" \
        --account DARLEAN --tracked-campaign 3867136 --untracked-campaign 3871482 \
        --daily-cap 100 --tracked-first 20

    # release today's leads (the cron entry point)
    python3 stagger_executor.py --release --batch <id>
    python3 stagger_executor.py --release --all

    python3 stagger_executor.py --batches            # list batches + progress

Add --json for one machine-readable line on stdout. --dry-run plans a release
without claiming or sending anything.
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
from smartlead.api import LEADS_PER_REQUEST, SmartleadClient
from smartlead.stagger import LeadState, parse_leads_csv, plan_release, suggest_mapping
from smartlead.stagger_store import StaggerStore


def _find_account(name: str):
    for a in discover_accounts():
        if a.name.lower() == name.lower():
            return a
    known = ", ".join(a.name for a in discover_accounts())
    raise SystemExit(f"ERROR: no Smartlead account named '{name}'. Known: {known}")


def _preview(path: str, company_column: str | None, as_json: bool) -> int:
    with open(path, "rb") as fh:
        result = parse_leads_csv(fh.read(), company_column=company_column)

    per_company: dict[str, int] = {}
    for lead in result.leads:
        per_company[lead.company] = per_company.get(lead.company, 0) + 1
    biggest = sorted(per_company.items(), key=lambda kv: -kv[1])[:5]

    payload = {
        "headers": result.headers,
        "mapping": result.mapping,
        "leads": len(result.leads),
        "companies": result.companies,
        "skipped_no_email": result.skipped_no_email,
        "skipped_duplicate": result.skipped_duplicate,
        "largest_companies": [{"company": c, "leads": n} for c, n in biggest],
        "sample": result.leads[0].to_smartlead() if result.leads else None,
    }
    if as_json:
        print(json.dumps(payload))
        return 0

    print(f"{len(result.leads)} leads across {result.companies} companies")
    if result.skipped_no_email or result.skipped_duplicate:
        print(f"  skipped: {result.skipped_no_email} without a valid email, "
              f"{result.skipped_duplicate} duplicate")
    print("  column mapping:")
    for header, target in result.mapping.items():
        note = "" if target in ("email",) else ""
        print(f"    {header:32} -> {target}{note}")
    if biggest:
        print("  largest companies: " + ", ".join(f"{c} ({n})" for c, n in biggest))
    return 0


def _upload(args) -> int:
    with open(args.upload, "rb") as fh:
        result = parse_leads_csv(fh.read(), company_column=args.company_column)
    if not result.leads:
        print("ERROR: no valid leads in that CSV", file=sys.stderr)
        return 2

    store = StaggerStore()
    batch_id = store.create_batch(
        name=args.name or os.path.basename(args.upload),
        account=args.account,
        tracked_campaign_id=args.tracked_campaign,
        untracked_campaign_id=args.untracked_campaign,
        daily_cap=args.daily_cap, tracked_first=args.tracked_first,
        created_by=args.created_by, company_column=args.company_column,
    )
    counts = store.add_leads(batch_id, result.leads)
    payload = {"batch_id": batch_id, "leads": counts["leads"],
               "companies": counts["companies"],
               "skipped_no_email": result.skipped_no_email,
               "skipped_duplicate": result.skipped_duplicate,
               "daily_cap": args.daily_cap, "tracked_first": args.tracked_first}
    if args.json:
        print(json.dumps(payload))
    else:
        print(f"Batch {batch_id}: {counts['leads']} leads across "
              f"{counts['companies']} companies stored. Nothing sent yet - "
              f"run --release (or wait for the daily cron).")
    return 0


async def _release_batch(store: StaggerStore, batch: dict, dry_run: bool) -> dict:
    batch_id = batch["id"] if "id" in batch else str(batch["_id"])
    name = batch.get("name", batch_id)

    if batch.get("paused"):
        return {"batch_id": batch_id, "name": name, "skipped": "paused", "released": 0}

    books = store.company_books(batch_id)
    chosen = plan_release(books, int(batch.get("daily_cap", 0)))
    if not chosen:
        return {"batch_id": batch_id, "name": name, "released": 0,
                "note": "nothing eligible - every company is paused, exhausted or empty"}

    # Confirm-then-send: the first N leads of a batch go to the open-tracking
    # twin so the team can see the campaign is landing; everything after that
    # goes to the untracked campaign, which is what actually runs.
    counts = store.lead_counts(batch_id)
    already_sent = counts.get(LeadState.SENT.value, 0)
    tracked_first = int(batch.get("tracked_first") or 0)
    tracked_id = batch.get("tracked_campaign_id")
    untracked_id = batch.get("untracked_campaign_id")

    split: list[tuple[int, list[str]]] = []
    if tracked_id and already_sent < tracked_first:
        head = chosen[: tracked_first - already_sent]
        tail = chosen[len(head):]
        if head:
            split.append((int(tracked_id), head))
        if tail and untracked_id:
            split.append((int(untracked_id), tail))
    else:
        target = untracked_id or tracked_id
        if not target:
            return {"batch_id": batch_id, "name": name, "released": 0,
                    "error": "batch has no campaign configured"}
        split.append((int(target), chosen))

    plan = [{"campaign_id": cid, "leads": len(emails)} for cid, emails in split]
    if dry_run:
        return {"batch_id": batch_id, "name": name, "dry_run": True,
                "would_release": len(chosen), "plan": plan,
                "companies": len({b.key for b in books if b.queued}),
                "sample": chosen[:5]}

    acc = _find_account(batch["account"])
    released = 0
    errors: list[str] = []
    async with SmartleadClient(acc.api_key, acc.name) as client:
        for campaign_id, emails in split:
            claimed = store.claim_leads(batch_id, emails, campaign_id)
            if not claimed:
                continue
            docs = store.leads_by_email(batch_id, claimed)
            payloads = [docs[e]["payload"] for e in claimed if e in docs]
            try:
                for i in range(0, len(payloads), LEADS_PER_REQUEST):
                    await client.add_campaign_leads_full(
                        str(campaign_id), payloads[i:i + LEADS_PER_REQUEST])
            except Exception as exc:  # noqa: BLE001
                # Put them back so the next run retries rather than losing them.
                store.release_claim(batch_id, claimed)
                errors.append(f"campaign {campaign_id}: {exc}")
                continue
            store.bump_sent(batch_id, [docs[e]["company"] for e in claimed if e in docs])
            released += len(claimed)

    out = {"batch_id": batch_id, "name": name, "released": released, "plan": plan}
    if errors:
        out["errors"] = errors
    return out


def _release(args) -> int:
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

    results = [asyncio.run(_release_batch(store, b, args.dry_run)) for b in batches]
    total = sum(r.get("released", 0) for r in results)

    if args.json:
        print(json.dumps({"released": total, "batches": results}))
        return 0
    for r in results:
        if r.get("dry_run"):
            print(f"[dry run] {r['name']}: would release {r['would_release']} "
                  f"lead(s) -> {r['plan']}")
        elif r.get("skipped"):
            print(f"{r['name']}: skipped ({r['skipped']})")
        else:
            print(f"{r['name']}: released {r['released']} lead(s)"
                  + (f" - ERRORS: {r['errors']}" if r.get("errors") else ""))
    print(f"total released: {total}")
    return 0


def _batches(as_json: bool) -> int:
    store = StaggerStore()
    rows = store.list_batches()
    if as_json:
        for row in rows:
            row["created_at"] = str(row.get("created_at"))
        print(json.dumps({"batches": rows}))
        return 0
    for row in rows:
        c, cc = row.get("counts", {}), row.get("company_counts", {})
        print(f"  {row['id']}  {row.get('name','')[:34]:34} "
              f"queued={c.get('QUEUED',0):<5} sent={c.get('SENT',0):<5} "
              f"skipped={c.get('SKIPPED',0):<4} | companies active={cc.get('ACTIVE',0)} "
              f"replied={cc.get('PAUSED_REPLY',0)} bounced={cc.get('PAUSED_BOUNCE',0)} "
              f"done={cc.get('EXHAUSTED',0)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Company-staggered lead release")
    ap.add_argument("--preview", metavar="CSV", help="Parse a CSV and report, storing nothing")
    ap.add_argument("--upload", metavar="CSV", help="Store a CSV as a new batch")
    ap.add_argument("--release", action="store_true", help="Release today's leads")
    ap.add_argument("--batches", action="store_true", help="List batches and progress")

    ap.add_argument("--name", default="", help="Batch name")
    ap.add_argument("--account", default="", help="Smartlead account")
    ap.add_argument("--tracked-campaign", type=int, help="Open-tracking campaign id")
    ap.add_argument("--untracked-campaign", type=int, help="No-open-tracking campaign id")
    ap.add_argument("--daily-cap", type=int, default=100, help="Leads to release per day")
    ap.add_argument("--tracked-first", type=int, default=0,
                    help="Send this many to the tracked campaign before switching")
    ap.add_argument("--company-column", help="CSV column that names the company")
    ap.add_argument("--created-by", default="")
    ap.add_argument("--batch", help="Batch id for --release")
    ap.add_argument("--all", action="store_true", help="Release every batch")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.preview:
        return _preview(args.preview, args.company_column, args.json)
    if args.upload:
        if not args.account:
            ap.error("--account is required with --upload")
        if not (args.tracked_campaign or args.untracked_campaign):
            ap.error("give --tracked-campaign and/or --untracked-campaign")
        return _upload(args)
    if args.release:
        if not (args.batch or args.all):
            ap.error("--release needs --batch <id> or --all")
        return _release(args)
    if args.batches:
        return _batches(args.json)
    ap.error("nothing to do: pass --preview, --upload, --release or --batches")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
