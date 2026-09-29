#!/usr/bin/env python3
"""Inbox setup jobs from the command line (Slack and the website call this too).

    python inbox_jobs.py --create --client Melior --kind owned --provider GOOGLE \
        --domain gomelior.com --per-domain 3 --names "Ann Lee;Bo Park" --signature
    python inbox_jobs.py --create --client Bettrdata --kind new --provider GOOGLE \
        --domain feedsworks.com --domain segmentline.com --run-on 2026-10-02
    python inbox_jobs.py --create --client Melior --kind prewarmed --provider GOOGLE \
        --domain apexgtmcraft.co --prewarmed-id <zapmail domain id>
    python inbox_jobs.py --approve <job_id> --by U09TQ9D7YNM     # after reading the cost
    python inbox_jobs.py --tick                                  # move every approved job forward
    python inbox_jobs.py --list | --show <job_id> | --cancel <job_id> --by <who>

--create only reads (prices, free slots) and records the job; nothing is bought
until --approve, and paid steps also need ZAPMAIL_ALLOW_SPEND=true.
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


def _summary(job: dict) -> dict:
    from smartlead.inbox_jobs import current_step, progress_line
    step = current_step(job) or {}
    return {"job_id": job["job_id"], "client": job["client"], "kind": job["kind"],
            "provider": job["provider"], "domains": job["domains"],
            "inboxes_per_domain": job["inboxes_per_domain"], "status": job["status"],
            "step": step.get("name"), "detail": step.get("detail", ""),
            "progress": progress_line(job), "cost": job.get("cost"),
            "run_on": job.get("run_on"), "created_by": job.get("created_by"),
            "approved_by": job.get("approved_by"), "inboxes": [i["email"] for i in job.get("inboxes") or []]}


def _print_job(s: dict) -> None:
    print(f"\n  Job {s['job_id']} — {s['client']} · {s['kind']} · "
          f"{'Google' if s['provider'] == 'GOOGLE' else 'Outlook'} · {', '.join(s['domains'])}")
    print(f"  Status: {s['status']}" + (f" (scheduled {s['run_on']})" if s.get("run_on") else ""))
    if s.get("cost"):
        for line in s["cost"]["lines"]:
            print(f"    ${line['usd']:>7.2f}  {line['what']}  ({line['when']})")
        print(f"    ${s['cost']['total_now_usd']:>7.2f}  TOTAL charged when it runs")
    print(f"  Steps: {s['progress']}")
    if s.get("detail"):
        print(f"  Now: {s['detail']}")


async def main() -> int:
    from smartlead.domain_purchase import spend_allowed
    from smartlead.inbox_job_steps import RealSteps
    from smartlead.inbox_jobs import JobStore, approve, cancel, new_job, tick

    ap = argparse.ArgumentParser(description="Inbox setup jobs")
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--client")
    ap.add_argument("--kind", choices=["new", "owned", "prewarmed"])
    ap.add_argument("--provider", choices=["GOOGLE", "MICROSOFT", "google", "microsoft"])
    ap.add_argument("--domain", action="append", default=[])
    ap.add_argument("--per-domain", type=int, default=3)
    ap.add_argument("--names", default="", help='"First Last;First Last" (else generated)')
    ap.add_argument("--signature", action="store_true")
    ap.add_argument("--run-on", help="YYYY-MM-DD (default: as soon as approved)")
    ap.add_argument("--prewarmed-id")
    ap.add_argument("--approve", metavar="JOB_ID")
    ap.add_argument("--cancel", metavar="JOB_ID")
    ap.add_argument("--by", default="")
    ap.add_argument("--tick", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--show", metavar="JOB_ID")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    store = JobStore()
    steps = RealSteps(store)
    out = lambda obj: print(json.dumps(obj, default=str)) if args.json else None  # noqa: E731
    try:
        if args.create:
            if not (args.client and args.kind and args.provider and args.domain):
                raise ValueError("--create needs --client, --kind, --provider and --domain")
            job = new_job(client=args.client, kind=args.kind, provider=args.provider,
                          domains=args.domain, inboxes_per_domain=args.per_domain,
                          names=[n for n in args.names.split(";") if n.strip()],
                          signature=args.signature, run_on=args.run_on,
                          prewarmed_domain_id=args.prewarmed_id, created_by=args.by)
            job["cost"] = await steps.cost(job)
            store.insert(job)
            s = _summary(job)
            out(s) if args.json else (_print_job(s), print(
                f"\n  Nothing bought yet. Approve with: inbox_jobs.py --approve {job['job_id']} --by <you>"))
            return 0
        if args.approve:
            job = approve(store, args.approve, args.by or "cli")
            job = (await tick_one(store, steps, job["job_id"], spend_allowed()))
            out(_summary(job)) if args.json else _print_job(_summary(job))
            return 0
        if args.cancel:
            job = cancel(store, args.cancel, args.by or "cli")
            out(_summary(job)) if args.json else _print_job(_summary(job))
            return 0
        if args.tick:
            res = await tick(store, steps, spend_ok=spend_allowed())
            out({"ticked": res}) if args.json else [print(f"  {r['job_id']} {r['client']}: {r['status']} — "
                                                           f"{r['step']} {r['detail']}") for r in res]
            return 0
        if args.show:
            job = store.get(args.show)
            if not job:
                raise ValueError(f"no job {args.show}")
            out(_summary(job)) if args.json else _print_job(_summary(job))
            return 0
        if args.list:
            jobs = [_summary(j) for j in store.list()]
            jobs.sort(key=lambda s: s["job_id"])
            if args.json:
                out({"jobs": jobs})
            else:
                for s in jobs:
                    print(f"  {s['job_id']}  {s['status']:18} {s['client']:14} {s['kind']:9} "
                          f"{', '.join(s['domains'])}  — {s['step'] or 'done'}")
            return 0
        ap.print_help()
        return 2
    except (ValueError, PermissionError, RuntimeError) as exc:
        print(json.dumps({"error": str(exc)}) if args.json else f"ERROR: {exc}")
        return 0 if args.json else 1


async def tick_one(store, steps, job_id: str, spend_ok: bool) -> dict:
    """Run one job right after approval (the scheduled tick covers the rest)."""
    from smartlead.inbox_jobs import advance
    if not store.claim(job_id):
        return store.get(job_id)
    try:
        job = await advance(store.get(job_id), steps, spend_ok=spend_ok)
        store.save(job)
        return job
    finally:
        store.release(job_id)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
