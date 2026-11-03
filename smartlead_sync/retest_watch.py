#!/usr/bin/env python3
"""Read-only check on the auto placement-test job (retest_executor.py).

Written for its first live week (from 3 Nov 2026): reads the tests the job
recorded in Mongo and says whether it stayed inside its rules. Spends nothing,
writes nothing.

    python retest_watch.py                 # last 7 days
    python retest_watch.py --since 2026-11-04 --json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except ImportError:  # pragma: no cover
    pass

from smartlead.config import RETEST_PER_CLIENT_DAILY_CAP, RETEST_DISABLE_WARMUP
from smartlead.retest_targets import MIN_DAYS_BETWEEN_TESTS


def _d(s: str) -> date:
    return datetime.fromisoformat(str(s)[:10]).date()


def check(tests: list[dict], today: date, cap: int) -> dict:
    """tests: placement_tests docs with source 'retest'. Returns
    {problems, warnings, results, per_day} - problems mean the job broke a rule."""
    problems: list[str] = []
    warnings: list[str] = []
    per_day = Counter((t.get("created", ""), t.get("client", "")) for t in tests)
    for (day, client), n in sorted(per_day.items()):
        if n > cap:
            problems.append(f"{client} got {n} tests on {day}; the cap is {cap} a day.")

    last: dict[str, date] = {}
    for t in sorted(tests, key=lambda t: t.get("created", "")):
        email = str(t.get("target") or "").lower()
        if not email or not t.get("created"):
            continue
        day = _d(t["created"])
        if email in last and (day - last[email]).days < MIN_DAYS_BETWEEN_TESTS:
            problems.append(f"{email} was tested on {last[email]} and again on {day}; "
                            f"the floor is {MIN_DAYS_BETWEEN_TESTS} days.")
        last[email] = day

    results: list[str] = []
    for t in tests:
        status, created = t.get("status"), t.get("created", "")
        tid, target = t.get("test_id"), t.get("target") or ", ".join(t.get("emails") or [])
        off = t.get("warmup_off_ids") or []
        if off and status != "ACTIVE":
            problems.append(f"test {tid} is {status} but {len(off)} inbox(es) still have warm-up off.")
        if status == "ACTIVE" and created and (today - _d(created)).days >= 1:
            warnings.append(f"test {tid} ({target}) has been running since {created}; "
                            "the job gives up after 3 days.")
        if status == "ABANDONED":
            warnings.append(f"test {tid} ({target}) never finished and was given up.")
        n = len(t.get("emails") or [])
        if n > 1:
            warnings.append(f"test {tid} went through a live campaign and tested {n} inboxes "
                            f"to check {target}.")
        if status == "DONE":
            results.append(f"{created} {target}: {t.get('result')} "
                           f"({float(t.get('inbox_pct') or 0):.0f}% inbox) - {t.get('reason', '')}")
    return {"problems": problems, "warnings": warnings, "results": sorted(results),
            "per_day": {f"{d} {c}": n for (d, c), n in sorted(per_day.items())}}


def render(res: dict, since: str, total: int, weekly: int | None = None) -> str:
    lines = [f"Auto placement tests since {since}: {total} created."]
    if not total:
        lines.append("None created. If RETEST_ENABLED=true was set before the last 11:00 IST run, "
                     "check Render deployed it, then the [retest] lines in its log.")
    lines.append("RULES BROKEN:" if res["problems"] else "Rules held (daily cap, 7-day floor, warm-up restored).")
    lines += [f"  - {p}" for p in res["problems"]]
    if res["warnings"]:
        lines.append("Worth a look:")
        lines += [f"  - {w}" for w in res["warnings"]]
    if res["per_day"]:
        lines.append("Per day: " + "; ".join(f"{k}: {n}" for k, n in res["per_day"].items()))
    if res["results"]:
        lines.append("Results:")
        lines += [f"  - {r}" for r in res["results"]]
    if weekly is not None:
        # Auto tests and the Mon/Tue scheduled tests spend the same SmartDelivery
        # credits, which the API can't show; a week with no scheduled results is
        # the first sign the auto tests drained them.
        lines.append(f"Weekly scheduled-test results saved since {since}: {weekly} inboxes"
                     f"{' - check the Mon/Tue tests ran (shared credits)' if not weekly else ''}.")
    if RETEST_DISABLE_WARMUP:
        lines.append("Note: RETEST_DISABLE_WARMUP is on here, so tests switch warm-up off while they run.")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Check the auto placement-test job (read-only)")
    ap.add_argument("--since", default=(date.today() - timedelta(days=7)).isoformat())
    ap.add_argument("--cap", type=int, default=RETEST_PER_CLIENT_DAILY_CAP,
                    help="the per-client daily cap set on Render (default: this machine's env)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    from smartlead.placement_store import PlacementStore
    store = PlacementStore()
    if not store.available:
        print("Mongo unavailable - can't check the auto placement tests.")
        return 1
    tests = list(store._tests.find({"source": "retest", "created": {"$gte": args.since}}, {"_id": 0}))
    res = check(tests, date.today(), args.cap)
    weekly = len(store._results.distinct("email", {"source": "weekly_report", "date": {"$gte": args.since}}))
    print(json.dumps({**res, "since": args.since, "created": len(tests), "weekly_results": weekly},
                     default=str)
          if args.json else render(res, args.since, len(tests), weekly))
    return 1 if res["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
