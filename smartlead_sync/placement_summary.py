#!/usr/bin/env python3
"""Fleet placement summary, read-only and credit-free.

`/spam-test/report/mailboxes-summary` returns each mailbox's placement rolled
up across every test it has ever appeared in. No test id, no credit, no send —
so this can run daily and surface drift that a weekly test would not catch for
another six days.

Its first run (2026-09-15, BETTRDATA) found all six Gmail mailboxes on the
`.co` persona domains between 47% and 82% inbox while every Outlook mailbox sat
at 100%, with DKIM and SPF passing everywhere. Authentication being clean while
one provider rejects is the signature of a content or reputation problem rather
than a setup problem, which is a different fix and a different owner.

Usage:
    python3 placement_summary.py [--client NAME] [--threshold 80]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import RETEST_INBOX_THRESHOLD
from smartlead.smart_delivery import SmartDeliveryClient, SmartDeliveryError


def group_by_domain(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        email = row.get("email", "")
        if "@" in email:
            out[email.split("@", 1)[1].lower()].append(row)
    return dict(out)


def esp_split(rows: list[dict]) -> dict[str, list[dict]]:
    """Group by sending ESP. A provider-specific failure is the interesting
    case — it separates 'this domain is burnt' from 'Gmail dislikes this
    content', which need opposite responses."""
    out: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        out[row.get("esp") or "unknown"].append(row)
    return dict(out)


async def run(client: str | None, threshold: float) -> int:
    accounts = discover_accounts()
    if client:
        accounts = [a for a in accounts if a.name.upper() == client.upper()]
        if not accounts:
            print(f"[Summary] no such client: {client}")
            return 1

    failing_total = 0
    for acc in accounts:
        try:
            async with SmartDeliveryClient(acc.api_key) as sd:
                rows = await sd.get_mailbox_summary()
        except SmartDeliveryError as exc:
            print(f"[Summary] {acc.name}: {exc}")
            continue

        rows = [r for r in rows if not is_excluded_inbox({"email": r["email"]})]
        if not rows:
            print(f"[Summary] {acc.name}: no placement history yet")
            continue

        print(f"\n[Summary] {acc.name}: {len(rows)} mailbox(es) with placement history")
        failing = [r for r in rows if r["inbox_pct"] < threshold]
        for r in sorted(rows, key=lambda x: x["inbox_pct"]):
            mark = "FAIL" if r["inbox_pct"] < threshold else "ok  "
            auth = ""
            # Authentication failing alongside placement points at DNS, which
            # is a different fix from a content problem — worth calling out.
            if r["dkim_pass_pct"] < 100 or r["spf_pass_pct"] < 100:
                auth = f"  [dkim {r['dkim_pass_pct']:.0f}% spf {r['spf_pass_pct']:.0f}%]"
            print(f"  {mark} {r['email']:38} {r['esp']:8} "
                  f"inbox {r['inbox_pct']:5.1f}%  spam {r['spam_pct']:5.1f}%  "
                  f"({r['tests']} test(s), {r['total']} seeds){auth}")

        for esp, group in sorted(esp_split(rows).items()):
            bad = [r for r in group if r["inbox_pct"] < threshold]
            if bad and len(bad) == len(group):
                print(f"  ! every {esp} mailbox is below {threshold:.0f}% "
                      f"while other providers are not - reads as a content or "
                      f"reputation problem, not a DNS one")

        for domain, group in sorted(group_by_domain(failing).items()):
            print(f"  ! {domain}: {len(group)} mailbox(es) below {threshold:.0f}%")
        failing_total += len(failing)

    return 2 if failing_total else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", help="limit to one client")
    ap.add_argument("--threshold", type=float, default=RETEST_INBOX_THRESHOLD,
                    help="inbox %% below which a mailbox is flagged")
    opts = ap.parse_args()
    sys.exit(asyncio.run(run(opts.client, opts.threshold)))


if __name__ == "__main__":
    main()
