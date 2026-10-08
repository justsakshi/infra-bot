#!/usr/bin/env python3
"""Create the deliverability-test campaigns: every connected sending inbox in
an account, split into batches of 15 (one SmartDelivery test per batch).

    python3 dt_campaigns.py --account BETTRDATA --prefix "DT BettrData"             # plan only
    python3 dt_campaigns.py --account BETTRDATA --prefix "DT BettrData" --apply     # create + attach

Idempotent: a batch whose campaign already exists (same name) is topped up
with any inbox missing from it, never duplicated. Campaigns are created as
DRAFTS with no leads, so nothing sends until a SmartDelivery test is started
on them. Their copy is kept current by copy_sync.py (names start with "DT ").

--client-id limits the set to one Smartlead client inside the account
(Precise Leads' own inboxes have none: use --client-id none).
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import httpx

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except Exception:
    pass

from smartlead.accounts import discover_accounts

SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}
BATCH = 50   # SmartDelivery takes up to 50 sender mailboxes in one test
PLACEHOLDER = {
    "subject": "Quick question about {{first_name}}",
    "email_body": "<p>Hi {{first_name}},</p><p>I wanted to ask how your team handles outbound "
                  "email at the moment, and whether a short chat would be useful.</p><p>Thanks,</p>",
}


def call(method, key, path, **kw):
    params = {"api_key": key, **kw.pop("params", {})}
    for i in range(5):
        r = httpx.request(method, SL + path, params=params, headers=UA, timeout=90, **kw)
        if r.status_code != 429:
            break
        time.sleep(3 * (i + 1))
    r.raise_for_status()
    return r.json()


def all_accounts(key):
    out, off = [], 0
    while True:
        page = call("GET", key, "/email-accounts/", params={"offset": off, "limit": 100})
        out += page
        if len(page) < 100:
            return out
        off += 100


def batches(accts):
    """Connected inboxes in as few batches of at most BATCH as possible, split as
    evenly as possible, never splitting a domain across two tests."""
    ok = [a for a in accts if a.get("is_smtp_success") and a.get("is_imap_success")]
    ok.sort(key=lambda a: (a["from_email"].split("@")[1].lower(), a["from_email"].lower()))
    if not ok:
        return []
    by_domain: dict[str, list] = {}
    for a in ok:
        by_domain.setdefault(a["from_email"].split("@")[1].lower(), []).append(a)
    n = -(-len(ok) // BATCH)
    target = -(-len(ok) // n)
    out, cur = [], []
    for dom in sorted(by_domain):
        group = by_domain[dom]
        if cur and (len(cur) + len(group) > BATCH or (len(cur) >= target and len(out) < n - 1)):
            out.append(cur)
            cur = []
        cur += group
    if cur:
        out.append(cur)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", required=True)
    ap.add_argument("--prefix", required=True, help='campaign name prefix, must start with "DT "')
    ap.add_argument("--client-id", default="all", help='"all", "none" or a Smartlead client id')
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.prefix.startswith("DT "):
        print('prefix must start with "DT " so copy_sync can find these campaigns')
        return 1
    acc = next((a for a in discover_accounts() if a.name.upper() == args.account.upper()), None)
    if not acc:
        print(f"no API key for {args.account}")
        return 1
    key = acc.api_key
    accts = all_accounts(key)
    if args.client_id != "all":
        want = None if args.client_id == "none" else int(args.client_id)
        accts = [a for a in accts if a.get("client_id") == want]
    groups = batches(accts)
    skipped = len(accts) - sum(len(g) for g in groups)
    print(f"{acc.name}: {len(accts)} inboxes, {skipped} not connected (left out), "
          f"{len(groups)} batch(es) of up to {BATCH}")
    existing = {c["name"]: c["id"] for c in call("GET", key, "/campaigns")}
    for n, group in enumerate(groups, 1):
        name = f"{args.prefix} #{n}"
        doms = sorted({a["from_email"].split("@")[1] for a in group})
        print(f"  {name}: {len(group)} inboxes on {len(doms)} domain(s): {', '.join(doms)}")
        if not args.apply:
            continue
        cid = existing.get(name)
        if cid is None:
            cid = call("POST", key, "/campaigns/create", json={"name": name})["id"]
            call("POST", key, f"/campaigns/{cid}/sequences", json={"sequences": [{
                "seq_number": 1, "seq_delay_details": {"delay_in_days": 0},
                "seq_variants": [dict(PLACEHOLDER, variant_label="A")]}]})
            print(f"      created campaign {cid}")
        have = {a["id"] for a in call("GET", key, f"/campaigns/{cid}/email-accounts") or []}
        missing = [a["id"] for a in group if a["id"] not in have]
        if missing:
            call("POST", key, f"/campaigns/{cid}/email-accounts", json={"email_account_ids": missing})
        time.sleep(3)
        got = call("GET", key, f"/campaigns/{cid}/email-accounts")
        print(f"      campaign {cid}: {len(got)} inbox(es) attached (wanted {len(group)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
