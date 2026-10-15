#!/usr/bin/env python3
"""Put every current-client inbox on its client's deliverability test campaigns,
so any inbox can be picked for the Monday placement test (Manveen, 15 Oct:
"add every inbox into those tests").

Per client: the main test campaign (copy_sync.TARGETS) and the emptier of its
"DT <client> #n" campaigns, which the weekly copy sync keeps on current copy.
Only PAUSED or DRAFTED campaigns are touched: a test campaign sends to seed
addresses only through Smart Delivery, never to leads. Each add is read back.

    python dt_senders.py            # dry run: what would be attached
    python dt_senders.py --apply    # attach; runs daily 06:00 IST
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
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

SAFE_STATUSES = {"PAUSED", "DRAFTED"}
BATCH = 50


def plan(inboxes: list[dict], campaigns: list[dict]) -> dict:
    """inboxes = [{id, email, client}]; campaigns = [{id, name, client, role: main|dt,
    status, attached: set of inbox ids}]. Returns {"adds": {campaign id: [inbox ids]},
    "problems": [...]}. Pure."""
    from smartlead.inbox_intake import CURRENT_CLIENTS
    adds: dict[int, list[int]] = {}
    problems: list[str] = []
    usable = []
    for c in campaigns:
        if str(c.get("status") or "").upper() in SAFE_STATUSES:
            usable.append({**c, "attached": set(c["attached"])})
        else:
            problems.append(f"test campaign '{c['name']}' is {c.get('status')}: no inboxes added to it")
    clients_without = set()
    for b in sorted(inboxes, key=lambda b: int(b["id"])):
        if b["client"] not in CURRENT_CLIENTS:
            continue
        mine = [c for c in usable if c["client"] == b["client"]]
        if not mine and not any(c["client"] == b["client"] for c in campaigns):
            clients_without.add(b["client"])
            continue
        i = int(b["id"])
        for c in mine:
            if c["role"] == "main" and i not in c["attached"]:
                c["attached"].add(i)
                adds.setdefault(int(c["id"]), []).append(i)
        dts = [c for c in mine if c["role"] == "dt"]
        if dts and not any(i in c["attached"] for c in dts):
            c = min(dts, key=lambda c: (len(c["attached"]), int(c["id"])))
            c["attached"].add(i)
            adds.setdefault(int(c["id"]), []).append(i)
    problems += [f"{c}: no test campaign to add its inboxes to" for c in sorted(clients_without)]
    return {"adds": adds, "problems": problems}


async def apply(sl, adds: dict[int, list[int]], batch: int = BATCH) -> dict:
    added, missing = 0, []
    for cid, ids in adds.items():
        for k in range(0, len(ids), batch):
            await sl.add_campaign_email_accounts(str(cid), ids[k:k + batch])
        now = {int(a.get("id") or a.get("email_account_id") or 0) for a in await sl.get_campaign_email_accounts(str(cid))}
        for i in ids:
            if i in now:
                added += 1
            else:
                missing.append(f"{i} not on campaign {cid} after the add")
    return {"added": added, "missing": missing}


async def _campaigns(c, account: str) -> list[dict]:
    from copy_sync import DT_SOURCE_CLIENT, TARGETS
    from smartlead.report_sheet import client_label
    main = {int(cid): client_label(account, src) for cid, src in TARGETS.get(account, [])}
    out = []
    for x in await c.list_campaigns():
        cid, name = int(x["id"]), str(x.get("name", ""))
        prefix = next((p for p in DT_SOURCE_CLIENT if name.startswith(p + " #")), None)
        if cid in main:
            client, role = main[cid], "main"
        elif prefix:
            client, role = client_label(account, DT_SOURCE_CLIENT[prefix]), "dt"
        else:
            continue
        att = await c.get_campaign_email_accounts(str(cid))
        out.append({"id": cid, "name": name, "client": client, "role": role, "status": x.get("status"),
                    "attached": {int(a.get("id") or a.get("email_account_id") or 0) for a in att}})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Every inbox on its client's deliverability test campaigns")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    from smartlead.client_filter import is_excluded_inbox
    from smartlead.inbox_intake import CURRENT_ACCOUNTS
    from smartlead.notify import redact
    from smartlead.report_sheet import client_label
    out: dict = {}

    async def one(acc):
        async with SmartleadClient(acc.api_key, acc.name) as c:
            boxes = [{"id": a["id"], "email": a.get("from_email"), "client": client_label(acc.name, a.get("client_id"))}
                     for a in await c.list_email_accounts()
                     if not is_excluded_inbox({"email": a.get("from_email") or "", "tags": a.get("tags", [])})]
            camps = await _campaigns(c, acc.name)
            p = plan(boxes, camps)
            names = {x["id"]: x["name"] for x in camps}
            res = {"would_add": {names[k]: len(v) for k, v in p["adds"].items()}, "problems": p["problems"]}
            if args.apply and p["adds"]:
                res.update(await apply(c, p["adds"]))
            return res

    for acc in discover_accounts():
        if acc.name.upper() not in CURRENT_ACCOUNTS:
            continue
        try:
            out[acc.name] = asyncio.run(one(acc))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            out[acc.name] = {"error": redact(str(exc))[:200]}
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
