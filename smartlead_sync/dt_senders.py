#!/usr/bin/env python3
"""Put every inbox we use (domains in the deliverability sheet, smartlead/tracked.py)
on its client's deliverability test campaigns,
so any inbox can be picked for the Monday placement test (Manveen, 15 Oct:
"add every inbox into those tests").

Per client: the main test campaign (copy_sync.TARGETS) and one of its
"DT <client> #n" campaigns, which the weekly copy sync keeps on current copy.
DT campaigns hold at most 50 inboxes each (one scheduled test per campaign) and
fill first-to-last; a new DT campaign is cloned from #1 when all are full, and
existing full campaigns never change (Manveen, 15 Oct).
Only PAUSED or DRAFTED campaigns are touched: a test campaign sends to seed
addresses only through Smart Delivery, never to leads. Each add is read back.

    python dt_senders.py            # dry run: what would be attached
    python dt_senders.py --gaps     # also: which scheduled tests miss inboxes (read-only)
    python dt_senders.py --apply    # attach; runs daily 06:00 IST, and DMs Manveen only about the
                                    #   inboxes it added (and campaigns it created) that run
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
DT_CAP = 50          # inboxes per DT campaign: one scheduled test each (Manveen, 15 Oct)


def _dt_number(name: str) -> int:
    try:
        return int(str(name).rsplit("#", 1)[1].strip())
    except (IndexError, ValueError):
        return 0


def _fill_first(dts: list[dict], members: set[int], cap: int) -> dict:
    """Lay out one client's DT campaigns: earlier campaigns full (cap each)
    before later ones hold anything; an inbox stays put unless it must move.
    Returns {"adds": {campaign: [ids]}, "removes": {id: [ids]}, "create": [...]}."""
    dts = sorted(dts, key=lambda c: _dt_number(c["name"]))
    n_needed = max(len(dts), -(-len(members) // cap)) if members else len(dts)
    slots = [{"key": c["id"], "have": set(c["attached"]) & members} for c in dts]
    create = []
    base = dts[-1]["name"].rsplit("#", 1)[0].strip() if dts else ""
    for k in range(len(dts), n_needed):
        name = f"{base} #{_dt_number(dts[-1]['name']) + (k - len(dts) + 1)}"
        create.append({"name": name, "client": dts[0]["client"], "clone_of": dts[0]["id"]})
        slots.append({"key": name, "have": set()})
    targets, left = [], len(members)
    for _ in slots:
        targets.append(min(cap, left))
        left -= targets[-1]
    keep = [sorted(sl["have"])[:t] for sl, t in zip(slots, targets)]
    kept = {i for ks in keep for i in ks}
    pool = sorted(members - kept)
    adds: dict = {}
    removes: dict = {}
    where = {i: sl["key"] for sl in slots for i in sl["have"]}
    for sl, ks, t in zip(slots, keep, targets):
        room = t - len(ks)
        take, pool = pool[:room], pool[room:]
        if take:
            adds[sl["key"]] = take
        for i in take:
            if i in where:
                removes.setdefault(where[i], []).append(i)
    return {"adds": adds, "removes": {k: sorted(v) for k, v in removes.items()}, "create": create}


def plan(inboxes: list[dict], campaigns: list[dict], cap: int = DT_CAP, drop_others: bool = False) -> dict:
    """inboxes = [{id, email, client}]; campaigns = [{id, name, client, role: main|dt,
    status, attached: set of inbox ids}]. Every current-client inbox goes on its
    client's main test campaign, and on exactly one of its DT campaigns, filled
    first-to-last at most ``cap`` each (Manveen 15 Oct: "a maximum of 50 ...
    prioritize filling the 50 first"). Returns {"adds": {campaign id or new name:
    [inbox ids]}, "removes": {campaign id: [inbox ids]} (moves), "drops": {campaign id:
    [inbox ids]} (inboxes we do not use, only when ``drop_others``: the sheet was
    read), "create": [{name, client, clone_of}], "problems": [...]}. Pure."""
    from smartlead.inbox_intake import CURRENT_CLIENTS
    adds: dict = {}
    removes: dict = {}
    drops: dict = {}
    create: list = []
    problems: list[str] = []
    usable = []
    for c in campaigns:
        if str(c.get("status") or "").upper() in SAFE_STATUSES:
            usable.append({**c, "attached": set(c["attached"])})
        else:
            problems.append(f"test campaign '{c['name']}' is {c.get('status')}: no inboxes added to it")
    ids_by_client: dict = {}
    for b in inboxes:
        if b["client"] in CURRENT_CLIENTS:
            ids_by_client.setdefault(b["client"], set()).add(int(b["id"]))
    for client, ids in sorted(ids_by_client.items()):
        mine = [c for c in usable if c["client"] == client]
        if not mine and not any(c["client"] == client for c in campaigns):
            problems.append(f"{client}: no test campaign to add its inboxes to")
            continue
        for c in mine:
            if c["role"] == "main":
                missing = sorted(ids - c["attached"])
                if missing:
                    adds[int(c["id"])] = missing
        dts = [c for c in mine if c["role"] == "dt"]
        if dts and drop_others:
            for c in dts:
                gone = sorted(c["attached"] - ids)
                if gone:
                    drops[int(c["id"])] = gone
                    c["attached"] = c["attached"] & ids
        if dts:
            members = ids if drop_others else ids | {i for c in dts for i in c["attached"]}
            lay = _fill_first(dts, members, cap)
            adds.update(lay["adds"])
            removes.update(lay["removes"])
            create += lay["create"]
    return {"adds": adds, "removes": removes, "drops": drops, "create": create, "problems": problems}


async def _clone(sl, src: int, name: str) -> int:
    """A new DT campaign set up like ``src``: one step with its copy, the same
    placeholder lead, settings and schedule; paused like the others, never started."""
    seq = await sl._get(f"/campaigns/{src}/sequences")
    steps = seq if isinstance(seq, list) else seq.get("sequences", [])
    first = sorted(steps, key=lambda x: x.get("seq_number", 0))[0] if steps else {}
    var = (first.get("seq_variants") or first.get("sequence_variants") or [first])[0] if first else {}
    camp = await sl._get(f"/campaigns/{src}")
    new_id = int((await sl.create_campaign(name))["id"])
    await sl.save_campaign_sequences(str(new_id), var.get("subject", ""), var.get("email_body", ""))
    leads = await sl.get_campaign_leads(str(src))
    emails = [str((x.get("lead") or x).get("email")) for x in leads if (x.get("lead") or x).get("email")]
    if emails:   # the placeholder lead also sits in the other DT campaigns: allow that, or Smartlead skips it
        await sl._request_json("POST", f"/campaigns/{new_id}/leads", {
            "lead_list": [{"email": e} for e in emails],
            "settings": {"ignore_global_block_list": True, "ignore_unsubscribe_list": True,
                         "ignore_duplicate_leads_in_other_campaign": False}})
    await sl.update_campaign_settings(str(new_id), {k: camp[k] for k in (
        "track_settings", "stop_lead_settings", "send_as_plain_text", "follow_up_percentage") if k in camp})
    cron = camp.get("scheduler_cron_value") or {}
    if cron:
        await sl.update_campaign_schedule(str(new_id), {
            "timezone": cron.get("tz"), "days_of_the_week": cron.get("days"), "start_hour": cron.get("startHour"),
            "end_hour": cron.get("endHour"), "min_time_btw_emails": camp.get("min_time_btwn_emails"),
            "max_new_leads_per_day": camp.get("max_leads_per_day")})
    await sl.set_campaign_status(str(new_id), "PAUSED")      # like the other DT campaigns; never START
    return new_id


async def _on(sl, cid) -> set[int]:
    return {int(a.get("id") or a.get("email_account_id") or 0) for a in await sl.get_campaign_email_accounts(str(cid))}


async def apply(sl, p: dict, batch: int = BATCH) -> dict:
    """Create new DT campaigns, add inboxes (read back), then remove each moved
    inbox from its old campaign only once the add is confirmed (read back)."""
    out = {"created": [], "added": 0, "removed": 0, "dropped": 0, "missing": []}
    for cid, gone in (p.get("drops") or {}).items():
        for k in range(0, len(gone), batch):
            await sl.remove_campaign_email_accounts(str(cid), gone[k:k + batch])
        still = await _on(sl, cid)
        out["dropped"] += sum(i not in still for i in gone)
        out["missing"] += [f"{i} still on campaign {cid} after the drop" for i in gone if i in still]
    ids = {}
    for c in p.get("create") or []:
        ids[c["name"]] = await _clone(sl, int(c["clone_of"]), c["name"])
        out["created"].append(c["name"])
    landed: set[int] = set()
    for key, add in (p.get("adds") or {}).items():
        cid = ids.get(key, key)
        for k in range(0, len(add), batch):
            await sl.add_campaign_email_accounts(str(cid), add[k:k + batch])
        now = await _on(sl, cid)
        for i in add:
            if i in now:
                out["added"] += 1
                landed.add(i)
            else:
                out["missing"].append(f"{i} not on campaign {cid} after the add")
    moved = {i for rem in (p.get("removes") or {}).values() for i in rem}
    out["new_ids"] = sorted(landed - moved)
    for cid, rem in (p.get("removes") or {}).items():
        go = [i for i in rem if i in landed]
        for k in range(0, len(go), batch):
            await sl.remove_campaign_email_accounts(str(cid), go[k:k + batch])
        still = await _on(sl, cid) if go else set()
        out["removed"] += sum(i not in still for i in go)
        out["missing"] += [f"{i} still on campaign {cid} after the move" for i in go if i in still]
    return out


def test_gaps(dts: list[dict], tests: list[dict]) -> list[dict]:
    """dts = [{id, name, emails}]; tests = scheduled tests [{id, name, campaign_id,
    senders}]. Per DT campaign, the inboxes no scheduled test on it includes. Pure."""
    out = []
    for c in sorted(dts, key=lambda c: (c["name"].rsplit("#", 1)[0], _dt_number(c["name"]))):
        mine = [t for t in tests if int(t["campaign_id"]) == int(c["id"])]
        covered = set().union(*(t["senders"] for t in mine)) if mine else set()
        missing = sorted(set(c["emails"]) - covered)
        if missing:
            out.append({"campaign": c["name"], "test": mine[0]["name"] if mine else None, "add": missing,
                        "count": len(c["emails"])})
    return out


def _n(k: int) -> str:
    return f"{k} inbox{'es' if k != 1 else ''}"


def gap_text(gaps: list[dict]) -> str:
    lines = ["Placement tests to update in Smart Delivery (the bot cannot edit scheduled tests):"]
    for g in gaps:
        if g["test"] is None:
            lines.append(f"{g['campaign']}: no scheduled test yet. Create a weekly test with all {_n(g['count'])} on it.")
        else:
            shown = ", ".join(g["add"][:25]) + (f", and {len(g['add']) - 25} more" if len(g["add"]) > 25 else "")
            lines.append(f"{g['campaign']}: add {_n(len(g['add']))} to the scheduled test \"{g['test']}\": {shown}")
    return "\n".join(lines)


def new_inbox_text(gaps: list[dict], added: set[str], created: list[str]) -> str:
    """The DM after a daily run: only inboxes added today that no scheduled test
    includes, and campaigns created today. Empty = nothing to tell. Pure."""
    lines = []
    for g in gaps:
        if g["campaign"] in created:
            lines.append(f"{g['campaign']} (new campaign): create a weekly test with all {_n(g['count'])} on it.")
            continue
        fresh = [e for e in g["add"] if e in added]
        if not fresh:
            continue
        if g["test"] is None:
            lines.append(f"{g['campaign']}: no scheduled test yet. Create a weekly test with all {_n(g['count'])} on it.")
        else:
            word = "new inbox" if len(fresh) == 1 else "new inboxes"
            lines.append(f"{g['campaign']}: add {len(fresh)} {word} to the scheduled test \"{g['test']}\": "
                         + ", ".join(fresh))
    if not lines:
        return ""
    return "New inboxes for the placement tests (the bot cannot edit scheduled tests in Smart Delivery):\n" + "\n".join(lines)


async def _scheduled_tests(key: str) -> list[dict]:
    """Active scheduled (recurring) tests with their senders. Read-only, no credit."""
    import httpx
    from smartlead.config import SMARTDELIVERY_BASE_URL
    ua = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(timeout=60, headers=ua) as h:
        rows = (await h.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report", params={"api_key": key},
                             json={"limit": 100})).json()
        out = []
        for t in rows if isinstance(rows, list) else []:
            if str(t.get("status")).upper() != "ACTIVE" or not t.get("every_days"):
                continue
            d = (await h.get(f"{SMARTDELIVERY_BASE_URL}/spam-test/{t['spam_test_id']}", params={"api_key": key})).json()
            d = d.get("data", d) if isinstance(d, dict) else {}
            s = (await h.get(f"{SMARTDELIVERY_BASE_URL}/spam-test/report/{t['spam_test_id']}/sender-accounts",
                             params={"api_key": key})).json()
            s = s if isinstance(s, list) else (s.get("data") or [])
            out.append({"id": t["spam_test_id"], "name": t.get("test_name"), "campaign_id": d.get("campaign_id") or 0,
                        "senders": {str(x.get("email") or x.get("from_email") or "").lower() for x in s}})
        return out


def _dm(text: str) -> bool:
    """Direct message to Manveen (Domain Suggester app, like intake's questions)."""
    import requests
    from smartlead.notify import redact
    token = (os.getenv("DOMAINS_SLACK_BOT_TOKEN") or "").strip()
    user = (os.getenv("INTAKE_ASK_USER") or "U09TQ9D7YNM").strip()
    if not token:
        return False
    try:
        r = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {token}"},
                          json={"channel": user, "text": redact(text)[:3900]}, timeout=15).json()
        return bool(r.get("ok"))
    except Exception:  # noqa: BLE001 - tried again next run (the text is not saved as sent)
        return False


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
    ap.add_argument("--gaps", action="store_true", help="also list scheduled tests that miss inboxes (read-only)")
    args = ap.parse_args()
    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    from smartlead.client_filter import is_excluded_inbox
    from smartlead.inbox_intake import CURRENT_ACCOUNTS
    from smartlead.notify import redact
    from smartlead.report_sheet import client_label
    from smartlead.tracked import only_tracked, tracked_domains
    out: dict = {}
    tracked = tracked_domains()
    if tracked is None:
        print("  [dt-senders] deliverability sheet unreadable: keeping every inbox, dropping none", file=sys.stderr)

    async def one(acc):
        async with SmartleadClient(acc.api_key, acc.name) as c:
            boxes = [{"id": a["id"], "email": a.get("from_email"), "client": client_label(acc.name, a.get("client_id"))}
                     for a in await c.list_email_accounts()
                     if not is_excluded_inbox({"email": a.get("from_email") or "", "tags": a.get("tags", [])})]
            camps = await _campaigns(c, acc.name)
            p = plan(only_tracked(boxes, tracked), camps, drop_others=tracked is not None)
            names = {x["id"]: x["name"] for x in camps}
            res = {"would_add": {names.get(k, k): len(v) for k, v in p["adds"].items()},
                   "would_remove": {names.get(k, k): len(v) for k, v in p["removes"].items()},
                   "would_drop": {names.get(k, k): len(v) for k, v in p["drops"].items()},
                   "would_create": [x["name"] for x in p["create"]], "problems": p["problems"]}
            if args.apply and (p["adds"] or p["removes"] or p["drops"] or p["create"]):
                res.update(await apply(c, p))
            if args.apply or args.gaps:
                emails = {int(a["id"]): str(a.get("from_email") or "").lower() for a in await c.list_email_accounts()}
                camps_now = await _campaigns(c, acc.name)
                dts = [{"id": x["id"], "name": x["name"], "emails": {emails[i] for i in x["attached"] if i in emails}}
                       for x in camps_now if x["role"] == "dt"]
                dt_ids = {i for x in camps_now if x["role"] == "dt" for i in x["attached"]}
                res["gaps"] = test_gaps(dts, await _scheduled_tests(acc.api_key))
                res["new_on_dt"] = sorted(emails[i] for i in res.get("new_ids") or [] if i in dt_ids and i in emails)
            return res

    for acc in discover_accounts():
        if acc.name.upper() not in CURRENT_ACCOUNTS:
            continue
        try:
            out[acc.name] = asyncio.run(one(acc))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            out[acc.name] = {"error": redact(str(exc))[:200]}
    if args.apply:
        # DM only about inboxes added and campaigns created by THIS run (Manveen, 15 Oct: tell me
        # about new inboxes; a standing gap is not repeated). `--gaps` prints the full list on demand.
        gaps = [g for r in out.values() if isinstance(r, dict) for g in r.get("gaps") or []]
        added = {e for r in out.values() if isinstance(r, dict) for e in r.get("new_on_dt") or []}
        created = [n for r in out.values() if isinstance(r, dict) for n in r.get("created") or []]
        text = new_inbox_text(gaps, added, created)
        if text:
            out["dm_sent"] = _dm(text)
    print(json.dumps(out, indent=1, default=sorted))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
