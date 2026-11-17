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

The DM is kept in Mongo (dt_senders.pending_dm) until Slack accepts it, so a failed DM goes out with the
next run. Inboxes taken off the test campaigns, current-client domains outside the deliverability sheet
(once each) and errors go to the infra channel (audit 7 Nov 2026: they were only printed). Exits 1 when
a DM or post was refused.

Daily digest (16 Nov 2026, smartlead/digest.py): with the digest on, the infra-channel post is replaced by
"done" lines (inboxes put on, moved, taken off, campaigns created, per campaign), an ask for domains outside the
deliverability sheet (each domain once, as before) and "broken" asks for errors or a DM that did not go out.
The DM itself is unchanged. If the digest can't be written, the old post goes out.

Pulled inboxes stay in a weekly test (Manveen, 16 Nov 2026): an inbox the v2 job took out after a failed v2 test
(tag "DO NOT USE - failed v2 (auto)") stays on, or is put on, one of its client's DT campaigns even when its domain is
not in the sheet, until the tag is off (back in use) or the inbox is gone or disconnected (cancelled). Only room the
other inboxes leave: never over 50, never a new campaign; with no room, a digest ask (plan(retest=...)).

Coverage rule (Manveen, 16 Nov 2026: "make sure we have all inboxes in test except scaled mail canceld orders";
docs/V2_WEEKLY.md): every connected inbox we use in exactly one ACTIVE weekly v2 test. The active tests come from the
v2 API; their senders stay on their test's DT campaign (plan(pinned=...)); inboxes in no test or in two, and two
tests with one name, are digest asks (coverage_asks). ScaledMail cancelled orders (asset tracker) are left out.

Coverage reminder (t16, 17 Nov 2026; Manveen: "make sure you remid me to update weekly scheduled test for all missed
inboxes ... mae sure we save thta on db as well"): the ask for inboxes in no test names, per DT campaign they sit on,
the active test with room (under 50 senders) or a new test on Saturday after 05:30 IST, with a ready Claude-in-Chrome
line (never "Run now"); on Fridays it is posted at once. Every --apply run saves the day in Mongo test_coverage
(coverage_record). With the tests read, inboxes are only put on DT campaigns that have an active weekly test
(plan(tested=...)); the bot never creates or stops a v2 test.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date

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
PENDING_DM = "pending_dm"    # Mongo dt_senders: the DM's inboxes and campaigns until Slack accepts it
UNTRACKED = "untracked"      # Mongo dt_senders: domains outside the sheet already reported
SOURCE = "dt-senders"        # its name in the daily digest
PULL_TAG = "do not use - failed v2 (auto)"   # v2_weekly.TAG, lower case: the v2 job pulled the inbox


def _dt_number(name: str) -> int:
    try:
        return int(str(name).rsplit("#", 1)[1].strip())
    except (IndexError, ValueError):
        return 0


def _fill_first(dts: list[dict], members: set[int], cap: int, pinned: dict | None = None, create_ok: bool = True) -> dict:
    """Lay out one client's DT campaigns: earlier campaigns full (cap each)
    before later ones hold anything; an inbox stays put unless it must move.
    ``pinned`` {inbox id: campaign id}: a sender of an active weekly v2 test never moves off its test's campaign
    (the coverage rule, 17 Nov 2026); the others fill first around them.
    ``create_ok`` False: no new campaign; inboxes that do not fit come back in "left".
    Returns {"adds": {campaign: [ids]}, "removes": {id: [ids]}, "create": [...], "left": [ids]}."""
    pinned = pinned or {}
    dts = sorted(dts, key=lambda c: _dt_number(c["name"]))
    n_needed = max(len(dts), -(-len(members) // cap)) if members and create_ok else len(dts)
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
    keep = []
    for sl, t in zip(slots, targets):
        here = sorted(i for i in sl["have"] if pinned.get(i) == sl["key"])
        keep.append(here[:cap] + sorted(sl["have"] - set(here))[:max(0, t - len(here))])
    kept = {i for ks in keep for i in ks}
    pool = sorted(members - kept)
    adds: dict = {}
    removes: dict = {}
    where = {i: sl["key"] for sl in slots for i in sl["have"]}
    for sl, ks, t in zip(slots, keep, targets):
        room = max(0, t - len(ks))
        take, pool = pool[:room], pool[room:]
        if take:
            adds[sl["key"]] = take
        for i in take:
            if i in where:
                removes.setdefault(where[i], []).append(i)
    return {"adds": adds, "removes": {k: sorted(v) for k, v in removes.items()}, "create": create, "left": pool}


def drop_cancelled(no_room: dict, boxes: list[dict], cancelled: set[str] | None) -> dict:
    """``no_room`` {client: [ids]} without inboxes of ScaledMail cancelled orders (they are being deleted, so no ask
    for them; they stay where they are). ``cancelled`` None = unknown: nothing left out. Pure."""
    if not cancelled:
        return dict(no_room)
    dom = {int(b["id"]): str(b.get("email") or "").lower().split("@")[-1] for b in boxes}
    out = {c: [i for i in ids if dom.get(int(i)) not in cancelled] for c, ids in no_room.items()}
    return {c: ids for c, ids in out.items() if ids}


def is_pulled(account: dict) -> bool:
    """The v2 job took this inbox out after a failed v2 test (its tag, any case) and Smartlead can still log in to
    it (a failed login = cancelled or dead: it leaves the retest). From a Smartlead email-account row."""
    tags = {str((t.get("tag_name") or t.get("name") or "") if isinstance(t, dict) else t).strip().lower()
            for t in account.get("tags") or []}
    dead = account.get("is_smtp_success") is False or account.get("is_imap_success") is False
    return PULL_TAG in tags and not dead


def plan(inboxes: list[dict], campaigns: list[dict], cap: int = DT_CAP, drop_others: bool = False,
         retest: list[dict] | None = None, pinned: dict | None = None, tested: set[int] | None = None) -> dict:
    """inboxes = [{id, email, client}]; campaigns = [{id, name, client, role: main|dt,
    status, attached: set of inbox ids}]. Every current-client inbox goes on its
    client's main test campaign, and on exactly one of its DT campaigns, filled
    first-to-last at most ``cap`` each (Manveen 15 Oct: "a maximum of 50 ...
    prioritize filling the 50 first"). Returns {"adds": {campaign id or new name:
    [inbox ids]}, "removes": {campaign id: [inbox ids]} (moves), "drops": {campaign id:
    [inbox ids]} (inboxes we do not use, only when ``drop_others``: the sheet was
    read), "create": [{name, client, clone_of}], "problems": [...]}. Pure.

    ``retest`` = [{id, email, client}]: inboxes the v2 job pulled (is_pulled). They stay on, or go on, one of their
    client's DT campaigns even when their domain is not in the sheet, so the weekly test keeps checking them until
    they are back in use (Manveen 16 Nov 2026). They only take room the other inboxes leave in the DT campaigns the
    client needs anyway: never a new campaign, never more than ``cap``. Also returns "retest": {campaign: [ids]}
    (pulled inboxes newly put on, a subset of "adds") and "no_room": {client: [ids]} (pulled inboxes left on none).

    ``pinned`` = {inbox id: DT campaign id} (pins()): senders of an active weekly v2 test stay on their test's campaign,
    in the sheet or not, never moved or taken off (a v2 test keeps the senders it was created with).

    ``tested`` = ids of the DT campaigns an active weekly v2 test runs on (None = the tests could not be read: the
    layout above, unchanged). With it (t16, 17 Nov 2026, the hold comment on PR #82): inboxes are only ever put on a
    DT campaign that has a test, never over ``cap``, and no campaign is created. Inboxes already on a campaign without
    a test stay there (nothing moves for it); a pulled inbox on one moves to a tested campaign with room, else it stays
    put and is in "no_room". New inboxes that do not fit are left off and listed in "unplaced" {client: [ids]} (the
    coverage reminder asks for a test for them)."""
    from smartlead.inbox_intake import CURRENT_CLIENTS
    adds: dict = {}
    removes: dict = {}
    drops: dict = {}
    create: list = []
    problems: list[str] = []
    retest_new: dict = {}
    no_room: dict = {}
    unplaced: dict = {}
    pulled_by_client: dict = {}
    for b in retest or []:
        if b["client"] in CURRENT_CLIENTS:
            pulled_by_client.setdefault(b["client"], set()).add(int(b["id"]))
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
        if not dts:
            continue
        on_dt = {i for c in dts for i in c["attached"]}
        mine_pinned = {i: k for i, k in (pinned or {}).items() if any(c["id"] == k and i in c["attached"] for c in dts)}
        base = (ids if drop_others else ids | on_dt) | set(mine_pinned)
        if tested is not None:
            lay = _tested_layout(client, dts, base, ids, on_dt, mine_pinned, pulled_by_client.get(client, set()),
                                 tested, cap, drop_others)
            for k, v in (("adds", adds), ("removes", removes), ("drops", drops), ("retest", retest_new)):
                v.update(lay[k])
            if lay["no_room"]:
                no_room[client] = lay["no_room"]
            unplaced[client] = lay["unplaced"]
            continue
        # pulled inboxes outside ``base``: those already on a DT campaign keep their place first, then by id
        extra = sorted(pulled_by_client.get(client, set()) - base, key=lambda i: (i not in on_dt, i))
        room = (max(len(dts), -(-len(base) // cap)) if base else len(dts)) * cap - len(base)
        fit, left = set(extra[:room]), sorted(extra[room:])
        if left:
            no_room[client] = left
        if drop_others:
            for c in dts:
                gone = sorted(c["attached"] - ids - fit - set(mine_pinned))
                if gone:
                    drops[int(c["id"])] = gone
                    c["attached"] = c["attached"] - set(gone)
        lay = _fill_first(dts, base | fit, cap, mine_pinned)
        adds.update(lay["adds"])
        removes.update(lay["removes"])
        create += lay["create"]
        for key, add in lay["adds"].items():
            new = [i for i in add if i in fit and i not in on_dt]
            if new:
                retest_new[key] = new
    return {"adds": adds, "removes": removes, "drops": drops, "create": create, "problems": problems,
            "retest": retest_new, "no_room": no_room, "unplaced": unplaced}


def _tested_layout(client: str, dts: list[dict], base: set[int], ids: set[int], on_dt: set[int], pinned: dict,
                   pulled: set[int], tested: set[int], cap: int, drop_others: bool) -> dict:
    """One client's DT layout when the active tests are known (plan(tested=...)). Pure."""
    t_dts = [c for c in dts if int(c["id"]) in tested]
    u_dts = [c for c in dts if int(c["id"]) not in tested]
    on_t = {i for c in t_dts for i in c["attached"]}
    on_u = {i for c in u_dts for i in c["attached"]}
    stay = (base & on_u) - on_t - pulled - set(pinned)          # on a campaign without a test: left where it is
    fill = base - stay
    extra = sorted(pulled - fill, key=lambda i: (i not in on_t, i in on_u, i))   # tested seats first, then by id
    room = max(0, len(t_dts) * cap - len(fill))
    fit, left = set(extra[:room]), extra[room:]
    drops: dict = {}
    if drop_others:
        for c in dts:
            gone = sorted(c["attached"] - ids - pulled - set(pinned))
            if gone:
                drops[int(c["id"])] = gone
                c["attached"] = c["attached"] - set(gone)
    lay = _fill_first(t_dts, fill | fit, cap, pinned, create_ok=False) if t_dts else \
        {"adds": {}, "removes": {}, "left": sorted(fill | fit)}
    placed = {i for v in lay["adds"].values() for i in v}
    removes = dict(lay["removes"])
    for c in u_dts:                                              # a pulled inbox moved off a campaign without a test
        moved = sorted(i for i in placed & c["attached"])
        if moved:
            removes.setdefault(int(c["id"]), []).extend(moved)
    retest = {}
    for key, add in lay["adds"].items():
        new = [i for i in add if i in pulled and i not in on_dt]
        if new:
            retest[key] = new
    not_placed = set(lay["left"])
    return {"adds": lay["adds"], "removes": {k: sorted(v) for k, v in removes.items()}, "drops": drops,
            "retest": retest, "no_room": sorted((set(left) | not_placed) & pulled),
            "unplaced": sorted(not_placed - pulled - on_dt)}


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
    out = {"created": [], "added": 0, "removed": 0, "dropped": 0, "missing": [],
           "added_by": {}, "removed_by": {}}     # per campaign (id, or a new campaign's name): confirmed counts
    for cid, gone in (p.get("drops") or {}).items():
        for k in range(0, len(gone), batch):
            await sl.remove_campaign_email_accounts(str(cid), gone[k:k + batch])
        still = await _on(sl, cid)
        out["dropped"] += sum(i not in still for i in gone)
        out.setdefault("dropped_ids", {})[cid] = [i for i in gone if i not in still]
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
                out["added_by"][key] = out["added_by"].get(key, 0) + 1
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
        if any(i not in still for i in go):
            out["removed_by"][cid] = sum(i not in still for i in go)
        out["missing"] += [f"{i} still on campaign {cid} after the move" for i in go if i in still]
    return out


def pins(tests: list[dict], campaigns: list[dict]) -> dict[int, int]:
    """{inbox id: campaign id} for every sender of an active weekly test (``_weekly_tests``) whose campaign is a DT
    campaign: the inbox stays there. Pure."""
    dt = {int(c["id"]) for c in campaigns if c.get("role") == "dt"}
    return {int(i): int(t["campaign_id"]) for t in tests if int(t.get("campaign_id") or 0) in dt
            for i in t.get("sender_ids") or ()}


def is_copy_test(name) -> bool:
    """The weekly copy check ("... synced copy"): a handful of inboxes already in another test send the client's
    current copy, so its senders are tested twice by design."""
    return "copy" in str(name or "").lower()


def twins(tests: list[dict]) -> dict[str, int]:
    """{name: how many} for names that more than one active weekly test carries (an old test left running next to
    its recreated copy, as BettrData's on 17 Nov 2026). Pure."""
    from collections import Counter
    n = Counter(str(t.get("name")) for t in tests)
    return {k: v for k, v in sorted(n.items()) if v > 1}


def cancelled_domains(tracker: dict[str, dict]) -> set[str]:
    """ScaledMail domains whose order is cancelled: the asset tracker's domain row is no longer Active
    (scaledmail_asset_sync only ever downgrades it). Their inboxes are left out of the coverage rule (Manveen
    16 Nov 2026: "all inboxes in test except scaled mail canceld orders"). Pure."""
    return {n for n, d in tracker.items() if "@" not in n and "scaled" in str(d.get("provider") or "").lower()
            and str(d.get("status") or "") != "Active"}


def coverage(members: dict[str, str], tests: list[dict]) -> dict[str, dict]:
    """The coverage rule (Manveen 16 Nov 2026: "make sure we have all inboxes in test except scaled mail canceld
    orders"): every connected inbox we use in exactly one ACTIVE weekly v2 test. ``members`` {email: client};
    ``tests`` = active weekly tests [{name, senders}]. {client: {"inboxes": n, "none": [emails in no test],
    "dupes": {email: [test names]}}}. The copy-check test counts for neither: an inbox whose only active test is
    the copy check is in "none", and being in it next to a weekly test is no duplicate. Pure."""
    where: dict[str, list[str]] = {}
    for t in tests:
        if is_copy_test(t.get("name")):     # reuses a few inboxes on purpose (copy_sync): no coverage, no duplicate
            continue
        for e in t.get("senders") or ():
            where.setdefault(str(e).lower(), []).append(str(t.get("name")))
    out: dict[str, dict] = {}
    for e, client in sorted(members.items()):
        c = out.setdefault(client, {"inboxes": 0, "none": [], "dupes": {}})
        c["inboxes"] += 1
        names = sorted(where.get(e.lower(), []))
        if not names:
            c["none"].append(e)
        elif len(names) > 1:
            c["dupes"][e] = names
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
    lines = ["Placement tests to update in Smart Delivery (a v2 test keeps the senders it was created with, so a "
             "missing inbox needs a new weekly test, at most 50 senders, starting Saturday after 05:30 IST):"]
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
    return ("🧪 *New inboxes for the placement tests*\n\n" + "\n".join("• " + l for l in lines)
            + "\n\n_The bot cannot edit scheduled tests in Smart Delivery, so these are for you to add._")


def deliver_dm(gaps: list[dict], added: set[str], created: list[str], pending: dict, *, complete: bool,
               send) -> tuple[bool | None, dict]:
    """Send the DM about this run's inboxes and campaigns plus any kept from runs whose DM failed.
    Returns (True / False = Slack accepted it or not, None = nothing to tell; what to keep for next run).
    A kept inbox that no longer misses its test is dropped quietly, but only when every account was read
    (``complete``): an unread account's gaps are unknown."""
    added_all = set(added) | set(pending.get("added") or [])
    created_all = list(dict.fromkeys(list(pending.get("created") or []) + list(created)))
    text = new_inbox_text(gaps, added_all, created_all)
    in_gaps = {e for g in gaps for e in g["add"]}
    gap_camps = {g["campaign"] for g in gaps}
    unshown = {"added": sorted(added_all - in_gaps) if not complete else [],
               "created": [n for n in created_all if n not in gap_camps] if not complete else []}
    if not text:
        return None, unshown
    if send(text):
        return True, unshown
    return False, {"added": sorted(added_all), "created": created_all}


def _no_room(results: dict) -> dict[str, int]:
    """{client: pulled inboxes on no DT campaign for lack of room}, over every account read."""
    out: dict[str, int] = {}
    for r in results.values():
        for client, k in ((r.get("no_room") or {}) if isinstance(r, dict) else {}).items():
            out[client] = out.get(client, 0) + int(k)
    return out


def infra_report(results: dict, known: set[str] | None, errors: list[str], dm_failed: bool,
                 today: date) -> tuple[list[str], list[str], set[str] | None]:
    """(headline, thread, every domain outside the sheet now) for the infra channel. ``results`` = per
    account {dropped: ["email (campaign)"], untracked: {domain: {client, inboxes}}, error}. Domains outside
    the sheet are told once (``known`` = already told; None = unknown, so none are told). Empty headline =
    nothing to post. Pure."""
    rows = [r for r in results.values() if isinstance(r, dict)]
    drops = [d for r in rows for d in r.get("dropped") or []]
    untracked: dict = {}
    for r in rows:
        untracked.update(r.get("untracked") or {})
    now = set(untracked) if any("untracked" in r for r in rows) else None
    new = sorted(d for d in untracked if known is not None and d not in known)
    parts, thread = [], []
    if drops:
        parts.append(f"{_n(len(drops))} taken off")
        thread.append("Taken off the DT test campaigns: their domain is not in the deliverability sheet, so they "
                      "leave the weekly placement test:\n" + "\n".join(f"• {d}" for d in drops))
    if new:
        parts.append(f"{len(new)} domain{'s' if len(new) != 1 else ''} not in the deliverability sheet")
        thread.append("In Smartlead for a current client but not in the deliverability sheet, so kept off the DT "
                      "test campaigns. Add the domain to the sheet if we use it (each domain is told once):\n"
                      + "\n".join(f"• {d} ({untracked[d]['client']}, {_n(untracked[d]['inboxes'])})" for d in new))
    room = _no_room(results)
    if room:
        parts.append(f"{sum(room.values())} pulled inbox{'es' if sum(room.values()) != 1 else ''} with no room on a "
                     "DT campaign")
        thread.append("Taken out after a failed v2 test, but every DT campaign of the client holds "
                      f"{DT_CAP}, so the weekly test does not check them. Make room, or create the next DT campaign and "
                      "its weekly test:\n" + "\n".join(f"• {cl}: {_n(k)}" for cl, k in sorted(room.items())))
    none = {cl: c for r in rows for cl, c in (r.get("coverage") or {}).items() if c.get("none")}
    dupes = {cl: c for r in rows for cl, c in (r.get("coverage") or {}).items() if c.get("dupes")}
    if none:
        k = sum(len(c["none"]) for c in none.values())
        parts.append(f"{_n(k)} in no active weekly v2 test")
        for cl, c in sorted(none.items()):
            on = c.get("on") or {}
            thread.append(f"{cl}: {_n(len(c['none']))} of {c['inboxes']} in no active weekly v2 test:\n"
                          + "\n".join(f"• {e} ({on.get(e) or 'no DT campaign'})" for e in c["none"]))
    if dupes:
        parts.append(f"{_n(sum(len(c['dupes']) for c in dupes.values()))} in 2 or more weekly tests")
        for cl, c in sorted(dupes.items()):
            thread.append(f"{cl}: in 2 or more active weekly v2 tests:\n"
                          + "\n".join(f"• {e}: {', '.join(n)}" for e, n in sorted(c["dupes"].items())))
    head = []
    if parts or errors or dm_failed:
        head.append(f"🧪 Test campaigns, {today.day} {today:%b}: " + (", ".join(parts) if parts else "could not finish"))
        if errors:
            names = sorted({e.split(":", 1)[0] for e in errors})
            head.append(f"Could not finish: {', '.join(names)} (details in the thread)")
            thread.append("Errors:\n" + "\n".join(f"• {e}" for e in errors))
        if dm_failed:
            head.append("The DM about new inboxes did not go out; it is kept and sent again next run.")
    return head, thread, now


def _tab(client: str) -> str:
    """The deliverability sheet's tab for a client (smartlead/tracked.TABS), or a plain hint."""
    from smartlead.tracked import TABS
    return next((t for t in TABS if t.lower() == str(client).lower()), "the client's tab")


SATURDAY = "Saturday after 05:30 IST"     # the seed pool refills 05:30 IST daily; new weekly tests start on Saturdays


def free_slots(taken: set[str], today: date, n: int) -> list[str]:
    """The first ``n`` half-hour start times on the next Saturday (after today) from 06:00 IST that no active weekly
    test uses ("HH:MM" schedule.timeOfDay, every account: the seed pool is shared). E.g. "Sat 21 Nov 11:30 IST". Pure."""
    from datetime import timedelta
    sat = today + timedelta(days=(5 - today.weekday()) % 7 or 7)
    out = []
    for m in range(6 * 60, 24 * 60, 30):
        hm = f"{m // 60:02d}:{m % 60:02d}"
        if hm not in taken:
            out.append(f"Sat {sat.day} {sat.strftime('%b')} {hm} IST")
            if len(out) == n:
                break
    return out


def _chrome(test: str | None, camp: str | None, n: int, slot: str = SATURDAY) -> str:
    """A ready Claude-in-Chrome line. Smartlead's API cannot edit a v2 test's senders and the help centre only shows
    picking them when a test is created (17 Nov 2026), so the line edits only if the page allows it."""
    them = "it" if n == 1 else f"the {n} inboxes"
    new = (f"create a new weekly v2 test (Automatic, campaign {camp or 'a DT campaign of the client'}, Google + "
           f"Microsoft, text only, weekly, first run {slot}, at most {DT_CAP} senders) with {them} listed in the "
           "digest thread")
    if test:
        return (f"Claude in Chrome: \"In Smartlead > Smart Delivery, open the weekly v2 test '{test}'. If it lets you edit "
                f"its sender accounts, add only {them} listed in the digest thread and save; change nothing else. "
                f"If its senders cannot be edited, leave it as it is and {new}. Change nothing else; never click Run now.\"")
    return f"Claude in Chrome: \"In Smartlead > Smart Delivery, {new}. Change nothing else; never click Run now.\""


def _gap_step(client: str, c: dict, dt_tests: dict, untested: list[str], next_slot=lambda: SATURDAY) -> str:
    """Per DT campaign the missing inboxes sit on: the active test there with room, or "needs a new test at <the next
    free Saturday slot>" (``next_slot`` hands out one slot per new test)."""
    on = c.get("on") or {}
    groups: dict = {}
    for e in c.get("none") or []:
        groups.setdefault(on.get(e), []).append(e)
    parts = []
    spare = untested[0] if untested else None
    for camp in sorted(groups, key=lambda x: (x is None, str(x))):
        n = len(groups[camp])
        it = "it" if n == 1 else "them"
        t = dt_tests.get(camp) if camp else None
        if t and t["senders"] + n <= DT_CAP:
            parts.append(f"{n} on {camp}: \"{t['test']}\" on {camp} has room ({t['senders']} of {DT_CAP} senders). "
                         + _chrome(t["test"], camp, n))
            continue
        slot = next_slot()
        if camp is None:
            where = spare or "a new DT campaign of the client (a copy of its DT #1)"
            parts.append(f"{n} on no DT campaign: needs a new test at {slot}; put {it} on {where} first, then "
                         + _chrome(None, where, n, slot))
        elif t:
            parts.append(f"{n} on {camp} (its test \"{t['test']}\" is full): needs a new test at {slot}, on "
                         f"{spare or camp}. " + _chrome(None, spare or camp, n, slot))
        else:
            parts.append(f"{n} on {camp}, which has no active weekly test: needs a new test at {slot}, there. "
                         + _chrome(None, camp, n, slot))
    return (" · ".join(parts) + f" A v2 test keeps the senders it was created with (at most {DT_CAP}), so putting an "
            "inbox on a campaign is not enough. The list is in the thread.")


def coverage_asks(results: dict, today: date | None = None) -> list:
    """Digest asks of the coverage rule, per client: inboxes in no active weekly v2 test (per DT campaign they sit on:
    the test with room or a new test on Saturday, with a Claude-in-Chrome line; on Fridays posted at once, once per
    Friday: Manveen 17 Nov 2026, "remid me to update weekly scheduled test for all missed inboxes"), inboxes in two or
    more, and an account whose tests could not be read (broken). Silent when every inbox is covered. Pure."""
    from smartlead import digest as dg
    friday = bool(today and today.weekday() == 4)
    taken = {x for r in results.values() if isinstance(r, dict) for x in r.get("slots") or []}
    pool = iter(free_slots(taken, today or date.today(), 48))
    asks = []
    for acc, r in sorted(results.items()):
        if not isinstance(r, dict) or "coverage" not in r:
            continue
        for name, k in sorted((r.get("twins") or {}).items()):
            asks.append(dg.Ask(f"{k} active weekly v2 tests in {acc} are called \"{name}\", so the same inboxes are "
                               "tested and paid for twice.",
                               "In Smart Delivery, stop the older one (its created date is in the test list).",
                               key=f"{SOURCE}:coverage-twins:{acc}:{name}"))
        if r["coverage"] is None:
            asks.append(dg.Ask(f"The weekly-test coverage check could not read the v2 tests of {acc} "
                               f"({r.get('coverage_error') or 'error'}), so it does not know which inboxes are tested.",
                               "Nothing to do if the next run (06:00 IST) reads them; if it keeps failing, look at the "
                               "dt-senders log.", key="dt-senders:coverage-unread", kind="broken"))
            continue
        for client, c in sorted(r["coverage"].items()):
            none, dupes = c.get("none") or [], c.get("dupes") or {}
            if none:
                verb = "is" if len(none) == 1 else "are"
                text = (f"{len(none)} {client} inbox{'es' if len(none) != 1 else ''} {verb} in no weekly test, "
                        f"so nothing checks {'its' if len(none) == 1 else 'their'} placement.")
                step = _gap_step(client, c, r.get("dt_tests") or {}, (r.get("untested_dts") or {}).get(client) or [],
                                 lambda: next(pool, SATURDAY))
                if friday:
                    asks.append(dg.Ask("Friday reminder: " + text, step, urgent=True,
                                       key=f"{SOURCE}:coverage:{client}:{today.isoformat()}"))
                else:
                    asks.append(dg.Ask(text, step, key=f"{SOURCE}:coverage:{client}"))
            if dupes:
                asks.append(dg.Ask(
                    f"{_n(len(dupes))} of {client} {'is' if len(dupes) == 1 else 'are'} in 2 or more active weekly v2 "
                    "tests, so their seed emails are paid twice.",
                    "Stop one of the tests named in the thread, or recreate it without them.",
                    key=f"{SOURCE}:coverage-dupes:{client}"))
    return asks


def digest_items(results: dict, known: set[str] | None, errors: list[str], dm_failed: bool,
                 today: date) -> tuple[list, list]:
    """(asks, done) for the daily digest, from the same inputs as infra_report. Done: inboxes put on, moved
    and taken off, and campaigns created, one line per campaign (key: date + campaign). Asks: domains outside
    the deliverability sheet not told before (each domain once, as the post did), errors and a DM that did not
    go out (kind "broken"). Pure."""
    from smartlead import digest as dg
    rows = [(acc, r) for acc, r in results.items() if isinstance(r, dict)]
    t = today.isoformat()
    done = []
    for acc, r in rows:
        for name in r.get("created") or []:
            done.append(dg.Done(f"Created test campaign {name} (a copy of the client's first DT campaign).",
                                key=f"{SOURCE}:{t}:{name}:created"))
        for camp, k in sorted((r.get("added_to") or {}).items()):
            done.append(dg.Done(f"Put {_n(k)} on {camp} for the weekly placement test.", key=f"{SOURCE}:{t}:{camp}:added"))
        for camp, k in sorted((r.get("retest_to") or {}).items()):
            them = "them until they are" if k != 1 else "it until it is"
            done.append(dg.Done(f"Put {_n(k)} taken out after a failed v2 test on {camp}, so the weekly test keeps "
                                f"checking {them} back in use.", key=f"{SOURCE}:{t}:{camp}:retest"))
        for camp, k in sorted((r.get("moved_off") or {}).items()):
            done.append(dg.Done(f"Moved {_n(k)} off {camp} to fill an earlier DT campaign first.",
                                key=f"{SOURCE}:{t}:{camp}:moved"))
        off: dict[str, int] = {}
        for d in r.get("dropped") or []:
            camp = d.rsplit(" (", 1)[-1].rstrip(")") if " (" in d else "a DT campaign"
            off[camp] = off.get(camp, 0) + 1
        for camp, k in sorted(off.items()):
            done.append(dg.Done(f"Took {_n(k)} off {camp}: their domain is not in the deliverability sheet.",
                                key=f"{SOURCE}:{t}:{camp}:off"))
    untracked: dict = {}
    for _, r in rows:
        untracked.update(r.get("untracked") or {})
    new = sorted(d for d in untracked if known is not None and d not in known)
    asks = []
    if new:
        k = len(new)
        tabs = sorted({_tab(untracked[d]["client"]) for d in new})
        text = (f"{k} current-client domain{'s are' if k != 1 else ' is'} in Smartlead but not in the deliverability "
                f"sheet, so {'their' if k != 1 else 'its'} inboxes are kept off the weekly placement test.")
        step = (f"If we use it, add {new[0]} to the deliverability sheet, tab {tabs[0]}, column A; the next run puts "
                "its inboxes on the test campaigns." if k == 1 else
                f"For each domain in the thread we use, add it to the deliverability sheet (tab {', '.join(tabs)}), "
                "column A; the next run puts its inboxes on the test campaigns.")
        asks.append(dg.Ask(text, step, key=f"{SOURCE}:untracked"))
    for client, k in sorted(_no_room(results).items()):
        asks.append(dg.Ask(
            f"{k} {client} inbox{'es' if k != 1 else ''} taken out after a failed v2 test {'are' if k != 1 else 'is'} on "
            f"no DT campaign: every DT campaign of the client holds {DT_CAP}, so the weekly test does not check "
            f"{'them' if k != 1 else 'it'}.",
            f"Make room on a DT campaign of {client}, or create the next DT campaign and its weekly test; the next run "
            f"(06:00 IST) puts {'them' if k != 1 else 'it'} on.",
            key=f"{SOURCE}:no-room:{client}"))
    asks += coverage_asks(results, today)
    if errors:
        names = sorted({e.split(":", 1)[0] for e in errors})
        asks.append(dg.Ask(f"The test-campaign job could not finish: {', '.join(names)}.",
                           "The errors are in the thread; it runs again tomorrow at 06:00 IST.",
                           key=f"{SOURCE}:errors", kind="broken"))
    if dm_failed:
        asks.append(dg.Ask("The DM about new inboxes for the placement tests did not go out.",
                           "Nothing to do: it is kept and sent again with the next run.", key=f"{SOURCE}:dm",
                           kind="broken"))
    return asks, done


def coverage_record(results: dict, prev: dict | None, today: date) -> dict:
    """Mongo test_coverage, one doc a day (Manveen 17 Nov 2026: "mae sure we save thta on db as well"): per client the
    inboxes in no weekly test (each with the day it was first seen missing), the duplicates, and "resolved": inboxes
    missing on the last record that are covered now (an inbox that left our list is "left", not resolved). An account
    whose tests were not read keeps its last record (stale) and resolves nothing. Pure."""
    prev = prev or {}
    pclients = prev.get("clients") or {}
    clients: dict[str, dict] = {}
    read: dict[str, bool] = {}
    accounts: dict[str, list[str]] = {}
    for acc, r in sorted(results.items()):
        if not isinstance(r, dict) or "coverage" not in r:
            continue
        read[acc] = r["coverage"] is not None
        if r["coverage"] is None:
            for client in (prev.get("accounts") or {}).get(acc) or []:
                old = pclients.get(client) or {}
                clients[client] = {**old, "resolved": [], "stale": True}
            accounts[acc] = list((prev.get("accounts") or {}).get(acc) or [])
            continue
        accounts[acc] = sorted(r["coverage"])
        for client, c in sorted(r["coverage"].items()):
            since = {m["email"]: m.get("since") for m in (pclients.get(client) or {}).get("missing") or []}
            none = sorted(c.get("none") or [])
            members = set(c["all"]) if "all" in c else None
            gone_or_covered = sorted(set(since) - set(none))
            resolved = [{"email": e, "since": since[e], "on": today.isoformat(), "resolved": True}
                        for e in gone_or_covered if members is None or e in members]
            left = [e for e in gone_or_covered if members is not None and e not in members]
            dupes = dict(sorted((c.get("dupes") or {}).items()))
            doc = {"inboxes": c.get("inboxes", 0),
                   "missing": [{"email": e, "since": since.get(e) or today.isoformat(), "resolved": False} for e in none],
                   "missing_n": len(none), "dupes": dupes, "dupes_n": len(dupes), "resolved": resolved}
            if left:
                doc["left"] = left
            clients[client] = doc
    all_read = bool(read) and all(read.values())
    return {"_id": today.isoformat(), "date": today.isoformat(), "tests_read": read, "accounts": accounts,
            "clients": clients,
            "all_covered": (all(not c["missing"] for c in clients.values()) if all_read else None)}


def _coverage_store():
    """(read the newest earlier record, save a record) on Mongo test_coverage; (None-returning, no-op) without Mongo."""
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    if db is None:
        return (lambda: None), (lambda doc: None)
    col = db["test_coverage"]
    today = date.today().isoformat()

    def prev():
        return next(iter(col.find({"_id": {"$lt": today}}).sort("_id", -1).limit(1)), None)

    def save(doc):
        col.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    return prev, save


def _state_col():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db["dt_senders"] if db is not None else None


def _post_infra(head: list[str], thread: list[str]) -> bool:
    from smartlead import notify
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    return bool(token and notify.post_with_thread(token, notify.infra_channel("DT_SENDERS_CHANNEL"), head, thread))


def _weekly_tests(key: str) -> list[dict]:
    """ACTIVE recurring Smart Delivery v2 tests with their senders: [{id, name, campaign_id, senders: {emails},
    sender_ids: {inbox ids}}]. Read-only, no credit. Raises v2_api.V2ApiError (no URL, no key in its text).
    Since 17 Nov 2026 the weekly tests are v2 (docs/V2_WEEKLY.md); the classic reader below is no longer called."""
    from smartlead import v2_api
    cl = v2_api.Client(key)
    out = []
    for t in cl.tests():
        if str(t.get("status")).upper() != "ACTIVE" or not t.get("schedule"):
            continue
        d = cl.test(str(t["id"]))
        snd = [x for x in d.get("senders") or [] if isinstance(x, dict)]
        out.append({"id": t["id"], "name": d.get("name") or t.get("name"),
                    "time": str((d.get("schedule") or t.get("schedule") or {}).get("timeOfDay") or "") or None,
                    "campaign_id": int(d.get("campaignId") or t.get("campaignId") or 0),
                    "senders": {str(x.get("email") or "").strip().lower() for x in snd if x.get("email")},
                    "sender_ids": {int(x["emailAccountId"]) for x in snd if str(x.get("emailAccountId") or "").isdigit()}})
    return out


async def _scheduled_tests(key: str) -> list[dict]:
    """Classic (v1) active scheduled tests with their senders. Read-only, no credit. Unused since 17 Nov 2026."""
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
    except Exception:  # noqa: BLE001 - kept in Mongo and tried again next run (deliver_dm)
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
    from smartlead.inbox_intake import CURRENT_ACCOUNTS, CURRENT_CLIENTS
    from smartlead.notify import redact
    from smartlead.report_sheet import client_label
    from smartlead.tracked import only_tracked, tracked_domains
    out: dict = {}
    tracked = tracked_domains()
    from smartlead.zapmail_asset_sync import read_tracker
    try:
        tracker = read_tracker()
    except Exception:  # noqa: BLE001 - coverage is then not judged (a broken ask), the rest goes on
        tracker = {}
    cancelled = cancelled_domains(tracker) if tracker else None
    if tracked is None:
        print("  [dt-senders] deliverability sheet unreadable: keeping every inbox, dropping none", file=sys.stderr)

    async def one(acc):
        async with SmartleadClient(acc.api_key, acc.name) as c:
            listed = await c.list_email_accounts()
            boxes = [{"id": a["id"], "email": a.get("from_email"), "client": client_label(acc.name, a.get("client_id"))}
                     for a in listed
                     if not is_excluded_inbox({"email": a.get("from_email") or "", "tags": a.get("tags", [])})]
            camps = await _campaigns(c, acc.name)
            pulled_ids = {int(a["id"]) for a in listed if is_pulled(a)}
            pulled = [b for b in boxes if int(b["id"]) in pulled_ids]
            from smartlead import v2_api
            try:
                tests, tests_error = await asyncio.to_thread(_weekly_tests, acc.api_key), ""
                pinned = pins(tests, camps)
                tested = {int(t["campaign_id"]) for t in tests if not is_copy_test(t.get("name"))}
            except v2_api.V2ApiError as exc:     # tests unknown: nobody on a DT campaign moves today
                tests, tests_error = None, v2_api.describe(exc)
                pinned = {int(i): int(x["id"]) for x in camps if x["role"] == "dt" for i in x["attached"]}
                tested = None
            used = only_tracked(boxes, tracked)
            p = plan(used, camps, drop_others=tracked is not None, retest=pulled, pinned=pinned, tested=tested)
            p["no_room"] = drop_cancelled(p["no_room"], boxes, cancelled)
            names = {x["id"]: x["name"] for x in camps}
            res = {"would_add": {names.get(k, k): len(v) for k, v in p["adds"].items()},
                   "would_remove": {names.get(k, k): len(v) for k, v in p["removes"].items()},
                   "would_drop": {names.get(k, k): len(v) for k, v in p["drops"].items()},
                   "would_create": [x["name"] for x in p["create"]], "problems": p["problems"],
                   "would_retest": {names.get(k, k): len(v) for k, v in p["retest"].items()},
                   "no_room": {cl: len(v) for cl, v in p["no_room"].items()},
                   "unplaced": {cl: len(v) for cl, v in p["unplaced"].items() if v}}
            ok = {int(a["id"]) for a in listed if a.get("is_smtp_success") and a.get("is_imap_success")}
            members = {str(b["email"]).strip().lower(): b["client"] for b in used + pulled
                       if b["client"] in CURRENT_CLIENTS and int(b["id"]) in ok and b.get("email")
                       and str(b["email"]).lower().split("@")[-1] not in (cancelled or set())}
            if tests is None or cancelled is None:
                res["coverage"] = None
                res["coverage_error"] = tests_error or "asset tracker unreadable, so cancelled orders are unknown"
            else:
                at = {int(i): x["name"] for x in camps if x["role"] == "dt" for i in x["attached"]}
                by_email = {str(b["email"]).strip().lower(): int(b["id"]) for b in boxes if b.get("email")}
                res["coverage"] = coverage(members, tests)
                res["twins"] = twins(tests)
                res["slots"] = sorted({t["time"] for t in tests if t.get("time")})
                for client, cv in res["coverage"].items():
                    cv["on"] = {e: at.get(by_email.get(e)) for e in cv["none"]}
                    cv["all"] = sorted(e for e, cl in members.items() if cl == client)
                names_by_id = {int(x["id"]): x["name"] for x in camps if x["role"] == "dt"}
                res["dt_tests"] = {names_by_id[int(t["campaign_id"])]: {"test": t["name"], "senders": len(t["senders"])}
                                   for t in tests if not is_copy_test(t.get("name"))
                                   and int(t["campaign_id"]) in names_by_id}
                res["untested_dts"] = {}
                for x in sorted(camps, key=lambda x: _dt_number(x["name"])):
                    if x["role"] == "dt" and x["name"] not in res["dt_tests"]:
                        res["untested_dts"].setdefault(x["client"], []).append(x["name"])
            if tracked is not None:
                unt: dict = {}
                for b in boxes:
                    d = str(b.get("email") or "").lower().split("@")[-1]
                    if int(b["id"]) in pulled_ids:       # kept on the tests by the retest rule, not "kept off"
                        continue
                    if b["client"] in CURRENT_CLIENTS and d and d not in tracked:
                        u = unt.setdefault(d, {"client": b["client"], "inboxes": 0})
                        u["inboxes"] += 1
                res["untracked"] = unt
            if args.apply and (p["adds"] or p["removes"] or p["drops"] or p["create"]):
                res.update(await apply(c, p))
                every = {int(a["id"]): str(a.get("from_email") or a["id"]) for a in listed}
                res["dropped"] = [f"{every.get(int(i), i)} ({names.get(int(cid), cid)})"
                                  for cid, ids in (res.get("dropped_ids") or {}).items() for i in ids]
                res["added_to"] = {str(names.get(k, k)): n for k, n in res.pop("added_by", {}).items()}
                res["moved_off"] = {str(names.get(k, k)): n for k, n in res.pop("removed_by", {}).items()}
                landed = set(res.get("new_ids") or [])
                res["retest_to"] = {}
                for k, ids in p["retest"].items():     # confirmed on; told as their own line, not as "added"
                    n, name = sum(i in landed for i in ids), str(names.get(k, k))
                    if n:
                        res["retest_to"][name] = n
                        res["added_to"][name] -= n
                        if res["added_to"][name] <= 0:
                            res["added_to"].pop(name)
            if args.apply or args.gaps:
                emails = {int(a["id"]): str(a.get("from_email") or "").lower() for a in await c.list_email_accounts()}
                camps_now = await _campaigns(c, acc.name)
                dts = [{"id": x["id"], "name": x["name"], "emails": {emails[i] for i in x["attached"] if i in emails}}
                       for x in camps_now if x["role"] == "dt"]
                dt_ids = {i for x in camps_now if x["role"] == "dt" for i in x["attached"]}
                res["gaps"] = test_gaps(dts, tests or [])
                res["new_on_dt"] = sorted(emails[i] for i in res.get("new_ids") or [] if i in dt_ids and i in emails)
            return res

    for acc in discover_accounts():
        if acc.name.upper() not in CURRENT_ACCOUNTS:
            continue
        try:
            out[acc.name] = asyncio.run(one(acc))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            out[acc.name] = {"error": redact(str(exc))[:200]}
    failed = False
    if args.apply:
        # DM only about inboxes added and campaigns created by THIS run (Manveen, 15 Oct: tell me
        # about new inboxes; a standing gap is not repeated), plus any whose DM did not go out.
        # `--gaps` prints the full list on demand.
        errors = [f"{n}: {r['error']}" for n, r in out.items() if isinstance(r, dict) and r.get("error")]
        if tracked is None:
            errors.append("deliverability sheet: unreadable, so every inbox was kept and none taken off")
        gaps = [g for r in out.values() if isinstance(r, dict) for g in r.get("gaps") or []]
        added = {e for r in out.values() if isinstance(r, dict) for e in r.get("new_on_dt") or []}
        created = [n for r in out.values() if isinstance(r, dict) for n in r.get("created") or []]
        col = _state_col()
        if col is None:
            errors.append("Mongo: unavailable, so a failed DM is not kept and domains outside the sheet are not checked")
        pending = (col.find_one({"_id": PENDING_DM}) or {}) if col is not None else {}
        complete = not any(isinstance(r, dict) and r.get("error") for r in out.values())
        sent, keep = deliver_dm(gaps, added, created, pending, complete=complete, send=_dm)
        if sent is not None:
            out["dm_sent"] = sent
            failed |= not sent
        if col is not None:
            if keep["added"] or keep["created"]:
                col.update_one({"_id": PENDING_DM}, {"$set": {**keep, "at": date.today().isoformat()}}, upsert=True)
            elif pending:
                col.delete_one({"_id": PENDING_DM})
        try:                                       # the daily coverage record (Mongo test_coverage)
            get_prev, save = _coverage_store()
            save(coverage_record(out, get_prev(), date.today()))
        except Exception as exc:  # noqa: BLE001 - the record is extra; the run goes on
            errors.append(f"test_coverage: {type(exc).__name__}")
        known = set((col.find_one({"_id": UNTRACKED}) or {}).get("domains") or []) if col is not None else None
        head, thread, now = infra_report(out, known, errors, sent is False, date.today())
        posted = True
        in_digest = False
        from smartlead import digest as dg
        if dg.on():          # every --apply run: a fixed problem clears
            asks, done = digest_items(out, known, errors, sent is False, date.today())
            in_digest = out["digest"] = dg.report(SOURCE, asks, done, ["\n".join(head)] + thread if head else [])
        if head and not in_digest:
            posted = out["infra_posted"] = _post_infra(head, thread)
            failed |= not posted
        if col is not None and now is not None and posted:
            told = now if complete else now | (known or set())      # an unread account's domains stay known
            col.update_one({"_id": UNTRACKED}, {"$set": {"domains": sorted(told)}}, upsert=True)
    print(json.dumps(out, indent=1, default=sorted))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
