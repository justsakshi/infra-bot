"""Inboxes their provider has removed, and the one digest ask to delete them from Smartlead by hand (t352, 20 Nov 2026).

On 20 Nov the digest told Manveen to reconnect the login of 3 inboxes on a ScaledMail order we had cancelled:
ScaledMail had removed the mailboxes, so there was nothing to reconnect. Manveen: "sure then if they are rmoved
don't remind me just remid me to delte make sure you are up to mark".

An inbox is "removed by its provider" only when the provider's own data says so, never from a login failure alone:

  * ScaledMail: its domain is registered at ScaledMail but has no inbox plan there and is not at Zapmail (the order
    was cancelled and ScaledMail took the mailboxes away), or its domain is listed with an order whose status is
    Cancelled;
  * Zapmail: its domain is one we still hold in a Zapmail account (not expired) and the mailbox is not on it (the
    domain has no mailboxes, or Zapmail lists every mailbox the domain has and this one is not among them);
  * Zapmail, a lost domain (t353): its domain is a Zapmail domain that has EXPIRED, or one the asset tracker files under
    Zapmail that no Zapmail account has any more, AND Smartlead says the inbox is disconnected (SMTP or IMAP fails).
    Both are needed, so an inbox that still works is never listed.

Only the clients the bot works for are listed (PL, Melior, BettrData; warmup_repair.CLIENTS): an old client's inboxes
(OSC) are left alone, as everywhere else.

An inbox a provider still lists is never called removed, whatever Smartlead says about its login (that stays the
"reconnect" ask). An inbox at neither provider (InboxKit, a client's own, a past client's) is not judged here. If a
provider could not be read, or answered with an empty list while orders are live, nothing is judged from it.

Who reads what:
  * zapmail_digest.py (09:40 IST, already reads both providers) saves the provider facts and every Smartlead inbox
    (email-accounts of each account, GETs: Mongo inbox_status and inbox_daily leave out inboxes in no campaign, and
    96 of PL's 422 were such on 20 Nov) in Mongo provider_inventory.
  * digest_post.py joins them with the sync's view (Mongo inbox_status: client, sending campaigns, gone today), so
    an inbox deleted by hand drops out of the reminder the next day. It is one ask ("N inboxes were removed by their
    provider; delete them from Smartlead by hand"), and every other ask is shown without those inboxes
    (digest.drop_removed): no reconnect, warm-up repair, repair-settings or weekly-test ask names them.
  * fleet_watch.py leaves them out of the warm-up repair asks and counts.

Delete now or check first (t353, 20 Nov 2026). Deleting an inbox from Smartlead takes it out of every campaign and loses
its threads. On 16-17 Nov a hand check kept 24 of 74 cancelled ScaledMail inboxes off the delete list: they sat in a
paused client campaign, and some had lead replies in the last 30 days. So the ask is in two lists:
  * delete now: in no campaign but test campaigns (v2_results.is_test_campaign), and no lead reply in the last 30
    days in a thread the inbox sent in;
  * check first, each with its reason: the campaigns it is still in (any status but finished: COMPLETED, STOPPED and
    ARCHIVED cannot send again), "lead reply in the last 30 days", or "could not check replies" / "campaigns not
    read" when the check did not complete. An inbox is never put under "delete now" on an incomplete check.
The check (read_use) runs once a day in zapmail_digest after the provider facts, only for the removed inboxes' accounts:
GET campaigns, each unfinished campaign's email-accounts, each non-test campaign's replied statistics, and the message
history of each reply in the window that Mongo lead_senders (inbox_evidence's cache) cannot place. Paced under
Smartlead's shared 200 a minute, with a time limit.

The bot never deletes an inbox: a Smartlead delete is immediate and permanent, so it is done by hand only.
"""
from __future__ import annotations

import asyncio
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

COLLECTION = "provider_inventory"
DOC_ID = "latest"
SOURCE = "provider-removed"
KEY = "provider-removed:delete"
MAX_AGE = timedelta(days=8)          # older provider facts are not used (digest_post.MAX_ASK_AGE)

SM_NO_PLAN = "order cancelled, no inboxes left"          # why, in the ask's list ('at ScaledMail (...)')
SM_CANCELLED = "order cancelled"
ZAP_EMPTY = "no inboxes left on the domain"
ZAP_GONE = "inbox gone from the domain"
ZAP_EXPIRED = "domain expired"                        # t353: with Smartlead showing the inbox disconnected
ZAP_LEFT = "domain no longer at Zapmail"              # tracker says Zapmail, no Zapmail account has it

# Delete now or check first (t353)
FINISHED = {"COMPLETED", "STOPPED", "ARCHIVED"}       # campaigns that cannot send again: membership not read
REPLY_DAYS = 30
USE_MAX_AGE = timedelta(hours=36)                     # the campaign/reply check is daily; older is "not checked"
PACE_S = 0.35                                         # at least this between Smartlead calls (200 a minute shared)
BUDGET_S = 15 * 60                                    # 09:40 job: done before the 10:00 sync shares the limit
REPLIED = "lead reply in the last 30 days"
NO_REPLY_CHECK = "could not check replies"
NO_CAMPAIGNS = "campaigns not read"
NOT_CHECKED = "campaigns and replies not checked"
LIST_LINES = 20                                       # lines of this ask's list in the digest thread (digest_post)

CLIENT_SPELLING = {"Bettrdata": "BettrData", "BETTRDATA": "BettrData", "PRECISE_LEADS": "Precise Leads",
                   "MELIOR": "Melior"}                 # the sync's labels -> the digest's
CLIENTS = ("Precise Leads", "Melior", "BettrData")       # warmup_repair.CLIENTS, in the digest's spelling
EXPIRED = "EXPIRED"


def _low(x) -> str:
    return str(x or "").strip().lower()


def _int(x) -> int | None:
    try:
        return int(str(x).strip())
    except (TypeError, ValueError):
        return None


# --- the provider facts (pure) ---------------------------------------------------------------------------------------

def scaledmail_facts(orders: list[dict] | None, domains: list[dict] | None, registered: list[dict] | None) -> dict:
    """{"read": True, "plan": {domain: order status}, "registered": [domain]} from ScaledMail's orders, domains (with
    their inbox plan) and purchased domains. ``read`` False when a list is missing, or when no domain has an inbox plan
    while an order is live (an empty answer is not evidence). Pure."""
    if orders is None or domains is None or registered is None:
        return {"read": False, "why": "not read"}
    status = {o.get("id"): str(o.get("status") or "") for o in orders}
    if not domains and any(s == "Active" for s in status.values()):
        return {"read": False, "why": "no domain listed while an order is live"}
    plan = {_low(d.get("domain")): status.get(d.get("payment_id"), "") for d in domains if d.get("domain")}
    return {"read": True, "plan": plan, "registered": sorted({_low(r.get("domain")) for r in registered if r.get("domain")})}


def zapmail_facts(domains: list[dict] | None, mailboxes: list[dict] | None, errors=(), past=()) -> dict:
    """{"read": True, "domains": {domain: {status, assigned, account}}, "mailboxes": [email], "past": [domain]} from
    zapmail_fleet.all_domains / all_mailboxes; ``past``: domains the asset tracker files under Zapmail. ``read`` False
    when a list failed on any account. Pure."""
    if domains is None or mailboxes is None or errors:
        return {"read": False, "why": "not read in full"}
    doms = {}
    for d in domains:
        name = _low(d.get("domain"))
        if name:
            doms[name] = {"status": str(d.get("status") or ""), "assigned": _int(d.get("assigned_mailboxes")),
                          "account": str(d.get("account") or "")}
    return {"read": True, "domains": doms, "mailboxes": sorted({_low(m.get("email")) for m in mailboxes if m.get("email")}),
            "past": sorted({_low(d) for d in past or () if _low(d) and "@" not in _low(d)})}


def smartlead_facts(rows: list[dict] | None, errors=()) -> dict:
    """{"read": True, "inboxes": [{email, client, account, disconnected}]} from every Smartlead account's email
    accounts (disconnected: SMTP or IMAP fails); ``read`` False when an account could not be read (then the sync's
    inbox_status is used alone). Pure."""
    if rows is None or errors:
        return {"read": False, "why": "not read in full"}
    seen = {}
    for r in rows:
        e = _low(r.get("email"))
        if "@" in e:
            seen.setdefault(e, {"email": e, "client": str(r.get("client") or ""), "account": str(r.get("account") or ""),
                                "disconnected": r.get("disconnected")})
    return {"read": True, "inboxes": [seen[e] for e in sorted(seen)]}


def inboxes_of(facts: dict | None, status: list[dict]) -> list[dict]:
    """Today's Smartlead inboxes: Smartlead's own list when read (inboxes in no campaign included), each with the
    sync's client, sending campaigns and state where it has them; an inbox the sync calls gone is left out. Without
    Smartlead's list, the sync's inboxes (inbox_status) alone. Pure."""
    by = {_low(d.get("email")): d for d in status or [] if d.get("email")}
    sl = (facts or {}).get("smartlead") or {}
    if sl.get("read") is not True:
        return [d for d in by.values() if d.get("state") != "gone"]
    out = []
    for x in sl.get("inboxes") or []:
        e = _low(x.get("email"))
        s = by.get(e) or {}
        if "@" not in e or s.get("state") == "gone":
            continue
        out.append({"email": e, "domain": e.rsplit("@", 1)[1], "client": s.get("client") or x.get("client") or "",
                    "campaigns": list(s.get("campaigns") or []), "state": s.get("state") or "in no campaign",
                    "account": x.get("account") or "", "disconnected": x.get("disconnected")})
    return out


def removed(facts: dict | None, inboxes: list[dict]) -> list[dict]:
    """The Smartlead inboxes (``inboxes``: Mongo inbox_status docs, email/domain/client/campaigns/state) their provider
    removed: [{email, domain, client, provider, why, live}]. An inbox any provider still lists is never in it. Pure."""
    facts = facts or {}
    sm = facts.get("scaledmail") or {}
    zap = facts.get("zapmail") or {}
    sm_ok, zap_ok = sm.get("read") is True, zap.get("read") is True
    plan = sm.get("plan") or {}
    registered = set(sm.get("registered") or ())
    zdoms = zap.get("domains") or {}
    zboxes = set(zap.get("mailboxes") or ())
    past = set(zap.get("past") or ())
    on_domain: dict[str, int] = defaultdict(int)
    for e in zboxes:
        on_domain[e.rsplit("@", 1)[-1]] += 1
    out = []
    for r in inboxes or []:
        email = _low(r.get("email"))
        client = CLIENT_SPELLING.get(str(r.get("client") or ""), str(r.get("client") or ""))
        if "@" not in email or r.get("state") == "gone" or client not in CLIENTS:
            continue
        dom = _low(r.get("domain")) or email.rsplit("@", 1)[1]
        if zap_ok and email in zboxes:
            continue                                     # Zapmail still has it
        if sm_ok and plan.get(dom) not in (None, "Cancelled"):
            continue                                     # on a ScaledMail plan that is not cancelled
        why, provider = None, None
        if sm_ok and plan.get(dom) == "Cancelled":
            why, provider = SM_CANCELLED, "ScaledMail"
        elif sm_ok and zap_ok and dom in registered and dom not in plan and dom not in zdoms:
            # registered at ScaledMail with no inbox plan, and not hosted at Zapmail (Zapmail read, so this is known)
            why, provider = SM_NO_PLAN, "ScaledMail"
        elif zap_ok and dom in zdoms and str(zdoms[dom].get("status") or "").upper() != EXPIRED:
            assigned = zdoms[dom].get("assigned")
            if assigned == 0 and on_domain[dom] == 0:
                why, provider = ZAP_EMPTY, "Zapmail"
            elif assigned is not None and 0 < assigned <= on_domain[dom]:
                why, provider = ZAP_GONE, "Zapmail"       # Zapmail listed every mailbox the domain has
        elif zap_ok and r.get("disconnected") is True:
            # a lost domain (t353): expired at Zapmail, or filed under Zapmail and in no Zapmail account now. Only
            # with Smartlead showing the inbox disconnected, so a working inbox is never listed.
            if dom in zdoms and str(zdoms[dom].get("status") or "").upper() == EXPIRED:
                why, provider = ZAP_EXPIRED, "Zapmail"
            elif dom not in zdoms and dom in past:
                why, provider = ZAP_LEFT, "Zapmail"
        if why:
            out.append({"email": email, "domain": dom, "client": client, "account": str(r.get("account") or ""),
                        "provider": provider, "why": why, "live": list(r.get("campaigns") or [])})
    return sorted(out, key=lambda x: (x["provider"], x["client"], x["domain"], x["email"]))


def whole_domains(rows: list[dict], inboxes: list[dict]) -> set[str]:
    """Domains every Smartlead inbox of which is removed (so a line naming only the domain is about removed ones)."""
    gone = {r["email"] for r in rows}
    by_dom: dict[str, set] = defaultdict(set)
    for r in inboxes or []:
        if r.get("state") != "gone" and "@" in _low(r.get("email")):
            by_dom[_low(r.get("domain")) or _low(r["email"]).rsplit("@", 1)[1]].add(_low(r["email"]))
    return {d for d, es in by_dom.items() if es and es <= gone}


# --- the digest ask (pure) -------------------------------------------------------------------------------------------

def _n(k: int, one: str = "inbox", many: str = "inboxes") -> str:
    return f"{k} {one if k == 1 else many}"


def ask_lines(rows: list[dict], whole: set[str], prefix: str = "") -> list[str]:
    """One line per provider and client, its domains with how many inboxes each (whole domain: search the domain and
    delete what it shows; otherwise the inboxes are named), and those still in a sending campaign."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r["provider"], r["client"], r["why"])].append(r)
    out = []
    for (provider, client, why), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], -len(kv[1]), kv[0][1])):
        by_dom: dict[str, list[dict]] = defaultdict(list)
        for r in rs:
            by_dom[r["domain"]].append(r)
        parts = []
        for dom, ds in sorted(by_dom.items()):
            part = f"{dom} {len(ds)}" if dom in whole else f"{dom} {', '.join(sorted(d['email'].split('@')[0] + '@' for d in ds))}"
            live = sum(1 for d in ds if d["live"])
            if live:
                part += f" ({live} still in a sending campaign)"
            parts.append(part)
        out.append(f"• {prefix}{client} {len(rs)} at {provider} ({why}, {_n(len(by_dom), 'domain', 'domains')}): "
                   + ", ".join(parts))
    return out


def use_fresh(facts: dict | None, now: datetime) -> dict | None:
    """Today's campaign and reply check (read_use), or None when it is missing, failed or older than USE_MAX_AGE."""
    use = (facts or {}).get("use") or {}
    at = _parse((facts or {}).get("use_at"))
    if use.get("read") is not True or not at or now - at > USE_MAX_AGE:
        return None
    return use


def split(rows: list[dict], use: dict | None) -> tuple[list[dict], list[dict]]:
    """(delete now, check first) of the removed inboxes (t353). ``use``: read_use's facts (None: not checked). A
    check-first row carries "campaigns" (the real campaigns it is still in, 'name (status)') and "notes" (a lead reply
    in the last 30 days, or what could not be checked). Delete now only when everything was checked and nothing was
    found. Pure."""
    from smartlead.v2_results import is_test_campaign
    seen = (use or {}).get("inboxes") or {}
    now_rows, check = [], []
    for r in rows:
        u = seen.get(r["email"]) if use else None
        camps: dict[str, str] = {}
        for c in (u or {}).get("campaigns") or []:
            name = str(c.get("name") or "").strip()
            if name and not is_test_campaign(name):
                camps[name] = str(c.get("status") or "").lower() or "status not known"
        for name in r.get("live") or []:                 # the sync's sending campaigns
            if str(name).strip() and not is_test_campaign(str(name)):
                camps.setdefault(str(name).strip(), "sending")
        notes = []
        if u is None:
            notes.append(NOT_CHECKED)
        else:
            if u.get("campaigns_unread"):
                notes.append(NO_CAMPAIGNS)
            if int(u.get("replied") or 0):
                notes.append(REPLIED)
            elif u.get("replies_unread"):
                notes.append(NO_REPLY_CHECK)
        if camps or notes:
            check.append({**r, "campaigns": [f"{n} ({st})" for n, st in sorted(camps.items())], "notes": notes})
        else:
            now_rows.append(r)
    return now_rows, check


def _where_check(r: dict) -> str:
    if r["campaigns"]:
        return "in " + ", ".join(r["campaigns"])
    if NOT_CHECKED in r["notes"]:
        return "with campaigns and replies not checked"
    if NO_CAMPAIGNS in r["notes"]:
        return "with campaigns not read"
    return "in no real campaign"


def check_lines(rows: list[dict], prefix: str = "") -> list[str]:
    """One line per client and where the inboxes are (their campaigns), each inbox with its own note (a lead reply in
    the last 30 days, replies not checked)."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r["client"], _where_check(r))].append(r)
    out = []
    for (client, where), rs in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        names = []
        for r in sorted(rs, key=lambda x: x["email"]):
            own = [n for n in r["notes"] if n in (REPLIED, NO_REPLY_CHECK) or (n == NO_CAMPAIGNS and r["campaigns"])]
            names.append(r["email"] + (f" ({'; '.join(own)})" if own else ""))
        out.append(f"• {prefix}{client} {len(rs)} {where}: " + ", ".join(names))
    return out


def _clients(rows: list[dict]) -> str:
    k: dict[str, int] = defaultdict(int)
    for r in rows:
        k[r["client"]] += 1
    return ", ".join(f"{c} {n}" for c, n in sorted(k.items(), key=lambda x: (-x[1], x[0])))


def _why_check(rows: list[dict]) -> str:
    why = []
    if any(r["campaigns"] for r in rows):
        why.append("still in a real campaign")
    if any(REPLIED in r["notes"] for r in rows):
        why.append("a lead replied in the last 30 days")
    if any(n in r["notes"] for r in rows for n in (NOT_CHECKED, NO_CAMPAIGNS, NO_REPLY_CHECK)):
        why.append("not fully checked")
    return " or ".join(why)


def ask(rows: list[dict], whole: set[str], check: list[dict] = ()):
    """The one delete reminder, or None: ``rows`` to delete now, ``check`` to check first (split), ``whole``: domains
    every Smartlead inbox of which is to be deleted now. Never words that read as a login or disconnect ask."""
    from smartlead import digest as dg
    check = list(check or ())
    if not rows and not check:
        return None
    d, c = len(rows), len(check)
    k = d + c
    head = f"{_n(k)} {'was' if k == 1 else 'were'} removed by {'its' if k == 1 else 'their'} provider"
    it = "it" if k == 1 else "them"
    if not check:
        text = f"{head}; delete {it} from Smartlead by hand ({_clients(rows)})."
    elif not rows:
        text = f"{head}; check {it} before deleting from Smartlead by hand ({_clients(check)}): {_why_check(check)}."
    else:
        text = (f"{head}: delete {d} from Smartlead by hand now ({_clients(rows)}), check {c} first "
                f"({_clients(check)}): {_why_check(check)}.")
    step = ("Delete the \"Delete now\" inboxes by hand in Smartlead, Email Accounts (the client's account): search "
            "each domain listed; a domain with a count goes whole, one with names only those. ") if rows else ""
    if check:
        step += ("A \"Check first\" inbox has its reason beside it: settle that campaign or reply before deleting "
                 "it. ")
    step += "The bot never deletes; an inbox leaves this list once it is gone."
    detail = ask_lines(rows, whole, "Delete now: " if check else "") + check_lines(check, "Check first: ")
    return dg.Ask(text, step, key=KEY, detail=detail)


# --- Mongo and the providers (I/O) -----------------------------------------------------------------------------------

def save_facts(db, facts: dict, now: datetime | None = None) -> bool:
    """Write today's provider facts. A provider that could not be read keeps its last facts. Never raises."""
    now = now or datetime.now(timezone.utc)
    try:
        old = db[COLLECTION].find_one({"_id": DOC_ID}) or {}
        doc = {"_id": DOC_ID, "at": now.isoformat(timespec="seconds")}
        for p in ("scaledmail", "zapmail", "smartlead"):
            new = facts.get(p) or {}
            doc[p] = new if new.get("read") is True else (old.get(p) or new)
            doc[f"{p}_at"] = doc["at"] if new.get("read") is True else old.get(f"{p}_at")
        doc["use"], doc["use_at"] = old.get("use"), old.get("use_at")      # the campaign/reply check (save_use)
        db[COLLECTION].replace_one({"_id": DOC_ID}, doc, upsert=True)
        return True
    except Exception as exc:  # noqa: BLE001 - the facts are a help, the job goes on
        print(f"  [Provider facts] not saved ({type(exc).__name__})")
        return False


def _parse(ts) -> datetime | None:
    try:
        d = datetime.fromisoformat(str(ts))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def load(db, now: datetime | None = None) -> tuple[dict | None, list[dict]]:
    """(provider facts, each part unused when older than MAX_AGE; today's Smartlead inboxes, inboxes_of). Never
    raises."""
    now = now or datetime.now(timezone.utc)
    try:
        facts = db[COLLECTION].find_one({"_id": DOC_ID})
        for p in ("scaledmail", "zapmail", "smartlead"):     # each part only while fresh
            at = _parse((facts or {}).get(f"{p}_at"))
            if facts and (not at or now - at > MAX_AGE):
                facts[p] = {"read": False, "why": "facts too old"}
        status = list(db["inbox_status"].find({}, {"_id": 0, "email": 1, "domain": 1, "client": 1, "campaigns": 1,
                                                    "state": 1}))
        return facts, inboxes_of(facts, status)
    except Exception as exc:  # noqa: BLE001 - nothing judged removed today
        print(f"  [Provider facts] not read ({type(exc).__name__})")
        return None, []


def emails(db, now: datetime | None = None) -> set[str]:
    """The removed inboxes, for jobs that leave them out (fleet watch). Empty when not known. Never raises."""
    facts, inboxes = load(db, now)
    return {r["email"] for r in removed(facts, inboxes)}


def source(db, now: datetime | None = None) -> dict | None:
    """The digest source for the delete reminder: {_id, at, asks: [ask doc + inboxes + domains]}, or None. The ask
    carries every removed inbox and whole domain, so the digest can leave them out of every other ask."""
    now = now or datetime.now(timezone.utc)
    facts, inboxes = load(db, now)
    rows = removed(facts, inboxes)
    now_rows, check = split(rows, use_fresh(facts, now))
    a = ask(now_rows, whole_domains(now_rows, inboxes), check)
    if a is None:
        return None
    doc = a.doc(SOURCE)
    doc.update(since=facts.get("at"), inboxes=sorted(r["email"] for r in rows),
               domains=sorted(whole_domains(rows, inboxes)),
               clients={c: sum(1 for r in rows if r["client"] == c) for c in sorted({r["client"] for r in rows})},
               delete_now=len(now_rows), check_first=len(check),
               where="",                     # the text says where (digest_post._where): no list heads after it
               lines=LIST_LINES)             # a working list: up to 20 lines in the thread, not 12
    return {"_id": SOURCE, "at": facts.get("at") or now.isoformat(), "asks": [doc]}


def read_scaledmail(sm) -> dict:
    """ScaledMail facts from an open ScaledMailClient (GET only). Never raises."""
    try:
        return scaledmail_facts(sm.orders(), sm.domains(), sm.purchased_domains())
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}


async def read_smartlead() -> dict:
    """Every Smartlead inbox of every account (GET /email-accounts, paged; the key-safe client). Never raises."""
    try:
        from smartlead.accounts import discover_accounts
        from smartlead.api import SmartleadClient
        from smartlead.manager_map import resolve_sub_client     # the sync's rule (processing.py)
        rows, errors = [], []
        for acc in discover_accounts():
            try:
                async with SmartleadClient(acc.api_key, acc.name) as c:
                    accs = await c.list_email_accounts()
            except Exception as exc:  # noqa: BLE001 - no URL or key in the message
                errors.append(f"{acc.name}: {type(exc).__name__}")
                continue
            for a in accs or []:
                email = str(a.get("from_email") or "")
                client = resolve_sub_client(acc.name, a.get("client_id"), email)
                rows.append({"email": email, "client": CLIENT_SPELLING.get(client, client), "account": acc.name,
                             "disconnected": not (a.get("is_smtp_success") and a.get("is_imap_success"))})
        return smartlead_facts(rows, errors)
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}


def tracked_zapmail(assets) -> list[str]:
    """Domains the asset tracker (Mongo assets, ``/infra``) files under Zapmail. Pure."""
    return sorted({_low(a.get("name")) for a in assets or [] if "zapmail" in _low(a.get("provider"))
                   and "@" not in _low(a.get("name")) and _low(a.get("name"))})


async def read_zapmail() -> dict:
    """Zapmail facts from every configured account (GET only), with the domains the asset tracker files under Zapmail
    (none when the tracker can't be read). Never raises."""
    try:
        from smartlead.zapmail_fleet import all_domains, all_mailboxes
        errors: list[str] = []
        domains = await all_domains(errors)
        mailboxes = await all_mailboxes(errors)
        try:
            from smartlead.zapmail_asset_sync import read_tracker
            past = tracked_zapmail((await asyncio.to_thread(read_tracker)).values())
        except Exception:  # noqa: BLE001 - no tracker, no lost-domain evidence
            past = []
        return zapmail_facts(domains, mailboxes, errors, past)
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}


# --- delete now or check first: campaigns and replies (I/O, t353) ---------------------------------------------------

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


class OutOfTime(Exception):
    """The check ran past BUDGET_S: what is left is "not checked"."""


def senders_of(history) -> set[str]:
    """Every inbox in a lead's thread: the sender of each SENT email and the address each REPLY went to. Pure."""
    rows = (history.get("history") if isinstance(history, dict) else history) or []
    out: set[str] = set()
    for m in rows:
        kind = str(m.get("type") or "").upper()
        field = m.get("from") if kind == "SENT" else m.get("to") if kind == "REPLY" else None
        out |= {e.lower() for e in _EMAIL.findall(str(field or ""))}
    return out


def sender_index(docs) -> dict[tuple, set[str]]:
    """{(account, campaign id, stats_id): inboxes in that lead's thread} from Mongo lead_senders (inbox_evidence's
    cache: {_id: account:campaign:lead id, sids: {stats_id: inbox}}), so a reply it already placed costs no call."""
    out: dict[tuple, set[str]] = {}
    for d in docs or []:
        parts = str(d.get("_id") or "").rsplit(":", 2)
        sids = d.get("sids") or {}
        if len(parts) != 3 or not isinstance(sids, dict):
            continue
        who = {_low(v) for v in sids.values() if _low(v)}
        for sid in sids:
            out[(parts[0], parts[1], str(sid))] = who
    return out


def _when(ts) -> datetime | None:
    return _parse(str(ts).replace("Z", "+00:00")) if ts else None


async def account_use(get, account: str, want: set[str], cut: datetime, senders: dict,
                      counts: dict | None = None) -> dict:
    """{inbox: {campaigns: [{name, status}], replied, campaigns_unread, replies_unread}} for the removed inboxes of
    one Smartlead account. ``get(path, params)``: a paced GET that raises on failure (OutOfTime past the budget).
    Campaigns: every non-test campaign not finished. Replies: every non-test campaign's replied statistics with a
    reply since ``cut``; the thread's inboxes from ``senders`` or the lead's message history. Any read that fails
    marks every inbox of the account (a pulled inbox can't be told apart), so none is called clean on a guess.
    A reply whose lead Smartlead answers it no longer has (an empty lead lookup: 3 BettrData leads of completed
    campaigns on 20 Nov, in no campaign's lead list either) has no thread left to lose: counted in ``counts``
    ("lead_gone"), not a failure."""
    counts = counts if counts is not None else {}
    from smartlead.v2_results import is_test_campaign
    out = {e: {"campaigns": [], "replied": 0, "campaigns_unread": False, "replies_unread": False} for e in want}

    def mark(*fields):
        for e in want:
            for f in fields:
                out[e][f] = True

    try:
        camps = [c for c in await get("/campaigns") or [] if isinstance(c, dict) and c.get("id") is not None
                 and not is_test_campaign(c.get("name"))]
        for c in camps:
            if str(c.get("status") or "").upper() in FINISHED:
                continue
            try:
                accs = await get(f"/campaigns/{c['id']}/email-accounts")
            except OutOfTime:
                raise
            except Exception:  # noqa: BLE001 - no URL or key in any text kept
                mark("campaigns_unread")
                continue
            for a in accs if isinstance(accs, list) else []:
                e = _low(a.get("from_email"))
                if e in out:
                    out[e]["campaigns"].append({"name": str(c.get("name") or ""), "status": str(c.get("status") or "")})
        hits = []
        for c in camps:
            off = 0
            while True:
                try:
                    b = await get(f"/campaigns/{c['id']}/statistics", {"email_status": "replied", "limit": 100,
                                                                      "offset": off})
                except OutOfTime:
                    raise
                except Exception:  # noqa: BLE001
                    mark("replies_unread")
                    break
                data = (b.get("data") if isinstance(b, dict) else b) or []
                for r in data:
                    t = _when(r.get("reply_time"))
                    if t and t >= cut:
                        hits.append((str(c["id"]), _low(r.get("lead_email")), str(r.get("stats_id") or "")))
                if len(data) < 100 or off >= 5000:
                    break
                off += 100
        for cid, lead, sid in hits:
            who = senders.get((account, cid, sid))
            if who is None:
                try:
                    found = await get("/leads/", {"email": lead})
                    if not isinstance(found, dict):
                        raise LookupError("unexpected lead answer")
                    lid = found.get("id")
                    if not lid:
                        if found:
                            raise LookupError("lead answer without an id")
                        counts["lead_gone"] = counts.get("lead_gone", 0) + 1      # deleted from Smartlead
                        continue
                    who = senders_of(await get(f"/campaigns/{cid}/leads/{lid}/message-history"))
                except OutOfTime:
                    raise
                except Exception:  # noqa: BLE001
                    mark("replies_unread")
                    continue
            for e in who & want:
                out[e]["replied"] += 1
    except OutOfTime:
        mark("campaigns_unread", "replies_unread")
    except Exception:  # noqa: BLE001 - the campaign list itself
        mark("campaigns_unread", "replies_unread")
    return out


class _Pace:
    """At least ``gap`` seconds between calls (Smartlead's 200 a minute is shared by every job on a key)."""

    def __init__(self, gap: float, clock=time.monotonic, sleep=asyncio.sleep):
        self.gap, self.clock, self.sleep, self.last = gap, clock, sleep, None

    async def wait(self):
        if self.last is not None:
            d = self.last + self.gap - self.clock()
            if d > 0:
                await self.sleep(d)
        self.last = self.clock()


async def read_use(rows: list[dict], now: datetime | None = None, senders: dict | None = None,
                   budget_s: float = BUDGET_S) -> dict:
    """{"read": True, "inboxes": {inbox: account_use's facts}, "calls": n} for the removed inboxes (``rows``, each with
    its Smartlead account). An inbox whose account is not configured is left out (split: not checked). GETs only, the
    key-safe client (no URL or key in any error). Never raises."""
    now = now or datetime.now(timezone.utc)
    cut = now - timedelta(days=REPLY_DAYS)
    want: dict[str, set] = defaultdict(set)
    for r in rows or []:
        if r.get("account"):
            want[r["account"]].add(r["email"])
    out = {"read": True, "inboxes": {}, "calls": 0, "lead_gone": 0}
    if not want:
        return out
    try:
        from smartlead.accounts import discover_accounts
        from smartlead.api import SmartleadClient
        accounts = discover_accounts()
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}
    deadline = time.monotonic() + budget_s
    pace = _Pace(PACE_S)
    for acc in accounts:
        mine = want.get(acc.name)
        if not mine:
            continue
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                async def get(path, params=None, c=c):
                    if time.monotonic() > deadline:
                        raise OutOfTime()
                    await pace.wait()
                    out["calls"] += 1
                    return await c._get(path, params)
                out["inboxes"].update(await account_use(get, acc.name, mine, cut, senders or {}, out))
        except Exception:  # noqa: BLE001 - the account's inboxes stay "not checked"
            continue
    return out


def save_use(db, use: dict, now: datetime | None = None) -> bool:
    """Keep today's campaign/reply check beside the provider facts. Never raises."""
    now = now or datetime.now(timezone.utc)
    try:
        doc = db[COLLECTION].find_one({"_id": DOC_ID}) or {"_id": DOC_ID}
        doc.update(use=use, use_at=now.isoformat(timespec="seconds"))
        db[COLLECTION].replace_one({"_id": DOC_ID}, doc, upsert=True)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [Removed inboxes] check not saved ({type(exc).__name__})")
        return False


async def check_use(db, now: datetime | None = None, budget_s: float = BUDGET_S) -> dict | None:
    """Once a day (zapmail_digest, after the provider facts): which removed inboxes are still in a real campaign or
    had a lead reply in the last 30 days. Skips when today's check is already saved. Never raises."""
    now = now or datetime.now(timezone.utc)
    try:
        doc = db[COLLECTION].find_one({"_id": DOC_ID}) or {}
        at = _parse(doc.get("use_at"))
        if at and now - at < timedelta(hours=20) and (doc.get("use") or {}).get("read") is True:
            print("  [Removed inboxes] campaigns and replies already checked today")
            return doc["use"]
        facts, inboxes = load(db, now)
        rows = removed(facts, inboxes)
        try:
            senders = sender_index(db["lead_senders"].find({}, {"sids": 1}))
        except Exception:  # noqa: BLE001 - every reply then needs its history read
            senders = {}
        t0 = time.monotonic()
        use = await read_use(rows, now, senders, budget_s)
        save_use(db, use, now)
        now_rows, check = split(rows, use if use.get("read") else None)
        print(f"  [Removed inboxes] {len(rows)} removed: {len(now_rows)} delete now, {len(check)} check first "
              f"({use.get('calls', 0)} Smartlead calls, {time.monotonic() - t0:.0f} s)")
        return use
    except Exception as exc:  # noqa: BLE001
        print(f"  [Removed inboxes] campaigns and replies not checked ({type(exc).__name__})")
        return None
