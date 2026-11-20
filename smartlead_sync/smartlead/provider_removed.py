"""Inboxes their provider has removed, and the one digest ask to delete them from Smartlead by hand (t352, 20 Nov 2026).

On 20 Nov the digest told Manveen to reconnect the login of 3 inboxes on a ScaledMail order we had cancelled:
ScaledMail had removed the mailboxes, so there was nothing to reconnect. Manveen: "sure then if they are rmoved
don't remind me just remid me to delte make sure you are up to mark".

An inbox is "removed by its provider" only when the provider's own data says so, never from a login failure:

  * ScaledMail: its domain is registered at ScaledMail but has no inbox plan there and is not at Zapmail (the order
    was cancelled and ScaledMail took the mailboxes away), or its domain is listed with an order whose status is
    Cancelled;
  * Zapmail: its domain is one we still hold in a Zapmail account (not expired) and the mailbox is not on it (the
    domain has no mailboxes, or Zapmail lists every mailbox the domain has and this one is not among them). An expired
    domain is not used as evidence.

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

The bot never deletes an inbox: a Smartlead delete is immediate and permanent, so it is done by hand only.
"""
from __future__ import annotations

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


def zapmail_facts(domains: list[dict] | None, mailboxes: list[dict] | None, errors=()) -> dict:
    """{"read": True, "domains": {domain: {status, assigned, account}}, "mailboxes": [email]} from
    zapmail_fleet.all_domains / all_mailboxes. ``read`` False when a list failed on any account. Pure."""
    if domains is None or mailboxes is None or errors:
        return {"read": False, "why": "not read in full"}
    doms = {}
    for d in domains:
        name = _low(d.get("domain"))
        if name:
            doms[name] = {"status": str(d.get("status") or ""), "assigned": _int(d.get("assigned_mailboxes")),
                          "account": str(d.get("account") or "")}
    return {"read": True, "domains": doms, "mailboxes": sorted({_low(m.get("email")) for m in mailboxes if m.get("email")})}


def smartlead_facts(rows: list[dict] | None, errors=()) -> dict:
    """{"read": True, "inboxes": [{email, client}]} from every Smartlead account's email accounts; ``read`` False when
    an account could not be read (then the sync's inbox_status is used alone). Pure."""
    if rows is None or errors:
        return {"read": False, "why": "not read in full"}
    seen = {}
    for r in rows:
        e = _low(r.get("email"))
        if "@" in e:
            seen.setdefault(e, {"email": e, "client": str(r.get("client") or "")})
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
                    "campaigns": list(s.get("campaigns") or []), "state": s.get("state") or "in no campaign"})
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
        if why:
            out.append({"email": email, "domain": dom, "client": client,
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


def ask_lines(rows: list[dict], whole: set[str]) -> list[str]:
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
        out.append(f"• {client} {len(rs)} at {provider} ({why}, {_n(len(by_dom), 'domain', 'domains')}): "
                   + ", ".join(parts))
    return out


def ask(rows: list[dict], whole: set[str]):
    """The one delete reminder, or None. Never words that read as a login or disconnect ask."""
    from smartlead import digest as dg
    if not rows:
        return None
    k = len(rows)
    return dg.Ask(f"{_n(k)} {'was' if k == 1 else 'were'} removed by {'its' if k == 1 else 'their'} provider; delete "
                  f"{'it' if k == 1 else 'them'} from Smartlead by hand.",
                  "Delete them by hand in Smartlead, Email Accounts (the client's account): search each domain "
                  "listed; a domain with a count goes whole, one with names only those. Or run the matching "
                  "CHROME-PROMPT-delete file in Claude in Chrome. The bot never deletes; an inbox leaves this list "
                  "once it is gone.",
                  key=KEY, detail=ask_lines(rows, whole))


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
    a = ask(rows, whole_domains(rows, inboxes))
    if a is None:
        return None
    doc = a.doc(SOURCE)
    doc.update(since=facts.get("at"), inboxes=sorted(r["email"] for r in rows),
               domains=sorted(whole_domains(rows, inboxes)),
               clients={c: sum(1 for r in rows if r["client"] == c) for c in sorted({r["client"] for r in rows})})
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
                rows.append({"email": email, "client": CLIENT_SPELLING.get(client, client)})
        return smartlead_facts(rows, errors)
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}


async def read_zapmail() -> dict:
    """Zapmail facts from every configured account (GET only). Never raises."""
    try:
        from smartlead.zapmail_fleet import all_domains, all_mailboxes
        errors: list[str] = []
        domains = await all_domains(errors)
        mailboxes = await all_mailboxes(errors)
        return zapmail_facts(domains, mailboxes, errors)
    except Exception as exc:  # noqa: BLE001
        return {"read": False, "why": type(exc).__name__}
