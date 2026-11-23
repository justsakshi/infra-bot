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
GET campaigns, each unfinished campaign's email-accounts, and, only when an inbox is in no real campaign, each non-test
campaign's replied statistics and the message history of each reply in the window that Mongo lead_senders
(inbox_evidence's cache) or this check's own cache (REPLY_CACHE) cannot place. Paced at half of Smartlead's shared 200
a minute, with a time limit.

A check that could not finish never moves an inbox (t386, 23 Nov 2026). On 23 Nov the check ran out of its 15 minutes
on the PL account after 921 calls (it read every reply of the last 30 days twice over, at up to 171 calls a minute on
the key every job shares, so the 09:45 key check met a 429), marked all 97 PL inboxes "not read", and the digest moved
them from "delete now" to "check first": 68/44 on 20 Nov became 8/104, with 63 full addresses each followed by "(could
not check replies)". Now: an inbox already in a real campaign needs no reply check (it is "check first" either way),
each reply's thread is looked up once and kept, the pace leaves half the limit to other jobs, and an inbox today's check
could not finish keeps its last complete result (up to KEEP_USE old) with its date; one with none is counted in one
"not checked" line, never listed address by address.

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
ZAP_EMPTY = "every inbox on the domain deleted there"     # t386: readers took "no inboxes left on the domain" for
ZAP_GONE = "the inbox deleted there"                      # Smartlead's view, where the domain still had inboxes
ZAP_EXPIRED = "domain expired"                        # t353: with Smartlead showing the inbox disconnected
ZAP_LEFT = "domain no longer at Zapmail"              # tracker says Zapmail, no Zapmail account has it

# Delete now or check first (t353)
FINISHED = {"COMPLETED", "STOPPED", "ARCHIVED"}       # campaigns that cannot send again: membership not read
REPLY_DAYS = 30
USE_MAX_AGE = timedelta(hours=36)                     # the campaign/reply check is daily; older says "as of <day>"
KEEP_USE = timedelta(days=8)                          # an inbox's last complete check is used this long (t386)
PACE_S = 0.6                                          # at most 100 calls a minute: half of Smartlead's shared 200, so
                                                      # the 09:45 key check, 09:50 jobs and the PC keep the other half
BUDGET_S = 15 * 60                                    # 09:40 job: done before the 10:00 sync shares the limit
REPLY_CACHE = "removed_reply_threads"                 # {_id: account:campaign:stats_id, who: [inbox], at}: each reply's
REPLY_CACHE_DAYS = 40                                 # thread is read once, not every morning (t386)
REPLIED = "lead reply in the last 30 days"
NO_REPLY_CHECK = "could not check replies"
NO_CAMPAIGNS = "campaigns not read"
NOT_CHECKED = "campaigns and replies not checked"
UNCHECKED = (NOT_CHECKED, NO_CAMPAIGNS, NO_REPLY_CHECK)
LIST_LINES = 30                                       # lines of this ask's list in the digest thread (digest_post):
                                                      # a working list, in a reply of its own on a long day (t386)
PER_LINE = 6                                          # domains or addresses named on one list line (digest_guard)
WHY_WORDS = {"rate limit": "Smartlead's rate limit", "out of time": "out of time",
             "error": "a Smartlead error", "not configured": "no key for the account"}

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


def domain_lines(rows: list[dict], whole: set[str]) -> list[str]:
    """The inboxes of one list group as short lines (t386): domains that go whole as 'a.co (3), b.co (2)', at most
    PER_LINE a line; a domain that goes only in part names its inboxes, at most PER_LINE a line. Never a whole list of
    full addresses on one line (23 Nov 2026: 63 of them in one line, split by Slack across 3 posts)."""
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_dom[r["domain"]].append(r)
    out, wholes = [], [f"{d} ({len(ds)})" for d, ds in sorted(by_dom.items()) if d in whole]
    for i in range(0, len(wholes), PER_LINE):
        out.append(("– all inboxes on these domains: " if i == 0 else "– ") + ", ".join(wholes[i:i + PER_LINE]))
    for dom, ds in sorted(by_dom.items()):
        if dom in whole:
            continue
        users = sorted(d["email"].split("@")[0] + "@" for d in ds)
        for i in range(0, len(users), PER_LINE):
            out.append(f"– {dom}: " + ", ".join(users[i:i + PER_LINE]))
    return out


def ask_lines(rows: list[dict], whole: set[str], prefix: str = "") -> list[str]:
    """One heading per provider, client and reason, then its domains (domain_lines): a whole domain is searched and
    deleted at once; otherwise the inboxes are named. Inboxes still in a sending campaign are counted in the heading."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r["provider"], r["client"], r["why"])].append(r)
    out = []
    for (provider, client, why), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], -len(kv[1]), kv[0][1])):
        live = sum(1 for d in rs if d["live"])
        out.append(f"• {prefix}{client} {len(rs)}, removed by {provider} ({why}"
                   + (f"; {live} still in a sending campaign" if live else "") + "):")
        out += domain_lines(rs, whole)
    return out


def use_fresh(facts: dict | None, now: datetime) -> dict | None:
    """The campaign and reply check (read_use, merged by save_use), or None when it is missing, failed or older than
    KEEP_USE. Each inbox's own "at" says how old its part is (split marks an older one with "as_of")."""
    use = (facts or {}).get("use") or {}
    at = _parse((facts or {}).get("use_at"))
    if use.get("read") is not True or not at or now - at > KEEP_USE:
        return None
    return {**use, "_at": use.get("_at") or (facts or {}).get("use_at")}


def complete(u: dict | None) -> bool:
    """True when an inbox's check finished (its campaigns and, where needed, its replies were read)."""
    return bool(u) and not u.get("campaigns_unread") and not u.get("replies_unread")


def split(rows: list[dict], use: dict | None, now: datetime | None = None) -> tuple[list[dict], list[dict]]:
    """(delete now, check first) of the removed inboxes (t353). ``use``: read_use's facts (None: not checked). A
    check-first row carries "campaigns" (the real campaigns it is still in, 'name (status)') and "notes" (a lead reply
    in the last 30 days, or what could not be checked). Delete now only when everything was checked and nothing was
    found. An inbox whose last complete check is older than USE_MAX_AGE (today's could not finish) carries "as_of",
    the day of that check. Pure."""
    from smartlead.v2_results import is_test_campaign
    seen = (use or {}).get("inboxes") or {}
    now_rows, check = [], []
    for r in rows:
        u = seen.get(r["email"]) if use else None
        at = _parse((u or {}).get("at") or (use or {}).get("_at"))
        if u and now and at and now - at > USE_MAX_AGE:
            r = {**r, "as_of": at.isoformat()}
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
        st = {c.rsplit(" (", 1)[-1] for c in r["campaigns"]}
        if len(st) == 1 and len(r["campaigns"]) > 1:      # 'in A, B and C (paused)': the status said once (t386)
            names = [c.rsplit(" (", 1)[0] for c in r["campaigns"]]
            return "in " + ", ".join(names[:-1]) + " and " + names[-1] + " (" + st.pop()
        return "in " + ", ".join(r["campaigns"])
    if NOT_CHECKED in r["notes"]:
        return "with campaigns and replies not checked"
    if NO_CAMPAIGNS in r["notes"]:
        return "with campaigns not read"
    return "in no real campaign"


def unchecked(r: dict) -> bool:
    """A check-first row whose only reason is that its check did not finish (no campaign, no reply found)."""
    return not r["campaigns"] and REPLIED not in r["notes"]


def check_lines(rows: list[dict], prefix: str = "", whole: set[str] = frozenset(), others=()) -> list[str]:
    """One heading per client, where the inboxes are (their campaigns) and why, then their inboxes by domain
    (domain_lines). A reason is said once per group, never after each address (t386). A domain whose every Smartlead
    inbox is removed (``whole``) and all in this one group (none in ``others``, the delete-now rows) goes as 'a.co (3)'.
    Rows that are "check first" only because their check did not finish are left to unchecked_lines."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if unchecked(r):
            continue
        why = "a lead replied in the last 30 days" if REPLIED in r["notes"] else ""
        groups[(r["client"], _where_check(r), why)].append(r)
    out = []
    for (client, where, why), rs in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        out.append(f"• {prefix}{client} {len(rs)} {where}" + (f", {why}" if why else "") + ":")
        mine = {r["email"] for r in rs}
        alone = {d for d in whole if all(r["email"] in mine for r in list(rows) + list(others) if r["domain"] == d)}
        out += domain_lines(rs, alone)
    return out


def _day(ts) -> str:
    d = _parse(ts)
    if not d:
        return ""
    d = d.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return f"{d:%a} {d.day} {d:%b}"


def _client_why(rows: list[dict], errors: dict | None) -> dict[str, str]:
    """{client: why its check did not finish} from read_use's per-account errors (WHY_WORDS; "" when not known)."""
    out: dict[str, str] = {}
    for r in rows:
        why = (errors or {}).get(r.get("account") or "")
        out.setdefault(r["client"], WHY_WORDS.get(why, "") if why else "")
    return out


def unchecked_lines(check: list[dict], errors: dict | None = None) -> list[str]:
    """One line per client for inboxes with no complete check to go by (counted, never listed), and one per client
    whose lists use an earlier day's check because today's could not finish."""
    out = []
    rows = [r for r in check if unchecked(r)]
    why = _client_why(rows, errors)
    by: dict[str, int] = defaultdict(int)
    for r in rows:
        by[r["client"]] += 1
    for c, n in sorted(by.items(), key=lambda x: (-x[1], x[0])):
        out.append(f"• Not checked yet, {c} {n}: the campaign and reply check did not finish"
                   + (f" ({why[c]})" if why[c] else "") + " and no earlier check of these inboxes is kept, so none is "
                   "on a list to delete yet. Nothing to do for them today; the next morning's check places them.")
    return out


def stale_lines(rows: list[dict], errors: dict | None = None) -> list[str]:
    """'Precise Leads: today's check could not finish (Smartlead's rate limit); these lists use its check of Fri 20
    Nov.' once per client whose rows carry "as_of"."""
    old: dict[str, str] = {}
    for r in rows:
        if r.get("as_of"):
            old[r["client"]] = min(old.get(r["client"], r["as_of"]), r["as_of"])
    why = _client_why([r for r in rows if r.get("as_of")], errors)
    return [f"• {c}: today's check did not finish" + (f" ({why[c]})" if why[c] else "") + ", so its inboxes are "
            f"placed by its check of {_day(at)}." for c, at in sorted(old.items())]


def _clients(rows: list[dict]) -> str:
    k: dict[str, int] = defaultdict(int)
    for r in rows:
        k[r["client"]] += 1
    return ", ".join(f"{c} {n}" for c, n in sorted(k.items(), key=lambda x: (-x[1], x[0])))


def ask(rows: list[dict], whole: set[str], check: list[dict] = (), errors: dict | None = None,
        whole_all: set[str] = frozenset()):
    """The one delete reminder, or None: ``rows`` to delete now, ``check`` to check first (split), ``whole``: domains
    every Smartlead inbox of which is to be deleted now; ``errors``: read_use's per-account reasons a check did not
    finish. Never words that read as a login or disconnect ask. The sentence and the step are short (two Slack lines
    in the post, t386); how to delete and what each list means are in ``more``, shown in the thread."""
    from smartlead import digest as dg
    check = list(check or ())
    if not rows and not check:
        return None
    later = [r for r in check if unchecked(r)]
    check = [r for r in check if not unchecked(r)]
    d, c, u = len(rows), len(check), len(later)
    k = d + c + u
    head = f"{_n(k)} {'was' if k == 1 else 'were'} removed by {'its' if k == 1 else 'their'} provider and can't send again"
    todo = []
    if d:
        todo.append(f"delete {d} now ({_clients(rows)})")
    if c:
        todo.append(f"check {c} first ({_clients(check)})")
    text = f"{head}: {', '.join(todo)}" if todo else head
    if u:
        why = sorted({w for w in _client_why(later, errors).values() if w})
        text += f"{'; ' if todo else ': '}{u} not checked yet ({_clients(later)}" + (f"; {', '.join(why)}" if why else "") + ")"
    text += "."
    if d and c:
        step = ("delete the \"delete now\" ones by hand in Smartlead (Email Accounts); take each \"check first\" one "
                "out of the campaign named, then delete it.")
    elif d:
        step = "delete them by hand in Smartlead (Email Accounts); the list is in the thread."
    elif c:
        step = "take each one out of the campaign named in the thread, then delete it by hand in Smartlead."
    else:
        step = "nothing to delete yet: the next morning's check places them."
    more = []
    if d:
        more.append("Search each domain in the client's Smartlead account: under \"all inboxes on these domains\" "
                    "every inbox of the domain goes, otherwise only the inboxes named.")
    if c:
        if any(REPLIED in r["notes"] for r in check):
            more.append("A \"check first\" inbox a lead replied to: answer that lead before deleting it.")
    more.append("The bot never deletes; an inbox leaves this list once it is gone.")
    detail = (stale_lines(rows + check, errors) + ask_lines(rows, whole, "Delete now: ")
              + check_lines(check + later, "Check first: ", set(whole_all), rows) + unchecked_lines(later, errors))
    return dg.Ask(text, step, key=KEY, detail=detail, more=" ".join(more))


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
    use = use_fresh(facts, now)
    now_rows, check = split(rows, use, now)
    a = ask(now_rows, whole_domains(now_rows, inboxes), check, (use or {}).get("errors"), whole_domains(rows, inboxes))
    if a is None:
        return None
    doc = a.doc(SOURCE)
    doc.update(since=facts.get("at"), inboxes=sorted(r["email"] for r in rows),
               domains=sorted(whole_domains(rows, inboxes)),
               clients={c: sum(1 for r in rows if r["client"] == c) for c in sorted({r["client"] for r in rows})},
               delete_now=len(now_rows), check_first=sum(1 for r in check if not unchecked(r)),
               not_checked=sum(1 for r in check if unchecked(r)),
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


def why_failed(exc: BaseException) -> str:
    """A short reason a read failed, for the digest (WHY_WORDS): never the exception text (it can hold a URL)."""
    if isinstance(exc, OutOfTime):
        return "out of time"
    code = getattr(getattr(exc, "response", None), "status_code", None)
    return "rate limit" if code == 429 else "error"


async def account_use(get, account: str, want: set[str], cut: datetime, senders: dict,
                      counts: dict | None = None, threads: dict | None = None) -> dict:
    """{inbox: {campaigns: [{name, status}], replied, campaigns_unread, replies_unread}} for the removed inboxes of
    one Smartlead account. ``get(path, params)``: a paced GET that raises on failure (OutOfTime past the budget).
    Campaigns: every non-test campaign not finished. Replies, only when an inbox is in no real campaign (one in a
    campaign is "check first" either way, t386): every non-test campaign's replied statistics with a reply since
    ``cut``; the thread's inboxes from ``senders`` (inbox_evidence's cache), ``threads`` (this check's own cache, filled
    here with each new lookup) or the lead's message history. Any read that fails marks every inbox of the account (a
    pulled inbox can't be told apart), so none is called clean on a guess; ``counts["why"]`` says why (why_failed).
    A reply whose lead Smartlead answers it no longer has (an empty lead lookup: 3 BettrData leads of completed
    campaigns on 20 Nov, in no campaign's lead list either) has no thread left to lose: counted in ``counts``
    ("lead_gone"), not a failure."""
    counts = counts if counts is not None else {}
    threads = threads if threads is not None else {}
    from smartlead.v2_results import is_test_campaign
    out = {e: {"campaigns": [], "replied": 0, "campaigns_unread": False, "replies_unread": False} for e in want}

    def mark(*fields, exc: BaseException | None = None):
        if exc is not None:
            counts.setdefault("why", why_failed(exc))
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
            except Exception as exc:  # noqa: BLE001 - no URL or key in any text kept
                mark("campaigns_unread", exc=exc)
                continue
            for a in accs if isinstance(accs, list) else []:
                e = _low(a.get("from_email"))
                if e in out:
                    out[e]["campaigns"].append({"name": str(c.get("name") or ""), "status": str(c.get("status") or "")})
        need = {e for e in want if not out[e]["campaigns"]}
        if not need:
            counts["replies_skipped"] = counts.get("replies_skipped", 0) + 1
            return out                     # every inbox is in a real campaign: "check first" without a reply check
        hits = []
        for c in camps:
            off = 0
            while True:
                try:
                    b = await get(f"/campaigns/{c['id']}/statistics", {"email_status": "replied", "limit": 100,
                                                                      "offset": off})
                except OutOfTime:
                    raise
                except Exception as exc:  # noqa: BLE001
                    mark("replies_unread", exc=exc)
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
            if all(out[e]["replied"] for e in need):
                break                      # every inbox that needed it has a reply found: nothing more to learn
            who = senders.get((account, cid, sid))
            if who is None:
                who = threads.get((account, cid, sid))
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
                        threads[(account, cid, sid)] = set()                     # not looked up again (t386)
                        continue
                    who = senders_of(await get(f"/campaigns/{cid}/leads/{lid}/message-history"))
                    threads[(account, cid, sid)] = set(who)
                    counts["new_threads"] = counts.get("new_threads", 0) + 1
                except OutOfTime:
                    raise
                except Exception as exc:  # noqa: BLE001
                    mark("replies_unread", exc=exc)
                    continue
            for e in who & want:
                out[e]["replied"] += 1
    except OutOfTime as exc:
        mark("campaigns_unread", "replies_unread", exc=exc)
    except Exception as exc:  # noqa: BLE001 - the campaign list itself
        mark("campaigns_unread", "replies_unread", exc=exc)
    return out


def load_threads(db, now: datetime) -> dict[tuple, set[str]]:
    """{(account, campaign id, stats_id): inboxes in that lead's thread} from REPLY_CACHE (the replies this check has
    placed before, up to REPLY_CACHE_DAYS old). Empty when it can't be read. Never raises."""
    out: dict[tuple, set[str]] = {}
    try:
        for d in db[REPLY_CACHE].find({}):
            parts = str(d.get("_id") or "").rsplit(":", 2)
            at = _parse(d.get("at"))
            if len(parts) == 3 and at and now - at <= timedelta(days=REPLY_CACHE_DAYS):
                out[tuple(parts)] = {_low(x) for x in d.get("who") or [] if _low(x)}
    except Exception:  # noqa: BLE001 - no cache: every reply's history is read
        return {}
    return out


def save_threads(db, new: dict, now: datetime) -> None:
    """Keep the replies placed today (REPLY_CACHE) and drop those older than REPLY_CACHE_DAYS. Never raises."""
    stamp = now.isoformat(timespec="seconds")
    try:
        for (acc, cid, sid), who in new.items():
            db[REPLY_CACHE].replace_one({"_id": f"{acc}:{cid}:{sid}"}, {"_id": f"{acc}:{cid}:{sid}", "who": sorted(who),
                                                                        "at": stamp}, upsert=True)
        cut = (now - timedelta(days=REPLY_CACHE_DAYS)).isoformat(timespec="seconds")
        db[REPLY_CACHE].delete_many({"at": {"$lt": cut}})
    except Exception as exc:  # noqa: BLE001
        print(f"  [Removed inboxes] reply cache not saved ({type(exc).__name__})")


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
                   budget_s: float = BUDGET_S, threads: dict | None = None) -> dict:
    """{"read": True, "inboxes": {inbox: account_use's facts}, "calls": n, "errors": {account: why}} for the removed
    inboxes (``rows``, each with its Smartlead account). An inbox whose account is not configured is left out (split:
    not checked). ``threads``: the reply cache (load_threads), filled with each new lookup. GETs only, the key-safe
    client (no URL or key in any error). Never raises."""
    now = now or datetime.now(timezone.utc)
    cut = now - timedelta(days=REPLY_DAYS)
    want: dict[str, set] = defaultdict(set)
    for r in rows or []:
        if r.get("account"):
            want[r["account"]].add(r["email"])
    out = {"read": True, "inboxes": {}, "calls": 0, "lead_gone": 0, "errors": {}}
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
                counts: dict = {}
                out["inboxes"].update(await account_use(get, acc.name, mine, cut, senders or {}, counts, threads))
                out["lead_gone"] += counts.get("lead_gone", 0)
                if counts.get("why"):
                    out["errors"][acc.name] = counts["why"]
        except Exception as exc:  # noqa: BLE001 - the account's inboxes stay "not checked"
            out["errors"][acc.name] = why_failed(exc)
            continue
    return out


def merge_use(old: dict | None, new: dict, now: datetime) -> dict:
    """Today's check with, for each inbox it could not finish, the last complete result (up to KEEP_USE old, with its
    own "at"), so a failed read never moves an inbox between the lists (t386). A check that failed as a whole keeps
    the old one and says why. Pure."""
    stamp = now.isoformat(timespec="seconds")
    old = old if (old or {}).get("read") is True else None
    old_at = (old or {}).get("_at")
    if new.get("read") is not True:
        return {**old, "failed": new.get("why") or "not read", "failed_at": stamp} if old else new
    kept, prev = {}, (old or {}).get("inboxes") or {}
    for e, p in prev.items():                  # an account whose read failed as a whole is not in today's check
        p_at = _parse(p.get("at") or old_at)
        if e not in (new.get("inboxes") or {}) and complete(p) and p_at and now - p_at <= KEEP_USE:
            kept[e] = dict(p, at=p_at.isoformat())
    for e, u in (new.get("inboxes") or {}).items():
        p = prev.get(e)
        p_at = _parse((p or {}).get("at") or old_at)
        if complete(u) or not (complete(p) and p_at and now - p_at <= KEEP_USE):
            kept[e] = {**u, "at": stamp}
        else:
            kept[e] = dict(p, at=p_at.isoformat())
    return {**new, "inboxes": kept, "_at": stamp}


def save_use(db, use: dict, now: datetime | None = None) -> bool:
    """Keep today's campaign/reply check beside the provider facts, merged with the last one (merge_use): an inbox
    today's check could not finish keeps its last complete result. Never raises."""
    now = now or datetime.now(timezone.utc)
    try:
        doc = db[COLLECTION].find_one({"_id": DOC_ID}) or {"_id": DOC_ID}
        old = doc.get("use")
        if old and not old.get("_at"):
            old = {**old, "_at": doc.get("use_at")}
        doc.update(use=merge_use(old, use, now), use_at=now.isoformat(timespec="seconds"))
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
        threads = load_threads(db, now)
        known = set(threads)
        t0 = time.monotonic()
        use = await read_use(rows, now, senders, budget_s, threads)
        save_threads(db, {k: v for k, v in threads.items() if k not in known}, now)
        save_use(db, use, now)
        merged = use_fresh(db[COLLECTION].find_one({"_id": DOC_ID}), now)
        now_rows, check = split(rows, merged, now)
        print(f"  [Removed inboxes] {len(rows)} removed: {len(now_rows)} delete now, {len(check)} check first "
              f"({use.get('calls', 0)} Smartlead calls, {time.monotonic() - t0:.0f} s"
              + (f"; not finished for {', '.join(sorted(use.get('errors') or {}))}" if use.get("errors") else "") + ")")
        return use
    except Exception as exc:  # noqa: BLE001
        print(f"  [Removed inboxes] campaigns and replies not checked ({type(exc).__name__})")
        return None
