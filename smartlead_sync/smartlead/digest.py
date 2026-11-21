"""One readable daily digest instead of a post per job (Manveen, 16 Nov 2026: "One daily digest"; "if you would just
spam that every check 15 minutes or 30 minutes throughout the day nobody will read that and nothing will happen").

A job no longer posts its own report to the inbox channel. It hands the digest three things:

  * asks: what a person must do, each with the exact step and its own list (which campaigns, inboxes, domains or
    bills it is about: ``detail``). A job's asks REPLACE its previous ones (they are the
    job's current view), so a fixed problem drops out the next time the job runs. ``since`` is kept per ask key,
    so the digest can say how long an ask has been open.
  * done: what the job did by itself (inboxes pulled, tagged, returned, test copy refreshed ...). Each is listed
    once, in the next digest.
  * detail: the job's full report, posted in the digest's thread for whoever wants it.

``urgent`` items (a declined card, a live campaign losing half its inboxes, a campaign left with only failed
inboxes) are posted at once, one time per key, and stay in the digest as asks until the job stops reporting them.
Once it is no longer reported (fixed), its key is forgotten, so the same problem coming back later posts again.

Every ask carries its list (17 Nov 2026, Manveen, on an urgent post that named no campaign: "what was the point of
this message if it's not gonna give me the complete info"). The urgent post shows up to URGENT_LINES of it and the
digest up to DIGEST_LINES under each ask, with "and N more"; so a step says "each campaign listed", never "in the
thread" (an urgent post has no thread).

The digest itself is posted by digest_post.py, once a day in the morning (IST), addressed to Manveen.

Safety: if Mongo can't be written, report() returns False and the caller posts the old way, so an ask is never
lost. DIGEST_MODE=off on a server turns the whole thing off (every job posts as before).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
OWNER = "U09TQ9D7YNM"            # Manveen (16 Nov 2026: "no i mean to say me manveen not anjali")
ASKS = "digest_asks"             # one doc per source: {_id: source, asks: [...], detail: [...], at}
DONE = "digest_done"             # one doc per thing done: {_id: key, source, text, at, posted_on}
URGENT = "digest_urgent"         # one doc per urgent key already posted: {_id: key, source, text, at}
META = "digest_meta"             # {_id: "last", at, ts, asks, done}: when the digest last went out


def on() -> bool:
    """The digest is on unless DIGEST_MODE is off (the undo: set DIGEST_MODE=off and every job posts as before)."""
    return (os.getenv("DIGEST_MODE") or "on").strip().lower() not in ("off", "0", "false", "no")


@dataclass
class Ask:
    text: str                    # what is wrong, in one plain sentence
    step: str = ""               # the exact thing to do
    key: str = ""                # stable id across runs (counts may change, the key does not)
    kind: str = "ask"            # "ask" (a person must act), "broken" (a check or job that broke) or "info" (a
                                 # fact with no step: shown once, not counted as a thing to do; 20 Nov 2026)
    urgent: bool = False         # also post at once, one time per key
    detail: list = field(default_factory=list)   # what it is about, one line each (campaign, inbox, domain, bill)

    def doc(self, source: str) -> dict:
        return {"text": self.text, "step": self.step, "key": self.key or default_key(source, self.text),
                "kind": self.kind if self.kind in ("ask", "broken", "info") else "ask", "urgent": bool(self.urgent),
                "detail": bullets(self.detail)[:KEEP_LINES]}


KEEP_LINES = 60                  # lines kept per ask in Mongo
URGENT_LINES = 10                # shown in an urgent post
DIGEST_LINES = 5                 # shown under an ask in the digest; the job's full report is in its thread


def bullets(lines) -> list[str]:
    """Non-blank lines, each starting with a bullet."""
    out = []
    for x in lines or []:
        x = str(x).strip()
        if x:
            out.append(x if x.startswith(("•", "-", "–")) else f"• {x}")
    return out


def by_domain(items, per_line: int = 8) -> list[str]:
    """Inboxes as one line per domain ('x.com: a@, b@ and 3 more'), so a list of 20 reads in a few lines. Items
    without an @ are kept as they are."""
    groups: dict[str, list[str]] = {}
    other = []
    for x in sorted({str(i).strip() for i in items or [] if str(i).strip()}):
        if "@" in x:
            user, dom = x.rsplit("@", 1)
            groups.setdefault(dom, []).append(user + "@")
        else:
            other.append(x)
    return [f"{d}: {', '.join(us[:per_line])}" + (f" and {len(us) - per_line} more" if len(us) > per_line else "")
            for d, us in sorted(groups.items())] + other


def detail_lines(a: dict, cap: int, rest: str = "") -> list[str]:
    """An ask's list, cut to ``cap`` lines plus '• and N more{rest}'."""
    lines = bullets(a.get("detail"))
    if len(lines) <= cap:
        return lines
    return lines[:cap] + [f"• and {len(lines) - cap} more{rest}"]


@dataclass
class Done:
    text: str
    key: str = ""


def default_key(source: str, text: str) -> str:
    """A key from the words of a line, numbers left out, so '7 inboxes are disconnected' stays one ask when it
    becomes 6."""
    words = re.sub(r"[^a-z ]+", " ", str(text).lower())
    return f"{source}:" + "-".join(words.split())[:80]


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[HEALTH_HISTORY_DB]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def merge_asks(source: str, asks: list, before: list[dict] | None, now: datetime) -> list[dict]:
    """The new asks, each keeping the ``since`` of the same key in the job's previous asks."""
    since = {a.get("key"): a.get("since") for a in (before or []) if a.get("key")}
    out, seen = [], set()
    for a in asks:
        a = a if isinstance(a, Ask) else Ask(str(a))
        d = a.doc(source)
        if d["key"] in seen:
            continue
        seen.add(d["key"])
        d["since"] = since.get(d["key"]) or now.isoformat(timespec="seconds")
        out.append(d)
    return out


def report(source: str, asks: list | None = None, done: list | None = None, detail: list[str] | None = None,
           *, db=None, now: datetime | None = None, post_urgent=None, numbers: dict | None = None) -> bool:
    """Hand the digest a job's current asks (replacing its previous ones), what it did by itself, and its full
    report for the thread. Urgent asks are posted at once, one time per key. False when Mongo can't be written:
    the caller then posts the old way. ``numbers`` ({client: {group: count}}, the inbox list only): where the
    inboxes stand, for the digest's quiet line and its numbers reply."""
    now = now or _now()
    try:
        db = db if db is not None else _db()
        prev = db[ASKS].find_one({"_id": source}) or {}
        merged = merge_asks(source, list(asks or []), prev.get("asks"), now)
        doc = {"_id": source, "asks": merged, "detail": list(detail or [])[:20], "at": now.isoformat(timespec="seconds")}
        if numbers:
            doc["numbers"] = {str(c): {str(g): int(n) for g, n in (v or {}).items()} for c, v in numbers.items()}
        db[ASKS].replace_one({"_id": source}, doc, upsert=True)
        # An urgent ask that is no longer reported was fixed: forget it was posted, so if it comes back it posts again.
        for key in {a.get("key") for a in prev.get("asks") or [] if a.get("urgent")} - {a["key"] for a in merged}:
            db[URGENT].delete_one({"_id": key})
        for d in done or []:
            d = d if isinstance(d, Done) else Done(str(d))
            key = d.key or f"{default_key(source, d.text)}:{now.astimezone(IST):%Y-%m-%d}"
            if not db[DONE].find_one({"_id": key}):
                db[DONE].replace_one({"_id": key}, {"_id": key, "source": source, "text": d.text,
                                                    "at": now.isoformat(timespec="seconds"), "posted_on": None},
                                     upsert=True)
    except Exception as exc:  # noqa: BLE001 - never lose an ask: the caller posts instead
        print(f"  [Digest] not written ({type(exc).__name__}); posting the old way")
        return False
    holds = load_holds(db) if any(a["urgent"] for a in merged) else []
    for a in merged:
        if a["urgent"] and not held_whole(a, holds, now):    # a decided item does not post at once either
            _urgent(db, source, a, now, post_urgent)
    print(f"  [Digest] {source}: {len(merged)} ask(s), {len(done or [])} done, {len(detail or [])} detail part(s)")
    return True


def urgent_text(source: str, a: dict) -> str:
    """What is wrong, what it is about (its list), the step, and that it stays in the digest. Each part apart."""
    lines = [plain(x) for x in detail_lines(a, URGENT_LINES)]
    return "\n".join([f"🚨 {plain(a['text'])}", ""] + (lines + [""] if lines else [])
                     + [f"<@{OWNER}> {cap(plain(a['step']))}" if a.get("step") else f"<@{OWNER}>",
                        "", "It stays in the daily digest until it is fixed."])


def _urgent(db, source: str, a: dict, now: datetime, post_urgent=None) -> bool:
    """Post an urgent ask at once, one time per key (a crash between post and record posts it at most twice)."""
    try:
        if db[URGENT].find_one({"_id": a["key"]}):
            return False
    except Exception:  # noqa: BLE001
        return False
    poster = post_urgent or _post_now
    if not poster(urgent_text(source, a)):
        return False
    try:
        db[URGENT].replace_one({"_id": a["key"]}, {"_id": a["key"], "source": source, "text": a["text"],
                                                  "at": now.isoformat(timespec="seconds")}, upsert=True)
    except Exception:  # noqa: BLE001
        pass
    return True


def channel() -> str:
    from smartlead.config import INFRA_CHANNEL
    return (os.getenv("DIGEST_CHANNEL") or "").strip() or INFRA_CHANNEL


# --- what the digest leaves out: decisions, repeats, asks without their list (18 Nov 2026) -----------------------
#
# Manveen, 18 Nov 2026, on the 11:30 digest: "why did i get these 13 messages". Four of them were one kill list told
# by two jobs; others had been decided already (another person handles them, a retest is booked, a delete list he
# does by hand) and came back every day; one said "each domain in the thread" with nothing under it.
#
# A decision is a doc in Mongo digest_holds (never in the repo, which is public):
#   {_id, reason, owner: "Manveen" | "Anjali" | "session <name>", until: "YYYY-MM-DD" or None,
#    domains: [...], inboxes: [...], keys: [ask keys, * allowed], when: None | "kill-list" | "disconnected",
#    set_on, set_by}
# It holds back every ask about those domains or inboxes (or with those keys), and with ``when`` only asks of that
# kind. It holds from now until ``until`` (the day the item comes back; None = until someone deletes the doc). An
# ask with part of its list held shows the rest. Owner Anjali, no date: the item never goes to Manveen. The digest
# says "Held back: N (decided)" and lists every held item, its reason, owner and date in the thread, so nothing
# held goes invisible.

HOLDS = "digest_holds"

_EMAIL = re.compile(r"[a-z0-9._%+'-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", re.I)
_DOMAIN = re.compile(r"(?<![\w@.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}(?![\w.-]*\w)", re.I)
_FILES = ("csv", "json", "py", "txt", "md", "png", "js", "log", "html", "pdf", "xlsx")

# Asks that are one kill list, told by different jobs (fleet watch daily, the kill report weekly). Value: what the
# ask says about an inbox it lists ("" = its lines are domains).
KILL_LIST = {"kill-report:sending": "still sending",
             "kill-report:idle": "in switched-on campaigns that are not sending",
             "fleet-watch:kill": "on the kill list",
             "kill-report:domains": ""}
_KILL_ORDER = ("still sending", "in switched-on campaigns that are not sending", "on the kill list")
_DOMAIN_REASON = {"kill-report:domains": "domain landing in spam on the classic tests"}
_WHEN = {"disconnected": re.compile(r"disconnect|can.?t log in|cannot log in|log in to them", re.I),
         "kill-list": re.compile(r"kill list|landing in spam", re.I)}
_NEEDS_LIST = re.compile(r"\blisted\b|in the thread|\beach (?:one|domain|inbox|campaign)\b|the list is", re.I)


def items_of(a: dict) -> tuple[set[str], set[str]]:
    """(inboxes, domains) an ask is about, read from its text and its list (lowercase). File names are not
    domains; an inbox's domain is not counted again as a domain."""
    inboxes, domains = set(), set()
    for line in [a.get("text") or ""] + [str(x) for x in a.get("detail") or []]:
        found = {e.lower() for e in _EMAIL.findall(line)}
        inboxes |= found
        rest = _EMAIL.sub(" ", line)
        domains |= {d.lower() for d in _DOMAIN.findall(rest) if d.rsplit(".", 1)[-1].lower() not in _FILES}
    return inboxes, domains


def load_holds(db) -> list[dict]:
    """Every recorded decision; none when Mongo can't be read (then nothing is held back, so nothing is lost)."""
    try:
        return list(db[HOLDS].find({}))
    except Exception:  # noqa: BLE001
        return []


def _until(h: dict):
    u = h.get("until")
    if not u:
        return None
    try:
        return datetime.fromisoformat(str(u)[:10]).date()
    except ValueError:
        return None


def active_holds(holds, now: datetime) -> list[dict]:
    """Holds in force today (IST): no end date, or today is before ``until``. A hold with an unreadable date is
    ignored, so its items show."""
    today = now.astimezone(IST).date()
    out = []
    for h in holds or []:
        if h.get("until") and _until(h) is None:
            continue
        if _until(h) is None or today < _until(h):
            out.append(h)
    return out


def _applies(h: dict, a: dict) -> bool:
    w = h.get("when")
    if not w:
        return True
    rx = _WHEN.get(w)
    if rx is None:
        return False                     # an unknown kind holds nothing
    return bool(rx.search(f"{a.get('key', '')} {a.get('text', '')}")) or (w == "kill-list" and a.get("key") in KILL_LIST)


def _held_item(h: dict, item: str) -> bool:
    doms = {d.lower() for d in h.get("domains") or []}
    if "@" in item:
        return item in {i.lower() for i in h.get("inboxes") or []} or item.rsplit("@", 1)[1] in doms
    return item in doms


def _key_held(h: dict, key: str) -> bool:
    from fnmatch import fnmatchcase
    return any(fnmatchcase(key or "", k) for k in h.get("keys") or [])


def _line_items(line: str) -> set[str]:
    i, d = items_of({"text": line})
    return i | d


def apply_holds(asks: list[dict], holds, now: datetime) -> tuple[list[dict], list[dict]]:
    """(asks still shown, held) where held = [{hold, ask, n, whole}]. An ask goes when a hold names its key, or
    every inbox and domain it is about; with part of its list held, its list keeps the rest and its text says
    how many are held back."""
    live = active_holds(holds, now)
    kept, held = [], []
    for a in asks:
        hs = [h for h in live if _applies(h, a)]
        if not hs:
            kept.append(a)
            continue
        whole = next((h for h in hs if _key_held(h, a.get("key", ""))), None)
        if whole:
            held.append({"hold": whole, "ask": a, "n": 0, "whole": True})
            continue
        inboxes, domains = items_of(a)
        items = inboxes | domains
        by = {x: next((h for h in hs if _held_item(h, x)), None) for x in items}
        if items and all(by.values()):
            for h in {id(h): h for h in by.values()}.values():
                held.append({"hold": h, "ask": a, "n": sum(1 for v in by.values() if v is h), "whole": True})
            continue
        if not any(by.values()):
            kept.append(a)
            continue
        lines, gone = [], {}
        for line in bullets(a.get("detail")):
            li = _line_items(line)
            hit = [by.get(x) for x in li]
            if li and all(hit):
                for h in hit:
                    gone[id(h)] = (h, gone.get(id(h), (h, 0))[1] + 1)
                continue
            lines.append(line)
        if not a.get("detail"):          # its items are in its text: they can't be cut out, only counted
            for h in (v for v in by.values() if v):
                gone[id(h)] = (h, gone.get(id(h), (h, 0))[1] + 1)
        n = sum(k for _, k in gone.values())
        for h, k in gone.values():
            held.append({"hold": h, "ask": a, "n": k, "whole": False})
        kept.append({**a, "detail": lines, "text": f"{a['text']} ({n} of them held back, decided)" if n else a["text"]})
    return kept, held


REMOVED_KEY = "provider-removed:delete"     # provider_removed.KEY: the one delete reminder
_MORE = re.compile(r"\band \d+ more\b")


def _strip(line: str, emails) -> str:
    """The line without these addresses (and the comma that joined each one to the list)."""
    for e in emails:
        e = re.escape(e)
        line = re.sub(r",\s*" + e + r"(?![\w.-])", "", line, flags=re.I)
        line = re.sub(e + r"(?![\w.-]),?\s*", "", line, flags=re.I)
    return re.sub(r"(^[•\-–]\s*|:\s*)and (\d+ more)", r"\1\2", line).rstrip(" ,")


def drop_removed(asks: list[dict], inboxes, domains=()) -> list[dict]:
    """Every ask without the inboxes their provider removed (t352, 20 Nov 2026: the digest asked to reconnect inboxes
    on a cancelled ScaledMail order). Those inboxes are in one ask only, the delete reminder (REMOVED_KEY), which this
    leaves as it is. ``inboxes``: the removed inboxes; ``domains``: domains every Smartlead inbox of which is removed,
    so a line naming only the domain is about removed inboxes. An ask about nothing else goes; one with some of them
    keeps the rest of its list (a line naming others too loses only those addresses) and says it left some out."""
    gone = {str(i).lower() for i in inboxes or ()} | {str(d).lower() for d in domains or ()}
    if not gone:
        return asks
    out = []
    for a in asks:
        if a.get("key") == REMOVED_KEY:
            out.append(a)
            continue
        inb, doms = items_of(a)
        items = inb | doms
        if not items & gone:
            out.append(a)
            continue
        lines = bullets(a.get("detail"))
        if items <= gone and not any(_MORE.search(x) for x in lines + [a.get("text") or ""]):
            continue                                  # about removed inboxes only
        kept, cut = [], []
        for line in lines:
            li = _line_items(line)
            hit = li & gone
            if not hit:
                kept.append(line)
            elif li <= gone and not _MORE.search(line):
                cut += sorted(hit)
            else:
                named = sorted(x for x in hit if "@" in x)
                cut += named
                kept.append(_strip(line, named) if named else line)
        if lines and not kept:
            continue
        if lines and not cut:
            out.append(a)                             # a removed one is named only in its text: nothing to cut
            continue
        named = cut if lines else sorted(items & gone)
        n = f"{len(named)} of them" if all("@" in x for x in named) else "some"   # a domain line: count not known
        out.append({**a, "detail": kept, "text": f"{str(a['text']).rstrip('.')} ({n} removed by their provider, "
                                                 f"left out: see the delete reminder)."})
    return out


def held_whole(a: dict, holds, now: datetime) -> bool:
    kept, _ = apply_holds([a], holds, now)
    return not kept


def listless(a: dict) -> bool:
    """An ask whose step or text points at a list it does not carry (17 Nov: "Look each domain in the thread up"
    with nothing under it): Manveen can't act on it, so it is left out and counted."""
    if a.get("kind") == "broken" or bullets(a.get("detail")):
        return False
    inboxes, domains = items_of({"text": a.get("text")})
    if inboxes or domains:
        return False
    return bool(_NEEDS_LIST.search(f"{a.get('step', '')} {a.get('text', '')}"))


def _n(k: int, one: str, many: str) -> str:
    return f"{k} {one if k == 1 else many}"


def merge_kill_list(asks: list[dict], labels: dict | None = None) -> tuple[list[dict], list[str]]:
    """(asks, thread lines): every kill-list ask, from whichever job, as one item with one line per domain and
    the number of jobs that report it. Its lines are what is still shown after holds."""
    labels = labels or {}
    members = [a for a in asks if a.get("key") in KILL_LIST]
    if not members:
        return asks, []
    inbox_reason: dict[str, str] = {}
    dom_reasons: dict[str, set] = {}
    for a in members:
        reason = KILL_LIST[a["key"]]
        for line in bullets(a.get("detail")):
            inboxes, domains = items_of({"text": line})
            if inboxes and reason:
                for i in inboxes:
                    old = inbox_reason.get(i)
                    if old is None or _KILL_ORDER.index(reason) < _KILL_ORDER.index(old):
                        inbox_reason[i] = reason
            else:
                for d in domains | {i.rsplit("@", 1)[1] for i in inboxes}:
                    dom_reasons.setdefault(d, set()).add(_DOMAIN_REASON.get(a["key"], "whole domain on the kill list"))
    if not inbox_reason and not dom_reasons:
        return [a for a in asks if a.get("key") not in KILL_LIST], []
    lines = []
    for dom in sorted({i.rsplit("@", 1)[1] for i in inbox_reason} | set(dom_reasons)):
        parts = []
        for reason in _KILL_ORDER:
            users = sorted(i.rsplit("@", 1)[0] + "@" for i, r in inbox_reason.items()
                           if r == reason and i.endswith("@" + dom))
            if users:
                shown = ", ".join(users[:4]) + (f" and {len(users) - 4} more" if len(users) > 4 else "")
                parts.append(f"{_n(len(users), 'inbox', 'inboxes')} {reason} ({shown})")
        parts += sorted(dom_reasons.get(dom, ()))
        lines.append(f"• {dom}: {'; '.join(parts)}")
    what = " and ".join(x for x in (_n(len(inbox_reason), "inbox", "inboxes") if inbox_reason else "",
                                    _n(len(dom_reasons), "domain", "domains") if dom_reasons else "") if x)
    reasons = set(inbox_reason.values())
    do = []
    if reasons & {"still sending", "on the kill list"}:
        do.append("take each inbox listed out of its campaigns (Smartlead, the campaign, Email Accounts tab)")
    if "in switched-on campaigns that are not sending" in reasons:
        do.append("pause the switched-on campaigns that still hold one, or take it out of them")
    if dom_reasons:
        do.append("don't add inboxes on the domains listed to campaigns, and don't renew those domains at their "
                  "next bill")
    step = "; ".join(do)
    step = step[:1].upper() + step[1:] + "."
    one = len(inbox_reason) + len(dom_reasons) == 1
    merged = {"text": f"{what[:1].upper()}{what[1:]} {'is' if one else 'are'} on the kill list: "
                      f"{'it' if one else 'they'} kept failing placement tests, so email sent from "
                      f"{'it' if one else 'them'} is likely to land in spam.",
              "step": step, "key": "digest:kill-list", "kind": "ask", "detail": lines, "source": "kill-list",
              "urgent": any(a.get("urgent") for a in members),
              "since": min((str(a.get("since")) for a in members if a.get("since")), default=None),
              "checked": max((a["checked"] for a in members if a.get("checked")), default=None)}
    thread = []
    for a in members:
        thread.append(f"{labels.get(a.get('source'), a.get('source'))}: {a['text']}")
        thread += [f"   {x}" for x in bullets(a.get("detail"))]
    out, placed = [], False
    for a in asks:
        if a.get("key") in KILL_LIST:
            if not placed:
                out.append(merged)
                placed = True
            continue
        out.append(a)
    return out, thread


# --- plain words and one item per set of inboxes (19 Nov 2026) ---------------------------------------------------
#
# Manveen, 19 Nov 2026, on the 11:00 digest: "i got this and it's long spammy i can not unserstand anything". It
# used the bot's own words ("dt_senders", "v2", "BUSY", "report-only until real replies agree with the seed test")
# and told the same 24 inboxes twice (a sheet the domains were missing from, and a tracker the inboxes were missing
# from). Every line the digest shows goes through plain(), and asks about the same inboxes become one item.

_NOT_WORD = r"(?<![\w.@/-])"          # never inside a domain, an address, a file or a campaign name
_PLAIN = [(re.compile(_NOT_WORD + p + r"(?![\w.@/-])"), w) for p, w in (
    (r"dt_senders", "the test-campaign job"),
    (r"v2 weekly(?: test)? job", "weekly test job"),
    (r"weekly v2 test(s?)", r"weekly test\1"),
    (r"DT (?:test )?campaign(s?)", r"test campaign\1"),
    (r"DT test(s?)", r"weekly test\1"),
    (r"(?:Smart Delivery )?v2 (?:weekly )?test(s?)", r"weekly test\1"),
    (r"v2 result(s?)", r"weekly test result\1"),
    (r"v2 pass(es)?", r"weekly test pass\1"),
    (r"failed v2", "failed the weekly test"),
    (r"v2", "weekly test"),
    (r"BUSY", "held back"),
    (r"FREE", "free"),
)] + [(re.compile(r"report-only until real replies agree with the seed test"),
       "not acted on yet: Google results wait until real replies agree")]


_TRACKER_KEY = re.compile(r"\s*\((?:t\d{1,4})(?:[,/ ]+t\d{1,4})*\)")     # "(t235)", "(t268/t269)": tracker keys


def plain(text) -> str:
    """The bot's own words in plain ones ('v2 test' -> 'weekly test', 'dt_senders' -> 'the test-campaign job'),
    never inside a domain, an inbox or a name like 'Campaign | V2'. Tracker keys ("(t235)", copied from a person's
    Smartlead tag) are left out: they mean nothing to the reader (21 Nov 2026)."""
    out = _TRACKER_KEY.sub("", str(text or ""))
    for rx, word in _PLAIN:
        out = rx.sub(word, out)
    return out


def reach(a: dict) -> set[str]:
    """The domains an ask is about: its domains and its inboxes' domains."""
    inboxes, domains = items_of(a)
    return domains | {i.rsplit("@", 1)[1] for i in inboxes}


def merge_same(asks: list[dict], broken: list[dict]) -> tuple[list[dict], list[dict]]:
    """(asks, broken) with every item about the same inboxes told once: an item whose domains are all in an earlier
    item's (or that holds all of an earlier item's) goes under it as ``also``, asks before broken checks. Items that
    name no domain are never merged."""
    hosts: list[dict] = []
    out_asks, out_broken = [], []
    for a, out in [(x, out_asks) for x in asks] + [(x, out_broken) for x in broken]:
        r = reach(a)
        host = next((h for h in hosts if r and h["_reach"] and (r <= h["_reach"] or h["_reach"] <= r)), None)
        if host is not None:
            host["item"].setdefault("also", []).append(a)
            continue
        a = {**a}
        hosts.append({"item": a, "_reach": r})
        out.append(a)
    return out_asks, out_broken


def cap(text: str) -> str:
    """The first letter upper case (a step reads after "Do:")."""
    return text[:1].upper() + text[1:]


def _post_now(text: str) -> bool:
    from smartlead.notify import _post
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        print("  [Digest] SLACK_BOT_TOKEN missing: urgent item not posted")
        return False
    return bool(_post(token, channel(), text))
