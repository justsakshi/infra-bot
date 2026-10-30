"""The "Show replies" button under each campaign in the daily stats post.

Asked for by Manveen, 30 Oct 2026: a button per campaign in the #pl_daily
thread that shows the replies behind the number, only to whoever clicks it,
and "no complete email, just the part we care about": who replied, their
company, Smartlead's category and the words they wrote, without the quoted
thread, signature or disclaimer.

Sources, one per platform:
  * Smartlead (email): the campaign's replied statistics, then each lead's
    message history. Smartlead itself, not the n8n reply log, because on
    29 Oct the log had missed two replies Smartlead had.
  * HeyReach (Melior LinkedIn): the inbox, filtered to the campaign.
  * Expandi (BettrData LinkedIn): its open API has no message text, so the
    "Expandi Responses" tab the n8n LinkedIn router fills in BettrData's
    Outbound Tracker V2. It dates replies by day only.

The window is the post's: from the previous post's 09:00 IST snapshot to this
one's. The button carries it, so a click days later still shows the replies
behind that day's number.
"""
from __future__ import annotations

import asyncio
import html
import re
import sys
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser

import httpx

IST = timezone(timedelta(hours=5, minutes=30))
MAX_SHOWN = 15          # replies per click; Slack allows 50 blocks a message
MAX_CHARS = 300         # per reply: their words, not the whole email
AUTO_CATEGORIES = {"out of office", "sender originated bounce"}

# ── cleaning ────────────────────────────────────────────────────────────────

_VOID = {"br", "img", "meta", "hr", "input", "link", "wbr", "col", "area", "base"}
_BLOCK = {"div", "p", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "ul", "ol"}
_SKIP_TAGS = {"head", "style", "script", "title", "blockquote"}
# Element ids / classes that hold a signature or the quoted earlier mail.
_SKIP_MARKS = ("signature", "gmail_quote", "gmail_attr", "yahoo_quoted", "moz-cite-prefix")
# Everything after these is the quoted earlier message (Outlook web, Gmail).
_STOP_MARKS = ("appendonsend", "divrplyfwdmsg", "gmail_quote")


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []
        self.stack: list[tuple[str, bool]] = []
        self.stopped = False

    def _skipping(self) -> bool:
        return bool(self.stack) and self.stack[-1][1]

    def handle_starttag(self, tag, attrs):
        marks = " ".join(str(v or "") for k, v in attrs if k in ("id", "class")).lower()
        if any(m in marks for m in _STOP_MARKS):
            self.stopped = True
        if tag in _VOID:
            if tag == "br" and not self._skipping():
                self.out.append("\n")
            return
        skip = (self._skipping() or tag in _SKIP_TAGS
                or any(m in marks for m in _SKIP_MARKS))
        if tag in _BLOCK and not skip:
            self.out.append("\n")
        self.stack.append((tag, skip))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                break
        if tag in _BLOCK:
            self.out.append("\n")

    def handle_data(self, data):
        if not self.stopped and not self._skipping():
            self.out.append(data.replace("\r", "").replace("\n", " "))


def _html_to_text(raw: str) -> str:
    p = _Text()
    try:
        p.feed(raw)
        p.close()
    except Exception:  # noqa: BLE001 - malformed mail: fall back to stripping tags
        return re.sub(r"<[^>]+>", " ", raw)
    return "".join(p.out)


_CUT = [re.compile(p, re.I) for p in (
    r"^\s*>",                                          # quoted line
    r"^\s*On\b.{0,250}\bwrote:?\s*$",                  # "On <date> <name> wrote:"
    r"^\s*-{2,}\s*(Original|Forwarded) Message",
    r"^\s*_{8,}\s*$",                                  # Outlook divider
    r"^\s*(From|De|Von):\s",                           # Outlook header block
    r"^\s*--\s*$",                                     # signature delimiter
    r"^\s*Sent from (my|Outlook|Mail)\b",
    r"^\s*Get Outlook for\b",
    r"^\s*(CONFIDENTIAL|DISCLAIMER|NOTICE:|This (e-?mail|message|communication)\b.{0,40}"
    r"\b(confidential|intended|privileged|attachments))",
)]
# Real markup, not an address in angle brackets ("Laurie <l@x.io>").
_HTML = re.compile(r"<(html|body|div|p|br|span|table|blockquote|meta|style|font|b|a)[\s/>]", re.I)
_CSS = re.compile(r"^\s*[\w.#*,\s-]*\{[^}]*\}?\s*$|margin-(top|bottom)\s*:", re.I)
_CONTACT = re.compile(r"(\+?\d[\d\s().-]{7,}\d)|https?://|www\.|\S+@\S+\.\w+", re.I)


def _looks_like_signature(par: list[str]) -> bool:
    """Name / title / phone lines. A paragraph that ends like a sentence is
    the person talking ("Sure, call me on 415-555-1234."), never a signature."""
    if re.search(r"[.?!]\s*$", par[-1]):
        return False
    short = all(len(l.split()) <= 6 for l in par)
    return short and (len(par) >= 2 or bool(_CONTACT.search(par[0])))


def _cap(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= MAX_CHARS else text[:MAX_CHARS - 1].rstrip() + "…"


def clean_email_reply(raw: str) -> str:
    """Only the words the person wrote this time, at most MAX_CHARS."""
    raw = str(raw or "")
    text = _html_to_text(raw) if _HTML.search(raw) else raw
    text = html.unescape(text).replace("\xa0", " ").replace("\r", "")
    kept: list[str] = []
    for line in text.split("\n"):
        if any(p.search(line) for p in _CUT):
            break
        if _CSS.search(line):
            continue
        kept.append(line.strip())
    pars: list[list[str]] = []
    cur: list[str] = []
    for line in kept + [""]:
        if line:
            cur.append(line)
        elif cur:
            pars.append(cur)
            cur = []
    # Drop one trailing signature block (name / title / phone), never the
    # only paragraph: more than one pass ate real answers in review.
    if len(pars) > 1 and _looks_like_signature(pars[-1]):
        pars.pop()
    return _cap(" ".join(" ".join(p) for p in pars))


def clean_text(text: str) -> str:
    """A LinkedIn message: already just their words; tidy and cap it."""
    return _cap(html.unescape(str(text or "")))


# ── time window ─────────────────────────────────────────────────────────────

def parse_time(value) -> datetime | None:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def in_window(value, start: datetime | None, end: datetime | None) -> bool:
    dt = parse_time(value)
    if dt is None:
        return False
    return (start is None or dt >= start) and (end is None or dt < end)


def post_window(base_day: str | None, today: str) -> tuple[str | None, str]:
    """(from, to) as UTC ISO strings: 09:00 IST on the base day and today."""
    def nine(d: str) -> str:
        return datetime.combine(date.fromisoformat(d), datetime.min.time(),
                                IST).replace(hour=9).astimezone(timezone.utc).isoformat()
    return (nine(base_day) if base_day else None), nine(today)


# ── campaign names ──────────────────────────────────────────────────────────

def short_names(names: list[str]) -> dict[str, str]:
    """'Roundtable | AI in the Data Business | Laurie' -> 'Roundtable · Laurie'.

    Among campaigns that share a first part, a middle part every one of them
    has says nothing, so it goes. A name left alone keeps every part.
    """
    parts = {n: [s.strip() for s in str(n).split("|") if s.strip()] for n in names}
    groups: dict[str, list[str]] = {}
    for n, segs in parts.items():
        if segs:
            groups.setdefault(segs[0].lower(), []).append(n)
    out = {}
    for n, segs in parts.items():
        group = groups.get(segs[0].lower(), []) if segs else []
        keep = segs
        if len(group) >= 2:
            common = set.intersection(*(set(s.lower() for s in parts[g][1:]) for g in group))
            trimmed = [segs[0]] + [s for s in segs[1:-1] if s.lower() not in common] + segs[-1:]
            if len(trimmed) >= 2:
                keep = trimmed
        out[n] = " · ".join(keep) if keep else str(n)
    if len(set(out.values())) < len(out):          # never merge two campaigns
        seen: dict[str, int] = {}
        for v in out.values():
            seen[v] = seen.get(v, 0) + 1
        out = {n: (v if seen[v] == 1 else " · ".join(parts[n])) for n, v in out.items()}
    return out


# ── Slack message ───────────────────────────────────────────────────────────

def _esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _when(value) -> str:
    s = str(value or "")
    if len(s) == 10:                                   # a day only (Expandi log)
        try:
            d = date.fromisoformat(s)
            return f"{d.day} {d:%b}"
        except ValueError:
            return s
    dt = parse_time(s)
    if not dt:
        return s
    dt = dt.astimezone(IST)
    return f"{dt.day} {dt:%b, %H:%M} IST"


def render(title: str, channel: str, replies: list[dict], window: str,
           note: str = "") -> dict:
    """{'text', 'blocks'} for chat.postEphemeral."""
    kind = "email" if channel == "Email" else "LinkedIn"
    n = len(replies)
    if not n:
        head = f"*{_esc(title)}*: No {kind} replies {window}."
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": head}}]
        if note:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": note}]})
        return {"text": f"No {kind} replies {window}", "blocks": blocks}
    word = "reply" if n == 1 else "replies"
    people = len({_who(r) for r in replies})
    from_who = f" from {people} {'person' if people == 1 else 'people'}" if people != n else ""
    blocks = [{"type": "section", "text": {"type": "mrkdwn",
                                           "text": f"*{_esc(title)}* · {n} {kind} {word}"
                                                   f"{from_who} {window}"}}]
    ordered = sorted(replies, key=lambda r: str(r.get("time") or ""), reverse=True)
    for r in ordered[:MAX_SHOWN]:
        who = " · ".join(p for p in (f"*{_esc(r.get('name') or 'Someone')}*",
                                     _esc(r.get("company") or ""),
                                     _esc(r.get("category") or ""),
                                     _when(r.get("time")),
                                     "_earlier reply, counted now_" if r.get("earlier") else "") if p)
        words = _esc(r.get("text") or "") or "_(no text)_"
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                                                   "text": f"{who}\n> {words}"}})
    tail = []
    if n > MAX_SHOWN:
        tail.append(f"…and {n - MAX_SHOWN} more. The rest are in the platform's inbox.")
    if note:
        tail.append(note)
    if tail:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": " ".join(tail)}]})
    return {"text": f"{n} {kind} {word} {window}", "blocks": blocks}


# ── fetching ────────────────────────────────────────────────────────────────

def from_lead(sender, lead_email: str) -> bool:
    """A REPLY in Smartlead's history can be the client's own team answering
    in the thread (29 Oct: Kenny at bettrdata.io in a Speedeon thread). Keep
    the lead's address and their colleagues' (same domain) only."""
    s = str(sender or "").lower().strip()
    if not s:
        return True
    dom = lambda a: a.rsplit("@", 1)[-1].strip("> ")
    return dom(s) == dom(str(lead_email or "").lower())


LOOK_BACK_DAYS = 14     # how far back a late-categorised reply is looked for


def _who(r: dict):
    """One person: their address when known (two unnamed leads stay two)."""
    return r.get("lead") or (r.get("name"), r.get("company"))


def select_replies(all_replies: list[dict], start, end, counted: int = 0) -> list[dict]:
    """The replies in the window. When the post counted more than that (an
    older reply Smartlead categorised since the last post, 27 Oct Aaron |
    Open tracking), add the latest earlier ones, marked as such."""
    inside = [r for r in all_replies if in_window(r.get("time"), start, end)]
    short = counted - len({_who(r) for r in inside})
    if short <= 0 or start is None:
        return inside
    seen = {_who(r) for r in inside}
    oldest = start - timedelta(days=LOOK_BACK_DAYS)
    earlier = sorted((r for r in all_replies
                      if in_window(r.get("time"), oldest, start) and _who(r) not in seen),
                     key=lambda r: str(r.get("time") or ""), reverse=True)
    picked, people = [], set()
    for r in earlier:
        key = _who(r)
        if key in people:
            continue
        if len(people) >= short:
            break
        people.add(key)
        picked.append({**r, "earlier": True})
    return inside + picked


async def _smartlead(cid: str, start, end) -> list[dict]:
    """Every reply (any time) from leads who replied in the last 30 days
    before the window; select_replies() picks the window's."""
    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient

    for acc in discover_accounts():
        async with SmartleadClient(acc.api_key, acc.name) as slc:
            # Another account's campaign comes back empty (200, no JSON body)
            # or as a 4xx; a network error or 5xx still fails the click.
            try:
                camp = await slc._get(f"/campaigns/{cid}")
            except ValueError:
                continue
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (400, 403, 404):
                    continue
                raise
            if not camp:
                continue
            stats, offset = [], 0
            while True:
                page = await slc._get(f"/campaigns/{cid}/statistics",
                                      {"email_status": "replied", "limit": 100, "offset": offset})
                rows = (page or {}).get("data") or []
                stats += rows
                if len(rows) < 100 or offset > 5000:
                    break
                offset += 100
            # A lead's first reply time; a lead who first replied long before
            # the window may still have written again inside it.
            floor = start - timedelta(days=30) if start else None
            wanted: dict[str, dict] = {}
            for r in stats:
                cat = str(r.get("lead_category") or "")
                if cat.lower() in AUTO_CATEGORIES or not r.get("lead_email"):
                    continue
                t = parse_time(r.get("reply_time"))
                if floor and (t is None or t < floor):
                    continue
                wanted.setdefault(str(r["lead_email"]).lower(), r)
            sem = asyncio.Semaphore(4)

            failed = 0

            async def one(email: str, stat: dict) -> list[dict]:
                nonlocal failed
                try:
                    return await fetch(email, stat)
                except Exception:  # noqa: BLE001 - one lead must not cost the rest
                    failed += 1
                    return []

            async def fetch(email: str, stat: dict) -> list[dict]:
                async with sem:
                    lead = await slc._get("/leads/", {"email": email})
                    lid = (lead or {}).get("id")
                    if not lid:
                        return []
                    hist = await slc._get(f"/campaigns/{cid}/leads/{lid}/message-history")
                name = " ".join(p for p in (lead.get("first_name"), lead.get("last_name")) if p) \
                    or stat.get("lead_name") or ""
                return [{"lead": email, "name": name, "company": lead.get("company_name") or "",
                         "category": stat.get("lead_category") or "",
                         "time": m.get("time"), "text": clean_email_reply(m.get("email_body"))}
                        for m in (hist or {}).get("history") or []
                        if m.get("type") == "REPLY" and from_lead(m.get("from"), email)]

            found = await asyncio.gather(*(one(e, s) for e, s in wanted.items()))
            if failed:
                print(f"[replies] {failed} lead(s) could not be read", file=sys.stderr)
            return [r for rs in found for r in rs], failed
    raise LookupError("campaign not found in any Smartlead account")


async def _heyreach(client: str, cid: str, start, end) -> list[dict]:
    from smartlead import campaign_metrics as cm
    from smartlead.heyreach import HeyReachClient
    from smartlead.heyreach_accounts import discover_heyreach_workspaces

    ws = [w for w in discover_heyreach_workspaces() if cm.display_client(w.name) == client]
    if not ws:
        raise LookupError(f"no HeyReach key for {client}")
    out: list[dict] = []
    async with HeyReachClient(ws[0].api_key, ws[0].name) as hrc:
        offset = 0
        while offset < 2000:
            page = await hrc._post("/inbox/GetConversationsV2",
                                   {"filters": {"campaignIds": [int(cid)]},
                                    "offset": offset, "limit": 100})
            items = (page or {}).get("items") or []
            for it in items:
                prof = it.get("correspondentProfile") or {}
                name = " ".join(p for p in (prof.get("firstName"), prof.get("lastName")) if p)
                for m in it.get("messages") or []:
                    if m.get("sender") == "CORRESPONDENT" and in_window(m.get("createdAt"), start, end):
                        out.append({"lead": prof.get("profileUrl") or prof.get("linkedin_id") or name,
                                    "name": name, "company": prof.get("companyName") or "",
                                    "category": "", "time": m.get("createdAt"),
                                    "text": clean_text(m.get("body"))})
            offset += 100
            if len(items) < 100 or offset >= int((page or {}).get("totalCount") or 0):
                break
    return out


EXPANDI_LOG_TAB = "Expandi Responses"


def _sheet_day(value) -> date | None:
    s = str(value or "").strip()
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _same_name(a: str, b: str) -> bool:
    """n8n writes the campaign name itself; ignore case and spacing."""
    norm = lambda s: re.sub(r"\s+", " ", str(s or "")).strip().lower()
    return norm(a) == norm(b)


def expandi_log_replies(values: list[list[str]], campaign: str,
                        start: datetime | None, end: datetime | None) -> list[dict]:
    """Rows of the Expandi Responses tab for one campaign, by day (IST)."""
    if not values:
        return []
    head = [h.strip() for h in values[0]]

    def col(label):
        return head.index(label) if label in head else None
    i_name, i_co, i_cat = col("Full Name"), col("Compay Name"), col("Lead Category")
    i_day, i_camp, i_text = col("Reply date"), col("Campaign Name"), col("Reply from Lead")
    if None in (i_day, i_camp, i_text):
        raise LookupError("Expandi Responses tab is missing a column")
    lo = start.astimezone(IST).date() if start else None
    hi = end.astimezone(IST).date() if end else None
    out = []
    for raw in values[1:]:
        row = raw + [""] * (len(head) - len(raw))
        if not _same_name(row[i_camp], campaign):
            continue
        d = _sheet_day(row[i_day])
        if d is None or (lo and d < lo) or (hi and d > hi):
            continue
        out.append({"lead": row[col("Person LinkedIn")] if col("Person LinkedIn") is not None else None,
                    "name": row[i_name] if i_name is not None else "",
                    "company": row[i_co] if i_co is not None else "",
                    "category": row[i_cat] if i_cat is not None else "",
                    "time": d.isoformat(), "text": clean_text(row[i_text])})
    return out


def _expandi(client: str, name: str, start, end) -> list[dict]:
    from smartlead.config import BETTRDATA_TRACKER_SHEET_ID
    from smartlead.sheets import SheetsWriter

    if client != "BettrData":
        raise LookupError(f"no Expandi reply log for {client}")
    ws = SheetsWriter(BETTRDATA_TRACKER_SHEET_ID)._sh.worksheet(EXPANDI_LOG_TAB)
    return expandi_log_replies(ws.get_all_values(), name, start, end)


def window_label(start: datetime | None, platform: str, end: datetime | None = None) -> str:
    if start is None:
        return "to date"
    d = start.astimezone(IST)
    if platform.lower() == "expandi":
        e = (end or start).astimezone(IST)
        return f"logged {d.day} {d:%b}–{e.day} {e:%b}"
    return f"since {d:%a} {d.day} {d:%b}, 9:00 IST"


async def fetch_and_render(payload: dict) -> dict:
    """The ephemeral message for one button click."""
    start = parse_time(payload.get("from"))
    end = parse_time(payload.get("to"))
    platform = str(payload.get("platform") or "")
    channel = str(payload.get("channel") or "Email")
    title = payload.get("title") or " · ".join(
        s.strip() for s in str(payload.get("name") or "").split("|"))
    label = window_label(start, platform, end)
    p = platform.lower()
    try:
        failed = 0
        if p == "smartlead":
            found, failed = await _smartlead(str(payload["id"]), start, end)
            replies = select_replies(found, start, end, int(payload.get("n") or 0))
        elif p == "heyreach":
            replies = await _heyreach(str(payload.get("client")), str(payload["id"]), start, end)
        elif p == "waalaxy":
            from smartlead import waalaxy_log
            values = await asyncio.to_thread(waalaxy_log.read_log)
            replies = waalaxy_log.replies(values, str(payload.get("name") or ""), start, end)
        elif p == "expandi":
            replies = await asyncio.to_thread(_expandi, str(payload.get("client")),
                                              str(payload.get("name") or ""), start, end)
        else:
            raise LookupError(f"replies from {platform or 'this platform'} can't be read yet")
    except LookupError as exc:
        return render(title, channel, [], label, note=f"⚠️ {exc}.")
    except Exception as exc:  # noqa: BLE001 - never echo the error: URLs carry API keys
        return render(title, channel, [], label,
                      note=f"⚠️ Couldn't read the replies ({type(exc).__name__}). Try again in a minute.")
    note = ("From the n8n LinkedIn reply log (Expandi's API has no message text); "
            "it dates replies by day only." if p == "expandi" else "")
    if failed:
        note = (note + " " if note else "") + \
            f"⚠️ {failed} lead(s) couldn't be read; try again for the full list."
    return render(title, channel, replies, label, note=note)
