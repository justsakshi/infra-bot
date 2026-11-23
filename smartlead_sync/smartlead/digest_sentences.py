"""The digest's approved sentences (t409, 23 Nov 2026): the only words Manveen's daily post may use.

Manveen, 23 Nov 2026: "why does it fail to be readable again and again", "make sure you fix them from root". Each job
wrote its own sentence, so every new job or wording change could make the post unreadable again. Now the post holds
only things a person must do, each one of the sentences below with its [brackets] filled from data. Code never writes
other words into the post; a person-only ask that fits none of them goes to the thread in the closest sentence's shape
and its wording waits for Manveen's OK (smartlead/digest_items.py routes every ask kind).

The sentences, as approved ("sure go aheaf", 23 Nov 2026):

  1  "[Client]: [domain] failed its weekly test ([n] of [m] reached the inbox at [provider]). Keep its [k] inboxes out
     of campaigns and don't renew the domain."
  2  "[Client]: retire [k] inboxes on [domain] in [provider] before [date], or they renew ([$])."
  3  "[Client]: delete [k] inboxes in Smartlead that [provider] already removed (list in thread)."
  4  "[Client]: [k] removed inboxes are still in [campaign]. Take them out, then delete them (list in thread)."
  5  "[Client]: [inbox] finished warm-up repair. Remove its DO NOT USE tag if you want it back."
  6  "[Client]: [domain] is on Spamhaus and in [campaign]. Take its [k] inboxes out."
  7  "[Client]: [k] inboxes have no weekly test. Add them to a test in Smart Delivery (steps in thread)."
  W  "Watching: [domain] replies are low ([a] vs [b] expected); the next weekly test on [date] decides."
     (under the items, at most 2, after the label "Watching, nothing to do" of the approved mock)

What the code may change, and nothing else: the brackets; "inboxes" -> "inbox" (and "are" -> "is", "have" -> "has",
"them" -> "it", "they renew" -> "it renews") when there is one; sentence 2 without "([$])" when the bill's amount is not the amount for these inboxes (the approved
mock: "... before 30 Nov, or they renew."); sentence 2's [k] may carry the inboxes' kind ("3 Google inboxes", the mock).

    sentence(item) -> str           the approved sentence of one item
    header(day, n) -> str           "Inbox digest · Mon 23 Nov · 4 things to do"
    check(text, facts) -> [str]     what an AI-written digest gets wrong against the day's facts (blind test, t409)
"""
from __future__ import annotations

import re

CLIENTS = {"pl": "Precise Leads", "precise leads": "Precise Leads", "precise_leads": "Precise Leads",
           "melior": "Melior", "bettrdata": "BettrData", "bd": "BettrData", "osc": "OSC"}

TEMPLATES = {
    "1": "{client}: {domain} failed its weekly test ({n} of {m} reached the inbox at {provider}). Keep its {k} inboxes "
         "out of campaigns and don't renew the domain.",
    "2": "{client}: retire {k} inboxes on {domain} in {provider} before {date}, or they renew ({cost}).",
    "2-": "{client}: retire {k} inboxes on {domain} in {provider} before {date}, or they renew.",
    "3": "{client}: delete {k} inboxes in Smartlead that {provider} already removed (list in thread).",
    "4": "{client}: {k} removed inboxes are still in {campaign}. Take them out, then delete them (list in thread).",
    "5": "{client}: {inbox} finished warm-up repair. Remove its DO NOT USE tag if you want it back.",
    "6": "{client}: {domain} is on Spamhaus and in {campaign}. Take its {k} inboxes out.",
    "7": "{client}: {k} inboxes have no weekly test. Add them to a test in Smart Delivery (steps in thread).",
    "W": "Watching: {domain} replies are low ({a} vs {b} expected); the next weekly test on {date} decides.",
}
FIELDS = {s: re.findall(r"\{(\w+)\}", t) for s, t in TEMPLATES.items()}
WATCH_LABEL = "Watching, nothing to do"
MAX_ITEMS = 5                       # numbered items in the post; the rest are in the thread (decision 4)
MAX_WATCH = 2                       # "Watching" lines under the items
MAX_NAMED = 3                       # a list of more addresses than this is "N inboxes, list in thread" (decision 4)


def client_name(c) -> str:
    """The client's display name (never a slug): 'Bettrdata', 'BETTRDATA', 'bd' -> 'BettrData'."""
    s = str(c or "").strip()
    return CLIENTS.get(s.lower(), s)


def _one(text: str, k) -> str:
    """'1 inboxes are ... them' -> '1 inbox is ... it' (number agreement only)."""
    try:
        if int(str(k).split()[0]) != 1:
            return text
    except (ValueError, IndexError):
        return text
    text = re.sub(r"\b1 ((?:\w+ )?)inboxes\b", r"1 \1inbox", text)
    text = re.sub(r"(\b1 (?:\w+ )?inbox) are\b", r"\1 is", text)
    text = re.sub(r"(\b1 (?:\w+ )?inbox) have\b", r"\1 has", text)
    text = text.replace("removed inbox are still", "removed inbox is still")
    text = text.replace("Take them out, then delete them", "Take it out, then delete it")
    text = text.replace("Add them to a test", "Add it to a test")
    text = text.replace("or they renew", "or it renews")
    return text


def sentence(item: dict) -> str:
    """The approved sentence of ``item`` ({"s": "1".."7" | "W", "f": {field: value}}). Raises KeyError when a
    bracket has no value: a sentence is never sent with a gap."""
    s, f = item["s"], dict(item.get("f") or {})
    if s == "2" and not f.get("cost"):
        s = "2-"
    if "client" in FIELDS[s]:
        f["client"] = client_name(f.get("client"))
    missing = [x for x in FIELDS[s] if f.get(x) in (None, "")]
    if missing:
        raise KeyError(f"sentence {s} has no {', '.join(missing)}")
    text = TEMPLATES[s].format(**{x: f[x] for x in FIELDS[s]})
    return _one(text, f.get("k")) if "k" in FIELDS[s] else text


def header(day: str, n: int) -> str:
    if not n:
        return f"Inbox digest · {day} · nothing to do today"
    return f"Inbox digest · {day} · {n} thing{'s' if n != 1 else ''} to do"


def watch_line(items: list[dict]) -> list[str]:
    """The "Watching" lines: with one, the mock's single line; with two, the label then one line each."""
    lines = [sentence(w) for w in items[:MAX_WATCH]]
    if not lines:
        return []
    if len(lines) == 1:
        return [f"{WATCH_LABEL}: {lines[0][len('Watching: '):]}"]
    return [f"{WATCH_LABEL}:"] + [f"• {x[len('Watching: '):]}" for x in lines]


def shape(item: dict) -> str:
    """An item with no approved sentence (a parked kind), in the closest sentence's shape: '[Client]: what. Step.'"""
    client = client_name((item.get("f") or {}).get("client"))
    what = str(item.get("what") or "").strip().rstrip(".")
    step = str(item.get("step") or "").strip()
    step = step[:1].upper() + step[1:] if step else ""
    return (f"{client}: " if client else "") + what + "." + (f" {step.rstrip('.')}." if step else "")


# --- the blind test's checker (deterministic) --------------------------------------------------------------------------

_NUM = re.compile(r"(?<![\w.@-])\$?\d[\d,]*(?:\.\d+)?%?(?![\w@-])")
_DOMAIN = re.compile(r"(?<![\w@.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}(?![\w.-]*\w)", re.I)
_EMAIL = re.compile(r"[a-z0-9._%+'-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", re.I)
_DAY = re.compile(r"\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w* (\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\b")
_DATE2 = re.compile(r"\b(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\b")


def _norm_num(x: str) -> str:
    x = x.replace(",", "").replace("$", "").rstrip("%")
    try:
        v = float(x)
    except ValueError:
        return x
    return str(int(v)) if v == int(v) else f"{v:g}"


def _allowed(facts: dict) -> tuple[set[str], set[str], set[str]]:
    """(numbers, names, dates) the day's facts hold: every value of every item, the header's count and date."""
    blob = [str(facts.get("day") or ""), str(facts.get("n") or 0)]
    for it in list(facts.get("items") or []) + list(facts.get("watch") or []) + list(facts.get("thread") or []):
        blob += [str(v) for v in (it.get("f") or {}).values()]
        blob += [str(it.get("what") or ""), str(it.get("step") or "")]
        blob += [str(x) for x in it.get("detail") or []] + [str(x) for x in it.get("steps") or []]
    text = "\n".join(blob)
    nums = {_norm_num(m.group(0)) for m in _NUM.finditer(text)}
    names = {m.group(0).lower() for m in _EMAIL.finditer(text)} | {m.group(0).lower() for m in _DOMAIN.finditer(text)}
    dates = {f"{int(d)} {m}" for d, m in _DATE2.findall(text)}
    nums |= {str(i) for i in range(0, 11)}           # list numbering, "1 of 2" and the like
    return nums, names, dates


def check(text: str, facts: dict, top_lines: int = 12) -> list[str]:
    """What a digest post gets wrong against the day's facts (empty = it passes): one of the first MAX_ITEMS items
    missing (its domain or inbox, or its client and count, must be in the text; the rest belong in the thread), a number, domain, address or date the facts don't hold,
    and the post's limits (at most MAX_ITEMS numbered items before the first thread marker, at most ``top_lines``
    non-blank lines in the post)."""
    out = []
    nums, names, dates = _allowed(facts)
    low = text.lower()
    for i, it in enumerate((facts.get("items") or [])[:MAX_ITEMS], 1):     # the rest are in the thread
        f = it.get("f") or {}
        keys = [str(f[x]).lower() for x in ("domain", "inbox") if f.get(x) and "list in thread" not in str(f[x])]
        if keys:
            if not all(k in low for k in keys):
                out.append(f"item {i} missing: {', '.join(keys)}")
        else:
            c, k = client_name(f.get("client")).lower(), str(f.get("k") or "").split(" ")[0]
            if c not in low or (k and not re.search(rf"(?<!\d){re.escape(k)}(?!\d)", text)):
                out.append(f"item {i} missing: {c} {k}")
    for m in _NUM.finditer(text):
        if _norm_num(m.group(0)) not in nums:
            out.append(f"number not in the facts: {m.group(0)}")
    for m in list(_EMAIL.finditer(text)) + list(_DOMAIN.finditer(text)):
        x = m.group(0).lower()
        if x in names or any(x in n for n in names) or x.endswith((".csv", ".json")):
            continue
        out.append(f"name not in the facts: {m.group(0)}")
    for d, mon in _DATE2.findall(text):
        if f"{int(d)} {mon[:3]}" not in dates:
            out.append(f"date not in the facts: {d} {mon}")
    post = text.split("--- thread ---")[0]
    lines = [x for x in post.split("\n") if x.strip()]
    if len(lines) > top_lines:
        out.append(f"post has {len(lines)} lines (max {top_lines})")
    numbered = [x for x in lines if re.match(r"\s*\d+[.)] ", x)]
    if len(numbered) > MAX_ITEMS:
        out.append(f"post has {len(numbered)} numbered items (max {MAX_ITEMS})")
    return list(dict.fromkeys(out))
