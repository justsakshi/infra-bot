"""The last step before the daily digest goes to Slack: hard limits on the final text, whatever item it holds (t386).

Manveen, 23 Nov 2026, after the posted digest: "why does it fail to be readable again and again". Three rounds of
readability fixes (PRs #108, #109, #113) were judged on one good day; on a bad day (a 429, 112 removed inboxes, a
domain three jobs reported) a delete list of 63 full addresses, each with "(could not check replies)", went out in one
line that Slack split across three posts, and a test results file name and a local part shown as a domain reached the
post. The composer (digest_post.build) aims at these limits; this module enforces them on the text itself, so no item
type, old stored wording or new job can get past them, and tests (test_digest_guard.py) fail the build when a messy
day breaks one.

    enforce(head, parts) -> (head, parts)     what is posted
    problems(head, parts) -> [str]            every limit the text breaks (empty = fine); the tests assert on it
"""
from __future__ import annotations

import re
from datetime import date

# --- the limits ------------------------------------------------------------------------------------------------------
TOP_LINES = 10         # non-blank lines in the top post: header, 5 items, 3 Watching lines, the 'more' line (t409)
TOP_CHARS = 1400       # characters in the top post: about one laptop screen of Slack
ITEM_CHARS = 280       # one numbered item in the top post: two lines in Slack's message pane on a laptop
MAX_NAMES = 6          # addresses or domains named on one line; the rest are "and N more" (a list, not a dump)
LINE_CHARS = 420       # one thread line: three lines in Slack; a longer one is a dump
REPLY_CHARS = 3500     # one thread reply: Slack cuts a message at 4,000 characters and splits it into more posts
REPLY_LINES = 40       # one thread reply in lines (notify._THREAD_CHUNK: one part is one Slack reply)
MAX_REPLIES = 3        # thread replies in all (19 Nov 2026, "long spammy")
REPEATS = 2            # the same "(...)" note on more names than this in one line is said once for the group

_EMAIL = re.compile(r"[a-z0-9._%+'-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", re.I)
_LOCAL = re.compile(r"(?<![\w.@%+'-])[a-z0-9._%+'-]+@(?![a-z0-9-])", re.I)         # 'jesse@': an inbox named by its user
_DOMAIN = re.compile(r"(?<![\w@.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}(?![\w.-]*\w)(?!@)", re.I)
_FILE_ENDS = ("csv", "json", "py", "txt", "md", "png", "js", "log", "html", "pdf", "xlsx")

# --- internal names: the bot's files, runs, keys and collections never reach the reader (t386, point D) ---------------
_CSV = re.compile(r"(?<![\w\"])([a-z0-9][\w.-]*?)(?:-run-\d+)?\.(?:csv|json|xlsx)\b", re.I)
_RUN = re.compile(r"\s*\(run \d+(?:, ([^)]*))?\)")
_ISO = re.compile(r"\((20\d\d)-(\d\d)-(\d\d)\)")
_TRACKER = re.compile(r"\s*\(t\d{1,4}(?:[,/ ]+t\d{1,4})*\)")
_SNAKE = re.compile(r"(?<![\w@.%+'-])([a-z][a-z0-9]*(?:_[a-z0-9]+)+)(?![\w@-])")
_ARROW = re.compile(r"\s*·?\s*warm-up \d+→\d+/day, reply \d+→\d+%")
_LAST_CHECKED = re.compile(r"\s*\(last checked [^)]*\)")


def _day(y: str, m: str, d: str) -> str:
    try:
        x = date(int(y), int(m), int(d))
    except ValueError:
        return f"{y}-{m}-{d}"
    return f"{x:%a} {x.day} {x:%b}"


def clean(text: str) -> str:
    """The text without the bot's internal names: a results file 'pl-test-v2-2-run-1.csv' is the test's name
    '"pl-test-v2-2"', '(2026-11-21)' is '(Sat 21 Nov)', a tracker key '(t235)' goes, 'failed_v2' reads 'failed
    v2' (then plain words), the warm-up settings the bot changed go, '(last checked ...)' goes (the item's own date
    says it). Slack links ('<https://...|the inbox sheet>') are left as they are."""
    pieces = re.split(r"(<[^<>]+>)", str(text or ""))
    return "".join(p if p.startswith("<") and p.endswith(">") else _clean(p) for p in pieces)


def _clean(out: str) -> str:
    from smartlead.digest import plain
    out = plain(out)                    # its phrases first ('marked failed_v2'), before snake_case becomes words
    out = _CSV.sub(lambda m: f'"{m.group(1)}"', out)
    out = _RUN.sub(lambda m: f" ({m.group(1)})" if m.group(1) else "", out)
    out = _ISO.sub(lambda m: f"({_day(*m.groups())})", out)
    out = _TRACKER.sub("", out)
    out = _ARROW.sub("", out)
    out = _LAST_CHECKED.sub("", out)
    out = _SNAKE.sub(lambda m: m.group(1).replace("_", " "), out)
    return plain(out)


def leaks(text: str) -> list[str]:
    """The internal names still in a text (empty = none): file names, runs, ISO dates, tracker keys, snake_case words."""
    found = []
    for rx, what in ((re.compile(r"[\w.-]+\.(?:csv|json|xlsx)\b", re.I), "file name"),
                     (re.compile(r"-run-\d+\b|\brun-\d+\b"), "run name"),
                     (re.compile(r"\b20\d\d-\d\d-\d\d\b"), "ISO date"), (_TRACKER, "tracker key"),
                     (_SNAKE, "internal word"), (re.compile(r"\d+→\d+/day"), "warm-up setting"),
                     (re.compile(r"\(last checked"), "job check time")):
        found += [f"{what}: {m.group(0).strip()}" for m in rx.finditer(str(text or "")) if not _in_url(text, m.start())]
    return found


def _in_url(text: str, i: int) -> bool:
    start = text.rfind("<", 0, i)
    return start != -1 and text.find(">", start) > i and text[start + 1:start + 5] == "http"


# --- names on a line --------------------------------------------------------------------------------------------------

def names(line: str) -> list[tuple[int, int]]:
    """(start, end) of every address, user ('jesse@') and domain named on a line, in order. File names and links are
    not names."""
    spans = []
    taken = [False] * (len(line) + 1)
    for rx in (_EMAIL, _LOCAL, _DOMAIN):
        for m in rx.finditer(line):
            if any(taken[m.start():m.end()]) or _in_url(line, m.start()):
                continue
            if rx is _DOMAIN and m.group(0).rsplit(".", 1)[-1].lower() in _FILE_ENDS:
                continue
            spans.append((m.start(), m.end()))
            for k in range(m.start(), m.end()):
                taken[k] = True
    return sorted(spans)


def cap_names(line: str, n: int = MAX_NAMES) -> str:
    """At most ``n`` names on a line: the rest become 'and K more'. The cut is at the end of a list item (the next
    comma, semicolon or the line's end), so no name is cut in half."""
    note = re.search(r" \(each: [^()]*\)$", line)
    if note:
        return cap_names(line[:note.start()], n) + note.group(0)
    spans = names(line)
    if len(spans) <= n:
        return line
    end = spans[n - 1][1]
    stop = [i for i in (line.find(",", end), line.find(";", end)) if i != -1]
    cut = min(stop) if stop else len(line)
    rest = len(spans) - n
    tail = line[cut:]
    close = ")" if line[:cut].count("(") > line[:cut].count(")") and ")" in tail else ""
    return f"{line[:cut].rstrip(' ,;')} and {rest} more{close}"


_NOTE = re.compile(r"\s\(([^()]{3,80})\)")


def group_notes(line: str) -> str:
    """'a@x.com (could not check replies), b@y.com (could not check replies)' -> 'a@x.com, b@y.com (each: could not
    check replies)': a note repeated after many names is said once for the group."""
    ends = {e for _, e in names(line)}
    notes = [m.group(1) for m in _NOTE.finditer(line) if m.start() in ends]       # a note after a name only
    for note in {x for x in notes if notes.count(x) > REPEATS}:
        line = line.replace(f" ({note})", "") + f" (each: {note})"
    return line


def shared_tail(lines: list[str], least: int = 3, size: int = 30) -> list[str]:
    """A run of ``least`` or more list lines that end in the same words ('...; last run 19 Nov sent 1 email per
    provider (4 needed); next run Sat 28 Nov' on every line) says them once, on a line of its own before the run
    (t386: readers saw the same sentence three times and took the list for a repeat)."""
    out, i = [], 0
    while i < len(lines):
        j = i
        while j < len(lines) and lines[j].startswith("• ") and not lines[j].startswith("• and "):
            j += 1
        run = lines[i:j]
        if len(run) >= least:
            tail = run[0]
            for x in run[1:]:
                k = 0
                while k < min(len(tail), len(x)) and tail[-1 - k] == x[-1 - k]:
                    k += 1
                tail = tail[len(tail) - k:]
            cut = min([p for p in (tail.find(", "), tail.find("; ")) if p != -1], default=-1)
            tail = tail[cut + 2:] if cut != -1 else ""
            if len(tail) >= size and all(x.endswith(tail) for x in run):
                out.append(f"Each: {tail}")
                out += [x[:-len(tail)].rstrip(" ,;") for x in run]
                i = j
                continue
        if j > i:
            out += run                     # a short run, or no shared ending: as it is
            i = j
        else:
            out.append(lines[i])
            i += 1
    return out


def fit(text: str, limit: int, tail: str = " … (the rest is in the thread)") -> str:
    """The text cut at a word before ``limit`` characters, with ``tail``; unchanged when it fits."""
    if len(text) <= limit:
        return text
    room = limit - len(tail)
    clause = max(text.rfind("; ", 0, room), text.rfind(". ", 0, room), text.rfind(": ", 0, room))
    cut = clause if clause > room * 0.6 else text.rfind(" ", 0, room)   # a whole clause where one ends near the limit
    return text[:cut if cut > room // 2 else room].rstrip(" ,;:") + tail


# --- the whole post ---------------------------------------------------------------------------------------------------

def _lines(head: list[str]) -> list[str]:
    return [x for h in head for x in str(h).split("\n") if x.strip()]


def too_big(head: list[str]) -> bool:
    """True when the post breaks TOP_LINES or TOP_CHARS once cleaned (the composer shows fewer items then)."""
    cleaned = [("\n" if str(h).startswith("\n") else "") + clean(str(h).lstrip("\n")) for h in head]
    return len(_lines(cleaned)) > TOP_LINES or len("\n".join(cleaned)) > TOP_CHARS


def is_item(line: str) -> bool:
    return bool(re.match(r"\d+\. ", line))


def enforce(head: list[str], parts: list[str]) -> tuple[list[str], list[str]]:
    """The digest with every limit kept. The composer aims at them; this is the net: internal names cleaned, each item
    at most ITEM_CHARS, the post at most TOP_LINES and TOP_CHARS (its last items give way: they are in the thread),
    each thread line with at most MAX_NAMES names and a repeated note said once, each reply under REPLY_CHARS and
    REPLY_LINES, at most MAX_REPLIES replies."""
    out_head = []
    for h in head:
        pre = "\n" if str(h).startswith("\n") else ""
        body = clean(str(h).lstrip("\n"))
        if is_item(body):
            body = fit(body, ITEM_CHARS)
        out_head.append(pre + fit(body, ITEM_CHARS * 2, " …"))
    while (len(_lines(out_head)) > TOP_LINES or len("\n".join(out_head)) > TOP_CHARS) and \
            any(is_item(h.lstrip("\n")) for h in out_head):
        last = max(i for i, h in enumerate(out_head) if is_item(h.lstrip("\n")))
        out_head.pop(last)
    while len(_lines(out_head)) > TOP_LINES:
        out_head.pop(-2 if len(out_head) > 2 else -1)
    out_parts = []
    for p in parts[:MAX_REPLIES]:
        lines = [fit(cap_names(group_notes(clean(x))), LINE_CHARS, " …") for x in shared_tail(str(p).split("\n"))]
        kept, size = [], 0
        for i, x in enumerate(lines):
            more = len(lines) - i
            if len(kept) >= REPLY_LINES - 1 and more > 1 or size + len(x) + 1 > REPLY_CHARS - 60:
                kept.append(f"… and {more} more line{'s' if more != 1 else ''} (the inbox sheet)")
                break
            kept.append(x)
            size += len(x) + 1
        out_parts.append("\n".join(kept))
    return out_head, out_parts


def problems(head: list[str], parts: list[str]) -> list[str]:
    """Every limit the digest breaks, one line each (empty = fine)."""
    out = []
    top = _lines(head)
    if len(top) > TOP_LINES:
        out.append(f"top post has {len(top)} lines (max {TOP_LINES})")
    if len("\n".join(head)) > TOP_CHARS:
        out.append(f"top post has {len(chr(10).join(head))} characters (max {TOP_CHARS})")
    for x in top:
        if is_item(x) and len(x) > ITEM_CHARS:
            out.append(f"item over {ITEM_CHARS} characters: {x[:60]}…")
        if len(names(x)) > 3 and is_item(x):
            out.append(f"item names {len(names(x))} inboxes or domains in the post (max 3): {x[:60]}…")
    if len(parts) > MAX_REPLIES:
        out.append(f"{len(parts)} thread replies (max {MAX_REPLIES})")
    for k, p in enumerate(parts, 1):
        if len(p) > REPLY_CHARS:
            out.append(f"reply {k} has {len(p)} characters (max {REPLY_CHARS})")
        if len(p.split("\n")) > REPLY_LINES:
            out.append(f"reply {k} has {len(p.split(chr(10)))} lines (max {REPLY_LINES})")
        for x in p.split("\n"):
            if len(x) > LINE_CHARS:
                out.append(f"reply {k}: a line over {LINE_CHARS} characters: {x[:60]}…")
            if len(names(x)) > MAX_NAMES:
                out.append(f"reply {k}: a line names {len(names(x))} (max {MAX_NAMES}): {x[:60]}…")
            ends = {e for _, e in names(x)}
            notes = [m.group(1) for m in _NOTE.finditer(x) if m.start() in ends]
            if any(notes.count(n) > REPEATS for n in notes):
                out.append(f"reply {k}: a note repeated on one line: {x[:60]}…")
    for x in top + [y for p in parts for y in p.split("\n")]:
        out += [f"internal name ({w}) in: {x[:60]}…" for w in leaks(x)]
    seen: dict[str, int] = {}
    for x in top + [y for p in parts for y in p.split("\n")]:
        s = re.sub(r"^[\d.•–\s]+|For info[^:]*:\s*", "", x).strip()
        if len(s) > 60:
            seen[s] = seen.get(s, 0) + 1
    out += [f"said twice: {s[:60]}…" for s, n in seen.items() if n > 1]
    return out
