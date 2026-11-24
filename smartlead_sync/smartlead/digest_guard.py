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
    pack(blocks, k) -> [str]                  items (one block each) as at most k replies, never one item in two

24 Nov 2026 (t403): the digest sent three replies inside these limits and Slack showed four, one of them a lone
"How:" line cut off from its item. Slack turns every address and bare domain into a link (<mailto:a@x|a@x>,
<http://x|x>) before it counts a message's 4,000 characters, so a 3,195-character reply naming ~60 inboxes was
~4,100 to Slack, which split it at the line that crossed 4,000. Reply sizes are now measured as Slack counts them
(slack_len), and the composer packs whole items into replies (pack): an item that does not fit is shortened inside
its reply (its list, never its How), and what does not fit at all is one pointer line, not another reply.
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
REPLY_CHARS = 3500     # one thread reply AS SLACK COUNTS IT (slack_len): Slack shows a message over 4,000 as more posts
REPLY_LINES = 40       # one thread reply in lines (notify._THREAD_CHUNK: one part is one Slack reply)
ROOM_LINES = REPLY_LINES - 2    # what the packer (pack) puts in one reply: room left for the guard's pointer line
ROOM_CHARS = REPLY_CHARS - 120
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


_MARKUP = re.compile(r"(<[^<>]+>)")
_LINK_EXTRA = len("<mailto:|>")             # == len("<http://|>"): what Slack adds around a name besides the name again


def slack_len(text: str) -> int:
    """The length of a message as Slack counts it when it decides to show it as more than one post: outside the links
    we wrote, '&', '<' and '>' are escaped and every address and bare domain becomes a link that holds the name twice
    (24 Nov 2026: 3,195 characters we sent were ~4,100 to Slack, so it split the reply)."""
    n = 0
    for piece in _MARKUP.split(str(text or "")):
        if piece.startswith("<") and piece.endswith(">"):
            n += len(piece)
            continue
        n += len(piece) + 4 * piece.count("&") + 3 * (piece.count("<") + piece.count(">"))
        taken: set[int] = set()
        for m in _EMAIL.finditer(piece):
            n += _LINK_EXTRA + len(m.group(0))
            taken.update(range(m.start(), m.end()))
        for m in _DOMAIN.finditer(piece):
            if m.start() not in taken and m.group(0).rsplit(".", 1)[-1].lower() not in _FILE_ENDS:
                n += _LINK_EXTRA + len(m.group(0))
    return n


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


# --- thread replies: whole items (t403) --------------------------------------------------------------------------------

def reply_lines(lines: list[str]) -> list[str]:
    """Thread lines as they are posted: internal names cleaned, an ending a run of lines shares said once, a repeated
    note said once, at most MAX_NAMES names and LINE_CHARS characters on a line."""
    return [fit(cap_names(group_notes(clean(x))), LINE_CHARS, " …") for x in shared_tail(list(lines))]


def fits(lines: list[str]) -> bool:
    """True when the lines fit the room of one packed reply (ROOM_LINES, ROOM_CHARS as Slack counts them)."""
    return len(lines) <= ROOM_LINES and slack_len("\n".join(lines)) <= ROOM_CHARS


_MORE = re.compile(r"^• and (\d+) more\b")
_STEP = ("How:", "Careful:")
_NUM = re.compile(r"^(\d+)(?: above:|\. )")
STARTS_INSIDE = ("How:", "Careful:", "• ", "Each:", "… and")      # lines that belong to an item above them


def _split_block(block: list[str]) -> tuple[list[str], list[str], list[str], int]:
    """(first line, list, How and Careful lines, how many list lines an '• and N more' line already stands for)."""
    end = next((i for i, x in enumerate(block) if i and x.startswith(_STEP)), len(block))
    head, rows, tail = block[:1], block[1:end], block[end:]
    hidden = 0
    if rows and _MORE.match(rows[-1]):
        hidden, rows = int(_MORE.match(rows[-1]).group(1)), rows[:-1]
    return head, rows, tail, hidden


def cut_list(block: list[str], keep: int) -> list[str]:
    """One item's block with at most ``keep`` lines of its list (the lines between its first line and its How) and
    one '• and N more (the inbox sheet)' line for the rest; its first line and its How and Careful lines stay."""
    head, rows, tail, hidden = _split_block(block)
    if keep >= len(rows):
        return block
    more = hidden + sum(1 for x in rows[keep:] if x.strip() and not x.startswith("Each:"))
    return head + rows[:keep] + [f"• and {more} more (the inbox sheet)"] + tail


def shorten(block: list[str], ok=fits) -> list[str] | None:
    """The longest version of one item's block (cut_list) that passes ``ok``; None when even the item without its
    list does not."""
    if ok(block):
        return block
    for keep in range(len(_split_block(block)[1]) - 1, -1, -1):
        out = cut_list(block, keep)
        if ok(out):
            return out
    return None


def _ranges(nums: list[int]) -> str:
    nums, out, i = sorted(set(nums)), [], 0
    while i < len(nums):
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        out.append(str(nums[i]) if i == j else f"{nums[i]}-{nums[j]}")
        i = j + 1
    return ", ".join(out)


def _pointer(left: list[list[str]]) -> str:
    """The one line that stands for the blocks that did not fit (never a reply of their own)."""
    nums = [int(m.group(1)) for b in left if b for m in [_NUM.match(str(b[0]))] if m]
    if nums:
        many = len(set(nums)) != 1
        return (f"… item{'s' if many else ''} {_ranges(nums)} did not fit here: {'their lists are' if many else 'its list is'}"
                f" in the inbox sheet.")
    k = sum(1 for b in left for x in b if str(x).strip())
    return f"… and {k} more line{'s' if k != 1 else ''} (the inbox sheet)"


def _greedy(blks: list[list[str]], k: int, last: bool) -> tuple[list[list[str]], list[list[str]]]:
    """(replies, blocks left over): whole blocks in order, a new reply when the next one does not fit, at most ``k``.
    ``last``: a block that does not fit the last reply is shortened there (its list), and what still does not fit is
    left over."""
    parts: list[list[str]] = []
    cur: list[str] = []
    for n, blk in enumerate(blks):
        sep = [""] if cur else []
        if fits(cur + sep + blk):
            cur += sep + blk
            continue
        if cur and len(parts) < k - 1:
            parts.append(cur)
            cur, sep = [], []
            if fits(blk):
                cur = list(blk)
                continue
        if not last:
            return parts + ([cur] if cur else []), blks[n:]
        small = shorten(blk, lambda b: fits(cur + sep + b))
        if small is not None:
            cur += sep + small
            continue
        if not cur:                            # one block too big even without its list: its lines while they fit
            for x in blk:
                if not fits(cur + [x]):
                    break
                cur.append(x)
            return parts + [cur], [blk[len(cur):]] + blks[n + 1:]
        return parts + [cur], blks[n:]
    return parts + ([cur] if cur else []), []


def pack(blocks: list[list[str]], k: int, ready: bool = False) -> list[str]:
    """Blocks (one per item, or per section) as at most ``k`` replies that each fit as Slack counts them (fits), in
    order, never one block in two replies. When they do not all fit, every item's list is cut to the same number of
    lines (the most that fit) before any item is left out, so each item keeps its first line and its How; only when even
    that does not fit, the last reply shortens what it can and one pointer line names the items left out (never a
    reply of their own). ``ready``: the lines are already as posted (reply_lines)."""
    blks = [b for b in ((list(r) if ready else reply_lines(r)) for r in blocks) if b]
    longest = max((len(_split_block(b)[1]) for b in blks), default=0)
    for keep in [None] + list(range(longest - 1, -1, -1)):      # the most list lines every item can keep
        trial = blks if keep is None else [cut_list(b, keep) for b in blks]
        parts, left = _greedy(trial, k, last=False)
        if not left:
            return ["\n".join(p) for p in parts]
    parts, left = _greedy(blks, k, last=True)
    if left:
        if not parts:
            parts = [[]]
        parts[-1].append(_pointer(left))
    return ["\n".join(p) for p in parts if p]


def blocks_of(lines: list[str]) -> list[list[str]]:
    out: list[list[str]] = [[]]
    for x in lines:
        if x.strip():
            out[-1].append(x)
        elif out[-1]:
            out.append([])
    return [b for b in out if b]


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
        lines = reply_lines(str(p).split("\n"))
        if len(lines) > REPLY_LINES or slack_len("\n".join(lines)) > REPLY_CHARS:
            out_parts += pack(blocks_of(lines), 1, ready=True)      # cut at an item's end, sized as Slack counts
        else:
            out_parts.append("\n".join(lines))
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
        if slack_len(p) > REPLY_CHARS:
            out.append(f"reply {k} has {slack_len(p)} characters as Slack counts them (max {REPLY_CHARS})")
        first = next((x for x in p.split("\n") if x.strip()), "")
        if first.startswith(STARTS_INSIDE):
            out.append(f"reply {k} starts inside an item: {first[:60]}…")
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
