"""The Monday weekly test results post: what the weekly Smart Delivery v2 tests found, per client, in plain words (t420).

Manveen, 23 Nov 2026: "usually on the monday there should be a message for deliverbility tests". Since 16 Nov the PC
job's report went into the daily digest, and by 22 Nov it had shrunk to one line there. The digest keeps the actions
(what a person must do); this post is results only: one top-level post, at most one thread reply with the lists.

Nothing here judges an inbox. Every number is counted from what the v2 job stored (Mongo v2_verdicts: the verdict at
the inbox's own provider, so a Google seed result for an Outlook inbox is never counted as a fail), and every state
comes from a source that already decides it:
  * kept out / still in campaigns / already recovering: the sync's view of All Inboxes (Mongo inbox_status: its
    Busy Reason through usable.send_verdict, its campaigns, its state);
  * did not send because the provider removed the inbox: provider_removed.removed (ScaledMail or Zapmail data);
  * pre-warmed by a vendor: external_warmup marks;
  * when an inbox is tested next: dt_senders' record of the active weekly tests (inbox_status.weekly_tests);
  * what the bot did: the v2 job's own report line ("Took N inboxes out of M live campaigns · tagged ... · returned N").

    build(...) -> (head lines, thread parts)    pure; weekly_tests_post.py reads Mongo and posts
    problems(head, parts) -> [str]              every limit the text breaks (empty = fine)
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from smartlead import digest_guard as guard

IST = timezone(timedelta(hours=5, minutes=30))
TOP_LINES = 15          # non-blank lines in the top post (digest_guard counts the same way)
TOP_CHARS = 1400        # characters in the top post (digest_guard.TOP_CHARS)
PARA_CHARS = 330        # one client's paragraph: three lines in Slack on a laptop
WEEK_SPAN = 2           # the nights of one week's tests: the newest weekly night and the two days before it
FRESH_DAYS = 7          # a newest weekly night older than this is not this week's
JUDGED = ("PASS", "WATCH", "FAIL")
NOT_SENT = "NOT_SENT"
CLIENT_ORDER = ("Precise Leads", "Melior", "BettrData")
SPELLING = {"Bettrdata": "BettrData", "BETTRDATA": "BettrData", "PRECISE_LEADS": "Precise Leads", "MELIOR": "Melior"}
OWN = {"GMAIL": (("google",), "Google"), "OUTLOOK": (("microsoft",), "Outlook")}
BOTH = (("google", "microsoft"), "Google and Outlook")
SMARTLEAD_PCT = ("Smartlead's own % counts every spam seed at both providers; we judge each inbox at its own provider "
                 "(Google inboxes at Google, Outlook inboxes at Outlook), so the numbers differ. The counts here are "
                 "the ones the bot acts on.")
NO_ACTIONS = "Nothing to do from this post: any step for a person, the delete list included, is in the 11:00 digest."
NO_ACTIONS_OFF = "Nothing to do from this post: any step for a person comes in the bot's own posts."   # DIGEST_MODE=off
FULL_LISTS = "Full lists: the All Inboxes tab of the inbox sheet."


def client_name(c) -> str:
    c = str(c or "").strip()
    return SPELLING.get(c, c) or "No client"


def _client_key(c: str):
    return (CLIENT_ORDER.index(c) if c in CLIENT_ORDER else len(CLIENT_ORDER), c)


def _day(d) -> str:
    d = date.fromisoformat(str(d)[:10]) if not isinstance(d, date) else d
    return f"{d:%a} {d.day} {d:%b}"


def nights_title(nights) -> str:
    """['2026-11-21'] -> 'Sat 21 Nov'; two days in one month -> 'Sat 21 and Sun 22 Nov'."""
    ds = sorted(date.fromisoformat(n) for n in set(nights))
    if not ds:
        return ""
    if len(ds) == 1:
        return _day(ds[0])
    if len({(d.year, d.month) for d in ds}) == 1:
        heads = [f"{d:%a} {d.day}" for d in ds]
        return f"{', '.join(heads[:-1])} and {heads[-1]} {ds[-1]:%b}"
    return " and ".join(_day(d) for d in ds)


def _n(k: int, one: str = "inbox", many: str = "inboxes") -> str:
    return f"{k} {one if k == 1 else many}"


# --- which rows ------------------------------------------------------------------------------------------------------

def week_rows(docs, today: date) -> tuple[list[dict], list[str]]:
    """(newest row per inbox, the nights) of the latest week of weekly tests: the newest weekly night (v2_verdicts
    ``weekly``, not a history file) if it is at most FRESH_DAYS old, and the weekly nights up to WEEK_SPAN days before
    it (tests that start on Saturday and Sunday are one week). ([], []) when no weekly test is that recent."""
    weekly = [d for d in docs or [] if d.get("weekly") and not d.get("history") and d.get("night") and d.get("email")]
    if not weekly:
        return [], []
    newest = max(d["night"] for d in weekly)
    if date.fromisoformat(newest) < today - timedelta(days=FRESH_DAYS):
        return [], []
    start = (date.fromisoformat(newest) - timedelta(days=WEEK_SPAN)).isoformat()
    best: dict[str, dict] = {}
    for d in weekly:
        if start <= d["night"] <= newest:
            e = d["email"].lower()
            if e not in best or d["night"] > best[e]["night"]:
                best[e] = d
    rows = sorted(best.values(), key=lambda d: d["email"].lower())
    return rows, sorted({d["night"] for d in rows})


def newest_weekly_night(docs) -> str | None:
    nights = [d["night"] for d in docs or [] if d.get("weekly") and not d.get("history") and d.get("night")]
    return max(nights) if nights else None


# --- what the bot did --------------------------------------------------------------------------------------------------

_TOOK = re.compile(r"took (\d+) inbox(?:es)? out of (\d+) live campaigns?", re.I)
_TAGGED_BOTH = re.compile(r"tagged (\d+) left in paused/draft campaigns and (\d+) in no campaign", re.I)
_TAGGED_NONE = re.compile(r"tagged (\d+) in no campaign", re.I)
_RETURNED = re.compile(r"returned (\d+)", re.I)


def parse_did(text) -> dict | None:
    """The v2 job's own line 'Took 0 inboxes out of 0 live campaigns · tagged 15 in no campaign · returned 0' ->
    {took, campaigns, tagged_paused, tagged_none, returned}; None when the text has no such line."""
    m = _TOOK.search(str(text or ""))
    if not m:
        return None
    t = str(text)
    both = _TAGGED_BOTH.search(t)
    none = _TAGGED_NONE.search(t)
    ret = _RETURNED.search(t)
    return {"took": int(m.group(1)), "campaigns": int(m.group(2)),
            "tagged_paused": int(both.group(1)) if both else 0,
            "tagged_none": int(both.group(2)) if both else int(none.group(1)) if none else 0,
            "returned": int(ret.group(1)) if ret else 0}


def find_did(texts, title: str) -> dict | None:
    """The job's report line for this week's tests among ``texts`` (the v2-weekly digest report and its done lines):
    only a text that names the same tests ('tests of Sat 21 Nov') counts."""
    for t in texts or []:
        if f"tests of {title}" in str(t or ""):
            d = parse_did(t)
            if d is not None:
                return d
    return None


def taken_from(rows: list[dict], first_night: str) -> str:
    """Whose inboxes the job took out this week ('Melior, a.test'; 'Melior, a.test and b.test; BettrData, 3 domains'):
    v2_verdicts' pulled_on on or after the week's first night. Readers could not tell which inboxes "tagged 15" meant."""
    by: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if str(r.get("pulled_on") or "") >= first_night:
            by[client_name(r.get("client"))].add(str(r.get("domain") or r["email"].rsplit("@", 1)[-1]).lower())
    bits = []
    for c in sorted(by, key=_client_key):
        ds = sorted(by[c])
        bits.append(f"{c}, " + (" and ".join(ds) if len(ds) <= 2 else f"{len(ds)} domains"))
    return "; ".join(bits)


def did_line(did: dict | None, rows: list[dict], first_night: str) -> str:
    """'What the bot did: ...' from the job's report, or (no report found) from the stored rows: inboxes the job took
    out of use on or after this week's first night."""
    if did is None:
        k = sum(1 for r in rows if str(r.get("pulled_on") or "") >= first_night)
        if not k:
            return "What the bot did: nothing changed in Smartlead."
        who = taken_from(rows, first_night)
        return (f"What the bot did: took {_n(k, 'failed inbox', 'failed inboxes')} out of use (tagged DO NOT USE"
                f"{'; ' + who if who else ''}).")
    parts = []
    if did["took"]:
        parts.append(f"took {_n(did['took'], 'failed inbox', 'failed inboxes')} out of "
                     f"{_n(did['campaigns'], 'live campaign', 'live campaigns')}")
    if did["tagged_paused"]:
        parts.append(f"tagged {_n(did['tagged_paused'], 'failed inbox', 'failed inboxes')} left in paused or draft "
                     "campaigns")
    if did["tagged_none"]:
        k = did["tagged_none"]
        parts.append(f"tagged {_n(k, 'failed inbox', 'failed inboxes')} that {'was' if k == 1 else 'were'} in no "
                     "campaign")
    who = taken_from(rows, first_night)
    out_parts = [k for k in ("took", "tagged_paused", "tagged_none") if did[k]]
    if who and len(out_parts) == 1:
        parts[-1] += f" ({who})"
    elif who and out_parts:
        parts.append(f"those inboxes: {who}")
    if did["returned"]:
        k = did["returned"]
        parts.append(f"returned {_n(k)} that passed again to {'its' if k == 1 else 'their'} campaigns")
    if not parts:
        return "What the bot did: nothing changed in Smartlead."
    if not did["took"]:
        parts.append("no live campaign lost an inbox")
    return "What the bot did: " + "; ".join(parts) + "."


# --- one inbox's facts ------------------------------------------------------------------------------------------------

def _own(row: dict) -> tuple[tuple[str, ...], str]:
    return OWN.get(str(row.get("type") or "").upper(), BOTH)


def _score(rows: list[dict]) -> tuple[int, int, str]:
    """(seeds in the inbox, seeds sent, provider word) at the inboxes' own provider."""
    keys, word = _own(rows[0])
    got = sum(int(((r.get(k) or {}).get("inbox")) or 0) for r in rows for k in keys)
    sent = sum(int(((r.get(k) or {}).get("sent")) or 0) for r in rows for k in keys)
    return got, sent, word


def fail_state(st: dict | None) -> str:
    """Where a failed inbox stands now, from the sync's view of All Inboxes: recovering (tagged, warm-up repairing),
    live (still in a campaign), out (in no campaign and the gate stops it), free (in no campaign, nothing stops it)."""
    from smartlead import usable
    st = st or {}
    if st.get("state") == "recovering":
        return "recovering"
    if st.get("campaigns"):
        return "live"
    verdict, _ = usable.send_verdict(st.get("busy_reason"), st.get("provider") or "")
    return "free" if verdict == "send" and st else "out"


STATE_MANY = {"recovering": "already tagged and recovering", "out": "kept out of campaigns",
              "live": "still in campaigns", "free": "in no campaign and not tagged"}
STATE_ONE = {"recovering": "Already tagged and recovering.", "out": "Kept out of campaigns.",
             "live": "Still in campaigns.", "free": "In no campaign and not tagged."}


OUT_WHY = {"never": "tagged DO NOT USE", "hold": "held back with no passing test yet",
           "cannot": "Smartlead can't send from them"}


def _state_one(state: str, rows: list[dict], status: dict) -> str:
    """One domain's state sentence; "kept out" says how when its inboxes are kept out in more than one way (readers
    asked why the bot tagged 15 of a domain's 25: the other 10 were held back by the inbox list, not tagged)."""
    from smartlead import usable
    if state != "out":
        return STATE_ONE[state]
    how: dict[str, int] = defaultdict(int)
    for r in rows:
        s = status.get(r["email"].lower()) or {}
        how[usable.send_verdict(s.get("busy_reason"), s.get("provider") or "")[0]] += 1
    if len(how) < 2 or not set(how) <= set(OUT_WHY):
        return STATE_ONE[state]
    return "Kept out of campaigns: " + ", ".join(f"{how[k]} {OUT_WHY[k]}" for k in OUT_WHY if how.get(k)) + "."


def not_sent_reason(email: str, st: dict | None, removed: dict) -> str:
    """Why an inbox sent nothing in the test: its provider removed it (provider_removed), Smartlead can't log in to it
    (All Inboxes: disconnected), or the test itself came back short."""
    from smartlead import usable
    r = removed.get(email)
    if r:
        if r.get("provider") == "ScaledMail":
            return "scaledmail"
        return "zapmail-expired" if "expired" in str(r.get("why") or "") else "zapmail"
    _, codes = usable.send_verdict((st or {}).get("busy_reason"), (st or {}).get("provider") or "")
    if "disconnected" in codes:
        return "login"
    return "short"


REASON_ALONE = {"scaledmail": "the cancelled ScaledMail inboxes, already on the delete list",
                "zapmail": "removed by Zapmail, on the delete list",
                "zapmail-expired": "their Zapmail domain expired, on the delete list",
                "login": "Smartlead can't log in to them",
                "short": "the test came back short (Smartlead sent them fewer than 4 seeds)"}
REASON_PART = {"scaledmail": "cancelled at ScaledMail", "zapmail": "removed by Zapmail",
               "zapmail-expired": "Zapmail domain expired", "login": "Smartlead can't log in",
               "short": "the test came back short"}
REASON_ORDER = ("scaledmail", "zapmail", "zapmail-expired", "login", "short")
REMOVED = ("scaledmail", "zapmail", "zapmail-expired")


# --- the post --------------------------------------------------------------------------------------------------------

def _by_domain(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        out[str(r.get("domain") or r["email"].rsplit("@", 1)[-1]).lower()].append(r)
    return dict(sorted(out.items()))


def _fail_sentences(fails: list[dict], status: dict) -> tuple[list[str], list[str]]:
    """(top-post sentences, thread lines) for one client's failed inboxes: one sentence per own provider. One domain
    is named with its score and state; several domains in one state are counted; several states are one list."""
    groups: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in fails:
        groups[_own(r)[1]][fail_state(status.get(r["email"].lower()))].append(r)
    order = ("live", "free", "out", "recovering")
    top, thread = [], []
    for word in sorted(groups):
        states = sorted(groups[word].items(), key=lambda kv: order.index(kv[0]))
        rs = [r for _, x in states for r in x]
        doms = _by_domain(rs)
        if len(doms) == 1 and len(states) == 1:
            (dom, drs), = doms.items()
            got, sent, _ = _score(drs)
            top.append(f"{dom} failed: {_n(len(drs))}, {got} of {sent} reached the inbox at {word}. "
                       f"{_state_one(states[0][0], drs, status)}")
        elif len(states) == 1:
            top.append(f"{len(rs)} failed at {word}: the {len(doms)} domains {STATE_MANY[states[0][0]]}.")
        else:
            top.append(f"{len(rs)} failed at {word} on {len(doms)} domains: "
                       + ", ".join(f"{len(x)} {STATE_MANY[k]}" for k, x in states) + ".")
        for state, x in states:
            names = [f"{dom} {_score(drs)[0]} of {_score(drs)[1]}" for dom, drs in _by_domain(x).items()]
            thread.append(f"• Failed at {word}, {STATE_MANY[state]} ({_n(len(x))}; seeds that reached the inbox at "
                          f"{word}): " + ", ".join(names))
    return top, thread


def _not_sent_sentence(rows: list[dict], status: dict, removed: dict) -> tuple[str, list[str]]:
    by: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        e = r["email"].lower()
        by[not_sent_reason(e, status.get(e), removed)].append(r)
    keys = [k for k in REASON_ORDER if k in by]
    if len(keys) == 1:
        top = f"{len(rows)} did not send: {REASON_ALONE[keys[0]]}."
    else:
        bits = [f"{len(by[k])} {REASON_PART[k]}" for k in keys]
        tail = " (the removed ones are already on the delete list)" if any(k in REMOVED for k in keys) else ""
        top = f"{len(rows)} did not send: {'; '.join(bits)}{tail}."
    thread = [_list_line(f"• Did not send, {REASON_PART[k]}", by[k]) for k in keys]
    return top, thread


def _list_line(head: str, rows: list[dict]) -> str:
    """'• Passed (inboxes per domain): a.test 3, b.test 25 and 9 more domains': at most guard.MAX_NAMES names, and the
    rest said as domains (readers took 'and 10 more' for inboxes)."""
    line = guard.cap_names(f"{head} (inboxes per domain): " + ", ".join(f"{d} {len(x)}" for d, x in _by_domain(rows).items()))
    return re.sub(r" and (\d+) more$", lambda m: f" and {m.group(1)} more domain{'s' if m.group(1) != '1' else ''}", line)


def _passed_lines(rows: list[dict], status: dict) -> list[str]:
    return [_list_line("• Passed", rows)]


def not_judged(rows: list[dict], status: dict, removed: dict, marks: dict, tests, ever: set[str],
               today: date) -> list[dict]:
    """Inboxes this week's tests did not judge: a stored result that is not a verdict (INCOMPLETE: some seeds had not
    come in), and inboxes in an active weekly test with no result this week (new, or added after the test ran).
    [{client, n, kind: 'incomplete' | 'first' | 'missing', vendor, next, domains}] grouped."""
    tested = {r["email"].lower() for r in rows}
    next_of: dict[str, str] = {}
    for t in tests or []:
        if t.get("copy") or str(t.get("campaign") or "").strip().lower().endswith(" copy check"):
            continue
        day = _ist_date(t.get("next_run"))
        for e in t.get("senders") or []:
            e = str(e).lower()
            if day and (e not in next_of or day < next_of[e]):
                next_of[e] = day
    items: dict[tuple, list[str]] = defaultdict(list)
    for r in rows:
        if r.get("verdict") not in JUDGED and r.get("verdict") != NOT_SENT:
            e = r["email"].lower()
            items[(client_name(r.get("client")), "incomplete", _vendor(e, marks), next_of.get(e))].append(e)
    for e, day in next_of.items():
        st = status.get(e)
        if e in tested or e in removed or not st or st.get("state") == "gone":
            continue
        items[(client_name(st.get("client")), "first" if e not in ever else "missing", _vendor(e, marks), day)].append(e)
    out = []
    for (client, kind, vendor, day), es in sorted(items.items(), key=lambda kv: (_client_key(kv[0][0]), kv[0][1:].__str__())):
        out.append({"client": client, "kind": kind, "vendor": vendor, "next": day, "n": len(es),
                    "domains": sorted({x.rsplit("@", 1)[-1] for x in es})})
    return out


def _vendor(email: str, marks: dict) -> str | None:
    """The vendor that warms this inbox (external_warmup marks: by address or by domain)."""
    return marks.get(email) or marks.get(email.rsplit("@", 1)[-1])


def _ist_date(iso) -> str | None:
    try:
        d = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    d = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return d.astimezone(IST).date().isoformat()


def _not_judged_bit(x: dict) -> str:
    who = f"{x['client']}'" if x["client"].endswith("s") else f"{x['client']}'s"
    pre = f" pre-warmed {x['vendor']}" if x.get("vendor") else ""
    what = f"{who} {x['n']}{pre} {'inbox' if x['n'] == 1 else 'inboxes'}"
    if x["kind"] == "first":
        return f"{what} (first weekly test {_day(x['next'])})"           # 'first' needs a test: it always has a day
    when = f" (next test {_day(x['next'])})" if x.get("next") else ""
    if x["kind"] == "missing":
        return f"{what} not in this week's results{when}"
    return f"{what} with no full result{when}"


def build(rows: list[dict], nights: list[str], status: dict, removed: dict, marks: dict, tests, ever: set[str],
          did: dict | None, today: date) -> tuple[list[str], list[str]]:
    """The post: (head lines, thread parts). ``rows``: week_rows. ``status``: {email: inbox_status doc}. ``removed``:
    {email: provider_removed row}. ``marks``: {email: vendor} (external_warmup). ``tests``: inbox_status.weekly_tests
    (None: not known). ``ever``: every inbox with a weekly row in v2_verdicts (a one-off test is not its first weekly test). ``did``: find_did (None: not found)."""
    title = nights_title(nights)
    head = [f"Weekly test results · tests of {title}"]
    thread = [SMARTLEAD_PCT]
    sections: list[tuple[str, list[str], list[str]]] = []      # (client, fail and not-sent lines, the other lists)
    paras: list[tuple[str, str]] = []            # (paragraph, its short form for a day when the post is too long)
    by_client: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_client[client_name(r.get("client"))].append(r)
    for client in sorted(by_client, key=_client_key):
        rs = by_client[client]
        passed = [r for r in rs if r.get("verdict") == "PASS"]
        watch = [r for r in rs if r.get("verdict") == "WATCH"]
        fails = [r for r in rs if r.get("verdict") == "FAIL"]
        unsent = [r for r in rs if r.get("verdict") == NOT_SENT]
        if not (passed or watch or fails or unsent):
            continue                                  # only not-judged inboxes: they are in the "Not judged yet" line
        bits, lines, extra = [f"{len(passed)} passed."], [], []
        if watch:
            bits.append(f"{len(watch)} borderline (2 or 3 of 4 seeds reached the inbox).")
            extra.append(_list_line("• Borderline", watch))
        if fails:
            top, more = _fail_sentences(fails, status)
            bits += top
            lines += more
        if unsent:
            top, more = _not_sent_sentence(unsent, status, removed)
            bits.append(top)
            lines += more
        if passed:
            extra += _passed_lines(passed, status)
        short = [f"{len(passed)} passed"] + [f"{len(x)} {w}" for x, w in ((watch, "borderline"), (fails, "failed"),
                                                                           (unsent, "did not send")) if x]
        brief = f"{client}: {', '.join(short)} (details in the thread)."
        full = f"{client}: " + " ".join(bits)
        paras.append((full if len(full) <= PARA_CHARS else brief, brief))    # never a sentence cut in half
        sections.append((client, lines, extra))
    head += [x for p, _ in paras for x in ("", p)]
    room = guard.REPLY_LINES - 8                  # the % line, the not-judged list, the closing line and blanks
    full = sum(2 + len(a) + len(b) for _, a, b in sections)
    for client, lines, extra in sections:         # a long week: the passed lists give way first (the sheet has them)
        thread += ["", f"*{client}*"] + lines + (extra if full <= room else [x for x in extra if x.startswith("• Bord")])
    head += ["", did_line(did, rows, min(nights) if nights else today.isoformat())]
    tail = ["", NO_ACTIONS]
    nj = not_judged(rows, status, removed, marks, tests, ever, today)
    if nj:
        head += ["", guard.fit("Not judged yet: " + "; ".join(_not_judged_bit(x) for x in nj) + ".", guard.ITEM_CHARS,
                               " … (the rest is in the thread)")]
        thread += ["", "*Not judged yet*"] + [re.sub(r" and (\d+) more$", r" and \1 more domains", guard.cap_names(
            f"• {x['client']}, {_n(x['n'])} on: " + ", ".join(x["domains"]))) for x in nj]
    head += tail
    thread += ["", FULL_LISTS]
    head = shorten([guard.clean(h) for h in head], {guard.clean(p): guard.clean(q) for p, q in paras})
    _, parts = guard.enforce([], ["\n".join(thread)])
    return head, parts


def shorten(head: list[str], short: dict[str, str]) -> list[str]:
    """The post within TOP_CHARS: the longest client paragraphs give way to their short form (counts only; the
    detail is in the thread) until it fits."""
    head = list(head)
    while len("\n".join(head)) > TOP_CHARS:
        long = [i for i, h in enumerate(head) if h in short and short[h] != h]
        if not long:
            break
        i = max(long, key=lambda k: len(head[k]))
        head[i] = short[head[i]]
    return head


def no_results(today: date, newest: str | None, heartbeat: dict | None) -> tuple[list[str], str]:
    """(the one-line post, the bot-health message) for a week with no weekly test results stored."""
    since = f" since {_day(newest)}" if newest else ""
    head = [f"Weekly test results · week of {_day(today)}",
            "", f"No weekly test results came in this week (none stored{since}). The bot team has been told why."]
    last = (heartbeat or {}).get("last_run")
    ran = f"; the PC job last ran {_day(_ist_date(last))} {_hhmm(last)} IST" if _ist_date(last) else \
        "; the PC job has no heartbeat"
    dev = (f"Weekly test results post ({_day(today)}): no weekly v2 results in Mongo from the last {FRESH_DAYS} days "
           f"(newest weekly night: {_day(newest) if newest else 'none'}{ran}). Check the task 'InfraBot v2 weekly' on "
           "Manveen's PC and that the Saturday tests ran in Smart Delivery v2.")
    return head, dev


def _hhmm(iso) -> str:
    try:
        d = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return ""
    d = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return f"{d.astimezone(IST):%H:%M}"


def problems(head: list[str], parts: list[str]) -> list[str]:
    """Every limit the post breaks (empty = fine): the top post's lines and characters, internal names, and the
    digest's thread limits (digest_guard) on the one reply."""
    out = []
    text = "\n".join(head)
    lines = [x for x in text.split("\n") if x.strip()]
    if len(lines) > TOP_LINES:
        out.append(f"top post has {len(lines)} lines (max {TOP_LINES})")
    if len(text) > TOP_CHARS:
        out.append(f"top post has {len(text)} characters (max {TOP_CHARS})")
    if len(parts) > 1:
        out.append(f"{len(parts)} thread replies (max 1)")
    for x in head:
        out += [f"internal name ({w}) in: {x[:60]}…" for w in guard.leaks(x)]
        if len(guard.names(x)) > 3:
            out.append(f"top post line names {len(guard.names(x))} inboxes or domains (max 3): {x[:60]}…")
        if re.search(r"\bDo:", x):
            out.append(f"an action in the results post: {x[:60]}…")
    out += guard.problems([], parts)
    return out
