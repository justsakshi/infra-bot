"""The daily inbox digest: one short post a morning (IST), addressed to Manveen, with only what a person must do
today, one line each (what, where, the one step), one line for jobs that failed, and at most three short thread
replies (the full lists; what was left out on purpose and what the bot did; where the inboxes stand). Jobs hand their
part over through smartlead/digest.py.

    python digest_post.py            # dry run: prints the digest, posts nothing, marks nothing
    python digest_post.py --post     # post it (the bot runs this every morning; once a day, --again to repeat)

Besides what the jobs hand over, the digest checks three things itself: the PC's v2 weekly job ran in the last
3 hours (Mongo v2_heartbeat), Render runs the latest code on main (RENDER_GIT_COMMIT against GitHub), and a sheet
sync has finished since the last 10:00 IST slot (the sync's own report time in digest_asks).

What it leaves out (18 Nov 2026, "why did i get these 13 messages"): the kill list told by several jobs is one item;
anything a recorded decision covers (Mongo digest_holds, see smartlead/digest.py) is left out and listed with its
reason, owner and date in the thread; an ask that points at a list it does not carry is left out and counted.
19 Nov 2026 ("i got this and it's long spammy i can not unserstand anything"): asks about the same inboxes are one
item, a failed job that has run fine since is not reported, the bot's own words are put plainly, and each job's own
report is no longer copied into the thread.
20 Nov 2026 ("this is still not fixed"): a job can hand over a fact with no step (kind "info", e.g. inboxes in the
weekly test whose last runs came back short). It is one "For info" line, after the things to do and not counted
among them, with its list in the thread.
20 Nov 2026 (t352, "if they are rmoved don't remind me just remid me to delte"): inboxes their provider removed
(smartlead/provider_removed.py: ScaledMail or Zapmail data, never a login failure alone) are one delete reminder,
and every other item is shown without them. Since t353 the reminder is in two lists, "delete now" and "check first"
(still in a real campaign, or a lead replied in the last 30 days), and says where in its own text.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from smartlead import digest as dg
from smartlead.config import DEFAULT_SHEET_ID
from smartlead.usable import TEAM_GROUPS

IST = dg.IST
MAX_ASK_AGE = timedelta(days=8)      # a job that stopped reporting for longer than this: its asks are dropped
STALE = timedelta(hours=30)          # older than this, an ask says when its job last checked
MAX_SHOWN = 4                        # things in the post itself; the rest are in the thread (19 Nov 2026)
MAX_PARTS = 3                        # thread replies, at most
PART_LINES = 38                      # lines per reply (notify._THREAD_CHUNK is 40: one part is one reply)
LIST_LINES = 12                      # lines of one item's list in the thread
INFO_LINES = 3                       # lines of a "For info" item's list in the thread

PC_STALE = timedelta(hours=3)
REPO = os.getenv("RENDER_GIT_REPO_SLUG") or "justsakshi/infra-bot"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{DEFAULT_SHEET_ID}"
WAIT_FOR = ("smartlead-sync", "fleet-watch")   # jobs whose asks the digest waits for when they are still running
WAIT_MAX_S = 30 * 60

LABELS = {"v2-weekly": "v2 weekly test (PC job)", "inbox-status": "Inboxes", "billing-watch": "Billing watch",
          "renewal-review": "Bills in the next 7 days", "fleet-watch": "Fleet watch", "key-health": "API keys",
          "blacklist": "Blocklists", "campaign-metrics": "Campaign stats job", "copy-sync": "Test copy",
          "dt-senders": "Test campaigns", "gate-selftest": "Gate self-test", "reply-monitor": "Reply check",
          "kill-report": "Kill warnings", "bot-health": "Bot health", "digest": "Digest checks",
          "real-recipients": "Real-recipient check", "placement-report": "Classic seed test",
          "zapmail-digest": "Domains and inboxes (Zapmail, ScaledMail)", "fleet-scorecard": "Fleet scorecard",
          "inbox-intake": "New inboxes set up", "inbox-ready": "ScaledMail inboxes made ready",
          "smartlead-sync": "Daily sheet sync", "warmup-repair": "Warm-up repair",
          "inbox-evidence": "Inbox evidence", "volume-review": "Volume review",
          "provider-removed": "Inboxes removed by their provider"}


# What each All Inboxes group means, in a few words (21 Nov 2026, Manveen: "i think this still is not readable";
# the numbers reply gave the words with no meaning). usable.team_group decides the group; this only explains it.
GROUP_MEANS = {"Safe to send": "cleared to send, warm-up fine",
               "Not proven yet": "held back until a weekly test passes",
               "Recovering": "warm-up under 90%, being repaired",
               "Do not use": "tagged, failed a test or can't connect"}


def _day(dt: datetime) -> str:
    d = dt.astimezone(IST)
    return f"{d:%a} {d.day} {d:%b}"


def _parse(ts) -> datetime | None:
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(ts))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def collect(sources: list[dict], now: datetime) -> tuple[list[dict], list[dict]]:
    """(asks, broken) from every job's last report, each with its source and how old its job's check is."""
    asks, broken = [], []
    for s in sources:
        at = _parse(s.get("at"))
        if not at or now - at > MAX_ASK_AGE:
            continue
        for a in s.get("asks") or []:
            item = {**a, "source": s["_id"], "checked": at}
            (broken if a.get("kind") == "broken" else asks).append(item)
    order = lambda a: (not a.get("urgent"), _parse(a.get("since")) or now, a["source"])  # noqa: E731
    return sorted(asks, key=order), sorted(broken, key=order)


def _head(line: str) -> str:
    """What a list line is about: the part before its first ': ' or ' (' ('a.test (Client, 3 inboxes)' -> 'a.test')."""
    cut = [i for i in (line.find(": "), line.find(" (")) if i > 0]
    return line[:min(cut)] if cut else line


def _where(a: dict) -> str:
    """What an item is about, in a few words: the domains (or, without any, the heads of its list lines) of its list,
    the first three and how many more. Never a whole list line (21 Nov 2026: "Drop: x.com (3 inboxes: tagged ...
    (t235))" glued onto an item made it unreadable); the full list is in the thread. An item that says where in its
    own text carries "where" (t353)."""
    if a.get("where") is not None:
        return dg.plain(a["where"])
    lines = [dg.plain(x.lstrip("•-– ").strip()) for x in dg.bullets(a.get("detail"))]
    if not lines:
        return ""
    heads = []
    for x in lines:
        inboxes, domains = dg.items_of({"text": x})
        found = sorted(domains | {i.rsplit("@", 1)[1] for i in inboxes})
        heads += found or [_head(x)]
    heads = list(dict.fromkeys(heads))
    return ", ".join(heads[:3]) + (f" and {len(heads) - 3} more" if len(heads) > 3 else "")


def _names_where(text: str, where: str) -> bool:
    """True when the item's own text already names what its list is about (its first domain or name)."""
    first = where.split(",")[0].split(" and ")[0].strip().lower()
    return bool(first) and first in text.lower()


def _tail(a: dict, now: datetime) -> str:
    since, checked = _parse(a.get("since")), a.get("checked")
    tail = []
    if since and now - since > timedelta(hours=20):
        tail.append(f"Open since {_day(since)}.")
    if checked and now - checked > STALE:
        tail.append(f"(last checked {_day(checked)})")
    return (" " + " ".join(tail)) if tail else ""


def _label(a: dict) -> str:
    return LABELS.get(a.get("source"), a.get("source") or "")


def _what(a: dict) -> str:
    """What an item is about, in its own words, plus "Which: ..." when its text names none of what its list holds."""
    text = dg.plain(a.get("text")).strip()
    if text and text[-1] not in ".!?)":
        text += "."
    where = _where(a)
    if where and not _names_where(text, where):
        text += f" Which: {where}."
    return text


def _info_line(infos: list[dict]) -> str:
    """The one line for facts with no step (kind "info"): the first in full, how many more are in the thread."""
    k = len(infos) - 1
    text = dg.plain(infos[0].get("text")).strip()
    text = text if not text or text[-1] in ".!?)" else text + "."
    return f"For info, nothing to do: {text}" + (f" ({k} more in the thread.)" if k else "")


def _item_line(i: int, a: dict, now: datetime) -> str:
    """One thing to do, in one paragraph: what, where, the one step. Items about the same inboxes say so."""
    text = _what(a)
    step = f" Do: {dg.cap(dg.plain(a['step']).strip())}" if a.get("step") else ""
    also = " Other checks flag the same inboxes too (thread)." if a.get("also") else ""
    return f"{i}. {text}{step}{also}{_tail(a, now)}"


def _unlabel(a: dict) -> str:
    """An item's text without its job's name in front ('Gate self-test: 24 inboxes ...' -> '24 inboxes ...')."""
    text = dg.plain(a.get("text")).strip()
    lab = _label(a)
    return text[len(lab) + 1:].strip() if lab and text.lower().startswith(lab.lower() + ":") else text


def one_per_job(broken: list[dict]) -> list[dict]:
    """One bot-health line per job: 'failed' before 'never finished' before 'did not run' (19 Nov 2026: 'Campaign
    stats post failed' and 'Campaign stats post started Wed 18 Nov and never finished' were two lines)."""
    rank = {"failed": 0, "unfinished": 1, "missed": 2}
    best: dict[str, dict] = {}
    out = []
    for b in broken:
        m = re.match(r"bot-health:(failed|unfinished|missed):(.+)$", str(b.get("key") or ""))
        if not m:
            out.append(b)
            continue
        old = best.get(m.group(2))
        if old is None:
            best[m.group(2)] = b
            out.append(b)
        elif rank[m.group(1)] < rank[old["key"].split(":")[1]]:
            out[out.index(old)] = b
            best[m.group(2)] = b
    return out


def _job_name(b: dict) -> str:
    """The job a broken line is about: bot health names it at the start of its text, the digest's own checks carry
    a name, any other job is its own label."""
    if str(b.get("key") or "").startswith("bot-health:"):
        return re.split(r" (?:failed|started|did not run)\b", str(b.get("text") or ""))[0].strip()
    return b.get("name") or _label(b)


def recovered(key: str, runs, now: datetime) -> bool:
    """True when a bot-health line ('X failed', 'X never finished', 'X did not run') is old news: the job's latest
    run, not counting one still going, ended fine. 19 Nov 2026: three 18 Nov runs 'never finished' sat in the digest
    although the sync had run fine since (PR #101 re-runs a sync cut off by a restart)."""
    m = re.match(r"bot-health:(?:failed|unfinished|missed):(.+)$", str(key or ""))
    if not m:
        return False
    job = m.group(1)
    over = []
    for r in runs or []:
        if r.get("job") != job or r.get("skipped"):
            continue
        started = _parse(r.get("started"))
        if not started:
            continue
        if r.get("ended") or r.get("lost") or now - started > timedelta(hours=3):
            over.append((started, r))
    if not over:
        return False
    last = max(over, key=lambda x: x[0])[1]
    return bool(last.get("ended")) and not last.get("error") and last.get("exit_code") == 0


def _numbers(sources: list[dict], now: datetime) -> dict:
    """{client: {group: n}} from the inbox list's last report (the All Inboxes groups, usable.team_group)."""
    s = next((s for s in sources if s.get("_id") == "inbox-status"), None)
    at = _parse((s or {}).get("at"))
    return dict(s.get("numbers") or {}) if s and at and now - at < STALE else {}


def _quiet_line(numbers: dict) -> str:
    """The 2-3 numbers that matter: inboxes safe to send per client, and how many are recovering or not usable."""
    safe = sorted(((c, int(g.get("Safe to send") or 0)) for c, g in numbers.items()), key=lambda x: (-x[1], x[0]))
    total = lambda grp: sum(int(g.get(grp) or 0) for g in numbers.values())  # noqa: E731
    line = "Inboxes safe to send: " + (", ".join(f"{c} {n}" for c, n in safe if n) or "none") + "."
    rest = [f"{w} {total(g)}" for g, w in (("Recovering", "recovering"), ("Not proven yet", "waiting for a test"),
                                            ("Do not use", "not usable")) if total(g)]
    return line + (f" Also {', '.join(rest)}." if rest else "")


def _cap(lines: list[str], n: int = 0) -> list[str]:
    n = n or PART_LINES
    return lines if len(lines) <= n else lines[:n - 1] + [f"… and {len(lines) - n + 1} more lines (the inbox sheet)"]


def _held_part(held: list[dict]) -> list[str]:
    """Each decision once, short: what, why (its first sentence), owner, until when."""
    lines, order = [], {}
    for x in held:
        order.setdefault(id(x["hold"]), x["hold"])
    for h in order.values():
        things = list(h.get("domains") or []) + list(h.get("inboxes") or []) + list(h.get("keys") or [])
        what = (", ".join(things[:3]) + (f" and {len(things) - 3} more" if len(things) > 3 else "")) or str(h.get("_id"))
        until = dg._until(h)
        when = f"until {_day(datetime(until.year, until.month, until.day, 12, tzinfo=IST))}" if until \
            else "no end date"
        why = str(h.get("reason") or "no reason recorded").strip()
        why = why.split(". ")[0].rstrip(".")
        lines.append(f"• {what}: {why}. Owner {h.get('owner') or 'not named'}, {when}.")
    return lines


def build(sources: list[dict], done: list[dict], checks: list[dict], now: datetime, holds=(), runs=()
          ) -> tuple[list[str], list[str]] | None:
    """(headline lines, thread parts), or None when there is nothing to say. ``checks``: the digest's own findings,
    as asks of kind 'broken' or 'ask'. ``holds``: the recorded decisions (Mongo digest_holds). ``runs``: recent
    cron_runs, so a job that has run fine since its failure is not reported.

    19 Nov 2026 (Manveen: "i got this and it's long spammy i can not unserstand anything"): the post is at most
    about 8 lines: the things to do, one line each (what, where, the one step); one line for jobs that failed; one
    line saying what else is in the thread. The thread is at most 3 short replies: the full lists, what was left
    out on purpose and what the bot did, and where the inboxes stand. Each job's own report is no longer posted."""
    asks, broken = collect(sources + [{"_id": "digest", "at": now.isoformat(), "asks": checks}], now)
    broken = one_per_job([b for b in broken if not recovered(b.get("key"), runs, now)])
    # A broken check whose step is Manveen's (deploy Render, switch the PC on) is a thing for him to do.
    his = [b for b in broken if str(b.get("step") or "").startswith("Manveen:")]
    broken = [b for b in broken if b not in his]
    asks += [{**b, "kind": "ask", "step": b["step"].split(":", 1)[1].strip()} for b in his]
    # Inboxes their provider removed are in the one delete reminder only (t352): every other item without them.
    gone = next((a for a in asks if a.get("key") == dg.REMOVED_KEY), None)
    if gone:
        asks = dg.drop_removed(asks, gone.get("inboxes"), gone.get("domains"))
    bare = [a for a in asks if dg.listless(a)]
    asks = [a for a in asks if not dg.listless(a)]
    asks, held = dg.apply_holds(asks, holds, now)
    asks, _ = dg.merge_kill_list(asks, LABELS)
    infos = [a for a in asks if a.get("kind") == "info"]      # a fact, no step: never merged into a thing to do
    asks = [a for a in asks if a.get("kind") != "info"]
    delete = [a for a in asks if a.get("key") == dg.REMOVED_KEY]    # never merged: other items name other inboxes
    asks, broken = dg.merge_same([a for a in asks if a.get("key") != dg.REMOVED_KEY], broken)
    asks += delete
    if bare:
        jobs = ", ".join(sorted({_label(a) for a in bare}))
        k = len(bare)
        broken.append({"kind": "broken", "source": "digest", "key": "digest:listless", "name": f"{jobs} (an ask without its list)",
                       "text": f"{k} ask{'s' if k != 1 else ''} came without {'its' if k == 1 else 'their'} list, so "
                               f"{'it was' if k == 1 else 'they were'} left out ({jobs}).",
                       "step": "Its next run sends the ask with its list.",
                       "detail": [f"{a['text']} ({_label(a)})" for a in bare]})
    numbers = _numbers(sources, now)
    if not asks and not broken and not done and not numbers and not infos:
        return None
    n = len(asks)
    head = [f"Inbox digest, {_day(now)}"]
    if n:
        head.append(f"\n<@{dg.OWNER}>, {n} thing{'s' if n != 1 else ''} need{'s' if n == 1 else ''} you today:")
        head += [f"\n{_item_line(i, a, now)}" for i, a in enumerate(asks[:MAX_SHOWN], 1)]
    else:
        head.append("\nNothing needs you today.")
        if numbers:
            head.append(f"\n{_quiet_line(numbers)}")
    if infos:
        head.append(f"\n{_info_line(infos)}")
    if broken:
        names = list(dict.fromkeys(_job_name(b) for b in broken))
        k = len(names)
        head.append(f"\n{k} job{'s' if k != 1 else ''} failed or could not check: {', '.join(names)}. "
                    f"Being looked at; what broke is in the thread.")

    # thread, part 1: the full lists of today's items and what broke
    lists = []
    for i, a in enumerate(asks, 1):
        rows = dg.bullets(a.get("detail"))
        if i > MAX_SHOWN:
            lists += ["", _item_line(i, a, now)] + [dg.plain(x) for x in dg.detail_lines(a, LIST_LINES)]
        elif rows:
            lists += ["", f"{i}. {dg.plain(a['text'])}"] + [dg.plain(x) for x in dg.detail_lines(a, a.get("lines") or LIST_LINES)]
        for x in a.get("also") or []:
            lists += ["", f"{i}. also, {_label(x)}: {_unlabel(x)}"
                      + (f" Do: {dg.cap(dg.plain(x['step']))}" if x.get("step") else "")]
            lists += [dg.plain(y) for y in dg.detail_lines(x, 3)]
    for i, a in enumerate(infos):
        rows = dg.bullets(a.get("detail"))
        if i or rows:
            # a fact needs no full list: 3 lines, so every "For info" fits in the reply (21 Nov 2026)
            lists += ["", f"For info: {dg.plain(a['text'])}"] + [dg.plain(x) for x in dg.detail_lines(a, INFO_LINES)]
    if broken:
        lists += ["", "What broke"]
        for b in broken:
            lists.append(f"• {dg.plain(b['text'])}" + (f" {dg.plain(b['step'])}" if b.get("step") else ""))
            lists += [f"   {dg.plain(x)}" for x in dg.detail_lines(b, 3)]
            for x in b.get("also") or []:
                lists.append(f"   also, {_label(x)}: {_unlabel(x)}")
    # part 2: left out on purpose, and what the bot did by itself
    other = []
    if held:
        other += ["Left out on purpose (decided earlier)"] + _held_part(held)
    if done:
        other += ([""] if other else []) + ["Done by the bot since the last digest"] + [f"• {dg.plain(d['text'])}" for d in done]
    # part 3: where the inboxes stand
    stand = []
    if numbers:
        stand = ["Where the inboxes stand (All Inboxes tab)"] + [
            f"• {c}: " + ", ".join(f"{int(g[k])} {k.lower()}" for k in TEAM_GROUPS if int(g.get(k) or 0))
            for c, g in sorted(numbers.items(), key=lambda x: -sum(int(v or 0) for v in x[1].values()))]
        stand.append("What the words mean: " + "; ".join(f"{k.lower()} = {GROUP_MEANS[k]}" for k in TEAM_GROUPS) + ".")
        stand.append(f"Full lists: <{SHEET_URL}|the inbox sheet>.")
    parts = []
    if lists:
        parts.append("\n".join(_cap(["Today's items in full"] + lists)))
    if other:
        parts.append("\n".join(_cap(other)))
    if stand:
        parts.append("\n".join(_cap(stand)))

    more = []
    if n > MAX_SHOWN:
        more.append(f"{n - MAX_SHOWN} more thing{'s' if n - MAX_SHOWN != 1 else ''} for you")
    if held:
        k = len({id(x["ask"]) for x in held})
        more.append(f"{k} decided item{'s' if k != 1 else ''} left out on purpose")
    if done:
        more.append(f"{len(done)} thing{'s' if len(done) != 1 else ''} the bot did by itself")
    if parts:
        head.append("\nIn the thread: " + (", ".join(more) if more else "the full lists") + ".")
    return head, parts[:MAX_PARTS]


# --- the digest's own checks ------------------------------------------------------------

def pc_check(heartbeat: dict | None, now: datetime) -> list[dict]:
    at = _parse((heartbeat or {}).get("last_run"))
    if at and now - at <= PC_STALE:
        return []
    when = f"since {_day(at)} {at.astimezone(IST):%H:%M} IST" if at else "for a while (no heartbeat in Mongo)"
    return [{"kind": "broken", "key": "digest:pc-job",
             "text": f"The PC's v2 weekly job has not reported {when}, so no inbox is pulled or returned.",
             "step": "Manveen: check the PC is on and the task 'InfraBot v2 weekly' is enabled (Task Scheduler)."}]


def render_check(running: str | None, compare: dict | None) -> list[dict]:
    """compare: {'behind': N} or {'error': why}. Nothing off Render."""
    if not running:
        return []
    if compare is None or "error" in compare:
        return []                   # GitHub not reachable: say nothing rather than guess
    n = int(compare.get("behind") or 0)
    if n <= 0:
        return []
    return [{"kind": "broken", "key": "digest:render-behind",
             "text": f"Render runs {running[:7]}, {n} commit{'s' if n != 1 else ''} behind main, so the newest bot "
                     f"changes are not live.",
             "step": "Manveen: Render, infra-bot-1, Manual Deploy, Deploy latest commit."}]


SYNC_SOURCE = "smartlead-sync"      # run.py SYNC_SOURCE: it reports to the digest at the very end of every run
SYNC_HOUR_IST = 10                  # its daily slot (bot_health.js JOBS, '0 10 * * *')


def sync_check(sources: list[dict], now: datetime) -> list[dict]:
    """A broken line when no sheet sync has finished since the last 10:00 IST slot. The sync (Render's, or one run
    by hand) reports to the digest when it ends, so an older report means All Inboxes still shows an older sync.
    17 Nov 2026: Render's 10:00 run never finished (429s, then most likely the noon redeploy) and the 11:30 digest
    said nothing; bot health only calls a run unfinished after 3 hours, so it would have said so the next day."""
    local = now.astimezone(IST)
    due = local.replace(hour=SYNC_HOUR_IST, minute=0, second=0, microsecond=0)
    if local < due:
        due -= timedelta(days=1)
    doc = next((s for s in sources if s.get("_id") == SYNC_SOURCE), None)
    at = _parse((doc or {}).get("at"))
    if at and at >= due:
        return []
    shows = (f"the sync that finished {_day(at)} {at.astimezone(IST):%H:%M} IST" if at
             else "an older sync (no finished sync on record)")
    return [{"kind": "broken", "key": "digest:sync-not-finished", "name": "Daily sheet sync",
             "text": f"The daily sheet sync has not finished since {_day(due)} {due:%H:%M} IST, so All Inboxes and "
                     f"the other sync tabs still show {shows}.",
             "step": "Look in the Render log (search [smartlead-sync]): a run still going writes the sheet when it "
                     "ends; a run lost to a restart is not repeated until tomorrow 10:00 IST, so for today's sheet "
                     "run smartlead_sync/run.py by hand."}]


def _compare(running: str) -> dict:
    import requests
    branch = os.getenv("RENDER_GIT_BRANCH") or "main"
    try:
        r = requests.get(f"https://api.github.com/repos/{REPO}/compare/{running}...{branch}", timeout=15,
                         headers={"User-Agent": "infra-bot", "Accept": "application/vnd.github+json"})
        if r.status_code != 200:
            return {"error": f"GitHub answered {r.status_code}"}
        return {"behind": int(r.json().get("ahead_by") or 0)}
    except Exception as exc:  # noqa: BLE001
        return {"error": type(exc).__name__}


def removed_source(db, now: datetime) -> list[dict]:
    """The delete reminder for inboxes their provider removed, as a digest source ([] when none). It is built here,
    from what zapmail_digest saved at 09:40 (provider facts and Smartlead's inbox list) and today's sync (Mongo
    inbox_status), so an inbox deleted by hand leaves it the next day. Never raises."""
    try:
        from smartlead import provider_removed as pr
        s = pr.source(db, now)
        return [s] if s else []
    except Exception as exc:  # noqa: BLE001 - the digest goes out without it
        print(f"  [Digest] removed inboxes not read ({type(exc).__name__})")
        return []


def wait_for_jobs(db, now_fn=lambda: datetime.now(timezone.utc), sleep=time.sleep, max_s: int = WAIT_MAX_S) -> None:
    """Wait (up to 30 minutes) for a morning job that is still running, so its asks are in today's digest."""
    waited = 0
    while waited < max_s:
        since = now_fn() - timedelta(hours=3)
        # A run a restart killed never ends; index.js marks it lost when the bot starts again (17-18 Nov 2026).
        running = [r for r in db.cron_runs.find({"job": {"$in": list(WAIT_FOR)}, "started": {"$gte": since},
                                                 "ended": {"$exists": False}}) if not r.get("lost")]
        if not running:
            return
        print(f"  waiting for {', '.join(sorted({r['job'] for r in running}))} to finish")
        sleep(60)
        waited += 60


def already_posted(meta: dict | None, now: datetime) -> bool:
    """True when today's digest (IST) is already out. 19 Nov 2026: the post read as if it came twice; Slack had one,
    but a second run the same day (a restart catch-up, a hand run with --post) would post the same headline again."""
    at = _parse((meta or {}).get("at"))
    return bool(meta and meta.get("posted") and at and at.astimezone(IST).date() == now.astimezone(IST).date())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--post", action="store_true", help="post it and mark what it listed (else a dry run)")
    ap.add_argument("--no-wait", action="store_true")
    ap.add_argument("--again", action="store_true", help="post even if today's digest is already out")
    args = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    try:
        db = dg._db()
    except Exception as exc:  # noqa: BLE001
        print(f"Mongo not reachable ({type(exc).__name__}): no digest today")
        return 1
    if args.post and not args.again and already_posted(db[dg.META].find_one({"_id": "last"}), datetime.now(timezone.utc)):
        print("Today's digest is already out: not posting it twice (--again to post anyway)")
        return 0
    if args.post and not args.no_wait:
        wait_for_jobs(db)
    now = datetime.now(timezone.utc)
    sources = list(db[dg.ASKS].find({}))
    done = sorted(db[dg.DONE].find({"posted_on": None}), key=lambda d: str(d.get("at")))
    done = [d for d in done if (_parse(d.get("at")) or now) > now - MAX_ASK_AGE]
    sources += removed_source(db, now)
    running = os.getenv("RENDER_GIT_COMMIT")
    checks = pc_check(db.v2_heartbeat.find_one({"_id": "pc"}), now)
    checks += render_check(running, _compare(running) if running else None)
    checks += sync_check(sources, now)
    try:
        runs = list(db.cron_runs.find({"started": {"$gte": now - timedelta(days=4)}}))
    except Exception:  # noqa: BLE001 - without runs every broken line stays, nothing is lost
        runs = []
    built = build(sources, done, checks, now, dg.load_holds(db), runs)
    if not built:
        print("Nothing needs anyone and nothing was done: no digest today")
        db[dg.META].replace_one({"_id": "last"}, {"_id": "last", "at": now.isoformat(), "posted": False}, upsert=True)
        return 0
    head, parts = built
    print("\n".join(head))
    for p in parts:
        print("\n--- thread ---\n" + p)
    if not args.post:
        print("\n(dry run: nothing posted)")
        return 0
    from smartlead.notify import post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    ts = post_with_thread(token, dg.channel(), head, parts) if token else None
    if not ts:
        print("UNPOSTED: the digest did not go out")
        return 1
    today = f"{now.astimezone(IST):%Y-%m-%d}"
    for d in done:
        db[dg.DONE].update_one({"_id": d["_id"]}, {"$set": {"posted_on": today}})
    db[dg.META].replace_one({"_id": "last"}, {"_id": "last", "at": now.isoformat(), "posted": True, "ts": ts,
                                              "asks": sum(1 for h in head if h[:1] == "\n" and h[1:2].isdigit()),
                                              "done": len(done)}, upsert=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
