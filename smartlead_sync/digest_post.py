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
23 Nov 2026 (t386, "why does it fail to be readable again and again"): every round was judged on one good day's data.
Now every stored ask is worded at post time from its facts (smartlead/digest_render.py), an item that All Inboxes
overrules is folded into the item that agrees with it, and the final text goes through hard limits
(smartlead/digest_guard.py) that tests built from a bad day's shape hold the build to (test_digest_guard.py).
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from smartlead import digest as dg
from smartlead import digest_guard as guard
from smartlead import digest_items as items
from smartlead import digest_render as render
from smartlead import digest_sentences as sentences

IST = dg.IST
MAX_ASK_AGE = timedelta(days=8)      # a job that stopped reporting for longer than this: its asks are dropped
STALE = timedelta(hours=30)          # older than this, an ask says when its job last checked
MAX_PARTS = 3                        # thread replies, at most
PART_LINES = 38                      # lines per reply (notify._THREAD_CHUNK is 40: one part is one reply)
LIST_LINES = 12                      # lines of one item's list in the thread
INFO_LINES = 8                       # lines of a "For info" item's list in the thread (3 until t386, when the
                                     # short facts got a reply of their own and readers asked for the whole list)

PC_STALE = timedelta(hours=3)
REPO = os.getenv("RENDER_GIT_REPO_SLUG") or "justsakshi/infra-bot"
WAIT_FOR = ("smartlead-sync", "fleet-watch")   # jobs whose asks the digest waits for when they are still running
WAIT_MAX_S = 30 * 60
FACTS = "digest_facts"              # one doc per IST day: what the post was made from (blind test, t409)

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


def _label(a: dict) -> str:
    return LABELS.get(a.get("source"), a.get("source") or "")


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


def _cap(lines: list[str], n: int = 0) -> list[str]:
    n = n or PART_LINES
    return lines if len(lines) <= n else lines[:n - 1] + [f"… and {len(lines) - n + 1} more lines (the inbox sheet)"]


def _sentence_or_none(it: dict) -> str | None:
    try:
        return sentences.sentence(it)
    except KeyError:
        return None


def plan(sources: list[dict], done: list[dict], checks: list[dict], now: datetime, holds=(), runs=(), ctx=None
         ) -> dict:
    """Where every ask goes today (smartlead/digest_items.route), after what PR #116 already did: today's words for
    every stored ask (digest_render), a job that has run fine since is not reported, holds (decided items), the
    removed inboxes only in their delete reminder, the kill list as one item, an item All Inboxes overrules folded.
    ``checks``: the digest's own findings (kind broken). Returns the route's plan plus held, superseded, done."""
    ctx = ctx or {}
    asks, broken = collect(sources + [{"_id": "digest", "at": now.isoformat(), "asks": checks}], now)
    asks = [render.render(a, ctx.get("v2") or ()) for a in asks]        # today's words, whenever it was stored
    broken = [render.render(b, ctx.get("v2") or ()) for b in broken]
    broken = one_per_job([b for b in broken if not recovered(b.get("key"), runs, now)])
    told = {b.get("source") for b in broken if not str(b.get("key") or "").startswith("bot-health:")}
    broken = [b for b in broken if not re.match(r"bot-health:failed:(.+)$", str(b.get("key") or ""))
              or re.match(r"bot-health:failed:(.+)$", b["key"]).group(1) not in told]
    gone = next((a for a in asks if a.get("key") == dg.REMOVED_KEY), None)
    if gone:
        asks = dg.drop_removed(asks, gone.get("inboxes"), gone.get("domains"))
    bare = [a for a in asks if dg.listless(a)]
    asks = [a for a in asks if not dg.listless(a)]
    asks, held = dg.apply_holds(asks, holds, now)
    asks, _ = dg.merge_kill_list(asks, LABELS)
    infos = [a for a in asks if a.get("kind") == "info"]
    asks = [a for a in asks if a.get("kind") != "info"]
    asks, superseded = render.resolve(asks, ctx.get("verdicts") or {})
    if bare:
        jobs = ", ".join(sorted({_label(a) for a in bare}))
        broken.append({"kind": "broken", "source": "digest", "key": "digest:listless",
                       "text": f"{len(bare)} ask{'s' if len(bare) != 1 else ''} from {jobs} came without "
                               f"{'its' if len(bare) == 1 else 'their'} list, so {'it was' if len(bare) == 1 else 'they were'}"
                               f" left out of the digest.",
                       "step": "Its next run sends the ask with its list."})
    p = items.route(asks, broken, infos, ctx, now)
    # a sentence with a bracket the data can't fill is never posted with a gap: it goes to the thread as the job wrote it
    ok = []
    for it in p["items"]:
        if _sentence_or_none(it) is None:
            p["thread"].append({**it, "s": "P7", "what": it.get("what") or (it.get("detail") or [""])[0], "step": ""})
        else:
            ok.append(it)
    p["items"] = ok
    p["watch"] = [w for w in p["watch"] if _sentence_or_none(w) is not None]
    p.update(held=held, superseded=superseded, done=list(done))
    return p


def _rows(it: dict, cap: int) -> list[str]:
    """An item's list in the thread, cut at ``cap`` lines (or the item's own, a working list like the delete
    reminder's)."""
    cap = int(it.get("cap") or cap)
    rows = [dg.plain(x) for x in (it.get("detail") or [])]
    return rows if len(rows) <= cap else rows[:cap - 1] + [f"• and {len(rows) - cap + 1} more (the inbox sheet)"]


def _steps(it: dict, seen: set | None = None) -> list[str]:
    """An item's "How:" lines; one said under an earlier item (``seen``) is not said again (the delete reminder's
    steps cover its every group)."""
    out = []
    for x in it.get("steps") or []:
        x = dg.cap(dg.plain(x).strip())
        if x and (seen is None or x not in seen):
            out.append(f"How: {x}")
            if seen is not None:
                seen.add(x)
    return out


def _names(xs: list[str]) -> str:
    return xs[0] if len(xs) == 1 else ", ".join(xs[:-1]) + " and " + xs[-1]


def _item_domains(it: dict) -> set[str]:
    return {d for x in it.get("detail") or [] for d in items._doms(x)}


def _split(blocks: list[list[str]], k: int) -> list[str]:
    """Blocks (one per item) as at most ``k`` replies of at most PART_LINES lines, never splitting a block unless it is
    longer than a reply. The last reply takes what is left (the guard cuts it)."""
    parts, cur = [], []
    for blk in blocks:
        if cur and len(cur) + 1 + len(blk) > PART_LINES and len(parts) < k - 1:
            parts.append(cur)
            cur = []
        cur += ([""] if cur else []) + blk
    if cur:
        parts.append(cur)
    return ["\n".join(_cap(x)) for x in parts]


def compose(p: dict, now: datetime) -> tuple[list[str], list[str]]:
    """(post lines, thread replies). The post: the header, the first MAX_ITEMS items (one approved sentence each), the
    Watching lines, and how many more are in the thread. The thread: first what the post could not hold (the items
    past MAX_ITEMS, oldest first, each with its sentence and list; the person-only asks with no approved sentence yet,
    in the closest shape), then the lists and steps of the items in the post, then one reply with what was left out on
    purpose, the facts with no step and what the bot did by itself. Never the inbox counts (All Inboxes)."""
    its, parked, watch = p["items"], p["thread"], p["watch"]
    n = len(its) + len(parked)
    shown = its[:sentences.MAX_ITEMS]
    head = [sentences.header(_day(now), n)]
    for i, it in enumerate(shown, 1):
        head.append(("\n" if i == 1 else "") + f"{i}. {sentences.sentence(it)}")
    for j, x in enumerate(sentences.watch_line(watch)):
        head.append(("\n" if j == 0 else "") + x)
    more = n - len(shown)
    if more:
        head.append(f"\n{more} more in the thread.")

    # The thread in number order (both readers of the t409 render: lists after later items read as out of order):
    # 1-5 under "N above", then 6 on with their sentence (they were opened first, oldest first), then the ones with
    # no approved sentence yet. Removed inboxes on the same domains as another item say which.
    blocks: list[list[str]] = []
    seen: set = set()
    doms = {i: _item_domains(it) for i, it in enumerate(its, 1) if it.get("kind", "").startswith("removed")}
    for i, it in enumerate(its, 1):
        head_line = f"{i} above:" if i <= len(shown) else f"{i}. {sentences.sentence(it)}"
        rows = _rows(it, LIST_LINES) + _steps(it, seen)
        same = [j for j, d in doms.items() if j != i and i in doms and d & doms[i]]
        if same:
            shared = sorted(set().union(*(doms[j] & doms[i] for j in same)))
            rows.append(f"Careful: {_names(shared)} also {'has' if len(shared) == 1 else 'have'} removed inboxes in "
                        f"item{'s' if len(same) > 1 else ''} {', '.join(map(str, same))}; on {'it' if len(shared) == 1 else 'them'} "
                        f"act only on the inboxes named here.")
        if rows or i > len(shown):
            blocks.append([head_line] + rows)
    for j, it in enumerate(parked, len(its) + 1):
        blocks.append([f"{j}. {sentences.shape(it)} (wording not approved yet)"] + _rows(it, LIST_LINES)
                      + _steps(it, seen))
    if len(watch) > sentences.MAX_WATCH:
        blocks.append([sentences.sentence(x) for x in watch[sentences.MAX_WATCH:]])
    wl = [dg.plain(y) for x in watch for y in x.get("detail") or []]
    if wl:
        blocks.append(["Watching:"] + [f"• {y}" if not y.startswith("•") and not y.startswith("If ") else y for y in wl])
    other: list[str] = []
    if p.get("held"):
        other += ["Left out on purpose (decided earlier)"] + _held_part(p["held"])
    lone = list(p.get("superseded") or [])        # folded or not: the failed item covers it, the thread says so
    if lone:
        doms = sorted({d for x in lone for d in x["_domains"]})
        other += ([""] if other else []) + [
            f"Left out: an older ask to re-run the weekly test of {_names(doms[:3])}{' and more' if len(doms) > 3 else ''}"
            f" (it looked like a broken test); All Inboxes now marks {'its' if len(doms) == 1 else 'their'} inboxes do "
            f"not use, so no re-run is needed."]
    if p.get("info"):
        other += ([""] if other else []) + ["For info, nothing to do"]
        for a in p["info"]:
            other.append(dg.plain(a.get("text")))
            other += [dg.plain(x) for x in dg.detail_lines(a, INFO_LINES)]
    if p.get("done"):
        other += ([""] if other else []) + ["Done by the bot since the last digest"] + \
                 [f"• {dg.plain(d['text'])}" for d in p["done"]]
    parts = _split(blocks, MAX_PARTS - (1 if other else 0)) if blocks else []
    if other:
        parts.append("\n".join(_cap(other)))
    return guard.enforce(head, parts[:MAX_PARTS])


def build(sources: list[dict], done: list[dict], checks: list[dict], now: datetime, holds=(), runs=(), ctx=None
          ) -> tuple[list[str], list[str]]:
    """(post lines, thread replies) for today: plan, then compose. A day with nothing to do still posts its one-line
    header ("nothing to do today"): a missing post can't be told from a bot that did not run."""
    return compose(plan(sources, done, checks, now, holds, runs, ctx), now)


DEV_LINES = 15


def dev_post(p: dict, now: datetime) -> str | None:
    """The bot's own problems (broken jobs and checks, lookups that found a job at fault), one message for the
    bot-health channel, never in Manveen's digest (t409 decision 3). None when there are none."""
    lines = list(dict.fromkeys(guard.clean(x) for x in p.get("dev") or [] if str(x).strip()))
    if not lines:
        return None
    k = len(lines)
    shown = [f"• {guard.fit(x, guard.LINE_CHARS, ' …')}" for x in lines[:DEV_LINES]]
    if k > DEV_LINES:
        shown.append(f"• and {k - DEV_LINES} more (Render log)")
    return f"Bot jobs · {_day(now)} · {k} problem{'s' if k != 1 else ''}\n\n" + "\n".join(shown)


def dev_channel() -> str:
    """Where the bot's own problems go: the bot-health channel (index.js healthChannel: BOT_HEALTH_CHANNEL, else the
    infra channel)."""
    from smartlead.config import INFRA_CHANNEL
    return (os.getenv("BOT_HEALTH_CHANNEL") or "").strip() or INFRA_CHANNEL


def _json(x):
    if isinstance(x, dict):
        return {str(k): _json(v) for k, v in x.items() if not str(k).startswith("_")}
    if isinstance(x, (list, tuple, set)):
        return [_json(v) for v in x]
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    return str(x)


def day_facts(p: dict, now: datetime, head: list[str], parts: list[str]) -> dict:
    """What today's post was made from, for the blind test (Mongo digest_facts, one doc per IST day): the items with
    their brackets, the Watching lines, the person-only asks with no sentence, and the posted text."""
    keep = ("s", "f", "by", "since", "detail", "steps", "kind", "what", "step")
    pick = lambda xs: [_json({k: x.get(k) for k in keep if k in x}) for x in xs]  # noqa: E731
    return {"_id": f"{now.astimezone(IST):%Y-%m-%d}", "day": _day(now), "n": len(p["items"]) + len(p["thread"]),
            "items": pick(p["items"]), "watch": pick(p["watch"]), "thread": pick(p["thread"]),
            "text": "\n".join(head) + "".join("\n\n--- thread ---\n" + x for x in parts),
            "at": now.isoformat(timespec="seconds")}


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


def load_ctx(db, now: datetime) -> dict:
    """What the digest judges items by, read-only: each domain's All Inboxes verdict and every inbox's client (Mongo
    inbox_status, written by the sync from the tab), the weekly test verdicts of the last 14 days (Mongo v2_verdicts:
    which domains an older held test was about, a domain's latest verdict and its numbers, t409) and the active weekly
    tests with their next run (Mongo test_coverage, t409). A part Mongo can't give is left out: then nothing is folded
    or looked up, nothing is lost (an ask that needs a lookup goes to the thread or the dev channel)."""
    out = {}
    try:
        status = list(db.inbox_status.find({}, {"_id": 0, "email": 1, "domain": 1, "busy_reason": 1, "provider": 1,
                                                "state": 1, "client": 1, "account": 1, "campaigns": 1}))
        out["verdicts"] = render.verdicts(status)
        out["status"] = status
    except Exception as exc:  # noqa: BLE001
        print(f"  [Digest] All Inboxes verdicts not read ({type(exc).__name__})")
    try:
        since = f"{(now - timedelta(days=14)).astimezone(IST):%Y-%m-%d}"
        out["v2"] = list(db.v2_verdicts.find({"night": {"$gte": since}}, {"_id": 0, "email": 1, "domain": 1, "client": 1,
                                                                         "night": 1, "type": 1, "weekly": 1,
                                                                         "history": 1, "google": 1, "microsoft": 1,
                                                                         "verdict": 1}))
    except Exception as exc:  # noqa: BLE001
        print(f"  [Digest] weekly test verdicts not read ({type(exc).__name__})")
    try:
        from smartlead.inbox_status import weekly_tests
        out["tests"] = weekly_tests(db, now.astimezone(IST).date()) or []
    except Exception as exc:  # noqa: BLE001
        print(f"  [Digest] weekly tests not read ({type(exc).__name__})")
    return out


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
    p = plan(sources, done, checks, now, dg.load_holds(db), runs, load_ctx(db, now))
    head, parts = compose(p, now)
    dev = dev_post(p, now)
    print("\n".join(head))
    for x in parts:
        print("\n--- thread ---\n" + x)
    for x in guard.problems(head, parts):          # the guard keeps the limits; this only says if one still slips
        print(f"  [Digest] readability limit: {x}")
    if dev:
        print(f"\n--- dev channel (separate message) ---\n{dev}")
    if not args.post:
        print("\n(dry run: nothing posted, nothing saved)")
        return 0
    from smartlead.notify import post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    ts = post_with_thread(token, dg.channel(), head, parts) if token else None
    if not ts:
        print("UNPOSTED: the digest did not go out")
        return 1
    if dev:
        from smartlead.notify import _post
        if not _post(token, dev_channel(), dev):
            print("  [Digest] the dev-channel message did not go out")
    today = f"{now.astimezone(IST):%Y-%m-%d}"
    for d in done:
        db[dg.DONE].update_one({"_id": d["_id"]}, {"$set": {"posted_on": today}})
    db[dg.META].replace_one({"_id": "last"}, {"_id": "last", "at": now.isoformat(), "posted": True, "ts": ts,
                                              "asks": len(p["items"]) + len(p["thread"]), "done": len(done)},
                            upsert=True)
    try:                                             # the blind test reads it (digest_blind_test.py, t409)
        db[FACTS].replace_one({"_id": today}, day_facts(p, now, head, parts), upsert=True)
    except Exception as exc:  # noqa: BLE001 - the digest is out; only the blind test misses a day
        print(f"  [Digest] today's facts not saved ({type(exc).__name__})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
