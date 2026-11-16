"""The daily inbox digest: one post a morning (IST), addressed to Manveen, with only what a person must do today
(each with its exact step), what the bot did by itself since the last digest, and any check that broke. Every
job's full report goes in the thread. Jobs hand their part over through smartlead/digest.py.

    python digest_post.py            # dry run: prints the digest, posts nothing, marks nothing
    python digest_post.py --post     # post it (the bot runs this every morning)

Besides what the jobs hand over, the digest checks two things itself: the PC's v2 weekly job ran in the last
3 hours (Mongo v2_heartbeat), and Render runs the latest code on main (RENDER_GIT_COMMIT against GitHub).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from smartlead import digest as dg

IST = dg.IST
MAX_ASK_AGE = timedelta(days=8)      # a job that stopped reporting for longer than this: its asks are dropped
STALE = timedelta(hours=30)          # older than this, an ask says when its job last checked
MAX_SHOWN = 12                       # asks in the post itself; the rest are in the thread
PC_STALE = timedelta(hours=3)
REPO = os.getenv("RENDER_GIT_REPO_SLUG") or "justsakshi/infra-bot"
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
          "smartlead-sync": "Daily sheet sync"}


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


def _ask_line(i: int, a: dict, now: datetime) -> str:
    since, checked = _parse(a.get("since")), a.get("checked")
    tail = []
    if since and now - since > timedelta(hours=20):
        tail.append(f"Open since {_day(since)}.")
    if checked and now - checked > STALE:
        tail.append(f"(last checked {_day(checked)})")
    step = f"\nDo: {a['step']}" if a.get("step") else ""
    return f"\n{i}. {a['text']}{step}" + (f" {' '.join(tail)}" if tail else "")


def build(sources: list[dict], done: list[dict], checks: list[dict], now: datetime
          ) -> tuple[list[str], list[str]] | None:
    """(headline lines, thread parts), or None when there is nothing to say (no post: routine 'all fine' is
    silent). ``checks``: the digest's own findings, as asks of kind 'broken' or 'ask'."""
    asks, broken = collect(sources + [{"_id": "digest", "at": now.isoformat(), "asks": checks}], now)
    if not asks and not broken and not done:
        return None
    n = len(asks)
    head = [f"Inbox digest, {_day(now)}",
            f"\n<@{dg.OWNER}>, " + (f"{n} thing{'s' if n != 1 else ''} need{'s' if n == 1 else ''} you today:"
                                    if n else "nothing needs you today.")]
    head += [_ask_line(i, a, now) for i, a in enumerate(asks[:MAX_SHOWN], 1)]
    if n > MAX_SHOWN:
        head.append(f"\n{n - MAX_SHOWN} more in the thread.")
    if done:
        head.append("\nDone by the bot since the last digest:")
        head += [f"• {d['text']}" for d in done[:15]]
        if len(done) > 15:
            head.append(f"• and {len(done) - 15} more (thread)")
    if broken:
        head.append("\nChecks that broke (being fixed, or the step says who acts):")
        head += [f"• {b['text']}" + (f" {b['step']}" if b.get("step") else "") for b in broken]
    parts = []
    if n > MAX_SHOWN:
        parts.append("More things that need you\n\n" + "\n\n".join(_ask_line(i, a, now).strip()
                                                                   for i, a in enumerate(asks[MAX_SHOWN:], MAX_SHOWN + 1)))
    if len(done) > 15:
        parts.append("Everything done by the bot\n\n" + "\n".join(f"• {d['text']}" for d in done))
    for s in sorted(sources, key=lambda s: s["_id"]):
        at = _parse(s.get("at"))
        if s.get("detail") and at and now - at < STALE:
            parts.append(f"{LABELS.get(s['_id'], s['_id'])}, as of {_day(at)} {at.astimezone(IST):%H:%M}\n\n"
                         + "\n\n".join(s["detail"]))
    if parts:
        head.append("\nEach check's full report is in the thread.")
    return head, parts


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


def wait_for_jobs(db, now_fn=lambda: datetime.now(timezone.utc), sleep=time.sleep, max_s: int = WAIT_MAX_S) -> None:
    """Wait (up to 30 minutes) for a morning job that is still running, so its asks are in today's digest."""
    waited = 0
    while waited < max_s:
        since = now_fn() - timedelta(hours=3)
        running = list(db.cron_runs.find({"job": {"$in": list(WAIT_FOR)}, "started": {"$gte": since},
                                          "ended": {"$exists": False}}))
        if not running:
            return
        print(f"  waiting for {', '.join(sorted({r['job'] for r in running}))} to finish")
        sleep(60)
        waited += 60


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--post", action="store_true", help="post it and mark what it listed (else a dry run)")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    try:
        db = dg._db()
    except Exception as exc:  # noqa: BLE001
        print(f"Mongo not reachable ({type(exc).__name__}): no digest today")
        return 1
    if args.post and not args.no_wait:
        wait_for_jobs(db)
    now = datetime.now(timezone.utc)
    sources = list(db[dg.ASKS].find({}))
    done = sorted(db[dg.DONE].find({"posted_on": None}), key=lambda d: str(d.get("at")))
    done = [d for d in done if (_parse(d.get("at")) or now) > now - MAX_ASK_AGE]
    running = os.getenv("RENDER_GIT_COMMIT")
    checks = pc_check(db.v2_heartbeat.find_one({"_id": "pc"}), now)
    checks += render_check(running, _compare(running) if running else None)
    built = build(sources, done, checks, now)
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
