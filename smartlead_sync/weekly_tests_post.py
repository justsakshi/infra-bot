#!/usr/bin/env python3
"""The Monday weekly test results post (t420): what last weekend's Smart Delivery v2 tests found, per client.

Manveen, 23 Nov 2026: "usually on the monday there should be a message for deliverbility tests". Results only: the
daily digest keeps what a person must do. One top-level post in the infra channel (the digest's channel) and one
thread reply with the lists. Built from Mongo only (no Smartlead calls): v2_verdicts (the PC job's stored verdicts),
inbox_status (the sync's All Inboxes view), provider_inventory (inboxes their provider removed), external_warmup,
test_coverage (when each inbox is tested next) and the v2 job's own report line (digest_asks / digest_done). Wording
and limits: smartlead/weekly_results.py.

If no weekly results are stored from the last 7 days, the post is one line, and the reason goes to the bot-health
channel (digest_post.dev_channel).

    python weekly_tests_post.py            # dry run: prints the post, posts nothing
    python weekly_tests_post.py --post     # post it (Render, Mondays 10:00 IST; once a week, --again to repeat)

Exit codes: 0 fine (posted, dry run, or this week's post is already out), 1 Mongo not reachable or Slack refused it.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from smartlead import digest as dg
from smartlead import weekly_results as wr

POSTED = "weekly_tests_post"        # one doc per week posted: {_id: week key, at, ts}


def _week_key(nights: list[str], today) -> str:
    return f"tests-{min(nights)}" if nights else f"none-{today.isoformat()}"


def load(db, now: datetime) -> dict:
    """Everything the post is built from, read-only."""
    from smartlead import provider_removed as pr
    from smartlead import external_warmup
    from smartlead.inbox_status import weekly_tests
    today = now.astimezone(wr.IST).date()
    fields = {"_id": 0, "email": 1, "domain": 1, "client": 1, "night": 1, "verdict": 1, "type": 1, "google": 1,
              "microsoft": 1, "weekly": 1, "history": 1, "pulled_on": 1}
    since = (today - wr.timedelta(days=wr.FRESH_DAYS + wr.WEEK_SPAN)).isoformat()
    docs = list(db.v2_verdicts.find({"night": {"$gte": since}}, fields))
    rows, nights = wr.week_rows(docs, today)
    out = {"today": today, "rows": rows, "nights": nights, "newest": None, "heartbeat": None}
    if not rows:
        newest = next(iter(db.v2_verdicts.find({"weekly": True, "history": {"$ne": True}}, {"night": 1})
                           .sort("night", -1).limit(1)), None)
        out["newest"] = (newest or {}).get("night")
        out["heartbeat"] = db.v2_heartbeat.find_one({"_id": "pc"})
        return out
    status = {d["email"].lower(): d for d in db.inbox_status.find({}, {"_id": 0, "email": 1, "client": 1,
                                                                       "busy_reason": 1, "provider": 1, "campaigns": 1,
                                                                       "state": 1}) if d.get("email")}
    facts, inboxes = pr.load(db, now)
    removed = {r["email"]: r for r in pr.removed(facts, inboxes)}
    marks = external_warmup.load(db[external_warmup.COLLECTION])     # never raises; unread = no vendor word
    title = wr.nights_title(nights)
    texts = []
    a = db[dg.ASKS].find_one({"_id": "v2-weekly"}) or {}
    texts += ["\n".join(str(x) for x in a.get("detail") or [])]
    texts += [d.get("text") for d in db[dg.DONE].find({"source": "v2-weekly"}, {"text": 1})]
    weekly_ever = db.v2_verdicts.distinct("email", {"weekly": True, "history": {"$ne": True}})
    out.update(status=status, removed=removed, marks={k.lower(): v for k, v in marks.items()},
               tests=weekly_tests(db, today), ever={str(e).lower() for e in weekly_ever},
               did=wr.find_did(texts, title))
    return out


def compose(ctx: dict) -> tuple[list[str], list[str], str | None]:
    """(head, thread parts, bot-health message or None)."""
    if not ctx["rows"]:
        head, dev = wr.no_results(ctx["today"], ctx.get("newest"), ctx.get("heartbeat"))
        return head, [], dev
    head, parts = wr.build(ctx["rows"], ctx["nights"], ctx["status"], ctx["removed"], ctx["marks"], ctx["tests"],
                           ctx["ever"], ctx["did"], ctx["today"])
    if not dg.on():                     # no daily digest: the jobs post their own asks
        head = [wr.NO_ACTIONS_OFF if h == wr.NO_ACTIONS else h for h in head]
    return head, parts, None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--post", action="store_true", help="post it (else a dry run)")
    ap.add_argument("--again", action="store_true", help="post even if this week's post is already out")
    args = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    try:
        db = dg._db()
        now = datetime.now(timezone.utc)
        ctx = load(db, now)
    except Exception as exc:  # noqa: BLE001
        print(f"Mongo not reachable ({type(exc).__name__}): no weekly test results post")
        return 1
    head, parts, dev = compose(ctx)
    key = _week_key(ctx["nights"], ctx["today"])
    print("\n".join(head))
    for x in parts:
        print("\n--- thread ---\n" + x)
    for x in wr.problems(head, parts):
        print(f"  [Weekly tests] readability limit: {x}")
    if dev:
        print(f"\n--- bot-health channel (separate message) ---\n{dev}")
    if not args.post:
        print("\n(dry run: nothing posted, nothing saved)")
        return 0
    if not args.again and db[POSTED].find_one({"_id": key}):
        print("This week's weekly test results post is already out: not posting it twice (--again to post anyway)")
        return 0
    from smartlead.notify import _post, post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    ts = post_with_thread(token, dg.channel(), head, parts) if token else None
    if not ts:
        print("UNPOSTED: the weekly test results post did not go out")
        return 1
    if dev:
        from digest_post import dev_channel
        if not _post(token, dev_channel(), dev):
            print("  [Weekly tests] the bot-health message did not go out")
    db[POSTED].replace_one({"_id": key}, {"_id": key, "at": now.isoformat(), "ts": ts, "nights": ctx["nights"]},
                           upsert=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
