#!/usr/bin/env python3
"""Rules review: are the pipeline's rules still right? Read-only.

    python rules_review.py            # print the review
    python rules_review.py --json     # the review and its evidence table as JSON
    python rules_review.py --post     # post it to the infra channel (headline + thread)

Holds each rule up against an outcome it did not use (smartlead/rules_review.py): the placement test against the
daily reply check, and the return rule against what returned inboxes did next. Reads the v2 verdicts, the reply
check's stored days and the asset tracker from Mongo. It never changes a rule or anything in Smartlead.
Exit code 0 = reviewed, 2 = could not read."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".." / ".env")
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from smartlead import rules_review as rv  # noqa: E402

V2_LOOKBACK_DAYS = 90      # far enough back to see an inbox fail, come back and fail again


def read(today: date) -> tuple[list[dict], list[dict], list[dict]]:
    """(v2 verdict rows, reply-check days, tracker INBOX rows) from Mongo. Raises when it cannot be read."""
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB, REPLY_STATS_COLLECTION
    from smartlead.zapmail_asset_sync import ASSETS_COLLECTION, _db
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    hist = MongoClient(uri, serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]
    v2 = list(hist["v2_verdicts"].find({"night": {"$gte": (today - timedelta(days=V2_LOOKBACK_DAYS)).isoformat()}},
                                       {"_id": 0, "email": 1, "night": 1, "verdict": 1, "client": 1}))
    replies = list(hist[REPLY_STATS_COLLECTION].find(
        {"date": {"$gte": (today - timedelta(days=rv.REPLY_DAYS)).isoformat()}},
        {"_id": 0, "domain": 1, "date": 1, "verdict": 1, "window_sent": 1, "window_replies": 1, "expected": 1}))
    db = _db()
    if db is None:
        raise RuntimeError("the asset tracker could not be read")
    assets = list(db[ASSETS_COLLECTION].find({"type": "INBOX"}, {"_id": 0, "name": 1, "type": 1, "workspace": 1}))
    return v2, replies, assets


def _post(headline: list[str], thread: list[str]) -> bool:
    from smartlead.notify import infra_channel, post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = infra_channel("RULES_REVIEW_CHANNEL", "ZAPMAIL_NOTIFY_CHANNEL")
    return bool(token and channel and post_with_thread(token, channel, headline, thread))


def main() -> int:
    ap = argparse.ArgumentParser(description="Review the pipeline's rules against real outcomes (read-only)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--post", action="store_true")
    args = ap.parse_args()
    today = date.today()
    try:
        res = rv.review(*read(today), today)
    except Exception as exc:  # noqa: BLE001 - say so; the type only
        msg = f"Rules review could not read its data ({type(exc).__name__}), so no rule was reviewed"
        if args.post:
            _post([f"🚨 {msg}"], ["Nothing was changed."])
        print(json.dumps({"error": msg}) if args.json else f"ERROR: {msg}")
        return 2
    head, thread = rv.format_review(res)
    if args.post:
        res["posted"] = _post(head, thread)
    print(json.dumps(res, default=str) if args.json else "\n".join(head) + "\n\n" + "\n\n".join(thread))
    return 0


if __name__ == "__main__":
    sys.exit(main())
