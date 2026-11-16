"""One readable daily digest instead of a post per job (Manveen, 16 Nov 2026: "One daily digest"; "if you would just
spam that every check 15 minutes or 30 minutes throughout the day nobody will read that and nothing will happen").

A job no longer posts its own report to the inbox channel. It hands the digest three things:

  * asks: what a person must do, each with the exact step. A job's asks REPLACE its previous ones (they are the
    job's current view), so a fixed problem drops out the next time the job runs. ``since`` is kept per ask key,
    so the digest can say how long an ask has been open.
  * done: what the job did by itself (inboxes pulled, tagged, returned, test copy refreshed ...). Each is listed
    once, in the next digest.
  * detail: the job's full report, posted in the digest's thread for whoever wants it.

``urgent`` items (a declined card, a live campaign losing half its inboxes, a campaign left with only failed
inboxes) are posted at once, one time per key, and stay in the digest as asks until the job stops reporting them.

The digest itself is posted by digest_post.py, once a day in the morning (IST), addressed to Manveen.

Safety: if Mongo can't be written, report() returns False and the caller posts the old way, so an ask is never
lost. DIGEST_MODE=off on a server turns the whole thing off (every job posts as before).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
OWNER = "U09TQ9D7YNM"            # Manveen (16 Nov 2026: "no i mean to say me manveen not anjali")
ASKS = "digest_asks"             # one doc per source: {_id: source, asks: [...], detail: [...], at}
DONE = "digest_done"             # one doc per thing done: {_id: key, source, text, at, posted_on}
URGENT = "digest_urgent"         # one doc per urgent key already posted: {_id: key, source, text, at}
META = "digest_meta"             # {_id: "last", at, ts, asks, done}: when the digest last went out


def on() -> bool:
    """The digest is on unless DIGEST_MODE is off (the undo: set DIGEST_MODE=off and every job posts as before)."""
    return (os.getenv("DIGEST_MODE") or "on").strip().lower() not in ("off", "0", "false", "no")


@dataclass
class Ask:
    text: str                    # what is wrong, in one plain sentence
    step: str = ""               # the exact thing to do
    key: str = ""                # stable id across runs (counts may change, the key does not)
    kind: str = "ask"            # "ask" (a person must act) or "broken" (a check or job that broke)
    urgent: bool = False         # also post at once, one time per key

    def doc(self, source: str) -> dict:
        return {"text": self.text, "step": self.step, "key": self.key or default_key(source, self.text),
                "kind": self.kind if self.kind in ("ask", "broken") else "ask", "urgent": bool(self.urgent)}


@dataclass
class Done:
    text: str
    key: str = ""


def default_key(source: str, text: str) -> str:
    """A key from the words of a line, numbers left out, so '7 inboxes are disconnected' stays one ask when it
    becomes 6."""
    words = re.sub(r"[^a-z ]+", " ", str(text).lower())
    return f"{source}:" + "-".join(words.split())[:80]


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[HEALTH_HISTORY_DB]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def merge_asks(source: str, asks: list, before: list[dict] | None, now: datetime) -> list[dict]:
    """The new asks, each keeping the ``since`` of the same key in the job's previous asks."""
    since = {a.get("key"): a.get("since") for a in (before or []) if a.get("key")}
    out, seen = [], set()
    for a in asks:
        a = a if isinstance(a, Ask) else Ask(str(a))
        d = a.doc(source)
        if d["key"] in seen:
            continue
        seen.add(d["key"])
        d["since"] = since.get(d["key"]) or now.isoformat(timespec="seconds")
        out.append(d)
    return out


def report(source: str, asks: list | None = None, done: list | None = None, detail: list[str] | None = None,
           *, db=None, now: datetime | None = None, post_urgent=None) -> bool:
    """Hand the digest a job's current asks (replacing its previous ones), what it did by itself, and its full
    report for the thread. Urgent asks are posted at once, one time per key. False when Mongo can't be written:
    the caller then posts the old way."""
    now = now or _now()
    try:
        db = db if db is not None else _db()
        prev = db[ASKS].find_one({"_id": source}) or {}
        merged = merge_asks(source, list(asks or []), prev.get("asks"), now)
        db[ASKS].replace_one({"_id": source}, {"_id": source, "asks": merged, "detail": list(detail or [])[:20],
                                               "at": now.isoformat(timespec="seconds")}, upsert=True)
        for d in done or []:
            d = d if isinstance(d, Done) else Done(str(d))
            key = d.key or f"{default_key(source, d.text)}:{now.astimezone(IST):%Y-%m-%d}"
            if not db[DONE].find_one({"_id": key}):
                db[DONE].replace_one({"_id": key}, {"_id": key, "source": source, "text": d.text,
                                                    "at": now.isoformat(timespec="seconds"), "posted_on": None},
                                     upsert=True)
    except Exception as exc:  # noqa: BLE001 - never lose an ask: the caller posts instead
        print(f"  [Digest] not written ({type(exc).__name__}); posting the old way")
        return False
    for a in merged:
        if a["urgent"]:
            _urgent(db, source, a, now, post_urgent)
    print(f"  [Digest] {source}: {len(merged)} ask(s), {len(done or [])} done, {len(detail or [])} detail part(s)")
    return True


def urgent_text(source: str, a: dict) -> str:
    return "\n".join([f"🚨 {a['text']}", "", f"<@{OWNER}> {a['step']}" if a.get("step") else f"<@{OWNER}>",
                      "", "It stays in the daily digest until it is fixed."])


def _urgent(db, source: str, a: dict, now: datetime, post_urgent=None) -> bool:
    """Post an urgent ask at once, one time per key (a crash between post and record posts it at most twice)."""
    try:
        if db[URGENT].find_one({"_id": a["key"]}):
            return False
    except Exception:  # noqa: BLE001
        return False
    poster = post_urgent or _post_now
    if not poster(urgent_text(source, a)):
        return False
    try:
        db[URGENT].replace_one({"_id": a["key"]}, {"_id": a["key"], "source": source, "text": a["text"],
                                                  "at": now.isoformat(timespec="seconds")}, upsert=True)
    except Exception:  # noqa: BLE001
        pass
    return True


def channel() -> str:
    from smartlead.config import INFRA_CHANNEL
    return (os.getenv("DIGEST_CHANNEL") or "").strip() or INFRA_CHANNEL


def _post_now(text: str) -> bool:
    from smartlead.notify import _post
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        print("  [Digest] SLACK_BOT_TOKEN missing: urgent item not posted")
        return False
    return bool(_post(token, channel(), text))
