"""Which domain to test next, and with which inbox.

This is the scheduling brain of the weekly placement run. It was previously
implied by the deliverability grid, which meant the schedule could only be as
correct as a hand-edited sheet — and because results were never written back to
that grid, every domain read as untested forever and the same few were
re-selected every run.

State lives in Mongo (`PlacementSchedule`); the decisions are pure functions
here so they can be tested without it.

Rotation advances on a *recorded result*, pass or fail. Advancing only on
success would re-test the same broken inbox every week and never reach the
other inboxes on that domain.
"""
from __future__ import annotations

import os
from datetime import date, datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB

SCHEDULE_COLLECTION = "placement_schedule"


def next_inbox(inboxes: list[str], rotation_index: int) -> str | None:
    """The inbox whose turn it is. Sorted so the order does not depend on
    whatever sequence Smartlead happened to return, and modulo so a shrinking
    inbox list cannot push the index out of range."""
    if not inboxes:
        return None
    ordered = sorted(inboxes)
    return ordered[rotation_index % len(ordered)]


def is_due(record: dict, today: str, interval_days: int) -> bool:
    """True when a domain has never been tested, or was last tested at least
    `interval_days` ago. An unparseable date counts as due: treating it as
    recent would park the domain indefinitely with no visible cause."""
    raw = str(record.get("last_tested_date", "")).strip()
    if not raw:
        return True
    try:
        last = datetime.fromisoformat(raw[:10]).date()
        now = datetime.fromisoformat(today[:10]).date()
    except ValueError:
        return True
    return (now - last).days >= interval_days


def advance(record: dict, email: str, today: str, result: str,
            by_provider: dict) -> dict:
    """Fold a completed result into the domain's record."""
    updated = dict(record)
    updated["rotation_index"] = int(record.get("rotation_index", 0)) + 1
    updated["last_tested_email"] = email
    updated["last_tested_date"] = today
    updated["last_result"] = result
    updated["last_by_provider"] = by_provider
    if result == "inbox":
        updated["consecutive_fails"] = 0
    else:
        updated["consecutive_fails"] = int(record.get("consecutive_fails", 0)) + 1
    return updated


def select_batch(domains: dict[str, dict], today: str, interval_days: int,
                 pending: set[str], cap: int | None = None) -> list[dict]:
    """Choose which domains to test, worst-first.

    `domains` maps domain -> record (carrying at least `inboxes`). `pending` is
    the set of inbox addresses already inside an in-flight test.

    Order matters when credits are short: a domain that has failed repeatedly is
    more urgent than one merely overdue, so the cap keeps the worst rather than
    an alphabetical prefix.
    """
    due: list[dict] = []
    for domain, record in domains.items():
        if not is_due(record, today, interval_days):
            continue
        email = next_inbox(record.get("inboxes", []), int(record.get("rotation_index", 0)))
        if not email or email in pending:
            continue
        due.append({
            "domain": domain,
            "email": email,
            "consecutive_fails": int(record.get("consecutive_fails", 0)),
            "last_tested_date": str(record.get("last_tested_date", "") or ""),
        })

    # Most failures first; then least recently tested, with never-tested
    # (empty string) sorting ahead of any real date.
    due.sort(key=lambda r: (-r["consecutive_fails"], r["last_tested_date"], r["domain"]))
    return due[:cap] if cap else due


class PlacementSchedule:
    """Mongo-backed store for the per-domain schedule."""

    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Schedule] Mongo unavailable - schedule store disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][SCHEDULE_COLLECTION]
            self._col.create_index([("client", 1), ("domain", 1)], unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Schedule] Mongo connect failed ({exc}) - disabled.")
            self._col = None

    @property
    def available(self) -> bool:
        return self._col is not None

    def load(self, client: str) -> dict[str, dict]:
        """All records for a client, keyed by domain."""
        if self._col is None:
            return {}
        try:
            return {d["domain"]: d for d in self._col.find({"client": client})}
        except PyMongoError as exc:
            print(f"  [Schedule] load failed: {exc}")
            return {}

    def sync_inboxes(self, client: str, inboxes_by_domain: dict[str, list[str]]) -> None:
        """Refresh each domain's inbox list from Smartlead.

        Smartlead is the truth for which inboxes exist; the stored list is a
        cache so rotation has something stable to index into.
        """
        if self._col is None:
            return
        for domain, inboxes in inboxes_by_domain.items():
            try:
                self._col.update_one(
                    {"client": client, "domain": domain},
                    {"$set": {"inboxes": sorted(inboxes)},
                     "$setOnInsert": {"rotation_index": 0, "consecutive_fails": 0}},
                    upsert=True,
                )
            except PyMongoError as exc:
                print(f"  [Schedule] sync {domain} failed: {exc}")

    def record_result(self, client: str, domain: str, email: str, result: str,
                      by_provider: dict, when: str | None = None) -> None:
        if self._col is None:
            return
        today = when or date.today().isoformat()
        try:
            current = self._col.find_one({"client": client, "domain": domain}) or {}
            updated = advance(current, email, today, result, by_provider)
            updated.pop("_id", None)
            self._col.update_one({"client": client, "domain": domain},
                                 {"$set": updated}, upsert=True)
        except PyMongoError as exc:
            print(f"  [Schedule] record_result {domain} failed: {exc}")
