"""Mongo state for auto placement tests + merged results."""
from __future__ import annotations

import os
from datetime import date as _date, timedelta

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import (
    HEALTH_HISTORY_DB, PLACEMENT_TESTS_COLLECTION, PLACEMENT_RESULTS_COLLECTION,
)


class PlacementStore:
    def __init__(self) -> None:
        self._tests = None
        self._results = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Retest] Mongo unavailable - state store disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[HEALTH_HISTORY_DB]
            self._tests = db[PLACEMENT_TESTS_COLLECTION]
            self._results = db[PLACEMENT_RESULTS_COLLECTION]
            self._tests.create_index("test_id", unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] Mongo connect failed ({exc}) - disabled.")
            self._tests = self._results = None

    @property
    def available(self) -> bool:
        return self._tests is not None

    def pending_tests(self) -> list[dict]:
        if self._tests is None:
            return []
        try:
            return list(self._tests.find({"status": "ACTIVE"}))
        except PyMongoError:
            return []

    def pending_emails(self) -> set[str]:
        out: set[str] = set()
        for t in self.pending_tests():
            out.update(t.get("emails", []))
        return out

    def record_created(self, test_id: int, client: str, campaign_id: int, emails: list[str],
                       warmup_off_ids: list[str] | None = None) -> None:
        if self._tests is None:
            return
        try:
            self._tests.update_one(
                {"test_id": test_id},
                {"$set": {"test_id": test_id, "client": client, "campaign_id": campaign_id,
                          "emails": emails, "status": "ACTIVE",
                          "created": _date.today().strftime("%Y-%m-%d"),
                          "warmup_off_ids": warmup_off_ids or []}},
                upsert=True,
            )
        except PyMongoError as exc:
            print(f"  [Retest] record_created failed: {exc}")

    def abandon_stale(self, max_age_days: int = 3) -> int:
        """Mark tests pending longer than max_age_days as ABANDONED so their
        inboxes free up for re-targeting. Returns how many were abandoned."""
        if self._tests is None:
            return 0
        cutoff = (_date.today() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
        try:
            res = self._tests.update_many(
                {"status": "ACTIVE",
                 "$or": [{"created": {"$lt": cutoff}}, {"created": {"$exists": False}}]},
                {"$set": {"status": "ABANDONED"}},
            )
            return res.modified_count or 0
        except PyMongoError as exc:
            print(f"  [Retest] abandon_stale failed: {exc}")
            return 0

    def mark_done(self, test_id: int, inbox_pct: float, status: str) -> None:
        if self._tests is None:
            return
        try:
            self._tests.update_one({"test_id": test_id},
                                   {"$set": {"status": "DONE", "inbox_pct": inbox_pct, "result": status}})
        except PyMongoError as exc:
            print(f"  [Retest] mark_done failed: {exc}")

    def save_result(self, email: str, domain: str, status: str, date: str, source: str) -> None:
        if self._results is None:
            return
        try:
            self._results.update_one(
                {"email": email, "date": date, "source": source},
                {"$set": {"email": email, "domain": domain, "status": status,
                          "date": date, "source": source}},
                upsert=True,
            )
        except PyMongoError as exc:
            print(f"  [Retest] save_result failed: {exc}")

    def close(self) -> None:
        pass
