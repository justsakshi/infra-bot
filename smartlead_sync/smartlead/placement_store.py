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

    def record_created(self, test_id, client: str, campaign_id, emails: list[str],
                       warmup_off_ids: list[str] | None = None) -> None:
        # test_id is an int for Smartlead SmartDelivery tests and a uuid string
        # for EmailGuard tests — both are stored as-is and disambiguated by the
        # `source` tag (see tag_source).
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

    def stale_tests(self, max_age_days: int = 3) -> list[dict]:
        """ACTIVE tests pending longer than max_age_days — candidates for
        abandonment. Returned (not blind-updated) so the caller can restore
        warmup on each test's warmup_off_ids BEFORE marking it abandoned;
        the old bulk-update version stranded those inboxes with warmup off
        forever (found in 2026-07-08 audit)."""
        if self._tests is None:
            return []
        cutoff = (_date.today() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
        try:
            return list(self._tests.find(
                {"status": "ACTIVE",
                 "$or": [{"created": {"$lt": cutoff}}, {"created": {"$exists": False}}]}))
        except PyMongoError as exc:
            print(f"  [Retest] stale_tests failed: {exc}")
            return []

    def tag_source(self, test_id, source: str, eg_uuid: str = "") -> None:
        """Mark which system owns a test. Smartlead and EmailGuard tests live in
        the same collection but are polled by different executors against
        different APIs — without this tag each would try to poll the other's
        ids and log a stream of failures."""
        if self._tests is None:
            return
        try:
            fields = {"source": source}
            if eg_uuid:
                fields["eg_uuid"] = eg_uuid
            self._tests.update_one({"test_id": test_id}, {"$set": fields})
        except PyMongoError as exc:
            print(f"  [Retest] tag_source failed: {exc}")

    def mark_abandoned(self, test_id: int) -> None:
        if self._tests is None:
            return
        try:
            self._tests.update_one({"test_id": test_id},
                                   {"$set": {"status": "ABANDONED"}})
        except PyMongoError as exc:
            print(f"  [Retest] mark_abandoned failed: {exc}")

    def update_warmup_off_ids(self, test_id: int, ids: list[str]) -> None:
        """Shrink a test's outstanding warmup-off list to the ids whose
        restore is still pending (retry target for the next run)."""
        if self._tests is None:
            return
        try:
            self._tests.update_one({"test_id": test_id},
                                   {"$set": {"warmup_off_ids": ids}})
        except PyMongoError as exc:
            print(f"  [Retest] update_warmup_off_ids failed: {exc}")

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
