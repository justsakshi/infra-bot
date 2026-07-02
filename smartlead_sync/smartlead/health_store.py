"""MongoDB history for daily inbox health scores. No-ops if Mongo is unavailable."""
from __future__ import annotations

import os
from datetime import date, timedelta

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, HEALTH_HISTORY_COLLECTION


class HealthStore:
    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Health] Mongo unavailable (no MONGO_URI) - history disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][HEALTH_HISTORY_COLLECTION]
            self._col.create_index([("client", 1), ("email", 1), ("date", 1)], unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Health] Mongo connect failed ({exc}) - history disabled.")
            self._col = None

    def save_daily(self, records: list[dict]) -> int:
        if self._col is None or not records:
            return 0
        ops = [
            UpdateOne(
                {"client": r["client"], "email": r["email"], "date": r["date"]},
                {"$set": r}, upsert=True,
            )
            for r in records
        ]
        try:
            res = self._col.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        except PyMongoError as exc:
            print(f"  [Health] Mongo write failed: {exc}")
            return 0

    def prior_score(self, client: str, email: str, days_ago: int, today: date) -> int | None:
        if self._col is None:
            return None
        target = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        try:
            # nearest record on-or-before the target date (within a 3-day window)
            floor = (today - timedelta(days=days_ago + 3)).strftime("%Y-%m-%d")
            doc = self._col.find_one(
                {"client": client, "email": email, "date": {"$lte": target, "$gte": floor}},
                sort=[("date", -1)],
            )
            return int(doc["score"]) if doc and "score" in doc else None
        except PyMongoError:
            return None

    def close(self) -> None:
        pass
