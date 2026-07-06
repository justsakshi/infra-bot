"""Mongo audit log + dedupe for inbox rotation swaps. Fail-safe like the others."""
from __future__ import annotations

import os
from datetime import date

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, ROTATION_LOG_COLLECTION


class RotationStore:
    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Rotation] Mongo unavailable - log disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][ROTATION_LOG_COLLECTION]
            self._col.create_index([("victim_email", 1), ("campaign_name", 1)])
        except Exception as exc:  # noqa: BLE001
            print(f"  [Rotation] Mongo connect failed ({exc}) - log disabled.")
            self._col = None

    @property
    def available(self) -> bool:
        return self._col is not None

    def already_swapped(self) -> set[tuple]:
        """(victim_email, campaign_name) pairs already swapped — never re-swap."""
        if self._col is None:
            return set()
        try:
            return {(d.get("victim_email", "").lower(), d.get("campaign_name", ""))
                    for d in self._col.find({"status": "done"})}
        except PyMongoError:
            return set()

    def log_swap(self, swap: dict, status: str, detail: str = "") -> None:
        if self._col is None:
            return
        try:
            self._col.insert_one({**swap, "status": status, "detail": detail,
                                  "date": date.today().strftime("%Y-%m-%d")})
        except PyMongoError as exc:
            print(f"  [Rotation] log failed: {exc}")
