"""Daily snapshots of Expandi's all-time campaign counters.

Expandi reports cumulative lifetime totals and offers no working date filter —
start_date/end_date and date_from/date_to are all accepted and silently
ignored. The only way to get a month-to-date figure is to record the counters
each day and subtract an earlier snapshot from today's.

That has a consequence worth stating plainly: **month-to-date is only correct
from the first snapshot onward.** There is no history to backfill from, so a
campaign's first reported month will read low until a baseline exists. The row
builder marks this rather than showing a number it cannot stand behind.

No-ops if Mongo is unavailable, like HealthStore.
"""
from __future__ import annotations

import os
from datetime import date

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, EXPANDI_SNAPSHOT_COLLECTION

# The cumulative counters worth storing. Deliberately not the whole stats dict:
# step_count and latest_action_id are not counters, and differencing them would
# produce nonsense.
COUNTER_FIELDS = (
    "people_in_campaign", "in_queue", "initiated", "connected",
    "contacted_people", "replied_msg", "replied_excl_msg",
    "interested_people", "finished", "stopped",
)


class ExpandiStore:
    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Expandi] Mongo unavailable (no MONGO_URI) - month-to-date disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][EXPANDI_SNAPSHOT_COLLECTION]
            self._col.create_index(
                [("workspace", 1), ("campaign_id", 1), ("date", 1)], unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Expandi] Mongo connect failed ({exc}) - month-to-date disabled.")
            self._col = None

    @property
    def available(self) -> bool:
        return self._col is not None

    def save_snapshot(self, workspace: str, campaigns: list[dict],
                      today: date) -> int:
        """Record today's counters for each campaign. Idempotent — re-running
        the sync on the same day overwrites rather than duplicating."""
        if self._col is None or not campaigns:
            return 0
        ops = []
        for c in campaigns:
            stats = c.get("stats") or {}
            doc = {
                "workspace": workspace,
                "campaign_id": c.get("id"),
                "campaign_name": c.get("name", ""),
                "date": today.isoformat(),
            }
            for f in COUNTER_FIELDS:
                doc[f] = int(stats.get(f) or 0)
            ops.append(UpdateOne(
                {"workspace": workspace, "campaign_id": doc["campaign_id"],
                 "date": doc["date"]},
                {"$set": doc}, upsert=True))
        try:
            res = self._col.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] snapshot save failed: {exc}")
            return 0

    def baseline(self, workspace: str, campaign_id: int,
                 month_start: date) -> dict | None:
        """The latest snapshot at or before `month_start`, or None.

        Returning None matters: it means no baseline exists, so month-to-date is
        unknowable for this campaign and the caller must say so rather than
        report the all-time figure as if it were this month's.
        """
        if self._col is None:
            return None
        try:
            return self._col.find_one(
                {"workspace": workspace, "campaign_id": campaign_id,
                 "date": {"$lte": month_start.isoformat()}},
                sort=[("date", -1)],
            )
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] baseline lookup failed: {exc}")
            return None

    def previous_day(self, workspace: str, campaign_id: int,
                     today: date) -> dict | None:
        """The most recent snapshot strictly before today — used for the
        'yesterday' columns."""
        if self._col is None:
            return None
        try:
            return self._col.find_one(
                {"workspace": workspace, "campaign_id": campaign_id,
                 "date": {"$lt": today.isoformat()}},
                sort=[("date", -1)],
            )
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] previous-day lookup failed: {exc}")
            return None
