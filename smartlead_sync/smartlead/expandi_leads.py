"""Per-lead Expandi activity, cached in Mongo.

Expandi's campaign `stats` are lifetime counters with no date filter, so a
"sent yesterday" or "sent this month" figure cannot be read from them. The
`messengers` endpoint carries per-lead timestamps — `invited_at` (connection
request sent) and `connected_at` (accepted) — which give exact daily figures
and, unlike snapshot differencing, cover history that predates this code.

Verified against campaign 812428: 60 rows with `invited_at` versus a stats
counter of 60 initiated, and 7 with `connected_at` versus 7 connected. Exact.

The catch is cost. The endpoint returns ~4.6 rows/second regardless of page
size (a 200-row page simply takes 20x longer), and the two BettrData accounts
hold ~11,000 leads — about 98 minutes to sweep in full. That is far too slow
for a daily job, so this module caches.

The cache is sound because the fields are immutable in practice: a lead invited
on 28 July carries that timestamp forever. Only leads that are new, or whose
`updated` has advanced, need re-fetching. A full sweep happens once; subsequent
runs read the tail.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, EXPANDI_LEADS_COLLECTION


def _day(value) -> str | None:
    """Date portion of an Expandi timestamp, or None.

    Expandi returns local-offset stamps like '2026-07-10T21:05:01+0200'. The
    date is taken after converting to UTC so a late-evening action is not
    counted under the following day (or the previous one, west of UTC) — the
    rest of the tab buckets by UTC and a mixed convention would put the same
    action on different days depending on the platform reporting it.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        # Expandi's offsets have no colon ('+0200'), which older parsers reject.
        try:
            dt = datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()


class ExpandiLeadStore:
    """Mongo-backed cache of per-lead invite/connect timestamps."""

    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Expandi] Mongo unavailable — per-day activity disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][EXPANDI_LEADS_COLLECTION]
            self._col.create_index(
                [("workspace", 1), ("messenger_id", 1)], unique=True)
            # Serves the per-campaign day counts without a collection scan.
            self._col.create_index([("workspace", 1), ("campaign_name", 1)])
        except Exception as exc:  # noqa: BLE001
            print(f"  [Expandi] Mongo connect failed ({exc}) — per-day activity disabled.")
            self._col = None

    @property
    def available(self) -> bool:
        return self._col is not None

    def mark_swept(self, workspace: str, campaign_name: str) -> None:
        """Record that this campaign's messenger pagination ran to the end.

        Needed because cached-row count cannot be compared against
        `stats.initiated` to decide completeness: for some campaigns the two
        genuinely disagree. Campaign 722234 reports initiated=60 while the
        messengers endpoint returns 62 rows of which only 49 carry an
        `invited_at` — contacts messaged directly, without a connection
        request, are counted by the stats but have no invite timestamp.

        Comparing against `initiated` therefore leaves such campaigns pinned to
        the fallback path forever, always "backfilling", never trusted. What
        actually matters is whether the sweep reached the last page.
        """
        if self._col is None:
            return
        try:
            self._col.update_many(
                {"workspace": workspace, "campaign_name": campaign_name},
                {"$set": {"sweep_complete": True}})
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] mark_swept failed: {exc}")

    def swept_campaigns(self, workspace: str) -> set[str]:
        """Campaigns whose messenger pagination has been exhausted."""
        if self._col is None:
            return set()
        try:
            return set(self._col.distinct(
                "campaign_name",
                {"workspace": workspace, "sweep_complete": True}))
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] swept lookup failed: {exc}")
            return set()

    def known_ids(self, workspace: str) -> set[int]:
        """Messenger ids already cached, so a sweep can stop early."""
        if self._col is None:
            return set()
        try:
            return {d["messenger_id"] for d in
                    self._col.find({"workspace": workspace}, {"messenger_id": 1})}
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] cache read failed: {exc}")
            return set()

    def save(self, workspace: str, campaign_name: str, messengers: list[dict]) -> int:
        if self._col is None or not messengers:
            return 0
        ops = []
        for m in messengers:
            mid = m.get("id")
            if mid is None:
                continue
            ops.append(UpdateOne(
                {"workspace": workspace, "messenger_id": mid},
                {"$set": {
                    "workspace": workspace,
                    "messenger_id": mid,
                    "campaign_name": campaign_name,
                    "invited_day": _day(m.get("invited_at")),
                    "connected_day": _day(m.get("connected_at")),
                    "first_step_day": _day(m.get("first_step_datetime")),
                    "last_day": _day(m.get("last_datetime")),
                    "updated": m.get("updated"),
                }},
                upsert=True))
        if not ops:
            return 0
        try:
            res = self._col.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] cache write failed: {exc}")
            return 0

    def counts(self, workspace: str, campaign_name: str,
               start: date, end: date, yesterday: date) -> dict:
        """Per-day activity for one campaign, aggregated in Mongo.

        Returns month and yesterday counts for invites and accepts. Aggregating
        server-side keeps this O(matching docs) rather than pulling every lead
        into memory on each run.
        """
        empty = {"invited_month": 0, "invited_yesterday": 0,
                 "connected_month": 0, "connected_yesterday": 0, "cached": 0,
                 "swept": False}
        if self._col is None:
            return empty
        s, e, y = start.isoformat(), end.isoformat(), yesterday.isoformat()
        try:
            cur = self._col.aggregate([
                {"$match": {"workspace": workspace, "campaign_name": campaign_name}},
                {"$group": {
                    "_id": None,
                    "cached": {"$sum": 1},
                    # The explicit string type-check matters: invited_day is
                    # null for a lead never invited, and BSON orders null below
                    # every string. Relying on that ordering to exclude nulls
                    # from a range comparison works, but silently — a future
                    # change to the bounds could start counting never-invited
                    # leads as this month's activity with nothing to flag it.
                    "invited_month": {"$sum": {"$cond": [
                        {"$and": [{"$eq": [{"$type": "$invited_day"}, "string"]},
                                  {"$gte": ["$invited_day", s]},
                                  {"$lte": ["$invited_day", e]}]}, 1, 0]}},
                    "invited_yesterday": {"$sum": {"$cond": [
                        {"$eq": ["$invited_day", y]}, 1, 0]}},
                    "connected_month": {"$sum": {"$cond": [
                        {"$and": [{"$eq": [{"$type": "$connected_day"}, "string"]},
                                  {"$gte": ["$connected_day", s]},
                                  {"$lte": ["$connected_day", e]}]}, 1, 0]}},
                    "connected_yesterday": {"$sum": {"$cond": [
                        {"$eq": ["$connected_day", y]}, 1, 0]}},
                    # True when the sweep has walked this campaign's messengers
                    # to the last page — see mark_swept for why row counts
                    # cannot answer this.
                    "swept": {"$max": {"$ifNull": ["$sweep_complete", False]}},
                }},
            ])
            for doc in cur:
                doc.pop("_id", None)
                return {**empty, **doc}
            return empty
        except PyMongoError as exc:  # noqa: BLE001
            print(f"  [Expandi] cache aggregate failed: {exc}")
            return empty
