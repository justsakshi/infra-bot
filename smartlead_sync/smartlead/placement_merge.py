"""Overlay API placement results onto the manual deliverability map (newer wins)."""
from __future__ import annotations

import os

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, PLACEMENT_RESULTS_COLLECTION


def apply_api_results(deliverability_map: dict[str, dict]) -> dict[str, dict]:
    """For each domain, if an API result is newer than the manual one, use it.
    deliverability_map: {domain: {"status","date"}}. Returns a new merged map."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return deliverability_map
    try:
        col = MongoClient(uri, serverSelectionTimeoutMS=5000)[HEALTH_HISTORY_DB][PLACEMENT_RESULTS_COLLECTION]
        api_rows = list(col.find({"source": "api"}))
    except Exception:  # noqa: BLE001
        return deliverability_map
    if not api_rows:
        return deliverability_map
    merged = dict(deliverability_map)
    # keep newest api result per domain
    best: dict[str, dict] = {}
    for r in api_rows:
        dom = r.get("domain", "")
        if not dom:
            continue
        if dom not in best or str(r.get("date", "")) > str(best[dom].get("date", "")):
            best[dom] = r
    for dom, r in best.items():
        existing = merged.get(dom)
        if existing is None or str(r.get("date", "")) >= str(existing.get("date", "")):
            merged[dom] = {"status": r.get("status", ""), "date": r.get("date", "")}
    return merged
