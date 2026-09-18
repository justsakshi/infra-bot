"""Sync every SmartDelivery placement test into Mongo, whoever created it.

`PlacementStore` only knows about tests this code creates via the API. Tests run
from the SmartDelivery UI - which is how the trustworthy ones get run, because
API-created tests close on a ~71-minute timer instead of the full 140-160 minute
window (confirmed by Smartlead support 2026-09-18) - were invisible to us. On
2026-09-17 that cost a day: four tests existed (535093, 535281, 535537, 535799),
three of them UI-created, and each had to be found by hand from a browser URL.

This module discovers tests by probing ids and records whatever it finds, so
"what did the last test say" is answerable without a sheet or a URL.

Two things are deliberate:

- A test is only scored when coverage clears PLACEMENT_MIN_COVERAGE. Below that,
  unclassified seeds are excluded from the denominator and Google reports slower
  than Microsoft, so a thin read is biased toward whichever provider answered
  first. Test 535281 read 88% inbox at 49% coverage and 57% at 84% - the domains
  had not changed, the sample had. `scored` records which side of the gate a row
  sits on; callers must check it rather than trusting `inbox_pct` alone.
- Rows are keyed on test_id and upserted, so re-running while a test is still
  ACTIVE refreshes it in place and a finished test stops moving.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timezone

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, PLACEMENT_MIN_COVERAGE

COLLECTION = os.getenv("PLACEMENT_TEST_RESULTS_COLLECTION", "placement_test_results")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _collection():
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][COLLECTION]
        col.create_index("test_id", unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [PlacementSync] Mongo unavailable ({exc})")
        return None


def summarise(test_id: int, report: dict, senders: dict) -> dict:
    """Shape one test into a stored row. Pure - no I/O, no clock beyond synced_at."""
    per_sender, per_domain = {}, defaultdict(lambda: {"inbox": 0, "classified": 0})
    for email, d in (senders or {}).items():
        e = email.lower()
        if not d.get("classified"):
            continue
        per_sender[e] = {
            "inbox": d["inbox"], "spam": d.get("spam", 0),
            "classified": d["classified"], "inbox_pct": round(d["inbox_pct"], 1),
            "spf_fail": d.get("spf_fail", 0), "dkim_fail": d.get("dkim_fail", 0),
            "dmarc_fail": d.get("dmarc_fail", 0),
        }
        dom = per_domain[e.split("@")[-1]]
        dom["inbox"] += d["inbox"]
        dom["classified"] += d["classified"]

    domains = {
        k: {**v, "inbox_pct": round(100.0 * v["inbox"] / v["classified"], 1)}
        for k, v in per_domain.items() if v["classified"]
    }
    coverage = float(report.get("coverage") or 0.0)
    return {
        "test_id": str(test_id),
        "synced_at": _now(),
        "status": report.get("status", ""),
        "coverage": round(coverage, 3),
        # Below the gate the percentages are a biased sample, not a verdict.
        "scored": coverage >= PLACEMENT_MIN_COVERAGE,
        "classified": report.get("classified", 0),
        "dispatched": report.get("dispatched", 0),
        "inbox_pct": round(float(report.get("inbox_pct") or 0.0), 1),
        "spam_pct": round(float(report.get("spam_pct") or 0.0), 1),
        "by_provider": report.get("by_provider") or {},
        "worst_provider_inbox_pct": report.get("worst_provider_inbox_pct"),
        "sender_count": len(per_sender),
        "senders": per_sender,
        "domains": domains,
    }


async def sync_test(client, test_id: int) -> dict | None:
    """Fetch one test. Returns None when the id is not ours or has no data."""
    try:
        report = await client.get_report(test_id)
    except Exception:
        return None
    if not report.get("dispatched") and not report.get("classified"):
        return None
    try:
        senders = await client.get_sender_report(test_id)
    except Exception:
        senders = {}
    # A test id belonging to another account answers the summary endpoint but
    # returns no per-sender rows; that is how we tell ours apart from theirs.
    if not senders:
        return None
    try:
        poll = await client.poll_test(test_id)
        report = {**report, "status": poll.get("status", "")}
    except Exception:
        pass
    return summarise(test_id, report, senders)


async def sync_range(client, start_id: int, end_id: int, store: bool = True) -> list[dict]:
    """Probe ids from start_id..end_id inclusive and record the ones that are ours."""
    col = _collection() if store else None
    found = []
    for tid in range(int(start_id), int(end_id) + 1):
        row = await sync_test(client, tid)
        if not row:
            continue
        found.append(row)
        if col is not None:
            try:
                col.update_one({"test_id": row["test_id"]}, {"$set": row}, upsert=True)
            except PyMongoError as exc:
                print(f"  [PlacementSync] write failed for {tid}: {exc}")
    return found


def stored_tests(limit: int = 20) -> list[dict]:
    col = _collection()
    if col is None:
        return []
    try:
        return list(col.find({}, {"senders": 0}).sort("test_id", -1).limit(limit))
    except PyMongoError:
        return []


def latest_scored_test() -> dict | None:
    """Most recent test that cleared the coverage gate - the last real verdict."""
    col = _collection()
    if col is None:
        return None
    try:
        rows = list(col.find({"scored": True}).sort("test_id", -1).limit(1))
        return rows[0] if rows else None
    except PyMongoError:
        return None


def domain_history(domain: str, limit: int = 10) -> list[dict]:
    """Every scored reading for one domain, newest first."""
    col = _collection()
    if col is None:
        return []
    try:
        out = []
        for r in col.find({"scored": True}).sort("test_id", -1).limit(limit):
            d = (r.get("domains") or {}).get(domain)
            if d:
                out.append({"test_id": r["test_id"], "synced_at": r.get("synced_at"),
                            "coverage": r.get("coverage"), **d})
        return out
    except PyMongoError:
        return []


# ── domain verdicts: the memory that answers "which inboxes can I attach?" ──

VERDICT_COLLECTION = os.getenv("DOMAIN_VERDICTS_COLLECTION", "domain_verdicts")

# Thresholds are the tiers the 2026-09-17 audit used. GOOD is deliberately high:
# in test 535537 every clean domain scored exactly 100% with no partial credit,
# so a domain sitting at 85% is doing something the clean ones are not.
GOOD_AT = 90.0
OK_AT = 60.0
WEAK_AT = 35.0


def verdict_for(inbox_pct: float) -> str:
    if inbox_pct >= GOOD_AT:
        return "GOOD"
    if inbox_pct >= OK_AT:
        return "OK"
    if inbox_pct >= WEAK_AT:
        return "WEAK"
    return "BAD"


def _verdict_collection():
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][VERDICT_COLLECTION]
        col.create_index("domain", unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [PlacementSync] verdict store unavailable ({exc})")
        return None


def refresh_verdicts() -> list[dict]:
    """Rebuild per-domain verdicts from the newest SCORED test.

    Only scored tests count. A 25%-coverage test put every weak domain at 100%
    inbox on 2026-09-18; writing that as a verdict would have told the team a
    28% domain was clean.
    """
    latest = latest_scored_test()
    col = _verdict_collection()
    if not latest or col is None:
        return []
    rows = []
    for domain, d in (latest.get("domains") or {}).items():
        row = {
            "domain": domain,
            "verdict": verdict_for(d["inbox_pct"]),
            "inbox_pct": d["inbox_pct"],
            "seeds": d["classified"],
            "source_test": latest["test_id"],
            "coverage": latest.get("coverage"),
            "updated_at": _now(),
        }
        rows.append(row)
        try:
            col.update_one({"domain": domain}, {"$set": row}, upsert=True)
        except PyMongoError as exc:
            print(f"  [PlacementSync] verdict write failed for {domain}: {exc}")
    return rows


def domain_verdicts() -> dict[str, dict]:
    """Every stored verdict, keyed by domain."""
    col = _verdict_collection()
    if col is None:
        return {}
    try:
        return {r["domain"]: r for r in col.find({}, {"_id": 0})}
    except PyMongoError:
        return {}


def attachable_domains(min_verdict: str = "OK") -> list[str]:
    """Domains safe to attach inboxes from, best first.

    `min_verdict` is the floor: "GOOD" for clean-only, "OK" to include the
    workable tier. Domains with no scored reading are excluded - unknown is not
    the same as good, and that conflation is what let dead mailboxes grade A on
    the Inboxes tab for a week.
    """
    order = {"GOOD": 3, "OK": 2, "WEAK": 1, "BAD": 0}
    floor = order.get(min_verdict.upper(), 2)
    rows = [r for r in domain_verdicts().values()
            if order.get(r.get("verdict"), 0) >= floor]
    rows.sort(key=lambda r: -r.get("inbox_pct", 0))
    return [r["domain"] for r in rows]
