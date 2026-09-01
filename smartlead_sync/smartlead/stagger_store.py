"""Mongo persistence for company-staggered batches.

Three collections in the shared ``infrabot`` database:

``stagger_batches``
    One per upload: which campaign(s) the leads feed, the daily cap, who
    uploaded it, and running counts.

``stagger_companies``
    One per company per batch: its state (ACTIVE / PAUSED_REPLY /
    PAUSED_BOUNCE / EXHAUSTED) and how many of its leads have gone out. This
    is what makes a reply or a bounce stop the *company* rather than just the
    one lead.

``stagger_leads``
    One per lead: its Smartlead payload, its company, its state, and the
    campaign it was pushed to. The payload is stored whole so a lead can be
    released days after upload without the operator re-uploading the CSV.

Idempotence matters more than speed here. The executor may be re-run after a
crash or a double cron fire, so a lead is claimed by moving it out of QUEUED
in a single atomic update - a second run finds nothing to claim rather than
emailing the same person twice.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

from smartlead.config import HEALTH_HISTORY_DB
from smartlead.stagger import CompanyBook, CompanyState, LeadState, ParsedLead

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

BATCHES = os.getenv("STAGGER_BATCHES_COLLECTION", "stagger_batches")
COMPANIES = os.getenv("STAGGER_COMPANIES_COLLECTION", "stagger_companies")
LEADS = os.getenv("STAGGER_LEADS_COLLECTION", "stagger_leads")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class StaggerStore:
    """Thin wrapper over the three collections.

    Raises on construction when Mongo is unreachable: a staggering run that
    silently proceeds without persistence would re-send the same leads every
    day, so failing loudly is the safe behaviour.
    """

    def __init__(self, uri: str | None = None):
        uri = uri or os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            raise RuntimeError(
                "MONGO_URI is not set or pymongo is missing - staggering needs "
                "persistence to know who has already been contacted")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client.get_default_database()
        self.db = db if db is not None else client[HEALTH_HISTORY_DB]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.db[LEADS].create_index([("batch_id", 1), ("email", 1)], unique=True)
        self.db[LEADS].create_index([("batch_id", 1), ("state", 1), ("company", 1)])
        self.db[LEADS].create_index("email")
        self.db[COMPANIES].create_index([("batch_id", 1), ("company", 1)], unique=True)
        self.db[BATCHES].create_index("created_at")

    # ── upload ───────────────────────────────────────────────────────────

    def create_batch(self, *, name: str, account: str, tracked_campaign_id: int | None,
                     untracked_campaign_id: int | None, daily_cap: int,
                     tracked_first: int, created_by: str = "",
                     company_column: str | None = None) -> str:
        """Register an upload and return its batch id.

        ``tracked_first`` is how many leads go to the open-tracking twin
        before the rest switch to the untracked campaign - the confirm-then-
        send pattern the twin feature exists for.
        """
        doc = {
            "name": name, "account": account,
            "tracked_campaign_id": tracked_campaign_id,
            "untracked_campaign_id": untracked_campaign_id,
            "daily_cap": int(daily_cap), "tracked_first": int(tracked_first),
            "company_column": company_column,
            "created_by": created_by, "created_at": _now(),
            "paused": False,
        }
        return str(self.db[BATCHES].insert_one(doc).inserted_id)

    def add_leads(self, batch_id: str, leads: list[ParsedLead]) -> dict[str, int]:
        """Store parsed leads and their companies. Returns insert counts.

        Re-uploading the same CSV into the same batch is harmless: leads are
        keyed by (batch_id, email) and existing rows are left untouched, so a
        lead already sent is never reset to QUEUED.
        """
        if not leads:
            return {"leads": 0, "companies": 0}

        lead_ops = [
            UpdateOne(
                {"batch_id": batch_id, "email": lead.email},
                {"$setOnInsert": {
                    "batch_id": batch_id, "email": lead.email,
                    "company": lead.company, "payload": lead.to_smartlead(),
                    "state": LeadState.QUEUED.value, "created_at": _now(),
                    "campaign_id": None, "sent_at": None,
                }},
                upsert=True,
            )
            for lead in leads
        ]
        lead_res = self.db[LEADS].bulk_write(lead_ops, ordered=False)

        company_ops = [
            UpdateOne(
                {"batch_id": batch_id, "company": company},
                {"$setOnInsert": {
                    "batch_id": batch_id, "company": company,
                    "state": CompanyState.ACTIVE.value, "sent": 0,
                    "created_at": _now(),
                }},
                upsert=True,
            )
            for company in {lead.company for lead in leads}
        ]
        company_res = self.db[COMPANIES].bulk_write(company_ops, ordered=False)

        return {"leads": lead_res.upserted_count, "companies": company_res.upserted_count}

    # ── reading ──────────────────────────────────────────────────────────

    def get_batch(self, batch_id: str) -> dict | None:
        from bson import ObjectId  # local import: only needed on the Mongo path
        try:
            return self.db[BATCHES].find_one({"_id": ObjectId(batch_id)})
        except Exception:  # noqa: BLE001 - malformed id is a not-found, not a crash
            return None

    def list_batches(self, limit: int = 50) -> list[dict]:
        out = []
        for doc in self.db[BATCHES].find().sort("created_at", -1).limit(limit):
            bid = str(doc["_id"])
            doc["id"] = bid
            doc.pop("_id", None)
            doc["counts"] = self.lead_counts(bid)
            doc["company_counts"] = self.company_counts(bid)
            out.append(doc)
        return out

    def lead_counts(self, batch_id: str) -> dict[str, int]:
        pipeline = [{"$match": {"batch_id": batch_id}},
                    {"$group": {"_id": "$state", "n": {"$sum": 1}}}]
        return {d["_id"]: d["n"] for d in self.db[LEADS].aggregate(pipeline)}

    def company_counts(self, batch_id: str) -> dict[str, int]:
        pipeline = [{"$match": {"batch_id": batch_id}},
                    {"$group": {"_id": "$state", "n": {"$sum": 1}}}]
        return {d["_id"]: d["n"] for d in self.db[COMPANIES].aggregate(pipeline)}

    def company_books(self, batch_id: str) -> list[CompanyBook]:
        """Build the release input: every company with its unsent leads.

        Only QUEUED leads are listed, so ``queued[0]`` is always the next
        person to contact - the invariant :func:`plan_release` relies on.
        """
        queued: dict[str, list[str]] = {}
        for doc in self.db[LEADS].find(
                {"batch_id": batch_id, "state": LeadState.QUEUED.value},
                {"email": 1, "company": 1, "_id": 0}).sort("created_at", 1):
            queued.setdefault(doc["company"], []).append(doc["email"])

        books = []
        for doc in self.db[COMPANIES].find({"batch_id": batch_id}):
            books.append(CompanyBook(
                key=doc["company"],
                state=CompanyState(doc.get("state", CompanyState.ACTIVE.value)),
                queued=queued.get(doc["company"], []),
                sent=int(doc.get("sent", 0)),
            ))
        return books

    def leads_by_email(self, batch_id: str, emails: list[str]) -> dict[str, dict]:
        if not emails:
            return {}
        cursor = self.db[LEADS].find({"batch_id": batch_id, "email": {"$in": emails}})
        return {doc["email"]: doc for doc in cursor}

    # ── writing ──────────────────────────────────────────────────────────

    def claim_leads(self, batch_id: str, emails: list[str], campaign_id: int) -> list[str]:
        """Atomically move leads QUEUED -> SENT and return the ones we won.

        The state check is part of the update, so two concurrent executor runs
        cannot both claim the same lead - the loser's update matches nothing.
        Claiming BEFORE the Smartlead call is deliberate: a duplicate send is
        worse than a missed one, and a lead left claimed but unsent shows up
        in the batch counts rather than being silently re-queued.
        """
        won: list[str] = []
        for email in emails:
            res = self.db[LEADS].update_one(
                {"batch_id": batch_id, "email": email, "state": LeadState.QUEUED.value},
                {"$set": {"state": LeadState.SENT.value, "campaign_id": int(campaign_id),
                          "sent_at": _now()}},
            )
            if res.modified_count:
                won.append(email)
        return won

    def release_claim(self, batch_id: str, emails: list[str]) -> int:
        """Put claimed leads back in the queue after a failed push."""
        if not emails:
            return 0
        res = self.db[LEADS].update_many(
            {"batch_id": batch_id, "email": {"$in": emails},
             "state": LeadState.SENT.value},
            {"$set": {"state": LeadState.QUEUED.value, "campaign_id": None,
                      "sent_at": None}},
        )
        return res.modified_count

    def bump_sent(self, batch_id: str, companies: list[str]) -> None:
        """Record one more send against each company, marking those with no
        leads left as EXHAUSTED so they stop being considered."""
        for company in companies:
            self.db[COMPANIES].update_one(
                {"batch_id": batch_id, "company": company},
                {"$inc": {"sent": 1}, "$set": {"last_sent_at": _now()}},
            )
        for company in set(companies):
            remaining = self.db[LEADS].count_documents(
                {"batch_id": batch_id, "company": company,
                 "state": LeadState.QUEUED.value})
            if remaining == 0:
                self.db[COMPANIES].update_one(
                    {"batch_id": batch_id, "company": company,
                     "state": CompanyState.ACTIVE.value},
                    {"$set": {"state": CompanyState.EXHAUSTED.value}},
                )

    def set_company_state(self, batch_id: str, company: str, state: CompanyState,
                          *, reason: str = "") -> bool:
        """Move a company to a terminal state and skip its queued leads.

        Only ACTIVE and EXHAUSTED companies are moved: a company already
        paused by a reply must not be quietly downgraded to a bounce pause,
        because the two mean different things when someone reads the batch.
        """
        res = self.db[COMPANIES].update_one(
            {"batch_id": batch_id, "company": company,
             "state": {"$in": [CompanyState.ACTIVE.value, CompanyState.EXHAUSTED.value]}},
            {"$set": {"state": state.value, "state_reason": reason,
                      "state_changed_at": _now()}},
        )
        if not res.modified_count:
            return False
        self.db[LEADS].update_many(
            {"batch_id": batch_id, "company": company, "state": LeadState.QUEUED.value},
            {"$set": {"state": LeadState.SKIPPED.value, "skip_reason": reason}},
        )
        return True

    def mark_lead(self, batch_id: str, email: str, state: LeadState,
                  *, reason: str = "") -> bool:
        res = self.db[LEADS].update_one(
            {"batch_id": batch_id, "email": email},
            {"$set": {"state": state.value, "state_reason": reason,
                      "state_changed_at": _now()}},
        )
        return bool(res.modified_count)

    def find_lead_anywhere(self, email: str) -> dict | None:
        """Locate a lead across batches - the reply webhook knows the address
        and campaign, not which upload it came from."""
        return self.db[LEADS].find_one({"email": email.strip().lower()},
                                       sort=[("created_at", -1)])

    def set_batch_paused(self, batch_id: str, paused: bool) -> bool:
        from bson import ObjectId
        try:
            res = self.db[BATCHES].update_one({"_id": ObjectId(batch_id)},
                                              {"$set": {"paused": bool(paused)}})
        except Exception:  # noqa: BLE001
            return False
        return bool(res.modified_count)


def store_or_none() -> StaggerStore | None:
    """Best-effort store for callers that must not crash (dashboard reads)."""
    try:
        return StaggerStore()
    except Exception as exc:  # noqa: BLE001
        print(f"  [Stagger] store unavailable: {exc}", file=sys.stderr)
        return None
