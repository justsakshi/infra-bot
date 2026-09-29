"""Batch / delayed domain purchase backend — the ONLY path that buys domains.

Buying N similar domains in one go is the batch fingerprint that gets a set
restricted. This module spreads a purchase across registrars and days (via
:func:`domain_naming.purchase_schedule`) and lets a human approve ONE batch at a
time, with a Mongo ledger so a batch is never bought twice.

Spending rules (on top of the gates in :mod:`domain_purchase`):

  * ``stage_batches`` is read-only on Zapmail — it checks availability/price,
    plans only buyable names (available, priced, under the ceiling, not already
    in the ledger), and records the plan. Buys nothing.
  * ``execute_one`` requires ``approve=True`` AND ``ZAPMAIL_ALLOW_SPEND`` AND a
    reachable ledger, refuses before the batch's ``earliest_date``, uses the
    client recorded on the batch (a different ``--client`` is refused), and
    claims the batch atomically so two concurrent runs cannot both buy it.
  * If the buy's outcome is unknown (timeout / network error / 5xx) the batch
    is marked ``unknown`` and stays blocked until :func:`reconcile_one` checks
    Zapmail for what was actually bought. Only a definitive 4xx marks it
    ``failed`` (retryable).

Ledger statuses::

    planned ──claim──> in_progress ──> purchased
       ^                   │  ├──────> failed    (definitive 4xx; retryable)
       └──(pre-flight refusal)  └────> unknown   (reconcile first)
    reconcile: unknown/in_progress/failed -> purchased | failed | partial

The ledger lives in the shared infrabot Mongo db (collection
``zapmail_domain_batches``), matching the other stores in this package.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

try:
    from pymongo import MongoClient, ReturnDocument
except ImportError:  # pragma: no cover
    MongoClient = None
    ReturnDocument = None

from smartlead.config import HEALTH_HISTORY_DB
from smartlead.domain_availability import DEFAULT_PRICE_CEILING_USD
from smartlead.domain_naming import purchase_schedule
from smartlead.domain_purchase import (
    execute_purchase, normalize_domains, spend_allowed, stage_purchase,
)
from smartlead.zapmail import (
    ZapmailClient, ZapmailHTTPError, ZapmailOutcomeUnknown, ZapmailSpendBlocked,
)
from smartlead.zapmail_accounts import (
    _norm, account_name_for_client, resolve_account_for_client,
)

BATCH_COLLECTION = os.getenv("ZAPMAIL_BATCH_COLLECTION", "zapmail_domain_batches")

# Default stagger: small batches, 2 days apart, Zapmail-only until separate
# registrar accounts exist (mirrors domain_generator.DEFAULT_REGISTRARS).
DEFAULT_REGISTRARS: tuple[str, ...] = ("Zapmail",)
DEFAULT_PER_BATCH: int = 3
DEFAULT_DAY_GAP: int = 2

# Statuses from which a batch may be (re)bought.
BUYABLE_STATUSES: tuple[str, ...] = ("planned", "failed")
# Statuses that hold a domain — it will not be planned again.
HOLDING_STATUSES: tuple[str, ...] = (
    "planned", "in_progress", "purchased", "unknown", "partial")


@dataclass
class Batch:
    batch_id: str
    day_offset: int
    registrar: str
    domains: list[str]
    status: str = "planned"
    client: str = ""
    earliest_date: str = ""
    prices: dict[str, float | None] = field(default_factory=dict)
    purchased_at: str = ""
    result: dict | None = None

    @property
    def estimated_usd(self) -> float:
        return round(sum(v or 0.0 for v in self.prices.values()), 2)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "day_offset": self.day_offset,
            "registrar": self.registrar,
            "domains": self.domains,
            "status": self.status,
            "client": self.client,
            "earliest_date": self.earliest_date,
            "prices": self.prices,
            "estimated_usd": self.estimated_usd,
            "purchased_at": self.purchased_at,
        }


def _batch_id(registrar: str, day_offset: int, domains: list[str]) -> str:
    raw = f"{registrar}|{day_offset}|{','.join(sorted(domains))}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def plan_batches(
    domains: list[str],
    *,
    registrars: tuple[str, ...] = DEFAULT_REGISTRARS,
    per_batch: int = DEFAULT_PER_BATCH,
    day_gap: int = DEFAULT_DAY_GAP,
    client: str = "",
    today: date | None = None,
) -> list[Batch]:
    """Pure stagger plan. No network, no spend."""
    schedule = purchase_schedule(list(domains), list(registrars),
                                 per_batch=per_batch, day_gap=day_gap)
    today = today or date.today()
    out: list[Batch] = []
    for s in schedule:
        out.append(Batch(
            batch_id=_batch_id(s.registrar, s.day_offset, list(s.domains)),
            day_offset=s.day_offset,
            registrar=s.registrar,
            domains=list(s.domains),
            client=client,
            earliest_date=(today + timedelta(days=s.day_offset)).isoformat(),
        ))
    return out


# ── ledger ───────────────────────────────────────────────────────────────────

def _mongo_db():
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        default_db = client.get_default_database()
        return default_db if default_db is not None else client[HEALTH_HISTORY_DB]
    except Exception:  # noqa: BLE001
        return None


class BatchStore:
    """Mongo-backed purchase ledger. Writes raise when Mongo is unreachable.

    ``collection`` lets tests inject a fake; production resolves Mongo.
    """

    def __init__(self, collection=None) -> None:
        self._col = collection
        if self._col is not None:
            return
        db = _mongo_db()
        if db is None:
            return
        try:
            self._col = db[BATCH_COLLECTION]
            self._col.create_index("batch_id", unique=True)
        except Exception:  # noqa: BLE001
            self._col = None

    @property
    def available(self) -> bool:
        return self._col is not None

    def _need(self) -> None:
        if self._col is None:
            raise RuntimeError(
                "Mongo is unreachable — refusing to touch a purchase we cannot "
                "record (a missing ledger risks a double-buy).")

    def load_all(self) -> list[dict]:
        if self._col is None:
            return []
        return list(self._col.find({}, {"_id": 0}))

    def get(self, batch_id: str) -> dict | None:
        if self._col is None:
            return None
        return self._col.find_one({"batch_id": batch_id}, {"_id": 0})

    def held_domains(self) -> set[str]:
        """Domains already in a batch that is planned, in flight, or bought."""
        if self._col is None:
            return set()
        held: set[str] = set()
        for row in self._col.find({"status": {"$in": list(HOLDING_STATUSES)}},
                                  {"_id": 0, "domains": 1}):
            held.update(d.lower() for d in row.get("domains") or [])
        return held

    def save_planned(self, batches: list[Batch]) -> None:
        self._need()
        now = datetime.now(timezone.utc)
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {"batch_id": b.batch_id},
                {"$setOnInsert": {**b.to_dict(), "created_at": now}},
                upsert=True,
            )
            for b in batches
        ]
        if ops:
            self._col.bulk_write(ops, ordered=False)

    def claim(self, batch_id: str) -> dict | None:
        """Atomically move a buyable batch to ``in_progress``.

        Returns the batch as it was BEFORE the claim, or None if another run
        got there first (or the status is not buyable).
        """
        self._need()
        return self._col.find_one_and_update(
            {"batch_id": batch_id, "status": {"$in": list(BUYABLE_STATUSES)}},
            {"$set": {"status": "in_progress",
                      "claimed_at": datetime.now(timezone.utc).isoformat()}},
            projection={"_id": 0},
            return_document=ReturnDocument.BEFORE if ReturnDocument else False,
        )

    def release(self, batch_id: str, status: str, note: str) -> None:
        """Undo a claim when a pre-flight check refused (nothing was spent)."""
        self._need()
        self._col.update_one(
            {"batch_id": batch_id, "status": "in_progress"},
            {"$set": {"status": status, "last_refusal": note[:300],
                      "updated_at": datetime.now(timezone.utc).isoformat()}},
        )

    def mark(self, batch_id: str, status: str, result: dict | None = None) -> None:
        self._need()
        now = datetime.now(timezone.utc).isoformat()
        fields: dict = {"status": status, "result": result, "updated_at": now}
        if status == "purchased":
            fields["purchased_at"] = now
        self._col.update_one({"batch_id": batch_id}, {"$set": fields})


# ── staged + gated operations ────────────────────────────────────────────────

async def stage_batches(
    domains: list[str],
    *,
    client: str = "",
    registrars: tuple[str, ...] = DEFAULT_REGISTRARS,
    per_batch: int = DEFAULT_PER_BATCH,
    day_gap: int = DEFAULT_DAY_GAP,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    store: BatchStore | None = None,
) -> dict:
    """Read-only on Zapmail: availability + price, plan buyable names, record.

    Only names that are available, priced, under ``price_ceiling`` and not
    already held by another ledger batch are planned. Everything else is
    reported back so the caller can show it. Buys nothing.
    """
    store = store if store is not None else BatchStore()
    wanted = normalize_domains(domains)
    held = store.held_domains()
    already = [d for d in wanted if d in held]
    fresh = [d for d in wanted if d not in held]

    plan = await stage_purchase(fresh, client=client or None,
                                price_ceiling=price_ceiling)
    buyable = [i.domain for i in plan.ready]
    prices = {i.domain: i.price_usd for i in plan.ready}

    batches = plan_batches(buyable, registrars=registrars, per_batch=per_batch,
                           day_gap=day_gap, client=client)
    for b in batches:
        b.prices = {d: prices.get(d) for d in b.domains}

    ledger_ok = store.available
    if ledger_ok and batches:
        store.save_planned(batches)

    spend_account = resolve_account_for_client(client or None, strict=True)
    return {
        "client": client,
        "account": account_name_for_client(client or None),
        # The account a buy would actually bill. None = buying is refused
        # until the client map / API key is fixed.
        "spend_account": spend_account.name if spend_account else None,
        "batches": [b.to_dict() for b in batches],
        "total_usd": round(sum(b.estimated_usd for b in batches), 2),
        "unavailable": plan.unavailable,
        "unknown": plan.unknowns,
        "over_ceiling": plan.over_ceiling,
        "already_planned": already,
        "price_ceiling": price_ceiling,
        "spend_allowed": spend_allowed(),
        "ledger_ok": ledger_ok,
    }


async def execute_one(
    batch_id: str,
    *,
    approve: bool = False,
    client: str = "",
    today: date | None = None,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    store: BatchStore | None = None,
) -> dict:
    """Buy exactly one planned batch. See the module docstring for the gates.

    ``client`` is only a cross-check: the batch is always bought for the client
    recorded when it was planned, and a mismatch is refused.
    """
    if not approve or not spend_allowed():
        raise ZapmailSpendBlocked(
            "refusing to spend: execute_one needs approve=True AND "
            "ZAPMAIL_ALLOW_SPEND=true.")
    store = store if store is not None else BatchStore()
    if not store.available:
        raise ZapmailSpendBlocked(
            "refusing to spend: ledger unavailable, cannot guarantee this batch "
            "is not a duplicate.")
    batch = store.get(batch_id)
    if not batch:
        raise ZapmailSpendBlocked(f"unknown batch {batch_id!r} — plan it first.")

    status = batch.get("status")
    if status == "purchased":
        raise ZapmailSpendBlocked(
            f"batch {batch_id} is already purchased — refusing to buy twice.")
    if status not in BUYABLE_STATUSES:
        raise ZapmailSpendBlocked(
            f"batch {batch_id} is '{status}' — run reconcile first "
            f"(zapmail_buy.py --reconcile {batch_id}).")

    ledger_client = batch.get("client") or ""
    if client and _norm(client) != _norm(ledger_client):
        raise ZapmailSpendBlocked(
            f"batch {batch_id} was planned for client={ledger_client!r}, not "
            f"{client!r} — refusing to bill a different account.")

    earliest = batch.get("earliest_date") or ""
    today_s = (today or date.today()).isoformat()
    if earliest and today_s < earliest:
        raise ZapmailSpendBlocked(
            f"batch {batch_id} is scheduled for {earliest} (today {today_s}) — "
            "the stagger is the point; wait for its date.")

    domains = list(batch.get("domains") or [])
    if not domains:
        raise ZapmailSpendBlocked(f"batch {batch_id} has no domains.")

    before = store.claim(batch_id)
    if not before:
        raise ZapmailSpendBlocked(
            f"batch {batch_id} could not be claimed — another run is buying it "
            "or its status just changed. Check --list.")
    prior = before.get("status") or "planned"

    try:
        result = await execute_purchase(
            domains, approve=True, client=ledger_client or None,
            price_ceiling=price_ceiling,
            expected_prices=batch.get("prices") or None)
    except ZapmailSpendBlocked as exc:
        # Pre-flight refusal: nothing was sent to /buy.
        store.release(batch_id, prior, str(exc))
        raise
    except ZapmailHTTPError as exc:
        # Definitive rejection from Zapmail: not processed, safe to retry.
        store.mark(batch_id, "failed",
                   {"error": str(exc)[:300], "status_code": exc.status_code})
        raise
    except ZapmailOutcomeUnknown as exc:
        store.mark(batch_id, "unknown", {"error": str(exc)[:300]})
        raise
    except Exception as exc:  # noqa: BLE001 — anything else: can't tell
        store.mark(batch_id, "unknown", {"error": repr(exc)[:300]})
        raise
    store.mark(batch_id, "purchased", result if isinstance(result, dict) else None)
    return {"batch_id": batch_id, "domains": domains,
            "result": result, "status": "purchased"}


async def _owned_domains(z: ZapmailClient, domains: list[str]) -> set[str]:
    """Which of ``domains`` exist on the Zapmail account (exact match)."""
    owned: set[str] = set()
    for d in domains:
        resp = await z.list_domains(contains=d, page=1, limit=50)
        rows = ((resp or {}).get("data") or {}).get("domains") or []
        if any(str(r.get("domain", "")).strip().lower() == d.lower() for r in rows):
            owned.add(d.lower())
    return owned


async def reconcile_one(batch_id: str, *, store: BatchStore | None = None) -> dict:
    """Settle a batch whose buy outcome is unclear, by reading Zapmail.

    Read-only on Zapmail (``list_domains``); writes only the ledger:

      * every domain on the account  -> ``purchased``
      * none on the account          -> ``failed`` (safe to execute again)
      * some                         -> ``partial`` (blocked; a human decides)
    """
    store = store if store is not None else BatchStore()
    if not store.available:
        raise RuntimeError("ledger unavailable — cannot reconcile.")
    batch = store.get(batch_id)
    if not batch:
        raise RuntimeError(f"unknown batch {batch_id!r}.")
    status = batch.get("status")
    if status not in ("unknown", "in_progress", "failed"):
        return {"batch_id": batch_id, "status": status,
                "detail": "nothing to reconcile"}

    account = resolve_account_for_client(batch.get("client") or None, strict=True)
    if account is None:
        raise RuntimeError(
            f"client {batch.get('client')!r} has no configured Zapmail account.")
    domains = [d.lower() for d in batch.get("domains") or []]
    async with ZapmailClient(api_key=account.api_key,
                             workspace_key=account.workspace_key) as z:
        owned = await _owned_domains(z, domains)

    missing = [d for d in domains if d not in owned]
    new_status = ("purchased" if not missing
                  else "failed" if not owned else "partial")
    store.mark(batch_id, new_status, {
        "reconciled": True, "owned": sorted(owned), "missing": missing,
        "previous": batch.get("result"),
    })
    return {"batch_id": batch_id, "status": new_status,
            "owned": sorted(owned), "missing": missing}
