"""GoDaddy domain purchases: stage (free) → a human says "place" → buy.

The only path that spends money on GoDaddy. Same shape as
``scaledmail_orders``:

  stage_plan   READ only. Screens the names (no client brand, no cold-email
               cliches), checks availability + price, records the plan in
               Mongo (``godaddy_domain_plans``) as ``planned``.
  place_plan   SPEND. approve=True AND GODADDY_ALLOW_SPEND=true. Claims the
               plan atomically, then for each domain not bought yet:
               quote (locks the price; refused above the ceiling or with
               premium fees) → register with a deterministic Idempotency-Key
               → poll → point the nameservers if the plan names them.
                 4xx           → that domain failed (not charged)
                 timeout / 5xx → that domain unknown: placing the plan again
                                 replays the SAME key, which GoDaddy dedupes,
                                 so it can never be bought twice
                 COMPLETED     → bought; expiry recorded
  The plan ends ``placed`` (all bought), ``partial``, ``unknown`` or
  ``failed`` and can be placed again until every domain is bought.

Consent: ``agreedAt`` is the moment the human approved the purchase (the
``place --approve`` call), never a made-up time (ICANN rule).
"""

from __future__ import annotations

import os
import secrets
import time
from datetime import datetime, timezone

from smartlead.godaddy import (GoDaddyBlocked, GoDaddyError, GoDaddyHTTPError,
                               GoDaddyOutcomeUnknown, idempotency_key, quote_terms, spend_allowed)
from smartlead.zapmail_clients import CURRENT_CLIENTS, _squash, infer_client

COLLECTION = "godaddy_domain_plans"
DOMAIN_CEILING_CENTS = int(float(os.getenv("GODADDY_DOMAIN_PRICE_CEILING", "15")) * 100)
PLAN_CEILING_CENTS = int(float(os.getenv("GODADDY_PLAN_CEILING", "300")) * 100)
PLACEABLE = ("planned", "failed", "partial", "unknown")
POLL_SECONDS = 3
POLL_TRIES = 20


def _collection():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db[COLLECTION] if db is not None else None


class PlanStore:
    def __init__(self, collection=None) -> None:
        self.col = collection if collection is not None else _collection()
        if self.col is None:
            raise GoDaddyError("domain ledger unavailable (Mongo) — nothing can be staged or bought")

    def insert(self, doc: dict) -> None:
        self.col.update_one({"plan_id": doc["plan_id"]}, {"$setOnInsert": doc}, upsert=True)

    def get(self, plan_id: str) -> dict | None:
        return self.col.find_one({"plan_id": plan_id}, {"_id": 0})

    def all(self) -> list[dict]:
        return list(self.col.find({}, {"_id": 0}))

    def claim(self, plan_id: str, user: str) -> dict | None:
        return self.col.find_one_and_update(
            {"plan_id": plan_id, "status": {"$in": list(PLACEABLE)}},
            {"$set": {"status": "in_progress", "claimed_by": user,
                      "claimed_at": datetime.now(timezone.utc).isoformat()}})

    def save(self, plan_id: str, **fields) -> None:
        self.col.update_one({"plan_id": plan_id}, {"$set": fields})


def name_problems(domain: str) -> list[str]:
    """Why a name must not be bought as a sending domain (empty = fine)."""
    from smartlead.domain_naming import BANNED_SUBSTRINGS
    d = domain.strip().lower()
    sld, _, tld = d.partition(".")
    out = []
    if tld != "com":
        out.append("only .com")
    brand = infer_client(d, None)
    if brand or any(b in sld for b in ("melior", "precise", "bettr", "belardi")):
        out.append("contains a client brand (blacklists flag brand lookalikes)")
    out += [f"cliche/spam word '{w}'" for w in sorted(BANNED_SUBSTRINGS) if w in sld]
    return out


def client_key(client: str) -> str:
    want = _squash(client)
    for key in CURRENT_CLIENTS:
        if _squash(key) == want:
            return key
    raise GoDaddyError(f"unknown client {client!r} (current: {', '.join(CURRENT_CLIENTS)})")


def stage_plan(gd, *, client: str, domains: list[str], nameservers: list[str] | None = None,
               user: str = "", store: PlanStore | None = None) -> dict:
    """READ only. Records the plan when every name is clean and buyable."""
    client = client_key(client)
    names = sorted({d.strip().lower() for d in domains if d.strip()})
    if not names:
        raise GoDaddyError("no domains")
    problems = {d: p for d in names if (p := name_problems(d))}
    checks = {c["domain"]: c for c in gd.check(names)}
    taken = [d for d in names if not (checks.get(d) or {}).get("available")]
    pricey = [d for d in names if (checks.get(d) or {}).get("price_cents") and
              checks[d]["price_cents"] > DOMAIN_CEILING_CENTS]
    premium = [d for d in names if (checks.get(d) or {}).get("fees")]
    total = sum((checks.get(d) or {}).get("price_cents") or 0 for d in names)
    result = {"client": client, "domains": names, "problems": problems, "taken": taken,
              "over_ceiling": pricey, "premium": premium, "total_cents": total,
              "prices": {d: (checks.get(d) or {}).get("price_cents") for d in names},
              "renewal": {d: (checks.get(d) or {}).get("renewal_cents") for d in names},
              "nameservers": nameservers or [], "staged": False}
    if problems or taken or pricey or premium or total > PLAN_CEILING_CENTS:
        if total > PLAN_CEILING_CENTS:
            result["error"] = f"plan total ${total / 100:.2f} is over GODADDY_PLAN_CEILING"
        return result
    store = store or PlanStore()
    plan_id = secrets.token_hex(3)
    store.insert({"plan_id": plan_id, "client": client, "domains": names,
                  "nameservers": nameservers or [], "period": 1, "status": "planned",
                  "indicative_cents": result["prices"], "renewal_cents": result["renewal"],
                  "results": {}, "staged_by": user,
                  "staged_at": datetime.now(timezone.utc).isoformat()})
    result.update(staged=True, plan_id=plan_id)
    return result


def _owned(gd, domain: str) -> dict | None:
    try:
        d = gd.domain(domain)
    except GoDaddyHTTPError as exc:
        if exc.status_code == 404:
            return None
        raise
    return d if str((d or {}).get("domain", "")).lower() == domain else None


def _poll(gd, reg: dict) -> dict:
    rid = reg.get("registrationId") or reg.get("id")
    status = str(reg.get("status") or "")
    for _ in range(POLL_TRIES):
        if status in ("COMPLETED", "FAILED") or not rid:
            break
        time.sleep(POLL_SECONDS)
        reg = gd.registration(rid) or reg
        status = str(reg.get("status") or "")
    return reg


def _buy_one(gd, plan: dict, domain: str, agreed_at: str, prev: dict) -> dict:
    """One domain. Returns its result row; never raises for API answers."""
    attempt = int(prev.get("attempt") or 0)
    if prev.get("status") in ("unknown", "failed"):
        # The last try may have bought it after all: read our own account first.
        try:
            owned = _owned(gd, domain)
        except GoDaddyError as exc:   # cannot tell: do not risk a second purchase
            return {**prev, "status": "unknown", "error": f"could not check the account: {exc}"}
        if owned:
            return {**prev, "status": "bought", "expires_at": owned.get("expiresAt"),
                    "note": "found in the account on retry"}
    if prev.get("status") == "failed":
        attempt += 1                # a definitive failure may be retried with a new key
    key = idempotency_key(plan["plan_id"], domain, str(attempt))
    row = {"attempt": attempt, "key": key}
    try:
        quote = gd.quote(domain, plan.get("period", 1))
        price, fees = quote_terms(quote)
        row["price_cents"] = price
        if quote.get("available") is False:
            return {**row, "status": "failed", "error": "no longer available"}
        if fees:
            return {**row, "status": "failed", "error": "premium fees — not bought"}
        if not price or price > DOMAIN_CEILING_CENTS:
            return {**row, "status": "failed", "error": f"quoted {price} cents, outside 1..ceiling"}
        if (quote.get("currencyCode") or (quote.get("price") or {}).get("currencyCode") or "USD") != "USD":
            return {**row, "status": "failed", "error": "quote not in USD"}
        reg = gd.register(domain, quote, agreed_at=agreed_at, key=key,
                          period=plan.get("period", 1), approve=True)
        reg = _poll(gd, reg)
    except GoDaddyHTTPError as exc:
        return {**row, "status": "failed", "error": str(exc)}
    except GoDaddyOutcomeUnknown as exc:
        return {**row, "status": "unknown", "error": str(exc)}
    status = str(reg.get("status") or "")
    row["registration_id"] = reg.get("registrationId") or reg.get("id")
    if status == "COMPLETED":
        row.update(status="bought", expires_at=reg.get("expiresAt"))
    elif status == "FAILED":
        row.update(status="failed", error=str(reg.get("error") or reg)[:300])
    else:
        # Answer shape not what the docs say, or still running: the account is the truth.
        try:
            owned = _owned(gd, domain)
        except GoDaddyError:
            owned = None
        if owned:
            row.update(status="bought", expires_at=owned.get("expiresAt"),
                       note=f"registration said {status or 'nothing'}; found in the account")
        else:
            row.update(status="unknown", error=f"still {status or 'pending'} after polling",
                       response=str(reg)[:500])
    return row


def _set_ns(gd, plan: dict, domain: str, prev: dict) -> dict:
    ns = plan.get("nameservers") or []
    if not ns or prev.get("ns") == "done":
        return {}
    try:
        gd.set_nameservers(domain, ns, key=idempotency_key(plan["plan_id"], domain, "ns"), approve=True)
        return {"ns": "done"}
    except GoDaddyError as exc:
        return {"ns": "failed", "ns_error": str(exc)[:300]}


def place_plan(gd, plan_id: str, *, approve: bool = False, user: str = "",
               store: PlanStore | None = None) -> dict:
    """SPEND. Buys every not-yet-bought domain of the plan."""
    if not approve:
        raise GoDaddyBlocked("buying domains charges the account: pass approve=True after a human confirmed it")
    if not spend_allowed():
        raise GoDaddyBlocked("GODADDY_ALLOW_SPEND is not 'true' — buying is switched off")
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan:
        raise GoDaddyError(f"no plan {plan_id}")
    if plan["status"] not in PLACEABLE:
        raise GoDaddyBlocked(f"plan {plan_id} is {plan['status']}")
    if not store.claim(plan_id, user):
        raise GoDaddyBlocked(f"plan {plan_id} was claimed by someone else just now")
    agreed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    results = dict(plan.get("results") or {})
    try:
        for domain in plan["domains"]:
            prev = results.get(domain) or {}
            if prev.get("status") != "bought":
                prev = {**prev, **_buy_one(gd, plan, domain, agreed_at, prev)}
            if prev.get("status") == "bought":
                prev.update(_set_ns(gd, plan, domain, prev))
            results[domain] = prev
            store.save(plan_id, results=results)        # after every domain: a crash loses nothing
    finally:
        states = {r.get("status") for r in results.values()}
        bought = sum(1 for r in results.values() if r.get("status") == "bought")
        if bought == len(plan["domains"]):
            final = "placed"
        elif bought:
            final = "partial"
        elif "unknown" in states or len(results) < len(plan["domains"]):
            final = "unknown"
        else:
            final = "failed"
        store.save(plan_id, status=final, results=results, placed_by=user,
                   settled_at=datetime.now(timezone.utc).isoformat())
    return {"plan_id": plan_id, "status": final, "results": results}


def drop_plan(plan_id: str, *, user: str = "", store: PlanStore | None = None) -> dict:
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan:
        raise GoDaddyError(f"no plan {plan_id}")
    if any(r.get("status") in ("bought", "unknown") for r in (plan.get("results") or {}).values()):
        raise GoDaddyBlocked(f"plan {plan_id} has bought or unknown domains — cannot drop")
    store.save(plan_id, status="dropped", dropped_by=user)
    return {"plan_id": plan_id, "status": "dropped"}


def client_by_domain(store: PlanStore | None = None) -> dict[str, str]:
    """Domain → client for everything the bot bought (the tracker sync uses it)."""
    try:
        store = store or PlanStore()
    except GoDaddyError:
        return {}
    out = {}
    for p in store.all():
        for d, r in (p.get("results") or {}).items():
            if r.get("status") == "bought":
                out[d] = p["client"]
    return out
