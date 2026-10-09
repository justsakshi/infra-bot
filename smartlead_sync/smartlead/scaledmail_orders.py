"""ScaledMail orders: stage (free) → approve → place once (charges the card).

The only path that spends money on ScaledMail. Same shape as the Zapmail
domain ledger (``domain_batch``):

  stage_order   READ only. Checks every domain is available, prices it,
                builds the exact ``/create-custom-order`` payload and records
                it in Mongo (``scaledmail_order_plans``) as ``planned``.
  place_order   SPEND. approve=True AND SCALEDMAIL_ALLOW_SPEND=true. Claims
                the plan atomically (planned/failed → in_progress, so two
                clicks cannot order twice), re-checks availability, sends the
                payload once — never retried.
                  4xx            → failed    (not processed; can be retried)
                  timeout / 5xx  → unknown   (may have charged: reconcile first)
                  200            → placed
  reconcile     READ. Looks for the plan's tag in ScaledMail's orders and
                settles ``unknown``/``in_progress`` to placed or failed.

The order tag is ``<client>-<plan id>`` (≤20 chars), so ScaledMail's own
order list says which client and plan every order came from, and the fleet
view assigns its domains to the client without a guess.
"""

from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone

from smartlead.scaledmail import (MAILBOXES_PER_DOMAIN, MAX_DOMAINS_PER_ORDER, ScaledMailBlocked,
                                  ScaledMailError, ScaledMailHTTPError, ScaledMailOutcomeUnknown,
                                  spend_allowed)
from smartlead.zapmail_clients import CURRENT_CLIENTS, _squash

COLLECTION = "scaledmail_order_plans"
PRICE = {"google": 3.50, "outlook": 50.0, "smtp": 3.75}   # live 2026-10-08
PRICE_BASIS = {"google": "mailbox", "outlook": "domain", "smtp": "domain"}
DOMAIN_PRICE_CEILING = float(os.getenv("SCALEDMAIL_DOMAIN_PRICE_CEILING", "25"))
MONTHLY_CEILING = float(os.getenv("SCALEDMAIL_ORDER_MONTHLY_CEILING", "500"))


def _collection():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db[COLLECTION] if db is not None else None


def monthly_cost(provider: str, domains: int, per_domain: int) -> float:
    if PRICE_BASIS[provider] == "mailbox":
        return round(PRICE[provider] * domains * per_domain, 2)
    return round(PRICE[provider] * domains, 2)


def make_tag(client: str, plan_id: str) -> str:
    return f"{_squash(client)[:13]}-{plan_id}"[:20]


def build_payload(provider: str, domains: list[str], senders: list[tuple[str, str]],
                  *, per_domain: int | None = None, redirect: str = "") -> dict:
    """``providers`` body for /create-custom-order. Pure."""
    if provider not in PRICE:
        raise ScaledMailError(f"provider must be one of {sorted(PRICE)}")
    allowed = MAILBOXES_PER_DOMAIN[provider]
    per_domain = per_domain or allowed[0]
    if per_domain not in allowed:
        raise ScaledMailError(f"{provider} takes {allowed} mailboxes per domain, not {per_domain}")
    if not domains:
        raise ScaledMailError("no domains")
    if len(domains) > MAX_DOMAINS_PER_ORDER[provider]:
        raise ScaledMailError(f"{provider} takes at most {MAX_DOMAINS_PER_ORDER[provider]} domains per order")
    if not senders:
        raise ScaledMailError("give at least one sender name (real people, e.g. 'Jane Doe')")
    rows = []
    for i, dom in enumerate(domains):
        first, last = senders[i % len(senders)]
        row = {"domain": dom, "first_name": first[:50], "last_name": last[:50]}
        if redirect:
            row["redirect"] = redirect
        rows.append(row)
    block: dict = {"domains": rows}
    if provider == "google":
        block["mailboxes_per_domain"] = per_domain
    return {provider: block}


def parse_senders(text: str) -> list[tuple[str, str]]:
    out = []
    for part in str(text or "").split(","):
        bits = part.split()
        if len(bits) >= 2:
            out.append((bits[0], " ".join(bits[1:])))
    return out


class PlanStore:
    def __init__(self, collection=None) -> None:
        self.col = collection if collection is not None else _collection()
        if self.col is None:
            raise ScaledMailError("order ledger unavailable (Mongo) — nothing can be staged or placed")

    def insert(self, doc: dict) -> None:
        self.col.update_one({"plan_id": doc["plan_id"]}, {"$setOnInsert": doc}, upsert=True)

    def get(self, plan_id: str) -> dict | None:
        return self.col.find_one({"plan_id": plan_id}, {"_id": 0})

    def all(self) -> list[dict]:
        return list(self.col.find({}, {"_id": 0}))

    def claim(self, plan_id: str, user: str) -> dict | None:
        """planned/failed → in_progress, atomically. None when not claimable."""
        return self.col.find_one_and_update(
            {"plan_id": plan_id, "status": {"$in": ["planned", "failed"]}},
            {"$set": {"status": "in_progress", "claimed_by": user,
                      "claimed_at": datetime.now(timezone.utc).isoformat()}})

    def settle(self, plan_id: str, status: str, **fields) -> None:
        self.col.update_one({"plan_id": plan_id},
                            {"$set": {"status": status, **fields,
                                      "settled_at": datetime.now(timezone.utc).isoformat()}})


def _check_available(sm, domains: list[str]) -> dict:
    """{available: {name: price}, taken: [], over_ceiling: [], blacklisted: []}."""
    out: dict = {"available": {}, "taken": [], "over_ceiling": [], "blacklisted": []}
    for i in range(0, len(domains), 30):
        for r in sm.search_domains(domains[i:i + 30]) or []:
            name = str(r.get("domain", "")).lower()
            if r.get("status") != "available":
                out["taken"].append(name)
            elif r.get("blacklisted"):
                out["blacklisted"].append(name)
            elif r.get("price") is None or float(r["price"]) > DOMAIN_PRICE_CEILING:
                out["over_ceiling"].append(name)
            else:
                out["available"][name] = float(r["price"])
    return out


def not_in_our_godaddy(domains: list[str]) -> list[str]:
    """Domains we do NOT own on GoDaddy (READ). Mailboxes on our own domains
    are only ordered for domains we really hold."""
    from smartlead.godaddy import GoDaddyClient, GoDaddyHTTPError
    missing = []
    with GoDaddyClient() as gd:
        for d in domains:
            try:
                if str((gd.domain(d) or {}).get("domain", "")).lower() != d:
                    missing.append(d)
            except GoDaddyHTTPError:
                missing.append(d)
    return missing


def stage_order(sm, *, client: str, provider: str, domains: list[str], senders_text: str,
                per_domain: int | None = None, redirect: str = "", user: str = "",
                own_domains: bool = False, ownership=None,
                store: PlanStore | None = None) -> dict:
    """READ only: price + record a plan. Nothing is ordered.

    ``own_domains``: the domains are already ours on GoDaddy (bought by the bot).
    ScaledMail then only sells the mailboxes (``provider=other``); no registrar
    login is sent (ScaledMail support, 2026-10-09: skip it, their team sends
    custom nameservers after the order)."""
    if client not in CURRENT_CLIENTS:
        raise ScaledMailError(f"{client!r} is not a current client ({', '.join(CURRENT_CLIENTS)})")
    domains = sorted({d.strip().lower() for d in domains if d.strip()})
    senders = parse_senders(senders_text)
    payload = build_payload(provider, domains, senders, per_domain=per_domain, redirect=redirect)
    per = payload[provider].get("mailboxes_per_domain") or MAILBOXES_PER_DOMAIN[provider][0]
    if own_domains:
        missing = (ownership or not_in_our_godaddy)(domains)
        check = {"available": {}, "taken": [], "blacklisted": [], "over_ceiling": [], "not_ours": missing}
    else:
        check = _check_available(sm, domains)
        check["not_ours"] = []
    problems = check["taken"] + check["blacklisted"] + check["over_ceiling"] + check["not_ours"]
    monthly = monthly_cost(provider, len(domains), per)
    result = {"client": client, "provider": provider, "domains": domains, "mailboxes_per_domain": per,
              "mailboxes": len(domains) * per, "monthly_usd": monthly,
              "domains_usd": round(sum(check["available"].values()), 2),
              "taken": check["taken"], "blacklisted": check["blacklisted"],
              "over_ceiling": check["over_ceiling"], "not_ours": check["not_ours"],
              "senders": [" ".join(s) for s in senders], "source": "other" if own_domains else "buy"}
    if problems:
        result["staged"] = False
        result["why"] = ("some domains are not in our GoDaddy account" if check["not_ours"]
                         else "some domains cannot be bought — drop them and stage again")
        return result
    if monthly > MONTHLY_CEILING:
        result["staged"] = False
        result["why"] = f"${monthly:.2f}/month is over the ${MONTHLY_CEILING:.0f} per-order ceiling"
        return result
    store = store or PlanStore()
    plan_id = secrets.token_hex(3)
    doc = {"plan_id": plan_id, "status": "planned", "client": client, "provider": provider,
           "tag": make_tag(client, plan_id), "source": "other" if own_domains else "buy", "payload": payload,
           "domains": domains, "mailboxes": result["mailboxes"], "monthly_usd": monthly,
           "domain_prices": check["available"], "domains_usd": result["domains_usd"],
           "staged_by": user, "staged_at": datetime.now(timezone.utc).isoformat()}
    store.insert(doc)
    result.update({"staged": True, "plan_id": plan_id, "tag": doc["tag"]})
    return result


def place_order(sm, plan_id: str, *, approve: bool = False, user: str = "",
                ownership=None, store: PlanStore | None = None) -> dict:
    """SPEND. The only call that orders on ScaledMail."""
    if not approve:
        raise ScaledMailBlocked("placing an order charges the card: pass approve=True after a human confirmed it")
    if not spend_allowed():
        raise ScaledMailBlocked("SCALEDMAIL_ALLOW_SPEND is not 'true' — ordering is switched off")
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan:
        raise ScaledMailError(f"no plan {plan_id}")
    if plan["status"] in ("placed", "in_progress", "unknown", "dropped"):
        raise ScaledMailBlocked(f"plan {plan_id} is {plan['status']} — "
                                + {"placed": "already ordered", "dropped": "it was dropped; stage a new one"}
                                .get(plan["status"], "reconcile it first"))
    if not store.claim(plan_id, user):
        raise ScaledMailBlocked(f"plan {plan_id} was claimed by someone else just now")
    try:
        if plan.get("source") == "other":
            bad = (ownership or not_in_our_godaddy)(plan["domains"])
        else:
            check = _check_available(sm, plan["domains"])
            bad = check["taken"] + check["blacklisted"] + check["over_ceiling"]
        if bad:
            store.settle(plan_id, "failed", error=f"no longer orderable: {', '.join(bad)}")
            return {"plan_id": plan_id, "status": "failed", "error": f"no longer orderable: {', '.join(bad)}"}
        res = sm.create_custom_order(plan["payload"], source=plan.get("source", "buy"),
                                     tag=plan["tag"], approve=True)
    except ScaledMailHTTPError as exc:
        store.settle(plan_id, "failed", error=str(exc))
        return {"plan_id": plan_id, "status": "failed", "error": str(exc)}
    except ScaledMailOutcomeUnknown as exc:
        store.settle(plan_id, "unknown", error=str(exc))
        return {"plan_id": plan_id, "status": "unknown", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - never leave a plan stuck in_progress
        store.settle(plan_id, "unknown", error=f"{type(exc).__name__}: {exc}")
        raise
    store.settle(plan_id, "placed", response=res, placed_by=user)
    return {"plan_id": plan_id, "status": "placed", "response": res, "tag": plan["tag"]}


def reconcile(sm, plan_id: str, *, store: PlanStore | None = None) -> dict:
    """READ ScaledMail's orders; settle an unknown/in_progress plan by its tag."""
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan:
        raise ScaledMailError(f"no plan {plan_id}")
    if plan["status"] not in ("unknown", "in_progress"):
        return {"plan_id": plan_id, "status": plan["status"], "note": "nothing to reconcile"}
    for o in sm.orders():
        detail = sm.order(o["id"])
        if str(detail.get("tag") or "") == plan["tag"]:
            store.settle(plan_id, "placed", order_id=o["id"], reconciled=True)
            return {"plan_id": plan_id, "status": "placed", "order_id": o["id"]}
    # Not found is not proof: ScaledMail may still be creating it. A human
    # checks the ScaledMail UI and marks it failed (mark_failed) to unblock.
    return {"plan_id": plan_id, "status": plan["status"],
            "note": "no order with tag " + plan["tag"] + " yet — check the ScaledMail UI, "
                    "then mark it failed only if it is really not there"}


def drop_plan(plan_id: str, *, user: str = "", store: PlanStore | None = None) -> dict:
    """Drop a staged plan that will not be ordered (only planned / failed)."""
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan or plan["status"] not in ("planned", "failed"):
        raise ScaledMailError(f"plan {plan_id} is not planned/failed - nothing to drop")
    store.settle(plan_id, "dropped", error=f"dropped by {user or 'operator'}")
    return {"plan_id": plan_id, "status": "dropped"}


def mark_failed(plan_id: str, *, user: str = "", store: PlanStore | None = None) -> dict:
    """Human override after checking ScaledMail: an unknown plan was not ordered."""
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan or plan["status"] not in ("unknown", "in_progress"):
        raise ScaledMailError(f"plan {plan_id} is not unknown/in_progress")
    store.settle(plan_id, "failed", error=f"marked not-ordered by {user or 'operator'}")
    return {"plan_id": plan_id, "status": "failed"}
