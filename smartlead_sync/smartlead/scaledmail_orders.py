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
from datetime import date, datetime, timedelta, timezone

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
        now = datetime.now(timezone.utc).isoformat()
        return self.col.find_one_and_update(
            {"plan_id": plan_id, "status": {"$in": ["planned", "failed"]}},
            {"$set": {"status": "in_progress", "claimed_by": user, "claimed_at": now},
             "$push": {"history": {"at": now, "status": "in_progress", "by": user}}})

    def settle(self, plan_id: str, status: str, **fields) -> None:
        """Set the current state AND append it to ``history``: the top-level
        fields show the latest answer, ``history`` keeps every request and
        answer (money trail; later steps never erase earlier ones)."""
        now = datetime.now(timezone.utc).isoformat()
        self.col.update_one({"plan_id": plan_id},
                            {"$set": {"status": status, **fields, "settled_at": now},
                             "$push": {"history": {"at": now, "status": status, **fields}}})

    def log(self, plan_id: str, event: str, *, set_fields: dict | None = None, **fields) -> None:
        """Append an event (verified, charge_confirmed, setup_live) WITHOUT
        changing the plan's status. ``set_fields`` also go on the top level."""
        now = datetime.now(timezone.utc).isoformat()
        update: dict = {"$push": {"history": {"at": now, **fields, "status": event}}}
        if set_fields:
            update["$set"] = dict(set_fields)
        self.col.update_one({"plan_id": plan_id}, update)


def _check_available(sm, domains: list[str]) -> dict:
    """{available: {name: price}, taken: [], over_ceiling: [], blacklisted: []}."""
    out: dict = {"available": {}, "taken": [], "over_ceiling": [], "blacklisted": [], "refused_raw": []}
    for i in range(0, len(domains), 30):
        for r in sm.search_domains(domains[i:i + 30]) or []:
            name = str(r.get("domain", "")).lower()
            if r.get("status") != "available":
                # Live 2026-10-12: a search said all 7 names were not available,
                # then "available" seconds later. Keep ScaledMail's own words.
                out["taken"].append(name)
                out["refused_raw"].append(r)
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
        result["refused_raw"] = check.get("refused_raw", [])
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


def payment_pending(error: str) -> bool:
    """ScaledMail made the subscription but the card was not charged (needs approval)."""
    e = str(error or "").lower()
    return "subscription created" in e or "unable to charge" in e


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
    if plan["status"] in ("placed", "in_progress", "unknown", "dropped", "payment_pending"):
        raise ScaledMailBlocked(f"plan {plan_id} is {plan['status']} — "
                                + {"placed": "already ordered", "dropped": "it was dropped; stage a new one"}
                                .get(plan["status"], "reconcile it first"))
    if not store.claim(plan_id, user):
        raise ScaledMailBlocked(f"plan {plan_id} was claimed by someone else just now")
    # Exactly what create_custom_order sends, kept with every answer.
    request = {"method": "POST", "path": "/create-custom-order",
               "params": {"provider": plan.get("source", "buy")},
               "body": {"providers": plan["payload"], "tag": plan["tag"][:20]}}
    try:
        if plan.get("source") == "other":
            bad = (ownership or not_in_our_godaddy)(plan["domains"])
        else:
            check = _check_available(sm, plan["domains"])
            bad = check["taken"] + check["blacklisted"] + check["over_ceiling"]
        if bad:
            store.settle(plan_id, "failed", error=f"no longer orderable: {', '.join(bad)}",
                         refused_raw=check.get("refused_raw", []) if plan.get("source") != "other" else [])
            return {"plan_id": plan_id, "status": "failed", "error": f"no longer orderable: {', '.join(bad)}"}
        res = sm.create_custom_order(plan["payload"], source=plan.get("source", "buy"),
                                     tag=plan["tag"], approve=True)
    except ScaledMailHTTPError as exc:
        if payment_pending(str(exc)):
            # Live 2026-10-09: "400 Subscription created but unable to charge".
            # The subscription EXISTS; placing again would create a second one.
            note = ("ScaledMail created the subscription but could not charge the card (it needs "
                    "approval). Pay or confirm it in the ScaledMail web app (Billing), then "
                    "run reconcile; or cancel it there. Never place this plan again.")
            store.settle(plan_id, "payment_pending", error=str(exc), note=note, request=request,
                         raw_response=getattr(exc, "body", ""))
            return {"plan_id": plan_id, "status": "payment_pending", "error": str(exc), "next": note}
        store.settle(plan_id, "failed", error=str(exc), request=request,
                     raw_response=getattr(exc, "body", ""))
        return {"plan_id": plan_id, "status": "failed", "error": str(exc)}
    except ScaledMailOutcomeUnknown as exc:
        store.settle(plan_id, "unknown", error=str(exc), request=request)
        return {"plan_id": plan_id, "status": "unknown", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - never leave a plan stuck in_progress
        store.settle(plan_id, "unknown", error=f"{type(exc).__name__}: {exc}", request=request)
        raise
    order_id = (res or {}).get("order_id") if isinstance(res, dict) else None
    store.settle(plan_id, "placed", response=res, placed_by=user, request=request,
                 **({"order_id": order_id} if order_id else {}))
    verify = verify_order(sm, plan, order_id)
    store.log(plan_id, "verified", set_fields={"verified": verify["ok"]}, **verify)
    return {"plan_id": plan_id, "status": "placed", "response": res, "tag": plan["tag"], "verify": verify}


def verify_order(sm, plan: dict, order_id: str | None) -> dict:
    """READ the order back after a spend: tag, domains and monthly amount must
    match the plan. Never raises (the money is already spent); problems are
    reported for a human to check in the ScaledMail web app."""
    out: dict = {"ok": False, "order_id": order_id, "problems": []}
    if not order_id:
        out["problems"].append("ScaledMail's answer had no order_id")
        return out
    try:
        o = sm.order(order_id) or {}
    except Exception as exc:  # noqa: BLE001
        out["problems"].append(f"could not read the order back: {exc}")
        return out
    if not o:
        out["problems"].append(f"order {order_id} not found")
        return out
    names = {str(d.get("name", "")).lower() for p in o.get("packages") or [] for d in p.get("domains") or []}
    missing = sorted(set(plan["domains"]) - names)
    out.update(order_status=o.get("status"), amount=o.get("amount"), tag=o.get("tag"),
               description=o.get("description"), domains=sorted(names))
    if str(o.get("tag") or "") != plan["tag"]:
        out["problems"].append(f"tag is {o.get('tag')!r}, expected {plan['tag']!r}")
    if missing:
        out["problems"].append("domains missing from the order: " + ", ".join(missing))
    try:
        if abs(float(o.get("amount")) - float(plan["monthly_usd"])) > 0.01:
            out["problems"].append(f"amount is ${o.get('amount')}/mo, plan said ${plan['monthly_usd']}")
    except (TypeError, ValueError, KeyError):
        out["problems"].append(f"amount unreadable: {o.get('amount')!r}")
    out["ok"] = not out["problems"]
    return out


def reconcile(sm, plan_id: str, *, store: PlanStore | None = None) -> dict:
    """READ ScaledMail's orders; settle an unknown/in_progress plan by its tag."""
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan:
        raise ScaledMailError(f"no plan {plan_id}")
    if plan["status"] not in ("unknown", "in_progress", "payment_pending"):
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


def mark_failed(plan_id: str, *, user: str = "", note: str = "",
                store: PlanStore | None = None) -> dict:
    """Human override after checking ScaledMail: an unknown plan was not ordered.
    ``note`` says why (e.g. what ScaledMail refunded), kept in ``history``."""
    store = store or PlanStore()
    plan = store.get(plan_id)
    if not plan or plan["status"] not in ("unknown", "in_progress", "payment_pending"):
        raise ScaledMailError(f"plan {plan_id} is not unknown/in_progress/payment_pending")
    store.settle(plan_id, "failed", error=f"marked not-ordered by {user or 'operator'}",
                 by=user or "operator", note=note)
    return {"plan_id": plan_id, "status": "failed"}


# ── setup watcher (READ; the 9:40 digest) ───────────────────────────────────
SETUP_DAYS = 3        # ScaledMail support, 2026-10-12: 2-3 business days, Smartlead upload after
LIVE_SHOWN_DAYS = 2   # keep the "ask support to upload" line up this many days


def business_days(start: date, end: date) -> int:
    """Weekdays after ``start`` up to and including ``end``."""
    n, d = 0, start
    while d < end:
        d += timedelta(days=1)
        n += d.weekday() < 5
    return n


def _event_day(plan: dict, status: str) -> date | None:
    for e in plan.get("history") or []:
        if e.get("status") == status and e.get("at"):
            return date.fromisoformat(str(e["at"])[:10])
    return None


def setup_watch(snap: dict, plans: list[dict], *, today: date) -> dict:
    """Pure. Where each placed order is in ScaledMail's setup.

    lines       Slack lines: setting up / late / LIVE (+ the message for support)
    newly_live  plan ids live for the first time: log ``setup_live`` on them
    covered     domains these lines speak for (the digest skips them elsewhere)"""
    lines, newly, covered = [], [], set()
    for plan in sorted(plans, key=lambda p: p.get("plan_id", "")):
        oid = plan.get("order_id") or ((plan.get("response") or {}).get("order_id")
                                       if isinstance(plan.get("response"), dict) else None)
        placed = _event_day(plan, "placed")
        if plan.get("status") != "placed" or not oid or not placed:
            continue
        live_day = _event_day(plan, "setup_live")
        if live_day and (today - live_day).days > LIVE_SHOWN_DAYS:
            continue
        rows = [d for d in snap.get("domains") or [] if d.get("order_id") == oid]
        names = sorted({d["domain"] for d in rows} | set(plan.get("domains") or []))
        covered.update(names)
        listed = {d["domain"] for d in rows}
        live = bool(rows) and set(plan.get("domains") or []) <= listed and all(d["status"] == "Active" for d in rows)
        if live:
            if not live_day:
                newly.append(plan["plan_id"])
            n = sum(int(d.get("mailboxes") or 0) for d in rows)
            lines.append(f"LIVE: {', '.join(names)} ({n} inboxes, order {oid}). Send ScaledMail support: "
                         f"\"Order {oid} is active. Could you upload its {n} inboxes to our Smartlead "
                         f"account? Thanks.\"")
            continue
        days = business_days(placed, today)
        waiting = ", ".join(f"{d['domain']} ({d['status'] or 'not listed'})" for d in rows) or ", ".join(names)
        if days > SETUP_DAYS:
            lines.append(f"Late, business day {days} (support says 2-3), order {oid}: {waiting}. Ask support.")
        else:
            lines.append(f"Setting up, business day {days} of 2-3, order {oid}: {len(names)} domain(s): "
                         + ", ".join(names))
    return {"lines": lines, "newly_live": newly, "covered": covered}
