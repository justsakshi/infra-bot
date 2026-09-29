"""Keep the /infra asset tracker in step with Zapmail — so nobody types it in.

The tracker (Mongo ``assets``, what ``/infra add`` writes and what expiry
reminders run on) was filled by hand: 180 domains + 364 inboxes on
2026-09-29, and 167 Zapmail domains were missing from it. This module makes
Zapmail the source for everything Zapmail knows; ScaledMail and other
providers stay manual.

Rules (conservative on purpose — the tracker is the team's):

* **Expiry dates** always follow Zapmail (it is the registrar/billing truth;
  a renewal done in Zapmail updates the tracker by itself).
* **Status** is only ever DOWNGRADED automatically (Active → Inactive when
  Zapmail says lapsed / not active). Nothing is flipped back to Active: a row
  the team marked Inactive stays Inactive.
* Missing fields (workspace, purchase date, domain of an inbox) are filled;
  fields the team set are never overwritten (client, provider, owner,
  channel, costs, notes).
* **New rows** are added only for current clients (``zapmail_clients``), only
  when live, and with no owner/channel — so no reminder is sent for them
  until someone assigns one. Past-client and unbranded domains are reported,
  never added.
* Nothing is deleted.

``plan_sync`` is pure (unit-tested); ``apply`` writes. Callers default to a
dry run (``zapmail_asset_sync.py``; the cron only applies when
``ZAPMAIL_ASSET_SYNC_ENABLED=true``).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from smartlead.zapmail_clients import TRACKER_SPELLING, infer_client

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

ASSETS_COLLECTION = os.getenv("ASSETS_COLLECTION", "assets")
_ACTIVE = {"ACTIVE", "CONNECTED"}


def _db():
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        from smartlead.config import HEALTH_HISTORY_DB
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        db = client.get_default_database()
        return db if db is not None else client[HEALTH_HISTORY_DB]
    except Exception:  # noqa: BLE001
        return None


def read_tracker() -> dict[str, dict]:
    """``{name (lowercase): asset doc}`` for every tracker row; {} if unreachable."""
    db = _db()
    if db is None:
        return {}
    out: dict[str, dict] = {}
    for doc in db[ASSETS_COLLECTION].find({}, {"_id": 0, "remindersSent": 0}):
        name = str(doc.get("name") or "").strip().lower()
        if name:
            out[name] = doc
    return out


def _day(value) -> str:
    if value is None or value == "":
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def _dt(day: str) -> datetime | None:
    try:
        return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _days_apart(a: str, b: str) -> int:
    try:
        return abs((datetime.strptime(a, "%Y-%m-%d")
                    - datetime.strptime(b, "%Y-%m-%d")).days)
    except ValueError:
        return 999


def _workspace(provider: str) -> str:
    return "Outlook" if str(provider).upper() == "MICROSOFT" else "Google"


def _zap_status(status, expire_on: str, today: str) -> str:
    if expire_on and expire_on < today:
        return "Inactive"
    return "Active" if str(status or "").upper() in _ACTIVE else "Inactive"


def _changes(existing: dict, *, expire_on: str, zap_status: str,
             workspace: str, first_day: str, domain: str | None = None) -> dict:
    """Fields to $set on an existing row under the rules above."""
    s: dict = {}
    current = _day(existing.get("expiryDate"))
    # <=1 day apart is a UTC-vs-IST rendering difference, not a new date.
    if expire_on and (not current or _days_apart(current, expire_on) > 1):
        s["expiryDate"] = _dt(expire_on)
    if zap_status == "Inactive" and str(existing.get("status")) == "Active":
        s["status"] = "Inactive"
    if not existing.get("workspace"):
        s["workspace"] = workspace
    if first_day and not existing.get("purchaseDate"):
        s["purchaseDate"] = _dt(first_day)
    if domain and not existing.get("domain"):
        s["domain"] = domain
    return s


def plan_sync(zap_domains: list[dict], zap_mailboxes: list[dict],
              tracker: dict[str, dict], *, today: str) -> dict:
    """Decide inserts/updates. Pure.

    Returns ``{"ops": [{action, type, name, set|doc, reason}], "skipped": [...],
    "counts": {...}}``.
    """
    ops: list[dict] = []
    skipped: list[dict] = []
    now = datetime.now(timezone.utc)
    domain_client: dict[str, str | None] = {}

    for r in zap_domains:
        name = r["domain"]
        existing = tracker.get(name)
        client = infer_client(name, r.get("account"), (existing or {}).get("client"))
        domain_client[name] = client
        status = _zap_status(r.get("status"), r.get("expire_on", ""), today)
        ws = _workspace(r.get("provider"))
        if existing:
            s = _changes(existing, expire_on=r.get("expire_on", ""), zap_status=status,
                         workspace=ws, first_day=r.get("registered_on", ""))
            if s:
                ops.append({"action": "update", "type": "DOMAIN", "name": name, "set": s,
                            "reason": ", ".join(sorted(s))})
            continue
        if client is None:
            skipped.append({"type": "DOMAIN", "name": name, "why": "not a current client"})
            continue
        if status != "Active":
            skipped.append({"type": "DOMAIN", "name": name, "why": "lapsed/inactive"})
            continue
        ops.append({"action": "insert", "type": "DOMAIN", "name": name, "reason": "new",
                    "doc": {
                        "type": "DOMAIN", "name": name,
                        "client": TRACKER_SPELLING[client], "provider": "Zapmail",
                        "workspace": ws,
                        "purchaseDate": _dt(r.get("registered_on", "")),
                        "expiryDate": _dt(r.get("expire_on", "")),
                        "status": "Active", "brandedPrewarmed": "Branded",
                        "currency": "USD",
                        "notes": f"Added automatically from Zapmail ({r.get('account')} account)",
                        "createdBy": "ZAPMAIL_SYNC", "remindersSent": [],
                        "createdAt": now, "updatedAt": now,
                    }})

    for m in zap_mailboxes:
        name = m["email"]
        existing = tracker.get(name)
        dom = m.get("domain") or name.partition("@")[2]
        client = domain_client.get(dom)
        if dom not in domain_client:
            client = infer_client(dom, m.get("account"),
                                  (tracker.get(dom) or {}).get("client"))
        raw = str(m.get("status") or "").upper()
        ws = _workspace(m.get("provider"))
        if existing:
            if raw in ("IN_PROGRESS", "INPROGRESS"):
                continue  # not final yet: leave the row alone
            status = _zap_status(raw, m.get("expire_on", ""), today)
            s = _changes(existing, expire_on=m.get("expire_on", ""), zap_status=status,
                         workspace=ws, first_day=m.get("assigned_on", ""), domain=dom)
            if s:
                ops.append({"action": "update", "type": "INBOX", "name": name, "set": s,
                            "reason": ", ".join(sorted(s))})
            continue
        if client is None:
            skipped.append({"type": "INBOX", "name": name, "why": "not a current client"})
            continue
        if raw != "ACTIVE":
            skipped.append({"type": "INBOX", "name": name, "why": f"status {raw or '?'}"})
            continue
        ops.append({"action": "insert", "type": "INBOX", "name": name, "reason": "new",
                    "doc": {
                        "type": "INBOX", "name": name, "domain": dom,
                        "client": TRACKER_SPELLING[client], "provider": "Zapmail",
                        "workspace": ws,
                        "purchaseDate": _dt(m.get("assigned_on", "")),
                        "expiryDate": _dt(m.get("expire_on", "")),
                        "status": "Active", "brandedPrewarmed": "Branded",
                        "currency": "USD",
                        "notes": f"Added automatically from Zapmail ({m.get('account')} account)",
                        "createdBy": "ZAPMAIL_SYNC", "remindersSent": [],
                        "createdAt": now, "updatedAt": now,
                    }})

    counts: dict[str, int] = {}
    for o in ops:
        k = f"{o['type'].lower()}_{o['action']}"
        counts[k] = counts.get(k, 0) + 1
    for s in skipped:
        k = f"{s['type'].lower()}_skipped"
        counts[k] = counts.get(k, 0) + 1
    return {"ops": ops, "skipped": skipped, "counts": counts}


def apply(ops: list[dict]) -> int:
    """Write the planned ops. Returns rows written. Raises if Mongo is down."""
    db = _db()
    if db is None:
        raise RuntimeError("Mongo unreachable — tracker not updated.")
    now = datetime.now(timezone.utc)
    writes = []
    for o in ops:
        if o["action"] == "insert":
            writes.append(UpdateOne({"name": o["name"]}, {"$setOnInsert": o["doc"]},
                                    upsert=True))
        else:
            writes.append(UpdateOne({"name": o["name"]},
                                    {"$set": {**o["set"], "updatedAt": now}}))
    if not writes:
        return 0
    res = db[ASSETS_COLLECTION].bulk_write(writes, ordered=False)
    return int(res.upserted_count + res.modified_count)


async def sync(*, apply_changes: bool = False, domain: str | None = None) -> dict:
    """Plan (and optionally apply) the Zapmail → tracker sync.

    ``domain`` limits it to one domain and its inboxes (webhook refresh).
    """
    from smartlead.zapmail_fleet import all_domains, all_mailboxes

    errors: list[str] = []
    domains = await all_domains(errors)
    if domain:
        domain = domain.strip().lower()
        domains = [d for d in domains if d["domain"] == domain]
    mailboxes = await all_mailboxes(errors, domain=domain)
    tracker = read_tracker()
    if not tracker:
        errors.append("tracker unreachable (Mongo) — nothing planned")
        return {"ok": False, "errors": errors, "counts": {}, "ops": [], "skipped": []}
    plan = plan_sync(domains, mailboxes, tracker,
                     today=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    plan["errors"] = errors
    plan["applied"] = apply(plan["ops"]) if apply_changes and not errors else 0
    plan["ok"] = not errors
    if apply_changes and errors:
        plan["note"] = "not applied: a Zapmail account/provider could not be read"
    return plan
