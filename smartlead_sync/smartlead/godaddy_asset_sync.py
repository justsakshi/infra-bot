"""Keep the /infra tracker in step with GoDaddy domains (same rules as Zapmail).

* **Expiry** always follows GoDaddy's ``expiresAt`` (a renewal moves it).
* **Status** only downgrades (Active → Inactive when GoDaddy no longer lists
  the domain as active).
* **New rows** only for a current client: the bot's purchase ledger says who
  a domain was bought for (sending domains never carry the brand, so the name
  alone cannot tell). Domains with no known client are reported, not added.
* Team fields are never overwritten; nothing is deleted.
"""

from __future__ import annotations

from datetime import datetime, timezone

from smartlead.zapmail_asset_sync import _changes, _dt, apply, read_tracker
from smartlead.zapmail_clients import TRACKER_SPELLING, client_from_tracker, infer_client

SOURCE = "GoDaddy"
RENEWAL_USD = 14.99      # v3 auto-renewal .com (docs 2026-10-09)


def plan_sync(domains: list[dict], tracker: dict[str, dict], bought_for: dict[str, str],
              today: str) -> dict:
    """Pure. ``domains`` = GoDaddyClient.domains(); ``bought_for`` = domain → client."""
    now = datetime.now(timezone.utc)
    ops, skipped = [], []
    for d in domains:
        name = str(d.get("domain") or "").lower()
        if not name:
            continue
        expires = str(d.get("expiresAt") or "")[:10]
        active = str(d.get("status") or "").upper() == "ACTIVE"
        existing = tracker.get(name)
        if existing:
            s = _changes(existing, expire_on=expires, zap_status="Active" if active else "Inactive",
                         workspace=existing.get("workspace") or "", first_day=str(d.get("createdAt") or "")[:10])
            if not s.get("workspace"):
                s.pop("workspace", None)    # GoDaddy does not know which mailbox provider sits on it
            if s:
                ops.append({"action": "update", "type": "DOMAIN", "name": name, "set": s,
                            "reason": ", ".join(sorted(s))})
            continue
        client = bought_for.get(name) or infer_client(name, None)
        if not client:
            skipped.append({"type": "DOMAIN", "name": name, "why": "no client (not bought by the bot)"})
            continue
        if not active:
            skipped.append({"type": "DOMAIN", "name": name, "why": f"not active ({d.get('status')})"})
            continue
        ops.append({"action": "insert", "type": "DOMAIN", "name": name, "reason": "new", "doc": {
            "type": "DOMAIN", "name": name, "client": TRACKER_SPELLING[client], "provider": SOURCE,
            "purchaseDate": _dt(str(d.get("createdAt") or "")[:10] or today),
            "expiryDate": _dt(expires), "status": "Active", "brandedPrewarmed": "Branded",
            "currency": "USD", "yearlyCost": RENEWAL_USD,
            "notes": "Added automatically from GoDaddy" + (" (auto-renew on)" if d.get("autoRenew") else
                                                          " (auto-renew OFF)"),
            "createdBy": "GODADDY_SYNC", "remindersSent": [], "createdAt": now, "updatedAt": now}})
    counts: dict[str, int] = {}
    for o in ops:
        k = f"domain_{o['action']}"
        counts[k] = counts.get(k, 0) + 1
    if skipped:
        counts["domain_skipped"] = len(skipped)
    return {"ops": ops, "skipped": skipped, "counts": counts}


def sync(*, apply_changes: bool = False) -> dict:
    from datetime import date

    from smartlead.godaddy import GoDaddyClient
    from smartlead.godaddy_orders import client_by_domain

    tracker = read_tracker()
    if not tracker:
        return {"ok": False, "errors": ["tracker unreachable (Mongo) — nothing planned"],
                "counts": {}, "ops": [], "skipped": []}
    with GoDaddyClient() as gd:
        domains = gd.domains()
    plan = plan_sync(domains, tracker, client_by_domain(), date.today().isoformat())
    plan["errors"] = []
    plan["applied"] = apply(plan["ops"]) if apply_changes else 0
    plan["ok"] = True
    return plan


__all__ = ["plan_sync", "sync", "client_from_tracker"]
