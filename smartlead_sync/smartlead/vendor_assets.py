"""Asset tracker rows for inboxes bought by hand from a vendor infra-bot cannot read, such as InboxKit (t248, 18 Nov
2026). Zapmail and ScaledMail rows come from their syncs (zapmail_asset_sync.py, scaledmail_asset_sync.py); a vendor
without an API had its rows typed in by hand (CSV_UPLOAD), so the gate self-test's "every connected inbox has a row"
check names them until someone does. This plans the same rows from the Smartlead inboxes, insert-only through the
syncs' own write (zapmail_asset_sync.apply: $setOnInsert by name, so a row the tracker has is never changed).

Dates the vendor does not tell us are not invented: a domain row has no expiry (the renewal check shows it as
"estimated: bought + 1 year", renewal_labels.js), and an inbox row's expiry is the monthly bill day given by hand,
called estimated in its notes. Costs are left empty.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

WORKSPACE = {"GMAIL": "Google", "OUTLOOK": "Outlook"}
CREATED_BY = "BY_HAND_CLI"


def _dt(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _day(d: date) -> str:
    return f"{d.day} {d.strftime('%b %Y')}"


def plan_rows(accounts: list[dict], tracker: dict[str, dict], *, vendor: str, client: str, bought: date,
              received: date, bill_day: date, kind: str, why: str) -> dict:
    """{"ops": insert ops for zapmail_asset_sync.apply, "counts"} for the Smartlead ``accounts`` (from_email, type):
    one DOMAIN row per domain and one INBOX row per inbox the tracker (``{name: doc}``) does not have. Pure."""
    now = datetime.now(timezone.utc)
    note = (f"Added by hand ({why}): {vendor} {kind.lower()}, bought {_day(bought)}, received {_day(received)}. "
            f"Warm-up is run by {vendor} outside Smartlead (Smartlead warm-up off on purpose).")
    ops, counts, seen = [], {"domain_insert": 0, "inbox_insert": 0, "already": 0}, set()
    for a in sorted(accounts, key=lambda a: str(a.get("from_email")).lower()):
        email = str(a.get("from_email") or "").strip().lower()
        if "@" not in email:
            continue
        dom, ws = email.split("@")[1], WORKSPACE.get(str(a.get("type") or "").upper(), "SMTP")
        base = {"client": client, "provider": vendor, "workspace": ws, "purchaseDate": _dt(bought),
                "status": "Active", "brandedPrewarmed": kind, "currency": "USD", "createdBy": CREATED_BY,
                "remindersSent": [], "createdAt": now, "updatedAt": now}
        if dom not in seen:
            seen.add(dom)
            if dom in tracker:
                counts["already"] += 1
            else:
                counts["domain_insert"] += 1
                ops.append({"action": "insert", "type": "DOMAIN", "name": dom, "reason": "new", "doc": {
                    "type": "DOMAIN", "name": dom, **base, "expiryDate": None, "yearlyCost": None,
                    "notes": note + " Domain renewal date not known."}})
        if email in tracker:
            counts["already"] += 1
            continue
        counts["inbox_insert"] += 1
        ops.append({"action": "insert", "type": "INBOX", "name": email, "reason": "new", "doc": {
            "type": "INBOX", "name": email, "domain": dom, **base, "expiryDate": _dt(bill_day), "monthlyCost": None,
            "notes": note + f" Expiry = monthly bill day, estimated from the purchase date ({_day(bill_day)})."}})
    return {"ops": ops, "counts": counts}
