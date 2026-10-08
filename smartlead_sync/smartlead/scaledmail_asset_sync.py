"""Keep the /infra asset tracker in step with ScaledMail (same rules as Zapmail).

Until 2026-10-08 the 144 ScaledMail rows in the tracker were typed in by
hand, and the inbox expiry dates had stopped moving (55 Melior inboxes said
"expires 2026-10-06" while their order kept billing). Rules:

* **Domain expiry** follows ScaledMail's registration ``renewal_at``.
* **Inbox expiry** is the order's next monthly billing day — always (the
  order is what bills; a hand-typed date was 2 days late on 2026-10-08).
* **Status** only downgrades (Active → Inactive when the domain is gone or
  its order is cancelled). Nothing is flipped back to Active.
* Missing fields are filled; team fields (client, owner, channel, costs,
  notes) are never overwritten.
* **New rows** only for current clients, only when live ("Active"), with no
  owner/channel. Unassigned domains are reported, never added — set the
  order tag to the client (Slack: Set client) and the next sync adds them.
* Nothing is deleted.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from smartlead.zapmail_asset_sync import _changes, _dt, apply, read_tracker
from smartlead.zapmail_clients import TRACKER_SPELLING
from smartlead.scaledmail_fleet import WORKSPACE

SOURCE = "Scaledmail"   # how the tracker spells the provider (also "Scaled Mail")


def _is_sm(doc: dict) -> bool:
    return "scaled" in str(doc.get("provider") or "").lower()


def plan_sync(snap: dict, tracker: dict[str, dict]) -> dict:
    """Pure. ``snap`` = scaledmail_fleet.snapshot(..., with_mailboxes=True)."""
    today = snap["today"]
    now = datetime.now(timezone.utc)
    ops: list[dict] = []
    skipped: list[dict] = []
    live_domains = set()

    for d in snap["domains"]:
        name = d["domain"]
        live_domains.add(name)
        existing = tracker.get(name)
        ws = WORKSPACE.get(d["provider"], "Google")
        live = d["status"] == "Active" and d["order_status"] == "Active"
        # Only a cancelled order downgrades; "In Progress" (being set up) does not.
        paid = "Active" if d["order_status"] == "Active" else "Inactive"
        if existing:
            s = _changes(existing, expire_on=d["renewal_at"], zap_status=paid,
                         workspace=ws, first_day=d["registered_on"])
            if s:
                ops.append({"action": "update", "type": "DOMAIN", "name": name, "set": s,
                            "reason": ", ".join(sorted(s))})
        elif d["client"] is None:
            skipped.append({"type": "DOMAIN", "name": name, "why": "no client (set the order tag)"})
        elif not live:
            skipped.append({"type": "DOMAIN", "name": name, "why": f"not live yet ({d['status']})"})
        else:
            ops.append({"action": "insert", "type": "DOMAIN", "name": name, "reason": "new", "doc": {
                "type": "DOMAIN", "name": name, "client": TRACKER_SPELLING[d["client"]],
                "provider": SOURCE, "workspace": ws,
                "purchaseDate": _dt(d["registered_on"]), "expiryDate": _dt(d["renewal_at"]),
                "status": "Active", "brandedPrewarmed": "Branded", "currency": "USD",
                "yearlyCost": d["renewal_price"],
                "notes": f"Added automatically from ScaledMail (order {d['order_id']})",
                "createdBy": "SCALEDMAIL_SYNC", "remindersSent": [], "createdAt": now, "updatedAt": now}})

        per_box = None
        if d["provider"] == "outlook" and d["mailboxes"]:
            per_box = round(50.0 / d["mailboxes"], 2)
        elif d["provider"] == "google":
            per_box = 3.5
        elif d["provider"] == "smtp" and d["mailboxes"]:
            per_box = round(3.75 / d["mailboxes"], 2)
        for m in d.get("mailbox_rows") or []:
            email = m["email"]
            existing = tracker.get(email)
            box_live = live and str(m.get("status")) == "Active"
            if existing:
                # The order's bill day IS the inbox's renewal day. 2026-10-08:
                # Precise Leads' 39 inboxes were typed in as "expire 10 Oct" while
                # ScaledMail billed them on the 8th, so the reminder was 2 days
                # late. The order date wins, like Zapmail's dates do.
                s = _changes(existing, expire_on=d["billing_day"], zap_status=paid,
                             workspace=ws, first_day="", domain=name)
                if s:
                    ops.append({"action": "update", "type": "INBOX", "name": email, "set": s,
                                "reason": ", ".join(sorted(s))})
            elif d["client"] is None:
                skipped.append({"type": "INBOX", "name": email, "why": "no client (set the order tag)"})
            elif not box_live:
                skipped.append({"type": "INBOX", "name": email, "why": f"not live yet ({m.get('status')})"})
            else:
                ops.append({"action": "insert", "type": "INBOX", "name": email, "reason": "new", "doc": {
                    "type": "INBOX", "name": email, "domain": name,
                    "client": TRACKER_SPELLING[d["client"]], "provider": SOURCE, "workspace": ws,
                    "purchaseDate": _dt(today), "expiryDate": _dt(d["billing_day"]),
                    "status": "Active", "brandedPrewarmed": "Branded", "currency": "USD",
                    "monthlyCost": per_box,
                    "notes": f"Added automatically from ScaledMail (order {d['order_id']})",
                    "createdBy": "SCALEDMAIL_SYNC", "remindersSent": [], "createdAt": now, "updatedAt": now}})

    # Tracked as ScaledMail but no longer on ScaledMail: the order was cancelled.
    # ScaledMail's mailbox list is not always the real one: on growmelior.com
    # (2026-10-08) it named 25 aliases that exist nowhere, while Smartlead and
    # the tracker agree on 25 others. So new inbox rows are only added for a
    # domain the tracker has no inboxes for yet.
    tracked_inbox_domains = {n.partition("@")[2] for n, doc in tracker.items()
                             if "@" in n and doc.get("type") == "INBOX"}
    kept = []
    for o in ops:
        if o["action"] == "insert" and o["type"] == "INBOX" and o["doc"]["domain"] in tracked_inbox_domains:
            skipped.append({"type": "INBOX", "name": o["name"],
                            "why": "ScaledMail lists an address the tracker does not have on a tracked domain"})
        else:
            kept.append(o)
    ops = kept

    gone = []
    # An empty domain list is more likely a bad answer than a cancelled fleet.
    for name, doc in (tracker.items() if live_domains else ()):
        if not _is_sm(doc) or str(doc.get("status")) != "Active":
            continue
        dom = name.partition("@")[2] if "@" in name else name
        if dom not in live_domains:
            gone.append(name)
            ops.append({"action": "update", "type": doc.get("type", "?"), "name": name,
                        "set": {"status": "Inactive"}, "reason": "not on ScaledMail any more"})

    counts: dict[str, int] = {}
    for o in ops:
        k = f"{str(o['type']).lower()}_{o['action']}"
        counts[k] = counts.get(k, 0) + 1
    for s in skipped:
        k = f"{s['type'].lower()}_skipped"
        counts[k] = counts.get(k, 0) + 1
    return {"ops": ops, "skipped": skipped, "counts": counts, "gone": gone}


def sync(*, apply_changes: bool = False) -> dict:
    from smartlead.scaledmail import ScaledMailClient
    from smartlead.scaledmail_fleet import snapshot

    tracker = read_tracker()
    if not tracker:
        return {"ok": False, "errors": ["tracker unreachable (Mongo) — nothing planned"],
                "counts": {}, "ops": [], "skipped": []}
    with ScaledMailClient() as sm:
        snap = snapshot(sm, tracker=tracker, today=date.today(), with_mailboxes=True)
    plan = plan_sync(snap, tracker)
    plan["errors"] = []
    plan["applied"] = apply(plan["ops"]) if apply_changes else 0
    plan["ok"] = True
    return plan
