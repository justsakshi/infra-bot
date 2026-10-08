"""Read-only view of the ScaledMail fleet: who owns what, what it costs, what renews.

ScaledMail returns four separate lists — orders (monthly subscriptions),
domains (with mailbox names), purchased domains (registrations with renewal
dates) and mailboxes per domain. This module joins them into one picture.
Everything here is READ: nothing spends, nothing changes.

Client of a domain (one rule, like ``zapmail_clients``):
  1. the /infra tracker's client, when the team recorded one;
  2. the order tag, when it names a current client ("client-melior", "Melior");
  3. the brand in the redirect target (getmelior.com → Melior);
  4. the brand in the domain name;
  5. otherwise None — unassigned, never guessed.

Billing: an order bills monthly on the day of the month it was created
(Stripe subscription), so a mailbox's "expiry" is that order's next billing
day. Domain registrations renew yearly on ``renewal_at``.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta

from smartlead.zapmail_clients import CURRENT_CLIENTS, _squash, client_from_tracker, infer_client

WORKSPACE = {"google": "Google", "outlook": "Outlook", "smtp": "SMTP"}


def _host(url: str | None) -> str:
    u = str(url or "").strip().lower()
    for p in ("https://", "http://"):
        if u.startswith(p):
            u = u[len(p):]
    return u.split("/")[0].removeprefix("www.")


def client_from_tag(tag: str | None) -> str | None:
    s = _squash(tag)
    if not s:
        return None
    for key in CURRENT_CLIENTS:
        if _squash(key) in s:
            return key
    return None


def client_for(domain: str, *, tracker_client: str | None = None, tag: str | None = None,
               redirect: str | None = None) -> str | None:
    """Profile key of the current client owning a ScaledMail domain, or None."""
    t = client_from_tracker(tracker_client)
    if t:
        return t
    if tracker_client:                  # tracked under a past client: keep it theirs
        return None
    by_tag = client_from_tag(tag)
    if by_tag:
        return by_tag
    host = _host(redirect)
    if host:
        by_redirect = infer_client(host, None)
        if by_redirect:
            return by_redirect
    return infer_client(domain, None)


def next_billing(created_at: str, today: date) -> str:
    """Next monthly billing day (today counts) for an order created on created_at."""
    try:
        d0 = datetime.strptime(str(created_at)[:10], "%Y-%m-%d").date()
    except ValueError:
        return ""
    y, m = today.year, today.month
    for _ in range(2):
        day = min(d0.day, calendar.monthrange(y, m)[1])
        cand = date(y, m, day)
        if cand >= today and cand >= d0:
            return cand.isoformat()
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return ""  # pragma: no cover


def _money(v) -> float:
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return 0.0


def snapshot(sm, *, tracker: dict[str, dict] | None = None, today: date | None = None,
             with_mailboxes: bool = False) -> dict:
    """Joined fleet picture. ``sm`` is an open ScaledMailClient."""
    today = today or date.today()
    tracker = tracker or {}
    orders = sm.orders()
    by_order = {o["id"]: o for o in orders}
    reg = {r["domain"].lower(): r for r in sm.purchased_domains()}
    domains = []
    for d in sm.domains():
        name = d["domain"].lower()
        o = by_order.get(d.get("payment_id")) or {}
        r = reg.get(name) or {}
        status = str(d.get("status") or "")
        row = {
            "domain": name, "domain_id": d.get("id"), "provider": d.get("order_type", ""),
            "status": status, "mailboxes": int(d.get("total_mailboxes") or 0),
            "senders": sorted({f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()
                               for m in d.get("mailbox") or []}),
            "aliases": [m.get("alias") for m in d.get("mailbox") or []],
            "redirect": d.get("redirect") or "", "tag": d.get("tag") or "",
            "order_id": d.get("payment_id"), "order": o.get("description", ""),
            "order_status": o.get("status", ""),
            "billing_day": next_billing(o.get("created_at", ""), today) if o.get("status") == "Active" else "",
            "registrar": d.get("domain_provider", ""),
            "renewal_at": r.get("renewal_at") or "", "renewal_price": r.get("renewal_price"),
            "registered_on": r.get("created_at") or "",
            "masking": bool(d.get("domain_masking")),
            "client": client_for(name, tracker_client=(tracker.get(name) or {}).get("client"),
                                 tag=d.get("tag"), redirect=d.get("redirect")),
        }
        if with_mailboxes and d.get("id"):
            row["mailbox_rows"] = [{"email": m.get("email", "").lower(), "status": m.get("status"),
                                    "name": f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()}
                                   for m in sm.mailboxes(d["id"])]
        domains.append(row)
    in_use = {d["domain"] for d in domains}
    inventory = [{"domain": n, "renewal_at": r.get("renewal_at"), "renewal_price": r.get("renewal_price")}
                 for n, r in sorted(reg.items()) if n not in in_use and not r.get("domain_id")]
    active = [o for o in orders if o.get("status") == "Active"]
    order_rows = []
    for o in orders:
        ds = [d for d in domains if d["order_id"] == o["id"]]
        clients = sorted({d["client"] or "unassigned" for d in ds})
        order_rows.append({"id": o["id"], "description": o.get("description", ""),
                           "amount": _money(o.get("amount")), "status": o.get("status"),
                           "created_at": str(o.get("created_at", ""))[:10],
                           "billing_day": next_billing(o.get("created_at", ""), today) if o.get("status") == "Active" else "",
                           "domains": len(ds), "mailboxes": sum(d["mailboxes"] for d in ds),
                           "clients": clients})
    counts: dict = {}
    for d in domains:
        k = d["provider"] or "?"
        c = counts.setdefault(k, {"domains": 0, "mailboxes": 0, "live": 0, "in_progress": 0})
        c["domains"] += 1
        c["mailboxes"] += d["mailboxes"]
        c["live" if d["status"] == "Active" else "in_progress"] += 1
    by_client: dict = {}
    for d in domains:
        c = by_client.setdefault(d["client"] or "unassigned", {"domains": 0, "mailboxes": 0})
        c["domains"] += 1
        c["mailboxes"] += d["mailboxes"]
    return {"domains": domains, "orders": order_rows, "inventory": inventory,
            "monthly_total": round(sum(_money(o.get("amount")) for o in active), 2),
            "active_orders": len(active), "by_provider": counts, "by_client": by_client,
            "today": today.isoformat()}


def renewals(snap: dict, *, domain_days: int = 60, billing_days: int = 14) -> dict:
    """Registrations renewing within ``domain_days`` and order billings within ``billing_days``."""
    today = date.fromisoformat(snap["today"])
    dlim = (today + timedelta(days=domain_days)).isoformat()
    blim = (today + timedelta(days=billing_days)).isoformat()
    regs = sorted(([{"domain": d["domain"], "renewal_at": d["renewal_at"], "price": d["renewal_price"],
                     "client": d["client"], "provider": d["provider"], "mailboxes": d["mailboxes"]}
                    for d in snap["domains"] if d["renewal_at"] and d["renewal_at"] <= dlim]
                   + [{"domain": i["domain"], "renewal_at": i["renewal_at"], "price": i["renewal_price"],
                       "client": None, "provider": "unused", "mailboxes": 0}
                      for i in snap["inventory"] if i["renewal_at"] and i["renewal_at"] <= dlim]),
                  key=lambda x: x["renewal_at"])
    bills = sorted((o for o in snap["orders"] if o["billing_day"] and o["billing_day"] <= blim),
                   key=lambda o: o["billing_day"])
    return {"domains": regs, "billing": bills, "domain_days": domain_days, "billing_days": billing_days}


def locate(sm, domain: str, *, tracker: dict[str, dict] | None = None) -> dict:
    """One domain: its row, mailboxes (no passwords), order, masking."""
    domain = domain.strip().lower()
    snap = snapshot(sm, tracker=tracker)
    hit = next((d for d in snap["domains"] if d["domain"] == domain), None)
    if not hit:
        inv = next((i for i in snap["inventory"] if i["domain"] == domain), None)
        return {"domain": domain, "found": False, "inventory": inv}
    hit["mailbox_rows"] = [{"email": m.get("email", "").lower(), "status": m.get("status"),
                            "name": f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()}
                           for m in sm.mailboxes(hit["domain_id"])] if hit.get("domain_id") else []
    if hit["masking"]:
        hit["masking_target"] = next((m.get("primary_domain") for m in sm.domain_masking()
                                      if str(m.get("domain", "")).lower() == domain), None)
    order = next((o for o in snap["orders"] if o["id"] == hit["order_id"]), None)
    return {"domain": domain, "found": True, "row": hit, "order": order}


def report_summary(reports: list[dict], *, bad_below: float = 80.0) -> dict:
    """Latest ScaledMail placement report: averages + domains under ``bad_below``."""
    if not reports:
        return {"has_reports": False}
    latest = max(reports, key=lambda r: str(r.get("weekof", "")))
    rows = [{"domain": d.get("domain_name"), "provider": d.get("order_type"),
             "score": (d.get("stats") or {}).get("overall_score"),
             "reputation": (d.get("stats") or {}).get("domain_reputation"),
             "blacklisted": (d.get("stats") or {}).get("blacklisted") or []}
            for d in latest.get("domains") or []]
    bad = [r for r in rows if (isinstance(r["score"], (int, float)) and r["score"] < bad_below) or r["blacklisted"]]
    return {"has_reports": True, "weekof": latest.get("weekof"), "avg_inbox": latest.get("avgInboxScore"),
            "avg_reputation": latest.get("avgReputation"), "domains": len(rows),
            "flagged": sorted(bad, key=lambda r: r["score"] if r["score"] is not None else -1)}
