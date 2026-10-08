"""Did every inbox subscription / order actually renew? Pure comparison logic.

Inboxes are paid per subscription (Zapmail) or order (ScaledMail). On
2026-10-08 two bills went wrong and nobody heard: BettrData's Zapmail Outlook
subscription failed ("No payment method on file") and ScaledMail's Precise
Leads order sat at "Requires confirmation" (card needed 3-D Secure). This
module compares today's billing state with the last one saved and says what
changed:

  renewed       bill date moved forward and no payment problem       (quiet ✓)
  not_renewed   bill date has passed but did not move (Zapmail)      → alert
  payment       a payment failure message appeared                    → alert
  status        ACTIVE → anything else (cancelled, past due)          → alert
  new           a subscription / order we have not seen before        → alert
                (someone bought in the provider's website)
  gone          it disappeared                                         → alert
  bill_day      ScaledMail bills today: its API does not report payment
                state, so the team is told to check the card payment  → note

``snapshot rows``: {key, provider, id, label, bills_on, status, payment_failure,
inboxes, clients}.
"""

from __future__ import annotations

from datetime import date


def key_of(row: dict) -> str:
    return f"{row['provider']}:{row['id']}"


def compare(prev: dict[str, dict], now: list[dict], today: date) -> list[dict]:
    """Events between the saved state (by key) and today's rows."""
    events = []
    t = today.isoformat()
    seen = set()
    for r in now:
        k = key_of(r)
        seen.add(k)
        p = prev.get(k)
        base = {"key": k, "provider": r["provider"], "label": r["label"], "clients": r.get("clients") or [],
                "bills_on": r.get("bills_on"), "inboxes": r.get("inboxes")}
        if p is None:
            if prev:      # first run ever: everything is "new", say nothing
                events.append({**base, "event": "new", "detail": f"new {r['provider']} subscription / order"})
            continue
        if r.get("payment_failure") and r.get("payment_failure") != p.get("payment_failure"):
            events.append({**base, "event": "payment", "detail": f"payment failed: {r['payment_failure']}"})
        if str(r.get("status")).upper() != str(p.get("status")).upper():
            bad = str(r.get("status")).upper() not in ("ACTIVE",)
            events.append({**base, "event": "status" if bad else "renewed",
                           "detail": f"status {p.get('status')} → {r.get('status')}"})
        pb, nb = p.get("bills_on") or "", r.get("bills_on") or ""
        if r["provider"] == "Zapmail":
            if nb > pb and pb:
                if not r.get("payment_failure"):
                    events.append({**base, "event": "renewed", "detail": f"renewed — next bill {nb}"})
            elif nb and nb < t:
                # Bill date is in the past and has not moved: Zapmail did not renew it.
                if not p.get("not_renewed_alerted") == nb:
                    events.append({**base, "event": "not_renewed", "detail": f"bill date {nb} passed, not renewed"})
        else:   # ScaledMail: bill day is computed from the order date; no payment state in the API
            if nb == t and p.get("bill_day_noted") != t:
                events.append({**base, "event": "bill_day",
                               "detail": "bills today — ScaledMail's API does not show payment state; check the card payment went through (it can need confirmation)"})
    for k, p in prev.items():
        if k not in seen and str(p.get("status")).upper() == "ACTIVE":
            events.append({"key": k, "provider": p["provider"], "label": p["label"], "clients": p.get("clients") or [],
                           "bills_on": p.get("bills_on"), "inboxes": p.get("inboxes"),
                           "event": "gone", "detail": "no longer listed by the provider (cancelled or removed)"})
    return events


ALERT = {"not_renewed", "payment", "status", "new", "gone", "bill_day"}
ICON = {"not_renewed": ":rotating_light:", "payment": ":x:", "status": ":warning:", "new": ":new:",
        "gone": ":wastebasket:", "bill_day": ":credit_card:", "renewed": ":white_check_mark:"}


def format_events(events: list[dict]) -> str:
    alerts = [e for e in events if e["event"] in ALERT]
    ok = [e for e in events if e["event"] == "renewed"]
    if not alerts and not ok:
        return ""
    lines = ["*💳 Inbox billing watch*"]
    for e in alerts + ok:
        who = ", ".join(e["clients"]) if e["clients"] else ""
        lines.append(f"{ICON[e['event']]} *{e['provider']} · {e['label']}*" + (f" ({who})" if who else "")
                     + f" — {e['detail']}")
    if any(e["event"] in ("not_renewed", "payment", "status") for e in alerts):
        lines.append("_Unpaid inboxes get suspended. Fix the card / wallet in the provider, or let them lapse on purpose._")
    return "\n".join(lines)


def remember(prev: dict[str, dict], now: list[dict], events: list[dict], today: date) -> dict[str, dict]:
    """New saved state: today's rows + which alerts were already sent."""
    out = {}
    t = today.isoformat()
    sent = {(e["key"], e["event"]) for e in events}
    for r in now:
        k = key_of(r)
        row = dict(r)
        p = prev.get(k) or {}
        row["not_renewed_alerted"] = r.get("bills_on") if (k, "not_renewed") in sent else p.get("not_renewed_alerted")
        row["bill_day_noted"] = t if (k, "bill_day") in sent else p.get("bill_day_noted")
        out[k] = row
    return out
