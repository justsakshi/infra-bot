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

import re
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
                "bills_on": r.get("bills_on"), "inboxes": r.get("inboxes"), "invoice_url": r.get("invoice_url") or ""}
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
                               "detail": "bills today, check the card payment went through"})
    for k, p in prev.items():
        if k not in seen and str(p.get("status")).upper() == "ACTIVE":
            events.append({"key": k, "provider": p["provider"], "label": p["label"], "clients": p.get("clients") or [],
                           "bills_on": p.get("bills_on"), "inboxes": p.get("inboxes"),
                           "event": "gone", "detail": "no longer listed by the provider (cancelled or removed)"})
    return events


ALERT = {"not_renewed", "payment", "status", "new", "gone", "bill_day"}
URGENT = {"not_renewed", "payment", "status"}
ICON = {"not_renewed": ":rotating_light:", "payment": ":x:", "status": ":warning:", "new": ":new:",
        "gone": ":wastebasket:", "bill_day": ":credit_card:", "renewed": ":white_check_mark:"}
ADVICE = "_Unpaid inboxes get suspended. Fix the card / wallet in the provider, or let them lapse on purpose._"
SCALEDMAIL_NOTE = ("_ScaledMail's API does not show payment state, so on its bill day check the card payment "
                   "went through in ScaledMail (it can need 3-D Secure confirmation)._")


_ISO_DAY = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")


def _plain(detail: str) -> str:
    """An event's saved detail as Slack shows it: '8 Nov' dates, a comma for the dash."""
    d = _ISO_DAY.sub(lambda m: f"{int(m[3])} {date(int(m[1]), int(m[2]), int(m[3])):%b}", str(detail or ""))
    return d.replace(" — ", ", ")


def format_events(events: list[dict], today: date | None = None) -> tuple[list[str], list[str]]:
    """``(headline_lines, thread_parts)``: a title line plus one line per event
    in the channel; the what-to-do advice once, in the thread."""
    alerts = [e for e in events if e["event"] in ALERT]
    ok = [e for e in events if e["event"] == "renewed"]
    if not alerts and not ok:
        return [], []
    icon = "🚨" if any(e["event"] in URGENT for e in alerts) else "⚠️" if alerts else "✅"
    when = f", {today.day} {today:%b}" if today else ""
    head = [f"{icon} Inbox billing watch{when}"]
    for e in alerts + ok:
        who = ", ".join(e["clients"]) if e["clients"] else ""
        pay = ""
        if e.get("invoice_url") and e["event"] in ("payment", "not_renewed", "status", "bill_day"):
            pay = f" · <{e['invoice_url']}|{'Pay / see invoice' if e['provider'] == 'Zapmail' else 'Open ScaledMail billing'}>"
        head.append(f"{ICON[e['event']]} *{e['provider']} · {e['label']}*" + (f" ({who})" if who else "")
                    + f" · {_plain(e['detail'])}{pay}")
    advice = []
    if any(e["event"] in URGENT for e in alerts):
        advice.append(ADVICE)
    if any(e["event"] == "bill_day" for e in alerts):
        advice.append(SCALEDMAIL_NOTE)
    return head, (["\n".join(advice)] if advice else [])


def history_records(events: list[dict], now: list[dict], today: date) -> list[dict]:
    """Every event as a permanent record (Mongo ``bill_history``): which bill,
    how much, the next bill date, its domains. The saved state above keeps only
    the latest picture; this is the memory of what renewed and when. Pure."""
    by_key = {key_of(r): r for r in now}
    out = []
    for e in events:
        r = by_key.get(e["key"]) or {}
        out.append({"at": today.isoformat(), "key": e["key"], "provider": e["provider"],
                    "id": r.get("id") or e["key"].split(":", 1)[-1], "event": e["event"], "label": e["label"],
                    "price": r.get("price"), "next_bill": r.get("bills_on") or e.get("bills_on"),
                    "domains": list(r.get("domains") or []), "inboxes": r.get("inboxes") or e.get("inboxes"),
                    "clients": list(e.get("clients") or []), "detail": e.get("detail", "")})
    return out


_WHAT = {"renewed": "renewed", "bill_day": "billed", "not_renewed": "not renewed", "new": "new subscription / order",
         "gone": "cancelled or removed"}


def history_lines(records: list[dict]) -> list[str]:
    """One line per record, newest first: '8 Oct · renewed · Zapmail · 21 Google
    inboxes · $68.25 · next bill 8 Nov'. Pure."""
    def day(iso):
        d = date.fromisoformat(str(iso)[:10])
        return f"{d.day} {d:%b}"
    lines = []
    for r in sorted(records, key=lambda x: x["at"], reverse=True):
        what = _WHAT.get(r["event"]) or r.get("detail") or r["event"]
        price = f" · ${float(r['price']):,.2f}" if r.get("price") is not None else ""
        nxt = f" · next bill {day(r['next_bill'])}" if r["event"] == "renewed" and r.get("next_bill") else ""
        lines.append(f"{day(r['at'])} · {what} · {r['provider']} · {r['label']}{price}{nxt}")
    return lines


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
