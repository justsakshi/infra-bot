#!/usr/bin/env python3
"""Watch every inbox subscription / order on Zapmail and ScaledMail and say
when one did NOT renew, failed payment, was cancelled, or appeared out of
nowhere. READ-ONLY on the providers; remembers state in Mongo
(``billing_watch``) so each change is reported once.

    python billing_watch.py            # compare, post changes, save state
    python billing_watch.py --dry      # print only (no post, no save)
    python billing_watch.py --json

Posts to RENEWAL_REVIEW_CHANNEL (else ZAPMAIL_NOTIFY_CHANNEL, else the infra
channel). The state is saved only after Slack accepted the post, or when there
was nothing to post: a refused alert goes out again next run (audit 7 Nov 2026).
Exits 1 when the post was refused. Rules: smartlead/billing_watch.py.

Daily digest (16 Nov 2026, smartlead/digest.py): with the digest on, nothing is posted here. Each alert is an
urgent ask in the digest (posted at once, one time per key) with where to pay or fix it; a ScaledMail bill day is
a plain ask. Alerts are one-off events, but the job runs 6 times a day and its asks replace the last run's, so
the open ones are kept in the state doc (``open``) until the bill looks fine again (paid, renewed, active) or,
for one-off events nothing can close (new, gone, bill day), for OPEN_DAYS. Read errors are "broken" asks.
If the digest can't be written, the old post goes out.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import billing_watch as bw
from smartlead import digest as dg

STATE_ID = "state"
SOURCE = "billing-watch"
OPEN_DAYS = 3                 # a new / gone / bill-day ask stays this long: nothing in the data closes it
DIGEST_URGENT = bw.ALERT - {"bill_day"}     # payment failed, not renewed, cancelled, bought outside the bot
SCALEDMAIL_BILLING = "https://app.scaledmail.com"


def _who(e: dict) -> str:
    return f" for {', '.join(e['clients'])}" if e.get("clients") else ""


def _ask_key(e: dict, row: dict | None, today: date) -> str:
    """Stable per thing and per bill: the same failure is one ask (posted once), next month's is a new one."""
    k = e["key"]                      # "<provider>:<subscription / order id>"
    bill = (row or {}).get("bills_on") or e.get("bills_on") or ""
    if e["event"] in ("payment", "not_renewed"):
        return f"{SOURCE}:{e['event']}:{k}:{bill}"
    if e["event"] == "status":
        return f"{SOURCE}:status:{k}:{(row or {}).get('status')}:{bill}"
    if e["event"] == "bill_day":
        return f"{SOURCE}:bill_day:{k}:{today.isoformat()}"
    return f"{SOURCE}:{e['event']}:{k}"           # new, gone


def _pay_step(e: dict) -> str:
    url = e.get("invoice_url") or ""
    if e["provider"] == "Zapmail":
        return ("Zapmail, Billing, pay the invoice" + (f" (<{url}|open the invoice>)" if url else "")
                + " or fix the card on file; if the inboxes should go, let them lapse on purpose.")
    return (f"ScaledMail, Billing, confirm the card payment (it can need 3-D Secure): {url or SCALEDMAIL_BILLING}; "
            "if the inboxes should go, let them lapse on purpose.")


def ask_doc(e: dict, row: dict | None, today: date) -> dict:
    """One alert event as a digest ask, kept in the state doc while it is open. Pure."""
    ev = e["event"]
    thing = f"{e['provider']}, {e['label']}{_who(e)}"
    if ev == "payment":
        text, step = f"Payment failed ({(row or {}).get('payment_failure') or 'no reason given'}): {thing}.", _pay_step(e)
    elif ev == "not_renewed":
        text, step = f"Not renewed, the bill date {bw._plain(e.get('bills_on'))} passed: {thing}.", _pay_step(e)
    elif ev == "status":
        text, step = f"Subscription is now {(row or {}).get('status') or 'not active'}: {thing}.", _pay_step(e)
    elif ev == "new":
        text = f"Bought outside the bot: {thing}."
        step = (f"In {e['provider']}, check who bought it and that its inboxes are wanted; cancel it there if "
                "nobody meant to buy it.")
    elif ev == "gone":
        text = f"Cancelled or removed: {thing}."
        step = f"In {e['provider']}, check it was cancelled on purpose; if not, ask {e['provider']} support to restore it."
    else:                                                     # bill_day (ScaledMail)
        text = f"Bills today: {thing}."
        step = ("ScaledMail, Billing, check today's card payment went through (it can need 3-D Secure "
                f"confirmation): {e.get('invoice_url') or SCALEDMAIL_BILLING}")
    return {"key": _ask_key(e, row, today), "event": ev, "row": e["key"], "provider": e["provider"],
            "text": text, "step": step, "urgent": ev in DIGEST_URGENT, "opened": today.isoformat()}


def still_open(a: dict, row: dict | None, today: date, failed: set[str]) -> bool:
    """Is a kept ask still true? Closed when the bill looks fine again; one-off events after OPEN_DAYS. Pure."""
    if a.get("provider") in failed:
        return True                                      # unread: nothing is known, keep it
    ev = a.get("event")
    if ev in ("new", "gone", "bill_day"):
        try:
            return (today - date.fromisoformat(a["opened"])).days < OPEN_DAYS
        except (KeyError, TypeError, ValueError):
            return False
    if row is None:
        return False                                     # no longer listed: its own "gone" ask says so
    if ev == "payment":
        return bool(row.get("payment_failure"))
    if ev == "not_renewed":
        return bool(row.get("bills_on")) and row["bills_on"] < today.isoformat()
    if ev == "status":
        return str(row.get("status")).upper() != "ACTIVE"
    return False


def open_asks(prev_open: dict, events: list[dict], rows: list[dict], today: date, failed: set[str]) -> dict:
    """{ask key: ask doc}: the asks kept from earlier runs that are still true, plus this run's alerts. Pure."""
    by_key = {bw.key_of(r): r for r in rows}
    out = {k: a for k, a in (prev_open or {}).items() if still_open(a, by_key.get(a.get("row")), today, failed)}
    for e in events:
        if e["event"] in bw.ALERT:
            a = ask_doc(e, by_key.get(e["key"]), today)
            a["opened"] = (out.get(a["key"]) or a)["opened"]
            out[a["key"]] = a
    return out


def _source_of(err: str) -> str:
    return "ScaledMail" if err.startswith("ScaledMail") else "Mongo" if err.startswith("Mongo") else "Zapmail"


def digest_asks(open_now: dict, errors: list[str]) -> list:
    """The open alerts (urgent first) and one "broken" ask per source that could not be read. Pure."""
    asks = [dg.Ask(a["text"], a["step"], key=a["key"], urgent=a["urgent"])
            for a in sorted(open_now.values(), key=lambda a: (not a["urgent"], a["opened"], a["key"]))]
    for src in sorted({_source_of(e) for e in errors}):
        if src == "Mongo":
            text = "Billing watch could not read its saved state (Mongo), so no bill change was checked this run."
        else:
            text = f"Billing watch could not read {src}, so a failed or missed {src} bill may go unnoticed."
        asks.append(dg.Ask(text, "It tries again at its next check (6 a day); the error is in the thread.",
                           key=f"{SOURCE}:read:{src}", kind="broken"))
    return asks


def collect(errors: list[str]) -> list[dict]:
    rows: list[dict] = []
    from smartlead.zapmail_fleet import subscriptions_billing
    for r in asyncio.run(subscriptions_billing(days=0, errors=errors)):
        rows.append({"provider": "Zapmail", "id": r["subscription_id"],
                     "label": f"{r['mailboxes'] or '?'} {'Outlook' if r['provider'] == 'MICROSOFT' else 'Google'} "
                              f"{r['kind']} ({r['account']})",
                     "bills_on": r["bills_on"], "status": r["status"], "payment_failure": r.get("payment_failure"),
                     "inboxes": r["mailboxes"], "clients": r.get("clients") or [],
                     "price": r.get("price"), "domains": r.get("domains") or [],
                     "invoice_url": r.get("invoice_url") or ""})
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if configured():
            from smartlead.scaledmail_fleet import snapshot
            with ScaledMailClient() as sm:
                snap = snapshot(sm)
            for o in snap["orders"]:
                if o["status"] != "Active" and not o["billing_day"]:
                    continue
                rows.append({"provider": "ScaledMail", "id": o["id"], "label": o["description"].replace("*", "×"),
                             "bills_on": o["billing_day"], "status": o["status"], "payment_failure": None,
                             "inboxes": o["mailboxes"], "clients": o["clients"], "price": o.get("amount"),
                             "domains": sorted(d["domain"] for d in snap["domains"] if d.get("order_id") == o["id"]),
                             "invoice_url": "https://app.scaledmail.com"})
    except Exception as exc:  # noqa: BLE001
        errors.append(f"ScaledMail: {str(exc)[:150]}")
    return rows


def _col():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db["billing_watch"] if db is not None else None


def _history_col():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db["bill_history"] if db is not None else None


def history_for(domain: str, limit: int = 5) -> dict:
    col = _history_col()
    if col is None:
        return {"domain": domain, "lines": [], "error": "bill history unavailable (Mongo)"}
    recs = list(col.find({"domains": domain}, {"_id": 0}).sort("at", -1).limit(limit))
    return {"domain": domain, "lines": bw.history_lines(recs)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Did every inbox bill renew? (read-only)")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--history", metavar="DOMAIN", help="this domain's past bills (renewals, failures), newest first")
    args = ap.parse_args()
    if args.history:
        out = history_for(args.history.strip().lower())
        print(json.dumps(out) if args.json else "\n".join(out["lines"]) or "No bill history yet.")
        return 0
    errors: list[str] = []
    rows = collect(errors)
    col = _col()
    if col is None:
        # Without the saved state every run looks like a first run and stays
        # silent - say so instead of quietly reporting nothing.
        errors.append("Mongo unavailable — cannot compare with the last check, so no renewal alerts this run")
    doc = (col.find_one({"_id": STATE_ID}) or {}) if col is not None else {}
    prev = doc.get("rows") or {}
    today = date.today()
    # A provider that could not be read must not look "cancelled".
    failed = {"ScaledMail" if e.startswith("ScaledMail") else "Zapmail" for e in errors}
    events = [e for e in bw.compare(prev, rows, today)
              if not (e["event"] == "gone" and e["provider"] in failed)]
    headline, thread = bw.format_events(events, today)
    if errors:
        if not headline:
            headline = [f"⚠️ Inbox billing watch, {today.day} {today:%b}"]
        headline.append(":warning: could not read: " + "; ".join(errors))
    text = "\n".join(headline) + ("\n\n" + "\n\n".join(thread) if thread else "")
    posted = saved = in_digest = False
    to_post = bool(text) and (any(e["event"] in bw.ALERT for e in events) or bool(errors))
    open_now = None
    if not args.dry:
        if dg.on():
            # Every run hands over the full list of open alerts (the digest replaces the last run's).
            open_now = open_asks(doc.get("open") or {}, events, rows, today, failed)
            in_digest = dg.report(SOURCE, digest_asks(open_now, errors), [], [text] if text else [])
        if to_post and not in_digest:
            from smartlead import notify
            token = os.getenv("SLACK_BOT_TOKEN", "")
            channel = notify.infra_channel("RENEWAL_REVIEW_CHANNEL", "ZAPMAIL_NOTIFY_CHANNEL")
            posted = bool(token and channel and notify.post_with_thread(token, channel, headline, thread))
        if posted or in_digest or not to_post:
            # Only now is it told: save what was seen and alerted, and add the events to the bill history.
            new_state = bw.remember(prev, rows, events, today)
            for k, p in prev.items():          # keep unreadable providers' rows as they were
                if k not in new_state and p["provider"] in failed:
                    new_state[k] = p
            if col is not None:
                fields = {"rows": new_state, "at": today.isoformat()}
                if open_now is not None:
                    fields["open"] = open_now
                col.update_one({"_id": STATE_ID}, {"$set": fields}, upsert=True)
            records = bw.history_records(events, rows, today)
            hist = _history_col() if records else None
            if hist is not None:
                hist.insert_many(records)
            saved = True
        else:
            print("  [billing-watch] Slack did not accept the post: state not saved, the same alerts go out next run",
                  file=sys.stderr)
    out = {"rows": len(rows), "events": events, "headline": headline, "thread": thread, "text": text, "errors": errors,
           "posted": posted, "saved": saved, "first_run": not prev, "mongo": col is not None, "digest": in_digest}
    print(json.dumps(out, default=str) if args.json else (text or f"No billing changes ({len(rows)} subscriptions / orders watched)."))
    return 1 if (to_post and not args.dry and not (posted or in_digest)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
