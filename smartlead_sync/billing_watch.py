#!/usr/bin/env python3
"""Watch every inbox subscription / order on Zapmail and ScaledMail and say
when one did NOT renew, failed payment, was cancelled, or appeared out of
nowhere. READ-ONLY on the providers; remembers state in Mongo
(``billing_watch``) so each change is reported once.

    python billing_watch.py            # compare, post changes, save state
    python billing_watch.py --dry      # print only (no post, no save)
    python billing_watch.py --json

Posts to RENEWAL_REVIEW_CHANNEL (else ZAPMAIL_NOTIFY_CHANNEL). Rules:
smartlead/billing_watch.py.
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

STATE_ID = "state"


def collect(errors: list[str]) -> list[dict]:
    rows: list[dict] = []
    from smartlead.zapmail_fleet import subscriptions_billing
    for r in asyncio.run(subscriptions_billing(days=0, errors=errors)):
        rows.append({"provider": "Zapmail", "id": r["subscription_id"],
                     "label": f"{r['mailboxes'] or '?'} {'Outlook' if r['provider'] == 'MICROSOFT' else 'Google'} "
                              f"{r['kind']} ({r['account']})",
                     "bills_on": r["bills_on"], "status": r["status"], "payment_failure": r.get("payment_failure"),
                     "inboxes": r["mailboxes"], "clients": r.get("clients") or [],
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
                             "inboxes": o["mailboxes"], "clients": o["clients"],
                             "invoice_url": "https://app.scaledmail.com"})
    except Exception as exc:  # noqa: BLE001
        errors.append(f"ScaledMail: {str(exc)[:150]}")
    return rows


def _col():
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    return db["billing_watch"] if db is not None else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Did every inbox bill renew? (read-only)")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    errors: list[str] = []
    rows = collect(errors)
    col = _col()
    if col is None:
        # Without the saved state every run looks like a first run and stays
        # silent - say so instead of quietly reporting nothing.
        errors.append("Mongo unavailable — cannot compare with the last check, so no renewal alerts this run")
    prev = ((col.find_one({"_id": STATE_ID}) or {}).get("rows") or {}) if col is not None else {}
    today = date.today()
    # A provider that could not be read must not look "cancelled".
    failed = {"ScaledMail" if e.startswith("ScaledMail") else "Zapmail" for e in errors}
    events = [e for e in bw.compare(prev, rows, today)
              if not (e["event"] == "gone" and e["provider"] in failed)]
    text = bw.format_events(events)
    if errors:
        text = (text + "\n" if text else "*💳 Inbox billing watch*\n") + ":warning: could not read: " + "; ".join(errors)
    posted = False
    if not args.dry:
        new_state = bw.remember(prev, rows, events, today)
        for k, p in prev.items():          # keep unreadable providers' rows as they were
            if k not in new_state and p["provider"] in failed:
                new_state[k] = p
        if col is not None:
            col.update_one({"_id": STATE_ID}, {"$set": {"rows": new_state, "at": today.isoformat()}}, upsert=True)
        if text and (any(e["event"] in bw.ALERT for e in events) or errors):
            from smartlead.notify import _post
            token = os.getenv("SLACK_BOT_TOKEN", "")
            channel = os.getenv("RENEWAL_REVIEW_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL", "")
            posted = bool(token and channel and _post(token, channel, text[:39000]))
    out = {"rows": len(rows), "events": events, "text": text, "errors": errors, "posted": posted,
           "first_run": not prev, "mongo": col is not None}
    print(json.dumps(out, default=str) if args.json else (text or f"No billing changes ({len(rows)} subscriptions / orders watched)."))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
