#!/usr/bin/env python3
"""Renewal review: before each Zapmail / ScaledMail bill, KEEP / RETIRE / CHECK
every inbox on it, from placement tests + Smartlead. READ-ONLY.

    python renewal_review.py                  # bills in the next 3 days
    python renewal_review.py --days 14 --json
    python renewal_review.py --post           # to RENEWAL_REVIEW_CHANNEL (else ZAPMAIL_NOTIFY_CHANNEL)

Rules: smartlead/renewal_review.py. Retiring is a separate, approver-only
button (Zapmail) — this script never changes anything.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, timedelta

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

import httpx

from smartlead import renewal_review as rr
from smartlead.accounts import discover_accounts
from smartlead.config import SMARTDELIVERY_BASE_URL

SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}
PAST = {"DARLEAN", "BELARDI WONG", "MYTHIC"}     # past clients' Smartlead accounts
TESTS_PER_ACCOUNT = 10


def _get(url, key, **params):
    import time
    for attempt in range(5):           # Smartlead rate-limits hard: back off on 429
        r = httpx.get(url, params={"api_key": key, **params}, headers=UA, timeout=120)
        if r.status_code != 429:
            break
        time.sleep(10 * (attempt + 1))
    r.raise_for_status()
    return r.json()


def smartlead_facts(errors: list[str]) -> dict[str, dict]:
    """{email: {in_smartlead, connected, reputation, pct, tested_on, campaigns}}."""
    from placement_report import active_attachments
    facts: dict[str, dict] = {}
    for acc in discover_accounts():
        if acc.name.upper() in PAST:
            continue
        key = acc.api_key
        try:
            off = 0
            while True:
                page = _get(f"{SL}/email-accounts/", key, offset=off, limit=100)
                for a in page:
                    w = a.get("warmup_details") or {}
                    facts[a["from_email"].lower()] = {
                        "in_smartlead": True, "account": acc.name,
                        "connected": bool(a.get("is_smtp_success") and a.get("is_imap_success")),
                        "reputation": w.get("warmup_reputation"), "pct": None, "tested_on": None, "campaigns": []}
                if len(page) < 100:
                    break
                off += 100
            for email, camps in active_attachments(key).items():
                if email in facts:
                    facts[email]["campaigns"] = camps
            r = httpx.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report", params={"api_key": key},
                           json={"limit": 50}, headers=UA, timeout=60)
            tests = sorted((t for t in r.json() if t.get("status") == "COMPLETED"),
                           key=lambda t: t["created_at"], reverse=True)[:TESTS_PER_ACCOUNT]
            seen: set[str] = set()
            for t in tests:      # newest first: the first result per inbox wins
                for e in _get(f"{SMARTDELIVERY_BASE_URL}/spam-test/report/{t['spam_test_id']}/sender-account-wise", key):
                    em = e["email"].lower()
                    if em in seen or em not in facts:
                        continue
                    folders = [str((d.get("reply") or {}).get("mail_folder", "")).strip().lower()
                               for d in e.get("details") or []]
                    folders = [f for f in folders if f]
                    if not folders:
                        continue
                    seen.add(em)
                    spam = sum(f in ("spam", "junk") for f in folders)
                    facts[em]["pct"] = round(100.0 * (len(folders) - spam) / len(folders), 1)
                    facts[em]["tested_on"] = t["created_at"][:10]
        except Exception as exc:  # noqa: BLE001 - one account must not hide the others
            errors.append(f"Smartlead {acc.name}: {str(exc)[:150]}")
    return facts


def zapmail_bills(days: int, errors: list[str]) -> list[dict]:
    from smartlead.zapmail_fleet import subscriptions_billing
    rows = asyncio.run(subscriptions_billing(days=days, errors=errors))
    horizon = (date.today() + timedelta(days=days)).isoformat()
    out = []
    for r in rows:
        if not r["bills_on"] or r["bills_on"] > horizon:
            continue
        label = f"{r['mailboxes'] or '?'} {'Outlook' if r['provider'] == 'MICROSOFT' else 'Google'} {r['kind']}"
        bill = {"provider": "Zapmail", "label": label, "bills_on": r["bills_on"], "price": r["price"],
                "mailboxes": r["mailboxes"], "inboxes": r.get("inboxes") or [], "clients": r.get("clients") or [],
                "subscription_id": r["subscription_id"], "account": r["account"]}
        if r.get("payment_failure"):
            bill["note"] = f"payment failed: {r['payment_failure']}"
        if r["kind"] == "pre-warmed":
            bill["note"] = "pre-warmed slots: Zapmail does not list which inboxes sit on this plan; review them by domain"
        out.append(bill)
    return out


def scaledmail_bills(days: int, errors: list[str]) -> list[dict]:
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if not configured():
            return []
        from smartlead.scaledmail_fleet import snapshot
        horizon = (date.today() + timedelta(days=days)).isoformat()
        with ScaledMailClient() as sm:
            snap = snapshot(sm)
            due = [o for o in snap["orders"] if o["billing_day"] and o["billing_day"] <= horizon]
            out = []
            for o in due:
                inboxes = []
                for d in snap["domains"]:
                    if d["order_id"] == o["id"] and d.get("domain_id"):
                        inboxes += [m.get("email", "").lower() for m in sm.mailboxes(d["domain_id"])]
                out.append({"provider": "ScaledMail", "label": o["description"].replace("*", "×"),
                            "bills_on": o["billing_day"], "price": o["amount"], "inboxes": inboxes,
                            "clients": o["clients"], "order_id": o["id"]})
            return out
    except Exception as exc:  # noqa: BLE001
        errors.append(f"ScaledMail: {str(exc)[:150]}")
        return []


def run(days: int) -> dict:
    errors: list[str] = []
    bills = zapmail_bills(days, errors) + scaledmail_bills(days, errors)
    n_before = len(errors)
    facts = smartlead_facts(errors) if bills else {}
    complete = len(errors) == n_before        # every Smartlead account was read
    today = date.today()
    reviews = sorted((rr.review_bill(b, facts, today, complete) for b in bills), key=lambda r: r["bills_on"])
    from smartlead.zapmail_clients import infer_client
    for rv in reviews:          # whose inbox it is: retiring is per client
        for row in rv["rows"]:
            row["client"] = infer_client(row["email"].split("@")[1], rv.get("account"))
    text = rr.format_review(reviews, days)
    if errors:
        text += "\n:warning: " + "; ".join(errors)
    return {"days": days, "reviews": reviews, "errors": errors, "text": text}


def main() -> int:
    ap = argparse.ArgumentParser(description="Review inboxes before their bill (read-only)")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--post", action="store_true")
    args = ap.parse_args()
    try:
        res = run(args.days)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}) if args.json else f"ERROR: {exc}")
        return 0 if args.json else 1
    if args.post and res["reviews"]:
        from smartlead.notify import _post
        token = os.getenv("SLACK_BOT_TOKEN", "")
        channel = os.getenv("RENEWAL_REVIEW_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL", "")
        res["posted"] = bool(token and channel and _post(token, channel, res["text"][:39000]))
    print(json.dumps(res, default=str) if args.json else res["text"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
