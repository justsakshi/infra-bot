"""The team's setup standard for a new sending inbox, by type. Pure: no network.

Spec: docs/superpowers/specs/2026-10-13-inbox-intake-design.md. Values live in
inbox_standard.json so they change without code. Read-back cannot see
daily_rampup, auto-adjust or weekdays-only (Smartlead does not return them).
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path

STANDARD_PATH = Path(os.getenv(
    "INBOX_STANDARD_FILE", Path(__file__).resolve().parent.parent / "inbox_standard.json"))
SETUP_TAG = "Bot-Setup"
READY_REPUTATION = 90
DENSE_OUTLOOK_AT = 10          # Outlook inboxes on one domain from which it counts as a shared tenant


def load() -> dict:
    with open(STANDARD_PATH, encoding="utf-8") as fh:
        return json.load(fh)


def inbox_kind(account: dict, inboxes_on_domain: int) -> str:
    t = str(account.get("type") or "").upper()
    host = str(account.get("smtp_host") or "").lower()
    if t == "GMAIL" or "gmail" in host or "google" in host:
        return "google"
    if t == "OUTLOOK" or "office365" in host or "outlook" in host:
        return "outlook_dense" if inboxes_on_domain >= DENSE_OUTLOOK_AT else "outlook"
    return "smtp"


def standard_for(kind: str) -> dict:
    s = load()
    return {**s["types"][kind], **s["every_type"]}


def account_fields(kind: str, vendor: str) -> dict:
    out = {"max_email_per_day": standard_for(kind)["max_email_per_day"]}
    if vendor in load().get("no_tracking_vendors", []):
        out["custom_tracking_url"] = ""
    return out


def warmup_args(kind: str) -> dict:
    s = standard_for(kind)
    return {"enabled": True, "total_per_day": s["total_warmup_per_day"], "daily_rampup": s["daily_rampup"],
            "reply_rate": s["reply_rate"], "auto_adjust": s["auto_adjust"], "weekdays_only": s["weekdays_only"]}


def _int(v) -> int | None:
    try:
        return int(str(v).rstrip("%"))
    except (TypeError, ValueError):
        return None


def drift(account: dict, kind: str, vendor: str) -> list[str]:
    """What no longer matches the type's standard (limit, warmup, reply rate, tracking)."""
    s = standard_for(kind)
    w = account.get("warmup_details") or {}
    out = []
    if str(w.get("status") or "").upper() != "ACTIVE":
        return ["warmup off"]
    if _int(account.get("message_per_day")) != s["max_email_per_day"]:
        out.append(f"daily limit {account.get('message_per_day')}, want {s['max_email_per_day']}")
    wmax = _int(w.get("max_email_per_day") or w.get("warmup_max_count"))
    if wmax != s["total_warmup_per_day"]:
        out.append(f"warmup {wmax}/day, want {s['total_warmup_per_day']}")
    if _int(w.get("reply_rate")) != s["reply_rate"]:
        out.append(f"reply rate {w.get('reply_rate')}%, want {s['reply_rate']}%")
    if vendor in load().get("no_tracking_vendors", []) and account.get("custom_tracking_domain"):
        out.append(f"tracking domain {account['custom_tracking_domain']}, want none")
    return out


def readback_mismatches(account: dict, kind: str, vendor: str, written: dict) -> list[str]:
    """drift() plus the signature and client we just wrote."""
    from smartlead.inbox_setup import signature_text
    out = drift(account, kind, vendor)
    if "signature" in written and signature_text(written["signature"]) != signature_text(account.get("signature") or ""):
        out.append("signature differs")
    if "client_id" in written and account.get("client_id") != written["client_id"]:
        out.append(f"client {account.get('client_id')}, want {written['client_id']}")
    return out


def warmup_started(account: dict) -> date | None:
    w = account.get("warmup_details") or {}
    iso = w.get("warmup_created_at") or w.get("created_at")     # list read / single-inbox read
    return date.fromisoformat(str(iso)[:10]) if iso else None


def ready_date(account: dict, kind: str) -> date | None:
    start = warmup_started(account)
    return start + timedelta(days=standard_for(kind)["ready_after_days"]) if start else None


def readiness(account: dict, kind: str, today: date) -> str:
    when = ready_date(account, kind)
    if when is None:
        return "unknown"
    if today < when:
        return "warming"
    rep = _int((account.get("warmup_details") or {}).get("warmup_reputation"))
    return "ready" if rep is not None and rep >= READY_REPUTATION else "low_reputation"
