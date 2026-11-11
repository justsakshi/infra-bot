"""The buying rule: no batch of a vendor + inbox type until six inboxes of that kind have passed twice.

Why (11 Nov 2026): 54 Google inboxes were bought in one go and every one landed in Google spam; a six-inbox trial
tested from its first week would have shown that for a tenth of the money. An order is fine when the kind is
proven, or when it is itself no bigger than a trial.

A kind is (vendor, inbox type), for example Zapmail + Google, across all clients: what a vendor's Google inboxes do
for one client is the best evidence there is for the next. An inbox counts as proven when its NEWEST judged result
is a pass and it has two passes at least ``GAP_DAYS`` apart (two different weekly tests). Pure: no network.
"""
from __future__ import annotations

from datetime import date

TRIAL = 6          # inboxes in a trial, and how many must pass twice to prove a kind
GAP_DAYS = 5       # two passes this far apart are two weekly tests (a Monday and the next Saturday)
JUDGED = ("PASS", "WATCH", "FAIL")
_VENDOR = {"scaledmail": "ScaledMail", "zapmail": "Zapmail", "inboxkit": "Inboxkit"}
_TYPE = {"google": "google", "gmail": "google", "googleworkspace": "google", "outlook": "outlook",
         "microsoft": "outlook", "microsoft365": "outlook", "smtp": "smtp"}
_TYPE_NAME = {"google": "Google", "outlook": "Outlook", "smtp": "SMTP"}


def _key(text) -> str:
    return "".join(ch for ch in str(text or "").lower() if ch.isalnum())


def kind_of(asset: dict) -> tuple[str, str] | None:
    """(vendor, type) of a tracker INBOX row, spelling ignored; None for a domain row or an unknown type."""
    if str(asset.get("type") or "").upper() != "INBOX":
        return None
    vendor, typ = _key(asset.get("provider")), _TYPE.get(_key(asset.get("workspace")))
    return (vendor, typ) if vendor and typ else None


def _proven(rows: list[dict]) -> bool:
    judged = sorted((str(r.get("night") or "")[:10], str(r.get("verdict") or "").upper()) for r in rows
                    if str(r.get("verdict") or "").upper() in JUDGED and r.get("night"))
    if not judged or judged[-1][1] != "PASS":
        return False
    passes = [date.fromisoformat(n) for n, v in judged if v == "PASS"]
    return (passes[-1] - passes[0]).days >= GAP_DAYS


def check(vendor: str, inbox_type: str, count: int, assets: list[dict], verdicts: list[dict]) -> dict:
    """May ``count`` inboxes of this kind be ordered? ``assets``: tracker rows; ``verdicts``: v2_verdicts rows
    ({email, night, verdict}). Returns {vendor, type, count, proven, tested, ok, limit, why}; ``limit`` is the most
    to buy now (None = no limit)."""
    want = (_key(vendor), _TYPE.get(_key(inbox_type)) or _key(inbox_type))
    mine = {str(a.get("name") or "").strip().lower() for a in assets or [] if kind_of(a) == want}
    by: dict[str, list[dict]] = {}
    for r in verdicts or []:
        e = str(r.get("email") or "").strip().lower()
        if e in mine:
            by.setdefault(e, []).append(r)
    proven = sum(1 for rows in by.values() if _proven(rows))
    name = f"{_VENDOR.get(want[0], str(vendor).strip())} {_TYPE_NAME.get(want[1], str(inbox_type).strip())}"
    if proven >= TRIAL:
        ok, limit = True, None
        why = f"{proven} {name} inboxes have passed their placement test twice"
    else:
        ok, limit = int(count) <= TRIAL, TRIAL
        why = (f"only {proven} {name} inbox{'es have' if proven != 1 else ' has'} passed their placement test twice "
               f"({TRIAL} needed): buy at most {TRIAL} as a trial, test them weekly from the first week, and buy "
               f"more once they pass twice")
    return {"vendor": want[0], "type": want[1], "count": int(count), "proven": proven, "tested": len(by),
            "ok": ok, "limit": limit, "why": why}
