"""PL LinkedIn (Waalaxy) replies, from the n8n reply log.

Waalaxy's public API lists campaigns and imports prospects; it has no invite,
accept or reply counts and no message text (docs.waalaxy.com, checked 30 Oct
2026). So on 30 Oct the n8n "pl - LinkedIn Reply Router (wallaxy)"
(ErgTadfS9Or2dgyy) got a step that appends every reply to the "Waalaxy
Responses" tab of the Follow ups log sheet, and this module reads it:
replies and positives per campaign for Campaign History, and the replies
themselves for the Show replies button. Replies before 30 Oct are not in it.

Accepts: a "CRM Sync" step placed after the invite in a Waalaxy campaign posts
each prospect who reaches it to the n8n webhook pl-waalaxy-crm-sync
(HARvEVpKftJKhew2), which keeps one row per prospect in "Waalaxy Accepts".
Invites sent stay unknown (only prospects who reach the step are sent).
"""
from __future__ import annotations

import os
from datetime import datetime

from smartlead.reply_view import clean_text, in_window

LOG_SHEET_ID = os.getenv("WAALAXY_LOG_SHEET_ID", "1TaOO0Znvpko10VR_5vSDqRZedleGA9HSZHsdiQ5lGRk")
LOG_TAB = "Waalaxy Responses"
ACCEPTS_TAB = "Waalaxy Accepts"
CLIENT = "Precise Leads"
_LABELS = {"POSITIVE": "Positive", "NEUTRAL": "Neutral", "UNKNOWN": "Unclear",
           "WRONG_PERSON": "Wrong person", "NOT_INTERESTED": "Not interested"}


def read_log(tab: str = LOG_TAB) -> list[list[str]]:
    from smartlead.sheets import SheetsWriter
    return SheetsWriter(LOG_SHEET_ID)._sh.worksheet(tab).get_all_values()


def _rows(values: list[list[str]]) -> list[dict]:
    if not values:
        return []
    head = [h.strip() for h in values[0]]
    return [dict(zip(head, raw + [""] * (len(head) - len(raw)))) for raw in values[1:]]


def _records(values: list[list[str]]) -> list[dict]:
    if not values:
        return []
    head = [h.strip() for h in values[0]]
    out = []
    for raw in values[1:]:
        r = dict(zip(head, raw + [""] * (len(head) - len(raw))))
        if r.get("Campaign", "").strip() and r.get("Received at", "").strip():
            out.append(r)
    return out


def history_rows(values: list[list[str]], accepts: list[list[str]] | None = None) -> list[dict]:
    """One Campaign History row per campaign in either log: replies and
    positives to date, and accepted when the accepts log has the campaign.
    The campaign name is its id (Waalaxy's webhook sends the list name)."""
    by: dict[str, list[dict]] = {}
    for r in _records(values):
        by.setdefault(r["Campaign"].strip(), []).append(r)
    acc: dict[str, int] = {}
    for r in _rows(accepts or []):
        name = r.get("Campaign", "").strip()
        if name and r.get("Connected on", "").strip():
            acc[name] = acc.get(name, 0) + 1
    rows = []
    for name in sorted(set(by) | set(acc)):
        rs = by.get(name, [])
        first = min(r["Received at"] for r in rs)[:10] if rs else ""
        rows.append({"client": CLIENT, "channel": "LinkedIn", "platform": "Waalaxy",
                     "campaign": name, "campaign_id": name, "status": "",
                     "launch_date": first, "total_leads": None,
                     "life_sent": None, "life_bounced": None, "life_invites": None,
                     "life_accepted": acc.get(name) if accepts is not None and name in acc else None,
                     "life_replies": len(rs),
                     "life_positive": sum(1 for r in rs
                                          if r.get("Category", "").upper() == "POSITIVE"),
                     "life_not_interested": None, "life_wrong_person": None})
    return rows


def replies(values: list[list[str]], campaign: str, start: datetime | None,
            end: datetime | None) -> list[dict]:
    """The log's replies for one campaign inside the window, for the button."""
    want = campaign.strip().lower()
    out = []
    for r in _records(values):
        if r["Campaign"].strip().lower() != want or not in_window(r["Received at"], start, end):
            continue
        cat = r.get("Category", "").upper()
        out.append({"lead": r.get("Profile URL") or r.get("Full Name"),
                    "name": r.get("Full Name", ""), "company": r.get("Company", ""),
                    "category": _LABELS.get(cat, cat.title()), "time": r["Received at"],
                    "text": clean_text(r.get("Reply from Lead", ""))})
    return out
