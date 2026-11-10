"""Which inboxes may be handed to a campaign: the evidence gate on top of the capacity rule.

The "All Inboxes" tab says FREE or BUSY for every inbox, and every tool that picks inboxes reads it
(precise-automator's inbox recommender, people, other agents). Until 10 Nov 2026 FREE meant: connected,
capacity left, warm-up reputation 90%+, a fresh "inbox" in the hand-kept test grid, DNS fine. It did not
look at the three things that showed the Google batch was in spam:

  * a tag starting "DO NOT USE" in Smartlead (a person's, or the v2 job's);
  * the Smart Delivery v2 verdict (Mongo ``v2_verdicts``, written by v2_weekly.py);
  * a Spamhaus DBL or URIBL listing of the sending domain (Mongo ``blacklist_checks``).

That day 88 inboxes were FREE: 49 on listed domains, 83 with no v2 result. The 60 tagged ones were BUSY only
because their grid rows happened to be stale.

Always BUSY: tagged DO NOT USE (``do_not_use``), newest judged v2 verdict FAIL (``failed_v2``).
BUSY when the switch is on, a note until then:
  AVAILABILITY_BLOCK_LISTED=true     listed sending domain -> ``blocklisted``
  AVAILABILITY_REQUIRE_V2_PASS=true  no v2 pass in the last 8 days -> ``no_v2_pass``
The notes go in the Notes column, so the warning is on the sheet before it blocks anything.

``gate`` is pure. ``load_gate_data`` reads Mongo and never raises: without it the tag rule still holds
(tags come from Smartlead) and every row gets a note saying the rest was not read.
"""
from __future__ import annotations

import os
from datetime import date

DO_NOT_USE = "do not use"            # tag prefix, any case (same as smartlead/fleet.py)
V2_FRESH_DAYS = 8                    # a weekly test, plus a day of slack (same as the renewal review)
JUDGED = ("PASS", "WATCH", "FAIL")   # INCOMPLETE and anything else is not a verdict
SENDER_LISTS = ("spamhaus", "uribl")  # lists that judge the sending domain (SURBL lists links in the copy)
NOT_READ = "v2 results and blocklists not read today"


def tag_names(account: dict) -> list[str]:
    """Tag names of a Smartlead email account (dicts with tag_name / name; plain strings kept)."""
    out = []
    for t in (account or {}).get("tags") or []:
        name = str(t.get("tag_name") or t.get("name") or "") if isinstance(t, dict) else str(t or "")
        if name.strip():
            out.append(name.strip())
    return out


def is_do_not_use(tags) -> bool:
    return any(str(t).strip().lower().startswith(DO_NOT_USE) for t in tags or [])


def _day(iso: str) -> str:
    try:
        d = date.fromisoformat(str(iso)[:10])
    except ValueError:
        return str(iso)
    return f"{d.day} {d.strftime('%b')}"


def gate(tags, v2: dict | None, listed: bool | None, today: date, *, checked: bool = True,
         block_listed: bool = False, require_v2_pass: bool = False) -> tuple[list[str], list[str]]:
    """(busy reasons, notes) for one inbox.

    ``v2`` = its newest judged verdict {"verdict", "night"} or None; ``listed`` = its domain is on
    Spamhaus DBL / URIBL (None = no blocklist row for it). ``checked`` False = Mongo was not read:
    only the tag rule applies and nothing is claimed about v2 or blocklists.
    """
    reasons, notes = [], []
    if is_do_not_use(tags):
        reasons.append("do_not_use")
    if not checked:
        return reasons, notes
    verdict = str((v2 or {}).get("verdict") or "").upper()
    night = str((v2 or {}).get("night") or "")
    if verdict == "FAIL":
        reasons.append("failed_v2")
    else:
        fresh = False
        if verdict == "PASS":
            try:
                fresh = (today - date.fromisoformat(night[:10])).days <= V2_FRESH_DAYS
            except ValueError:
                fresh = False
        if not fresh:
            why = (f"v2 watch on {_day(night)}" if verdict == "WATCH"
                   else f"last v2 pass {_day(night)}, older than {V2_FRESH_DAYS} days" if verdict == "PASS"
                   else "no v2 result")
            if require_v2_pass:
                reasons.append("no_v2_pass")
            notes.append(why)
    if listed:
        if block_listed:
            reasons.append("blocklisted")
        notes.append("domain on Spamhaus DBL or URIBL")
    return reasons, notes


def newest_judged(rows) -> dict[str, dict]:
    """{email: {"verdict", "night"}}: each inbox's newest v2 row that is a verdict. Pure."""
    out: dict[str, dict] = {}
    for d in rows or []:
        email, night = str(d.get("email") or "").lower(), str(d.get("night") or "")
        verdict = str(d.get("verdict") or "").upper()
        if not email or verdict not in JUDGED:
            continue
        if email not in out or night > out[email]["night"]:
            out[email] = {"verdict": verdict, "night": night}
    return out


def listed_domains(rows) -> set[str]:
    """Domains whose NEWEST blocklist row lists them on Spamhaus DBL or URIBL. Pure."""
    newest: dict[str, dict] = {}
    for d in rows or []:
        dom, day = str(d.get("domain") or "").lower(), str(d.get("date") or "")
        if dom and (dom not in newest or day > str(newest[dom].get("date") or "")):
            newest[dom] = d
    return {dom for dom, d in newest.items()
            if any(k in str(z).lower() for z in d.get("listed_on") or [] for k in SENDER_LISTS)}


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    return MongoClient(uri, serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]


def load_gate_data() -> dict:
    """{"v2": {email: verdict row}, "listed": {domain}, "checked": bool, "error": str}. Never raises."""
    from smartlead.config import BLACKLIST_COLLECTION
    try:
        db = _db()
        v2 = newest_judged(db["v2_verdicts"].find({}, {"_id": 0, "email": 1, "night": 1, "verdict": 1}))
        listed = listed_domains(db[BLACKLIST_COLLECTION].find({}, {"_id": 0, "domain": 1, "date": 1, "listed_on": 1}))
        return {"v2": v2, "listed": listed, "checked": True, "error": ""}
    except Exception as exc:  # noqa: BLE001 - the tag rule still holds; the sheet says the rest was not read
        print(f"  [Usable] v2 verdicts and blocklists not read: {str(exc)[:150]}")
        return {"v2": {}, "listed": set(), "checked": False, "error": str(exc)[:150]}


def apply(item: dict, data: dict | None, today: date, *, block_listed: bool, require_v2_pass: bool) -> list[str]:
    """Fill the inbox row's v2 and note fields and return its extra busy reasons.
    ``data`` None (callers that never loaded it) = the tag rule only, no notes."""
    email = str(item.get("email") or "").lower()
    checked = bool(data and data.get("checked"))
    v2 = (data or {}).get("v2", {}).get(email)
    listed = email.split("@")[-1] in (data or {}).get("listed", set()) if checked else None
    reasons, notes = gate(item.get("tags"), v2, listed, today, checked=checked,
                          block_listed=block_listed, require_v2_pass=require_v2_pass)
    if data is not None and not checked:
        notes.append(NOT_READ)
    item["v2_status"] = (v2 or {}).get("verdict", "")
    item["v2_date"] = (v2 or {}).get("night", "")
    item["use_notes"] = "; ".join(notes)
    return reasons
