"""Spamhaus DBL / URIBL status of a client's domains, for the classic placement
report and kill warnings. Read-only.

blacklist_monitor.py writes one `blacklist_checks` doc per domain per check day:
{domain, date, listed_on: ["Spamhaus DBL (127.0.1.2)", ...], lookup_errors,
inconclusive_zones, clients}. Its `clients` field holds Smartlead ACCOUNT names
(PRECISE_LEADS holds Melior too), so callers pass a client's domains themselves,
taken from the inboxes in Smartlead now.

A classic seed test never sees a blocklist. A client with a listed domain never
gets an all-clear (audit REACH-5, 7 Nov 2026: the classic posts said every
mailbox landed in the inbox while several of that account's domains were on DBL).
"""
from __future__ import annotations

import os
import re
from datetime import date, timedelta

SERIOUS = ("spamhaus", "uribl")   # the lists that judge the sending domain (SURBL judges links)
STALE_DAYS = 21                   # an older check says nothing about today


def _labels(values) -> list[str]:
    """'Spamhaus DBL (127.0.1.2)' -> 'Spamhaus DBL', keeping only the serious lists."""
    return [re.sub(r"\s*\(.*\)\s*$", "", str(v)) for v in values or []
            if any(k in str(v).lower() for k in SERIOUS)]


def latest_by_domain(docs) -> dict[str, dict]:
    """The newest check per domain."""
    out: dict[str, dict] = {}
    for d in docs or []:
        dom = str(d.get("domain") or "").strip().lower()
        if dom and (dom not in out or str(d.get("date") or "") > str(out[dom].get("date") or "")):
            out[dom] = d
    return out


def _unsure(doc: dict) -> bool:
    """A check that could not read DBL or URIBL for this domain."""
    zones = list(doc.get("lookup_errors") or []) + list(doc.get("inconclusive_zones") or [])
    extra = doc.get("unknown")
    if isinstance(extra, (list, tuple)):
        zones += list(extra)
    elif extra:
        return True
    return bool(_labels(zones))


def _fresh(day: str, today: date) -> bool:
    try:
        return (today - date.fromisoformat(day[:10])).days <= STALE_DAYS
    except ValueError:
        return False


def summarise(domains, latest: dict[str, dict] | None, today: date) -> dict:
    """Pure. ``latest`` = latest_by_domain(...), or None when the checks could not be read.

    {read, domains (count), listed {domain: [list names]}, listed_dates, checked_dates,
    unknown [domains with no usable check in STALE_DAYS]}."""
    doms = sorted({str(d).strip().lower() for d in domains or [] if d})
    out = {"read": latest is not None, "domains": len(doms), "listed": {}, "listed_dates": [],
           "checked_dates": [], "unknown": [], "notes": {}}
    if latest is None:
        out["unknown"] = doms
        return out
    listed_dates, checked_dates = set(), set()
    for dom in doms:
        doc = latest.get(dom)
        day = str((doc or {}).get("date") or "")[:10]
        if not doc or not _fresh(day, today):
            out["unknown"].append(dom)
            continue
        hits = _labels(doc.get("listed_on"))
        if hits:
            out["listed"][dom] = hits
            listed_dates.add(day)
            checked_dates.add(day)
        elif _unsure(doc):
            out["unknown"].append(dom)
        else:
            checked_dates.add(day)
    out["listed_dates"], out["checked_dates"] = sorted(listed_dates), sorted(checked_dates)
    return out


def _collection():
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        from smartlead.config import BLACKLIST_COLLECTION, HEALTH_HISTORY_DB
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client[HEALTH_HISTORY_DB][BLACKLIST_COLLECTION]
    except Exception as exc:  # noqa: BLE001 - the caller says "unknown"
        print(f"  [Blocklist] Mongo unavailable: {exc}")
        return None


def read_latest(domains, today: date, collection=None) -> dict[str, dict] | None:
    """Newest check per domain from Mongo, or None when it cannot be read."""
    col = collection if collection is not None else _collection()
    if col is None:
        return None
    try:
        since = (today - timedelta(days=STALE_DAYS)).isoformat()
        docs = col.find({"domain": {"$in": sorted({str(d).lower() for d in domains or []})},
                         "date": {"$gte": since}},
                        {"_id": 0, "domain": 1, "date": 1, "listed_on": 1, "lookup_errors": 1,
                         "inconclusive_zones": 1, "unknown": 1})
        return latest_by_domain(list(docs))
    except Exception as exc:  # noqa: BLE001
        print(f"  [Blocklist] read failed: {exc}")
        return None


def _day(iso: str) -> str:
    d = date.fromisoformat(iso[:10])
    return f"{d.day} {d:%b}"


def when(dates: list[str]) -> str:
    """'check of 2 Nov', or 'newest check 7 Nov' when the checks span days."""
    ds = sorted(set(dates))
    if not ds:
        return ""
    return f"check of {_day(ds[0])}" if len(ds) == 1 else f"newest check {_day(ds[-1])}"


def headline_line(s: dict | None, whose: str = "its") -> str | None:
    """One line under the result, or None when there is nothing to say up top."""
    if s is None:
        return None
    if not s["read"]:
        return "Blocklist status unknown: could not read the blocklist checks."
    n, total = len(s["listed"]), s["domains"]
    if n:
        who = f"{whose[:1].upper()}{whose[1:]} only domain is" if total == 1 else f"{n} of {whose} {total} domains"
        return f"{who} on Spamhaus DBL or URIBL ({when(s['listed_dates'])})"
    if total and len(s["unknown"]) == total:
        return f"Blocklist status unknown: no check of {whose} domains in the last {STALE_DAYS} days."
    return None


def detail(s: dict | None, whose: str = "its") -> tuple[str | None, str | None]:
    """(thread part listing the listed / unchecked domains, one-line note for the footer)."""
    if s is None:
        return None, None
    if not s["read"]:
        return None, "Blocklists: could not read the checks, so this post says nothing about Spamhaus DBL or URIBL."
    lines = []
    if s["listed"]:
        lines.append(f"*On Spamhaus DBL or URIBL* ({when(s['listed_dates'])})")
        for dom, hits in sorted(s["listed"].items()):
            note = s.get("notes", {}).get(dom)
            lines.append(f"• {dom} · {', '.join(hits)}" + (f" · {note}" if note else ""))
    unknown = s["unknown"]
    if unknown and len(unknown) < s["domains"]:
        shown = ", ".join(unknown[:15]) + (f" and {len(unknown) - 15} more" if len(unknown) > 15 else "")
        lines.append(f"No usable blocklist check in the last {STALE_DAYS} days for "
                     f"{len(unknown)} domain{'s' if len(unknown) != 1 else ''}: {shown}")
    part = "\n".join(lines) or None
    note = None
    if not s["listed"] and len(unknown) < s["domains"]:
        clean = s["domains"] - len(unknown)
        note = (f"Blocklists: none of {whose} {clean} checked domain{'s' if clean != 1 else ''} on Spamhaus DBL "
                f"or URIBL ({when(s['checked_dates'])}).")
    return part, note
