"""Inboxes whose warm-up runs outside Smartlead (t248, 18 Nov 2026).

Manveen, 18 Nov 2026: "these are prewarmed inboxes we got today from inboxxkit ... never turn off warmup for them
inboxxkit handles warmup". A vendor's pre-warmed inbox is warmed by the vendor, so its Smartlead warm-up is OFF on
purpose and its Smartlead warm-up reputation is empty or 0%. For a marked inbox infra-bot:

  * never judges Smartlead warm-up: off, blocked, low or missing reputation, warm-up spam, warm-up age or ready date
    are no BUSY reason in All Inboxes (processing.py, usable.google_gate), no fleet case or digest ask (fleet.py),
    no warm-up repair entry (warmup_repair.py) and no infra audit finding (infra_audit.py);
  * never changes Smartlead warm-up or sets the inbox up: the warm-up planner skips it (state "external"), inbox
    intake leaves it alone (no ask, no write), the retest executor never switches its warm-up off or back on.

Everything else is judged as for any inbox: connection, DO NOT USE tags, v2 and the own-provider rule, real sends,
bounces, DNS, blocklists, limits.

The marks live in Mongo ``external_warmup``, one doc per inbox address or per whole domain (a vendor's pre-warmed
domain holds only its inboxes): {_id: address or domain (lower case), vendor, since, who, why, off?}. ``off: true``
turns a mark off without losing its history. No address or domain is kept in the repo (public): add marks with
external_warmup_cli.py.
"""
from __future__ import annotations

COLLECTION = "external_warmup"
STATE = "external"                  # processing.py's warmup_state for a marked inbox (the warm-up planner skips it)


def vendor_label(name) -> str | None:
    """One spelling per vendor ("inboxkit" -> "InboxKit"), as warmup_changes.py reports vendors."""
    from smartlead.warmup_changes import vendor_name
    return vendor_name(name)


def from_docs(docs) -> dict[str, str]:
    """{address or domain: vendor} from the collection's docs; a doc switched off or without a vendor marks
    nothing. Pure."""
    out: dict[str, str] = {}
    for d in docs or []:
        key, vendor = str(d.get("_id") or "").strip().lower(), vendor_label(str(d.get("vendor") or "").strip())
        if key and vendor and not d.get("off"):
            out[key] = vendor
    return out


def load(col) -> dict[str, str]:
    """The marks from a Mongo collection (or None). Never raises: unread = no marks, and the inbox is then judged
    as any other (it may show BUSY for warm-up; the caller's own "not read" note says why)."""
    if col is None:
        return {}
    try:
        return from_docs(col.find({}, {"_id": 1, "vendor": 1, "off": 1}))
    except Exception as exc:  # noqa: BLE001 - never stops a job
        print(f"  [external warm-up] marks not read ({type(exc).__name__})")
        return {}


def load_live() -> dict[str, str]:
    """The marks from the live database (MONGO_URI); {} when it is not set or cannot be read."""
    try:
        from smartlead.zapmail_asset_sync import _db
        db = _db()
    except Exception:  # noqa: BLE001
        db = None
    return load(db[COLLECTION]) if db is not None else {}


def vendor(email, marks: dict | None) -> str | None:
    """The vendor that warms this inbox (an address mark first, then its domain's), or None. Pure."""
    e = str(email or "").strip().lower()
    if not e or not marks:
        return None
    return marks.get(e) or (marks.get(e.split("@")[-1]) if "@" in e else None)


def note(vendor_name: str) -> str:
    return f"warm-up run by {vendor_name} outside Smartlead: Smartlead warm-up and its reputation are not used"


def mark(col, keys, *, vendor: str, since: str, who: str, why: str) -> int:
    """Upsert one mark per address or domain (lower case, de-duplicated). A mark that exists keeps its first
    ``since``; vendor, who and why are refreshed and ``off`` cleared. Returns the number of keys written."""
    done = set()
    for k in keys:
        k = str(k or "").strip().lower()
        if not k or k in done:
            continue
        done.add(k)
        col.update_one({"_id": k}, {"$set": {"vendor": vendor, "who": who, "why": why, "off": False},
                                    "$setOnInsert": {"since": since}}, upsert=True)
    return len(done)
