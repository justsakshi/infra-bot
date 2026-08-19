"""Domains already in the estate, so the generator never re-suggests one.

The team already records every domain they buy in infrabot's asset tracker
(`/infra add`), including purchase and expiry dates they renew against. That
tracker is the source of truth, so this module reads it directly rather than
asking anyone to maintain a second list — a duplicate-entry step nobody would
keep up, and the one most likely to be skipped precisely when it matters.

Sources, merged (all read-only):

  * **`assets` where type=DOMAIN** — what the team registers by hand. The
    authoritative list, and the only one that knows about a domain bought but
    not yet connected to any inbox.
  * **`assets` where type=INBOX** — each inbox row carries the domain it
    belongs to, which catches domains added as inboxes without a DOMAIN row.
  * **`domain_registry`** — written by the deliverability pipeline from live
    sending data; catches anything in flight that never got a manual entry.
  * **Smartlead** — every sending domain across every configured account.
  * **A seed file** (`owned_domains.txt`) — optional, for entries worth
    reviewing in a diff.
  * **A caller-supplied list** — `exclude=` on a single run, for one-offs.

Every status counts as owned, including Inactive and expired. A lapsed domain
is safe to re-buy, but a still-registered one is a wasted purchase, and the
status field cannot reliably tell the two apart.

Deliberately NOT scoped per-client. A domain we own for one client must not be
suggested for another: the estate is shared, and reusing a name across senders
is how one client's reputation problem becomes everyone's.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import HEALTH_HISTORY_DB
from smartlead.processing import get_domain_from_email

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

OWNED_COLLECTION: str = os.getenv("OWNED_DOMAINS_COLLECTION", "owned_domains")
# infrabot's own asset tracker — what /infra add writes, and what the team
# already maintains with purchase and expiry dates.
ASSETS_COLLECTION: str = os.getenv("ASSETS_COLLECTION", "assets")
DOMAIN_REGISTRY_COLLECTION: str = os.getenv("DOMAIN_REGISTRY_COLLECTION",
                                            "domain_registry")

# Checked into the repo next to this package's parent, so a bulk list can be
# reviewed in a diff. Slack-added domains live in Mongo instead.
SEED_FILE = Path(__file__).resolve().parent.parent / "owned_domains.txt"


async def fetch_owned_domains() -> dict[str, set[str]]:
    """``{domain: {client, ...}}`` across all Smartlead accounts.

    Mirrors ``blacklist_monitor._collect_domains`` — same traversal, same
    exclusion filter — but returns rather than checks, and does NOT drop
    excluded-client inboxes from the OWNERSHIP view: a domain belonging to a
    churned client is still a domain we own and must not re-suggest.
    """
    domains: dict[str, set[str]] = {}
    for acc in discover_accounts():
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                accounts = await c.list_email_accounts()
        except Exception as exc:  # noqa: BLE001
            # A failed account means an INCOMPLETE ownership picture. Say so
            # loudly: silently continuing risks re-suggesting a live domain.
            print(f"  [Estate] ⚠ {acc.name}: account list failed ({exc}) — "
                  "owned-domain list is incomplete for this account",
                  file=sys.stderr)
            continue
        for a in accounts:
            email = str(a.get("from_email", "")).strip()
            if not email:
                continue
            dom = get_domain_from_email(email)
            if not dom:
                continue
            label = acc.name
            if is_excluded_inbox(a):
                label = f"{acc.name} (inactive)"
            domains.setdefault(dom, set()).add(label)
    return domains


def _normalise(raw: str) -> str:
    """'  HTTPS://Foo.COM/path ' -> 'foo.com'. Empty string when unusable."""
    d = raw.strip().lower()
    if not d or d.startswith("#"):
        return ""
    d = d.split("//")[-1].split("/")[0].split("@")[-1]
    return d if "." in d else ""


def read_seed_file(path: Path = SEED_FILE) -> list[str]:
    """Domains from the checked-in seed list. Blank lines and `#` comments
    are ignored, so the file can carry notes about why a domain is listed."""
    try:
        raw = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    return [d for d in (_normalise(line) for line in raw) if d]


def _mongo_db():
    """The shared infrabot database, or None when Mongo is unavailable.

    Node connects with mongoose using the database embedded in MONGO_URI, so
    prefer that database over HEALTH_HISTORY_DB — otherwise this reads an
    empty database while the asset tracker sits in another one.
    """
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        default_db = client.get_default_database()
        if default_db is not None:
            return default_db
        return client[HEALTH_HISTORY_DB]
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] Mongo unavailable ({exc}) — owned-domain dedupe "
              "will be incomplete", file=sys.stderr)
        return None


def _owned_collection():
    """The ad-hoc `/domains own` collection, or None."""
    db = _mongo_db()
    if db is None:
        return None
    try:
        col = db[OWNED_COLLECTION]
        col.create_index("domain", unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] could not open {OWNED_COLLECTION} ({exc})",
              file=sys.stderr)
        return None


def read_asset_tracker() -> tuple[list[str], bool]:
    """``(domains, ok)`` from infrabot's own asset tracker.

    Reads three collections in the shared `infrabot` database:
      * `assets` type=DOMAIN  — the team's manual registrations (`/infra add`)
      * `assets` type=INBOX   — the `.domain` each inbox belongs to
      * `domain_registry`     — domains observed by the deliverability pipeline

    ``ok`` is False when the database could not be read at all, so the caller
    can say "dedupe is degraded" instead of silently suggesting owned domains.
    """
    db = _mongo_db()
    if db is None:
        return [], False

    found: set[str] = set()
    ok = False
    try:
        assets = db[ASSETS_COLLECTION]
        # Every status counts, Inactive included — see the module docstring.
        for row in assets.find({"type": "DOMAIN"}, {"name": 1, "_id": 0}):
            d = _normalise(str(row.get("name", "")))
            if d:
                found.add(d)
        for row in assets.find({"type": "INBOX"}, {"domain": 1, "_id": 0}):
            d = _normalise(str(row.get("domain", "")))
            if d:
                found.add(d)
        ok = True
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] could not read the asset tracker ({exc})",
              file=sys.stderr)

    try:
        for row in db[DOMAIN_REGISTRY_COLLECTION].find({}, {"domain": 1, "_id": 0}):
            d = _normalise(str(row.get("domain", "")))
            if d:
                found.add(d)
        ok = True
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] could not read domain_registry ({exc})", file=sys.stderr)

    return sorted(found), ok


def read_registered_domains() -> list[str]:
    """Domains recorded ad hoc via `/domains own`.

    Retained for domains that belong nowhere else, but the asset tracker is
    the primary source — nobody should need this for a domain they already
    entered with `/infra add`.
    """
    db = _mongo_db()
    if db is None:
        return []
    try:
        return [d for d in (_normalise(str(r.get("domain", "")))
                            for r in db[OWNED_COLLECTION].find({}, {"domain": 1}))
                if d]
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] could not read owned domains ({exc})", file=sys.stderr)
        return []


def register_domains(domains: list[str], added_by: str = "") -> tuple[list[str], list[str]]:
    """Record domains as owned. Returns ``(saved, rejected_as_malformed)``.

    Idempotent: re-adding an existing domain refreshes its metadata rather
    than erroring, so a team member repeating a command sees success.
    """
    cleaned, bad = [], []
    for raw in domains:
        d = _normalise(raw)
        (cleaned if d else bad).append(d or raw)

    if not cleaned:
        return [], bad

    col = _owned_collection()
    if col is None:
        raise RuntimeError("Mongo is not reachable, so the domains were not saved")

    now = datetime.now(timezone.utc)
    col.bulk_write([
        UpdateOne({"domain": d},
                  {"$set": {"domain": d, "added_by": added_by, "added_at": now}},
                  upsert=True)
        for d in cleaned
    ], ordered=False)
    return cleaned, bad


async def owned_domain_list(
    extra: list[str] | None = None,
) -> tuple[list[str], bool, dict[str, int]]:
    """``(domains, complete, counts)`` — merged estate view.

    ``complete`` is False when a source we expected to read failed, so the
    caller can warn that dedupe is degraded rather than quietly suggesting a
    domain we already own. ``counts`` reports per-source totals for display.

    The asset tracker is checked first and is the one source whose failure
    genuinely degrades the result: it is where the team records every purchase.
    """
    merged: set[str] = set()
    counts: dict[str, int] = {}
    complete = True

    tracker, tracker_ok = read_asset_tracker()
    merged.update(tracker)
    counts["asset_tracker"] = len(tracker)
    if not tracker_ok:
        complete = False

    try:
        fetched = await fetch_owned_domains()
        merged.update(fetched)
        counts["smartlead"] = len(fetched)
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] ⚠ Smartlead unreachable ({exc})", file=sys.stderr)
        counts["smartlead"] = 0
        complete = False

    seed = read_seed_file()
    merged.update(seed)
    counts["seed_file"] = len(seed)

    recorded = read_registered_domains()
    merged.update(recorded)
    counts["recorded"] = len(recorded)

    manual = [d for d in (_normalise(x) for x in (extra or [])) if d]
    merged.update(manual)
    counts["passed_in"] = len(manual)

    return sorted(merged), complete, counts
