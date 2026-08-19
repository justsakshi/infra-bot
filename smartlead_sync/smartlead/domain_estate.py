"""Domains already in the estate, so the generator never re-suggests one.

Four sources, merged:

  * **Smartlead** — every sending domain across every configured account.
    Authoritative for anything already live, and nobody maintains it by hand.
    Its blind spot: a domain bought but not yet connected to an inbox is
    invisible, which is exactly the window where a duplicate purchase happens.
  * **Mongo (`owned_domains`)** — domains the team registers from Slack with
    `/domains own <domains>`. Covers the blind spot above without a deploy.
  * **A seed file** (`owned_domains.txt`) — bulk entries checked into the repo,
    for a list you would rather review in a diff than type into Slack.
  * **A caller-supplied list** — `exclude=` on a single run, for one-offs.

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


def _mongo_collection():
    """The owned_domains collection, or None when Mongo is unavailable."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][OWNED_COLLECTION]
        col.create_index("domain", unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] Mongo unavailable ({exc}) — Slack-added domains "
              "will not be applied", file=sys.stderr)
        return None


def read_registered_domains() -> list[str]:
    """Domains the team added from Slack via `/domains own`."""
    col = _mongo_collection()
    if col is None:
        return []
    try:
        return [d for d in (_normalise(str(r.get("domain", "")))
                            for r in col.find({}, {"domain": 1})) if d]
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

    col = _mongo_collection()
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


async def owned_domain_list(extra: list[str] | None = None) -> tuple[list[str], bool]:
    """``(domains, smartlead_ok)`` — merged estate view.

    ``smartlead_ok`` is False when Smartlead could not be reached at all, so
    the caller can warn that dedupe ran on the manual sources only.
    """
    merged: set[str] = set()
    smartlead_ok = True
    try:
        fetched = await fetch_owned_domains()
        merged.update(fetched)
        if not fetched:
            smartlead_ok = False
    except Exception as exc:  # noqa: BLE001
        print(f"  [Estate] ⚠ Smartlead unreachable ({exc}) — dedupe will use "
              "only the recorded and passed-in domains", file=sys.stderr)
        smartlead_ok = False

    merged.update(read_seed_file())
    merged.update(read_registered_domains())
    for d in (_normalise(x) for x in (extra or [])):
        if d:
            merged.add(d)
    return sorted(merged), smartlead_ok
