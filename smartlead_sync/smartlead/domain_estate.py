"""Domains already in the estate, so the generator never re-suggests one.

Two sources, merged:

  * **Smartlead** — every sending domain across every configured account. This
    is authoritative for anything already live, and it means nobody has to
    maintain a list by hand.
  * **A caller-supplied list** — domains bought but not yet connected to
    Smartlead, or held for a client who is not on our accounts. Smartlead
    cannot know about these, so they are passed in explicitly.

Deliberately NOT scoped per-client. A domain we own for one client must not be
suggested for another: the estate is shared, and reusing a name across senders
is how one client's reputation problem becomes everyone's.
"""

from __future__ import annotations

import sys

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.client_filter import is_excluded_inbox
from smartlead.processing import get_domain_from_email


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


async def owned_domain_list(extra: list[str] | None = None) -> tuple[list[str], bool]:
    """``(domains, smartlead_ok)`` — merged estate view.

    ``smartlead_ok`` is False when Smartlead could not be reached at all, so
    the caller can warn that dedupe is running on the manual list only.
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
              "only the domains you passed in", file=sys.stderr)
        smartlead_ok = False

    for d in extra or []:
        d = d.strip().lower()
        if d:
            merged.add(d)
    return sorted(merged), smartlead_ok
