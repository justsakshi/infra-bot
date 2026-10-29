"""Exclude old/inactive clients' inboxes from all tracking.

An inbox is excluded if any EXCLUDED_CLIENTS keyword appears (case-insensitive)
in its email domain, its tags, or its `client` field. Used to skip dead clients
so we don't spend API calls / test credits / warmup on them.
"""
from __future__ import annotations

import re

from smartlead.config import ACTIVE_CLIENTS, EXCLUDED_CLIENTS

# Past clients' campaigns, by name. Darlean, Avench and StaffAI campaigns also
# ran from the PRECISE_LEADS account with no client_id we map, so the name is
# the only mark they carry. Whole words, so "Oscar" is not OSC.
_PAST_CLIENT_CAMPAIGN = re.compile(
    r"\b(?:belardi|bw|darlean|mythic|avench|osc|staff ?ai|monarch|capsule|gofloater)\b",
    re.I)


def _haystack(inbox: dict) -> str:
    # match on the email DOMAIN only (not the local part - a sender named
    # "oscar@..." must not match the "osc" keyword), plus tags + client field
    email = str(inbox.get("email", "") or inbox.get("from_email", ""))
    domain = email.split("@", 1)[1] if "@" in email else ""
    parts = [domain, str(inbox.get("client", ""))]
    for t in (inbox.get("tags") or []):
        if isinstance(t, dict):
            parts.append(str(t.get("tag_name", "")))
        else:
            parts.append(str(t))
    return " ".join(parts).lower()


def is_excluded_inbox(inbox: dict) -> bool:
    hay = _haystack(inbox)
    return any(kw in hay for kw in EXCLUDED_CLIENTS)


def is_past_client_campaign(campaign: dict) -> bool:
    """A campaign run for a past client: kept off the Campaign Summary tabs
    (Manveen, 29 Oct 2026: "remove ... all the campaigns that are for past
    clients"). Known by its Smartlead client_id (OSC inside PRECISE_LEADS) or
    by a past client's name in the campaign name."""
    from smartlead.manager_map import SUB_CLIENT_BY_ID
    try:
        sub = SUB_CLIENT_BY_ID.get(int(campaign.get("client_id")))
    except (TypeError, ValueError):
        sub = None
    active = {c.replace("_", " ").upper() for c in ACTIVE_CLIENTS}
    if sub and sub.upper() not in active:
        return True
    return bool(_PAST_CLIENT_CAMPAIGN.search(str(campaign.get("name", ""))))
