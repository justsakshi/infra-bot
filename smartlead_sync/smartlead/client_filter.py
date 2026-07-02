"""Exclude old/inactive clients' inboxes from all tracking.

An inbox is excluded if any EXCLUDED_CLIENTS keyword appears (case-insensitive)
in its email domain, its tags, or its `client` field. Used to skip dead clients
so we don't spend API calls / test credits / warmup on them.
"""
from __future__ import annotations

from smartlead.config import EXCLUDED_CLIENTS


def _haystack(inbox: dict) -> str:
    parts = [str(inbox.get("email", "")), str(inbox.get("client", ""))]
    for t in (inbox.get("tags") or []):
        if isinstance(t, dict):
            parts.append(str(t.get("tag_name", "")))
        else:
            parts.append(str(t))
    return " ".join(parts).lower()


def is_excluded_inbox(inbox: dict) -> bool:
    hay = _haystack(inbox)
    return any(kw in hay for kw in EXCLUDED_CLIENTS)
