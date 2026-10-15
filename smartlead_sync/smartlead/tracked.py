"""The inboxes we use: domains listed in the deliverability grid sheet, per
current client tab. Every other inbox in Smartlead is a retainer kept for a
client and is not judged for performance (Manveen, 15 Oct). Renewals still
cover every domain; this list is only for performance tracking (Fleet watch,
the volume review, the placement-test campaigns).
"""
from __future__ import annotations

import re

TABS = ("Precise Leads", "Bettrdata", "Melior")
DEAD = ("replaced", "cancel", "deletion", "delete")
_DOMAIN = re.compile(r"^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}$")


def parse_tab(rows: list[list[str]]) -> set[str]:
    """Domains in column A, minus rows marked replaced / cancelled / scheduled for deletion. Pure."""
    out = set()
    for r in rows:
        if not r:
            continue
        dom = str(r[0]).strip().lower()
        if not _DOMAIN.match(dom):
            continue
        status = " ".join(str(c) for c in r[1:4]).lower()
        if any(w in status for w in DEAD):
            continue
        out.add(dom)
    return out


def tracked_domains() -> set[str] | None:
    """Every current client's tracked domains, or None when the sheet cannot be read."""
    try:
        from smartlead.config import TEST_SHEET_ID
        from smartlead.sheets import _authorize
        sh = _authorize().open_by_key(TEST_SHEET_ID)
        out: set[str] = set()
        for tab in TABS:
            out |= parse_tab(sh.worksheet(tab).get_all_values())
        return out
    except Exception:  # noqa: BLE001 - callers keep every inbox rather than go blind
        return None


def only_tracked(accounts: list[dict], domains: set[str] | None) -> list[dict]:
    if domains is None:
        return accounts
    return [a for a in accounts
            if str(a.get("from_email") or a.get("email") or "").lower().split("@")[-1] in domains]
