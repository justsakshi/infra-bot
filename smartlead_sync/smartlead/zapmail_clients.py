"""Which client a Zapmail domain belongs to — one answer for every feature.

Zapmail knows the account a domain sits in, not the client: the Precise Leads
account holds BettrData, Melior AND Precise Leads' own domains. Tracker sync,
Slack actions (export target, routing) and reports all need the client, so
the rule lives here once:

  1. The /infra tracker's client for that domain, when the team recorded one.
  2. The Belardi Wong Zapmail account → Belardi Wong.
  3. A brand in the name (``bettrdata``/``ingest`` → BettrData, ``melior`` →
     Melior, ``precise`` → Precise Leads, ``belardi`` → Belardi Wong).
  4. Otherwise None: unassigned — never guessed into a client.

Current clients (2026-10-08): Precise Leads, BettrData, Melior. Belardi Wong
stopped being a client on 2026-10-08: its domains now read as a past client's
(left to lapse, no buttons, not added to the tracker).
Names are the profile keys used by ``zapmail_accounts`` / ``domain_clients.json``.
"""

from __future__ import annotations

import re

# Profile key -> how the /infra tracker spells the client (from live rows).
TRACKER_SPELLING: dict[str, str] = {
    "Belardi Wong": "Belardiwong",
    "Bettrdata": "Bettrdata",
    "Melior": "Melior",
    "Precise Leads": "Precise Leads",
}
CURRENT_CLIENTS: tuple[str, ...] = ("Bettrdata", "Melior", "Precise Leads")

_BRANDS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"bettr|ingest"), "Bettrdata"),
    (re.compile(r"melior"), "Melior"),
    (re.compile(r"precise"), "Precise Leads"),
    (re.compile(r"belardi|(^|reach|send|swiftby)bw|^bw"), "Belardi Wong"),
)


def _squash(name: str | None) -> str:
    return re.sub(r"[^a-z]", "", str(name or "").lower())


def client_from_tracker(tracker_client: str | None) -> str | None:
    """Tracker spelling ('Belardiwong', 'Preciseleads', 'OSC - Srivatsan') →
    profile key, or None when it is not a current client."""
    s = _squash(tracker_client)
    if not s:
        return None
    for key in CURRENT_CLIENTS:
        if _squash(key) == s or _squash(TRACKER_SPELLING[key]) == s:
            return key
    return None


def infer_client(domain: str, account: str | None,
                 tracker_client: str | None = None) -> str | None:
    """Profile key of the current client owning ``domain``, or None."""
    from_tracker = client_from_tracker(tracker_client)
    if from_tracker:
        return from_tracker
    if tracker_client:            # tracked under a past client: keep it theirs
        return None
    if _squash(account) == "belardiwong":
        return None                      # past client's own account
    sld = str(domain or "").lower().split(".")[0]
    for rx, client in _BRANDS:
        if rx.search(sld):
            return client if client in CURRENT_CLIENTS else None
    return None
