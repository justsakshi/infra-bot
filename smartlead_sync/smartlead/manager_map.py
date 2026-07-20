"""Per-client owner map. Ownership is per client (not per Smartlead account):
one account (e.g. PRECISE_LEADS) holds several clients, each with its own manager.
Fill slack handles at rollout (Slack member IDs like 'U01234' or '@name')."""
from __future__ import annotations

MANAGER_MAP: dict[str, dict] = {
    "DARLEAN":       {"name": "Balasankar", "slack": "U091D7REGGN"},
    "Mythic":        {"name": "Balasankar", "slack": "U091D7REGGN"},
    "Melior":        {"name": "Anjali",     "slack": "U045NBCSA3F"},
    "Belardi Wong":  {"name": "Anjali",     "slack": "U045NBCSA3F"},
    "Bettrdata":     {"name": "Varsha",     "slack": "U0767GZUM8S"},  # Better Data
    "Precise Leads": {"name": "Varsha",     "slack": "U0767GZUM8S"},  # internal
    "OSC":           {"name": "Balasankar", "slack": "U091D7REGGN"},
    "StaffAI":       {"name": "Balasankar", "slack": "U091D7REGGN"},
    "Avench":        {"name": "Unassigned", "slack": ""},  # old client - ignore
}

# Founders — not client owners, but useful for fleet-wide notify targets.
FOUNDERS: dict[str, str] = {
    "Avinash": "U026H4M2X09",
    "Aravind": "U03AF9U985V",
}


# PRECISE_LEADS is an agency account holding THREE real Smartlead clients plus
# internal campaigns (verified from GET /client + campaign client_ids, 2026-07-10):
#   12256  Ryan Markman (ryan@getmelior.com)   -> Melior     (215 campaigns)
#   456214 Aaron dix (aaron.dix@bettrdata.io)  -> Bettrdata  (5 campaigns)
#   145916 Srivatsan (srivatsan@svsg.co)       -> OSC        (32 campaigns)
#   client_id None                              -> Precise Leads internal (369)
SUB_CLIENT_BY_ID: dict[int, str] = {
    12256: "Melior",
    456214: "Bettrdata",
    145916: "OSC",
}

# Fallback for bench inboxes (no campaign -> no client_id): infer from domain.
_DOMAIN_HINTS: list[tuple[str, str]] = [
    ("melior", "Melior"),
    ("bettr", "Bettrdata"),
    ("svsg", "OSC"),
]


def resolve_sub_client(account_name: str, client_id=None, email: str = "") -> str:
    """Best-effort sub-client for an inbox row. Only the PRECISE_LEADS agency
    account has sub-clients; every other account IS the client."""
    if _norm(account_name) != "precise leads":
        return account_name
    if client_id is not None:
        try:
            mapped = SUB_CLIENT_BY_ID.get(int(client_id))
            if mapped:
                return mapped
        except (TypeError, ValueError):
            pass
    domain = email.split("@", 1)[1].lower() if "@" in email else ""
    for hint, label in _DOMAIN_HINTS:
        if hint in domain:
            return label
    return "Precise Leads"


def _norm(name: str) -> str:
    """Client names arrive in several spellings ('PRECISE_LEADS' account name,
    'Precise Leads' human label, 'DARLEAN' from env-var casing) — normalize
    case/underscores so every spelling resolves to the same manager."""
    return name.strip().lower().replace("_", " ")


_NORMALIZED_MAP: dict[str, dict] = {_norm(k): v for k, v in MANAGER_MAP.items()}


def resolve_manager(client: str) -> dict:
    return _NORMALIZED_MAP.get(_norm(client), {"name": "Unassigned", "slack": ""})
