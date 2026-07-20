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


def _norm(name: str) -> str:
    """Client names arrive in several spellings ('PRECISE_LEADS' account name,
    'Precise Leads' human label, 'DARLEAN' from env-var casing) — normalize
    case/underscores so every spelling resolves to the same manager."""
    return name.strip().lower().replace("_", " ")


_NORMALIZED_MAP: dict[str, dict] = {_norm(k): v for k, v in MANAGER_MAP.items()}


def resolve_manager(client: str) -> dict:
    return _NORMALIZED_MAP.get(_norm(client), {"name": "Unassigned", "slack": ""})
