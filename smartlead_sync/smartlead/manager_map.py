"""Per-client owner map. Ownership is per client (not per Smartlead account):
one account (e.g. PRECISE_LEADS) holds several clients, each with its own manager.
Fill slack handles at rollout (Slack member IDs like 'U01234' or '@name')."""
from __future__ import annotations

MANAGER_MAP: dict[str, dict] = {
    "DARLEAN":       {"name": "Balasankar", "slack": ""},
    "Mythic":        {"name": "Balasankar", "slack": ""},
    "Melior":        {"name": "Anjali",     "slack": ""},
    "Belardi Wong":  {"name": "Anjali",     "slack": ""},
    "Bettrdata":     {"name": "Varsha",     "slack": ""},  # Better Data
    "Precise Leads": {"name": "Varsha",     "slack": ""},  # internal
    "OSC":           {"name": "Balasankar", "slack": ""},
    "StaffAI":       {"name": "Balasankar", "slack": ""},
    "Avench":        {"name": "Unassigned", "slack": ""},  # old client - ignore
}


def resolve_manager(client: str) -> dict:
    return MANAGER_MAP.get(client, {"name": "Unassigned", "slack": ""})
