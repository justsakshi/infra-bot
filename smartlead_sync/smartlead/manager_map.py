"""Per-client owner map. Ownership is per client (not per Smartlead account):
one account (e.g. PRECISE_LEADS) holds several clients, each with its own manager.
Fill slack handles at rollout (Slack member IDs like 'U01234' or '@name')."""
from __future__ import annotations

MANAGER_MAP: dict[str, dict] = {
    "DARLEAN":       {"name": "Unassigned", "slack": ""},
    "Melior":        {"name": "Unassigned", "slack": ""},
    "Precise Leads": {"name": "Unassigned", "slack": ""},
    "Bettrdata":     {"name": "Unassigned", "slack": ""},  # Better Data
    "Mythic":        {"name": "Unassigned", "slack": ""},
    "Belardi Wong":  {"name": "Unassigned", "slack": ""},
    "Avench":        {"name": "Unassigned", "slack": ""},
    "OSC":           {"name": "Unassigned", "slack": ""},
    "StaffAI":       {"name": "Unassigned", "slack": ""},
}


def resolve_manager(client: str) -> dict:
    return MANAGER_MAP.get(client, {"name": "Unassigned", "slack": ""})
