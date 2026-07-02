"""Pure planner: decide which inboxes need warmup enabled/disabled.

Rule: warmup ON by default; OFF only when the inbox is actively sending in a
live (ACTIVE) campaign. Emit a change only when the actual warmup_state differs
from the target. Never touch 'blocked' inboxes (needs human investigation).
"""
from __future__ import annotations

from smartlead.config import WARMUP_ACTIVE_SENT_MIN

# warmup_state values that mean "warmup is currently ON"
_ON_STATES = {"warming", "ramped", "on"}
# never auto-manage these (human-only)
_SKIP_STATES = {"blocked"}


def _int(v) -> int:
    try:
        return int(float(str(v)))
    except (TypeError, ValueError):
        return 0


def _is_actively_sending(row: dict) -> bool:
    """Actively sending = in an ACTIVE campaign AND sent at/above the min today."""
    status = str(row.get("campaign_status", "")).upper()
    sent = _int(row.get("sent_today", 0))
    return status == "ACTIVE" and sent >= WARMUP_ACTIVE_SENT_MIN


def plan_warmup_changes(health_rows: list[dict]) -> list[dict]:
    """Return list of {email, account_id, client, action('enable'|'disable'), reason}."""
    changes: list[dict] = []
    for row in health_rows:
        state = str(row.get("warmup_state", "")).strip().lower()
        if state in _SKIP_STATES:
            continue
        email = str(row.get("email", "")).strip()
        if not email:
            continue
        currently_on = state in _ON_STATES
        should_be_on = not _is_actively_sending(row)

        if should_be_on and not currently_on:
            changes.append({
                "email": email, "account_id": row.get("account_id", ""),
                "client": row.get("client", ""), "action": "enable",
                "reason": "idle/not-actively-sending -> keep warm",
            })
        elif not should_be_on and currently_on:
            changes.append({
                "email": email, "account_id": row.get("account_id", ""),
                "client": row.get("client", ""), "action": "disable",
                "reason": "actively sending in live campaign -> free send budget",
            })
    return changes
