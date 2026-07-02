"""Pure planner: decide which inboxes need warmup enabled/disabled.

Rule (3-way):
  - In a LIVE (ACTIVE + fresh) campaign            -> warmup OFF (it's working).
  - In an ACTIVE-but-STALE campaign (dead 14d+)    -> warmup ON  (rescue it).
  - Idle / paused / completed / no campaign        -> warmup ON.

SAFETY: never enable warmup on an inbox attached to an ACTIVE, non-stale
campaign — regardless of today's send count (0 sends today may just be a
weekend/off-hour, not a dead campaign). Staleness is judged by campaign
freshness (leads/sends in the last 14 days), NOT by today's count.

Emit a change only when the actual warmup_state differs from the target.
Never touch 'blocked' inboxes (needs human investigation).
"""
from __future__ import annotations

from smartlead.config import WARMUP_MAINTENANCE_TRICKLE

# warmup_state values that mean "warmup is currently ON"
_ON_STATES = {"warming", "ramped", "on"}
# never auto-manage these (human-only)
_SKIP_STATES = {"blocked"}


def _in_live_campaign(row: dict) -> bool:
    """True if the inbox is in an ACTIVE campaign that is NOT stale.

    `campaign_is_stale` is computed upstream (per campaign) and stamped on the
    inbox row. When absent we default to NOT stale (safe: keep warmup off for
    anything ACTIVE, never risk warming a live sender)."""
    status = str(row.get("campaign_status", "")).upper()
    if status != "ACTIVE":
        return False
    return not bool(row.get("campaign_is_stale", False))


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
        should_be_on = not _in_live_campaign(row)

        if should_be_on and not currently_on:
            stale = bool(row.get("campaign_is_stale", False))
            reason = ("in a STALE campaign (dead 14d+) -> rescue: keep warm"
                      if stale else "idle/not in a live campaign -> keep warm")
            changes.append({
                "email": email, "account_id": row.get("account_id", ""),
                "client": row.get("client", ""), "action": "enable", "reason": reason,
            })
        elif not should_be_on and currently_on:
            # R2: 2026 says never fully off on active senders. With the trickle
            # toggle ON, keep a low maintenance warmup instead of disabling.
            if WARMUP_MAINTENANCE_TRICKLE:
                changes.append({
                    "email": email, "account_id": row.get("account_id", ""),
                    "client": row.get("client", ""), "action": "trickle",
                    "reason": "live sender -> maintenance warmup trickle (don't erode rep)",
                })
            else:
                changes.append({
                    "email": email, "account_id": row.get("account_id", ""),
                    "client": row.get("client", ""), "action": "disable",
                    "reason": "in a live ACTIVE campaign -> free send budget",
                })
    return changes
