"""Pure planner: decide the warmup profile every inbox should be on.

State machine (docs/DELIVERABILITY_MASTER_PLAN.md §1 — warmup is ALWAYS ON;
only the volume/ramp/reply-rate profile changes by state):

  RECOVERING  rep < 90                          -> 15/day, ramp 0, reply 30 (boost
              (known rep only)                     the REPLY RATE, not volume —
                                                   replies are the reputation signal)
  ACTIVE      in a LIVE (ACTIVE + fresh)        -> 20/day, ramp 0, reply 25,
              campaign                             auto-adjust ON (Smartlead trims
                                                   warmup 7-10 while sending)
  NEW         warmup_state == "warming"         -> 40/day cap, ramp +5, reply 25
              (inbox still inside ramp window)
  IDLE        everything else (insurance/bench, -> 20/day, ramp 0, reply 25
              paused/completed/stale campaign)
  LONG_IDLE   IDLE + no real send in            -> same 20/day (no volume change —
              WARMUP_LONG_IDLE_DAYS+ (default 30)  no evidence 20 is insufficient),
                                                   reply rate nudged to 28% (cheap
                                                   freshness signal) + retest_due flag
                                                   for the manager before ACTIVE promotion

SAFETY:
  - never touch 'blocked' inboxes (human-only investigation);
  - emit a change only when the live state differs from the profile: warmup off,
    or volume off-target (with a tolerance on auto-adjusted inboxes so we don't
    fight Smartlead's own 7-10/day reduction);
  - stale ACTIVE campaigns (dead 14d+) are NOT live — their inboxes fall through
    to NEW/IDLE and keep warming (rescue).

Legacy fallback (WARMUP_ALWAYS_ON=false): old 3-way on/off/trickle behavior.
"""
from __future__ import annotations

from datetime import date, datetime

from smartlead.config import (
    WARMUP_MAINTENANCE_TRICKLE, WARMUP_ALWAYS_ON, HEALTH_WARMUP_ZERO,
    WARMUP_NEW_PER_DAY, WARMUP_ACTIVE_PER_DAY, WARMUP_IDLE_PER_DAY,
    WARMUP_RECOVERY_PER_DAY, WARMUP_DAILY_RAMPUP, WARMUP_REPLY_RATE,
    WARMUP_BOOST_REPLY_RATE, WARMUP_AUTO_ADJUST_ACTIVE, WARMUP_RETUNE_TOLERANCE,
    WARMUP_TRICKLE_PER_DAY, WARMUP_LONG_IDLE_DAYS, WARMUP_LONG_IDLE_REPLY_RATE,
)

# warmup_state values that mean "warmup is currently ON"
_ON_STATES = {"warming", "ramped", "on"}
# never auto-manage these (human-only)
_SKIP_STATES = {"blocked"}


def _rep(row: dict) -> float:
    try:
        return float(str(row.get("warmup_rep_pct", "")).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _current_volume(row: dict) -> int:
    try:
        return int(float(row.get("warmup_max_count", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _in_live_campaign(row: dict) -> bool:
    """True if the inbox is in an ACTIVE campaign that is NOT stale.

    `campaign_is_stale` is computed upstream (per campaign) and stamped on the
    inbox row. When absent we default to NOT stale (safe: treat as live)."""
    status = str(row.get("campaign_status", "")).upper()
    if status != "ACTIVE":
        return False
    return not bool(row.get("campaign_is_stale", False))


def _idle_days(row: dict) -> int | None:
    """Days since last_active_date (last real send). None if unknown."""
    raw = str(row.get("last_active_date", "")).strip()
    if not raw:
        return None
    try:
        d = datetime.fromisoformat(raw[:10]).date()
    except ValueError:
        return None
    return (date.today() - d).days


def _profile(row: dict) -> tuple[str, dict]:
    """Return (state_name, profile) for a row. Profile keys mirror set_warmup args."""
    rep = _rep(row)
    if 0 < rep < HEALTH_WARMUP_ZERO:
        return "RECOVERING", {
            "per_day": WARMUP_RECOVERY_PER_DAY, "rampup": 0,
            "reply_rate": WARMUP_BOOST_REPLY_RATE, "auto_adjust": None,
        }
    if _in_live_campaign(row):
        return "ACTIVE", {
            "per_day": WARMUP_ACTIVE_PER_DAY, "rampup": 0,
            "reply_rate": WARMUP_REPLY_RATE,
            "auto_adjust": True if WARMUP_AUTO_ADJUST_ACTIVE else None,
        }
    if str(row.get("warmup_state", "")).strip().lower() == "warming":
        return "NEW", {
            "per_day": WARMUP_NEW_PER_DAY, "rampup": WARMUP_DAILY_RAMPUP,
            "reply_rate": WARMUP_REPLY_RATE, "auto_adjust": None,
        }
    idle_days = _idle_days(row)
    if idle_days is not None and idle_days >= WARMUP_LONG_IDLE_DAYS:
        # same volume as IDLE (no evidence 20/day is insufficient) — only the
        # reply rate is nudged, plus a retest flag for the manager to act on
        # before this inbox is ever promoted back into a live campaign.
        return "LONG_IDLE", {
            "per_day": WARMUP_IDLE_PER_DAY, "rampup": 0,
            "reply_rate": WARMUP_LONG_IDLE_REPLY_RATE, "auto_adjust": None,
            "retest_due": True, "idle_days": idle_days,
        }
    return "IDLE", {
        "per_day": WARMUP_IDLE_PER_DAY, "rampup": 0,
        "reply_rate": WARMUP_REPLY_RATE, "auto_adjust": None,
    }


def _reply_rate_off_target(row: dict, target: int) -> bool:
    """True if the inbox's current reply-rate setting differs from target.

    warmup_reply_rate_pct is populated from Smartlead's live warmup_details
    (see smartlead/processing.py) whenever the sync ran successfully. If it's
    still None (row predates that pass, or the field is genuinely absent
    upstream), fail safe: report off-target (apply once) rather than silently
    skipping the freshness nudge — set_warmup is idempotent, so a harmless
    repeat call is the safer failure mode than never applying it."""
    current = row.get("warmup_reply_rate_pct")
    if current is None:
        return True
    try:
        return int(float(current)) != int(target)
    except (TypeError, ValueError):
        return True


def _volume_off_target(current: int, target: int, auto_adjusted: bool) -> bool:
    """Auto-adjust legitimately drags the live number below target — allow slack."""
    if current <= 0:
        return True  # unknown -> apply profile once to be sure
    if auto_adjusted:
        return abs(current - target) > WARMUP_RETUNE_TOLERANCE
    return current != target


def _liveness_rank(row: dict) -> int:
    """Most-live campaign row wins when one inbox appears on several campaigns:
    live ACTIVE (3) > stale ACTIVE (2) > other campaign (1) > none (0).
    Without this, the profile chosen depended on API response order — an inbox
    on both an ACTIVE and a completed campaign could get the IDLE profile."""
    status = str(row.get("campaign_status", "")).upper()
    if status == "ACTIVE":
        return 3 if not bool(row.get("campaign_is_stale", False)) else 2
    campaign = str(row.get("campaign_name", ""))
    if campaign and not campaign.startswith("N/A"):
        return 1
    return 0


def _merge_per_inbox(rows: list[dict]) -> list[dict]:
    """One row per (client, email), keeping the most-live campaign's row."""
    best: dict[tuple, dict] = {}
    for row in rows:
        email = str(row.get("email", "")).strip().lower()
        if not email:
            continue
        key = (row.get("client", ""), email)
        cur = best.get(key)
        if cur is None or _liveness_rank(row) > _liveness_rank(cur):
            best[key] = row
    return list(best.values())


def plan_warmup_changes(health_rows: list[dict]) -> list[dict]:
    """Return list of change dicts:
    {email, account_id, client, action, reason, per_day, rampup, reply_rate, auto_adjust}
    action: 'enable' (was off) | 'retune' (on, wrong volume) | 'boost' (recovering)
    plus legacy 'disable'/'trickle' when WARMUP_ALWAYS_ON is false."""
    changes: list[dict] = []
    for row in _merge_per_inbox(health_rows):
        state = str(row.get("warmup_state", "")).strip().lower()
        if state in _SKIP_STATES:
            continue
        email = str(row.get("email", "")).strip()
        if not email:
            continue
        currently_on = state in _ON_STATES

        if WARMUP_ALWAYS_ON:
            profile_name, prof = _profile(row)
            base = {
                "email": email, "account_id": row.get("account_id", ""),
                "client": row.get("client", ""), **prof,
            }
            if not currently_on:
                changes.append({
                    **base, "action": "enable",
                    "reason": f"warmup off -> ON as {profile_name} "
                              f"({prof['per_day']}/day, reply {prof['reply_rate']}%)",
                })
            elif profile_name == "RECOVERING" and _volume_off_target(
                    _current_volume(row), prof["per_day"], auto_adjusted=False):
                changes.append({
                    **base, "action": "boost",
                    "reason": f"low warmup rep ({_rep(row):.0f}%) -> RECOVERING: "
                              f"{prof['per_day']}/day + reply rate {prof['reply_rate']}% "
                              "(boost replies, not volume)",
                })
            elif profile_name == "LONG_IDLE" and _reply_rate_off_target(row, prof["reply_rate"]):
                # volume unchanged from IDLE — only the reply-rate nudge + a
                # retest flag for the manager; make sure this doesn't get lost
                # just because the volume already matches (see IDLE == LONG_IDLE
                # per_day above).
                changes.append({
                    **base, "action": "retune",
                    "reason": f"LONG_IDLE ({prof['idle_days']}d no send) -> reply rate "
                              f"{prof['reply_rate']}% (freshness nudge, volume unchanged) "
                              "+ retest recommended before promoting to ACTIVE",
                })
            elif _volume_off_target(_current_volume(row), prof["per_day"],
                                    auto_adjusted=bool(prof["auto_adjust"])):
                changes.append({
                    **base, "action": "retune",
                    "reason": f"{profile_name}: warmup {_current_volume(row)}/day "
                              f"-> {prof['per_day']}/day (state profile)",
                })
            continue

        # ── legacy 3-way path (WARMUP_ALWAYS_ON=false) ──────────────────────
        should_be_on = not _in_live_campaign(row)
        legacy = {
            "email": email, "account_id": row.get("account_id", ""),
            "client": row.get("client", ""),
            "per_day": WARMUP_NEW_PER_DAY, "rampup": WARMUP_DAILY_RAMPUP,
            "reply_rate": WARMUP_REPLY_RATE, "auto_adjust": None,
        }
        if should_be_on and not currently_on:
            stale = bool(row.get("campaign_is_stale", False))
            reason = ("in a STALE campaign (dead 14d+) -> rescue: keep warm"
                      if stale else "idle/not in a live campaign -> keep warm")
            changes.append({**legacy, "action": "enable", "reason": reason})
        elif not should_be_on and currently_on:
            if WARMUP_MAINTENANCE_TRICKLE:
                changes.append({
                    **legacy, "action": "trickle",
                    "per_day": WARMUP_TRICKLE_PER_DAY, "rampup": 0,
                    "reason": "live sender -> maintenance warmup trickle (don't erode rep)",
                })
            else:
                changes.append({
                    **legacy, "action": "disable",
                    "reason": "in a live ACTIVE campaign -> free send budget",
                })
    return changes
