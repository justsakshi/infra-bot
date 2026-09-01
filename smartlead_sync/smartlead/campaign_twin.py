"""Clone a Smartlead campaign into an open-tracking twin.

The team creates a campaign in Smartlead as usual. This module produces its
twin: identical sequences, schedule, settings and sender inboxes, differing
only in whether email-open tracking is on. Leads go to the tracked copy first
to confirm the campaign is landing, then to the untracked copy for the real
run, where the tracking pixel cannot hurt deliverability.

Both campaigns are renamed with a visible marker so nobody has to open the
settings to know which is which:

    Hospitality - Multi-Property Chains - Open Tracking
    Hospitality - Multi-Property Chains - No Open Tracking

Two Smartlead quirks this module exists to absorb:

  * GET /campaigns/{id} reports tracking in a SHORT enum (``DONT_EMAIL_OPEN``)
    that POST /campaigns/{id}/settings rejects - it wants
    ``DONT_TRACK_EMAIL_OPEN``. Copying the GET value straight into the POST
    silently leaves tracking at the default.
  * GET /campaigns/{id}/sequences nests variants under ``sequence_variants``
    with server-side ids, while the POST wants ``seq_variants`` with only
    subject / body / label. Single-variant campaigns put subject and body on
    the step itself and leave the variants list empty.

The twin is created but never started. ``bounce_autopause_threshold`` is not
readable through the API and so is not copied - set it by hand if the source
had one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from smartlead.api import SmartleadClient

# GET short form -> POST long form. Anything already in long form passes through.
_TRACK_GET_TO_POST: dict[str, str] = {
    "DONT_EMAIL_OPEN": "DONT_TRACK_EMAIL_OPEN",
    "DONT_LINK_CLICK": "DONT_TRACK_LINK_CLICK",
    "DONT_REPLY_TO_AN_EMAIL": "DONT_TRACK_REPLY_TO_AN_EMAIL",
}
OPEN_TRACKING_OFF = "DONT_TRACK_EMAIL_OPEN"

# Matches the convention the team already uses by hand (BETTRDATA campaigns
# 3867136 / 3871482): "<name> - No Open Tracking". Older "·" tags still strip.
TAG_ON = "- Open Tracking"
TAG_OFF = "- No Open Tracking"
_TAG_RE = re.compile(r"\s*[-·]\s*(no )?open tracking\s*$", re.IGNORECASE)

# Smartlead rejects schedules with a gap under 3 minutes (verified live).
_MIN_GAP_MINUTES = 3


# ── pure helpers (unit-tested, no network) ───────────────────────────────────

def to_post_track_settings(values: list[str] | None) -> list[str]:
    """Normalise a track_settings list to the form the settings POST accepts."""
    out: list[str] = []
    for v in values or []:
        v = str(v).strip().upper()
        v = _TRACK_GET_TO_POST.get(v, v)
        if v and v not in out:
            out.append(v)
    return out


def has_open_tracking(values: list[str] | None) -> bool:
    return OPEN_TRACKING_OFF not in to_post_track_settings(values)


def with_open_tracking(values: list[str] | None, enabled: bool) -> list[str]:
    """Same settings with open tracking forced on or off; other flags untouched."""
    out = [v for v in to_post_track_settings(values) if v != OPEN_TRACKING_OFF]
    if not enabled:
        out.append(OPEN_TRACKING_OFF)
    return out


def strip_tag(name: str) -> str:
    return _TAG_RE.sub("", name or "").rstrip()


def twin_name(name: str, open_tracking: bool) -> str:
    """'X' / 'X · open tracking' / 'X · no open tracking' -> tagged for the
    requested state. Idempotent, so re-running never stacks tags."""
    return f"{strip_tag(name)} {TAG_ON if open_tracking else TAG_OFF}"


def sequences_to_post_shape(sequences: list[dict]) -> list[dict]:
    """GET sequence records -> POST body.

    Drops server ids and timestamps, renames ``sequence_variants`` to
    ``seq_variants``, and synthesises a single 'A' variant for steps that
    carry subject/body directly. Deleted variants are skipped.
    """
    out: list[dict] = []
    for step in sorted(sequences, key=lambda s: int(s.get("seq_number") or 0)):
        delay = step.get("seq_delay_details") or {}
        delay_days = delay.get("delay_in_days", delay.get("delayInDays", 0)) or 0

        variants: list[dict] = []
        for v in step.get("sequence_variants") or step.get("seq_variants") or []:
            if v.get("is_deleted"):
                continue
            variant = {
                "subject": v.get("subject") or "",
                "email_body": v.get("email_body") or "",
                "variant_label": v.get("variant_label") or chr(ord("A") + len(variants)),
            }
            pct = v.get("variant_distribution_percentage")
            if pct not in (None, "", "None"):
                variant["variant_distribution_percentage"] = pct
            variants.append(variant)

        if not variants:
            variants = [{
                "subject": step.get("subject") or "",
                "email_body": step.get("email_body") or "",
                "variant_label": "A",
            }]

        out.append({
            "seq_number": int(step.get("seq_number") or len(out) + 1),
            "seq_delay_details": {"delay_in_days": int(delay_days)},
            "seq_variants": variants,
        })
    return out


def schedule_from_campaign(detail: dict) -> dict:
    """GET campaign record -> schedule POST body (flat form, verified live)."""
    cron = detail.get("scheduler_cron_value") or {}
    body: dict[str, Any] = {
        "timezone": cron.get("tz") or "America/New_York",
        "days_of_the_week": list(cron.get("days") or [1, 2, 3, 4, 5]),
        "start_hour": cron.get("startHour") or "09:00",
        "end_hour": cron.get("endHour") or "17:00",
        "min_time_btw_emails": max(_MIN_GAP_MINUTES,
                                   int(detail.get("min_time_btwn_emails") or _MIN_GAP_MINUTES)),
    }
    if detail.get("max_leads_per_day") is not None:
        body["max_new_leads_per_day"] = int(detail["max_leads_per_day"])
    if detail.get("schedule_start_time"):
        body["schedule_start_time"] = detail["schedule_start_time"]
    return body


def settings_from_campaign(detail: dict, *, name: str, open_tracking: bool) -> dict:
    """GET campaign record -> settings POST body with tracking set as asked."""
    body: dict[str, Any] = {
        "name": name,
        "track_settings": with_open_tracking(detail.get("track_settings"), open_tracking),
    }
    for key in ("stop_lead_settings", "unsubscribe_text", "send_as_plain_text",
                "follow_up_percentage", "enable_ai_esp_matching"):
        if detail.get(key) is not None:
            body[key] = detail[key]
    if detail.get("client_id") is not None:
        body["client_id"] = detail["client_id"]
    return body


# ── network path ─────────────────────────────────────────────────────────────

@dataclass
class Blueprint:
    detail: dict
    sequences_post: list[dict]
    account_ids: list[int] = field(default_factory=list)

    @property
    def name(self) -> str:
        return str(self.detail.get("name") or "")

    @property
    def open_tracking(self) -> bool:
        return has_open_tracking(self.detail.get("track_settings"))


async def fetch_blueprint(client: SmartleadClient, campaign_id: str | int) -> Blueprint:
    detail = await client.get_campaign(str(campaign_id))
    if not isinstance(detail, dict) or not detail.get("id"):
        raise ValueError(f"campaign {campaign_id} not found on this account")
    seqs = await client.get_campaign_sequences(str(campaign_id))
    accounts = await client.get_campaign_email_accounts(str(campaign_id))
    ids = [int(a["id"]) for a in (accounts or []) if a.get("id") is not None]
    return Blueprint(detail=detail, sequences_post=sequences_to_post_shape(seqs or []),
                     account_ids=ids)


async def create_twin(
    client: SmartleadClient,
    source_id: str | int,
    *,
    open_tracking: bool | None = None,
    rename_source: bool = True,
    dry_run: bool = False,
) -> dict:
    """Create the twin of ``source_id``. Returns a summary dict.

    ``open_tracking`` defaults to the OPPOSITE of the source, which is the
    only twin that makes sense. On any failure after creation the half-built
    twin is deleted so a retry starts clean instead of leaving a stray
    campaign behind.
    """
    bp = await fetch_blueprint(client, source_id)
    twin_tracking = (not bp.open_tracking) if open_tracking is None else open_tracking
    new_name = twin_name(bp.name, twin_tracking)
    source_name = twin_name(bp.name, bp.open_tracking)

    summary = {
        "source_id": int(bp.detail["id"]),
        "source_name": source_name,
        "source_open_tracking": bp.open_tracking,
        "twin_name": new_name,
        "twin_open_tracking": twin_tracking,
        "steps": len(bp.sequences_post),
        "email_accounts": len(bp.account_ids),
        "renamed_source": False,
        "dry_run": dry_run,
    }
    if dry_run:
        summary["twin_id"] = None
        return summary

    created = await client.create_campaign(new_name, client_id=bp.detail.get("client_id"))
    twin_id = str(created.get("id") or "")
    if not twin_id:
        raise RuntimeError(f"create_campaign returned no id: {created}")

    try:
        await client.update_campaign_settings(
            twin_id, settings_from_campaign(bp.detail, name=new_name,
                                            open_tracking=twin_tracking))
        await client.update_campaign_schedule(twin_id, schedule_from_campaign(bp.detail))
        if bp.sequences_post:
            await client.save_campaign_sequences_full(twin_id, bp.sequences_post)
        if bp.account_ids:
            await client.add_campaign_email_accounts(twin_id, bp.account_ids)
    except Exception:
        # Never leave a half-configured campaign that looks real in the UI.
        try:
            await client.delete_campaign(twin_id)
        finally:
            raise

    if rename_source and source_name != bp.name:
        await client.update_campaign_settings(str(bp.detail["id"]), {"name": source_name})
        summary["renamed_source"] = True

    summary["twin_id"] = int(twin_id)
    summary["tracked_id"] = int(twin_id) if twin_tracking else int(bp.detail["id"])
    summary["untracked_id"] = int(bp.detail["id"]) if twin_tracking else int(twin_id)
    return summary
