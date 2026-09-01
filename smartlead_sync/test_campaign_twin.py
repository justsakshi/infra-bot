"""Tests for the campaign twin cloner.

The network path is exercised against a fake client; the pure conversions are
what carry the risk (a wrong enum or a dropped variant produces a campaign
that LOOKS right in the UI and sends wrong).
"""
from __future__ import annotations

import asyncio

import pytest

from smartlead.campaign_twin import (
    OPEN_TRACKING_OFF, create_twin, has_open_tracking, schedule_from_campaign,
    sequences_to_post_shape, settings_from_campaign, strip_tag,
    to_post_track_settings, twin_name, with_open_tracking,
)


# ── track_settings enum translation ─────────────────────────────────────────

def test_short_get_form_is_translated_to_post_form():
    """GET says DONT_EMAIL_OPEN; POST only accepts DONT_TRACK_EMAIL_OPEN."""
    assert to_post_track_settings(["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"]) == \
        ["DONT_TRACK_EMAIL_OPEN", "DONT_TRACK_LINK_CLICK"]


def test_long_form_passes_through_and_dedupes():
    assert to_post_track_settings(["DONT_TRACK_LINK_CLICK", "DONT_LINK_CLICK"]) == \
        ["DONT_TRACK_LINK_CLICK"]


@pytest.mark.parametrize("values,expected", [
    ([], True),
    (None, True),
    (["DONT_LINK_CLICK"], True),
    (["DONT_EMAIL_OPEN"], False),
    (["DONT_TRACK_EMAIL_OPEN", "DONT_TRACK_LINK_CLICK"], False),
])
def test_has_open_tracking(values, expected):
    assert has_open_tracking(values) is expected


def test_with_open_tracking_flips_only_the_open_flag():
    src = ["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"]
    assert with_open_tracking(src, True) == ["DONT_TRACK_LINK_CLICK"]
    assert with_open_tracking(src, False) == ["DONT_TRACK_LINK_CLICK", OPEN_TRACKING_OFF]
    assert with_open_tracking([], False) == [OPEN_TRACKING_OFF]


# ── naming ───────────────────────────────────────────────────────────────────

def test_twin_name_is_idempotent():
    assert twin_name("Hospitality", True) == "Hospitality - Open Tracking"
    assert twin_name("Hospitality - Open Tracking", False) == "Hospitality - No Open Tracking"
    assert twin_name("Hospitality - No Open Tracking", False) == "Hospitality - No Open Tracking"
    # legacy dot tag still recognised
    assert twin_name("Hospitality · no open tracking", True) == "Hospitality - Open Tracking"


def test_strip_tag_leaves_untagged_names_alone():
    assert strip_tag("Plain name") == "Plain name"
    assert strip_tag("Name - No Open Tracking ") == "Name"
    assert strip_tag("Marketplace - Home Services") == "Marketplace - Home Services",         "a hyphenated name that is not a tag must survive"


# ── sequences ────────────────────────────────────────────────────────────────

def test_multi_variant_sequences_convert_and_drop_server_fields():
    get_shape = [{
        "seq_number": 2, "id": 9, "created_at": "x",
        "seq_delay_details": {"delayInDays": 3, "delay_in_days": 3},
        "sequence_variants": [
            {"id": 1, "subject": "A subj", "email_body": "A body", "variant_label": "A",
             "variant_distribution_percentage": "None", "is_deleted": False},
            {"id": 2, "subject": "B subj", "email_body": "B body", "variant_label": "B",
             "variant_distribution_percentage": 40, "is_deleted": False},
            {"id": 3, "subject": "gone", "email_body": "gone", "variant_label": "C",
             "is_deleted": True},
        ],
    }, {
        "seq_number": 1, "seq_delay_details": {"delay_in_days": 0},
        "subject": "Step one", "email_body": "Hi {{first_name}}", "sequence_variants": [],
    }]
    post = sequences_to_post_shape(get_shape)

    assert [s["seq_number"] for s in post] == [1, 2], "steps must be ordered"
    assert post[0]["seq_variants"] == [
        {"subject": "Step one", "email_body": "Hi {{first_name}}", "variant_label": "A"}]
    assert post[1]["seq_delay_details"] == {"delay_in_days": 3}
    labels = [v["variant_label"] for v in post[1]["seq_variants"]]
    assert labels == ["A", "B"], "deleted variant must be dropped"
    assert "id" not in post[1]["seq_variants"][0]
    assert "variant_distribution_percentage" not in post[1]["seq_variants"][0]
    assert post[1]["seq_variants"][1]["variant_distribution_percentage"] == 40


# ── schedule / settings ──────────────────────────────────────────────────────

def test_schedule_uses_flat_form_and_enforces_min_gap():
    detail = {"scheduler_cron_value": {"tz": "Asia/Kolkata", "days": [1, 2, 3],
                                       "startHour": "10:00", "endHour": "16:00"},
              "min_time_btwn_emails": 1, "max_leads_per_day": 50}
    s = schedule_from_campaign(detail)
    assert s["timezone"] == "Asia/Kolkata"
    assert s["days_of_the_week"] == [1, 2, 3]
    assert s["start_hour"] == "10:00" and s["end_hour"] == "16:00"
    assert s["min_time_btw_emails"] == 3, "Smartlead rejects gaps under 3"
    assert s["max_new_leads_per_day"] == 50


def test_settings_copy_fields_and_set_tracking():
    detail = {"track_settings": ["DONT_LINK_CLICK"], "stop_lead_settings": "REPLY_TO_AN_EMAIL",
              "follow_up_percentage": 50, "send_as_plain_text": False,
              "enable_ai_esp_matching": True, "client_id": None, "unsubscribe_text": ""}
    s = settings_from_campaign(detail, name="N", open_tracking=False)
    assert s["name"] == "N"
    assert s["track_settings"] == ["DONT_TRACK_LINK_CLICK", OPEN_TRACKING_OFF]
    assert s["stop_lead_settings"] == "REPLY_TO_AN_EMAIL"
    assert s["follow_up_percentage"] == 50
    assert "client_id" not in s, "null client_id must not be sent"


# ── create_twin against a fake client ───────────────────────────────────────

class FakeClient:
    def __init__(self, *, fail_at: str | None = None):
        self.calls: list[tuple] = []
        self.fail_at = fail_at
        self.deleted: list[str] = []

    async def get_campaign(self, cid):
        return {"id": int(cid), "name": "Src", "track_settings": ["DONT_LINK_CLICK"],
                "scheduler_cron_value": {"tz": "UTC", "days": [1], "startHour": "09:00",
                                         "endHour": "17:00"},
                "min_time_btwn_emails": 5, "client_id": None}

    async def get_campaign_sequences(self, cid):
        return [{"seq_number": 1, "subject": "s", "email_body": "b",
                 "seq_delay_details": {"delay_in_days": 0}, "sequence_variants": []}]

    async def get_campaign_email_accounts(self, cid):
        return [{"id": 11}, {"id": 12}]

    async def create_campaign(self, name, client_id=None):
        self.calls.append(("create", name))
        return {"id": 999, "name": name}

    async def _step(self, label, *a):
        self.calls.append((label, *a))
        if self.fail_at == label:
            raise RuntimeError(f"boom at {label}")
        return {}

    async def update_campaign_settings(self, cid, body):
        return await self._step("settings", cid, body)

    async def update_campaign_schedule(self, cid, body):
        return await self._step("schedule", cid, body)

    async def save_campaign_sequences_full(self, cid, seqs):
        return await self._step("sequences", cid, seqs)

    async def add_campaign_email_accounts(self, cid, ids):
        return await self._step("accounts", cid, ids)

    async def delete_campaign(self, cid):
        self.deleted.append(cid)
        return {}


def test_create_twin_flips_tracking_and_copies_everything():
    fc = FakeClient()
    s = asyncio.run(create_twin(fc, 42))

    assert s["source_open_tracking"] is True
    assert s["twin_open_tracking"] is False
    assert s["twin_id"] == 999
    assert s["tracked_id"] == 42 and s["untracked_id"] == 999
    assert s["twin_name"] == "Src - No Open Tracking"
    assert s["renamed_source"] is True

    labels = [c[0] for c in fc.calls]
    assert labels == ["create", "settings", "schedule", "sequences", "accounts", "settings"]
    settings_body = fc.calls[1][2]
    assert OPEN_TRACKING_OFF in settings_body["track_settings"]
    assert fc.calls[4][2] == [11, 12]
    assert fc.calls[5] == ("settings", "42", {"name": "Src - Open Tracking"})
    assert fc.deleted == []


def test_create_twin_rolls_back_on_failure():
    """A half-built twin must not survive - it looks real in the Smartlead UI."""
    fc = FakeClient(fail_at="sequences")
    with pytest.raises(RuntimeError):
        asyncio.run(create_twin(fc, 42))
    assert fc.deleted == ["999"]


def test_dry_run_creates_nothing():
    fc = FakeClient()
    s = asyncio.run(create_twin(fc, 42, dry_run=True))
    assert s["twin_id"] is None and s["dry_run"] is True
    assert fc.calls == []


def test_dry_run_still_labels_the_pair():
    """The dashboard preview decides which row gets the ON/OFF badge from
    tracked_id / untracked_id, so a dry run must report them even though the
    twin does not exist yet."""
    fc = FakeClient()  # source has open tracking ON
    s = asyncio.run(create_twin(fc, 42, dry_run=True))
    assert s["tracked_id"] == 42
    assert s["untracked_id"] is None
