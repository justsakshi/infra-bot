"""Tests for Smartlead statistics -> company events.

Every field arrives from Smartlead as a STRING, "None" included. Reading
"False" as truthy would pause every company in a batch, so the parsing is
what these tests mostly guard.
"""
from __future__ import annotations

import pytest

from smartlead.stagger import CompanyState
from smartlead.stagger_events import classify_row, classify_rows


def row(**kw) -> dict:
    base = {"lead_email": "a@acme.com", "lead_category": "None",
            "is_bounced": "False", "ignore_reply": "False", "reply_time": "None"}
    base.update(kw)
    return base


# ── the rule the team asked for ──────────────────────────────────────────────

def test_out_of_office_does_not_pause_the_company():
    """Live example: ktaylor@farotech.com, category "Out Of Office",
    ignore_reply True. An OOO says nothing about interest."""
    e = classify_row(row(lead_category="Out Of Office", ignore_reply="True",
                         reply_time="2026-08-27T18:24:14.000Z"))
    assert e.kind == "auto_reply"
    assert e.company_state is None
    assert not e.pauses_company


def test_do_not_contact_pauses_the_company():
    """Live example: lauren@channelforemedia.com. An opt-out binds the org."""
    e = classify_row(row(lead_category="Do Not Contact",
                         reply_time="2026-08-31T14:58:17.000Z"))
    assert e.kind == "reply_company"
    assert e.company_state is CompanyState.PAUSED_REPLY


def test_interest_pauses_the_company_so_a_human_can_run_the_thread():
    """A thread is open; another cold email from the same sender cuts across it."""
    e = classify_row(row(lead_category="Interested", reply_time="x"))
    assert e.company_state is CompanyState.PAUSED_REPLY


@pytest.mark.parametrize("category", ["Not Interested", "Wrong Person"])
def test_a_personal_no_keeps_the_company_in_rotation(category):
    """One person saying the tool is wrong for THEIR role says nothing about
    a colleague in a different one. "Wrong Person" is literally an
    instruction to ask someone else at the same company."""
    e = classify_row(row(lead_category=category, reply_time="x"))
    assert e.kind == "reply_person"
    assert e.company_state is None, "colleagues must stay contactable"
    assert e.pauses_lead_only
    assert not e.pauses_company


def test_company_stop_outranks_a_personal_no_at_the_same_company():
    """If one person opted the org out and another merely said 'not me', the
    opt-out wins."""
    events = classify_rows([
        row(lead_email="a@acme.com", lead_category="Not Interested", reply_time="x"),
        row(lead_email="a@acme.com", lead_category="Do Not Contact", reply_time="x"),
    ])
    assert events["a@acme.com"].kind == "reply_company"


def test_bounce_pauses_the_company():
    """Live example: bcline@brunnerworks.com, is_bounced True."""
    e = classify_row(row(is_bounced="True"))
    assert e.kind == "bounce"
    assert e.company_state is CompanyState.PAUSED_BOUNCE


def test_sender_originated_bounce_category_counts_as_a_bounce():
    assert classify_row(row(lead_category="Sender Originated Bounce")).kind == "bounce"


def test_plain_sent_row_changes_nothing():
    e = classify_row(row())
    assert e.kind == "sent" and e.company_state is None


# ── string parsing (Smartlead sends everything as text) ──────────────────────

@pytest.mark.parametrize("value", ["False", "false", "None", "", None])
def test_falsey_bounce_flags_are_not_bounces(value):
    assert classify_row(row(is_bounced=value)).kind != "bounce"


@pytest.mark.parametrize("value", ["True", "true", "1", "yes"])
def test_truthy_bounce_flags_are_bounces(value):
    assert classify_row(row(is_bounced=value)).kind == "bounce"


def test_none_string_category_is_not_a_category():
    assert classify_row(row(lead_category="None")).kind == "sent"


def test_row_without_an_email_is_ignored():
    assert classify_row(row(lead_email="None")) is None


# ── ordering and collapsing ──────────────────────────────────────────────────

def test_bounce_wins_over_a_category_on_the_same_row():
    """A bounced row can still carry a category; the bounce is the fact that
    matters for whether the colleagues are safe to email."""
    assert classify_row(row(is_bounced="True", lead_category="Interested")).kind == "bounce"


def test_auto_reply_wins_over_a_bare_reply_time():
    assert classify_row(row(ignore_reply="True", reply_time="x")).kind == "auto_reply"


def test_uncategorised_reply_stops_the_company():
    """We cannot tell an opt-out from a personal no, and re-emailing someone
    who asked us to stop is the worse error."""
    assert classify_row(row(reply_time="2026-08-31T00:00:00Z")).kind == "reply_company"


def test_rows_collapse_to_the_most_consequential_event_per_lead():
    """One row per sequence step: a lead can be 'sent' on step 1 and
    'bounced' on step 2."""
    events = classify_rows([
        row(lead_email="a@acme.com"),
        row(lead_email="a@acme.com", reply_time="x", lead_category="Interested"),
        row(lead_email="a@acme.com", is_bounced="True"),
        row(lead_email="b@beta.io", ignore_reply="True", lead_category="Out Of Office"),
    ])
    assert events["a@acme.com"].kind == "bounce"
    assert events["b@beta.io"].kind == "auto_reply"


def test_classify_rows_skips_rows_without_email():
    assert classify_rows([row(lead_email="")]) == {}
