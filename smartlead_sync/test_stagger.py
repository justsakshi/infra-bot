"""Tests for company-staggered lead release.

The release order is the whole feature: getting it wrong either wastes the
daily cap (fewer sends than we paid for) or piles sends onto one company,
which is exactly the behaviour the feature exists to stop.
"""
from __future__ import annotations

import pytest

from smartlead.stagger import (
    CompanyBook, CompanyState, ParsedLead, company_key, normalise_header,
    parse_leads_csv, plan_release, state_after_event, suggest_mapping,
)


# ── company keying ───────────────────────────────────────────────────────────

def test_company_key_uses_email_domain():
    assert company_key({"email": "a@acme.com"}) == "acme.com"


def test_public_mailbox_leads_are_separate_companies():
    """Two gmail addresses are two unrelated people. Keying them both as
    'gmail.com' would release one lead a day from all of Gmail."""
    a = company_key({"email": "a@gmail.com"})
    b = company_key({"email": "b@gmail.com"})
    assert a != b
    assert a == "a@gmail.com"


def test_explicit_company_column_wins():
    row = {"email": "a@acme.com", "account_id": "ACME GLOBAL"}
    assert company_key(row, company_column="account_id") == "acme global"


def test_falls_back_to_website_then_name_for_public_domains():
    assert company_key({"email": "a@gmail.com", "company_url": "https://www.Acme.com/x"}) == "acme.com"
    assert company_key({"email": "a@gmail.com", "company_name": "Acme Ltd"}) == "acme ltd"


def test_empty_explicit_column_falls_through_to_domain():
    assert company_key({"email": "a@acme.com", "account_id": "  "},
                       company_column="account_id") == "acme.com"


# ── header mapping ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("header,expected", [
    ("Email Address", "email"), ("first name", "first_name"),
    ("Company", "company_name"), ("LinkedIn URL", "linkedin_profile"),
    ("Personalized First Line", "personalized_first_line"),
])
def test_normalise_header(header, expected):
    assert normalise_header(header) == expected


def test_unknown_headers_survive_as_custom_field_keys():
    mapping = suggest_mapping(["Email", "Subject Line", "Personalized First Line"])
    assert mapping["Subject Line"] == "subject_line"
    assert mapping["Personalized First Line"] == "personalized_first_line"


# ── parsing ──────────────────────────────────────────────────────────────────

CSV = (
    "Email,First Name,Company,Subject Line\n"
    "a@acme.com,Ann,Acme,hello acme\n"
    "b@acme.com,Bob,Acme,hi acme\n"
    "c@beta.io,Cal,Beta,hey beta\n"
    "not-an-email,Dud,Bad,x\n"
    "a@acme.com,Dupe,Acme,dupe\n"
)


def test_parse_groups_by_company_and_drops_bad_rows():
    r = parse_leads_csv(CSV)
    assert len(r.leads) == 3
    assert r.companies == 2
    assert r.skipped_no_email == 1
    assert r.skipped_duplicate == 1


def test_parse_splits_standard_and_custom_fields():
    lead = parse_leads_csv(CSV).leads[0]
    payload = lead.to_smartlead()
    assert payload["email"] == "a@acme.com"
    assert payload["first_name"] == "Ann"
    assert payload["company_name"] == "Acme"
    assert payload["custom_fields"] == {"subject_line": "hello acme"}


def test_parse_handles_utf8_bom():
    r = parse_leads_csv("﻿Email\na@acme.com\n".encode("utf-8"))
    assert [lead.email for lead in r.leads] == ["a@acme.com"]


def test_ignored_columns_are_dropped():
    r = parse_leads_csv(CSV, mapping={"Email": "email", "Subject Line": "__ignore__"})
    assert r.leads[0].to_smartlead().get("custom_fields") is None


def test_lead_with_no_custom_fields_omits_the_key():
    assert "custom_fields" not in ParsedLead(email="a@x.com", company="x.com",
                                             fields={"email": "a@x.com"}).to_smartlead()


# ── release order: the core rule ─────────────────────────────────────────────

def _books(spec: dict[str, int], sent: dict[str, int] | None = None,
           states: dict[str, CompanyState] | None = None) -> list[CompanyBook]:
    """`spec` is total leads per company; `sent` how many already went out.

    `queued` holds only the leads still to send, matching how the executor
    builds it from Mongo - a lead that has been sent is no longer queued.
    """
    sent = sent or {}
    states = states or {}
    books = []
    for k, total in spec.items():
        already = sent.get(k, 0)
        books.append(CompanyBook(
            key=k,
            queued=[f"{k}-{i}" for i in range(already, total)],
            sent=already,
            state=states.get(k, CompanyState.ACTIVE)))
    return books


def test_one_lead_per_company_before_any_second():
    """Three companies with three leads each and a cap of 3 must produce one
    lead from each company, never three from one."""
    chosen = plan_release(_books({"a": 3, "b": 3, "c": 3}), cap=3)
    assert chosen == ["a-0", "b-0", "c-0"]


def test_untouched_companies_come_before_second_contacts():
    """'a' already had one sent, so 'b' and 'c' are served first."""
    chosen = plan_release(_books({"a": 3, "b": 3, "c": 3}, sent={"a": 1}), cap=3)
    assert chosen == ["b-0", "c-0", "a-1"]


def test_cap_is_filled_from_depth_when_breadth_runs_out():
    """Two companies, cap of 5: fill the cap rather than send only 2. Volume
    must not drop just because we ran out of new companies."""
    chosen = plan_release(_books({"a": 3, "b": 3}), cap=5)
    assert len(chosen) == 5
    assert chosen[:2] == ["a-0", "b-0"], "breadth still comes first"
    assert chosen[2:] == ["a-1", "b-1", "a-2"], "then alternate, not a-1,a-2,a-3"


def test_depth_alternates_between_companies():
    """Even when going deep, sends stay spread rather than emptying one org."""
    chosen = plan_release(_books({"a": 4, "b": 4}), cap=8)
    assert chosen == ["a-0", "b-0", "a-1", "b-1", "a-2", "b-2", "a-3", "b-3"]


def test_cap_larger_than_supply_returns_everything_once():
    chosen = plan_release(_books({"a": 2, "b": 1}), cap=99)
    assert sorted(chosen) == ["a-0", "a-1", "b-0"]
    assert len(chosen) == len(set(chosen)), "no lead may be released twice"


@pytest.mark.parametrize("state", [CompanyState.PAUSED_REPLY,
                                   CompanyState.PAUSED_BOUNCE,
                                   CompanyState.EXHAUSTED])
def test_paused_companies_are_never_released(state):
    chosen = plan_release(_books({"a": 3, "b": 3}, states={"a": state}), cap=5)
    assert all(not e.startswith("a-") for e in chosen), f"{state} company was emailed"


def test_zero_cap_and_empty_input_send_nothing():
    assert plan_release(_books({"a": 3}), cap=0) == []
    assert plan_release([], cap=10) == []
    assert plan_release(_books({"a": 0}), cap=10) == []


# ── state transitions ────────────────────────────────────────────────────────

@pytest.mark.parametrize("event,expected", [
    ("reply", CompanyState.PAUSED_REPLY),
    ("bounce", CompanyState.PAUSED_BOUNCE),
    ("blocked", CompanyState.PAUSED_BOUNCE),
    ("sent", CompanyState.ACTIVE),
])
def test_events_move_company_state(event, expected):
    assert state_after_event(CompanyState.ACTIVE, event) is expected


def test_auto_reply_does_not_pause_a_company():
    """An out-of-office says nothing about interest; pausing on it would
    silently drop companies that never actually answered."""
    assert state_after_event(CompanyState.ACTIVE, "auto_reply") is CompanyState.ACTIVE


def test_paused_states_are_terminal():
    for event in ("sent", "auto_reply", "bounce", "reply"):
        assert state_after_event(CompanyState.PAUSED_REPLY, event) is CompanyState.PAUSED_REPLY
