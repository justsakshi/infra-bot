"""Signatures and sender addresses match what the team already uses.

The templates in inbox_profiles.json were derived from the live Smartlead
signatures on 30 Sep 2026; these tests pin that each person's rendered
signature reads exactly like their current one.
"""

from __future__ import annotations

import pytest

from smartlead import inbox_setup as ix
from smartlead.domain_lifecycle import identities_for_senders

P = ix.load_profiles()


def sig_for(client, full, email="x@y.com"):
    first, last = ix.split_name(full)
    acc = {"id": 1, "from_email": email, "from_name": full, "signature": "", "client_id": None}
    ch = ix.plan_changes(acc, P[client], email=email, set_signature=True)
    assert not ch.error, ch.error
    return ch.fields.get("signature", "")


@pytest.mark.parametrize("client, person, reads", [
    ("Belardi Wong", "Sam Lonsdale",
     "Sam Lonsdale\nVice President, Business Development\nBelardi Wong\nT 212-381-1709"),
    ("Precise Leads", "Aravind Haridas", "Aravind Haridas\nCo-Founder\nPrecise Leads"),
    ("Precise Leads", "Avinash Haridas", "Avinash Haridas\nFounder\nPrecise Leads"),
    ("Melior", "Ryan Markman", "Best,\nRyan"),
    ("Bettrdata", "Aaron Dix", "Aaron Dix\nFounder & CEO\nBettrData"),
    ("Bettrdata", "Penny Stone", "Penny Stone\nBettrData"),         # no title: line left out
])
def test_each_sender_reads_like_their_current_signature(client, person, reads):
    assert ix.signature_text(sig_for(client, person)) == reads


def test_same_words_in_different_html_is_not_a_change():
    acc = {"id": 1, "from_email": "a@b.com", "from_name": "Penny Stone",
           "signature": "<div>Penny Stone<br>BettrData</div>", "client_id": None}
    ch = ix.plan_changes(acc, P["Bettrdata"], email="a@b.com", set_signature=True)
    assert "signature" not in ch.fields


def test_empty_lines_are_dropped_not_left_blank():
    assert ix.render_lines(["{full_name}", "{title}", "T {phone}"], {"full_name": "A B"}) == "<div>A B</div>"


def test_default_senders_are_the_confirmed_people():
    assert P["Belardi Wong"]["default_senders"] == ["Sam Lonsdale"]
    assert P["Melior"]["default_senders"] == ["Ryan Markman"]
    assert P["Precise Leads"]["default_senders"] == ["Aravind Haridas", "Avinash Haridas"]


# ── addresses ───────────────────────────────────────────────────────────────

def test_one_person_gets_distinct_addresses_on_a_domain():
    ids = identities_for_senders("gomelior.com", ["Ryan Markman"], 3)
    assert [i["mailboxUsername"] for i in ids] == ["ryan", "ryan.markman", "ryanm"]
    assert all(i["firstName"] == "Ryan" and i["lastName"] == "Markman" for i in ids)


def test_two_people_take_turns():
    ids = identities_for_senders("x.com", ["Aravind Haridas", "Avinash Haridas"], 4)
    assert [i["mailboxUsername"] for i in ids] == ["aravind", "avinash", "aravind.haridas", "avinash.haridas"]


def test_existing_addresses_are_skipped():
    ids = identities_for_senders("x.com", ["Ryan Markman"], 2, exclude={"ryan", "ryanm"})
    assert [i["mailboxUsername"] for i in ids] == ["ryan.markman", "ryanmarkman"]


def test_usernames_follow_zapmail_rules():
    for i in identities_for_senders("x.com", ["Mary-Jane O'Neil"], 7):
        u = i["mailboxUsername"]
        assert u[0].isalnum() and u[-1].isalnum() and ".." not in u
        assert all(c.isalnum() or c == "." for c in u)


def test_a_styled_line_break_still_separates_lines():
    """Ryan's real signature uses <br style=...>; it must read 'Best,' then 'Ryan'."""
    ryan = ('<div bis_skin_checked="1" ><span style="color: rgb(0, 1, 21)">Best,</span>'
            '<br style="color: rgb(0, 1, 21); font-style: normal"><span style="color: rgb(0, 1, 21)">Ryan</span></div>')
    assert ix.signature_text(ryan) == "Best,\nRyan"
    acc = {"id": 1, "from_email": "ryan@meliorgrow.com", "from_name": "Ryan Markman",
           "signature": ryan, "client_id": 12256}
    assert ix.plan_changes(acc, P["Melior"], email="ryan@meliorgrow.com", set_signature=True).empty
