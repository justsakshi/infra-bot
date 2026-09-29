"""Names must read like a company in the client's business.

Fixtures are the real names from the 2026-09-29 live runs, labelled by what
a person would say about them.
"""

from __future__ import annotations

import pytest

from smartlead.domain_naming import BANNED_SUBSTRINGS
from smartlead.domain_quality import BusinessWords, build_names, judge, segment
from smartlead.domain_suggest import ai_seeds, load_profiles

PROFILES = load_profiles()
BETTR = BusinessWords.from_profile(PROFILES["Bettrdata"])
BW = BusinessWords.from_profile(PROFILES["Belardi Wong"])
PL = BusinessWords.from_profile(PROFILES["Precise Leads"])


@pytest.mark.parametrize("name", ["streamintake", "batchloader", "ingestcraft", "clearrecords"])
def test_good_bettrdata_names_pass(name):
    assert judge(name, BETTR).ok, judge(name, BETTR)


@pytest.mark.parametrize("name,why", [
    ("recordflume", "unfamiliar"),     # odd word
    ("streamsluice", "unfamiliar"),
    ("batchriver", "unfamiliar"),
    ("payloadpath", "unfamiliar"),     # not about the business
    ("ingestdedupe", "not a noun"),    # verb + verb
    ("datadedupehq", "not a noun"),
    ("ingestdatahq", "not a noun"),
])
def test_bad_bettrdata_names_fail(name, why):
    v = judge(name, BETTR)
    assert not v.ok and why in v.reason, v


@pytest.mark.parametrize("name", ["postalcraft", "postalnest", "postcarddesk", "catalogworks"])
def test_good_belardi_wong_names_pass(name):
    assert judge(name, BW).ok, judge(name, BW)


@pytest.mark.parametrize("name", [
    "catalogpostal", "catalogretail", "postalretail",  # adjective last
    "couriermark", "dispatchpage",                       # nothing about the business
    "catalogcatalogs", "mailmailbox",                    # the same word twice
])
def test_bad_belardi_wong_names_fail(name):
    assert not judge(name, BW).ok


def test_a_join_that_reads_as_another_word_fails():
    """Live 2026-09-29: cleaningest.com was offered - it reads 'cleaning est'."""
    v = judge("cleaningest", BETTR)
    assert not v.ok and "join" in v.reason
    assert judge("audienceingest", BETTR).ok  # vowel before the join reads fine


def test_ai_seeds_avoid_onboarding():
    """'onboarding' pulled Zapmail's AI into HR words (enrollcove, signupledger,
    welcomefile) - all 24 dropped on 2026-09-29."""
    assert all("onboarding" not in s for s in ai_seeds(PROFILES["Bettrdata"]))


def test_melior_is_set_up_by_brand_name_without_a_website():
    from smartlead.domain_naming import ClientVocabulary, screen
    from smartlead.domain_suggest import brand_of, profile_for
    key, prof = profile_for("Melior")
    assert brand_of(prof) == "melior"          # not "getmelior.com": the stem is stricter
    from smartlead.domain_suggest import site_of
    assert site_of(prof) == "getmelior.com"
    mw = BusinessWords.from_profile(prof)
    assert judge("agencyplaybook", mw).ok
    assert judge("appliedinsights", mw).ok
    vocab = ClientVocabulary(name="Melior", main_domain=brand_of(prof),
                             value_nouns=sorted(mw.on_topic))
    assert not screen("melioragency", ".com", vocab).ok   # the brand never appears
    assert not screen("getmeliorhq", ".com", vocab).ok


def test_mailshot_is_a_spam_word():
    assert "mailshot" in BANNED_SUBSTRINGS


def test_a_name_made_only_of_generic_words_is_not_about_the_business():
    v = judge("clearpath", BETTR)
    assert not v.ok and "nothing about the business" in v.reason


def test_two_words_beat_three_and_specific_beats_generic():
    assert judge("clearrecords", BETTR).score > judge("clearrecordshq", BETTR).score
    assert judge("datarecords", BETTR).score > judge("dataworks", BETTR).score


def test_segment_prefers_fewest_words():
    assert segment("streamintake", BETTR.vocabulary) == ("stream", "intake")
    assert segment("flumeworks", BETTR.vocabulary) is None


@pytest.mark.parametrize("bw", [BETTR, BW, PL])
def test_generator_output_all_passes_the_judge_best_first(bw):
    built = build_names(bw)
    assert built, "profile produced no names"
    scores = [v.score for _, _, v in built]
    assert scores == sorted(scores, reverse=True)
    for sld, _, v in built[:200]:
        assert judge(sld, bw).ok
    top = [s for s, _, _ in built[:60]]
    assert not any(t in s for s in top for t in ("cedar", "oak", "blue", "flume")), top


def test_old_profiles_without_roles_still_work():
    bw = BusinessWords.from_profile({"keywords": ["catalog", "print", "mail"]})
    assert judge("catalogprint", bw).ok


def test_ai_seeds_come_from_the_profile_else_keyword_windows():
    assert ai_seeds(PROFILES["Belardi Wong"])[0] == ["catalog", "marketing", "retail"]
    assert ai_seeds({"keywords": ["a", "b", "c", "d"]}) == [["a", "b", "c"], ["d"]]
