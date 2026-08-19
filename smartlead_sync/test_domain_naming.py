"""Tests for the domain naming rules.

The rules that matter are the rejections: a generator that emits
brand permutations is worse than no generator, because the names LOOK
reasonable while carrying exactly the fingerprint the DNSBLs cluster on.
"""
from __future__ import annotations

import pytest

from smartlead.domain_naming import (
    ClientVocabulary, PurchaseBatch, generate, generate_with_rejects,
    owned_stems_from, purchase_schedule, screen,
)


@pytest.fixture
def betterdata() -> ClientVocabulary:
    return ClientVocabulary(
        name="Better Data",
        main_domain="betterdata.com",
        value_nouns=["data", "ingest", "clarity"],
        problem_nouns=["signal", "coverage"],
        industry_nouns=["pipeline", "warehouse"],
    )


# ── brand-permutation rejection (the core rule) ──────────────────────────────

@pytest.mark.parametrize("sld", [
    "getbetterdata",   # affix + stem
    "trybetterdata",
    "betterdatahq",    # stem + affix
    "betterdataco",
    "betterdata",      # the stem itself
    "betterdatapro",
])
def test_brand_permutations_rejected(betterdata, sld):
    c = screen(sld, ".com", betterdata)
    assert not c.ok
    assert any("brand-permutation" in r for r in c.rejections)


def test_near_miss_misspelling_rejected(betterdata):
    """betterdta reads as typosquatting, not as a distinct brand."""
    c = screen("betterdta", ".com", betterdata)
    assert not c.ok
    assert any("brand-permutation" in r for r in c.rejections)


def test_industry_adjacent_name_passes(betterdata):
    """Shares no visible stem with betterdata — this is the shape we want."""
    c = screen("dataingest", ".com", betterdata)
    assert c.ok, c.rejections
    assert c.similarity_to_main < 0.62


# ── stem-fragment rule ───────────────────────────────────────────────────────

@pytest.fixture
def preciseleads() -> ClientVocabulary:
    """Vocabulary that deliberately overlaps the brand: 'precise' and 'leads'
    are both fragments of 'preciseleads'."""
    return ClientVocabulary(
        name="Precise Leads",
        main_domain="preciseleads.in",
        value_nouns=["precise", "leads", "signal", "intent"],
        problem_nouns=["outbound", "pipeline"],
    )


def test_brand_fragment_tokens_are_reported(preciseleads):
    assert set(preciseleads.brand_fragment_tokens()) == {"precise", "leads"}


@pytest.mark.parametrize("sld,tokens", [
    ("precisesignal", ("precise", "signal")),
    ("signalprecise", ("signal", "precise")),
    ("leadsprecise", ("leads", "precise")),
    ("leadsoutbound", ("leads", "outbound")),
    ("preciseoutbound", ("precise", "outbound")),
])
def test_names_reusing_a_brand_fragment_are_rejected(preciseleads, sld, tokens):
    """Any component that is a piece of the brand stem re-exposes the brand,
    regardless of which side of the compound it sits on."""
    c = screen(sld, ".com", preciseleads, source_tokens=tokens)
    assert not c.ok
    assert any("brand-permutation" in r for r in c.rejections), c.rejections


def test_fragment_free_names_still_pass(preciseleads):
    c = screen("signalintent", ".com", preciseleads,
               source_tokens=("signal", "intent"))
    assert c.ok, c.rejections


def test_generation_never_emits_a_brand_fragment_name(preciseleads):
    """The whole vocabulary is legal input; the generator must still refuse to
    build anything that reuses 'precise' or 'leads'."""
    for c in generate(preciseleads, limit=60):
        assert "precise" not in c.sld, c.domain
        assert "leads" not in c.sld, c.domain


def test_long_compound_sharing_letters_is_not_a_typosquat(preciseleads):
    """A long distinct compound must not trip the fuzzy-similarity rule just
    because it shares letters with the stem."""
    c = screen("outboundpipeline", ".com", preciseleads,
               source_tokens=("outbound", "pipeline"))
    assert c.ok, c.rejections


# ── phishing / spam shapes ───────────────────────────────────────────────────

@pytest.mark.parametrize("sld,marker", [
    ("d4taingest", "phishing-shape"),
    ("data-ingest", "hyphen"),
    ("databulksend", "banned-substring"),
    ("dataspamhub", "banned-substring"),
])
def test_bad_shapes_rejected(betterdata, sld, marker):
    c = screen(sld, ".com", betterdata)
    assert not c.ok
    assert any(marker in r for r in c.rejections), c.rejections


def test_awkward_seam_word_rejected(betterdata):
    """'the' + 'rapist' is the classic joined-token collision."""
    c = screen("therapistdata", ".com", betterdata)
    assert not c.ok
    assert any("awkward-substring" in r for r in c.rejections)


def test_non_com_tld_rejected(betterdata):
    c = screen("dataingest", ".xyz", betterdata)
    assert not c.ok
    assert any("tld:" in r for r in c.rejections)


@pytest.mark.parametrize("sld", ["ab", "datac", "a" * 25])
def test_length_bounds_enforced(betterdata, sld):
    c = screen(sld, ".com", betterdata)
    assert not c.ok
    assert any("length:" in r for r in c.rejections)


# ── generation ───────────────────────────────────────────────────────────────

def test_generate_emits_only_passing_candidates(betterdata):
    cands = generate(betterdata, limit=20)
    assert cands
    assert all(c.ok for c in cands)
    assert all(c.domain.endswith(".com") for c in cands)


def test_generate_never_emits_the_main_stem(betterdata):
    """The whole inversion: the main domain builds nothing, it only rejects."""
    for c in generate(betterdata, limit=50):
        assert "betterdata" not in c.sld


def test_generated_names_are_two_token_compounds(betterdata):
    for c in generate(betterdata, limit=10):
        assert len(c.source_tokens) == 2
        assert c.sld == "".join(c.source_tokens)


def test_generate_is_deterministic(betterdata):
    assert [c.domain for c in generate(betterdata, limit=15)] == \
           [c.domain for c in generate(betterdata, limit=15)]


def test_rejects_are_retained_for_audit(betterdata):
    all_c = generate_with_rejects(betterdata, limit=40)
    assert len(all_c) >= len(generate(betterdata, limit=40))


def test_shorter_names_rank_first(betterdata):
    passing = generate(betterdata, limit=30)
    lengths = [len(c.sld) for c in passing]
    assert lengths == sorted(lengths)


def test_vocabulary_is_deduplicated_and_normalized():
    v = ClientVocabulary(
        name="X", main_domain="x.com",
        value_nouns=["Data", "data", "DATA "],
        problem_nouns=["sig-nal"],
    )
    assert v.token_bank() == ["data", "signal"]


def test_main_stem_strips_tld_and_punctuation():
    v = ClientVocabulary(name="X", main_domain="try-better-data.com")
    assert v.main_stem() == "trybetterdata"


# ── estate exclusion (never re-suggest a domain we own) ──────────────────────

def test_owned_stems_normalizes_domains_and_labels():
    """Full domains and bare labels must collapse to the same stem so callers
    can mix Smartlead output with hand-typed lists."""
    assert owned_stems_from(["IngestSignal.com", "signalclarity", "a-b.com"]) == \
        frozenset({"ingestsignal", "signalclarity", "ab"})


def test_owned_stems_ignores_blanks():
    assert owned_stems_from(["", "  ", "real.com"]) == frozenset({"real"})


def test_already_owned_domain_is_rejected(betterdata):
    owned = owned_stems_from(["ingestsignal.com"])
    c = screen("ingestsignal", ".com", betterdata,
               source_tokens=("ingest", "signal"), owned_stems=owned)
    assert not c.ok
    assert "already-owned" in c.rejections


def test_near_sibling_of_owned_domain_is_rejected(betterdata):
    """Buying signalclarity.com when we own signalclarify.com rebuilds the
    batch fingerprint, just pointing at us instead of the client."""
    owned = owned_stems_from(["signalclarify.com"])
    c = screen("signalclarity", ".com", betterdata,
               source_tokens=("signal", "clarity"), owned_stems=owned)
    assert not c.ok
    assert any("too-close-to-owned" in r for r in c.rejections), c.rejections


def test_distinct_name_survives_a_populated_estate(betterdata):
    owned = owned_stems_from(["ingestsignal.com", "claritypipeline.com"])
    c = screen("accuracysignal", ".com", betterdata,
               source_tokens=("accuracy", "signal"), owned_stems=owned)
    assert c.ok, c.rejections


def test_short_owned_stem_does_not_reject_unrelated_longer_names(betterdata):
    """A short owned stem must not blanket-reject longer distinct names."""
    owned = owned_stems_from(["ingest.com"])
    c = screen("clarityaccuracy", ".com", betterdata,
               source_tokens=("clarity", "accuracy"), owned_stems=owned)
    assert c.ok, c.rejections


def test_generate_excludes_owned_domains_end_to_end(betterdata):
    owned = owned_stems_from(["ingestsignal.com", "signalingest.com"])
    produced = {c.domain for c in generate(betterdata, limit=60, owned_stems=owned)}
    assert "ingestsignal.com" not in produced
    assert "signalingest.com" not in produced
    assert produced, "estate exclusion must not empty the whole result"


def test_empty_estate_changes_nothing(betterdata):
    assert [c.domain for c in generate(betterdata, limit=20)] == \
           [c.domain for c in generate(betterdata, limit=20,
                                       owned_stems=frozenset())]


# ── purchase scheduling ──────────────────────────────────────────────────────

def test_purchase_schedule_spreads_registrars_and_days():
    domains = [f"d{i}.com" for i in range(9)]
    batches = purchase_schedule(domains, ["A", "B", "C"], per_batch=3, day_gap=2)

    assert len(batches) == 3
    assert [b.registrar for b in batches] == ["A", "B", "C"]
    assert [b.day_offset for b in batches] == [0, 2, 4]
    # Every domain lands in exactly one batch.
    assert sorted(d for b in batches for d in b.domains) == sorted(domains)


def test_purchase_schedule_wraps_registrars_when_batches_exceed_them():
    batches = purchase_schedule([f"d{i}.com" for i in range(6)], ["A", "B"],
                                per_batch=2, day_gap=1)
    assert [b.registrar for b in batches] == ["A", "B", "A"]


def test_purchase_schedule_handles_partial_final_batch():
    batches = purchase_schedule(["a.com", "b.com", "c.com", "d.com"], ["A"],
                                per_batch=3)
    assert len(batches[-1].domains) == 1


def test_purchase_schedule_rejects_empty_registrars():
    with pytest.raises(ValueError):
        purchase_schedule(["a.com"], [])


def test_purchase_schedule_rejects_bad_batch_size():
    with pytest.raises(ValueError):
        purchase_schedule(["a.com"], ["A"], per_batch=0)


def test_purchase_batch_is_immutable():
    b = PurchaseBatch(day_offset=0, registrar="A", domains=("a.com",))
    with pytest.raises(Exception):
        b.registrar = "B"  # type: ignore[misc]


# ── owned-domain sources (seed file parsing) ─────────────────────────────────

def test_seed_file_normalizes_and_filters(tmp_path):
    """The seed file is hand-edited, so it must tolerate URLs, casing,
    trailing space, comments, blanks, and reject non-domains outright."""
    from smartlead.domain_estate import read_seed_file

    f = tmp_path / "owned_domains.txt"
    f.write_text(
        "# a comment\n"
        "\n"
        "boughtlastweek.com   \n"
        "HTTPS://Another.COM/some/path\n"
        "notadomain\n"
        "sales@thirdparty.com\n",
        encoding="utf-8",
    )
    assert read_seed_file(f) == [
        "boughtlastweek.com", "another.com", "thirdparty.com",
    ]


def test_seed_file_missing_is_not_an_error(tmp_path):
    from smartlead.domain_estate import read_seed_file
    assert read_seed_file(tmp_path / "does-not-exist.txt") == []


# ── DNS registration gate ────────────────────────────────────────────────────
#
# Zapmail's search answers "can I suggest names around this?", not "is this
# exact name free?" — it reported five registered .com compounds as available
# on 2026-08-19. DNS is the authority: a registered domain publishes NS
# records. These tests pin the response decoding, not the network.

import asyncio  # noqa: E402

import pytest  # noqa: E402  (already imported above; kept local for clarity)

from smartlead.domain_availability import _is_registered  # noqa: E402


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeClient:
    """Stands in for httpx.AsyncClient, returning one canned DNS answer."""

    def __init__(self, payload=None, raises=False):
        self._payload = payload
        self._raises = raises

    async def get(self, *_args, **_kwargs):
        if self._raises:
            raise RuntimeError("resolver unreachable")
        return _FakeResponse(self._payload)


def _check(payload=None, raises=False):
    return asyncio.run(_is_registered(_FakeClient(payload, raises), "example.com"))


def test_nxdomain_means_not_registered():
    assert _check({"Status": 3}) is False


def test_ns_answer_means_registered():
    assert _check({"Status": 0, "Answer": [{"type": 2, "data": "ns1.example."}]}) is True


def test_noerror_with_authority_only_is_inconclusive():
    """The name exists but publishes no NS at this level — never call it free."""
    assert _check({"Status": 0, "Authority": [{"type": 6}]}) is None


def test_servfail_is_inconclusive_not_available():
    """A broken lookup must not be read as 'nobody owns this'."""
    assert _check({"Status": 2}) is None


def test_resolver_error_is_inconclusive():
    assert _check(raises=True) is None
