"""Tests for one-click domain suggestions (Zapmail AI + our generator).

Pure logic only — no network. The live behaviour these encode was measured on
2026-09-28 (AI rows are objects with status + price).

    .venv\\Scripts\\python.exe -m pytest test_domain_suggest.py -q
"""
from __future__ import annotations

import json

import pytest

from smartlead import domain_suggest
from smartlead.domain_ai import _extract_words, parse_ai_domains
from smartlead.domain_naming import ClientVocabulary
from smartlead.domain_suggest import keyword_windows, merge, pick_ai_rows, profile_for

LIVE_AI_RESULT = {"status": "generating", "progress": 100, "domains": [
    {"domainName": "streamintake.com", "status": "AVAILABLE", "isPremiumDomain": False,
     "domainPrice": "12.99", "renewPrice": "20.99"},
    {"domainName": "batchloader.com", "status": "AVAILABLE", "isPremiumDomain": False,
     "domainPrice": "12.99", "renewPrice": "20.99"},
    {"domainName": "takenname.com", "status": "UNAVAILABLE", "isPremiumDomain": False,
     "domainPrice": "0.00", "renewPrice": "0.00"},
    {"domainName": "fancy.com", "status": "AVAILABLE", "isPremiumDomain": True,
     "domainPrice": "2400.00", "renewPrice": "20.99"},
    {"domainName": "ingest.io", "status": "AVAILABLE", "isPremiumDomain": False,
     "domainPrice": "39.00", "renewPrice": "39.00"},
    "plainstring.com",
]}


def test_parse_ai_domains_reads_live_object_rows():
    rows = parse_ai_domains(LIVE_AI_RESULT)
    by = {r["domain"]: r for r in rows}
    assert by["streamintake.com"] == {"domain": "streamintake.com", "available": True,
                                      "price": 12.99, "renew_price": 20.99, "premium": False}
    assert by["takenname.com"]["available"] is False
    assert by["fancy.com"]["premium"] is True
    assert "ingest.io" not in by                       # not registrable on Zapmail
    assert by["plainstring.com"]["available"] is None   # string rows still accepted


def _vocab():
    return ClientVocabulary(name="BettrData", main_domain="bettrdata.io",
                            value_nouns=["data", "ingest", "pipeline"])


def test_pick_ai_rows_keeps_only_buyable_clean_names():
    rows = parse_ai_domains(LIVE_AI_RESULT)
    picked = pick_ai_rows(rows, _vocab(), owned={"batchloader.com"},
                          owned_stems=frozenset(), price_ceiling=25.0)
    names = [r["domain"] for r in picked]
    assert names == ["streamintake.com"]  # taken, premium, owned, unknown all dropped
    assert picked[0]["source"] == "ai" and picked[0]["renew_price"] == 20.99


def test_pick_ai_rows_applies_naming_rules():
    rows = [{"domain": "bettrdatahq.com", "available": True, "price": 12.99,
             "premium": False}]
    assert pick_ai_rows(rows, _vocab(), owned=set(), owned_stems=frozenset(),
                        price_ceiling=25.0) == []   # brand lookalike rejected


def test_merge_keeps_a_third_for_generator_and_dedupes():
    ai = [{"domain": f"ai{i}.com", "source": "ai"} for i in range(10)]
    gen = [{"domain": f"gen{i}.com", "source": "generator"} for i in range(5)]
    out = merge(ai, gen, 12)
    assert len(out) == 12
    assert sum(r["source"] == "generator" for r in out) == 4   # 12 // 3
    assert out[0]["domain"] == "ai0.com"
    assert len({r["domain"] for r in merge(ai, ai, 5)}) == 5


def test_merge_falls_back_to_ai_when_generator_empty():
    ai = [{"domain": f"ai{i}.com", "source": "ai"} for i in range(10)]
    assert len(merge(ai, [], 8)) == 8


def test_keyword_windows():
    assert keyword_windows(["a", "b", "c", "d", "e", "f", "g"]) == [["a", "b", "c"],
                                                                    ["d", "e", "f"]]
    assert keyword_windows(["a", "b"]) == [["a", "b"]]


def test_profiles(tmp_path, monkeypatch):
    f = tmp_path / "clients.json"
    f.write_text(json.dumps({"clients": {
        "Bettrdata": {"label": "BettrData", "main_domain": "bettrdata.io",
                      "keywords": ["data", "ingest", "pipeline"]},
        "Melior": {"label": "Melior", "main_domain": "", "keywords": []},
    }}), encoding="utf-8")
    monkeypatch.setattr(domain_suggest, "PROFILES_PATH", f)
    assert profile_for("bettrdata")[0] == "Bettrdata"
    assert profile_for("BettrData")[0] == "Bettrdata"
    with pytest.raises(ValueError, match="needs a main_domain"):
        profile_for("Melior")
    with pytest.raises(ValueError, match="no domain profile"):
        profile_for("Acme")


def test_real_profiles_file_is_valid():
    profiles = domain_suggest.load_profiles()
    for key in ("Bettrdata", "Belardi Wong", "Precise Leads", "Melior"):
        assert profile_for(key)[0] == key


def _fake_sources(monkeypatch, ai_names, held):
    import asyncio

    async def owned():
        return [], True, {}

    async def ai(words, desired_count=12, client=None):
        return [{"domain": d, "available": True, "premium": False, "price": 12.99}
                for d in ai_names]

    async def clean(domains):
        return {}

    class _Store:
        available = True

        def held_domains(self):
            return set(held)

    import smartlead.domain_batch as batch
    monkeypatch.setattr(domain_suggest, "owned_domain_list", owned)
    monkeypatch.setattr(domain_suggest, "ai_suggest", ai)
    monkeypatch.setattr(domain_suggest, "check_blacklists", clean)
    monkeypatch.setattr(batch, "BatchStore", _Store)
    return lambda: asyncio.run(domain_suggest.suggest("Belardi Wong", use_generator=False))


def test_names_already_in_a_purchase_plan_are_not_suggested(monkeypatch):
    run = _fake_sources(monkeypatch, ["postalcraft.com", "postalnest.com", "postcarddesk.com"],
                        held={"postalcraft.com", "postalnest.com"})
    got = run()
    assert [s["domain"] for s in got["suggestions"]] == ["postcarddesk.com"]
    assert got["held_in_plans"] == 2


def test_an_unreachable_ledger_is_reported_not_fatal(monkeypatch):
    run = _fake_sources(monkeypatch, ["postcarddesk.com"], held=())
    import smartlead.domain_batch as batch

    class _Down:
        available = False
    monkeypatch.setattr(batch, "BatchStore", _Down)
    got = run()
    assert [s["domain"] for s in got["suggestions"]] == ["postcarddesk.com"]
    assert any("purchase ledger" in e for e in got["errors"])


def test_scrape_drops_site_chrome_words():
    words = _extract_words("Home Skip to content BettrData helps teams ingest "
                           "and dedupe data pipeline records")
    assert "home" not in words and "skip" not in words and "content" not in words
    assert {"ingest", "dedupe", "pipeline", "records"} <= set(words)
