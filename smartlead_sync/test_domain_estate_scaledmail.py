"""Estate: every domain ScaledMail holds for us counts as owned (live
2026-10-12: names bought on ScaledMail were suggested again). No network."""
import asyncio

from smartlead import domain_estate


class FakeSM:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def domains(self):
        return [{"domain": "WarmMeetings.com"}, {"domain": "growmelior.com"}]

    def purchased_domains(self, available_only=False):
        return [{"domain": "preciseleadshq.com"}, {"domain": "warmmeetings.com"}]


def test_scaledmail_domains_and_inventory_are_owned(monkeypatch):
    import smartlead.scaledmail as sm
    monkeypatch.setattr(sm, "ScaledMailClient", FakeSM)
    monkeypatch.setenv("SCALEDMAIL_API_KEY", "k")
    assert domain_estate.scaledmail_owned() == ["growmelior.com", "preciseleadshq.com", "warmmeetings.com"]


def test_owned_list_includes_scaledmail_and_flags_a_failure(monkeypatch):
    monkeypatch.setattr(domain_estate, "read_asset_tracker", lambda: (["a.com"], True))

    async def none():
        return {}
    monkeypatch.setattr(domain_estate, "fetch_owned_domains", none)
    import smartlead.zapmail_fleet as zf

    async def no_zap(errors):
        return []
    monkeypatch.setattr(zf, "all_domains", no_zap)
    monkeypatch.setattr(domain_estate, "read_seed_file", lambda: [])
    monkeypatch.setattr(domain_estate, "read_registered_domains", lambda: [])
    monkeypatch.setattr(domain_estate, "scaledmail_owned", lambda: ["preciseleadshq.com"])
    got, complete, counts = asyncio.run(domain_estate.owned_domain_list())
    assert "preciseleadshq.com" in got and complete and counts["scaledmail"] == 1

    def down():
        raise RuntimeError("502")
    monkeypatch.setattr(domain_estate, "scaledmail_owned", down)
    got, complete, counts = asyncio.run(domain_estate.owned_domain_list())
    assert got == ["a.com"] and not complete and counts["scaledmail"] == 0
