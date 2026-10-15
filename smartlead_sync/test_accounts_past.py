"""Past clients' Smartlead accounts are never read (Manveen 15 Oct: drop Belardi Wong). No network."""
from smartlead import accounts


def test_past_client_accounts_are_skipped_even_with_keys_set(monkeypatch):
    monkeypatch.setenv("SMARTLEAD_API_KEY", "k-bw")                      # the default key is Belardi Wong's
    monkeypatch.setenv("SMARTLEAD_API_KEY_DARLEAN", "k-d")
    monkeypatch.setenv("SMARTLEAD_API_KEY_PRECISE_LEADS", "k-pl")
    monkeypatch.setenv("SMARTLEAD_API_KEY_BETTRDATA", "k-b")
    monkeypatch.delenv("SMARTLEAD_IGNORE_ACCOUNTS", raising=False)
    names = {a.name.upper() for a in accounts.discover_accounts()}
    assert "PRECISE_LEADS" in names and "BETTRDATA" in names
    assert "BELARDI WONG" not in names and "DARLEAN" not in names
