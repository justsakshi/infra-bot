"""`zapmail_maintenance.py --renewals --client Melior` lists Melior's domains
only (2026-09-29 it printed every renewal on the Precise Leads account)."""

from __future__ import annotations

from smartlead import zapmail_asset_sync
from smartlead.zapmail_maintenance import _only_client

ROWS = [{"domain": d, "provider": "GOOGLE"} for d in (
    "gomelior.com", "launchmelior.com", "kombinatorfunds.com", "trycreflo.com",
    "askbettrdata.com")]


def test_only_the_clients_domains(monkeypatch):
    monkeypatch.setattr(zapmail_asset_sync, "read_tracker", lambda: {})
    got = [r["domain"] for r in _only_client(ROWS, "Melior")]
    assert got == ["gomelior.com", "launchmelior.com"]


def test_tracker_client_wins_over_the_name(monkeypatch):
    monkeypatch.setattr(zapmail_asset_sync, "read_tracker",
                        lambda: {"kombinatorfunds.com": {"client": "Melior"}})
    got = [r["domain"] for r in _only_client(ROWS, "Melior")]
    assert "kombinatorfunds.com" in got


def test_unknown_client_lists_nothing(monkeypatch):
    monkeypatch.setattr(zapmail_asset_sync, "read_tracker", lambda: {})
    assert _only_client(ROWS, "Darlean") == []
