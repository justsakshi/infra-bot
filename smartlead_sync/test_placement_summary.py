"""placement_summary exit codes: 0 only when every account was read and none is below the threshold
(audit 7 Nov 2026: a crash or an unread account was logged as "all mailboxes above threshold"). No network."""
import asyncio
from types import SimpleNamespace

import placement_summary as ps
from smartlead.smart_delivery import SmartDeliveryError


def row(email, pct):
    return {"email": email, "esp": "gmail", "inbox": int(pct), "total": 100, "spam": 100 - int(pct),
            "inbox_pct": float(pct), "tests": 3, "dkim_pass_pct": 100.0, "spf_pass_pct": 100.0}


def fake_client(by_key):
    class FakeSD:
        def __init__(self, key):
            self.key = key

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get_mailbox_summary(self):
            got = by_key[self.key]
            if isinstance(got, Exception):
                raise got
            return got
    return FakeSD


def run(monkeypatch, by_key, client=None):
    accounts = [SimpleNamespace(name=k.upper(), api_key=k) for k in by_key]
    monkeypatch.setattr(ps, "discover_accounts", lambda: accounts)
    monkeypatch.setattr(ps, "SmartDeliveryClient", fake_client(by_key))
    return asyncio.run(ps.run(client, 80.0))


def test_all_read_and_above_threshold_is_0(monkeypatch):
    assert run(monkeypatch, {"a": [row("a@example.com", 95)], "b": [row("b@example.com", 100)]}) == 0


def test_a_mailbox_below_threshold_is_2(monkeypatch, capsys):
    assert run(monkeypatch, {"a": [row("a@example.com", 40), row("c@example.com", 99)]}) == 2
    assert "example.com: 1 mailbox(es) below 80%" in capsys.readouterr().out


def test_an_unread_account_is_1_not_all_clear(monkeypatch, capsys):
    code = run(monkeypatch, {"a": [row("a@example.com", 95)], "b": SmartDeliveryError("401 Unauthorized")})
    assert code == 1
    assert "could not read 1 account(s): B" in capsys.readouterr().out


def test_below_threshold_wins_over_unread_but_both_are_printed(monkeypatch, capsys):
    code = run(monkeypatch, {"a": [row("a@example.com", 10)], "b": SmartDeliveryError("timeout")})
    assert code == 2
    assert "could not read 1 account(s): B" in capsys.readouterr().out


def test_no_accounts_is_1(monkeypatch):
    monkeypatch.setattr(ps, "discover_accounts", lambda: [])
    assert asyncio.run(ps.run(None, 80.0)) == 1


def test_unknown_client_is_still_1(monkeypatch):
    assert run(monkeypatch, {"a": [row("a@example.com", 95)]}, client="nope") == 1
