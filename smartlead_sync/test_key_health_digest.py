"""key_health_monitor.py and the daily digest (16 Nov 2026): a dead Smartlead key is an urgent ask (an unreachable one a broken line)
with where to replace it, all keys valid clears it, and the old alert goes out when the digest can't be written
or is off. No network."""
import asyncio
from types import SimpleNamespace

import pytest

import key_health_monitor as khm
from smartlead import digest as dg

STATES = {"PRECISE_LEADS": "OK", "BETTRDATA": "DEAD_KEY", "MELIOR": "UNREACHABLE"}


@pytest.fixture
def run(monkeypatch):
    def go(states, report_ok=True):
        alerts, reports = [], []

        async def probe(acc):
            st = states[acc.name]
            detail = {"OK": "1 account(s) visible", "DEAD_KEY": "HTTP 401: bad key",
                      "UNREACHABLE": "ConnectTimeout: timed out"}[st]
            return {"client": acc.name, "state": st, "detail": detail, "key_tail": "abcd1234"}
        monkeypatch.setattr(khm, "discover_accounts", lambda: [SimpleNamespace(name=n, api_key="k") for n in states])
        monkeypatch.setattr(khm, "_probe", probe)
        monkeypatch.setattr(khm, "_save", lambda results, at: None)
        monkeypatch.setattr(khm, "_alert", lambda dead, unreachable: alerts.append((dead, unreachable)))
        monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None:
                            reports.append((src, asks, done, detail)) or report_ok)
        with pytest.raises(SystemExit) as code:
            asyncio.run(khm.main())
        return code.value.code, alerts, reports
    return go


def test_with_the_digest_on_a_dead_key_is_an_urgent_ask_per_account_and_no_alert(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    code, alerts, reports = run(STATES)
    assert code == 2 and alerts == []
    (src, asks, done, detail), = reports
    assert src == "key-health" and done == []
    assert [(a.key, a.urgent) for a in asks] == [("key-health:dead:BETTRDATA", True),
                                                 ("key-health:unreachable:MELIOR", False)]
    assert asks[0].text == "Smartlead rejects the BETTRDATA API key, so every job is blind for that account."
    assert "SMARTLEAD_API_KEY_BETTRDATA on Render" in asks[0].step and "plan is paid" in asks[0].step
    assert "(ConnectTimeout)" in asks[1].text
    assert "abcd1234" not in "\n".join([a.text + a.step for a in asks] + detail)      # never any part of a key
    assert detail[0].startswith("🚨 Smartlead API key alert: 2 accounts")


def test_all_keys_valid_hands_over_an_empty_list(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    code, alerts, reports = run({"PRECISE_LEADS": "OK"})
    assert code == 0 and reports == [("key-health", [], [], [])] and alerts == []


def test_when_the_digest_cannot_be_written_the_old_alert_goes_out(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    code, alerts, reports = run(STATES, report_ok=False)
    assert code == 2 and reports and [r["client"] for r in alerts[0][0]] == ["BETTRDATA"]


def test_with_the_digest_off_the_alert_goes_out_and_nothing_is_reported(run):
    code, alerts, reports = run(STATES)
    assert code == 2 and reports == [] and len(alerts) == 1


def test_the_default_account_names_its_own_env_var():
    assert khm.env_name("Belardi Wong") == "SMARTLEAD_API_KEY" and khm.env_name("MELIOR") == "SMARTLEAD_API_KEY_MELIOR"


def test_an_unreachable_key_is_a_broken_line_not_an_urgent_post():
    """Lead review 16 Nov: a network blip must not page Manveen; a rejected key still does."""
    import key_health_monitor as khm
    asks = khm.digest_asks([{"client": "A"}], [{"client": "B", "state": "UNREACHABLE", "detail": "ConnectTimeout: x"}])
    assert [(a.key, a.urgent, a.kind) for a in asks] == [("key-health:dead:A", True, "ask"),
                                                         ("key-health:unreachable:B", False, "broken")]
