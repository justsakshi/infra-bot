"""metrics_only.py and the daily digest (16 Nov 2026): a failed campaign-stats run is one "broken" ask (what was
not read, where the log is, the env var names when a key is missing), a clean run clears it, and the old post goes
out when the digest can't be written or is off. No network."""
import pytest

import metrics_only as mo
from smartlead import digest as dg

FAILED = ["Expandi BETTRDATA (no EXPANDEE_API_KEY + EXPANDEE_SECRET on this server)", "HeyReach MELIOR"]


@pytest.fixture
def run(monkeypatch):
    def go(code, failed, report_ok=True, dry=False):
        alerts, reports = [], []
        monkeypatch.setattr(mo, "_alert", lambda text: alerts.append(text))
        monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None:
                            reports.append((src, asks, done, detail)) or report_ok)
        mo.hand_over(code, failed, dry)
        return alerts, reports
    return go


def test_with_the_digest_on_a_failed_run_is_one_broken_ask_naming_the_missing_keys(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    alerts, reports = run(2, FAILED)
    assert alerts == []
    (src, (a,), done, detail), = reports
    assert src == "campaign-metrics" and done == [] and a.kind == "broken" and a.key == "campaign-metrics:failed"
    assert a.text == "The 9:00 campaign stats job did not finish cleanly; not read: Expandi BETTRDATA; HeyReach MELIOR."
    assert a.step == ("Set EXPANDEE_API_KEY and EXPANDEE_SECRET on Render (Environment); the log is on Render, "
                      "search for [campaign-metrics].")
    assert "Being fixed" not in a.step and detail[0].startswith("⚠️ Campaign stats run failed")


def test_a_failure_with_no_missing_key_says_where_the_log_is(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    _, reports = run(1, [])
    a = reports[0][1][0]
    assert a.text == "The 9:00 campaign stats job did not finish cleanly (exit 1)."
    assert a.step == ("Read why in the log on Render (search for [campaign-metrics]); the job runs again tomorrow "
                      "at 9:00 IST.")


def test_a_clean_run_clears_the_ask_and_posts_nothing(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    alerts, reports = run(0, [])
    assert reports == [("campaign-metrics", [], [], [])] and alerts == []


def test_when_the_digest_cannot_be_written_or_is_off_the_old_post_goes_out(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    alerts, reports = run(2, FAILED, report_ok=False)
    assert reports and alerts == [mo.failure_text(2, FAILED)]
    assert alerts[0].endswith("Not read: " + "; ".join(FAILED))
    monkeypatch.setenv("DIGEST_MODE", "off")
    alerts, reports = run(2, FAILED)
    assert reports == [] and alerts == [mo.failure_text(2, FAILED)]
    alerts, reports = run(0, [])
    assert reports == [] and alerts == []


def test_a_dry_run_says_nothing(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    assert run(2, FAILED, dry=True) == ([], [])


def test_a_failed_source_says_why_so_nobody_needs_the_render_log():
    """16 Nov: 'Not read: Expandi BETTRDATA' every day 11-16 Nov; the PC read it fine, the reason was only in
    Render's log. The failure now carries the error type (never its text) into the digest line."""
    import metrics_only as mo
    a = mo.digest_ask(2, ["Expandi BETTRDATA (ReadTimeout)", "HeyReach MELIOR"])
    assert a.text == ("The 9:00 campaign stats job did not finish cleanly; not read: Expandi BETTRDATA (ReadTimeout); "
                      "HeyReach MELIOR.")


def test_an_expandi_workspace_failure_is_named_with_its_error_type(monkeypatch):
    import asyncio
    from datetime import datetime
    from types import SimpleNamespace
    from smartlead import expandi_rows as er

    class Boom:
        def __init__(self, *a): pass
        async def __aenter__(self): raise TimeoutError("http://x.com/?secret=s")
        async def __aexit__(self, *a): return False
    monkeypatch.setattr(er, "discover_expandi_workspaces", lambda: [SimpleNamespace(name="BETTRDATA", api_key="k", api_secret="s")])
    monkeypatch.setattr(er, "ExpandiClient", Boom)
    monkeypatch.setattr(er, "ExpandiStore", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(er, "ExpandiLeadStore", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(er.cm, "row_client_wanted", lambda *a: True)
    failed = []
    asyncio.run(er.build_expandi_rows(datetime(2026, 11, 16), datetime(2026, 11, 1), failed=failed))
    assert failed == ["Expandi BETTRDATA (TimeoutError)"]


def test_expandi_saying_invalid_credentials_is_its_own_error_and_other_400s_are_not():
    """17 Nov (t267): Render's run failed 11-17 Nov with HTTPStatusError 400. Expandi answers a wrong key/secret
    pair with 400 {"error": "invalid credentials"} (a garbage value just hangs), so that 400 means the env pair."""
    import asyncio
    import httpx
    from smartlead import expandi as ex

    def client_for(status, body):
        c = ex.ExpandiClient("KEYVALUE1", "SECRETVALUE2", "BETTRDATA")
        c._client = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: httpx.Response(status, json=body)))
        return c
    with pytest.raises(ex.ExpandiCredentialsRejected) as e:
        asyncio.run(client_for(400, {"error": "invalid credentials"}).list_li_accounts())
    assert not any(v in str(e.value) for v in ("http", "KEYVALUE1", "SECRETVALUE2"))
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(client_for(400, {"error": "page_size too big"}).list_li_accounts())


@pytest.mark.parametrize("suffix,key_var,secret_var", [
    ("", "EXPANDEE_API_KEY", "EXPANDEE_SECRET"),
    ("_ACME", "EXPANDEE_API_KEY_ACME", "EXPANDEE_SECRET_ACME")])
def test_a_rejected_pair_is_named_by_its_env_vars(monkeypatch, suffix, key_var, secret_var):
    import asyncio
    from datetime import datetime
    from types import SimpleNamespace
    from smartlead import expandi_rows as er
    from smartlead.expandi import ExpandiCredentialsRejected
    from smartlead.expandi_accounts import discover_expandi_workspaces

    for k in [k for k in __import__("os").environ if k.startswith("EXPANDEE_")]:
        monkeypatch.delenv(k)
    monkeypatch.setenv(key_var, "k")
    monkeypatch.setenv(secret_var, "s")
    (ws,) = discover_expandi_workspaces()
    assert (ws.key_var, ws.secret_var) == (key_var, secret_var)

    class Rejected:
        def __init__(self, *a): pass
        async def __aenter__(self): raise ExpandiCredentialsRejected()
        async def __aexit__(self, *a): return False
    monkeypatch.setattr(er, "discover_expandi_workspaces", lambda: [ws])
    monkeypatch.setattr(er, "ExpandiClient", Rejected)
    monkeypatch.setattr(er, "ExpandiStore", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(er, "ExpandiLeadStore", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(er.cm, "row_client_wanted", lambda *a: True)
    failed = []
    asyncio.run(er.build_expandi_rows(datetime(2026, 11, 17), datetime(2026, 11, 1), failed=failed))
    assert failed == [f"Expandi {ws.name} (Expandi rejected {key_var} + {secret_var} on this server)"]


def test_a_rejected_pair_tells_manveen_which_env_vars_to_set(run, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    _, reports = run(2, ["Expandi BETTRDATA (Expandi rejected EXPANDEE_API_KEY + EXPANDEE_SECRET on this server)"])
    a = reports[0][1][0]
    assert a.text == "The 9:00 campaign stats job did not finish cleanly; not read: Expandi BETTRDATA."
    assert a.step.startswith("Set EXPANDEE_API_KEY and EXPANDEE_SECRET on Render (Environment)")
