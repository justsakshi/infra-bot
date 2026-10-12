"""Zapmail domain forwarding (redirect) for bought domains. No network.

The endpoint (POST /v2/domains/forwarding) also takes `contains` (a name
filter) and `tagIds`; how they combine with `domainIds` is undocumented, so
`contains` is always the exact domain: only that one domain can ever match.
"""
import asyncio
from contextlib import asynccontextmanager

import pytest

from smartlead import domain_lifecycle as dl
from smartlead import inbox_jobs as ij
from smartlead.zapmail import ZapmailClient, ZapmailSpendBlocked


def run(coro):
    return asyncio.run(coro)


def test_client_needs_approval_one_domain_and_a_name_filter(monkeypatch):
    z = ZapmailClient(api_key="k")
    with pytest.raises(ZapmailSpendBlocked):
        run(z.add_domain_forwarding("d1", "preciseleads.in", contains="a.com"))
    with pytest.raises(ValueError):
        run(z.add_domain_forwarding("", "preciseleads.in", contains="a.com", approve=True))
    with pytest.raises(ValueError):
        run(z.add_domain_forwarding("d1", "preciseleads.in", contains="", approve=True))
    with pytest.raises(ValueError):
        run(z.add_domain_forwarding("d1", "not a host", contains="a.com", approve=True))
    sent = []

    async def fake(method, path, **kw):
        sent.append((method, path, kw))
        return {"status": 200, "message": "ok", "data": None}
    monkeypatch.setattr(z, "_request", fake)
    run(z.add_domain_forwarding("d1", "preciseleads.in", contains="a.com", approve=True))
    assert sent == [("POST", "/v2/domains/forwarding",
                     {"json": {"domainIds": ["d1"], "forwardTo": "https://preciseleads.in", "contains": "a.com", "tagIds": []}})]


class FakeZ:
    def __init__(self):
        self.calls = []

    async def add_domain_forwarding(self, domain_id, forward_to, *, contains, approve=False):
        assert approve
        self.calls.append((domain_id, forward_to, contains))
        return {"status": 200}


@pytest.fixture
def zap(monkeypatch):
    z = FakeZ()

    @asynccontextmanager
    async def fake_open(client, provider=None):
        yield z

    async def provider(domain, *, client):
        return "GOOGLE" if domain == "a.com" else None

    async def row(_z, domain):
        if domain != "a.com":
            return None
        return {"id": "d1", "domain": domain, "forwardTo": "https://PreciseLeads.in" if z.calls else None}
    monkeypatch.setattr(dl, "open_client", fake_open)
    monkeypatch.setattr(dl, "domain_provider", provider)
    monkeypatch.setattr(dl, "_domain_row", row)
    return z


def test_set_forwarding_is_a_dry_run_first_and_sends_a_bare_host(zap):
    r = run(dl.set_domain_forwarding("A.com", "https://preciseleads.in/", client="Precise Leads"))
    assert r["dry_run"] and r["forward_to"] == "preciseleads.in" and zap.calls == []
    r = run(dl.set_domain_forwarding("a.com", "https://preciseleads.in/", client="Precise Leads", approve=True))
    assert r["ok"] and r["confirmed"] == "https://PreciseLeads.in" and zap.calls == [("d1", "preciseleads.in", "a.com")]


def test_set_forwarding_says_so_when_zapmail_did_not_keep_it(zap, monkeypatch):
    async def row(_z, domain):
        return {"id": "d1", "domain": domain, "forwardTo": "https://other.com"}
    monkeypatch.setattr(dl, "_domain_row", row)
    r = run(dl.set_domain_forwarding("a.com", "preciseleads.in", client="Precise Leads", approve=True))
    assert r["ok"] is False and "other.com" in r["error"]


def test_set_forwarding_refuses_a_domain_not_on_the_account(zap):
    r = run(dl.set_domain_forwarding("b.com", "preciseleads.in", client="Precise Leads", approve=True))
    assert r["ok"] is False and zap.calls == []


def test_new_jobs_redirect_right_after_the_domains_are_ready():
    steps = ij.STEPS["new"]
    assert steps.index("redirect") == steps.index("domains_active") + 1
    job = ij.new_job(client="Precise Leads", kind="new", provider="GOOGLE", domains=["a.com"],
                     inboxes_per_domain=3, redirect="https://preciseleads.in")
    assert job["redirect"] == "https://preciseleads.in"
    assert "redirect" not in ij.STEPS["owned"]


def _steps(monkeypatch, result):
    from smartlead import inbox_job_steps as ijs
    calls = []

    async def fake(domain, forward_to, *, client, approve=False):
        calls.append((domain, forward_to, client, approve))
        return result
    monkeypatch.setattr(dl, "set_domain_forwarding", fake)
    return ijs.RealSteps(store=None), calls


def test_redirect_step_sets_every_domain(monkeypatch):
    steps, calls = _steps(monkeypatch, {"ok": True})
    job = {"client": "Precise Leads", "domains": ["a.com", "b.com"], "redirect": "https://preciseleads.in"}
    out = run(steps.redirect(job, {}))
    assert out.state == "done" and [c[0] for c in calls] == ["a.com", "b.com"] and all(c[3] for c in calls)


def test_redirect_step_without_a_redirect_does_nothing(monkeypatch):
    steps, calls = _steps(monkeypatch, {"ok": True})
    out = run(steps.redirect({"client": "Melior", "domains": ["a.com"]}, {}))
    assert out.state == "done" and calls == []


def test_redirect_step_failure_stops_the_job(monkeypatch):
    steps, calls = _steps(monkeypatch, {"ok": False, "error": "nope"})
    out = run(steps.redirect({"client": "Melior", "domains": ["a.com"], "redirect": "x.com"}, {}))
    assert out.state == "failed" and "nope" in out.detail
