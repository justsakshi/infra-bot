"""Retry failed inboxes, retire inboxes at renewal, jobs retrying once."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from smartlead import inbox_jobs as ij
from smartlead import inbox_maintenance as im
from smartlead.inbox_job_steps import RealSteps
from smartlead.zapmail import ZapmailClient, ZapmailSpendBlocked


def run(c):
    return asyncio.run(c)


class FakeZ:
    def __init__(self, boxes):
        self.boxes, self.retried, self.removed = boxes, [], []

    async def list_mailboxes(self, contains=None, page=1, limit=50):
        return {"data": {"domains": [{"mailboxes": [b for b in self.boxes if b["email"].endswith("@" + contains)]}]}}

    async def retry_failed_mailboxes(self, ids, approve=False):
        assert approve
        self.retried.append(ids)
        return {"status": 200}

    async def schedule_mailbox_removal(self, ids, remove=True, approve=False):
        assert approve
        self.removed.append((ids, remove))
        return {"message": "ok"}


@pytest.fixture
def world(monkeypatch):
    import smartlead.zapmail_accounts as za
    import smartlead.zapmail_fleet as zf
    boxes = [{"id": "m1", "email": "ryan@gomelior.com", "status": "ACTIVE"},
             {"id": "m2", "email": "ryanm@gomelior.com", "status": "FAILED"}]
    z = FakeZ(boxes)

    async def locate(domain):
        if domain != "gomelior.com":
            return []
        return [{"account": "PRECISE_LEADS", "provider": "GOOGLE", "domain_id": "d1",
                 "mailboxes": [{"email": b["email"], "status": b["status"]} for b in boxes]}]

    @asynccontextmanager
    async def fake_open(client, provider=None):
        yield z

    monkeypatch.setattr(zf, "locate_domain", locate)
    monkeypatch.setattr(za, "open_client", fake_open)
    monkeypatch.setattr(za, "require_account", lambda c: type("A", (), {"name": "PRECISE_LEADS"})())
    return z


def test_client_guards():
    z = ZapmailClient(api_key="k")
    with pytest.raises(ZapmailSpendBlocked):
        run(z.retry_failed_mailboxes(["d1"]))
    with pytest.raises(ZapmailSpendBlocked):
        run(z.schedule_mailbox_removal(["m1"]))
    with pytest.raises(ValueError):
        run(z.schedule_mailbox_removal([], approve=True))


def test_retry_is_a_dry_run_first_then_uses_the_domain_id(world):
    r = run(im.retry_failed("gomelior.com", client="Melior"))
    assert r["dry_run"] and r["would_retry"] == ["ryanm@gomelior.com"] and world.retried == []
    r = run(im.retry_failed("gomelior.com", client="Melior", approve=True))
    assert r["ok"] and world.retried == [["d1"]]


def test_retire_by_mailbox_id_and_undo(world):
    r = run(im.retire(["Ryan@gomelior.com"], client="Melior"))
    assert r["dry_run"] and r["inboxes"] == ["ryan@gomelior.com"] and world.removed == []
    assert run(im.retire(["ryan@gomelior.com"], client="Melior", approve=True))["ok"]
    assert run(im.retire(["ryan@gomelior.com"], client="Melior", approve=True, undo=True))["ok"]
    assert world.removed == [(["m1"], True), (["m1"], False)]


def test_retire_refuses_inboxes_that_are_not_the_clients(world):
    r = run(im.retire(["x@other.com"], client="Melior", approve=True))
    assert not r["ok"] and "x@other.com" in r["error"] and world.removed == []


class Saves:
    def save(self, job):
        pass


def test_a_job_retries_failed_inboxes_once_then_stops(world):
    j = ij.new_job(client="Melior", kind="owned", provider="GOOGLE", domains=["gomelior.com"])
    j["inboxes"] = [{"email": "ryan@gomelior.com"}, {"email": "ryanm@gomelior.com"}]
    st = next(s for s in j["steps"] if s["name"] == "inboxes_active")
    out = run(RealSteps(Saves()).inboxes_active(j, st))
    assert out.state == "waiting" and "retrying 1 failed" in out.detail and world.retried == [["d1"]]
    out = run(RealSteps(Saves()).inboxes_active(j, st))       # still failed after the retry
    assert out.state == "failed" and "already retried once" in out.detail
    assert world.retried == [["d1"]]                           # not retried again
