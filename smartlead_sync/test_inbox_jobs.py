"""Inbox setup jobs: validation, cost, approval, and the engine's money rules.

A fake set of steps stands in for Zapmail/Smartlead and counts every paid call,
so these tests can prove: nothing runs before approval, nothing paid runs with
spending off, a crash in the middle of a paid step never pays twice, a
definite refusal fails cleanly, and scheduled jobs wait for their date.
"""

from __future__ import annotations

import asyncio
import copy
from datetime import date

import pytest

from smartlead import inbox_jobs as ij
from smartlead.inbox_jobs import StepOutcome


# ── in-memory store with the JobStore interface ─────────────────────────────

class MemStore:
    def __init__(self):
        self.jobs, self.leases, self.saves = {}, set(), 0

    available = True

    def insert(self, job):
        self.jobs[job["job_id"]] = copy.deepcopy(job)

    def get(self, job_id):
        return copy.deepcopy(self.jobs.get(job_id))

    def save(self, job):
        self.saves += 1
        self.jobs[job["job_id"]] = copy.deepcopy(job)

    def list(self, statuses=None):
        return [copy.deepcopy(j) for j in self.jobs.values() if not statuses or j["status"] in statuses]

    def claim(self, job_id):
        if job_id in self.leases:
            return False
        self.leases.add(job_id)
        return True

    def release(self, job_id):
        self.leases.discard(job_id)


class FakeSteps:
    """Scripted outcomes per step; records every call."""

    def __init__(self, store, script=None):
        self.store, self.calls = store, []
        self.script = script or {}

    def save(self, job):
        self.store.save(job)

    def __getattr__(self, name):
        if name not in ij.STEP_LABELS:
            raise AttributeError(name)

        async def step(job, st):
            self.calls.append(name)
            todo = self.script.get(name)
            if isinstance(todo, list):
                todo = todo.pop(0) if todo else "done"
            if callable(todo):
                return todo(job, st)
            if isinstance(todo, Exception):
                raise todo
            if name == "create_inboxes":
                job["inboxes"] = [{"email": f"a@{d}"} for d in job["domains"]]
            return StepOutcome(todo or "done", f"{name} {todo or 'done'}")
        return step


def job(kind="owned", **kw):
    base = dict(client="Melior", kind=kind, provider="GOOGLE", domains=["gomelior.com"],
                inboxes_per_domain=2)
    if kind == "prewarmed":
        base.update(domains=["apexgtmcraft.co"], prewarmed_domain_id="pw-1")
    base.update(kw)
    j = ij.new_job(**base)
    j["cost"] = {"lines": [], "total_now_usd": 0.0}
    return j


def run(coro):
    return asyncio.run(coro)


# ── validation ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad, msg", [
    (dict(kind="rent"), "kind"), (dict(provider="yahoo"), "provider"),
    (dict(domains=[]), "at least one"), (dict(domains=["not a domain"]), "not a domain"),
    (dict(inboxes_per_domain=6), "1-5"), (dict(names=["Cher"]), "First Last"),
    (dict(run_on="tomorrow"), "isoformat"),
])
def test_bad_requests_are_refused(bad, msg):
    with pytest.raises(ValueError, match=msg):
        ij.new_job(**{**dict(client="Melior", kind="owned", provider="GOOGLE",
                             domains=["gomelior.com"]), **bad})


def test_new_outlook_domains_are_allowed():
    """Zapmail 2026-09-30: x-service-provider on /buy files the domain under Outlook."""
    j = ij.new_job(client="Melior", kind="new", provider="MICROSOFT", domains=["a.com"])
    assert j["provider"] == "MICROSOFT" and j["steps"][0]["name"] == "buy_domains"


def test_topping_up_an_owned_domain_counts_only_the_missing_inboxes():
    c = ij.estimate(job(), domain_prices={}, free_slots=0, plan="growth", needed=1)
    assert c["monthly_usd"] == 3.25


def test_prewarmed_needs_exactly_one_chosen_domain():
    with pytest.raises(ValueError, match="one chosen domain"):
        ij.new_job(client="Melior", kind="prewarmed", provider="GOOGLE", domains=["a.co", "b.co"])


def test_steps_per_kind():
    assert [s["name"] for s in job("new")["steps"]][:3] == ["buy_domains", "domains_active", "inbox_slots"]
    assert job("owned")["steps"][0]["name"] == "inbox_slots"
    assert [s["name"] for s in job("prewarmed")["steps"]][:2] == ["prewarmed_slot", "assign_prewarmed"]


# ── cost ────────────────────────────────────────────────────────────────────

def test_cost_counts_domains_missing_slots_and_monthly_separately():
    j = job("new", domains=["a.com", "b.com"], inboxes_per_domain=3)
    c = ij.estimate(j, domain_prices={"a.com": 12.99, "b.com": 12.99}, free_slots=2, plan="Growth")
    assert c["once_usd"] == 25.98
    assert c["monthly_usd"] == round(4 * 3.25, 2)         # 6 needed, 2 free
    assert c["total_now_usd"] == round(25.98 + 13.0, 2)
    assert "4 extra inbox slot(s) (2 free, 6 needed)" in c["lines"][-1]["what"]


def test_no_slot_cost_when_enough_are_free():
    c = ij.estimate(job(), domain_prices={}, free_slots=10, plan="growth")
    assert c["lines"] == [] and c["total_now_usd"] == 0


def test_prewarmed_with_a_free_slot_costs_nothing_new():
    """Zapmail 2026-09-30: assigning is free; the listed domain price is not charged."""
    c = ij.estimate(job("prewarmed"), domain_prices={}, free_slots=0, plan="growth", free_prewarmed=2)
    assert c["total_now_usd"] == 0 and "no new charge" in c["lines"][0]["what"]


def test_prewarmed_without_a_free_slot_costs_a_monthly_plan():
    c = ij.estimate(job("prewarmed"), domain_prices={}, free_slots=0, plan="growth", free_prewarmed=0)
    assert c["first_month_usd"] == 39.0 and "then $24/month" in c["lines"][0]["when"]
    assert "only while it renews" in c["lines"][0]["what"]
    assert "prewarmed_slot" in ij.PAID_STEPS and "assign_prewarmed" not in ij.PAID_STEPS


# ── approval gates ──────────────────────────────────────────────────────────

def test_nothing_runs_before_approval():
    st = MemStore(); j = job(); st.insert(j)
    steps = FakeSteps(st)
    assert run(ij.tick(st, steps, spend_ok=True)) == []
    assert steps.calls == []


def test_approval_needs_a_cost_estimate():
    st = MemStore(); j = job(); j["cost"] = None; st.insert(j)
    with pytest.raises(ValueError, match="cost estimate"):
        ij.approve(st, j["job_id"], "U1")


def test_happy_path_runs_every_step_in_order():
    st = MemStore(); j = job(); st.insert(j)
    ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st)
    run(ij.tick(st, steps, spend_ok=True))
    done = st.get(j["job_id"])
    assert done["status"] == "done"
    assert steps.calls == ij.STEPS["owned"]
    assert "approved by U1" in done["log"][0]


def test_spending_off_stops_before_any_paid_step():
    st = MemStore(); j = job("new"); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st)
    run(ij.tick(st, steps, spend_ok=False))
    got = st.get(j["job_id"])
    assert steps.calls == [] and got["status"] == "waiting"
    assert "switched off" in got["steps"][0]["detail"]


def test_waiting_step_resumes_on_the_next_tick():
    st = MemStore(); j = job(); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st, {"inboxes_active": ["waiting", "waiting", "done"]})
    for _ in range(3):
        run(ij.tick(st, steps, spend_ok=True))
    assert st.get(j["job_id"])["status"] == "done"
    assert steps.calls.count("create_inboxes") == 1          # never re-created
    assert steps.calls.count("inboxes_active") == 3


def test_scheduled_job_waits_for_its_date():
    st = MemStore(); j = job(run_on="2026-10-05"); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st)
    run(ij.tick(st, steps, spend_ok=True, today=date(2026, 10, 4)))
    assert steps.calls == [] and st.get(j["job_id"])["status"] == "waiting"
    run(ij.tick(st, steps, spend_ok=True, today=date(2026, 10, 5)))
    assert st.get(j["job_id"])["status"] == "done"


def test_uncertain_paid_step_stops_for_a_person_and_never_repeats():
    st = MemStore(); j = job("new"); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st, {"buy_domains": TimeoutError("network died mid-purchase")})
    run(ij.tick(st, steps, spend_ok=True))
    run(ij.tick(st, steps, spend_ok=True))
    got = st.get(j["job_id"])
    assert got["status"] == "needs_check"
    assert steps.calls == ["buy_domains"]                     # not retried by the next tick


def test_definite_refusal_fails_cleanly():
    from smartlead.zapmail import ZapmailHTTPError
    st = MemStore(); j = job("new"); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st, {"buy_domains": ZapmailHTTPError("400 insufficient wallet", 400)})
    run(ij.tick(st, steps, spend_ok=True))
    assert st.get(j["job_id"])["status"] == "failed"


def test_a_crash_in_a_free_step_fails_it_without_touching_paid_ones():
    st = MemStore(); j = job(); st.insert(j); ij.approve(st, j["job_id"], "U1")
    steps = FakeSteps(st, {"into_smartlead": RuntimeError("smartlead 502")})
    run(ij.tick(st, steps, spend_ok=True))
    got = st.get(j["job_id"])
    assert got["status"] == "failed"
    assert [s["state"] for s in got["steps"]][:4] == ["done", "done", "done", "failed"]


def test_every_step_is_saved_as_it_finishes():
    st = MemStore(); j = job(); st.insert(j); ij.approve(st, j["job_id"], "U1")
    before = st.saves
    run(ij.tick(st, FakeSteps(st), spend_ok=True))
    assert st.saves - before >= len(ij.STEPS["owned"])


def test_two_runners_never_advance_the_same_job():
    st = MemStore(); j = job(); st.insert(j); ij.approve(st, j["job_id"], "U1")
    st.claim(j["job_id"])                                     # another runner holds it
    steps = FakeSteps(st)
    run(ij.tick(st, steps, spend_ok=True))
    assert steps.calls == []


def test_cancel_keeps_a_note_when_something_was_bought():
    st = MemStore(); j = job("new"); j["steps"][0]["state"] = "done"; st.insert(j)
    got = ij.cancel(st, j["job_id"], "U1")
    assert got["status"] == "cancelled" and "what was bought stays" in got["log"][-1]


def test_progress_line_reads_plainly():
    j = job()
    j["steps"][0]["state"] = "done"; j["steps"][1]["state"] = "waiting"
    assert ij.progress_line(j).startswith("Inbox slots ✓ · Create inboxes …")
