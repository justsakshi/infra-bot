"""Inbox setup jobs: buy -> create -> Smartlead -> name/signature/warmup -> tracker.

Steps 2-4 of docs/INBOX_PIPELINE_PLAN.md. A job is one request from the team
("3 Google inboxes on each of these 2 new domains for Melior"), saved in Mongo
and moved forward one step at a time by ``advance`` (called every few minutes
by Infra Bot, and right after approval). Nothing waits inside a step: a step
that needs Zapmail to finish something returns ``waiting`` and is re-checked on
the next tick, so a restart or crash simply resumes.

Three kinds of job:

  new        buy domains -> domains active -> inbox slots -> create inboxes ->
             inboxes active -> into Smartlead -> Smartlead setup -> tracker
  owned      (domain we already have) inbox slots -> create inboxes -> ...
  prewarmed  pre-warmed slot (a plan only if none is free) -> assign the chosen domain -> ...

Money rules (same as domain buying):
  * a job does nothing until a person approves it, after seeing its cost;
  * paid steps also need ZAPMAIL_ALLOW_SPEND=true on the server;
  * a paid step records "attempted" BEFORE it calls Zapmail, so if the process
    dies mid-call the next run does not pay again: it stops as ``needs_check``
    for a person to confirm (domain buys use the purchase ledger, which already
    works this way);
  * inbox slots are bought at most once per job, from the wallet only (Zapmail
    never charges a card for slots): the wallet is checked first; a pre-warmed
    plan is bought only when no slot is free, at most once, and only when the
    wallet covers its first month.

Where inboxes go in Smartlead:
  * a client with a Zapmail export target (BettrData, Belardi Wong): Zapmail export;
  * otherwise (Precise Leads, Melior share a Smartlead Zapmail cannot export
    to): added directly with the inbox's own login (Google: app password).
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

JOBS_COLLECTION = os.getenv("INBOX_JOBS_COLLECTION", "inbox_setup_jobs")

KINDS = ("new", "owned", "prewarmed")
PROVIDERS = ("GOOGLE", "MICROSOFT")
STEPS: dict[str, list[str]] = {
    "new": ["buy_domains", "domains_active", "inbox_slots", "create_inboxes",
            "inboxes_active", "into_smartlead", "smartlead_setup", "tracker"],
    "owned": ["inbox_slots", "create_inboxes", "inboxes_active", "into_smartlead",
              "smartlead_setup", "tracker"],
    # Zapmail (2026-09-30, second answer): assigning is FREE but needs a free
    # slot on a pre-warmed subscription; the subscription is what costs money,
    # monthly. So the paid step is getting a slot (buying a plan only when none
    # is free), and assigning the chosen domain comes after it.
    "prewarmed": ["prewarmed_slot", "assign_prewarmed", "inboxes_active",
                  "into_smartlead", "smartlead_setup", "tracker"],
}
PAID_STEPS = frozenset({"buy_domains", "inbox_slots", "prewarmed_slot"})
STEP_LABELS = {
    "buy_domains": "Buy domains", "domains_active": "Domains ready",
    "inbox_slots": "Inbox slots", "create_inboxes": "Create inboxes",
    "inboxes_active": "Inboxes ready", "into_smartlead": "Into Smartlead",
    "smartlead_setup": "Name, signature, warmup", "tracker": "Tracker",
    "prewarmed_slot": "Pre-warmed slot", "assign_prewarmed": "Assign pre-warmed domain",
}
MAX_INBOXES_PER_DOMAIN = 5
# Pre-warmed plans when no slot is free: first month / then monthly / inboxes
# (Zapmail docs 2026-09-29). Billed monthly; inboxes stay only while it renews.
PREWARMED_PLANS = {"starter": (39.0, 24.0, 3), "growth": (149.0, 84.0, 12), "pro": (339.0, 180.0, 30)}
PREWARMED_PLAN = "starter"   # one domain's worth (3 inboxes)

# Monthly price of one extra inbox slot by Zapmail plan (docs, 2026-09-29).
ADDON_PRICE = {"starter": 3.50, "growth": 3.25, "pro": 3.00}

_DOMAIN_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z]{2,})+$")
_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z' .-]{0,39}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── job document ────────────────────────────────────────────────────────────

@dataclass
class StepOutcome:
    state: str                      # done | waiting | failed | needs_check
    detail: str = ""
    data: dict = field(default_factory=dict)


def new_job(
    *,
    client: str,
    kind: str,
    provider: str,
    domains: list[str],
    inboxes_per_domain: int = 3,
    names: list[str] | None = None,
    signature: bool = False,
    run_on: str | None = None,
    prewarmed_domain_id: str | None = None,
    created_by: str = "",
) -> dict:
    """Validate a request and build its job document (status: awaiting_approval)."""
    kind = (kind or "").strip().lower()
    provider = (provider or "").strip().upper()
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}")
    if provider not in PROVIDERS:
        raise ValueError("provider must be GOOGLE or MICROSOFT")
    doms = [d.strip().lower() for d in domains or [] if d.strip()]
    doms = list(dict.fromkeys(doms))
    if not doms:
        raise ValueError("give at least one domain")
    bad = [d for d in doms if not _DOMAIN_RE.match(d)]
    if bad:
        raise ValueError(f"not a domain: {', '.join(bad)}")
    if kind == "prewarmed":
        if len(doms) != 1 or not prewarmed_domain_id:
            raise ValueError("a pre-warmed job is one chosen domain (with its Zapmail id)")
    elif not 1 <= int(inboxes_per_domain) <= MAX_INBOXES_PER_DOMAIN:
        raise ValueError(f"inboxes per domain must be 1-{MAX_INBOXES_PER_DOMAIN}")
    clean_names = []
    for full in names or []:
        parts = [p for p in str(full).strip().split() if p]
        if not parts:
            continue
        if len(parts) < 2 or not all(_NAME_RE.match(p) for p in parts):
            raise ValueError(f"sender names are 'First Last' in letters: {full!r}")
        clean_names.append(" ".join(parts))
    if run_on:
        date.fromisoformat(run_on)          # raises on a bad date
    return {
        "job_id": uuid.uuid4().hex[:10],
        "client": client, "kind": kind, "provider": provider, "domains": doms,
        "inboxes_per_domain": int(inboxes_per_domain) if kind != "prewarmed" else 0,
        "names": clean_names, "signature": bool(signature), "run_on": run_on,
        "prewarmed_domain_id": prewarmed_domain_id,
        "status": "awaiting_approval", "created_by": created_by, "created_at": _now(),
        "approved_by": None, "approved_at": None, "cost": None,
        "inboxes": [],                      # [{email, first, last}] once known
        "steps": [{"name": s, "state": "pending", "detail": "", "data": {}, "at": None}
                  for s in STEPS[kind]],
        "log": [],
    }


def current_step(job: dict) -> dict | None:
    return next((s for s in job["steps"] if s["state"] != "done"), None)


def progress_line(job: dict) -> str:
    """'Buy domains ✓ · Domains ready … · Create inboxes' — for Slack / web."""
    marks = {"done": "✓", "waiting": "…", "failed": "✗", "needs_check": "?", "pending": ""}
    return " · ".join(f"{STEP_LABELS[s['name']]} {marks.get(s['state'], '')}".strip()
                      for s in job["steps"])


# ── cost ────────────────────────────────────────────────────────────────────

def estimate(job: dict, *, domain_prices: dict[str, float], free_slots: int,
             plan: str, needed: int | None = None, free_prewarmed: int = 0) -> dict:
    """What approving this job will cost. Pure. ``needed`` overrides the inbox
    count when some already exist (a domain we own is topped up, not refilled)."""
    lines: list[dict] = []
    if job["kind"] == "new":
        for d in job["domains"]:
            lines.append({"what": f"domain {d} (1 year)", "usd": round(float(domain_prices[d]), 2),
                          "when": "once"})
    if job["kind"] in ("new", "owned"):
        if needed is None:
            needed = job["inboxes_per_domain"] * len(job["domains"])
        extra = max(0, needed - max(0, free_slots))
        if extra:
            each = ADDON_PRICE.get((plan or "growth").lower(), ADDON_PRICE["growth"])
            lines.append({"what": f"{extra} extra inbox slot(s) ({free_slots} free, {needed} needed)",
                          "usd": round(extra * each, 2), "when": "per month"})
    if job["kind"] == "prewarmed":
        if free_prewarmed >= 1:
            lines.append({"what": f"uses 1 of {free_prewarmed} free pre-warmed slot(s) on your existing "
                                  "subscription (already billed monthly) - no new charge",
                          "usd": 0.0, "when": "once"})
        else:
            first, renew, boxes = PREWARMED_PLANS[PREWARMED_PLAN]
            lines.append({"what": f"new pre-warmed {PREWARMED_PLAN} subscription ({boxes} inboxes; "
                                  "the inboxes stay only while it renews)",
                          "usd": first, "when": f"first month, then ${renew:.0f}/month"})
    once = round(sum(x["usd"] for x in lines if x["when"] == "once"), 2)
    monthly = round(sum(x["usd"] for x in lines if x["when"] == "per month"), 2)
    first_month = round(sum(x["usd"] for x in lines if x["when"].startswith("first month")), 2)
    return {"lines": lines, "once_usd": once, "monthly_usd": monthly,
            "first_month_usd": first_month, "total_now_usd": round(once + monthly + first_month, 2)}


# ── storage ─────────────────────────────────────────────────────────────────

class JobStore:
    """Mongo-backed jobs. ``collection`` lets tests pass a fake."""

    def __init__(self, collection=None) -> None:
        self._col = collection
        if self._col is None:
            from smartlead.domain_batch import _mongo_db
            db = _mongo_db()
            self._col = db[JOBS_COLLECTION] if db is not None else None
            if self._col is not None:
                try:
                    self._col.create_index("job_id", unique=True)
                except Exception:  # noqa: BLE001
                    pass

    @property
    def available(self) -> bool:
        return self._col is not None

    def _need(self):
        if self._col is None:
            raise RuntimeError("Mongo is unreachable - refusing to run a job we cannot record.")

    def insert(self, job: dict) -> None:
        self._need()
        self._col.insert_one(dict(job))

    def get(self, job_id: str) -> dict | None:
        if self._col is None:
            return None
        return self._col.find_one({"job_id": job_id}, {"_id": 0})

    def save(self, job: dict) -> None:
        self._need()
        job["updated_at"] = _now()
        self._col.replace_one({"job_id": job["job_id"]}, {k: v for k, v in job.items() if k != "_id"})

    def list(self, statuses: tuple[str, ...] | None = None) -> list[dict]:
        if self._col is None:
            return []
        q = {"status": {"$in": list(statuses)}} if statuses else {}
        return list(self._col.find(q, {"_id": 0}))

    def claim(self, job_id: str, lease_s: int = 900) -> bool:
        """Atomically become the one runner for this job (crash frees it after lease_s)."""
        self._need()
        now = datetime.now(timezone.utc).timestamp()
        res = self._col.update_one(
            {"job_id": job_id, "$or": [{"lease_until": None}, {"lease_until": {"$exists": False}},
                                       {"lease_until": {"$lt": now}}]},
            {"$set": {"lease_until": now + lease_s}})
        return bool(res.modified_count)

    def release(self, job_id: str) -> None:
        if self._col is not None:
            self._col.update_one({"job_id": job_id}, {"$set": {"lease_until": None}})


def approve(store: JobStore, job_id: str, by: str) -> dict:
    job = store.get(job_id)
    if not job:
        raise ValueError(f"no job {job_id}")
    if job["status"] != "awaiting_approval":
        raise ValueError(f"job {job_id} is {job['status']}, not awaiting approval")
    if not job.get("cost"):
        raise ValueError("job has no cost estimate yet - estimate it before approving")
    job.update(status="approved", approved_by=by, approved_at=_now())
    job["log"].append(f"{_now()} approved by {by} (${job['cost']['total_now_usd']:.2f} now)")
    store.save(job)
    return job


def cancel(store: JobStore, job_id: str, by: str) -> dict:
    job = store.get(job_id)
    if not job:
        raise ValueError(f"no job {job_id}")
    if job["status"] in ("done", "cancelled"):
        return job
    if any(s["state"] == "done" and s["name"] in PAID_STEPS for s in job["steps"]):
        job["log"].append(f"{_now()} cancelled by {by} after a paid step - what was bought stays")
    job.update(status="cancelled")
    store.save(job)
    return job


# ── the engine ──────────────────────────────────────────────────────────────

RUNNABLE = ("approved", "running", "waiting")


async def advance(job: dict, adapters, *, spend_ok: bool, today: date | None = None) -> dict:
    """Run a job's steps until one waits, fails or the job is done. Returns the job.

    The caller saves the job (the runner does it after every step via
    ``adapters.save``, so a crash loses at most the step in flight).
    """
    today = today or date.today()
    if job["status"] not in RUNNABLE:
        return job
    if job.get("run_on") and date.fromisoformat(job["run_on"]) > today:
        job["status"] = "waiting"
        _set_note(job, f"scheduled for {job['run_on']}")
        return job

    job["status"] = "running"
    while True:
        step = current_step(job)
        if step is None:
            job["status"] = "done"
            job["log"].append(f"{_now()} done")
            adapters.save(job)
            return job

        name = step["name"]
        if name in PAID_STEPS and not spend_ok:
            step.update(state="waiting", detail="spending is switched off on the server "
                        "(ZAPMAIL_ALLOW_SPEND) - nothing was charged", at=_now())
            job["status"] = "waiting"
            adapters.save(job)
            return job

        try:
            out: StepOutcome = await getattr(adapters, name)(job, step)
        except Exception as exc:  # noqa: BLE001 - one step's crash must not lose the job
            out = StepOutcome(_classify(exc, paid=name in PAID_STEPS),
                              f"{type(exc).__name__}: {str(exc)[:200]}")

        step.update(state=out.state, detail=out.detail, at=_now())
        step["data"].update(out.data or {})
        job["log"].append(f"{_now()} {name}: {out.state}{' - ' + out.detail if out.detail else ''}")
        if out.state == "done":
            adapters.save(job)
            continue
        job["status"] = {"waiting": "waiting", "failed": "failed",
                         "needs_check": "needs_check"}[out.state]
        adapters.save(job)
        return job


def _classify(exc: Exception, *, paid: bool) -> str:
    """A refusal we KNOW charged nothing fails the step; anything uncertain on
    a paid step stops the job for a person to check (never an automatic retry)."""
    from smartlead.zapmail import ZapmailHTTPError, ZapmailSpendBlocked
    if isinstance(exc, (ZapmailSpendBlocked, ZapmailHTTPError, ValueError, PermissionError)):
        return "failed"
    return "needs_check" if paid else "failed"


def _set_note(job: dict, note: str) -> None:
    step = current_step(job)
    if step is not None:
        step["detail"] = note


async def tick(store: JobStore, adapters, *, spend_ok: bool, today: date | None = None) -> list[dict]:
    """Advance every runnable job once. Returns ``[{job_id, status, step, detail}]``."""
    out = []
    for job in store.list(RUNNABLE):
        if not store.claim(job["job_id"]):
            continue
        try:
            fresh = store.get(job["job_id"]) or job
            before = (fresh["status"], (current_step(fresh) or {}).get("name"))
            fresh = await advance(fresh, adapters, spend_ok=spend_ok, today=today)
            store.save(fresh)
            step = current_step(fresh) or {}
            out.append({"job_id": fresh["job_id"], "client": fresh["client"],
                        "domains": fresh["domains"], "created_by": fresh.get("created_by"),
                        "status": fresh["status"], "step": step.get("name"),
                        "detail": step.get("detail", ""), "progress": progress_line(fresh),
                        "changed": before != (fresh["status"], step.get("name"))})
        finally:
            store.release(job["job_id"])
    return out
