# Inbox Intake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every new inbox that lands in a current client's Smartlead account is set up once (daily limit, warmup, signature, client, tags), proven by reading it back, tracked to its ready-to-use date, and afterwards only watched.

**Architecture:** One pure module holds the standard and every rule about it (`smartlead/inbox_standard.py` + `inbox_standard.json`). One network module runs intake per Smartlead account with a Mongo ledger (`smartlead/inbox_intake.py`), driven by a CLI (`inbox_intake_cli.py`) on the existing 2-hour cron. The infra audit and the 09:40 digest read the same standard for drift and ready dates. The Zapmail job step reuses the standard's warmup.

**Tech Stack:** Python 3.11 (httpx, pytest, pymongo via existing helpers), Node 24 cron in `index.js`, Smartlead REST API v1.

**Spec:** `docs/superpowers/specs/2026-10-13-inbox-intake-design.md`

## Global Constraints

- Standard values (from the spec, verbatim): Google limit 15, warmup 25, ramp +5, reply 30%, ready after 14 days; Outlook (<10 inboxes per domain) 10 / 20 / +5 / 30% / 21 days; Outlook (10+ per domain) 5 / 5 / ramp off / 30% / 21 days; SMTP 7 / 20 / +5 / 30% / 21 days. Every type: warmup on, weekdays only, auto-adjust on. ScaledMail inboxes: no custom tracking domain.
- Ready to use = days since Smartlead's `warmup_details.warmup_created_at` ≥ the type's days **and** `warmup_reputation` ≥ 90.
- Smartlead API limits: `daily_rampup` 5-20 (or omit and send `is_rampup_enabled: false`); `reply_rate_percentage` 20-100; `total_warmup_per_day` 1-50.
- Smartlead's read-back does **not** return `daily_rampup`, `auto_adjust_warmup` or `send_warmups_only_on_weekdays`. Read-back proves everything else; those three are accepted on a 2xx write and the report says so.
- Writes only when `INBOX_INTAKE_ENABLED=true` (cron) or `--apply` (hand run). Without it nothing is written.
- Only inboxes created on or after `INTAKE_SINCE` (default `2026-10-12`) and not already done in the ledger are ever written. Each inbox at most 3 attempts.
- Current-client Smartlead accounts only: `PRECISE_LEADS`, `BETTRDATA` (Melior lives in `PRECISE_LEADS` under client 12256).
- Never print or post API keys; any error text goes through `smartlead.notify.redact` (Python) / `slack_text.redact` (JS).
- Never load `index.js` locally; check it with `node --check index.js` and run `npm test`.
- Python in WSL: `smartlead_sync/.venv/Scripts/python.exe` with `PYTHONIOENCODING=utf-8 WSLENV=PYTHONIOENCODING`. Commands below say `python`.
- Commits: no co-author line. Push only when Manveen says "push it".

## Review Focus

1. **An older inbox, or one already in a campaign, is never written**, even if it lacks the setup tag: intake must filter on `created_at >= INTAKE_SINCE` first (test in Task 3).
2. **An inbox whose client cannot be known** (Precise Leads account, no Smartlead client, domain in no order/job/tracker row) must be reported, not given a guessed signature (test in Task 3).
3. **Smartlead accepts the write but reads back different values** (it clamps, or ignores a field): the inbox is not marked done, is retried, and after 3 tries is reported as stuck (test in Task 3).
4. **A delivery arrives in parts** (ScaledMail uploads 30 of 50): nothing on that order is written until all are present (test in Task 3).
5. **One Smartlead call fails mid-run** (429/5xx after retries, or a timeout): that inbox is reported with a redacted error and the rest still run (test in Task 3).

---

### Task 1: The standard (pure)

**Files:**
- Create: `smartlead_sync/inbox_standard.json`
- Create: `smartlead_sync/smartlead/inbox_standard.py`
- Test: `smartlead_sync/test_inbox_standard.py`

**Interfaces:**
- Produces:
  - `SETUP_TAG = "Bot-Setup"`, `READY_REPUTATION = 90`, `DENSE_OUTLOOK_AT = 10`
  - `inbox_kind(account: dict, inboxes_on_domain: int) -> str` → `"google" | "outlook" | "outlook_dense" | "smtp"`
  - `standard_for(kind: str) -> dict` (keys: `label, max_email_per_day, total_warmup_per_day, daily_rampup, reply_rate, ready_after_days, weekdays_only, auto_adjust`)
  - `account_fields(kind: str, vendor: str) -> dict` (update-account body: `max_email_per_day`, plus `custom_tracking_url: ""` for ScaledMail)
  - `warmup_args(kind: str) -> dict` (kwargs for `SmartleadClient.set_warmup`)
  - `readback_mismatches(account: dict, kind: str, vendor: str, written: dict) -> list[str]`
  - `drift(account: dict, kind: str, vendor: str) -> list[str]`
  - `warmup_started(account: dict) -> date | None`, `ready_date(account: dict, kind: str) -> date | None`
  - `readiness(account: dict, kind: str, today: date) -> str` → `"warming" | "ready" | "low_reputation" | "unknown"`

- [ ] **Step 1: Write the failing tests**

```python
"""The team's setup standard per inbox type (spec 2026-10-13 inbox intake). No network."""
from datetime import date

from smartlead import inbox_standard as st


def acct(**kw):
    a = {"id": 1, "from_email": "a@x.com", "type": "GMAIL", "smtp_host": None, "message_per_day": 15,
         "custom_tracking_domain": None, "signature": "<div>A</div>", "client_id": None,
         "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "reply_rate": 30,
                            "warmup_created_at": "2026-10-13T10:49:25.235Z", "warmup_reputation": "100%"}}
    a.update(kw)
    return a


def test_kind_from_type_host_and_density():
    assert st.inbox_kind(acct(type="GMAIL"), 3) == "google"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="smtp.gmail.com"), 3) == "google"      # Zapmail Google added by SMTP
    assert st.inbox_kind(acct(type="OUTLOOK"), 3) == "outlook"
    assert st.inbox_kind(acct(type="OUTLOOK"), 25) == "outlook_dense"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="smtp.office365.com"), 25) == "outlook_dense"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="mail.accountsworks.com"), 4) == "smtp"


def test_standard_values_match_the_spec():
    g, o, d, s = (st.standard_for(k) for k in ("google", "outlook", "outlook_dense", "smtp"))
    assert (g["max_email_per_day"], g["total_warmup_per_day"], g["daily_rampup"], g["reply_rate"], g["ready_after_days"]) == (15, 25, 5, 30, 14)
    assert (o["max_email_per_day"], o["total_warmup_per_day"], o["daily_rampup"], o["reply_rate"], o["ready_after_days"]) == (10, 20, 5, 30, 21)
    assert (d["max_email_per_day"], d["total_warmup_per_day"], d["daily_rampup"], d["reply_rate"], d["ready_after_days"]) == (5, 5, 0, 30, 21)
    assert (s["max_email_per_day"], s["total_warmup_per_day"], s["daily_rampup"], s["reply_rate"], s["ready_after_days"]) == (7, 20, 5, 30, 21)
    assert all(st.standard_for(k)["weekdays_only"] and st.standard_for(k)["auto_adjust"] for k in ("google", "smtp"))


def test_account_fields_and_warmup_args():
    assert st.account_fields("google", "scaledmail") == {"max_email_per_day": 15, "custom_tracking_url": ""}
    assert st.account_fields("google", "zapmail") == {"max_email_per_day": 15}
    assert st.warmup_args("outlook_dense") == {"enabled": True, "total_per_day": 5, "daily_rampup": 0,
                                               "reply_rate": 30, "auto_adjust": True, "weekdays_only": True}


def test_readback_matches_and_mismatches():
    assert st.readback_mismatches(acct(), "google", "scaledmail", {"max_email_per_day": 15}) == []
    bad = acct(message_per_day=10, warmup_details={"status": "ACTIVE", "max_email_per_day": 30, "reply_rate": 65,
                                                    "warmup_created_at": None, "warmup_reputation": "100%"},
               custom_tracking_domain="track.x.com")
    miss = st.readback_mismatches(bad, "google", "scaledmail", {"signature": "<div>B</div>", "client_id": 12256})
    assert "daily limit 10, want 15" in miss and "warmup 30/day, want 25" in miss and "reply rate 65%, want 30%" in miss
    assert "tracking domain track.x.com, want none" in miss and "signature differs" in miss and "client None, want 12256" in miss
    assert st.readback_mismatches(acct(warmup_details={"status": "INACTIVE"}), "google", "zapmail", {})[0] == "warmup off"


def test_drift_ignores_signature_and_client():
    assert st.drift(acct(message_per_day=20), "google", "zapmail") == ["daily limit 20, want 15"]


def test_ready_dates_and_reputation():
    a = acct()
    assert st.warmup_started(a) == date(2026, 10, 13)
    assert st.ready_date(a, "google") == date(2026, 10, 27) and st.ready_date(a, "smtp") == date(2026, 11, 3)
    assert st.readiness(a, "google", date(2026, 10, 26)) == "warming"
    assert st.readiness(a, "google", date(2026, 10, 27)) == "ready"
    low = acct(warmup_details={**a["warmup_details"], "warmup_reputation": 82})
    assert st.readiness(low, "google", date(2026, 10, 27)) == "low_reputation"
    assert st.readiness(acct(warmup_details={}), "google", date(2026, 10, 27)) == "unknown"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_standard.py`
Expected: FAIL, `ImportError: cannot import name 'inbox_standard'`

- [ ] **Step 3: Write the standard file and module**

`smartlead_sync/inbox_standard.json`:

```json
{
  "_doc": "Setup standard for a new Smartlead inbox. Spec: docs/superpowers/specs/2026-10-13-inbox-intake-design.md. daily_rampup 0 = ramp off (Smartlead accepts 5-20).",
  "types": {
    "google":        {"label": "Google",                  "max_email_per_day": 15, "total_warmup_per_day": 25, "daily_rampup": 5, "reply_rate": 30, "ready_after_days": 14},
    "outlook":       {"label": "Outlook",                 "max_email_per_day": 10, "total_warmup_per_day": 20, "daily_rampup": 5, "reply_rate": 30, "ready_after_days": 21},
    "outlook_dense": {"label": "Outlook (shared domain)", "max_email_per_day": 5,  "total_warmup_per_day": 5,  "daily_rampup": 0, "reply_rate": 30, "ready_after_days": 21},
    "smtp":          {"label": "SMTP",                    "max_email_per_day": 7,  "total_warmup_per_day": 20, "daily_rampup": 5, "reply_rate": 30, "ready_after_days": 21}
  },
  "every_type": {"weekdays_only": true, "auto_adjust": true},
  "no_tracking_vendors": ["scaledmail"]
}
```

`smartlead_sync/smartlead/inbox_standard.py`:

```python
"""The team's setup standard for a new sending inbox, by type. Pure: no network.

Spec: docs/superpowers/specs/2026-10-13-inbox-intake-design.md. Values live in
inbox_standard.json so they change without code. Read-back cannot see
daily_rampup, auto-adjust or weekdays-only (Smartlead does not return them).
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path

STANDARD_PATH = Path(os.getenv(
    "INBOX_STANDARD_FILE", Path(__file__).resolve().parent.parent / "inbox_standard.json"))
SETUP_TAG = "Bot-Setup"
READY_REPUTATION = 90
DENSE_OUTLOOK_AT = 10          # Outlook inboxes on one domain from which it counts as a shared tenant


def load() -> dict:
    with open(STANDARD_PATH, encoding="utf-8") as fh:
        return json.load(fh)


def inbox_kind(account: dict, inboxes_on_domain: int) -> str:
    t = str(account.get("type") or "").upper()
    host = str(account.get("smtp_host") or "").lower()
    if t == "GMAIL" or "gmail" in host or "google" in host:
        return "google"
    if t == "OUTLOOK" or "office365" in host or "outlook" in host:
        return "outlook_dense" if inboxes_on_domain >= DENSE_OUTLOOK_AT else "outlook"
    return "smtp"


def standard_for(kind: str) -> dict:
    s = load()
    return {**s["types"][kind], **s["every_type"]}


def account_fields(kind: str, vendor: str) -> dict:
    out = {"max_email_per_day": standard_for(kind)["max_email_per_day"]}
    if vendor in load().get("no_tracking_vendors", []):
        out["custom_tracking_url"] = ""
    return out


def warmup_args(kind: str) -> dict:
    s = standard_for(kind)
    return {"enabled": True, "total_per_day": s["total_warmup_per_day"], "daily_rampup": s["daily_rampup"],
            "reply_rate": s["reply_rate"], "auto_adjust": s["auto_adjust"], "weekdays_only": s["weekdays_only"]}


def _int(v) -> int | None:
    try:
        return int(str(v).rstrip("%"))
    except (TypeError, ValueError):
        return None


def drift(account: dict, kind: str, vendor: str) -> list[str]:
    """What no longer matches the type's standard (limit, warmup, reply rate, tracking)."""
    s = standard_for(kind)
    w = account.get("warmup_details") or {}
    out = []
    if str(w.get("status") or "").upper() != "ACTIVE":
        return ["warmup off"]
    if _int(account.get("message_per_day")) != s["max_email_per_day"]:
        out.append(f"daily limit {account.get('message_per_day')}, want {s['max_email_per_day']}")
    wmax = _int(w.get("max_email_per_day") or w.get("warmup_max_count"))
    if wmax != s["total_warmup_per_day"]:
        out.append(f"warmup {wmax}/day, want {s['total_warmup_per_day']}")
    if _int(w.get("reply_rate")) != s["reply_rate"]:
        out.append(f"reply rate {w.get('reply_rate')}%, want {s['reply_rate']}%")
    if vendor in load().get("no_tracking_vendors", []) and account.get("custom_tracking_domain"):
        out.append(f"tracking domain {account['custom_tracking_domain']}, want none")
    return out


def readback_mismatches(account: dict, kind: str, vendor: str, written: dict) -> list[str]:
    """drift() plus the signature and client we just wrote."""
    from smartlead.inbox_setup import signature_text
    out = drift(account, kind, vendor)
    if "signature" in written and signature_text(written["signature"]) != signature_text(account.get("signature") or ""):
        out.append("signature differs")
    if "client_id" in written and account.get("client_id") != written["client_id"]:
        out.append(f"client {account.get('client_id')}, want {written['client_id']}")
    return out


def warmup_started(account: dict) -> date | None:
    iso = (account.get("warmup_details") or {}).get("warmup_created_at")
    return date.fromisoformat(str(iso)[:10]) if iso else None


def ready_date(account: dict, kind: str) -> date | None:
    start = warmup_started(account)
    return start + timedelta(days=standard_for(kind)["ready_after_days"]) if start else None


def readiness(account: dict, kind: str, today: date) -> str:
    when = ready_date(account, kind)
    if when is None:
        return "unknown"
    if today < when:
        return "warming"
    rep = _int((account.get("warmup_details") or {}).get("warmup_reputation"))
    return "ready" if rep is not None and rep >= READY_REPUTATION else "low_reputation"
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_standard.py`
Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/inbox_standard.json smartlead_sync/smartlead/inbox_standard.py smartlead_sync/test_inbox_standard.py
git commit -m "Inbox standard: setup values per inbox type, read-back check and ready dates (pure)"
```

---

### Task 2: `set_warmup` sends weekdays-only

**Files:**
- Modify: `smartlead_sync/smartlead/api.py:150-176` (`SmartleadClient.set_warmup`)
- Test: `smartlead_sync/test_inbox_standard.py` (append)

**Interfaces:**
- Consumes: `warmup_args(kind)` from Task 1 (its keys are `set_warmup`'s keyword names).
- Produces: `SmartleadClient.set_warmup(account_id, enabled, total_per_day=40, daily_rampup=5, reply_rate=20, auto_adjust=None, weekdays_only=None) -> dict`; `weekdays_only=None` leaves Smartlead's current value.

- [ ] **Step 1: Write the failing test**

```python
def test_set_warmup_sends_the_standard(monkeypatch):
    import asyncio
    from smartlead.api import SmartleadClient

    sent = []

    class Resp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": True}

    class FakeHttp:
        async def post(self, url, params, json):
            sent.append(json)
            return Resp()

    c = SmartleadClient("k", account_name="X")
    c._client = FakeHttp()
    asyncio.run(c.set_warmup("1", **st.warmup_args("google")))
    asyncio.run(c.set_warmup("2", **st.warmup_args("outlook_dense")))
    assert sent[0] == {"warmup_enabled": "true", "total_warmup_per_day": 25, "reply_rate_percentage": 30,
                       "daily_rampup": 5, "is_rampup_enabled": True, "auto_adjust_warmup": True,
                       "send_warmups_only_on_weekdays": True}
    assert sent[1]["is_rampup_enabled"] is False and "daily_rampup" not in sent[1]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_standard.py::test_set_warmup_sends_the_standard`
Expected: FAIL, `TypeError: set_warmup() got an unexpected keyword argument 'weekdays_only'`

- [ ] **Step 3: Add the parameter**

In `smartlead/api.py`, change the signature and the enabled branch:

```python
    async def set_warmup(self, account_id: str, enabled: bool,
                         total_per_day: int = 40, daily_rampup: int = 5,
                         reply_rate: int = 20, auto_adjust: bool | None = None,
                         weekdays_only: bool | None = None) -> dict:
```

and after the `auto_adjust` lines inside `if enabled:` add:

```python
            if weekdays_only is not None:
                body["send_warmups_only_on_weekdays"] = bool(weekdays_only)
```

Add to the docstring: `weekdays_only` → `send_warmups_only_on_weekdays`; None leaves it.

- [ ] **Step 4: Run to verify it passes, plus the existing callers' tests**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_standard.py test_inbox_setup.py test_warmup_planner.py`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/smartlead/api.py smartlead_sync/test_inbox_standard.py
git commit -m "Smartlead set_warmup: weekdays-only option"
```

---

### Task 3: Intake run with ledger and read-back

**Files:**
- Create: `smartlead_sync/smartlead/inbox_intake.py`
- Test: `smartlead_sync/test_inbox_intake.py`

**Interfaces:**
- Consumes: everything Task 1 produces; `SmartleadClient.list_email_accounts()`, `.list_tags()`, `.update_email_account(id, fields)`, `.set_warmup(id, **warmup_args)`, `.get_email_account(id)`, `.assign_tags(ids, tag_ids)` (max 25 ids); `inbox_setup.plan_changes(account, prof, email=..., set_signature=True) -> InboxChange` (`.fields` holds `signature` / `client_id`); `notify.redact(text)`.
- Produces:
  - `IntakeStore(collection=None)` with `.done(email) -> bool`, `.tries(email) -> int`, `.record(email, ok: bool, **fields)`
  - `Delivery` dicts: `{"source": "scaledmail"|"zapmail", "id": str, "client": str, "domains": set[str], "expected": int}`
  - `deliveries(plans: list[dict], jobs: list[dict]) -> dict[str, dict]` (domain → Delivery)
  - `async run_account(sl, account_name: str, *, approve: bool, since: date, today: date, by_domain: dict, tracker: dict, store, profile) -> dict` returning `{"set_up": [row], "would": [row], "waiting": [str], "problems": [str], "complete": [delivery id]}` where `row = {"email", "client", "kind", "vendor", "domain", "ready_on": date|None}`
  - `MAX_TRIES = 3`, `CURRENT_ACCOUNTS = ("PRECISE_LEADS", "BETTRDATA")`

- [ ] **Step 1: Write the failing tests**

```python
"""Inbox intake: new inboxes set up once, proven by read-back (spec 2026-10-13). No network."""
import asyncio
from datetime import date

from smartlead import inbox_intake as ii

TODAY, SINCE = date(2026, 10, 13), date(2026, 10, 12)
PROF = {"signature_lines": ["{full_name}", "Precise Leads"], "company": "Precise Leads", "website": "preciseleads.in"}
MELIOR = {**PROF, "smartlead_client_id": 12256}
TAGS = [{"id": 3212, "name": "PreciseLeads"}, {"id": 266037, "name": "Melior"}, {"id": 153335, "name": "ScaledMail-Google"},
        {"id": 154156, "name": "ScaledMail-Microsoft"}, {"id": 9, "name": "Bot-Setup"}]


def acct(i, email, *, created="2026-10-13T10:00:00Z", type_="GMAIL", tags=(), client_id=None):
    return {"id": i, "from_email": email, "from_name": "Avinash Haridas", "type": type_, "smtp_host": None,
            "created_at": created, "client_id": client_id, "signature": "", "message_per_day": 10,
            "custom_tracking_domain": None, "tags": [{"tag_id": t, "tag_name": n} for t, n in tags],
            "warmup_details": {"status": "ACTIVE", "max_email_per_day": 30, "reply_rate": 65,
                               "warmup_created_at": "2026-10-13T10:49:25Z", "warmup_reputation": "100%"}}


class FakeSL:
    """Applies writes to its accounts, so read-back sees them; `ignore` drops a field."""
    def __init__(self, accounts, ignore=(), fail=()):
        self.accounts = {a["id"]: a for a in accounts}
        self.ignore, self.fail, self.tagged, self.writes = set(ignore), set(fail), [], []

    async def list_email_accounts(self): return list(self.accounts.values())
    async def list_tags(self): return TAGS

    async def update_email_account(self, i, fields):
        if int(i) in self.fail:
            raise RuntimeError("429 for url 'https://server.smartlead.ai/x?api_key=SECRET'")
        self.writes.append((int(i), dict(fields)))
        a = self.accounts[int(i)]
        if "max_email_per_day" in fields and "limit" not in self.ignore:
            a["message_per_day"] = fields["max_email_per_day"]
        for k in ("signature", "client_id"):
            if k in fields:
                a[k] = fields[k]

    async def set_warmup(self, i, enabled, total_per_day, daily_rampup, reply_rate, auto_adjust, weekdays_only):
        w = self.accounts[int(i)]["warmup_details"]
        w["max_email_per_day"] = total_per_day
        w["reply_rate"] = reply_rate if "reply" not in self.ignore else w["reply_rate"]

    async def get_email_account(self, i): return self.accounts[int(i)]
    async def assign_tags(self, ids, tag_ids): self.tagged.append((sorted(ids), sorted(tag_ids)))


class FakeCol:
    def __init__(self): self.docs = {}
    def find_one(self, q, proj=None): return self.docs.get(q["email"])
    def update_one(self, q, upd, upsert=False):
        d = self.docs.setdefault(q["email"], {"email": q["email"], "tries": 0, "history": []})
        d.update(upd.get("$set", {}))
        for k, v in upd.get("$inc", {}).items(): d[k] = d.get(k, 0) + v
        for k, v in upd.get("$push", {}).items(): d[k].append(v)


PLAN = {"plan_id": "13845d", "status": "placed", "client": "Precise Leads", "provider": "google",
        "domains": ["warmmeetings.com"], "mailboxes": 3}


def run(sl, store=None, approve=True, plans=(PLAN,), jobs=(), tracker=None, account="PRECISE_LEADS"):
    store = store or ii.IntakeStore(FakeCol())
    out = asyncio.run(ii.run_account(sl, account, approve=approve, since=SINCE, today=TODAY,
                                     by_domain=ii.deliveries(list(plans), list(jobs)), tracker=tracker or {},
                                     store=store, profile=lambda c: MELIOR if c == "Melior" else PROF))
    return out, store


THREE = [acct(i, f"a{i}@warmmeetings.com") for i in (1, 2, 3)]


def test_new_scaledmail_inboxes_are_set_up_proven_and_tagged():
    sl = FakeSL(THREE)
    out, store = run(sl)
    assert [r["email"] for r in out["set_up"]] == ["a1@warmmeetings.com", "a2@warmmeetings.com", "a3@warmmeetings.com"]
    a = sl.accounts[1]
    assert a["message_per_day"] == 15 and a["warmup_details"]["max_email_per_day"] == 25 and a["warmup_details"]["reply_rate"] == 30
    assert "Avinash Haridas" in a["signature"] and ("custom_tracking_url", "") in sl.writes[0][1].items()
    assert sl.tagged == [([1, 2, 3], [9, 3212, 153335])]
    assert store.done("a1@warmmeetings.com") and out["complete"] == ["13845d"]
    assert out["set_up"][0]["ready_on"] == date(2026, 10, 27)


def test_preview_writes_nothing():
    sl = FakeSL(THREE)
    out, store = run(sl, approve=False)
    assert len(out["would"]) == 3 and not out["set_up"] and not sl.writes and not sl.tagged
    assert not store.done("a1@warmmeetings.com")


def test_old_or_already_done_inboxes_are_never_written():           # Review Focus 1
    old = acct(7, "old@warmmeetings.com", created="2026-09-01T00:00:00Z")
    tagged = acct(8, "t@warmmeetings.com", tags=[(9, "Bot-Setup")])
    sl = FakeSL([old, tagged])
    out, _ = run(sl, plans=())
    assert not sl.writes and not out["set_up"] and not out["problems"]


def test_unknown_client_is_reported_not_guessed():                  # Review Focus 2
    sl = FakeSL([acct(5, "x@mystery.com")])
    out, _ = run(sl, plans=())
    assert not sl.writes and any("mystery.com" in p and "no client" in p for p in out["problems"])


def test_melior_from_tracker_gets_melior_client():
    sl = FakeSL([acct(6, "r@meliorbase.com")])
    out, _ = run(sl, plans=(), tracker={"meliorbase.com": {"client": "Melior", "provider": "Scaledmail"}})
    assert sl.accounts[6]["client_id"] == 12256 and out["set_up"][0]["client"] == "Melior"


def test_readback_mismatch_is_retried_then_stuck():                 # Review Focus 3
    store = ii.IntakeStore(FakeCol())
    for n in range(3):
        sl = FakeSL([acct(1, "a1@warmmeetings.com")], ignore={"reply"})
        out, _ = run(sl, store=store, plans=({**PLAN, "mailboxes": 1},))
        assert not out["set_up"] and not sl.tagged
        assert any("reply rate 65%, want 30%" in p for p in out["problems"])
    sl = FakeSL([acct(1, "a1@warmmeetings.com")], ignore={"reply"})
    out, _ = run(sl, store=store, plans=({**PLAN, "mailboxes": 1},))
    assert not sl.writes and any("stuck after 3 tries" in p for p in out["problems"])


def test_partial_delivery_waits():                                  # Review Focus 4
    sl = FakeSL(THREE[:2])
    out, _ = run(sl)
    assert not sl.writes and out["waiting"] == ["⏳ 2 of 3 inboxes in Smartlead for ScaledMail order 13845d (warmmeetings.com), waiting for the rest"]


def test_one_failing_inbox_does_not_stop_the_rest_and_hides_keys():  # Review Focus 5
    sl = FakeSL(THREE, fail={2})
    out, _ = run(sl)
    assert [r["email"] for r in out["set_up"]] == ["a1@warmmeetings.com", "a3@warmmeetings.com"]
    msg = " ".join(out["problems"])
    assert "a2@warmmeetings.com" in msg and "SECRET" not in msg and out["complete"] == []


def test_dense_outlook_gets_five_and_five():
    boxes = [acct(i, f"o{i}@qualifiedcalendar.com", type_="OUTLOOK") for i in range(1, 26)]
    sl = FakeSL(boxes)
    plan = {"plan_id": "85b1cd", "status": "placed", "client": "Precise Leads", "provider": "outlook",
            "domains": ["qualifiedcalendar.com"], "mailboxes": 25}
    out, _ = run(sl, plans=(plan,))
    assert len(out["set_up"]) == 25 and sl.accounts[1]["message_per_day"] == 5
    assert sl.accounts[1]["warmup_details"]["max_email_per_day"] == 5 and out["set_up"][0]["kind"] == "outlook_dense"
    assert [len(ids) for ids, _ in sl.tagged] == [25]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py`
Expected: FAIL, `ImportError: cannot import name 'inbox_intake'`

- [ ] **Step 3: Write the module**

```python
"""Inbox intake: set every new Smartlead inbox up once, prove it, then only watch.

Spec: docs/superpowers/specs/2026-10-13-inbox-intake-design.md. Per current-
client Smartlead account: inboxes created on/after INTAKE_SINCE and not done
in the ledger get the type's standard (inbox_standard), the client's signature
and Smartlead client, and tags; then each is read back. Only a matching
read-back counts as done (ledger + Bot-Setup tag). A ScaledMail order or a
Zapmail job waits until all its inboxes are in Smartlead. approve=False
writes nothing.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from smartlead import inbox_standard as st

COLLECTION = "inbox_intake"
MAX_TRIES = 3
TAG_BATCH = 25
CURRENT_ACCOUNTS = ("PRECISE_LEADS", "BETTRDATA")
CLIENT_TAG = {"Precise Leads": "PreciseLeads", "Melior": "Melior", "Bettrdata": "BettrData"}
VENDOR_TAG = {("scaledmail", "google"): "ScaledMail-Google", ("scaledmail", "outlook"): "ScaledMail-Microsoft",
              ("scaledmail", "outlook_dense"): "ScaledMail-Microsoft", ("scaledmail", "smtp"): "ScaledMail-SMTP",
              ("zapmail", "google"): "Zapmail-Google", ("zapmail", "outlook"): "Zapmail-Microsoft"}
CURRENT_CLIENTS = ("Precise Leads", "Melior", "Bettrdata")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class IntakeStore:
    """One Mongo document per inbox: tries, ok, the values written, history."""

    def __init__(self, collection=None) -> None:
        if collection is None:
            from smartlead.zapmail_asset_sync import _db
            db = _db()
            collection = db[COLLECTION] if db is not None else None
        if collection is None:
            raise RuntimeError("intake ledger unavailable (Mongo); nothing was written")
        self.col = collection

    def _doc(self, email: str) -> dict:
        return self.col.find_one({"email": email}, {"_id": 0}) or {}

    def done(self, email: str) -> bool:
        return bool(self._doc(email).get("ok"))

    def tries(self, email: str) -> int:
        return int(self._doc(email).get("tries") or 0)

    def record(self, email: str, ok: bool, **fields) -> None:
        self.col.update_one({"email": email},
                            {"$set": {"ok": ok, "updated_at": _now(), **fields}, "$inc": {"tries": 1},
                             "$push": {"history": {"at": _now(), "ok": ok, **fields}}}, upsert=True)


def deliveries(plans: list[dict], jobs: list[dict]) -> dict[str, dict]:
    """domain -> the ScaledMail order or Zapmail job that brought it."""
    out: dict[str, dict] = {}
    for p in plans:
        if p.get("status") == "placed":
            d = {"source": "scaledmail", "id": p["plan_id"], "client": p.get("client"),
                 "domains": {x.lower() for x in p.get("domains") or []}, "expected": int(p.get("mailboxes") or 0)}
            out.update({dom: d for dom in d["domains"]})
    for j in jobs:
        if j.get("status") not in ("cancelled",) and j.get("inboxes"):
            d = {"source": "zapmail", "id": j["job_id"], "client": j.get("client"),
                 "domains": {x.lower() for x in j.get("domains") or []}, "expected": len(j["inboxes"])}
            out.update({dom: d for dom in d["domains"]})
    return out


def _created(a: dict) -> date | None:
    v = a.get("created_at")
    return date.fromisoformat(str(v)[:10]) if v else None


def _has_tag(a: dict, name: str) -> bool:
    return any((t.get("tag_name") or t.get("name")) == name for t in a.get("tags") or [])


def _client(a: dict, account_name: str, d: dict | None, tracker_row: dict, melior_id) -> str | None:
    if d and d.get("client") in CURRENT_CLIENTS:
        return d["client"]
    if account_name.upper() == "BETTRDATA":
        return "Bettrdata"
    if melior_id is not None and a.get("client_id") == melior_id:
        return "Melior"
    c = (tracker_row or {}).get("client")
    return c if c in CURRENT_CLIENTS else None


def _vendor(d: dict | None, tracker_row: dict) -> str:
    if d:
        return d["source"]
    p = str((tracker_row or {}).get("provider") or "").lower().replace(" ", "")
    return "scaledmail" if "scaledmail" in p else "zapmail" if "zapmail" in p else "unknown"


async def run_account(sl, account_name: str, *, approve: bool, since: date, today: date,
                      by_domain: dict, tracker: dict, store, profile) -> dict:
    from smartlead.inbox_setup import plan_changes
    from smartlead.notify import redact
    out: dict = {"set_up": [], "would": [], "waiting": [], "problems": [], "complete": []}
    accounts = await sl.list_email_accounts()
    tags = {t["name"]: t["id"] for t in await sl.list_tags()}
    per_domain: dict[str, int] = {}
    for a in accounts:
        dom = str(a.get("from_email", "")).split("@")[-1].lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
    melior_id = (profile("Melior") or {}).get("smartlead_client_id")
    new = [a for a in accounts
           if (_created(a) or date.min) >= since and not _has_tag(a, st.SETUP_TAG)
           and not store.done(str(a["from_email"]).lower())]
    seen_waiting: set = set()
    ok_by_tags: dict[tuple, list[int]] = {}
    attempted: dict[str, list[bool]] = {}
    for a in sorted(new, key=lambda x: x.get("id") or 0):
        email = str(a["from_email"]).lower()
        dom = email.split("@")[-1]
        d = by_domain.get(dom)
        if d:
            found = sum(per_domain.get(x, 0) for x in d["domains"])
            if found < d["expected"]:
                if d["id"] not in seen_waiting:
                    seen_waiting.add(d["id"])
                    what = "ScaledMail order" if d["source"] == "scaledmail" else "Zapmail job"
                    out["waiting"].append(f"⏳ {found} of {d['expected']} inboxes in Smartlead for {what} {d['id']} "
                                          f"({', '.join(sorted(d['domains']))}), waiting for the rest")
                continue
        if store.tries(email) >= MAX_TRIES:
            out["problems"].append(f"`{email}`: stuck after {MAX_TRIES} tries, set it up by hand in Smartlead")
            continue
        client = _client(a, account_name, d, tracker.get(dom), melior_id)
        if not client:
            out["problems"].append(f"`{email}`: no client known for {dom} (no order, job or tracker row), not set up")
            continue
        kind = st.inbox_kind(a, per_domain.get(dom, 1))
        vendor = _vendor(d, tracker.get(dom))
        written = {**plan_changes(a, profile(client), email=email, set_signature=True).fields,
                   **st.account_fields(kind, vendor)}
        row = {"email": email, "client": client, "kind": kind, "vendor": vendor, "domain": dom,
               "ready_on": st.ready_date(a, kind)}
        if not approve:
            out["would"].append(row)
            continue
        back = a
        try:
            await sl.update_email_account(str(a["id"]), written)
            await sl.set_warmup(str(a["id"]), **st.warmup_args(kind))
            back = await sl.get_email_account(str(a["id"]))
            miss = st.readback_mismatches(back, kind, vendor, written)
            row["ready_on"] = st.ready_date(back, kind)
        except Exception as exc:  # noqa: BLE001 - one inbox failing must not stop the rest
            miss = [f"Smartlead error: {redact(str(exc))[:160]}"]
        store.record(email, ok=not miss, kind=kind, vendor=vendor, client=client, account=account_name,
                     written=written, mismatches=miss, warmup_started=str(st.warmup_started(back) or ""))
        if d:
            attempted.setdefault(d["id"], []).append(not miss)
        if miss:
            out["problems"].append(f"`{email}`: {'; '.join(miss)} (try {store.tries(email)} of {MAX_TRIES})")
            continue
        out["set_up"].append(row)
        names = (st.SETUP_TAG, CLIENT_TAG.get(client), VENDOR_TAG.get((vendor, kind)))
        ok_by_tags.setdefault(tuple(sorted(tags[n] for n in names if n and n in tags)), []).append(int(a["id"]))
    for tag_ids, ids in ok_by_tags.items():
        for i in range(0, len(ids), TAG_BATCH):
            if tag_ids:
                await sl.assign_tags(ids[i:i + TAG_BATCH], list(tag_ids))
    for name in {VENDOR_TAG.get((r["vendor"], r["kind"])) for r in out["set_up"]} | {st.SETUP_TAG}:
        if name and name not in tags:
            out["problems"].append(f"Smartlead has no tag `{name}` in {account_name}: create it so new inboxes get it")
    for did, results in attempted.items():
        d = next(v for v in by_domain.values() if v["id"] == did)
        if len(results) >= d["expected"] and all(results):
            out["complete"].append(did)
    return out
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py`
Expected: `10 passed`. If `test_new_scaledmail_inboxes_are_set_up_proven_and_tagged` fails on the tag order, compare `sorted(...)` on both sides; tag ids are sorted by design.

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/smartlead/inbox_intake.py smartlead_sync/test_inbox_intake.py
git commit -m "Inbox intake: set new inboxes up once, prove by read-back, ledger, retries, wait for whole deliveries"
```

---

### Task 4: CLI, channel post, order history, cron

**Files:**
- Create: `smartlead_sync/inbox_intake_cli.py`
- Modify: `index.js:1499-1508` (the inbox-readiness cron)
- Test: `smartlead_sync/test_inbox_intake.py` (append)

**Interfaces:**
- Consumes: `run_account`, `deliveries`, `IntakeStore`, `CURRENT_ACCOUNTS` (Task 3); `discover_accounts()`; `SmartleadClient(key, account_name=...)`; `profile_for(client) -> (key, prof)`; `PlanStore().all()` / `.log(plan_id, status, **fields)`; `JobStore().list()`; `read_tracker() -> {domain_or_email: row}`; `notify.post_with_thread(token, channel, head, parts)`.
- Produces: `intake_message(results: dict[str, dict], today: date) -> tuple[list[str], list[str]]` (pure); CLI `python inbox_intake_cli.py [--apply] [--post] [--since YYYY-MM-DD] [--json]`.

- [ ] **Step 1: Write the failing test for the message**

```python
def test_intake_message_headline_and_ready_dates():
    from inbox_intake_cli import intake_message
    res = {"PRECISE_LEADS": {
        "set_up": [{"email": f"a{i}@warmmeetings.com", "client": "Precise Leads", "kind": "google", "vendor": "scaledmail",
                    "domain": "warmmeetings.com", "ready_on": date(2026, 10, 27)} for i in range(3)],
        "would": [], "waiting": ["⏳ 2 of 21 inboxes in Smartlead for ScaledMail order 2cb59b (x.com), waiting for the rest"],
        "problems": ["`x@mystery.com`: no client known for mystery.com (no order, job or tracker row), not set up"],
        "complete": ["13845d"]}}
    head, parts = intake_message(res, TODAY)
    assert head[0] == "⚠️ Set up 3 new inboxes in Smartlead, 1 needs a look"
    assert head[1] == "Precise Leads 3 · ready to use from 27 Oct"
    body = "\n".join(parts)
    assert "warmmeetings.com · Google · 3 inboxes · limit 15, warmup 25/day, reply 30%, weekdays only · ready from 27 Oct" in body
    assert "⏳ 2 of 21" in body and "mystery.com" in body
    assert "Smartlead does not show ramp, auto-adjust or weekdays-only, so those are set but not re-read." in body
    preview = intake_message({"PRECISE_LEADS": {**res["PRECISE_LEADS"], "set_up": [], "would": res["PRECISE_LEADS"]["set_up"]}}, TODAY)[0]
    assert preview[0].startswith("👀 Would set up 3 new inboxes")
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py::test_intake_message_headline_and_ready_dates`
Expected: FAIL, `ModuleNotFoundError: No module named 'inbox_intake_cli'`

- [ ] **Step 3: Write the CLI**

```python
#!/usr/bin/env python3
"""Inbox intake: new Smartlead inboxes -> the team's standard, once (spec 2026-10-13).

    python inbox_intake_cli.py                    # preview: writes nothing
    python inbox_intake_cli.py --apply --post     # write, read back, post (the cron, INBOX_INTAKE_ENABLED=true)
    python inbox_intake_cli.py --since 2026-10-12 --json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from datetime import date

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import inbox_standard as st


def _day(d: date | None) -> str:
    return f"{d.day} {d:%b}" if d else "?"


def _n(k: int, one: str, many: str) -> str:
    return f"{k} {one if k == 1 else many}"


def intake_message(results: dict[str, dict], today: date) -> tuple[list[str], list[str]]:
    """(headline, thread). Pure."""
    rows = [r for res in results.values() for r in res["set_up"]]
    would = [r for res in results.values() for r in res["would"]]
    waiting = [w for res in results.values() for w in res["waiting"]]
    problems = [p for res in results.values() for p in res["problems"]]
    shown = rows or would
    verb = "Set up" if rows or not would else "Would set up"
    icon = "👀" if verb == "Would set up" else ("⚠️" if problems else "✅")
    look = f", {len(problems)} {'needs' if len(problems) == 1 else 'need'} a look" if problems else ""
    head = [f"{icon} {verb} {_n(len(shown), 'new inbox', 'new inboxes')} in Smartlead{look}"]
    if shown:
        per = Counter(r["client"] for r in shown)
        first = min((r["ready_on"] for r in shown if r["ready_on"]), default=None)
        head.append(" · ".join(f"{c} {k}" for c, k in per.most_common())
                    + (f" · ready to use from {_day(first)}" if first else ""))
    parts: list[str] = []
    by_dom: dict[tuple, list[dict]] = {}
    for r in shown:
        by_dom.setdefault((r["domain"], r["kind"], r["client"]), []).append(r)
    lines = []
    for (dom, kind, client), rs in sorted(by_dom.items()):
        s = st.standard_for(kind)
        lines.append(f"• {dom} · {s['label']} · {_n(len(rs), 'inbox', 'inboxes')} · limit {s['max_email_per_day']}, "
                     f"warmup {s['total_warmup_per_day']}/day, reply {s['reply_rate']}%, weekdays only · "
                     f"ready from {_day(rs[0]['ready_on'])}")
    if lines:
        parts.append("\n".join(["*Set up*" if verb == "Set up" else "*Would set up*"] + lines))
    if waiting:
        parts.append("\n".join(["*Waiting for the rest of a delivery*"] + ["• " + w for w in waiting]))
    if problems:
        parts.append("\n".join(["*Needs a look*"] + ["• " + p for p in problems]))
    parts.append("_Signature, client, tags, daily limit and warmup are read back from Smartlead before an inbox "
                 "counts as set up. Smartlead does not show ramp, auto-adjust or weekdays-only, so those are set but "
                 "not re-read._")
    return head, parts


def main() -> int:
    ap = argparse.ArgumentParser(description="Set new Smartlead inboxes up once")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--since", default=os.getenv("INTAKE_SINCE", "2026-10-12"))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    from smartlead.inbox_intake import CURRENT_ACCOUNTS, IntakeStore, deliveries, run_account
    from smartlead.inbox_setup import profile_for
    from smartlead.notify import redact

    today, since = date.today(), date.fromisoformat(args.since)
    try:
        from smartlead.scaledmail_orders import PlanStore
        plan_store = PlanStore()
        plans = plan_store.all()
    except Exception:  # noqa: BLE001
        plan_store, plans = None, []
    try:
        from smartlead.inbox_jobs import JobStore
        jobs = JobStore().list()
    except Exception:  # noqa: BLE001
        jobs = []
    try:
        from smartlead.zapmail_asset_sync import read_tracker
        tracker = {k: v for k, v in read_tracker().items() if "@" not in k}
    except Exception:  # noqa: BLE001
        tracker = {}
    store = IntakeStore()
    by_domain = deliveries(plans, jobs)
    results: dict[str, dict] = {}

    async def run_all():
        for acc in discover_accounts():
            if acc.name.upper() not in CURRENT_ACCOUNTS:
                continue
            try:
                async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
                    results[acc.name] = await run_account(
                        sl, acc.name, approve=args.apply, since=since, today=today, by_domain=by_domain,
                        tracker=tracker, store=store, profile=lambda c: profile_for(c)[1])
            except Exception as exc:  # noqa: BLE001 - say so, keep the other account
                results[acc.name] = {"set_up": [], "would": [], "waiting": [], "complete": [],
                                     "problems": [f"{acc.name}: could not read Smartlead ({redact(str(exc))[:120]})"]}
    asyncio.run(run_all())

    if args.apply and plan_store is not None:
        for pid in {p for r in results.values() for p in r["complete"]}:
            plan = next((p for p in plans if p.get("plan_id") == pid), None)
            if plan is not None:
                plan_store.log(pid, "in_smartlead", found=plan.get("mailboxes"), expected=plan.get("mailboxes"))
                plan_store.log(pid, "ready", by="inbox_intake")

    head, parts = intake_message(results, today)
    changed = any(r["set_up"] or r["problems"] for r in results.values())
    posted = False
    if args.post and changed:
        from smartlead.notify import post_with_thread
        channel = (os.getenv("SCALEDMAIL_NOTIFY_CHANNEL") or os.getenv("ZAPMAIL_NOTIFY_CHANNEL") or "").strip()
        token = (os.getenv("SLACK_BOT_TOKEN") or "").strip()
        posted = bool(channel and token and post_with_thread(token, channel, head, parts))
    out = {"headline": head, "thread": parts, "posted": posted,
           "counts": {k: sum(len(r[k]) for r in results.values()) for k in ("set_up", "would", "waiting", "problems")}}
    print(json.dumps(out, default=str) if args.json else "\n".join(head + [""] + parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py`
Expected: `11 passed`

- [ ] **Step 5: Switch the cron (intake when enabled, old readiness job until then)**

In `index.js`, replace the body of the `'15 */2 * * *'` cron with:

```js
    cron.schedule('15 */2 * * *', () => {
      // Inbox intake (spec 2026-10-13): every new inbox -> the team's standard,
      // once, proven by read-back. Until INBOX_INTAKE_ENABLED=true, the old
      // ScaledMail readiness job (signature, client, tags) keeps running.
      if (process.env.INBOX_INTAKE_ENABLED === 'true') {
        console.log(`[CRON] Inbox intake firing at ${new Date().toISOString()}`);
        const proc = spawn('python', ['inbox_intake_cli.py', '--apply', '--post', '--json'], {
          cwd: path.join(__dirname, 'smartlead_sync'), env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
        });
        proc.stdout.on('data', d => process.stdout.write(`[inbox-intake] ${d}`));
        proc.stderr.on('data', d => process.stderr.write(`[inbox-intake] ${d}`));
        proc.on('close', code => console.log(`[inbox-intake] finished with code ${code}`));
        return;
      }
      if (process.env.INBOX_READY_ENABLED !== 'true') return;
      console.log(`[CRON] Inbox readiness firing at ${new Date().toISOString()}`);
      runScaledMail('inbox-ready', ['--json', 'ready', '--apply', '--post']);
    }, {
      timezone: 'Asia/Kolkata'
    });
```

Run: `node --check index.js && npm test`
Expected: no syntax error; `22/22 test files passed`

- [ ] **Step 6: Commit**

```bash
git add smartlead_sync/inbox_intake_cli.py smartlead_sync/test_inbox_intake.py index.js
git commit -m "Inbox intake CLI + channel post; cron runs intake when INBOX_INTAKE_ENABLED=true"
```

---

### Task 5: Zapmail job step uses the standard's warmup

**Files:**
- Modify: `smartlead_sync/smartlead/inbox_setup.py:42-49` (`NEW_INBOX_WARMUP`), `:201-202` (`plan_changes` new-inbox branch), `:262-266` (`apply_changes` warmup call)
- Test: `smartlead_sync/test_inbox_setup.py`

**Interfaces:**
- Consumes: `inbox_kind`, `warmup_args` (Task 1); `set_warmup(..., weekdays_only=)` (Task 2).
- Produces: `InboxChange.warmup` for a new inbox = `warmup_args(inbox_kind(account, 1))` (Zapmail puts 3 inboxes per domain, never dense).

- [ ] **Step 1: Find and update the test that pins the old values**

Run: `cd smartlead_sync && grep -n "NEW_INBOX_WARMUP\|warmup" test_inbox_setup.py`
Replace each assertion on the old values (40/day, reply 25, auto-adjust False) with the standard. For a Google account the expectation is:

```python
    assert ch.warmup == {"enabled": True, "total_per_day": 25, "daily_rampup": 5, "reply_rate": 30,
                         "auto_adjust": True, "weekdays_only": True}
```

and add, for an Outlook account (`"type": "OUTLOOK"`):

```python
    assert ch.warmup["total_per_day"] == 20 and ch.warmup["reply_rate"] == 30
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_setup.py`
Expected: FAIL on the warmup dict (still 40 / 25 / False)

- [ ] **Step 3: Use the standard**

Delete `NEW_INBOX_WARMUP` (lines 42-49) and its docstring bullet ("40/day ramping +5 from 5, reply rate 25%, no auto-adjust") → "the team standard by inbox type (inbox_standard.json)". In `plan_changes`:

```python
    if new_inbox:
        from smartlead.inbox_standard import inbox_kind, warmup_args
        ch.warmup = warmup_args(inbox_kind(account, 1))
```

In `apply_changes`, replace the `set_warmup(...)` call with:

```python
                    await sl.set_warmup(str(ch.account_id), **ch.warmup)
```

Run: `grep -rn "NEW_INBOX_WARMUP" smartlead_sync --include=*.py | grep -v .venv`
Expected: no output

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_setup.py test_inbox_job_steps.py test_inbox_jobs.py`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/smartlead/inbox_setup.py smartlead_sync/test_inbox_setup.py
git commit -m "Zapmail job setup: warmup from the inbox standard (one standard, two callers)"
```

---

### Task 6: Audit watches drift and per-type warmup days

**Files:**
- Modify: `smartlead_sync/smartlead/infra_audit.py:22-30` (constants), `:128-175` (`check_mailboxes`), `:226` (labels)
- Test: `smartlead_sync/test_infra_audit.py`

**Interfaces:**
- Consumes: `inbox_kind`, `standard_for`, `drift`, `SETUP_TAG` (Task 1).
- Produces: finding `setup_drift` (P2, label "Set-up inbox no longer matches its standard"); `young_inbox_in_campaign` uses `standard_for(kind)["ready_after_days"]`.

- [ ] **Step 1: Write the failing tests**

Run: `cd smartlead_sync && grep -n "def test_\|check_mailboxes(" test_infra_audit.py | head` to see how accounts are built, then append (adapting `acct` to that file's helper if it has one):

```python
def test_young_google_inbox_counts_from_its_own_days():
    from datetime import datetime, timezone
    from smartlead import infra_audit as ia
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    g = {"from_email": "a@g.com", "type": "GMAIL", "message_per_day": 15, "tags": [],
         "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "warmup_created_at": "2026-10-13T10:00:00Z"}}
    o = {**g, "from_email": "b@o.com", "type": "OUTLOOK", "message_per_day": 10,
         "warmup_details": {**g["warmup_details"], "max_email_per_day": 20}}
    found = ia.check_mailboxes([g, o], {"a@g.com": "Melior", "b@o.com": "Melior"},
                               {"a@g.com": ["Exec"], "b@o.com": ["Exec"]}, now)
    young = [f for f in found if f["check"] == "young_inbox_in_campaign"]
    assert [f["subject"] for f in young] == ["b@o.com"]           # Google ready at 14 days, Outlook not until 21


def test_setup_drift_only_for_set_up_inboxes():
    from datetime import datetime, timezone
    from smartlead import infra_audit as ia
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    base = {"type": "GMAIL", "message_per_day": 20, "custom_tracking_domain": None,
            "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "reply_rate": 30,
                               "warmup_created_at": "2026-09-01T00:00:00Z"}}
    done = {**base, "from_email": "a@x.com", "tags": [{"tag_name": "Bot-Setup"}]}
    other = {**base, "from_email": "b@x.com", "tags": []}
    found = ia.check_mailboxes([done, other], {"a@x.com": "Melior", "b@x.com": "Melior"}, {}, now)
    drift = [f for f in found if f["check"] == "setup_drift"]
    assert [f["subject"] for f in drift] == ["a@x.com"] and "daily limit 20, want 15" in drift[0]["detail"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd smartlead_sync && python -m pytest -q test_infra_audit.py -k "own_days or setup_drift"`
Expected: FAIL (Google flagged at day 17; no `setup_drift`)

- [ ] **Step 3: Implement in `check_mailboxes`**

Before the per-account loop, count inboxes per domain:

```python
    from smartlead import inbox_standard as std
    on_domain: dict[str, int] = defaultdict(int)
    for a in accounts:
        on_domain[a["from_email"].lower().split("@")[-1]] += 1
```

(`defaultdict` is already imported). Inside the loop replace the young-inbox block with:

```python
        stype = std.inbox_kind(a, on_domain[email.split("@")[-1]])
        need = std.standard_for(stype)["ready_after_days"]
        if in_use and age is not None and age < need:
            out.append(finding("young_inbox_in_campaign", "P0", client, email,
                               f"warmed {age:.0f} days, already in {', '.join(active[email][:2])}",
                               f"Take it out of the campaign until day {need}."))
        if any((t.get("tag_name") or t.get("name")) == std.SETUP_TAG for t in a.get("tags") or []):
            vendor = "scaledmail" if any("ScaledMail" in str(t.get("tag_name") or t.get("name")) for t in a.get("tags") or []) else "zapmail"
            off = std.drift(a, stype, vendor)
            if off:
                out.append(finding("setup_drift", "P2", client, email, "; ".join(off),
                                   "Check whether this was changed on purpose; if not, set it back in Smartlead."))
```

Delete `MIN_WARMUP_DAYS = 21` if nothing else uses it (`grep -n MIN_WARMUP_DAYS smartlead/infra_audit.py`), add `"setup_drift": "Set-up inbox no longer matches its standard"` to the labels dict at line ~226, and change the `young_inbox_in_campaign` label to `"Inbox in a campaign before its warmup days"`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_infra_audit.py test_alert_threads.py`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/smartlead/infra_audit.py smartlead_sync/test_infra_audit.py
git commit -m "Infra audit: setup drift on set-up inboxes; young-inbox check uses each type's warmup days"
```

---

### Task 7: The 09:40 digest says what became ready

**Files:**
- Modify: `smartlead_sync/smartlead/inbox_intake.py` (add `readiness_lines`)
- Modify: `smartlead_sync/zapmail_digest.py` (`format_digest` gets `readiness: dict | None = None`; `extra_sections` fills it)
- Test: `smartlead_sync/test_inbox_intake.py`, `smartlead_sync/test_zapmail_digest_thread.py`

**Interfaces:**
- Consumes: `readiness`, `inbox_kind`, `ready_date`, `SETUP_TAG` (Task 1).
- Produces: `readiness_lines(accounts: list[dict], today: date) -> dict` → `{"ready_today": [str], "low": [str]}`; `format_digest(..., readiness=...)` adds "Ready to use today: …" to the headline and a "*Ready to send*" thread part.

- [ ] **Step 1: Write the failing tests**

In `test_inbox_intake.py`:

```python
def test_readiness_lines_group_by_domain():
    def a(e, t="GMAIL", rep="100%", start="2026-10-13T10:00:00Z"):
        return {"from_email": e, "type": t, "tags": [{"tag_name": "Bot-Setup"}],
                "warmup_details": {"warmup_created_at": start, "warmup_reputation": rep}}
    boxes = [a("x1@warmmeetings.com"), a("x2@warmmeetings.com"), a("y@low.com", rep="80%"),
             a("z@notyet.com", t="OUTLOOK"), {**a("n@untracked.com"), "tags": []}]
    r = ii.readiness_lines(boxes, date(2026, 10, 27))
    assert r["ready_today"] == ["2 Google inboxes on warmmeetings.com"]
    assert r["low"] == ["1 Google inbox on low.com: warmup reputation 80% (needs 90%)"]
```

In `test_zapmail_digest_thread.py`, using that file's existing `format_digest` call pattern, append a case passing `readiness={"ready_today": ["2 Google inboxes on warmmeetings.com"], "low": []}` and assert:

```python
    assert any(h.startswith("Ready to use today: 2 Google inboxes on warmmeetings.com") for h in head)
    assert any(p.startswith("*Ready to send*") for p in parts)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py::test_readiness_lines_group_by_domain test_zapmail_digest_thread.py`
Expected: FAIL (`readiness_lines` missing; `format_digest` has no `readiness`)

- [ ] **Step 3: Implement**

In `inbox_intake.py`:

```python
def readiness_lines(accounts: list[dict], today: date) -> dict:
    """Set-up inboxes that reach their ready date today, and ones past it but
    under 90% warmup reputation, grouped by domain."""
    per_domain: dict[str, int] = {}
    for a in accounts:
        dom = str(a.get("from_email", "")).split("@")[-1].lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
    ready: dict[tuple, int] = {}
    low: dict[tuple, list] = {}
    for a in accounts:
        if not _has_tag(a, st.SETUP_TAG):
            continue
        dom = str(a["from_email"]).split("@")[-1].lower()
        kind = st.inbox_kind(a, per_domain[dom])
        state = st.readiness(a, kind, today)
        key = (dom, st.standard_for(kind)["label"])
        if state == "ready" and st.ready_date(a, kind) == today:
            ready[key] = ready.get(key, 0) + 1
        elif state == "low_reputation":
            low.setdefault(key, []).append((a.get("warmup_details") or {}).get("warmup_reputation"))
    def n(k, label):
        return f"{k} {label} {'inbox' if k == 1 else 'inboxes'}"
    return {"ready_today": [f"{n(k, label)} on {dom}" for (dom, label), k in sorted(ready.items())],
            "low": [f"{n(len(r), label)} on {dom}: warmup reputation {r[0]} (needs {st.READY_REPUTATION}%)"
                    for (dom, label), r in sorted(low.items())]}
```

In `zapmail_digest.py` `format_digest`, add the keyword `readiness: dict | None = None`, and before `# ── headline ──`:

```python
    rd = readiness or {}
    if rd.get("low"):
        look.append(f"{_plural(len(rd['low']), 'domain')} past their warmup days but under 90% reputation")
```

after the `setup_bits` headline line:

```python
    if rd.get("ready_today"):
        head.append("Ready to use today: " + "; ".join(rd["ready_today"]))
```

and in the thread, after the ScaledMail part:

```python
    if rd.get("ready_today") or rd.get("low"):
        parts.append("\n".join(["*Ready to send*"] + ["• " + x for x in rd.get("ready_today", [])]
                               + ["• ⚠️ " + x for x in rd.get("low", [])]))
```

In `extra_sections(today)`, read every current-client account's inboxes and add `readiness`:

```python
    try:
        from smartlead.accounts import discover_accounts
        from smartlead.api import SmartleadClient
        from smartlead.inbox_intake import CURRENT_ACCOUNTS, readiness_lines
        boxes = []
        for acc in discover_accounts():
            if acc.name.upper() in CURRENT_ACCOUNTS:
                async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
                    boxes += await sl.list_email_accounts()
        out["readiness"] = readiness_lines(boxes, today)
    except Exception as exc:  # noqa: BLE001 - the digest still posts
        out["errors"].append(f"Ready-to-send check could not run: {str(exc)[:80]}")
```

Put this block just before `extra_sections` returns `out` (its dict already has `"errors": []`).

- [ ] **Step 4: Run to verify it passes**

Run: `cd smartlead_sync && python -m pytest -q test_inbox_intake.py test_zapmail_digest_thread.py`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add smartlead_sync/smartlead/inbox_intake.py smartlead_sync/zapmail_digest.py smartlead_sync/test_inbox_intake.py smartlead_sync/test_zapmail_digest_thread.py
git commit -m "Digest: inboxes ready to use today, and those past their days but under 90% reputation"
```

---

### Task 8: Full suite, live preview, catch-up with Manveen

**Files:** none new (verification and the one-time run).

- [ ] **Step 1: Full suites**

Run: `cd smartlead_sync && python -m pytest -q --ignore=test_smart_delivery.py --ignore=test_domain_suggest.py` then `cd .. && node --check index.js && npm test`
Expected: all Python tests pass (518 before this plan, plus the new ones); `22/22 test files passed`. (`test_smart_delivery.py` is a module-level live script that fails before this plan too; leave it.)

- [ ] **Step 2: Live preview (writes nothing)**

Run: `cd smartlead_sync && python inbox_intake_cli.py --since 2026-10-12`
Expected: "👀 Would set up N new inboxes", with today's ScaledMail orders (77129b SMTP 20, 13845d Google 3, 85b1cd Outlook 50 as "Outlook (shared domain)") and their ready dates; 2cb59b under "Waiting for the rest" if ScaledMail has not uploaded it. Show Manveen the output. Check the `Bot-Setup` tag exists in PRECISE_LEADS and BETTRDATA (the preview's "Needs a look" says so if not); Manveen creates it in Smartlead if missing.

- [ ] **Step 3: Catch-up run, only after Manveen says go**

Run: `cd smartlead_sync && python inbox_intake_cli.py --since 2026-10-12 --apply`
Expected: "✅ Set up 73 new inboxes" (or the preview's count), no problems.

- [ ] **Step 4: Independent read-back**

Re-read every inbox on the order domains straight from Smartlead (`GET /email-accounts/?offset=…`, fields `message_per_day`, `custom_tracking_domain`, `signature`, `client_id`, `tags`, `warmup_details.max_email_per_day`, `reply_rate`, `status`) and print one line per domain: count, limit, warmup/day, reply, tracking, signature yes/no, tags. Never print credentials. Expected: every row matches the standard table.

- [ ] **Step 5: Report and hand over**

Tell Manveen: what was set, the read-back table, the ready dates per domain, and the two Render steps: set `INBOX_INTAKE_ENABLED=true` (the cron then runs intake instead of the old readiness job) and optionally `INTAKE_SINCE`. Push only on "push it".
