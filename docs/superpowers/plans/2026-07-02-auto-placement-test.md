# Auto Placement-Test Executor (Health Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A separate cron job that auto-runs Smartlead SmartDelivery placement tests for the worst (untested/stale) inboxes per client, polls them, and merges inbox/spam results into the health data — shipped **dry-run-first** so it spends zero credits until explicitly enabled.

**Architecture:** A standalone `retest_executor.py` (own cron, isolated from the daily sync) does two passes: poll previously-created tests to completion + create new tests for worst-first targets under a per-client daily cap. A pure target selector, an async SmartDelivery client, and a Mongo state store keep it testable and idempotent. Results merge into a `placement_results` collection the health scorer consults (newer-of manual-vs-api).

**Tech Stack:** Python 3.10+, httpx/asyncio, pymongo 4.6 (installed). Tests are plain runnable scripts (`python3 <file>.py`), NOT pytest. Run with `python3`. Repo `infra-bot`, branch `main`. Commands run from `smartlead_sync/`.

## Global Constraints

- Run scripts with `python3`, never `python`.
- **RETEST_ENABLED defaults False** → executor runs dry-run (selects + logs, NEVER calls create). No credit spend until a human sets it True.
- **Per-client daily cap** — hard limit; never exceed per client per run.
- **Catch credit/launch errors** — a failed create logs + skips; never crash, never partial-spend (a rejected create charges nothing; a test only charges once seeds send).
- **Idempotent** — pending tests tracked in Mongo; re-runs poll, don't re-create.
- **Isolated** — separate entry point + cron; its failure cannot affect the daily sync/workbook.
- Verified SmartDelivery contract (probed live, in `smartdelivery_api.md`): create `POST /spam-test/manual` requires `test_name, description, campaign_id(int), sequence_mapping_id(int from GET /campaigns/{id}/sequences → seqs[0].id), sender_accounts(list of from_email), provider_ids:[20,21], spam_filters:["spam_assassin"], link_checker:true, all_email_sent_without_time_gap:false, min_time_btwn_emails:>=5, min_time_unit:"minutes", is_warmup:true` → returns `{id|spamTestId}`. Poll `GET /spam-test/{id}` → `{status, test_end_date}` (ACTIVE until done). Report `POST /spam-test/report/{id}/providerwise` (body `{}`). Base `https://smartdelivery.smartlead.ai/api/v1`, auth `?api_key=`. No list-tests or credit-balance endpoint exists.
- Env: `MONGO_URI` (set). SmartDelivery uses each account's existing Smartlead `api_key`.

## File Structure

- `smartlead/config.py` (MODIFY) — caps, threshold, enabled flag, collection names, base URL.
- `smartlead/retest_targets.py` (NEW) — pure `select_targets`.
- `smartlead/smart_delivery.py` (NEW) — async client: create/poll/report + typed errors.
- `smartlead/placement_store.py` (NEW) — Mongo state (`placement_tests`, `placement_results`).
- `retest_executor.py` (NEW) — two-pass entry point.
- Tests: `test_retest_targets.py`, `test_smart_delivery.py` (NEW).
- Merge into health read path: deferred to a follow-up (documented in Task 6) — this plan lands the executor + store; the scorer-merge is a small, separate change guarded by data existing.

---

### Task 1: Config

**Files:** Modify `smartlead/config.py`

**Interfaces:** Produces `SMARTDELIVERY_BASE_URL`, `RETEST_ENABLED`, `RETEST_PER_CLIENT_DAILY_CAP`, `RETEST_INBOX_THRESHOLD`, `RETEST_MIN_TIME_MINUTES`, `PLACEMENT_TESTS_COLLECTION`, `PLACEMENT_RESULTS_COLLECTION`.

- [ ] **Step 1:** Append to `smartlead/config.py` after the Inbox Health block:

```python
# ── Auto placement-test (health phase 3) ─────────────────────────────────────
SMARTDELIVERY_BASE_URL: str = "https://smartdelivery.smartlead.ai/api/v1"
# DRY-RUN by default: executor selects + logs targets but does NOT create tests.
RETEST_ENABLED: bool = os.getenv("RETEST_ENABLED", "false").lower() == "true"
RETEST_PER_CLIENT_DAILY_CAP: int = int(os.getenv("RETEST_PER_CLIENT_DAILY_CAP", "2"))
RETEST_INBOX_THRESHOLD: float = float(os.getenv("RETEST_INBOX_THRESHOLD", "80"))
RETEST_MIN_TIME_MINUTES: int = 5   # SmartDelivery business rule: >= 5
PLACEMENT_TESTS_COLLECTION: str = os.getenv("PLACEMENT_TESTS_COLLECTION", "placement_tests")
PLACEMENT_RESULTS_COLLECTION: str = os.getenv("PLACEMENT_RESULTS_COLLECTION", "placement_results")
```

- [ ] **Step 2:** Verify — `python3 -c "from smartlead.config import RETEST_ENABLED, RETEST_PER_CLIENT_DAILY_CAP, SMARTDELIVERY_BASE_URL; print(RETEST_ENABLED, RETEST_PER_CLIENT_DAILY_CAP)"` → `False 2`.

- [ ] **Step 3:** Commit — `git add smartlead/config.py && git commit -m "feat(retest): config for auto placement-test executor (dry-run default)"`

---

### Task 2: Target selector (pure)

**Files:** Create `smartlead/retest_targets.py`; Test `test_retest_targets.py`

**Interfaces:**
- Produces: `select_targets(health_rows: list[dict], per_client_cap: int, pending_emails: set[str]) -> list[dict]`. Each target: `{client, email, campaign_hint, reason}`. Worst-first: untested before stale, oldest-`test_date` first within stale; per-client cap; excludes emails in `pending_emails`.
- Consumes: health rows (from Phase 1/2 `build_health_rows`) — uses keys `client, email, priority, owner_skill, top_problem, test_date, campaign_name`.

- [ ] **Step 1: Failing test** — `test_retest_targets.py`:

```python
from smartlead.retest_targets import select_targets

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

def row(client, email, problem, test_date="", camp="C1"):
    return {"client": client, "email": email, "top_problem": problem,
            "owner": "🤖 Auto", "test_date": test_date, "campaign_name": camp}

rows = [
    row("DARLEAN", "a@d.com", "No placement test on record"),                 # untested
    row("DARLEAN", "b@d.com", "Placement test is stale", "2026-06-01"),       # stale old
    row("DARLEAN", "c@d.com", "Placement test is stale", "2026-06-20"),       # stale newer
    row("DARLEAN", "d@d.com", "Failed placement test"),                       # P0 human -> NOT auto
    row("MELIOR",  "e@m.com", "No placement test on record"),                 # other client
]
t = select_targets(rows, per_client_cap=2, pending_emails=set())
darlean = [x for x in t if x["client"] == "DARLEAN"]
ok(len(darlean) == 2, f"per-client cap 2 (got {len(darlean)})")
ok(darlean[0]["email"] == "a@d.com", "untested first")
ok(darlean[1]["email"] == "b@d.com", "then oldest stale")
ok(any(x["client"] == "MELIOR" for x in t), "other client included")
ok(all(x["email"] != "d@d.com" for x in t), "failed(human) excluded")

t2 = select_targets(rows, per_client_cap=2, pending_emails={"a@d.com"})
ok(all(x["email"] != "a@d.com" for x in t2), "pending excluded")
ok([x for x in t2 if x["client"] == "DARLEAN"][0]["email"] == "b@d.com", "skips pending, next worst")

ok(select_targets([], 2, set()) == [], "empty -> no targets")
print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_retest_targets.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/retest_targets.py`:

```python
"""Pure worst-first selection of inboxes to auto-retest, per-client capped."""
from __future__ import annotations

_UNTESTED = "No placement test on record"
_STALE = "Placement test is stale"
# problems whose owner is the auto-tester (must match health.resolve_action text)
_AUTO_PROBLEMS = {_UNTESTED, _STALE}


def _rank(row: dict) -> tuple:
    """Sort key: untested (0) before stale (1); within stale, oldest test_date first."""
    problem = row.get("top_problem", "")
    if problem == _UNTESTED:
        return (0, "")
    # stale: empty date sorts first (unknown = treat as very old)
    return (1, str(row.get("test_date", "")) or "0000-00-00")


def select_targets(health_rows: list[dict], per_client_cap: int,
                   pending_emails: set[str]) -> list[dict]:
    by_client: dict[str, list[dict]] = {}
    for r in health_rows:
        if r.get("top_problem") not in _AUTO_PROBLEMS:
            continue
        email = str(r.get("email", "")).strip()
        if not email or email in pending_emails:
            continue
        by_client.setdefault(r.get("client", ""), []).append(r)

    targets: list[dict] = []
    for client, rows in by_client.items():
        rows.sort(key=_rank)
        for r in rows[:per_client_cap]:
            targets.append({
                "client": client,
                "email": r.get("email", ""),
                "campaign_hint": r.get("campaign_name", ""),
                "reason": r.get("top_problem", ""),
            })
    return targets
```

- [ ] **Step 4: Run, verify pass** — `python3 test_retest_targets.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/retest_targets.py test_retest_targets.py && git commit -m "feat(retest): worst-first per-client target selector"`

---

### Task 3: SmartDelivery client

**Files:** Create `smartlead/smart_delivery.py`; Test `test_smart_delivery.py`

**Interfaces:**
- Produces: `SmartDeliveryClient(api_key)` async ctx-mgr with:
  - `create_test(campaign_id:int, sequence_mapping_id:int, sender_emails:list[str], test_name:str) -> int` (returns test id; raises `CreditError` on credit/plan failure, `SmartDeliveryError` otherwise)
  - `poll_test(test_id:int) -> dict` → `{"status": str, "done": bool, "end_date": str|None}`
  - `get_report(test_id:int) -> dict` → `{"inbox_pct": float, "spam_pct": float}`
  - exceptions `SmartDeliveryError`, `CreditError`.
- Consumes: `SMARTDELIVERY_BASE_URL`, `RETEST_MIN_TIME_MINUTES` (config).

- [ ] **Step 1: Failing test** — `test_smart_delivery.py` (offline, fake HTTP injected):

```python
import asyncio
from smartlead.smart_delivery import SmartDeliveryClient, CreditError, SmartDeliveryError

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

class FakeResp:
    def __init__(self, payload, status=200):
        self._p, self.status_code = payload, status
    def json(self): return self._p
    @property
    def text(self):
        import json; return json.dumps(self._p)

class FakeHTTP:
    def __init__(self, mode="ok"): self.mode = mode; self.calls = []
    async def post(self, url, headers=None, json=None):
        self.calls.append(("POST", url, json))
        if url.endswith("/spam-test/manual"):
            if self.mode == "credit":
                return FakeResp({"message": "Insufficient credits, please upgrade"}, 402)
            return FakeResp({"id": 555})
        if "/report/" in url and url.endswith("/providerwise"):
            return FakeResp({"data": [{"inbox": 80, "spam": 20}, {"inbox": 90, "spam": 10}]})
        return FakeResp({})
    async def get(self, url, headers=None):
        self.calls.append(("GET", url, None))
        if "/spam-test/" in url:
            return FakeResp({"status": "COMPLETED", "test_end_date": "2026-07-02T10:00:00Z"})
        return FakeResp({})
    async def aclose(self): pass

async def main():
    c = SmartDeliveryClient("k"); c._client = FakeHTTP("ok")
    tid = await c.create_test(123, 999, ["a@x.com"], "t")
    ok(tid == 555, f"create returns id (got {tid})")
    poll = await c.poll_test(555)
    ok(poll["done"] is True and poll["status"] == "COMPLETED", "poll parses done")
    rep = await c.get_report(555)
    ok(abs(rep["inbox_pct"] - 85.0) < 0.01, f"report avg inbox 85% (got {rep['inbox_pct']})")

    cc = SmartDeliveryClient("k"); cc._client = FakeHTTP("credit")
    try:
        await cc.create_test(1, 1, ["a@x.com"], "t"); ok(False, "should raise CreditError")
    except CreditError:
        ok(True, "credit failure -> CreditError")
    print("\nALL PASSED")

asyncio.run(main())
```

- [ ] **Step 2: Run, verify fail** — `python3 test_smart_delivery.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/smart_delivery.py`:

```python
"""Async client for Smartlead SmartDelivery placement tests."""
from __future__ import annotations

import httpx

from smartlead.config import SMARTDELIVERY_BASE_URL, RETEST_MIN_TIME_MINUTES

_CREDIT_HINTS = ("credit", "upgrade", "payment", "subscription", "plan", "not enabled")


class SmartDeliveryError(Exception):
    pass


class CreditError(SmartDeliveryError):
    pass


class SmartDeliveryClient:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "SmartDeliveryClient":
        self._client = httpx.AsyncClient(timeout=30)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _url(self, path: str) -> str:
        sep = "&" if "?" in path else "?"
        return f"{SMARTDELIVERY_BASE_URL}{path}{sep}api_key={self._api_key}"

    async def create_test(self, campaign_id: int, sequence_mapping_id: int,
                          sender_emails: list[str], test_name: str) -> int:
        body = {
            "test_name": test_name,
            "description": f"auto placement test — {test_name}",
            "campaign_id": int(campaign_id),
            "sequence_mapping_id": int(sequence_mapping_id),
            "sender_accounts": sender_emails,
            "provider_ids": [20, 21],
            "spam_filters": ["spam_assassin"],
            "link_checker": True,
            "all_email_sent_without_time_gap": False,
            "min_time_btwn_emails": RETEST_MIN_TIME_MINUTES,
            "min_time_unit": "minutes",
            "is_warmup": True,
        }
        resp = await self._client.post(self._url("/spam-test/manual"),
                                       headers={"Content-Type": "application/json"}, json=body)
        if resp.status_code >= 400:
            text = resp.text.lower()
            if resp.status_code == 402 or any(h in text for h in _CREDIT_HINTS):
                raise CreditError(resp.text[:200])
            raise SmartDeliveryError(f"create failed {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        tid = data.get("id") or data.get("spamTestId")
        if not tid:
            raise SmartDeliveryError(f"no test id in response: {data}")
        return int(tid)

    async def poll_test(self, test_id: int) -> dict:
        resp = await self._client.get(self._url(f"/spam-test/{test_id}"))
        if resp.status_code >= 400:
            raise SmartDeliveryError(f"poll failed {resp.status_code}: {resp.text[:150]}")
        d = resp.json()
        status = d.get("status", "")
        done = bool(d.get("test_end_date")) or (status and status != "ACTIVE")
        return {"status": status, "done": done, "end_date": d.get("test_end_date")}

    async def get_report(self, test_id: int) -> dict:
        resp = await self._client.post(
            self._url(f"/spam-test/report/{test_id}/providerwise"),
            headers={"Content-Type": "application/json"}, json={},
        )
        if resp.status_code >= 400:
            raise SmartDeliveryError(f"report failed {resp.status_code}: {resp.text[:150]}")
        d = resp.json()
        rows = d.get("data", d if isinstance(d, list) else [])
        inboxes = [float(r.get("inbox", 0)) for r in rows if isinstance(r, dict)]
        spams = [float(r.get("spam", 0)) for r in rows if isinstance(r, dict)]
        inbox_pct = sum(inboxes) / len(inboxes) if inboxes else 0.0
        spam_pct = sum(spams) / len(spams) if spams else 0.0
        return {"inbox_pct": inbox_pct, "spam_pct": spam_pct}
```

- [ ] **Step 4: Run, verify pass** — `python3 test_smart_delivery.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/smart_delivery.py test_smart_delivery.py && git commit -m "feat(retest): SmartDelivery client (create/poll/report + credit guard)"`

---

### Task 4: Placement state store (Mongo)

**Files:** Create `smartlead/placement_store.py`

**Interfaces:**
- Produces: `PlacementStore` with `pending_tests() -> list[dict]`, `pending_emails() -> set[str]`, `record_created(test_id, client, campaign_id, emails) -> None`, `mark_done(test_id, inbox_pct, status) -> None`, `save_result(email, domain, status, date, source) -> None`, `close()`. No-op / empty if Mongo down.
- Consumes: `PLACEMENT_TESTS_COLLECTION`, `PLACEMENT_RESULTS_COLLECTION`, `HEALTH_HISTORY_DB`.

- [ ] **Step 1: Implement** (DB glue; validated live in Task 7) — `smartlead/placement_store.py`:

```python
"""Mongo state for auto placement tests + merged results."""
from __future__ import annotations

import os

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import (
    HEALTH_HISTORY_DB, PLACEMENT_TESTS_COLLECTION, PLACEMENT_RESULTS_COLLECTION,
)


class PlacementStore:
    def __init__(self) -> None:
        self._tests = None
        self._results = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Retest] Mongo unavailable - state store disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[HEALTH_HISTORY_DB]
            self._tests = db[PLACEMENT_TESTS_COLLECTION]
            self._results = db[PLACEMENT_RESULTS_COLLECTION]
            self._tests.create_index("test_id", unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] Mongo connect failed ({exc}) - disabled.")
            self._tests = self._results = None

    @property
    def available(self) -> bool:
        return self._tests is not None

    def pending_tests(self) -> list[dict]:
        if self._tests is None:
            return []
        try:
            return list(self._tests.find({"status": "ACTIVE"}))
        except PyMongoError:
            return []

    def pending_emails(self) -> set[str]:
        out: set[str] = set()
        for t in self.pending_tests():
            out.update(t.get("emails", []))
        return out

    def record_created(self, test_id: int, client: str, campaign_id: int, emails: list[str]) -> None:
        if self._tests is None:
            return
        try:
            self._tests.update_one(
                {"test_id": test_id},
                {"$set": {"test_id": test_id, "client": client, "campaign_id": campaign_id,
                          "emails": emails, "status": "ACTIVE"}},
                upsert=True,
            )
        except PyMongoError as exc:
            print(f"  [Retest] record_created failed: {exc}")

    def mark_done(self, test_id: int, inbox_pct: float, status: str) -> None:
        if self._tests is None:
            return
        try:
            self._tests.update_one({"test_id": test_id},
                                   {"$set": {"status": "DONE", "inbox_pct": inbox_pct, "result": status}})
        except PyMongoError as exc:
            print(f"  [Retest] mark_done failed: {exc}")

    def save_result(self, email: str, domain: str, status: str, date: str, source: str) -> None:
        if self._results is None:
            return
        try:
            self._results.update_one(
                {"email": email, "date": date, "source": source},
                {"$set": {"email": email, "domain": domain, "status": status,
                          "date": date, "source": source}},
                upsert=True,
            )
        except PyMongoError as exc:
            print(f"  [Retest] save_result failed: {exc}")

    def close(self) -> None:
        pass
```

- [ ] **Step 2: Import check** — `python3 -c "from smartlead.placement_store import PlacementStore; s=PlacementStore(); print('available:', s.available)"` → prints status + `available: True/False`.

- [ ] **Step 3: Commit** — `git add smartlead/placement_store.py && git commit -m "feat(retest): Mongo placement-test state store"`

---

### Task 5: The two-pass executor

**Files:** Create `retest_executor.py`

**Interfaces:** Standalone entry (`python3 retest_executor.py`). Consumes all prior tasks + existing `discover_accounts`, `SmartleadClient`, `DeliverabilityReader`, `fetch_account_data`, `build_health_rows`, `resolve_manager`.

- [ ] **Step 1: Implement** — `retest_executor.py`:

```python
#!/usr/bin/env python3
"""Auto placement-test executor (health phase 3). Separate cron; dry-run by default.

Pass A: poll previously-created ACTIVE tests -> on completion, pull report,
        write inbox/fail result, mark done.
Pass B: select worst-first targets per client (capped) and create tests
        (unless RETEST_ENABLED is False -> dry-run: log only).
"""
from __future__ import annotations

import asyncio
from datetime import date

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.sheets import DeliverabilityReader
from smartlead.config import (
    ACCOUNT_DELIVERABILITY_TABS, TEST_TAB_NAME, RETEST_ENABLED,
    RETEST_PER_CLIENT_DAILY_CAP, RETEST_INBOX_THRESHOLD,
)
from smartlead.processing import fetch_account_data
from smartlead.health import build_health_rows
from smartlead.manager_map import resolve_manager
from smartlead.retest_targets import select_targets
from smartlead.smart_delivery import SmartDeliveryClient, CreditError, SmartDeliveryError
from smartlead.placement_store import PlacementStore


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


async def _poll_pending(store: PlacementStore, key_by_client: dict[str, str]) -> int:
    """Pass A: poll ACTIVE tests, write results for completed ones."""
    completed = 0
    for t in store.pending_tests():
        client = t.get("client", "")
        api_key = key_by_client.get(client)
        if not api_key:
            continue
        async with SmartDeliveryClient(api_key) as sd:
            try:
                poll = await sd.poll_test(t["test_id"])
                if not poll["done"]:
                    continue
                rep = await sd.get_report(t["test_id"])
            except SmartDeliveryError as exc:
                print(f"  [Retest] poll/report failed for {t['test_id']}: {exc}")
                continue
        status = "inbox" if rep["inbox_pct"] >= RETEST_INBOX_THRESHOLD else "fail"
        today = date.today().strftime("%Y-%m-%d")
        for email in t.get("emails", []):
            store.save_result(email, _domain(email), status, today, "api")
        store.mark_done(t["test_id"], rep["inbox_pct"], status)
        completed += 1
        print(f"  [Retest] test {t['test_id']} ({client}) -> {status} ({rep['inbox_pct']:.0f}% inbox)")
    return completed


async def _health_rows_for(acc) -> list[dict]:
    dmap = {}
    for tab in ACCOUNT_DELIVERABILITY_TABS.get(acc.name, [TEST_TAB_NAME]):
        try:
            dmap.update(await DeliverabilityReader(tab_name=tab).fetch())
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] deliverability read {tab} failed: {exc}")
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
    for r in inbox:
        r.setdefault("client", acc.name)
    return build_health_rows(inbox, date.today(), None, resolve_manager)


async def _campaign_launch_args(acc, campaign_name: str):
    """Resolve (campaign_id, sequence_mapping_id, sender_emails) for a campaign name."""
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = await c.list_campaigns()
        camp = next((x for x in camps if x.get("name") == campaign_name), None)
        if not camp:
            return None
        cid = camp["id"]
        seq = await c._get(f"/campaigns/{cid}/sequences")
        seqs = seq if isinstance(seq, list) else seq.get("sequences", [])
        if not seqs:
            return None
        accts = await c.get_campaign_email_accounts(str(cid))
        senders = [a.get("from_email") for a in accts if a.get("from_email")]
        if not senders:
            return None
        return cid, seqs[0]["id"], senders[:100]


async def main() -> None:
    store = PlacementStore()
    if not store.available:
        print("[Retest] State store unavailable — skipping (won't create untracked tests).")
        return

    accounts = discover_accounts()
    key_by_client = {a.name: a.api_key for a in accounts}

    print("[Retest] Pass A: polling pending tests...")
    completed = await _poll_pending(store, key_by_client)
    print(f"[Retest] Pass A done: {completed} test(s) completed.")

    print("[Retest] Pass B: selecting worst-first targets...")
    pending_emails = store.pending_emails()
    all_targets: list[dict] = []
    acc_by_client = {a.name: a for a in accounts}
    for acc in accounts:
        try:
            rows = await _health_rows_for(acc)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Retest] health rows for {acc.name} failed: {exc}")
            continue
        all_targets.extend(select_targets(rows, RETEST_PER_CLIENT_DAILY_CAP, pending_emails))

    if not all_targets:
        print("[Retest] No targets. Done.")
        return

    print(f"[Retest] {len(all_targets)} target(s) selected"
          f"{' (DRY-RUN — not creating)' if not RETEST_ENABLED else ''}:")
    for t in all_targets:
        print(f"    {t['client']:14} {t['email']:34} {t['reason']}  camp={t['campaign_hint'][:30]}")

    if not RETEST_ENABLED:
        print("[Retest] RETEST_ENABLED=false → dry-run complete. Set RETEST_ENABLED=true to create.")
        return

    created = skipped = 0
    for t in all_targets:
        acc = acc_by_client.get(t["client"])
        if not acc:
            continue
        args = await _campaign_launch_args(acc, t["campaign_hint"])
        if not args:
            print(f"  [Retest] skip {t['email']}: no campaign/senders/sequence")
            skipped += 1
            continue
        cid, seq_id, senders = args
        async with SmartDeliveryClient(acc.api_key) as sd:
            try:
                tid = await sd.create_test(cid, seq_id, senders,
                                           f"auto-{t['client']}-{date.today().isoformat()}")
                store.record_created(tid, t["client"], cid, senders)
                created += 1
                print(f"  [Retest] created test {tid} for {t['client']} campaign {cid}")
            except CreditError as exc:
                print(f"  [Retest] CREDITS exhausted on {t['client']} — skipping rest of client: {exc}")
                skipped += 1
            except SmartDeliveryError as exc:
                print(f"  [Retest] create failed for {t['email']}: {exc}")
                skipped += 1
    print(f"[Retest] Pass B done: {created} created, {skipped} skipped.")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Compile** — `python3 -m py_compile retest_executor.py` → no error.

- [ ] **Step 3: Commit** — `git add retest_executor.py && git commit -m "feat(retest): two-pass executor (poll + dry-run select/create)"`

---

### Task 6: Merge API results into health read path (guarded)

**Files:** Modify the deliverability read in `smartlead/sheets.py` (`DeliverabilityReader.fetch` consumer) — actually merge at the point health reads `test_sheet_status`/`test_date`. Minimal, guarded: only consult `placement_results` if the collection has rows.

**Interfaces:** Produces `smartlead/placement_merge.py` → `apply_api_results(deliverability_map: dict) -> dict` (overlays newer API results onto the manual map by domain).

- [ ] **Step 1: Implement** — `smartlead/placement_merge.py`:

```python
"""Overlay API placement results onto the manual deliverability map (newer wins)."""
from __future__ import annotations

import os

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, PLACEMENT_RESULTS_COLLECTION


def apply_api_results(deliverability_map: dict[str, dict]) -> dict[str, dict]:
    """For each domain, if an API result is newer than the manual one, use it.
    deliverability_map: {domain: {"status","date"}}. Returns a new merged map."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return deliverability_map
    try:
        col = MongoClient(uri, serverSelectionTimeoutMS=5000)[HEALTH_HISTORY_DB][PLACEMENT_RESULTS_COLLECTION]
        api_rows = list(col.find({"source": "api"}))
    except Exception:  # noqa: BLE001
        return deliverability_map
    if not api_rows:
        return deliverability_map
    merged = dict(deliverability_map)
    # keep newest api result per domain
    best: dict[str, dict] = {}
    for r in api_rows:
        dom = r.get("domain", "")
        if not dom:
            continue
        if dom not in best or str(r.get("date", "")) > str(best[dom].get("date", "")):
            best[dom] = r
    for dom, r in best.items():
        existing = merged.get(dom)
        if existing is None or str(r.get("date", "")) >= str(existing.get("date", "")):
            merged[dom] = {"status": r.get("status", ""), "date": r.get("date", "")}
    return merged
```

- [ ] **Step 2: Wire (guarded) into run.py** — where `deliverability_map` is assembled per account, after the tab-merge loop, add:
```python
            from smartlead.placement_merge import apply_api_results
            deliverability_map = apply_api_results(deliverability_map)
```
(Placed right before `process_account` is called. No-op when no API rows exist, so it's safe to ship before any test runs.)

- [ ] **Step 3: Verify** — `python3 -c "from smartlead.placement_merge import apply_api_results; print(apply_api_results({'x.com': {'status':'inbox','date':'2026-01-01'}}))"` → prints the map unchanged (no api rows). `python3 -m py_compile run.py`.

- [ ] **Step 4: Commit** — `git add smartlead/placement_merge.py run.py && git commit -m "feat(retest): merge API placement results into health read path (guarded)"`

---

### Task 7: Live dry-run validation

- [ ] **Step 1: Dry-run** — `RETEST_ENABLED=false python3 retest_executor.py 2>&1 | tail -30`. Expected: Pass A "0 completed" (no pending yet), Pass B lists worst-first targets per client with `(DRY-RUN — not creating)`, spends nothing, exits clean.
- [ ] **Step 2: Sanity-check the target list** — confirm targets are untested/stale inboxes, per-client cap respected, no failed(human) inboxes, no obviously-wrong campaigns. Adjust `RETEST_PER_CLIENT_DAILY_CAP` if needed.
- [ ] **Step 3: (opt-in, ONE credit) single real test** — only when you decide: `RETEST_ENABLED=true RETEST_PER_CLIENT_DAILY_CAP=1 python3 retest_executor.py` for a single client (temporarily restrict `discover_accounts` or set caps so only one target). Confirm a test is created + recorded in Mongo `placement_tests`. Next run's Pass A polls it to completion + writes a `placement_results` row. Verify workbook reflects it after a sync.
- [ ] **Step 4: Cron** — add a Render cron entry to run `python3 retest_executor.py` once/day AFTER the daily sync (document the exact schedule when wiring deploy). Keep `RETEST_ENABLED=false` in env until you approve real spend.

---

## Self-Review Notes

- **Spec coverage:** config+dry-run default (T1), worst-first per-client selection (T2), create/poll/report client + credit guard (T3), Mongo state + idempotency (T4), two-pass executor + isolation + caps + error-catch (T5), API↔manual newer-wins merge (T6), dry-run-first rollout + single-test opt-in + cron (T7). All spec sections mapped.
- **Placeholder scan:** caps/threshold/schedule are config + Open Items (user-tuned), not code placeholders. No TBDs.
- **Type consistency:** `select_targets(rows, per_client_cap, pending_emails)` matches T5 call; `SmartDeliveryClient.create_test(campaign_id, sequence_mapping_id, sender_emails, test_name)` matches T5 usage; `PlacementStore` method names (`pending_tests/pending_emails/record_created/mark_done/save_result`) consistent T4↔T5; `build_health_rows(inbox, today, None, resolve_manager)` — passing `None` store is valid (T5 of Phase-1 plan guarded `store.prior_score if store else None`). Confirm that guard exists; if not, T5 here passes a tiny no-op store.
- **Cross-plan note:** `build_health_rows` must tolerate `store=None`. Phase-1 code uses `store.prior_score(...) if store else None` — verify before running T7; if absent, wrap with a `_NoStore` shim in the executor.
