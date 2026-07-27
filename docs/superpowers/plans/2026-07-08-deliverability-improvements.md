# Deliverability Improvements Round 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the slow executors fast, close the warmup reply-rate blind spot, add "order N domains by DATE" capacity advisories, and build the per-domain reply-rate early-warning monitor (the best leading indicator of deliverability decay).

**Architecture:** Four independent tasks on the existing `smartlead_sync` Python codebase. Pure logic goes in `smartlead/` modules with unit tests; each new automation is a standalone executor script wired into `index.js` cron, following the repo's dry-run-by-default convention. Read-only jobs (capacity, reply monitor) need no enable flag.

**Tech Stack:** Python 3.10+ (httpx async, pymongo, gspread via existing `SheetsWriter`), node-cron in `index.js`, pytest for tests.

## Global Constraints

- Run Python locally as `python3` (not `python`); `index.js` crons spawn `python` (Render image resolves it — do not change).
- All crons use `timezone: 'Asia/Kolkata'`.
- Every inbox-level loop must drop excluded clients via `smartlead.client_filter.is_excluded_inbox(row)`.
- Anything that WRITES to Smartlead/Sheets-settings ships DRY-RUN by default behind an `*_ENABLED` env flag (existing convention). Read-only reporting jobs don't need a flag.
- Mongo access mirrors `health_store.py`: `MongoClient(uri, serverSelectionTimeoutMS=5000)`, db `HEALTH_HISTORY_DB` (default `infrabot`), graceful no-op when `MONGO_URI` unset.
- Warmup profile numbers are fixed by policy (docs/DELIVERABILITY_MASTER_PLAN.md §1): ACTIVE 20/day, IDLE 20/day, RECOVERING 15/day, NEW 40/day cap. Do not change them in this plan.
- Tests live in `smartlead_sync/` as `test_*.py`, run with `python3 -m pytest <file> -v` from `smartlead_sync/`.
- Commit after every task (repo commits directly to `main`; conventional-commit style, see `git log`).

---

### Task 1: Fast stale-campaign check (shared helper, ~1h runtime → minutes)

The warmup and headroom executors each take ~1 hour because `_stale_campaign_names` fetches EVERY lead of EVERY active campaign (paginated, rate-limited) just to find the newest lead's `created_at`. The lead fetch is only needed to decide staleness for campaigns that are NOT sending — a campaign with sends in the last 14 days is fresh by definition (see `is_campaign_stale`: stale requires no sends AND no new leads). So: check `sent_count` first (one cheap analytics call per campaign), and fetch leads ONLY when `sent14 == 0`. In this fleet most active campaigns send daily, so this skips the expensive call for almost every campaign.

**Files:**
- Modify: `smartlead_sync/smartlead/campaign_freshness.py` (add async helper at end of file)
- Modify: `smartlead_sync/warmup_executor.py` (delete local `_stale_campaign_names`, lines 35-54; import shared helper)
- Modify: `smartlead_sync/headroom_fix_executor.py` (delete local `_stale_campaign_names`, lines 48-67; import shared helper)
- Test: `smartlead_sync/test_stale_campaign_names.py` (create)

**Interfaces:**
- Consumes: `SmartleadClient.list_campaigns()`, `.get_campaign_leads(cid)`, `.get_analytics_by_date(cid, start, end)` (all exist in `smartlead/api.py`); `is_campaign_stale(newest_lead_created, sent14, today)` and `STALE_DAYS` (exist in `campaign_freshness.py`).
- Produces: `async def stale_campaign_names(client, today: date) -> set[str]` in `smartlead/campaign_freshness.py` — returns names of ACTIVE campaigns that are stale. Both executors call it as `await stale_campaign_names(c, today)`.

- [ ] **Step 1: Write the failing test**

Create `smartlead_sync/test_stale_campaign_names.py`:

```python
"""stale_campaign_names must (a) classify correctly and (b) only fetch leads
for campaigns with zero sends in the window — the perf contract."""
import asyncio
from datetime import date

from smartlead.campaign_freshness import stale_campaign_names


class FakeClient:
    """Stub of SmartleadClient recording which campaigns had leads fetched."""

    def __init__(self, campaigns, sent_by_id, leads_by_id):
        self._campaigns = campaigns
        self._sent = sent_by_id
        self._leads = leads_by_id
        self.leads_fetched_for: list[str] = []

    async def list_campaigns(self):
        return self._campaigns

    async def get_analytics_by_date(self, cid, start, end):
        return {"sent_count": self._sent.get(str(cid), 0)}

    async def get_campaign_leads(self, cid):
        self.leads_fetched_for.append(str(cid))
        return self._leads.get(str(cid), [])


def test_sending_campaign_skips_lead_fetch_and_is_fresh():
    c = FakeClient(
        campaigns=[{"id": 1, "name": "sender", "status": "ACTIVE"}],
        sent_by_id={"1": 500},
        leads_by_id={},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()
    assert c.leads_fetched_for == []  # perf contract: no lead fetch


def test_zero_sent_with_old_leads_is_stale_and_fetches_leads():
    c = FakeClient(
        campaigns=[{"id": 2, "name": "zombie", "status": "ACTIVE"}],
        sent_by_id={"2": 0},
        leads_by_id={"2": [{"created_at": "2026-05-01T00:00:00Z"}]},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == {"zombie"}
    assert c.leads_fetched_for == ["2"]


def test_zero_sent_with_fresh_leads_is_not_stale():
    c = FakeClient(
        campaigns=[{"id": 3, "name": "just-loaded", "status": "ACTIVE"}],
        sent_by_id={"3": 0},
        leads_by_id={"3": [{"created_at": "2026-07-07T00:00:00Z"}]},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()


def test_non_active_campaigns_ignored_entirely():
    c = FakeClient(
        campaigns=[{"id": 4, "name": "done", "status": "COMPLETED"}],
        sent_by_id={},
        leads_by_id={},
    )
    stale = asyncio.run(stale_campaign_names(c, date(2026, 7, 8)))
    assert stale == set()
    assert c.leads_fetched_for == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd smartlead_sync && python3 -m pytest test_stale_campaign_names.py -v`
Expected: FAIL with `ImportError: cannot import name 'stale_campaign_names'`

- [ ] **Step 3: Add the shared helper to campaign_freshness.py**

Append to `smartlead_sync/smartlead/campaign_freshness.py`:

```python
async def stale_campaign_names(client, today) -> set[str]:
    """Names of ACTIVE campaigns that are stale (dead 14d+).

    PERF CONTRACT (2026-07-08): the full-lead pagination is the expensive
    call (~1h across the fleet). A campaign with ANY sends in the window is
    fresh by definition (is_campaign_stale requires no sends AND no new
    leads), so leads are fetched ONLY for campaigns whose 14d sent_count
    is 0 — in practice a handful instead of all of them.
    `client` is a SmartleadClient (or any object with list_campaigns /
    get_analytics_by_date / get_campaign_leads)."""
    from datetime import timedelta

    stale: set[str] = set()
    start = (today - timedelta(days=STALE_DAYS)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    for camp in await client.list_campaigns():
        if str(camp.get("status", "")).upper() != "ACTIVE":
            continue
        cid = str(camp["id"])
        try:
            an = await client.get_analytics_by_date(cid, start, end)
            sent14 = int(float(an.get("sent_count", 0) or 0))
            if sent14 > 0:
                continue  # sending -> fresh; skip the expensive lead fetch
            leads = await client.get_campaign_leads(cid)
            newest = max((l.get("created_at", "") for l in leads), default="")
        except Exception as exc:  # noqa: BLE001
            print(f"  [Freshness] check failed for {cid}: {exc}")
            continue
        if is_campaign_stale(newest, sent14, today):
            stale.add(camp.get("name", ""))
    return stale
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd smartlead_sync && python3 -m pytest test_stale_campaign_names.py -v`
Expected: 4 passed. Also run the existing suite to catch regressions: `python3 -m pytest test_campaign_freshness.py -v` — expected: all pass.

- [ ] **Step 5: Point warmup_executor at the shared helper**

In `smartlead_sync/warmup_executor.py`:
- Delete the whole local function `async def _stale_campaign_names(...)` (lines 35-54).
- Change the import line `from smartlead.campaign_freshness import is_campaign_stale, STALE_DAYS` to `from smartlead.campaign_freshness import stale_campaign_names`.
- Delete the now-unused `from datetime import date, timedelta` → keep `date` only if still used (it is, in `main()`): change to `from datetime import date`.
- In `_raw_inbox_rows`, change `stale_names = await _stale_campaign_names(c, today)` to `stale_names = await stale_campaign_names(c, today)`.

- [ ] **Step 6: Point headroom_fix_executor at the shared helper**

In `smartlead_sync/headroom_fix_executor.py`: same three edits — delete local `_stale_campaign_names` (lines 48-67), import `stale_campaign_names` from `smartlead.campaign_freshness` (replacing the `is_campaign_stale, STALE_DAYS` import), fix the call site in `_plan_for_account` and trim `timedelta` from the datetime import if now unused.

- [ ] **Step 7: Compile-check + verify runtime drop with a live dry-run**

Run: `cd smartlead_sync && python3 -m py_compile warmup_executor.py headroom_fix_executor.py smartlead/campaign_freshness.py && echo OK`
Expected: OK

Run: `time python3 headroom_fix_executor.py 2>&1 | tail -5`
Expected: same plan counts as the previous run (~110 inboxes) but wall-clock minutes, not ~1h. If counts differ wildly (±50%), STOP — the gate logic changed classification; investigate before committing.

- [ ] **Step 8: Commit**

```bash
git add smartlead_sync/smartlead/campaign_freshness.py smartlead_sync/warmup_executor.py smartlead_sync/headroom_fix_executor.py smartlead_sync/test_stale_campaign_names.py
git commit -m "perf(freshness): fetch leads only for zero-send campaigns; share stale check"
```

---

### Task 2: Pull live warmup reply-rate through the sync (closes LONG_IDLE idempotency gap)

`warmup_planner._reply_rate_off_target` already reads `row["warmup_reply_rate_pct"]` but nothing populates it, so the LONG_IDLE nudge re-applies every run (documented fail-safe). Smartlead's account detail carries the current setting in `warmup_details`. Pull it through so the planner emits a change only when the value actually differs.

**Files:**
- Modify: `smartlead_sync/smartlead/processing.py` (`_build_warmup_data` rep_map ~line 542; `process_inbox_availability` ~line 357)
- Test: `smartlead_sync/test_reply_rate_passthrough.py` (create)

**Interfaces:**
- Consumes: `acc["warmup_details"]` dict from Smartlead account detail (already used for `warmup_reputation`, `warmup_max_count`).
- Produces: inbox rows gain `warmup_reply_rate_pct: int | None` (None = field absent upstream). `warmup_planner._reply_rate_off_target` consumes it unchanged.

- [ ] **Step 1: Verify the live field name (one-off, before writing code)**

Run from `smartlead_sync/`:

```bash
python3 - <<'EOF'
import asyncio
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient

async def main():
    acc = discover_accounts()[0]
    async with SmartleadClient(acc.api_key, acc.name) as c:
        accounts = await c.list_email_accounts()
        detail = await c.get_email_account(str(accounts[0]["id"]))
    print(sorted((detail.get("warmup_details") or {}).keys()))

asyncio.run(main())
EOF
```

Expected: a key list containing a reply-rate field — likely `reply_rate` or `reply_rate_percentage`. **Use whichever name actually prints** in Step 4's code (the code below assumes `reply_rate`; adjust if the live key differs). If NO reply-rate-like key exists, STOP — report back; do not fake the field.

- [ ] **Step 2: Write the failing test**

Create `smartlead_sync/test_reply_rate_passthrough.py`:

```python
"""warmup_reply_rate_pct must flow from warmup_details into inbox rows,
and the planner must stop re-emitting LONG_IDLE nudges once it matches."""
from smartlead.processing import process_inbox_availability


def _mk_row(email="a@x.com"):
    return {
        "email": email, "connection_ok": True, "message_per_day": 30,
        "test_sheet_status": "inbox", "dns_spf_ok": True,
        "dns_dkim_ok": True, "dns_dmarc_ok": True,
    }


def test_reply_rate_flows_into_row():
    row = _mk_row()
    rep_map = {"a@x.com": {"rep": "99%", "warmup_state": "on",
                           "warmup_max_count": 20, "last_active_date": "",
                           "warmup_spam_count": 0, "warmup_reply_rate_pct": 28}}
    process_inbox_availability([row], rep_map, {})
    assert row["warmup_reply_rate_pct"] == 28


def test_missing_reply_rate_is_none():
    row = _mk_row()
    rep_map = {"a@x.com": {"rep": "99%", "warmup_state": "on",
                           "warmup_max_count": 20, "last_active_date": "",
                           "warmup_spam_count": 0}}
    process_inbox_availability([row], rep_map, {})
    assert row["warmup_reply_rate_pct"] is None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd smartlead_sync && python3 -m pytest test_reply_rate_passthrough.py -v`
Expected: FAIL with `KeyError: 'warmup_reply_rate_pct'`

- [ ] **Step 4: Implement the pass-through**

In `smartlead_sync/smartlead/processing.py`, inside `_build_warmup_data`, in the `rep_map[email] = {...}` dict (~line 542), add one entry (using the field name verified in Step 1):

```python
            "warmup_reply_rate_pct": warmup_details.get("reply_rate"),
```

In `process_inbox_availability` (~line 357, next to the other `item[...] = info.get(...)` lines), add:

```python
        item["warmup_reply_rate_pct"] = info.get("warmup_reply_rate_pct") if isinstance(info, dict) else None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd smartlead_sync && python3 -m pytest test_reply_rate_passthrough.py -v`
Expected: 2 passed. Also confirm the planner consumes it end-to-end:

```bash
python3 - <<'EOF'
from smartlead.warmup_planner import plan_warmup_changes
from datetime import date, timedelta
old = (date.today() - timedelta(days=45)).isoformat()
row = {"email": "z@x.com", "account_id": "9", "client": "C", "warmup_state": "on",
       "warmup_rep_pct": "99%", "warmup_max_count": 20, "campaign_status": "",
       "last_active_date": old, "warmup_reply_rate_pct": 28}
assert plan_warmup_changes([row]) == [], "LONG_IDLE at target 28 must emit nothing"
row["warmup_reply_rate_pct"] = 25
assert plan_warmup_changes([row]), "reply rate 25 != 28 must emit the nudge"
print("PLANNER_IDEMPOTENCY_OK")
EOF
```

Expected: `PLANNER_IDEMPOTENCY_OK`

- [ ] **Step 6: Commit**

```bash
git add smartlead_sync/smartlead/processing.py smartlead_sync/test_reply_rate_passthrough.py
git commit -m "feat(warmup): pull live reply-rate through sync; LONG_IDLE nudge now idempotent"
```

---

### Task 3: Capacity planner — "order N domains by DATE" advisories

Read-only Monday job. Per client: demand (7-day average actual sends) vs safe capacity (healthy inboxes × capped daily volume) vs bench (warmed idle inboxes). Emits ORDER advisories with the 4-6-week lead time baked in. Also builds the `domain_registry` Mongo collection (first-seen date per domain) that future age-gating needs.

**Files:**
- Create: `smartlead_sync/smartlead/capacity.py` (pure math — no I/O)
- Create: `smartlead_sync/capacity_planner.py` (executor)
- Modify: `smartlead_sync/smartlead/sheets.py` (add `CAPACITY_COLUMNS` + `write_capacity` after `write_campaign_metrics`, ~line 634)
- Modify: `smartlead_sync/smartlead/config.py` (constants, after the blacklist block)
- Modify: `index.js` (cron Monday 09:30 IST, after the blacklist cron)
- Test: `smartlead_sync/test_capacity.py` (create)

**Interfaces:**
- Consumes: inbox rows from `fetch_account_data` (fields: `warmup_rep_pct`, `warmup_state`, `test_sheet_status`, `connection_ok`, `message_per_day`, `campaign_status`, `email`, `client`); `stale_campaign_names` from Task 1; `get_analytics_by_date` for demand.
- Produces: `compute_client_capacity(rows: list[dict], demand_per_day: float, churn_per_month: int) -> dict` in `smartlead/capacity.py` returning `{sendable_inboxes, safe_capacity, bench, bench_target, demand_per_day, headroom_pct, order_inboxes, order_domains, order_by, status}`; `SheetsWriter.write_capacity(rows: list[dict])`.

- [ ] **Step 1: Add config constants**

In `smartlead_sync/smartlead/config.py`, after the `BLACKLIST_COLLECTION` line:

```python
# ── Capacity planner (read-only Monday advisory) ─────────────────────────────
CAPACITY_TAB_NAME: str = os.getenv("CAPACITY_TAB_NAME", "Capacity")
CAPACITY_PER_INBOX_CAP: int = int(os.getenv("CAPACITY_PER_INBOX_CAP", "30"))   # sends/day counted per healthy inbox
CAPACITY_HEADROOM: float = float(os.getenv("CAPACITY_HEADROOM", "1.2"))        # demand buffer (20%)
CAPACITY_BENCH_RATIO: float = float(os.getenv("CAPACITY_BENCH_RATIO", "0.25")) # bench = 25% of active fleet
CAPACITY_BENCH_MIN: int = int(os.getenv("CAPACITY_BENCH_MIN", "5"))
CAPACITY_LEAD_TIME_DAYS: int = int(os.getenv("CAPACITY_LEAD_TIME_DAYS", "35")) # purchase+warmup ≈ 5 weeks
DOMAIN_REGISTRY_COLLECTION: str = os.getenv("DOMAIN_REGISTRY_COLLECTION", "domain_registry")
```

- [ ] **Step 2: Write the failing tests for the pure math**

Create `smartlead_sync/test_capacity.py`:

```python
from smartlead.capacity import compute_client_capacity


def _inbox(rep="99%", state="on", test="inbox", conn=True, mpd=30, status="ACTIVE"):
    return {"warmup_rep_pct": rep, "warmup_state": state, "test_sheet_status": test,
            "connection_ok": conn, "message_per_day": mpd, "campaign_status": status,
            "email": "e@x.com", "client": "C"}


def test_healthy_fleet_no_order():
    # 10 healthy active inboxes (cap 30 -> 300/day), demand 100/day, bench 5
    rows = [_inbox() for _ in range(10)] + \
           [_inbox(status="", state="on") for _ in range(5)]  # idle bench
    out = compute_client_capacity(rows, demand_per_day=100, churn_per_month=0)
    assert out["safe_capacity"] == 300
    assert out["bench"] == 5
    assert out["bench_target"] == 5  # max(5, 25% of 10 active = 2.5 -> 3) -> 5
    assert out["order_inboxes"] == 0
    assert out["status"] == "OK"


def test_capacity_shortfall_orders_inboxes_and_domains():
    # 2 healthy active inboxes = 60/day capacity, demand 200/day
    rows = [_inbox() for _ in range(2)]
    out = compute_client_capacity(rows, demand_per_day=200, churn_per_month=0)
    # need 200*1.2=240 -> shortfall 180 -> ceil(180/30)=6 inboxes + bench deficit 5
    assert out["order_inboxes"] == 11
    assert out["order_domains"] == 6  # ceil(11/2)
    assert out["status"] == "ORDER NOW"


def test_unhealthy_inboxes_do_not_count():
    rows = [
        _inbox(rep="70%"),            # low rep
        _inbox(test="fail"),          # failed placement
        _inbox(conn=False),           # disconnected
        _inbox(state="blocked"),      # warmup blocked
    ]
    out = compute_client_capacity(rows, demand_per_day=0, churn_per_month=0)
    assert out["sendable_inboxes"] == 0
    assert out["safe_capacity"] == 0


def test_churn_added_to_order():
    rows = [_inbox() for _ in range(2)]
    base = compute_client_capacity(rows, demand_per_day=200, churn_per_month=0)
    churned = compute_client_capacity(rows, demand_per_day=200, churn_per_month=4)
    assert churned["order_inboxes"] == base["order_inboxes"] + 4
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd smartlead_sync && python3 -m pytest test_capacity.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartlead.capacity'`

- [ ] **Step 4: Implement the pure math**

Create `smartlead_sync/smartlead/capacity.py`:

```python
"""Pure capacity math: demand vs healthy supply vs bench. No I/O.

Sources for the constants (docs/SCALE_ROADMAP.md §1): bench 20-25% of active
fleet, 20% demand headroom, 30/day per-inbox send cap, 2 inboxes/domain,
order lead time ≈ 4-6 weeks (purchase 1-2d + warmup 21-30d)."""
from __future__ import annotations

import math
from datetime import date, timedelta

from smartlead.config import (
    CAPACITY_PER_INBOX_CAP, CAPACITY_HEADROOM, CAPACITY_BENCH_RATIO,
    CAPACITY_BENCH_MIN, CAPACITY_LEAD_TIME_DAYS,
)

_ON_STATES = {"warming", "ramped", "on"}


def _rep(row: dict) -> float:
    try:
        return float(str(row.get("warmup_rep_pct", "")).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _healthy(row: dict) -> bool:
    return (bool(row.get("connection_ok"))
            and str(row.get("warmup_state", "")).lower() in _ON_STATES
            and _rep(row) >= 90.0
            and str(row.get("test_sheet_status", "")).lower() == "inbox")


def _active(row: dict) -> bool:
    return str(row.get("campaign_status", "")).upper() == "ACTIVE"


def compute_client_capacity(rows: list[dict], demand_per_day: float,
                            churn_per_month: int) -> dict:
    """rows = deduped inbox rows for ONE client."""
    healthy = [r for r in rows if _healthy(r)]
    active = [r for r in healthy if _active(r)]
    bench = [r for r in healthy if not _active(r)]

    safe_capacity = sum(
        min(int(r.get("message_per_day", 0) or 0) or CAPACITY_PER_INBOX_CAP,
            CAPACITY_PER_INBOX_CAP)
        for r in active)
    bench_target = max(CAPACITY_BENCH_MIN,
                       math.ceil(len(active) * CAPACITY_BENCH_RATIO))

    needed = demand_per_day * CAPACITY_HEADROOM
    shortfall = max(0.0, needed - safe_capacity)
    order_inboxes = (math.ceil(shortfall / CAPACITY_PER_INBOX_CAP)
                     + max(0, bench_target - len(bench))
                     + churn_per_month)
    order_domains = math.ceil(order_inboxes / 2)  # 2 inboxes/domain policy

    headroom_pct = (round(100.0 * safe_capacity / needed) if needed
                    else (100 if safe_capacity else 0))
    status = "OK" if order_inboxes == 0 else "ORDER NOW"
    order_by = ((date.today() + timedelta(days=CAPACITY_LEAD_TIME_DAYS))
                .isoformat() if order_inboxes else "")

    return {
        "sendable_inboxes": len(active),
        "safe_capacity": safe_capacity,
        "bench": len(bench),
        "bench_target": bench_target,
        "demand_per_day": round(demand_per_day, 1),
        "headroom_pct": headroom_pct,
        "order_inboxes": order_inboxes,
        "order_domains": order_domains,
        "order_by": order_by,
        "status": status,
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd smartlead_sync && python3 -m pytest test_capacity.py -v`
Expected: 4 passed. (If `test_healthy_fleet_no_order` fails on `bench_target`, re-check: `max(5, ceil(10*0.25)) = max(5,3) = 5` — the implementation, not the test, is wrong.)

- [ ] **Step 6: Add the Sheets writer method**

In `smartlead_sync/smartlead/sheets.py`, after `write_campaign_metrics` (~line 634), add — mirroring `write_inbox_health`'s projection pattern but on a SHARED tab (no account prefix), like `write_master_inboxes` does internally:

```python
    CAPACITY_COLUMNS = [
        "client", "status", "demand_per_day", "safe_capacity", "headroom_pct",
        "sendable_inboxes", "bench", "bench_target", "churn_per_month",
        "order_inboxes", "order_domains", "order_by",
    ]

    def write_capacity(self, rows: list[dict]) -> None:
        """Write the shared 'Capacity' advisory tab (one row per client)."""
        from smartlead.config import CAPACITY_TAB_NAME
        if not rows:
            print("  [Sheets] No capacity rows - skipping.")
            return
        ws = self._get_or_create_shared_tab(CAPACITY_TAB_NAME, rows=100, cols=len(self.CAPACITY_COLUMNS) + 2)
        ws.clear()
        header = [c.replace("_", " ").title() for c in self.CAPACITY_COLUMNS]
        values = [header] + [
            [str(r.get(c, "")) for c in self.CAPACITY_COLUMNS] for r in rows
        ]
        ws.update(values, "A1")
        print(f"  [Sheets] Capacity tab written: {len(rows)} client row(s)")
```

Note for the implementer: open `write_master_inboxes` (line ~574) first and match how IT clears/updates its shared worksheet — if it uses a different update call signature (e.g. `ws.update("A1", values)` argument order), copy that exact style; gspread argument order changed between versions and this repo pins one of them.

- [ ] **Step 7: Build the executor**

Create `smartlead_sync/capacity_planner.py`:

```python
#!/usr/bin/env python3
"""Capacity planner (read-only, Mondays). Demand vs healthy supply vs bench
per client -> ORDER advisories with 4-6-week lead time. Also maintains the
domain_registry (first-seen date per domain) for future age-gating.

No enable flag: this job only reads Smartlead and writes the advisory tab
+ Mongo registry — it never touches inbox or campaign settings."""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.capacity import compute_client_capacity
from smartlead.campaign_freshness import stale_campaign_names
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import HEALTH_HISTORY_DB, DOMAIN_REGISTRY_COLLECTION, DEFAULT_SHEET_ID
from smartlead.processing import fetch_account_data, get_domain_from_email
from smartlead.sheets import SheetsWriter, _dedupe_inbox_rows

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None


async def _demand_per_day(c: SmartleadClient, today: date) -> float:
    """7-day average of actual sends across ACTIVE, non-stale campaigns."""
    start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    stale = await stale_campaign_names(c, today)
    total = 0.0
    for camp in await c.list_campaigns():
        if str(camp.get("status", "")).upper() != "ACTIVE":
            continue
        if camp.get("name", "") in stale:
            continue
        try:
            an = await c.get_analytics_by_date(str(camp["id"]), start, end)
            total += float(an.get("sent_count", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Capacity] analytics failed for {camp.get('id')}: {exc}")
    return total / 7.0


def _register_domains(rows: list[dict], today: str) -> int:
    """Upsert first-seen dates. Returns how many domains are on record."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [Capacity] Mongo unavailable - domain registry skipped.")
        return 0
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][DOMAIN_REGISTRY_COLLECTION]
        col.create_index("domain", unique=True)
        ops = []
        for r in rows:
            dom = get_domain_from_email(str(r.get("email", "")))
            if not dom:
                continue
            ops.append(UpdateOne(
                {"domain": dom},
                {"$setOnInsert": {"domain": dom, "first_seen": today},
                 "$set": {"client": r.get("client", ""), "last_seen": today}},
                upsert=True))
        if ops:
            col.bulk_write(ops, ordered=False)
        return col.count_documents({})
    except Exception as exc:  # noqa: BLE001
        print(f"  [Capacity] domain registry failed: {exc}")
        return 0


def _churn_per_month(client_rows: list[dict]) -> int:
    """V1 placeholder: proper churn needs 30d of health history; until the
    history accumulates, use count of currently-broken inboxes (failed test
    or blocked) as the replacement-rate proxy. Revisit after 30 days."""
    broken = sum(1 for r in client_rows
                 if str(r.get("test_sheet_status", "")).lower() in {"fail", "spam"}
                 or str(r.get("warmup_state", "")).lower() == "blocked")
    return broken


async def main() -> None:
    today = date.today()
    out_rows: list[dict] = []
    all_rows: list[dict] = []
    for acc in discover_accounts():
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                inbox, _, _ = await fetch_account_data(c, {}, active_only=False)
                demand = await _demand_per_day(c, today)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Capacity] {acc.name} failed: {exc}")
            continue
        inbox = [r for r in inbox if not is_excluded_inbox(r)]
        for r in inbox:
            r.setdefault("client", acc.name)
        deduped = _dedupe_inbox_rows(inbox)
        all_rows.extend(deduped)
        churn = _churn_per_month(deduped)
        cap = compute_client_capacity(deduped, demand, churn)
        cap["client"] = acc.name
        cap["churn_per_month"] = churn
        out_rows.append(cap)
        print(f"  [Capacity] {acc.name}: {cap['status']} — demand {cap['demand_per_day']}/d, "
              f"capacity {cap['safe_capacity']}/d, bench {cap['bench']}/{cap['bench_target']}, "
              f"order {cap['order_domains']} domain(s) by {cap['order_by'] or '—'}")

    registered = _register_domains(all_rows, today.isoformat())
    print(f"[Capacity] domain registry: {registered} domain(s) on record")

    writer = SheetsWriter(DEFAULT_SHEET_ID)
    writer.write_capacity(out_rows)
    print(f"[Capacity] done: {len(out_rows)} client(s).")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 8: Compile + live smoke run**

Run: `cd smartlead_sync && python3 -m py_compile capacity_planner.py smartlead/capacity.py && python3 capacity_planner.py 2>&1 | tail -12`
Expected: one `[Capacity] <client>: ...` line per client with sane numbers (demand > 0 for active clients, capacity > 0), `Capacity tab written`, no traceback. Check the Google Sheet — a shared "Capacity" tab should exist with one row per client. If `SheetsWriter(DEFAULT_SHEET_ID)` raises on a missing second arg, pass `account_name=None` explicitly (check `__init__` at sheets.py:516).

- [ ] **Step 9: Wire the cron**

In `index.js`, after the blacklist cron block, add:

```javascript
    // Capacity planner at 9:30 AM IST every Monday (after the blacklist run).
    // Read-only: writes the Capacity advisory tab + domain registry only.
    cron.schedule('30 9 * * 1', () => {
      console.log(`[CRON] Capacity planner firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['capacity_planner.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[capacity] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[capacity] ${d}`));
      proc.on('close', code => console.log(`[capacity] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });
```

Run: `node --check index.js` — expected: exit 0.

- [ ] **Step 10: Commit**

```bash
git add smartlead_sync/smartlead/capacity.py smartlead_sync/capacity_planner.py smartlead_sync/smartlead/sheets.py smartlead_sync/smartlead/config.py smartlead_sync/test_capacity.py index.js
git commit -m "feat(capacity): Monday order-advisory planner + domain registry + Capacity tab"
```

---

### Task 4: Per-domain reply-rate early warning (the leading indicator)

A domain's reply rate drops ~48h before opens/bounces show a problem. Daily job: pull per-mailbox stats for every ACTIVE campaign, aggregate sent/replies per SENDING DOMAIN, store daily in Mongo, alert when (a) 7-day reply rate < 70% of the domain's own prior 7-day average, or (b) reply rate < 1% after 200+ sends (the "1% rule").

**Files:**
- Create: `smartlead_sync/smartlead/reply_stats.py` (pure aggregation + alert rules)
- Create: `smartlead_sync/reply_monitor.py` (executor)
- Modify: `smartlead_sync/smartlead/api.py` (add `get_campaign_mailbox_statistics` after `get_analytics_by_date`, ~line 240)
- Modify: `smartlead_sync/smartlead/config.py` (constants)
- Modify: `index.js` (cron daily 13:00 IST)
- Test: `smartlead_sync/test_reply_stats.py` (create)

**Interfaces:**
- Consumes: Smartlead mailbox-statistics endpoint (verified in Step 1); Mongo collection `domain_reply_stats` `{domain, client, date, sent, replies}` unique on `(domain, date)`.
- Produces: `aggregate_domain_stats(mailbox_rows: list[dict]) -> dict[str, dict]` (domain → `{sent, replies}`) and `evaluate_alerts(domain, current: dict, history: list[dict]) -> list[str]` in `smartlead/reply_stats.py`.

- [ ] **Step 1: Verify the endpoint (do NOT skip — the path below is unconfirmed until this passes)**

Check https://api.smartlead.ai/reference for the campaign mailbox/sender statistics endpoint (search "mailbox statistics" / "statistics"). Then verify live from `smartlead_sync/`:

```bash
python3 - <<'EOF'
import asyncio
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient

async def main():
    acc = discover_accounts()[0]
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camps = [x for x in await c.list_campaigns()
                 if str(x.get("status","")).upper() == "ACTIVE"]
        cid = str(camps[0]["id"])
        # candidate endpoint — adjust to whatever the reference documents:
        data = await c._get(f"/campaigns/{cid}/mailbox-statistics",
                            extra_params={"offset": 0, "limit": 10})
    print(type(data), str(data)[:600])

asyncio.run(main())
EOF
```

Expected: JSON containing per-mailbox rows with a sender email field and sent/reply counts (names may be `sent_count`/`reply_count` or similar). **Record the exact path + field names and use them in Steps 3-5.** STOP CONDITION: if no such endpoint exists (404 on every documented candidate), stop and report — fallback design (campaign-level attribution) needs a human decision, don't improvise it.

- [ ] **Step 2: Write the failing tests for the pure logic**

Create `smartlead_sync/test_reply_stats.py`:

```python
from smartlead.reply_stats import aggregate_domain_stats, evaluate_alerts


def test_aggregate_sums_per_domain():
    rows = [
        {"email": "a@dom1.com", "sent_count": 100, "reply_count": 5},
        {"email": "b@dom1.com", "sent_count": 50, "reply_count": 1},
        {"email": "c@dom2.com", "sent_count": 30, "reply_count": 2},
    ]
    out = aggregate_domain_stats(rows)
    assert out["dom1.com"] == {"sent": 150, "replies": 6}
    assert out["dom2.com"] == {"sent": 30, "replies": 2}


def test_alert_on_drop_vs_own_average():
    # prior week: 5% reply rate; current week: 1.2% -> >30% drop -> alert
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 250, "replies": 3}
    alerts = evaluate_alerts("dom1.com", current, history)
    assert any("drop" in a for a in alerts)


def test_alert_on_one_percent_rule():
    current = {"sent": 250, "replies": 1}   # 0.4% after 200+ sends
    alerts = evaluate_alerts("dom1.com", current, [])
    assert any("1% rule" in a for a in alerts)


def test_no_alert_when_healthy():
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 100, "replies": 5}
    assert evaluate_alerts("dom1.com", current, history) == []


def test_no_drop_alert_on_thin_data():
    # under the send floor, drop-vs-average must not fire (noise)
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 20, "replies": 0}
    alerts = evaluate_alerts("dom1.com", current, history)
    assert not any("drop" in a for a in alerts)
```

- [ ] **Step 3: Run tests to verify they fail, then implement the pure module**

Run: `cd smartlead_sync && python3 -m pytest test_reply_stats.py -v` → expected `ModuleNotFoundError`.

Create `smartlead_sync/smartlead/reply_stats.py`:

```python
"""Pure per-domain reply-rate aggregation + early-warning rules. No I/O.

Why domain-level: placement shifts hit a DOMAIN's replies ~48h before
opens/bounces move, and campaign-level rates can't isolate which domain is
degrading (docs/SCALE_ROADMAP.md §3)."""
from __future__ import annotations

from smartlead.config import (
    REPLY_ALERT_DROP_RATIO, REPLY_ALERT_MIN_SENT, REPLY_ONE_PERCENT_MIN_SENT,
)


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in str(email) else ""


def aggregate_domain_stats(mailbox_rows: list[dict]) -> dict[str, dict]:
    """mailbox_rows: per-sender stats rows with email + sent/reply counts.
    Field names follow the live endpoint (verified in Task 4 Step 1)."""
    out: dict[str, dict] = {}
    for r in mailbox_rows:
        dom = _domain(str(r.get("email", "") or r.get("from_email", "")))
        if not dom:
            continue
        d = out.setdefault(dom, {"sent": 0, "replies": 0})
        d["sent"] += int(r.get("sent_count", 0) or 0)
        d["replies"] += int(r.get("reply_count", 0) or 0)
    return out


def _rate(sent: int, replies: int) -> float:
    return (replies / sent) if sent else 0.0


def evaluate_alerts(domain: str, current: dict, history: list[dict]) -> list[str]:
    """current = this week's {sent, replies}; history = prior daily records
    (each {sent, replies}), most recent first or any order — summed as the
    baseline window."""
    alerts: list[str] = []
    cur_rate = _rate(current.get("sent", 0), current.get("replies", 0))

    base_sent = sum(int(h.get("sent", 0) or 0) for h in history)
    base_replies = sum(int(h.get("replies", 0) or 0) for h in history)
    base_rate = _rate(base_sent, base_replies)

    if (base_rate > 0 and current.get("sent", 0) >= REPLY_ALERT_MIN_SENT
            and cur_rate < base_rate * REPLY_ALERT_DROP_RATIO):
        alerts.append(
            f"reply-rate drop: {cur_rate:.1%} vs own baseline {base_rate:.1%} "
            f"(>{100 - int(REPLY_ALERT_DROP_RATIO * 100)}% down) — placement "
            "likely shifting; act within 48h")

    if (current.get("sent", 0) >= REPLY_ONE_PERCENT_MIN_SENT and cur_rate < 0.01):
        alerts.append(
            f"1% rule: {cur_rate:.1%} reply rate after "
            f"{current.get('sent', 0)} sends — inbox/domain underperforming")
    return alerts
```

And in `smartlead_sync/smartlead/config.py`, after the capacity block:

```python
# ── Per-domain reply-rate early warning ──────────────────────────────────────
REPLY_STATS_COLLECTION: str = os.getenv("REPLY_STATS_COLLECTION", "domain_reply_stats")
REPLY_ALERT_DROP_RATIO: float = float(os.getenv("REPLY_ALERT_DROP_RATIO", "0.7"))  # alert below 70% of own baseline
REPLY_ALERT_MIN_SENT: int = int(os.getenv("REPLY_ALERT_MIN_SENT", "50"))           # drop-alert send floor (this window)
REPLY_ONE_PERCENT_MIN_SENT: int = int(os.getenv("REPLY_ONE_PERCENT_MIN_SENT", "200"))
```

Run: `python3 -m pytest test_reply_stats.py -v` → expected: 5 passed.

- [ ] **Step 4: Add the API wrapper (using the endpoint verified in Step 1)**

In `smartlead_sync/smartlead/api.py`, after `get_analytics_by_date` (~line 240):

```python
    async def get_campaign_mailbox_statistics(self, campaign_id: str) -> list[dict]:
        """Per-sender-mailbox stats for a campaign (paginated).
        Endpoint + field names verified live on 2026-XX-XX (Task 4 Step 1)."""
        all_rows: list[dict] = []
        offset = 0
        page = 100
        while True:
            resp = await self._get(f"/campaigns/{campaign_id}/mailbox-statistics",
                                   extra_params={"offset": offset, "limit": page})
            data = resp.get("data", []) if isinstance(resp, dict) else (resp or [])
            all_rows.extend(data)
            if len(data) < page:
                break
            offset += page
        return all_rows
```

(Adjust path/response-shape to what Step 1 actually returned; update the docstring date.)

- [ ] **Step 5: Build the executor**

Create `smartlead_sync/reply_monitor.py`:

```python
#!/usr/bin/env python3
"""Per-domain reply-rate early warning (daily, read-only).

Pulls per-mailbox stats for every ACTIVE campaign, aggregates sent/replies
per SENDING DOMAIN, stores a daily record in Mongo, and alerts when a
domain's reply rate falls >30% below its own trailing baseline (leading
indicator — fires ~48h before opens/bounces move) or breaks the 1% rule."""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import HEALTH_HISTORY_DB, REPLY_STATS_COLLECTION
from smartlead.reply_stats import aggregate_domain_stats, evaluate_alerts

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None


def _col():
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][REPLY_STATS_COLLECTION]
        col.create_index([("domain", 1), ("date", 1)], unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [ReplyMon] Mongo unavailable: {exc}")
        return None


async def _fleet_domain_stats() -> dict[str, dict]:
    """{domain: {sent, replies, client}} across all ACTIVE campaigns."""
    fleet: dict[str, dict] = {}
    for acc in discover_accounts():
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                camps = [x for x in await c.list_campaigns()
                         if str(x.get("status", "")).upper() == "ACTIVE"]
                for camp in camps:
                    rows = await c.get_campaign_mailbox_statistics(str(camp["id"]))
                    for dom, s in aggregate_domain_stats(rows).items():
                        d = fleet.setdefault(dom, {"sent": 0, "replies": 0,
                                                   "client": acc.name})
                        d["sent"] += s["sent"]
                        d["replies"] += s["replies"]
        except Exception as exc:  # noqa: BLE001
            print(f"  [ReplyMon] {acc.name} failed: {exc}")
    return fleet


async def main() -> None:
    today = date.today().isoformat()
    fleet = await _fleet_domain_stats()
    print(f"[ReplyMon] collected stats for {len(fleet)} domain(s)")

    col = _col()
    alerts_total = 0
    if col is not None:
        ops = [UpdateOne({"domain": d, "date": today},
                         {"$set": {"domain": d, "date": today, **s}}, upsert=True)
               for d, s in fleet.items()]
        if ops:
            col.bulk_write(ops, ordered=False)

    baseline_start = (date.today() - timedelta(days=14)).isoformat()  # 14d baseline window
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    for dom, cur in sorted(fleet.items()):
        history = []
        if col is not None:
            history = list(col.find({"domain": dom,
                                     "date": {"$gte": baseline_start, "$lte": yesterday}}))
        for alert in evaluate_alerts(dom, cur, history):
            alerts_total += 1
            print(f"  🚨 {dom} ({cur.get('client','')}): {alert}")

    if alerts_total == 0:
        print("[ReplyMon] ✅ no reply-rate alerts.")
    else:
        print(f"[ReplyMon] {alerts_total} alert(s) — check the domains above "
              "(pause/investigate per OPERATOR_PLAYBOOK daily P0 flow).")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Compile + live smoke run**

Run: `cd smartlead_sync && python3 -m py_compile reply_monitor.py smartlead/reply_stats.py smartlead/api.py && python3 reply_monitor.py 2>&1 | tail -10`
Expected: `collected stats for N domain(s)` with N > 0, then either alerts or `✅ no reply-rate alerts`. First run has no history → only 1%-rule alerts can fire; drop-alerts start working after ~a week of daily records. No traceback.

- [ ] **Step 7: Wire the cron**

In `index.js` after the capacity cron:

```javascript
    // Per-domain reply-rate early warning at 1:00 PM IST daily (read-only).
    cron.schedule('0 13 * * *', () => {
      console.log(`[CRON] Reply monitor firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['reply_monitor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[reply-mon] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[reply-mon] ${d}`));
      proc.on('close', code => console.log(`[reply-mon] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });
```

Run: `node --check index.js` — expected exit 0.

- [ ] **Step 8: Commit**

```bash
git add smartlead_sync/smartlead/reply_stats.py smartlead_sync/reply_monitor.py smartlead_sync/smartlead/api.py smartlead_sync/smartlead/config.py smartlead_sync/test_reply_stats.py index.js
git commit -m "feat(reply-monitor): per-domain reply-rate early warning (daily, read-only)"
```

---

## Out of scope (later rounds — do not build here)

Rest-based domain rotation, SpamAssassin report parsing into health score, list-hygiene gate, Zapmail provisioning automation, Slack alert routing for the new jobs (blocked on channel config). Tracked in docs/SCALE_ROADMAP.md.
