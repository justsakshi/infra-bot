# Campaign Metrics Dashboard (Darlean) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a daily-synced "Campaign Metrics" Google-Sheets tab for the DARLEAN client, one row per campaign across Smartlead (email) + HeyReach (LinkedIn), matching the reference dashboard image.

**Architecture:** New async `HeyReachClient` (mirrors `SmartleadClient`) + workspace discovery from env. A pure `campaign_metrics` module assembles rows from Smartlead `campaign_summary` (already fetched) + per-campaign Smartlead leads/analytics + HeyReach campaigns/stats/leads, with UTC date bucketing. `run.py` builds + writes the tab once after the existing sync, isolated in try/except.

**Tech Stack:** Python 3.10+, httpx (async), polars, gspread. Tests are plain runnable scripts (`python3 <file>.py`), like `test_sync_wiring.py` — NOT pytest. Run everything with `python3` (the `python` on this box lacks deps). Project IS a git repo (`infra-bot`), branch `main`.

## Global Constraints

- Run scripts with `python3`, never `python`.
- All new API calls reuse the existing rate-limit pattern (chunked via `_gather_chunked`, retry on 429/5xx/transport, honor Retry-After).
- HeyReach base URL `https://api.heyreach.io/api/public`, auth header `X-API-KEY`.
- Dates are UTC. "This month" = first of current month 00:00 UTC → now. "Yesterday" = the UTC calendar day before today. `today` is passed in (never call `datetime.now()` deep in helpers — pass it as a param for testability).
- Scope = DARLEAN only (`CAMPAIGN_METRICS_CLIENTS = {"DARLEAN"}` for Smartlead; HeyReach = all `HEYREACH_API_KEY_*` workspaces, currently just DARLEAN).
- Numeric "-" placeholder for columns N/A to a platform (e.g. connections for Smartlead).

**Verified API shapes (live, 2026-06-30):**
- HeyReach `POST /campaign/GetAll {offset,limit}` → `{totalCount, items:[{id,name,status,progressStats:{totalUsers,totalUsersInProgress,...},creationTime}]}`.
- HeyReach `POST /stats/GetOverallStats {campaignIds:[id],accountIds:[],startDate,endDate}` → `{overallStats:{connectionsSent,connectionsAccepted,messagesSent,totalMessageReplies,autoTaggedInterested,...}, byDayStats:{"2026-06-01T00:00:00Z":{...sameFields}}}`. Dates null = all-time.
- HeyReach `POST /campaign/GetLeadsFromCampaign {campaignId,offset,limit}` → `{totalCount, items:[{creationTime, leadCampaignStatus, ...}]}`. NOTE: returns only the in-progress subset (e.g. 35 of 99) — leads-added is best-effort.
- Smartlead `GET /campaigns/{id}/leads?offset&limit` → `{total_leads, data:[{created_at, lead_category_id, status}], offset, limit}`.
- Smartlead `GET /campaigns/{id}/analytics-by-date?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` → range-aggregate `{reply_count, sent_count, ...}` (strings).

---

### Task 1: Config + constants

**Files:**
- Modify: `smartlead/config.py`

**Interfaces:**
- Produces: `HEYREACH_BASE_URL`, `CAMPAIGN_METRICS_TAB_NAME`, `CAMPAIGN_METRICS_SHEET_ID`, `CAMPAIGN_METRICS_CLIENTS`, `SMARTLEAD_POSITIVE_CATEGORY_IDS` (set[int]).

- [ ] **Step 1: Add constants** — append after the `MASTER_TAB_NAME` block in `smartlead/config.py`:

```python
# ── HeyReach ─────────────────────────────────────────────────────────────────
HEYREACH_BASE_URL: str = "https://api.heyreach.io/api/public"

# ── Campaign Metrics dashboard ───────────────────────────────────────────────
CAMPAIGN_METRICS_TAB_NAME: str = os.getenv("CAMPAIGN_METRICS_TAB_NAME", "Campaign Metrics")
CAMPAIGN_METRICS_SHEET_ID: str = os.getenv("CAMPAIGN_METRICS_SHEET_ID", DEFAULT_SHEET_ID)
# Smartlead accounts (by discovered name) to include in the metrics tab
CAMPAIGN_METRICS_CLIENTS: set[str] = {"DARLEAN"}
# Smartlead lead-category ids treated as positive/neutral (tune after discovery;
# Smartlead defaults: 1=Interested, 2=Meeting Request/Booked, 9=Information Request).
SMARTLEAD_POSITIVE_CATEGORY_IDS: set[int] = {1, 2, 9}
```

- [ ] **Step 2: Verify** — `python3 -c "from smartlead.config import HEYREACH_BASE_URL, CAMPAIGN_METRICS_TAB_NAME, CAMPAIGN_METRICS_CLIENTS, SMARTLEAD_POSITIVE_CATEGORY_IDS; print('ok')"` → `ok`

- [ ] **Step 3: (discovery) confirm positive category ids** — run, then tune the set if needed:
```bash
python3 -c "
import asyncio
from pathlib import Path; from dotenv import load_dotenv; load_dotenv(Path('..')/'.env')
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
async def m():
    acc=next(a for a in discover_accounts() if a.name=='DARLEAN')
    async with SmartleadClient(acc.api_key,acc.name) as c:
        for p in ('/leads/fetch-categories','/lead/categories','/lead-category'):
            try: print(p, await c._get(p)); break
            except Exception as e: print(p,'no')
asyncio.run(m())"
```
If a categories list is found, set `SMARTLEAD_POSITIVE_CATEGORY_IDS` to the ids whose names are Interested / Meeting Request / Information Request. If none found, keep the default `{1,2,9}` (Smartlead's standard ids) and note it.

- [ ] **Step 4: Commit** — `git add smartlead/config.py && git commit -m "feat(metrics): config for campaign metrics dashboard"`

---

### Task 2: HeyReach API client

**Files:**
- Create: `smartlead/heyreach.py`
- Test: `test_heyreach_client.py`

**Interfaces:**
- Produces: `HeyReachClient(api_key, workspace_name)` async context manager with:
  - `async list_campaigns() -> list[dict]` (all, paginated)
  - `async get_overall_stats(campaign_id:int, start:str|None=None, end:str|None=None) -> dict` → `{overallStats, byDayStats}`
  - `async get_campaign_leads(campaign_id:int) -> list[dict]` (the available/in-progress subset)
  - property `masked_key`

- [ ] **Step 1: Write the failing test** — `test_heyreach_client.py`:

```python
"""Offline test for HeyReachClient paging + stat shape (monkeypatched HTTP)."""
from __future__ import annotations
import asyncio
from smartlead.heyreach import HeyReachClient


class FakeResp:
    def __init__(self, payload, status=200):
        self._p, self.status_code, self.headers = payload, status, {}
    def json(self): return self._p
    def raise_for_status(self): pass


class FakeHTTP:
    """Stands in for httpx.AsyncClient; routes by URL suffix."""
    def __init__(self):
        self.calls = []
    async def post(self, url, headers=None, json=None):
        self.calls.append((url, json))
        if url.endswith("/campaign/GetAll"):
            off = json["offset"]
            items = [] if off >= 2 else [{"id": 1, "name": "C1", "status": "IN_PROGRESS",
                     "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}}] * (2 if off == 0 else 0)
            # return exactly 2 on first page (== limit triggers another page), then 0
            return FakeResp({"totalCount": 2, "items": [{"id": 1, "name": "C1", "status": "IN_PROGRESS",
                     "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}},
                     {"id": 2, "name": "C2", "status": "PAUSED",
                     "progressStats": {"totalUsers": 10, "totalUsersInProgress": 3}}] if off == 0 else []})
        if url.endswith("/stats/GetOverallStats"):
            return FakeResp({"overallStats": {"connectionsSent": 25, "connectionsAccepted": 4,
                     "messagesSent": 12, "totalMessageReplies": 3, "autoTaggedInterested": 1},
                     "byDayStats": {"2026-06-29T00:00:00Z": {"totalMessageReplies": 1, "autoTaggedInterested": 1}}})
        if url.endswith("/campaign/GetLeadsFromCampaign"):
            off = json["offset"]
            return FakeResp({"totalCount": 1, "items": [{"creationTime": "2026-06-29T10:00:00Z"}] if off == 0 else []})
        return FakeResp({})
    async def aclose(self): pass


def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


async def main():
    c = HeyReachClient("k", "DARLEAN")
    c._client = FakeHTTP()  # inject fake (bypass __aenter__)
    camps = await c.list_campaigns()
    ok(len(camps) == 2, f"list_campaigns paged to 2 (got {len(camps)})")
    ok(camps[0]["progressStats"]["totalUsers"] == 99, "campaign progressStats present")
    s = await c.get_overall_stats(1, start="2026-06-01T00:00:00Z", end="2026-06-30T23:59:59Z")
    ok(s["overallStats"]["connectionsSent"] == 25, "overall stats parsed")
    ok("2026-06-29T00:00:00Z" in s["byDayStats"], "byDayStats present")
    leads = await c.get_campaign_leads(1)
    ok(len(leads) == 1 and leads[0]["creationTime"].startswith("2026-06-29"), "leads paged")
    print("\nALL PASSED")

if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Run, verify fail** — `python3 test_heyreach_client.py` → ModuleNotFoundError / AttributeError (no `heyreach` module).

- [ ] **Step 3: Implement** — `smartlead/heyreach.py`:

```python
"""Async HTTP client for the HeyReach public API."""
from __future__ import annotations

import asyncio
from typing import Any

import httpx

from smartlead.config import (
    HEYREACH_BASE_URL, API_CHUNK_SIZE, API_TIMEOUT, API_CHUNK_DELAY,
    API_MAX_RETRIES, API_RETRY_BASE_DELAY, API_RETRY_MAX_DELAY,
)

_PAGE = 100


class HeyReachClient:
    """Thin async wrapper around the HeyReach public API (X-API-KEY auth)."""

    def __init__(self, api_key: str, workspace_name: str = "Default") -> None:
        self._api_key = api_key
        self.workspace_name = workspace_name
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "HeyReachClient":
        self._client = httpx.AsyncClient(
            timeout=API_TIMEOUT,
            headers={"X-API-KEY": self._api_key, "Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _post(self, path: str, body: dict) -> Any:
        assert self._client, "Use `async with HeyReachClient(...)`."
        url = f"{HEYREACH_BASE_URL}{path}"
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(url, json=body)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [HR {self.workspace_name}] network error {path}: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    ra = resp.headers.get("Retry-After")
                    try:
                        delay = min(float(ra), API_RETRY_MAX_DELAY) if ra else min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    except ValueError:
                        delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [HR {self.workspace_name}] {resp.status_code} {path} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    async def _paginate(self, path: str, base_body: dict) -> list[dict]:
        out: list[dict] = []
        offset = 0
        while True:
            body = {**base_body, "offset": offset, "limit": _PAGE}
            data = await self._post(path, body)
            items = data.get("items", []) if isinstance(data, dict) else []
            out.extend(items)
            if len(items) < _PAGE:
                break
            offset += _PAGE
        return out

    async def list_campaigns(self) -> list[dict]:
        return await self._paginate("/campaign/GetAll", {})

    async def get_overall_stats(self, campaign_id: int, start: str | None = None, end: str | None = None) -> dict:
        return await self._post("/stats/GetOverallStats", {
            "campaignIds": [campaign_id], "accountIds": [], "startDate": start, "endDate": end,
        })

    async def get_campaign_leads(self, campaign_id: int) -> list[dict]:
        return await self._paginate("/campaign/GetLeadsFromCampaign", {"campaignId": campaign_id})

    @property
    def masked_key(self) -> str:
        k = self._api_key
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"
```

- [ ] **Step 4: Run, verify pass** — `python3 test_heyreach_client.py` → `ALL PASSED`.

- [ ] **Step 5: Live smoke (read-only)** — confirms real shapes:
```bash
python3 -c "
import asyncio,os; from pathlib import Path; from dotenv import load_dotenv; load_dotenv(Path('..')/'.env')
from smartlead.heyreach import HeyReachClient
async def m():
    async with HeyReachClient(os.getenv('HEYREACH_API_KEY_DARLEAN'),'DARLEAN') as c:
        cs=await c.list_campaigns(); print('campaigns:',len(cs))
        s=await c.get_overall_stats(cs[0]['id'])
        print('overall keys ok:', 'overallStats' in s)
asyncio.run(m())"
```
Expected: prints campaign count (~16) and `overall keys ok: True`.

- [ ] **Step 6: Commit** — `git add smartlead/heyreach.py test_heyreach_client.py && git commit -m "feat(metrics): HeyReach API client"`

---

### Task 3: HeyReach workspace discovery

**Files:**
- Create: `smartlead/heyreach_accounts.py`
- Test: `test_heyreach_accounts.py`

**Interfaces:**
- Produces: `discover_heyreach_workspaces() -> list[HeyReachWorkspace]` where `HeyReachWorkspace` has `.name: str`, `.api_key: str`.

- [ ] **Step 1: Failing test** — `test_heyreach_accounts.py`:

```python
import os
from smartlead.heyreach_accounts import discover_heyreach_workspaces

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

os.environ["HEYREACH_API_KEY_DARLEAN"] = "key_d"
os.environ["HEYREACH_API_KEY_MELIOR"] = "key_m"
ws = {w.name: w.api_key for w in discover_heyreach_workspaces()}
ok(ws.get("DARLEAN") == "key_d", f"DARLEAN discovered ({ws})")
ok(ws.get("MELIOR") == "key_m", "MELIOR discovered")
print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_heyreach_accounts.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/heyreach_accounts.py`:

```python
"""Discover HeyReach workspaces from HEYREACH_API_KEY_<NAME> env vars."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

_PREFIX = "HEYREACH_API_KEY_"


@dataclass
class HeyReachWorkspace:
    name: str
    api_key: str


def discover_heyreach_workspaces() -> list[HeyReachWorkspace]:
    out: list[HeyReachWorkspace] = []
    for key, value in sorted(os.environ.items()):
        if key.startswith(_PREFIX) and value:
            out.append(HeyReachWorkspace(name=key[len(_PREFIX):], api_key=value.strip()))
    return out
```

- [ ] **Step 4: Run, verify pass** — `python3 test_heyreach_accounts.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/heyreach_accounts.py test_heyreach_accounts.py && git commit -m "feat(metrics): HeyReach workspace discovery"`

---

### Task 4: Smartlead leads + dated-analytics helpers

**Files:**
- Modify: `smartlead/api.py` (add two methods to `SmartleadClient`)
- Test: extend `test_heyreach_client.py`? No — add `test_smartlead_leads.py` (live read-only, DARLEAN).

**Interfaces:**
- Produces on `SmartleadClient`:
  - `async get_campaign_leads(campaign_id:str) -> list[dict]` → all leads, each `{created_at, lead_category_id, status, ...}` (paginated by `/campaigns/{id}/leads`).
  - `async get_analytics_by_date(campaign_id:str, start_date:str, end_date:str) -> dict` → range aggregate (`reply_count`, etc. as ints).

- [ ] **Step 1: Implement** (no offline unit test — these are thin REST wrappers; verified live in Step 2). Add to `SmartleadClient` in `smartlead/api.py`, after `get_warmup_stats`:

```python
    async def get_campaign_leads(self, campaign_id: str) -> list[dict]:
        """Fetch all leads for a campaign (paginated). Each: created_at, lead_category_id, status."""
        all_leads: list[dict] = []
        offset = 0
        page = 100
        while True:
            resp = await self._get(f"/campaigns/{campaign_id}/leads",
                                   extra_params={"offset": offset, "limit": page})
            data = resp.get("data", []) if isinstance(resp, dict) else []
            all_leads.extend(data)
            if len(data) < page:
                break
            offset += page
        return all_leads

    async def get_analytics_by_date(self, campaign_id: str, start_date: str, end_date: str) -> dict:
        """Range-aggregate analytics for [start_date, end_date] (YYYY-MM-DD). Values may be strings."""
        return await self._get(f"/campaigns/{campaign_id}/analytics-by-date",
                               extra_params={"start_date": start_date, "end_date": end_date})
```

- [ ] **Step 2: Live verify** — `test_smartlead_leads.py`:
```python
import asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient

def ok(c,m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c,m

async def main():
    acc=next(a for a in discover_accounts() if a.name=="DARLEAN")
    async with SmartleadClient(acc.api_key,acc.name) as c:
        cid=str((await c.list_campaigns())[0]["id"])
        leads=await c.get_campaign_leads(cid)
        ok(isinstance(leads,list), "leads is a list")
        if leads: ok("created_at" in leads[0], "lead has created_at")
        a=await c.get_analytics_by_date(cid,"2026-06-01","2026-06-30")
        ok("reply_count" in a, "analytics-by-date has reply_count")
    print("\nALL PASSED")
asyncio.run(main())
```
Run: `python3 test_smartlead_leads.py` → `ALL PASSED`.

- [ ] **Step 3: Commit** — `git add smartlead/api.py test_smartlead_leads.py && git commit -m "feat(metrics): Smartlead campaign-leads + dated-analytics helpers"`

---

### Task 5: Campaign-metrics assembly (pure logic)

**Files:**
- Create: `smartlead/campaign_metrics.py`
- Test: `test_campaign_metrics.py`

**Interfaces:**
- Consumes: Smartlead `campaign_summary` rows (have `name,status,total_leads,in_progress,sent,replied`), Smartlead leads (`created_at,lead_category_id`), Smartlead dated analytics (`reply_count`), HeyReach campaign dict + overall (all-time) + overall (month) + leads.
- Produces:
  - `month_start(today) -> datetime`, `yesterday_range(today) -> tuple[datetime,datetime]` (UTC, helpers)
  - `COLUMNS: list[str]` (the metric-row key order)
  - `smartlead_metric_row(summary, leads, month_replies, yest_replies, today, positive_ids) -> dict`
  - `heyreach_metric_row(campaign, overall_alltime, overall_month, leads, today) -> dict`
  - `total_row(rows) -> dict`

- [ ] **Step 1: Failing test** — `test_campaign_metrics.py`:

```python
from datetime import datetime, timezone
from smartlead.campaign_metrics import (
    smartlead_metric_row, heyreach_metric_row, total_row, COLUMNS,
)

def ok(c,m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c,m

TODAY = datetime(2026, 6, 15, tzinfo=timezone.utc)  # yesterday = 2026-06-14; month = June

# --- Smartlead ---
summary = {"name": "SL Camp", "status": "ACTIVE", "total_leads": 200,
           "in_progress": 40, "sent": 100, "replied": 7}
leads = [
    {"created_at": "2026-06-10T00:00:00Z", "lead_category_id": 1},   # this month, positive
    {"created_at": "2026-06-14T00:00:00Z", "lead_category_id": 3},   # yesterday, not positive
    {"created_at": "2026-05-20T00:00:00Z", "lead_category_id": 1},   # last month
]
slr = smartlead_metric_row(summary, leads, month_replies=5, yest_replies=1,
                           today=TODAY, positive_ids={1, 2, 9})
ok(slr["platform"] == "Smartlead", "platform")
ok(slr["total_leads"] == 200, "total_leads")
ok(slr["leads_added_month"] == 2, f"leads added this month==2 (got {slr['leads_added_month']})")
ok(slr["leads_added_yesterday"] == 1, f"leads added yesterday==1 (got {slr['leads_added_yesterday']})")
ok(slr["connections_sent"] == "-", "connections '-' for smartlead")
ok(slr["total_responses_month"] == 5, "total responses month")
ok(slr["positive_neutral_month"] == 2, f"positive/neutral (cat 1 x2)==2 (got {slr['positive_neutral_month']})")

# --- HeyReach ---
camp = {"name": "HR Camp", "status": "IN_PROGRESS",
        "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}}
overall_all = {"overallStats": {"connectionsSent": 25, "connectionsAccepted": 4,
               "messagesSent": 12, "totalMessageReplies": 9, "autoTaggedInterested": 3}}
overall_month = {"overallStats": {"totalMessageReplies": 4, "autoTaggedInterested": 2},
                 "byDayStats": {"2026-06-14T00:00:00Z": {"autoTaggedInterested": 1, "totalMessageReplies": 1}}}
hr_leads = [{"creationTime": "2026-06-14T09:00:00Z"}, {"creationTime": "2026-06-02T09:00:00Z"},
            {"creationTime": "2026-05-30T09:00:00Z"}]
hrr = heyreach_metric_row(camp, overall_all, overall_month, hr_leads, today=TODAY)
ok(hrr["platform"] == "Heyreach", "platform hr")
ok(hrr["total_leads"] == 99, "hr total_leads")
ok(hrr["leads_in_progress"] == 35, "hr in progress")
ok(hrr["connections_sent"] == 25, "hr connections sent")
ok(hrr["msg_sent"] == 12, "hr msg sent")
ok(hrr["leads_added_yesterday"] == 1, f"hr leads yest==1 (got {hrr['leads_added_yesterday']})")
ok(hrr["leads_added_month"] == 2, f"hr leads month==2 (got {hrr['leads_added_month']})")
ok(hrr["total_responses_month"] == 4, "hr responses month")
ok(hrr["positive_responses_yesterday"] == 1, "hr positive yesterday")
ok(hrr["positive_neutral_month"] == 2, "hr positive month")

# --- Total footer ---
tr = total_row([slr, hrr])
ok(tr["campaign"] == "Total", "total label")
ok(tr["total_leads"] == 299, f"total leads sum==299 (got {tr['total_leads']})")
ok(tr["msg_sent"] == 112, f"msg sent sum (100+12)==112 (got {tr['msg_sent']})")
ok(set(COLUMNS) >= {"campaign","platform","status","total_leads"}, "COLUMNS defined")
print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_campaign_metrics.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/campaign_metrics.py`:

```python
"""Assemble per-campaign metric rows (Smartlead + HeyReach) with UTC date buckets."""
from __future__ import annotations

from datetime import datetime, timezone

COLUMNS = [
    "campaign", "platform", "status", "total_leads", "leads_added_month",
    "leads_added_yesterday", "leads_in_progress", "connections_sent",
    "connections_accepted", "msg_sent", "positive_responses_yesterday",
    "total_responses_month", "positive_neutral_month",
]

# numeric columns summed in the Total row
_NUMERIC = [
    "total_leads", "leads_added_month", "leads_added_yesterday", "leads_in_progress",
    "connections_sent", "connections_accepted", "msg_sent",
    "positive_responses_yesterday", "total_responses_month", "positive_neutral_month",
]


def _parse(dt: str) -> datetime | None:
    if not dt:
        return None
    try:
        d = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def month_start(today: datetime) -> datetime:
    return today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _in_month(dt: datetime | None, today: datetime) -> bool:
    return dt is not None and dt >= month_start(today) and dt <= today


def _is_yesterday(dt: datetime | None, today: datetime) -> bool:
    if dt is None:
        return False
    y = today.date().toordinal() - 1
    return dt.date().toordinal() == y


def _int(v) -> int:
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def smartlead_metric_row(summary: dict, leads: list[dict], month_replies: int,
                         yest_replies: int, today: datetime, positive_ids: set[int]) -> dict:
    added_month = added_yest = pos_neutral = 0
    for lead in leads:
        d = _parse(lead.get("created_at", ""))
        if _in_month(d, today):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1
        if lead.get("lead_category_id") in positive_ids:
            pos_neutral += 1
    return {
        "campaign": summary.get("name", ""),
        "platform": "Smartlead",
        "status": summary.get("status", ""),
        "total_leads": _int(summary.get("total_leads", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(summary.get("in_progress", 0)),
        "connections_sent": "-",
        "connections_accepted": "-",
        "msg_sent": _int(summary.get("sent", 0)),
        # Smartlead positive-by-date is API-limited; yesterday-positive not reliably
        # available -> "-" (HeyReach has it). positive/neutral = current category snapshot.
        "positive_responses_yesterday": "-",
        "total_responses_month": _int(month_replies),
        "positive_neutral_month": pos_neutral,
    }


def heyreach_metric_row(campaign: dict, overall_alltime: dict, overall_month: dict,
                        leads: list[dict], today: datetime) -> dict:
    ps = campaign.get("progressStats", {}) or {}
    oa = (overall_alltime or {}).get("overallStats", {}) or {}
    om = (overall_month or {}).get("overallStats", {}) or {}
    by_day = (overall_month or {}).get("byDayStats", {}) or {}

    added_month = added_yest = 0
    for lead in leads:
        d = _parse(lead.get("creationTime", ""))
        if _in_month(d, today):
            added_month += 1
        if _is_yesterday(d, today):
            added_yest += 1

    # yesterday's positive from byDayStats (key = UTC midnight of yesterday)
    y = (today.replace(hour=0, minute=0, second=0, microsecond=0))
    y = y.fromordinal(y.toordinal() - 1).replace(tzinfo=timezone.utc)
    pos_yest = 0
    for k, v in by_day.items():
        d = _parse(k)
        if d and d.date() == y.date():
            pos_yest = _int(v.get("autoTaggedInterested", 0))
            break

    return {
        "campaign": campaign.get("name", ""),
        "platform": "Heyreach",
        "status": campaign.get("status", ""),
        "total_leads": _int(ps.get("totalUsers", 0)),
        "leads_added_month": added_month,
        "leads_added_yesterday": added_yest,
        "leads_in_progress": _int(ps.get("totalUsersInProgress", 0)),
        "connections_sent": _int(oa.get("connectionsSent", 0)),
        "connections_accepted": _int(oa.get("connectionsAccepted", 0)),
        "msg_sent": _int(oa.get("messagesSent", 0)),
        "positive_responses_yesterday": pos_yest,
        "total_responses_month": _int(om.get("totalMessageReplies", 0)),
        "positive_neutral_month": _int(om.get("autoTaggedInterested", 0)),
    }


def total_row(rows: list[dict]) -> dict:
    out = {c: "" for c in COLUMNS}
    out["campaign"] = "Total"
    for col in _NUMERIC:
        out[col] = sum(r.get(col, 0) for r in rows if isinstance(r.get(col), int))
    return out
```

- [ ] **Step 4: Run, verify pass** — `python3 test_campaign_metrics.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/campaign_metrics.py test_campaign_metrics.py && git commit -m "feat(metrics): campaign-metrics row assembly + date bucketing"`

---

### Task 6: Sheets writer for the metrics tab

**Files:**
- Modify: `smartlead/sheets.py`

**Interfaces:**
- Consumes: `campaign_metrics.COLUMNS`, list of metric rows (incl Total row).
- Produces: `SheetsWriter.write_campaign_metrics(rows: list[dict]) -> None` (writes the `CAMPAIGN_METRICS_TAB_NAME` tab).

- [ ] **Step 1: Add header labels** — in `_HEADER_LABELS` add:
```python
    "Campaign Metrics": {
        "campaign": "Campaign name", "platform": "Platform", "status": "Campaign Status",
        "total_leads": "Total leads", "leads_added_month": "Leads added this month",
        "leads_added_yesterday": "Leads added yesterday", "leads_in_progress": "Leads in progress",
        "connections_sent": "Connections sent", "connections_accepted": "Connections accepted",
        "msg_sent": "Msg Sent", "positive_responses_yesterday": "Positive Responses Yesterday",
        "total_responses_month": "Total Responses this month",
        "positive_neutral_month": "Positive/ Neutral responses this month",
    },
```
and in `_COL_WIDTHS`:
```python
    "Campaign Metrics": {"campaign": 320, "platform": 100, "status": 130},
```

- [ ] **Step 2: Add the method** — in `SheetsWriter`, after `write_master_inboxes`:
```python
    def write_campaign_metrics(self, rows: list[dict]) -> None:
        """Write the 'Campaign Metrics' tab (Smartlead + HeyReach campaigns)."""
        from smartlead.config import CAMPAIGN_METRICS_TAB_NAME
        from smartlead.campaign_metrics import COLUMNS
        if not rows:
            print("  [Sheets] No campaign metrics rows — skipping.")
            return
        projected = [{c: r.get(c, "") for c in COLUMNS} for r in rows]
        self._write_tab(CAMPAIGN_METRICS_TAB_NAME, projected)
```

- [ ] **Step 3: Status coloring** — in `_status_colors`, add a branch (uses existing `_status_style`):
```python
        from smartlead.config import CAMPAIGN_METRICS_TAB_NAME
        if tab_key == CAMPAIGN_METRICS_TAB_NAME and "status" in columns:
            col_idx = columns.index("status")
            for ri, row in enumerate(data, start=1):
                bg, fg = _status_style(str(row.get("status", "")).upper())
                if bg:
                    reqs.append(_format_range(sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                                              bg=bg, fg=fg, bold=True, h_align="CENTER"))
```
(Place near the other `if tab_key ==` blocks. `_status_style` already maps ACTIVE/IN_PROGRESS→green, PAUSE→yellow, COMPLETE→gray; confirm IN_PROGRESS/COMPLETE handled — if `_status_style` keys on prefix, IN_PROGRESS may not match; if so, the cell just stays uncolored, which is acceptable.)

- [ ] **Step 4: Verify compile + import** — `python3 -c "from smartlead.sheets import SheetsWriter; print('write_campaign_metrics' in dir(SheetsWriter))"` → `True`. And `python3 -m py_compile smartlead/sheets.py`.

- [ ] **Step 5: Commit** — `git add smartlead/sheets.py && git commit -m "feat(metrics): sheets writer for Campaign Metrics tab"`

---

### Task 7: run.py wiring

**Files:**
- Modify: `run.py`

**Interfaces:**
- Consumes: everything above. Produces the live tab.

- [ ] **Step 1: Return campaign_summary from process_account** — `process_account` currently returns inbox rows only. Change it to also surface the account's campaign_summary. Simplest: make it return a tuple. Find the current `return inbox_data` and the call site.

  Change the signature/return of `process_account` to `return inbox_data, campaign_summary` (it already has `campaign_summary` from `fetch_account_data`). Update the early-return to `return [], []`.

  At the call site in `main()`:
```python
            rows, campaigns = await process_account(acc.api_key, acc.sheet_id, acc.name, deliverability_map, active_only)
            rows_by_sheet.setdefault(acc.sheet_id, []).extend(rows)
            if acc.name in CAMPAIGN_METRICS_CLIENTS:
                smartlead_campaigns_for_metrics.append((acc, campaigns))
```
  Add before the loop: `smartlead_campaigns_for_metrics: list = []`

- [ ] **Step 2: Add imports** — top of `run.py`:
```python
from smartlead.config import (TEST_TAB_NAME, ACCOUNT_DELIVERABILITY_TABS, MASTER_TAB_NAME,
                              CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_SHEET_ID,
                              CAMPAIGN_METRICS_TAB_NAME, SMARTLEAD_POSITIVE_CATEGORY_IDS)
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead import campaign_metrics as cm
from datetime import timezone
```
(`datetime` already imported; ensure `timezone` available.)

- [ ] **Step 3: Build + write the metrics tab** — after the master-tab writes in `main()`, add:
```python
    # ── Campaign Metrics dashboard (Smartlead + HeyReach) ────────────────────
    try:
        today = datetime.now(timezone.utc)
        ms = month_start = cm.month_start(today)
        ms_str, end_str = ms.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
        yday = today.fromordinal(today.toordinal() - 1)
        yday_str = yday.strftime("%Y-%m-%d")
        metric_rows: list[dict] = []

        # Smartlead rows (DARLEAN account(s))
        for acc, campaigns in smartlead_campaigns_for_metrics:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                for camp in campaigns:
                    cid = str(camp.get("campaign_id") or camp.get("id") or "")
                    if not cid:
                        continue
                    try:
                        leads = await slc.get_campaign_leads(cid)
                        m_an = await slc.get_analytics_by_date(cid, ms_str, end_str)
                        month_replies = int(float(m_an.get("reply_count", 0) or 0))
                    except Exception as exc:
                        print(f"  [metrics] Smartlead campaign {cid} failed: {exc}")
                        leads, month_replies = [], 0
                    metric_rows.append(cm.smartlead_metric_row(
                        camp, leads, month_replies, 0, today, SMARTLEAD_POSITIVE_CATEGORY_IDS))

        # HeyReach rows (all workspaces; currently DARLEAN)
        for ws in discover_heyreach_workspaces():
            try:
                async with HeyReachClient(ws.api_key, ws.name) as hrc:
                    camps = await hrc.list_campaigns()
                    for camp in camps:
                        cid = camp["id"]
                        try:
                            oa = await hrc.get_overall_stats(cid)
                            om = await hrc.get_overall_stats(cid, start=ms.isoformat().replace("+00:00", "Z"),
                                                             end=today.isoformat().replace("+00:00", "Z"))
                            leads = await hrc.get_campaign_leads(cid)
                        except Exception as exc:
                            print(f"  [metrics] HeyReach campaign {cid} failed: {exc}")
                            oa, om, leads = {}, {}, []
                        metric_rows.append(cm.heyreach_metric_row(camp, oa, om, leads, today))
            except Exception as exc:
                print(f"  [metrics] HeyReach workspace {ws.name} failed: {exc}")

        if metric_rows:
            metric_rows.append(cm.total_row(metric_rows))
            SheetsWriter(CAMPAIGN_METRICS_SHEET_ID).write_campaign_metrics(metric_rows)
            print(f"[*] Campaign Metrics tab written: {len(metric_rows)-1} campaigns")
    except Exception as exc:
        print(f"[!] Campaign Metrics dashboard failed: {exc}")
```
(`SmartleadClient` already imported in run.py. Remove the unused `month_start` alias if your linter complains — keep only `ms`.)

- [ ] **Step 4: Compile + mock run** — `python3 -m py_compile run.py` then `python3 run.py --mock` (mock path returns before metrics; just confirms no import/syntax error). Expected: mock output, no traceback.

- [ ] **Step 5: Commit** — `git add run.py && git commit -m "feat(metrics): wire Campaign Metrics into the daily sync"`

---

### Task 8: Live end-to-end validation

**Files:**
- Create (temp): `_metrics_live.py`; delete after.

- [ ] **Step 1: Live read-only build (no sheet write)** — `_metrics_live.py`:
```python
import asyncio, os
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from smartlead.heyreach import HeyReachClient
from smartlead.heyreach_accounts import discover_heyreach_workspaces
from smartlead import campaign_metrics as cm

async def main():
    today=datetime.now(timezone.utc); ms=cm.month_start(today)
    rows=[]
    for ws in discover_heyreach_workspaces():
        async with HeyReachClient(ws.api_key, ws.name) as c:
            for camp in await c.list_campaigns():
                cid=camp["id"]
                oa=await c.get_overall_stats(cid)
                om=await c.get_overall_stats(cid, start=ms.isoformat().replace("+00:00","Z"), end=today.isoformat().replace("+00:00","Z"))
                leads=await c.get_campaign_leads(cid)
                rows.append(cm.heyreach_metric_row(camp,oa,om,leads,today))
    print(f"HeyReach campaigns: {len(rows)}")
    for r in rows[:5]:
        print(f"  {r['campaign'][:30]:30} leads={r['total_leads']} sent={r['connections_sent']} acc={r['connections_accepted']} msg={r['msg_sent']} resp_m={r['total_responses_month']} pos_y={r['positive_responses_yesterday']}")
asyncio.run(main())
```
Run: `python3 _metrics_live.py`. Expected: ~16 HeyReach campaigns with sensible numbers matching the HeyReach UI.

- [ ] **Step 2: Full live sync (writes the tab)** — set a scratch tab name to avoid clobbering during test:
```bash
CAMPAIGN_METRICS_TAB_NAME=__metrics_test python3 run.py 2>&1 | grep -E "Campaign Metrics|metrics"
```
Expected: `Campaign Metrics tab written: N campaigns`. Open the sheet, confirm the `__metrics_test` tab has Smartlead + HeyReach rows + a Total row. Then delete the scratch tab.

- [ ] **Step 3: Clean up** — `rm -f _metrics_live.py`.

- [ ] **Step 4: Final commit** — nothing to commit (temp deleted); the feature commits landed per task.

---

## Self-Review Notes

- **Spec coverage:** config (T1), HeyReach client (T2), workspace discovery (T3), Smartlead leads/analytics (T4), row assembly + dates + positive + Total (T5), sheets writer (T6), run wiring + daily cron path (T7), live validation (T8). All spec sections mapped.
- **Known v1 limitations (per spec):** HeyReach leads-added is best-effort (in-progress subset); Smartlead `positive_responses_yesterday` = "-" (API-limited; HeyReach has it); Smartlead `positive_neutral_month` = current category snapshot. Documented in code comments + spec.
- **Type consistency:** `smartlead_metric_row`/`heyreach_metric_row` both return dicts keyed by `COLUMNS`; `total_row` sums `_NUMERIC`; sheets projects `COLUMNS`. HeyReach campaign id is int; Smartlead campaign id stringified. `_int` coerces string analytics values.
- **No placeholders:** category-id set is a tunable config value with a discovery step (T1.3), not a placeholder.
