# Campaign Metrics Dashboard (Darlean) — Design

**Date:** 2026-06-30
**Status:** Approved (pending spec review)

## Purpose

Add a daily-synced **Campaign Metrics** dashboard tab that mirrors the existing
manual spreadsheet: one row per campaign across **Smartlead** (email) and
**HeyReach** (LinkedIn), with leads / connections / messages / responses
metrics. Scope for v1: the **DARLEAN** client only (its Smartlead account +
its HeyReach workspace). Written by the existing `run.py` sync (daily cron).

The tab lives in the existing dashboard sheet for now; a dedicated sheet id is
configurable for later.

## Scope

In scope (v1, DARLEAN only):
- New `Campaign Metrics` tab with all 13 columns from the reference image.
- New HeyReach API client + workspace discovery (`HEYREACH_API_KEY_<NAME>`).
- Smartlead campaign metrics from the DARLEAN account (reuses fetched data +
  adds per-campaign lead/category/date calls).

Out of scope (v1):
- Other clients (Belardi Wong, PRECISE_LEADS, Mythic) — wiring is generic, just
  add their HeyReach keys + include their Smartlead accounts later.
- HeyReach **manual** tags for positive counting (auto-tag only in v1 — see below).
- Inbox/deliverability changes (separate feature, already shipped).

## Columns

One row per campaign. Cumulative columns reflect current totals; period columns
reflect a date window (current calendar month / yesterday, UTC).

| # | Column | Type | Smartlead source | HeyReach source |
|---|---|---|---|---|
| 1 | Campaign name | text | campaign name | `name` |
| 2 | Platform | text | "Smartlead" | "Heyreach" |
| 3 | Campaign Status | text | status | `status` |
| 4 | Total leads | cumulative | `campaign_lead_stats.total` | `progressStats.totalUsers` |
| 5 | Leads added this month | period | leads with `created_at` in current month | leads with `creationTime` in current month |
| 6 | Leads added yesterday | period | `created_at` == yesterday | `creationTime` == yesterday |
| 7 | Leads in progress | cumulative | `campaign_lead_stats.inprogress` | `progressStats.totalUsersInProgress` |
| 8 | Connections sent | cumulative | "-" | `connectionsSent` (all-time) |
| 9 | Connections accepted | cumulative | "-" | `connectionsAccepted` (all-time) |
| 10 | Msg Sent | cumulative | `sent_count` | `messagesSent` (all-time) |
| 11 | Positive Responses Yesterday | period | positive-category leads, reply date == yesterday | `byDayStats[yesterday].autoTaggedInterested` |
| 12 | Total Responses this month | period | replies with date in current month | sum `byDayStats[month].totalMessageReplies` |
| 13 | Positive/Neutral this month | period | positive+neutral-category leads, reply date in month | `autoTaggedInterested` for the month (dated overall) |

The tab also writes a **Total** footer row summing numeric columns (matches the
reference image's Total row).

### Positive definition

- **Smartlead** — by lead **category** (campaign-manager tags):
  - Positive/Neutral = `Interested`, `Meeting Request`, `Information Request`
    (configurable set `SMARTLEAD_POSITIVE_CATEGORIES`).
  - Negative = `Not Interested`, `Do Not Contact`, `Wrong Person`.
  - `Out Of Office` / auto-replies = counted in *Total Responses*, excluded from positive.
- **HeyReach** — `autoTaggedInterested` (HeyReach AI interested tag). Manual
  HeyReach tags are NOT counted in v1 (fetching per-lead tags = ~lead-count×
  campaigns calls/run, too heavy for a daily job). Documented limitation.

## Architecture

```
run.py
  ├─ (existing) per Smartlead account: fetch_account_data -> campaign_summary
  │     └─ keep DARLEAN account's campaign_summary
  ├─ HeyReach: for each HEYREACH_API_KEY_<NAME> workspace
  │     └─ HeyReachClient.list_campaigns + per-campaign stats/leads
  ├─ campaign_metrics.build_rows(smartlead_campaigns, heyreach_rows, today)
  └─ SheetsWriter.write_campaign_metrics(rows) -> "Campaign Metrics" tab
```

### New files / changes

**`smartlead/heyreach.py`** — async `HeyReachClient` (mirrors `SmartleadClient`):
- Base `https://api.heyreach.io/api/public`, header `X-API-KEY`.
- `_post(path, body)` / `_get(path)` with the same retry/backoff as Smartlead
  (429 / 5xx / transport errors, Retry-After).
- `list_campaigns()` → paginate `POST /campaign/GetAll` `{offset, limit}` until
  `len < limit` or `totalCount` reached. Returns campaign dicts (incl
  `progressStats`).
- `get_overall_stats(campaign_id, start=None, end=None)` →
  `POST /stats/GetOverallStats {campaignIds:[id], accountIds:[], startDate, endDate}`
  → `{overallStats, byDayStats}`.
- `get_campaign_leads(campaign_id)` → paginate
  `POST /campaign/GetLeadsFromCampaign {campaignId, offset, limit}`; returns all
  leads (each has `creationTime`). **Build-time check:** confirm this pages the
  full lead set (the 35-vs-99 observation) — if it returns only a subset, the
  leads-added counts use what's returned and a `log()` notes the cap.

**`smartlead/heyreach_accounts.py`** — `discover_heyreach_workspaces()` reads
`HEYREACH_API_KEY_<NAME>` env vars → `[HeyReachWorkspace(name, api_key)]`
(same convention as Smartlead account discovery).

**`smartlead/campaign_metrics.py`** — pure assembly + date bucketing:
- `month_bounds(today)` / `yesterday(today)` → UTC datetimes.
- `smartlead_metric_row(campaign_summary_row, leads, today)` — builds a row;
  `leads` are the campaign's leads (created_at, category, reply date) fetched
  once per campaign via the Smartlead client.
- `heyreach_metric_row(campaign, overall_alltime, overall_month, leads, today)`.
- `build_rows(...)` → ordered list + a `Total` footer row.

**`smartlead/api.py`** (SmartleadClient) — add helpers if missing:
- `get_campaign_leads(campaign_id)` paginated (created_at, category, reply date).
  Reuse `_gather_chunked`. **Build-time check:** confirm lead objects expose
  `created_at`, category, and a reply timestamp; if a field is absent, fall back
  to the campaign analytics-by-date endpoint for responses and `log()` it.

**`smartlead/sheets.py`** — `write_campaign_metrics(rows)`:
- New tab (default name `Campaign Metrics`). Reuse `_write_tab` pattern; add
  `_HEADER_LABELS["Campaign Metrics"]` + `_COL_WIDTHS` + status coloring
  (IN_PROGRESS green, PAUSED yellow, COMPLETE/COMPLETED gray) and a bold Total
  row. Write to `CAMPAIGN_METRICS_SHEET_ID`.

**`smartlead/config.py`** — add:
- `HEYREACH_BASE_URL`, HeyReach chunk/retry reuse Smartlead constants.
- `CAMPAIGN_METRICS_TAB_NAME = "Campaign Metrics"`,
  `CAMPAIGN_METRICS_SHEET_ID = getenv("CAMPAIGN_METRICS_SHEET_ID", DEFAULT_SHEET_ID)`.
- `SMARTLEAD_POSITIVE_CATEGORIES = {"Interested","Meeting Request","Information Request"}`.
- `CAMPAIGN_METRICS_CLIENTS = {"DARLEAN"}` — which Smartlead accounts to include.

**`run.py`** — after the account loop: filter DARLEAN's `campaign_summary`
(surface it up like inbox rows), fetch DARLEAN HeyReach rows, build + write the
tab. Wrapped in try/except (failure logs, doesn't crash the inbox sync).

## Date logic

- "This month" = first day of current month 00:00 UTC → now.
- "Yesterday" = the UTC calendar day before today (matches HeyReach `byDayStats`
  keys, which are UTC midnight). Smartlead dates parsed and compared in UTC.
- Today is passed in from `run.py` (the script's `datetime.now(timezone.utc)`).

## API cost (DARLEAN)

- HeyReach: 1 `GetAll` + per campaign (16): 1 all-time stats + 1 month stats + 1
  leads paging ≈ 16×3 + 1 ≈ 49 calls, chunked + rate-limited.
- Smartlead: reuses existing analytics; adds 1 leads paging per DARLEAN campaign.

## Error handling

- Per-campaign failures isolated (gather with `return_exceptions`), logged, that
  campaign's row uses available fields with `0`/`-` for the rest.
- HeyReach workspace failure (bad key / 401) logs and skips that workspace; the
  Smartlead rows still write.
- Whole-tab write wrapped in try/except in `run.py`.

## Testing

- Extend `test_sync_wiring.py` (or a new `test_campaign_metrics.py`) with a fake
  HeyReach client returning canned `GetAll` / stats / leads, and assert:
  - column values map correctly (total_leads, in_progress, connections, msg_sent,
    responses),
  - month/yesterday bucketing (leads added, responses, positive) given a fixed
    `today` and canned dates,
  - Smartlead positive-category counting,
  - the Total footer row sums correctly.
- Live read-only smoke against DARLEAN HeyReach (no sheet write) to confirm real
  field shapes, then a real sync writing the tab to a scratch tab name.

## Notes / risks

- **HeyReach lead paging completeness** (35 vs 99) — confirmed at build; if
  capped, leads-added is best-effort + logged.
- **HeyReach manual tags** — not in v1 positive count (auto-tag only).
- **Smartlead lead field availability** (category, reply date) — confirmed at
  build; analytics-by-date fallback for responses if needed.
- Adding more clients later = add their HeyReach key + include their Smartlead
  account in `CAMPAIGN_METRICS_CLIENTS` (or drop the filter for all).
