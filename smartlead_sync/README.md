# Smartlead Sync

Pulls every inbox across all Smartlead accounts (clients) into a single
Google Sheet, computes per-inbox **availability**, and writes a consolidated
**All Inboxes** tab that a downstream tool reads to select free inboxes and
connect them to campaigns. Read-only against Smartlead — never mutates campaigns.

## What it writes

Per Smartlead account: `Campaign Summary`, `Inboxes`, `Warmup Reputation` tabs.
Once, across all accounts: an **All Inboxes** master tab — one row per unique
inbox — plus `Column Glossary` and `Last Sync`.

### All Inboxes columns (the selection source)

| column | meaning |
| --- | --- |
| `client` | which Smartlead account owns the inbox |
| `email`, `name`, `provider`, `account_id` | identity (`account_id` = key to connect) |
| `availability` | **FREE** = connectable now, else **BUSY** |
| `busy_reason` | why BUSY: `disconnected` / `warmup_blocked` / `no_capacity` / `low_rep` / `stale_test` / `failed_test` / `untested` |
| `campaigns` | how many active campaigns the inbox is already in |
| `max_per_day`, `sent_today`, `capacity_left` | today's volume (`capacity_left = max_per_day − sent_today`) |
| `true_load`, `available_capacity` | aggregate load / headroom across all campaigns |
| `warmup_state` | `off` / `blocked` / `warming` / `ramped` |
| `warmup_rep_pct`, `warmup_max_count` | warmup detail |
| `test_sheet_status`, `test_date` | deliverability result + when last tested (>14d ⇒ `stale`) |
| `last_active_date` | last date the inbox sent |

### FREE rule

An inbox is `FREE` only if **all** hold: connected (`connection_ok`), warmup not
blocked, `capacity_left > 0`, warmup rep ≥ 90%, and a fresh `inbox` deliverability
test (a test older than `TEST_STALE_DAYS` = 14 days is `stale`, not `inbox`).
Any failing check is listed in `busy_reason`.

## How a downstream tool selects inboxes

```
pool = rows where client == X and availability == "FREE"
pool = sort(pool, capacity_left desc)
pick top inboxes until summed capacity_left >= needed daily volume
connect picked account_id values to the campaign (within client X's Smartlead account)
```

Only connect an inbox to a campaign **within its own Smartlead account** (the
`client` column) — never across clients.

## Run locally

```bash
pip install -r requirements.txt
python run.py            # real sync, all accounts -> Google Sheets
python run.py --mock     # fake data, no API calls, no sheet writes
python run.py --all      # include completed/stopped campaigns too
python test_sync_wiring.py   # offline regression test
```

> On the Windows dev box, use `python3` (the default `python` may point at a
> venv without deps).

## Configuration (environment variables)

Set in `.env` locally, or as host env vars in production.

| var | purpose |
| --- | --- |
| `SMARTLEAD_API_KEY` | default account (Belardi Wong) |
| `SMARTLEAD_API_KEY_<NAME>` | additional accounts (e.g. `_PRECISE_LEADS`, `_D`) |
| `SHEET_ID` | dashboard sheet id (also default `MASTER_SHEET_ID`) |
| `TEST_SHEET_ID` | deliverability test sheet id |
| `SMARTLEAD_SERVICE_ACCOUNT_JSON` | full service-account JSON (preferred in prod) |
| `SERVICE_ACCOUNT_FILE` | path to service-account file (local fallback) |
| `MASTER_SHEET_ID`, `MASTER_TAB_NAME` | override master tab location (optional) |

Account → deliverability test-tab mapping lives in `smartlead/config.py`
(`ACCOUNT_DELIVERABILITY_TABS`).

## Deployment

Deployed as part of the parent `infra-bot` container: `node index.js` runs a
`node-cron` job (daily 10:00 IST) that `spawn`s `python run.py` in this folder.
No separate service.

**In production the service-account file is NOT in the image** (it is git- and
docker-ignored). Provide credentials via the `SMARTLEAD_SERVICE_ACCOUNT_JSON`
env var, and all `SMARTLEAD_API_KEY*` / `*SHEET_ID` values as host env vars.
