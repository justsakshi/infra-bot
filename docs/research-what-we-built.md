# What We Built — Deliverability/Fleet Automation Inventory

Read-only audit. Repo: `infrabot/infra-bot`. Scope: `smartlead_sync/*_executor.py`,
`*_monitor.py`, `placement_*.py`, cron wiring in `index.js`, config defaults in
`smartlead_sync/smartlead/config.py`.

All cron times are IST. All Python entrypoints are launched from `index.js` via
`spawn('python', [...])` with `cwd: smartlead_sync`.

---

## 1. Executor/monitor inventory

### `retest_executor.py` — Auto placement-test (SmartDelivery, connected)
- **Does:** Pass A polls in-flight SmartDelivery tests and records pass/fail; Pass B
  picks worst-first inboxes per client (capped) and creates new connected tests.
- **Flag:** `RETEST_ENABLED` — default **OFF** (`false`).
- **Cron:** `0 11 * * *` (daily, 11:00).
- **Smartlead/SmartDelivery endpoints:** campaigns list/details, campaign email-accounts
  add/get, `SmartDeliveryClient` (create/poll test, report), `slc.set_warmup()`.
- **Sheets:** reads `DeliverabilityReader` (deliverability tabs per `ACCOUNT_DELIVERABILITY_TABS`), writes via `placement_sheet`.
- **Mongo:** `placement_tests`, `placement_results` (via `placement_store`).
- **Size:** 318 lines. **Tests:** none directly (`test_retest_targets.py`, `test_retest_roundtrip.py`, `test_smart_delivery.py` cover helper modules it depends on, not the executor itself).

### `placement_executor.py` — Weekly placement batch (SmartDelivery, connected, multi-domain/test)
- **Does:** Thursday batched placement testing across many senders per test (cost-optimized), plus a `--collect-only` mode for result collection.
- **Flag:** driven by `PLACEMENT_ENABLED` env var checked in `index.js` (not in `config.py`) — `process.env.PLACEMENT_ENABLED !== 'true'` appends `--dry-run`. Default **OFF** (dry-run) since the env var is unset in `config.py`'s defaults.
- **Cron:** batch `0 10,12,14,16 * * 4` (Thursdays, waves); collector `30 12,16 * * *` (daily).
- **Smartlead/SmartDelivery endpoints:** `list_campaigns`, `list_email_accounts`, `add_campaign_email_accounts`, `get_campaign_email_accounts`, `SmartDeliveryClient`, plus low-level `_get`.
- **Sheets:** `smartlead.sheets`, `placement_grid` (writes per-client deliverability grid).
- **Mongo:** `placement_tests`, `placement_results` via `placement_store`.
- **Size:** 422 lines (largest executor). **Tests:** none for the executor itself; `test_placement_copy.py`, `test_placement_grid.py`, `test_placement_schedule.py` cover its helper modules (`placement_copy.py`, `placement_grid.py`, `placement_schedule.py`).

### `placement_summary.py` — Fleet placement rollup (read-only, credit-free)
- **Does:** Calls SmartDelivery's `/spam-test/report/mailboxes-summary` to show each mailbox's placement rolled up across all past tests — no new test, no credit spend.
- **Flag:** none (read-only, always runs).
- **Cron:** `50 9 * * *` (daily, 09:50). Exit code 2 = a mailbox below threshold.
- **Smartlead endpoints:** `SmartDeliveryClient` (mailboxes-summary report only).
- **Sheets:** none (console/exit-code only).
- **Mongo:** none.
- **Size:** 120 lines. **Tests:** none directly.

### `nc_test_executor.py` — Non-connected (Anjali-style) placement tests
- **Does:** Semi-automated: a human creates a non-connected SmartDelivery test manually in the Smartlead UI and pastes seed list + Track-ID into the "NC Tests" sheet tab; this executor sends the seed email via a one-off campaign from the inbox's own account, polls the test, writes results, and restores the inbox's daily limit.
- **Flag:** `NC_TEST_ENABLED` — default **OFF**.
- **Cron:** `15 9-21 * * *` (hourly, 9am-9pm).
- **Smartlead/SmartDelivery endpoints:** `create_campaign`, `add_campaign_leads`, `add_campaign_email_accounts`, `save_campaign_sequences`, `set_campaign_status`, `update_campaign_schedule`, `update_email_account`, `list_email_accounts`, `SmartDeliveryClient`.
- **Sheets:** `NC Tests` tab (read/write state machine), `API Tests` tab (results, via `placement_sheet.append_api_result`).
- **Mongo:** `placement_tests`/`placement_results` via `placement_store`.
- **Size:** 375 lines. **Tests:** none directly.

### `eg_test_executor.py` — EmailGuard placement testing (credit-free alternative)
- **Does:** Same two-pass pattern as `retest_executor.py` (poll then create) but against EmailGuard instead of Smartlead SmartDelivery, so it doesn't burn Smartlead's limited placement-test quota.
- **Flag:** `EG_TEST_ENABLED` — default **OFF**.
- **Cron:** `20 11 * * *` (daily, 11:20).
- **Smartlead endpoints:** `create_campaign`, `add_campaign_leads` (implied), `add_campaign_email_accounts`, `save_campaign_sequences`, `set_campaign_status`, `update_campaign_schedule`, `list_email_accounts`; plus `EmailGuardClient` (non-Smartlead).
- **Sheets:** `API Tests` tab via `placement_sheet.append_api_result`.
- **Mongo:** `placement_tests`/`placement_results` via `placement_store`.
- **Size:** 226 lines. **Tests:** none directly.

### `warmup_executor.py` — Auto warmup (state-based profiles)
- **Does:** Applies warmup on/off/volume changes computed by `warmup_planner.py` per the NEW/ACTIVE/IDLE/RECOVERING state model (policy: warmup always ON, never fully disabled).
- **Flag:** `WARMUP_AUTO_ENABLED` — default **OFF** (dry-run: logs only). Related: `WARMUP_ALWAYS_ON` defaults **ON** (true).
- **Cron:** `30 11 * * *` (daily, 11:30).
- **Smartlead endpoints:** raw `c.get(...)`, warmup-setting calls inside `warmup_planner`/`SmartleadClient`.
- **Sheets:** none directly (reads inbox data via `fetch_account_data`, not sheets).
- **Mongo:** none directly.
- **Size:** 109 lines (smallest). **Tests:** none for the executor; `test_warmup_planner.py` and `test_campaign_freshness.py` cover its dependency modules.

### `rotation_executor.py` — Auto inbox rotation (swap broken senders for bench)
- **Does:** Swaps P0-broken sender inboxes out of live campaigns for healthy bench inboxes of the same client (add replacement, handle in-flight leads, remove victim).
- **Flag:** `ROTATION_ENABLED` — default **OFF**.
- **Cron:** `0 12 * * *` (daily, 12:00).
- **Smartlead endpoints:** `list_campaigns`, `get_campaign_email_accounts`, `add_campaign_email_accounts`, `remove_campaign_email_accounts`, `get_campaign_leads`, `resume_lead`.
- **Sheets:** `DeliverabilityReader` (deliverability tabs) for source health data.
- **Mongo:** `rotation_log` via `rotation_store.RotationStore`.
- **Size:** 160 lines. **Tests:** none for the executor; `test_rotation_planner.py` covers `rotation_planner.py`.

### `bounce_protect_executor.py` — Bounce auto-protection sweep
- **Does:** Ensures every ACTIVE campaign has Smartlead's built-in "High Bounce Rate Auto Protection" threshold set (`bounce_autopause_threshold`), so Smartlead itself pauses a campaign on bounce spikes.
- **Flag:** `BOUNCE_PROTECT_ENABLED` — default **OFF**.
- **Cron:** `30 12 * * *` (daily, 12:30).
- **Smartlead endpoints:** `list_campaigns`, `fetch_all_campaign_details`, `update_campaign_settings`.
- **Sheets:** none.
- **Mongo:** `bounce_protect_applied` (tracks what was already set, since Smartlead's GET doesn't return the field).
- **Size:** 171 lines. **Tests:** none directly.

### `blacklist_monitor.py` — DNSBL blacklist monitor
- **Does:** Checks every client sending domain against Spamhaus DBL / SURBL / URIBL via Google DNS-over-HTTPS; read-only, no Smartlead writes.
- **Flag:** none (always runs; read-only).
- **Cron:** `0 9 * * 1` (weekly, Monday 09:00).
- **Smartlead endpoints:** `list_email_accounts` (to enumerate domains) — no writes.
- **Sheets:** none (console + notify).
- **Mongo:** `blacklist_checks`.
- **Size:** 335 lines. **Tests:** none directly.

### `key_health_monitor.py` — API-key health watchdog
- **Does:** One cheap authenticated GET per Smartlead account; flags dead (401/403) vs unreachable (network/5xx) keys before the rest of the day's jobs run blind.
- **Flag:** none (always runs, read-only, "no enable flag" per its own docstring).
- **Cron:** `45 9 * * *` (daily, 09:45, before the 10:00 sync).
- **Smartlead endpoints:** raw `GET /email-accounts/`.
- **Sheets:** none.
- **Mongo:** `key_health_checks`.
- **Size:** 139 lines. **Tests:** none directly.

### `reply_monitor.py` — Per-domain reply-rate early warning
- **Does:** Aggregates per-mailbox stats into per-domain daily deltas and alerts when a domain's reply rate drops materially below its own trailing baseline (a leading indicator, ~48h before bounces/opens move).
- **Flag:** none (always runs, read-only).
- **Cron:** `0 13 * * *` (daily, 13:00).
- **Smartlead endpoints:** `list_campaigns`, `get_campaign_mailbox_statistics`, raw `c.get(...)`.
- **Sheets:** none directly.
- **Mongo:** `domain_reply_stats`.
- **Size:** 180 lines. **Tests:** `test_reply_stats.py`, `test_reply_rate_passthrough.py` cover the underlying `reply_stats.py` module, not the executor entrypoint.

### (Referenced by cron but out of the requested `*_executor`/`*_monitor`/`placement_*` scope)
- `run.py` — main daily sync (10:00 daily), writes the master workbook/sheets. Not an executor/monitor per the naming filter but is the backbone job everything else assumes has run.
- `capacity_planner.py` — Monday 09:30, read-only advisory (Capacity tab + `domain_registry` collection).

---

## 2. Capability table

| Capability | Owning script | Enabled by default? | Has direct tests? |
|---|---|---|---|
| Connected placement retest (worst-first, daily) | `retest_executor.py` | OFF (`RETEST_ENABLED=false`) | No (helpers only) |
| Weekly batched placement test (multi-sender/test) | `placement_executor.py` | OFF (`PLACEMENT_ENABLED` env, unset) | No (helpers only) |
| Fleet placement rollup (credit-free, read-only) | `placement_summary.py` | Always on (no flag) | No |
| Non-connected (manual-assisted) placement test | `nc_test_executor.py` | OFF (`NC_TEST_ENABLED=false`) | No |
| EmailGuard placement test (credit-free alt.) | `eg_test_executor.py` | OFF (`EG_TEST_ENABLED=false`) | No |
| State-based auto warmup (NEW/ACTIVE/IDLE/RECOVERING) | `warmup_executor.py` | OFF (`WARMUP_AUTO_ENABLED=false`); policy `WARMUP_ALWAYS_ON=true` | No (helpers only) |
| Auto inbox rotation (swap broken senders) | `rotation_executor.py` | OFF (`ROTATION_ENABLED=false`) | No (helpers only) |
| Bounce auto-protection threshold sweep | `bounce_protect_executor.py` | OFF (`BOUNCE_PROTECT_ENABLED=false`) | No |
| DNSBL blacklist monitor (Spamhaus/SURBL/URIBL) | `blacklist_monitor.py` | Always on (read-only) | No |
| API-key health watchdog | `key_health_monitor.py` | Always on (read-only) | No |
| Per-domain reply-rate early warning | `reply_monitor.py` | Always on (read-only) | No (helpers only) |
| Capacity planning (bench sizing, lead time) | `capacity_planner.py` | Always on (read-only) | Yes (`test_capacity.py`) |

**Pattern:** every mutating capability (retest, weekly placement, NC test, EG test, warmup, rotation, bounce-protect) ships **dry-run/OFF by default** and none of the executor entrypoints themselves have direct tests — only their pure-logic helper modules (`*_planner.py`, `*_targets.py`, `*_schedule.py`, `*_copy.py`, `*_grid.py`) are unit-tested. The executors are thin orchestration wrappers around those tested modules.

---

## 3. Dead / duplicated / abandoned — flagged for follow-up

- **Doc sprawl:** 8 files match `docs/DELIVERABILITY*.md` plus 2 `docs/PLACEMENT*.md` files. Several explicitly supersede others (`DELIVERABILITY_MASTER_PLAN_CONSOLIDATED_2026-07-29.md` says "supersedes as the single entry point," `DELIVERABILITY_PLAN_FOR_TEAM.md` and `DELIVERABILITY_RESEARCH_AND_WORKFLOW.md` both claim "this is the document to read"). None of the superseded ones appear deleted. Worth pruning to one canonical doc + a changelog rather than 8 co-existing "read this first" files.
- **`PLACEMENT_ENABLED` inconsistency:** every other mutating capability is gated by a flag defined in `config.py` (so it's documented and greppable in one place); `PLACEMENT_ENABLED` for `placement_executor.py` is instead checked ad hoc directly in `index.js` and never defined/defaulted in `config.py`. Easy to miss when auditing "what's on."
- **Warmup headroom job — confirmed dead, already removed:** `index.js` (~line 1805) has a large comment block documenting that a warmup-headroom cron job was deliberately deleted 2026-07-10 after its premise was proven wrong. Good hygiene, but worth noting the code graveyard exists in comments, not just history.
- **Three overlapping placement-testing paths** (`retest_executor.py` connected/SmartDelivery, `placement_executor.py` batched/SmartDelivery, `eg_test_executor.py` EmailGuard, `nc_test_executor.py` manual-assisted) all solve "get placement data for an inbox," each against a different constraint (quota, batch cost, credit-free, non-connected-only). Per `docs/PLACEMENT_TESTING_LOOP_DESIGN_2026-09-11.md`, "placement testing exists in code but the loop does not close" — i.e., the multiple testers were built before the consuming/decision loop was, so verify with the team whether all four are still meant to coexist or whether one should be retired once the loop design ships.
- **Test coverage gap:** no executor entrypoint (`*_executor.py`, `*_monitor.py`, `placement_executor.py`, `placement_summary.py`) has a same-named test file; all 28 test files test helper/planner modules. Fine for logic coverage, but the cron-triggered orchestration layer (auth wiring, sheet/Mongo I/O, argument handling like `--collect-only`/`--dry-run`) is untested end-to-end.
