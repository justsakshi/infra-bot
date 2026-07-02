# Auto Placement-Test Executor (Inbox Health Phase 3) — Design Spec

**Date:** 2026-07-02
**Status:** Approved (pending written-spec review)
**Owner:** infra-bot / smartlead_sync
**Depends on:** Inbox Health Workbook (Phase 1+2), already shipped.

## Goal

Automatically run Smartlead SmartDelivery **placement tests** for the inboxes the
health workbook flags as **untested or stale**, pull the inbox-vs-spam result back,
and feed it into the health score — so those "🤖 Auto" rows actually self-heal instead
of being recommendations. Runs for **all clients**, **worst-first**, under a **per-client
daily cap**, and **never over-spends or crashes** on missing credits.

## Non-Goals

- Not replacing Smartlead's native Automatic Test scheduler (complementary; we fill gaps
  the API can trigger on-demand).
- Not testing every inbox constantly — only the worst (untested → oldest stale), capped.
- Not fixing failed inboxes (DNS/copy/reconnect stay human).
- Not reading Smartlead's pre-existing tests (API cannot list them — see Constraints).

## Verified API Contract (probed live 2026-07-02, zero credits spent)

Base `https://smartdelivery.smartlead.ai/api/v1` (auth `?api_key=`).
All 4 accounts (PRECISE_LEADS, DARLEAN, Belardi Wong, MYTHIC) have SmartDelivery.

**Create (spends 1 credit once seeds send):** `POST /spam-test/manual`
Required body: `test_name`, `description`, `campaign_id` (int), `sequence_mapping_id`
(int, from `GET /campaigns/{id}/sequences` → `seqs[0].id`), `sender_accounts`
(list of `from_email`, from campaign email-accounts), `provider_ids:[20,21]`
(20=GSuite, 21=Office365), `spam_filters:["spam_assassin"]`, `link_checker:true`,
`all_email_sent_without_time_gap:false`, `min_time_btwn_emails:>=5`,
`min_time_unit:"minutes"`, `is_warmup:true`. Returns `{id|spamTestId}`.

**Poll (zero credit):** `GET /spam-test/{id}` → `{status, test_end_date}`. `status=ACTIVE`
until done; `test_end_date` set when complete (5–20 min).

**Report (zero credit, after complete):** `POST /spam-test/report/{id}/providerwise`
(body `{}`) → per-provider inbox/spam/tab counts. Also `groupwise`, `sender-account-wise`.

**Cannot (confirmed):** list existing tests, read credit balance — no such endpoints.
So we only track tests **we** create; credit balance is discovered at launch (error if 0).

## Architecture

A **separate scheduled job** (`retest_executor.py`), NOT part of the daily sync — so a
slow/failed 5–20 min test batch never delays or breaks the sync. It runs on its own cron
(e.g. once/day, after the sync). Two-pass design because tests take minutes to finish:

```
RUN (once/day):
  Pass A — POLL previously-created tests that were still ACTIVE:
    for each pending test in Mongo `placement_tests` (status=ACTIVE):
      GET /spam-test/{id}
      if complete -> POST providerwise report -> compute inbox% -> status inbox/fail
                  -> write result into deliverability layer (merge: newer wins)
                  -> mark test done in Mongo
  Pass B — CREATE new tests for worst-first targets, within caps:
    targets = health rows with priority owner=auto (untested, then stale oldest-first)
    group by client; for each client, take up to PER_CLIENT_DAILY_CAP targets:
      pick the target's campaign (needs senders + a sequence)
      POST /spam-test/manual  (guarded: catch credit/launch errors -> log+skip)
      on success -> store {test_id, client, campaign_id, inboxes, created_at, status:ACTIVE} in Mongo
  Log a summary (created, polled, completed, skipped, errors).
```

Tests created today are polled+read on the **next** run (or a short same-run re-poll after
a wait, but cross-run is simpler and cron-friendly).

### Result → health merge

A completed test reports **per-campaign** inbox placement (all its senders). We translate
`inbox% >= THRESHOLD` (e.g. ≥80%) → `inbox`, else → `fail`, dated today, and merge into the
placement source the health scorer reads. **Merge rule (API supplements manual):** for a
given domain/inbox, use the **newer** of the manual deliverability-sheet result and the API
result. Stored in Mongo `placement_results` with `source: "api"|"manual"` so provenance is
visible. The next daily sync re-scores using the freshest placement → workbook self-heals.

## Components (files)

- `smartlead/smart_delivery.py` (NEW) — async SmartDelivery client:
  `create_test(campaign) -> test_id`, `poll_test(id) -> {status, done, end_date}`,
  `get_report(id) -> {inbox_pct, spam_pct, per_provider}`. Uses the verified contract;
  all methods guard on non-2xx and raise typed errors (`SmartDeliveryError`,
  `CreditError`).
- `smartlead/placement_store.py` (NEW) — Mongo `placement_tests` (created tests + status)
  and `placement_results` (merged inbox/fail by domain+date+source). Fail-safe like
  `health_store` (no-op if Mongo down — but here that means the executor can't track
  state, so it logs and exits rather than blind-creating).
- `smartlead/retest_targets.py` (NEW) — pure: `select_targets(health_rows, per_client_cap,
  already_pending) -> list[target]`. Worst-first (untested before stale, oldest stale
  first), per-client cap, excludes inboxes already covered by an ACTIVE test.
- `retest_executor.py` (NEW) — the two-pass entry point (Pass A poll, Pass B create),
  summary logging, top-level try/except.
- `smartlead/config.py` (MODIFY) — `RETEST_PER_CLIENT_DAILY_CAP` (default small, e.g. 2),
  `RETEST_INBOX_THRESHOLD` (default 80), `RETEST_ENABLED` (default False — dry-run until
  explicitly turned on), `PLACEMENT_RESULTS_COLLECTION`, `PLACEMENT_TESTS_COLLECTION`.
- Deliverability read path (MODIFY, small) — when building the health snapshot's
  `test_sheet_status`/`test_date`, also consult `placement_results` (API source) and take
  the newer of manual-vs-api. Keeps the merge in one place.

## Safety / Guardrails

- **RETEST_ENABLED=False by default** → executor runs in **dry-run**: selects + logs
  targets, does NOT call create. Flip to True only after reviewing the dry-run.
- **Per-client daily cap** — hard limit; never exceed per client per run.
- **Credit/launch errors caught** — a create that fails on credits/plan/launch is logged
  (`credits exhausted on <client>`) and skipped; the run continues. No crash, no partial
  spend (a test only charges once seeds send; a rejected create charges nothing).
- **Idempotent** — pending tests tracked in Mongo; re-runs poll them, don't re-create.
- **Separate cron** — isolated from the daily sync; its failure can't affect the workbook.
- **min_time_btwn_emails forced ≥5** (business rule), is_warmup=true (else truncated).

## Data Flow / Error Handling

- Executor top-level try/except → logs and exits non-zero on fatal, but never touches the
  sync process.
- Mongo unavailable → executor logs "state store unavailable, skipping" and exits (does
  NOT create tests it can't track).
- A single campaign missing senders/sequence → skip that target, continue.
- Report fetch failure on a completed test → leave test pending, retry next run.

## Testing

- `test_retest_targets.py` (plain script) — `select_targets`: worst-first ordering
  (untested before stale, oldest-first), per-client cap enforced, ACTIVE-test exclusion,
  empty/healthy input → no targets.
- `test_smart_delivery.py` — offline, fake-HTTP: create returns id; poll ACTIVE→done;
  report parses inbox%/spam%; non-2xx → typed errors; credit-error body → `CreditError`.
- Live **dry-run** validation (RETEST_ENABLED=False): run executor, confirm it selects
  sensible worst-first targets per client and logs them, spends nothing.
- Live **single real test** (opt-in, one credit): enable for ONE client cap=1, run, confirm
  a test is created, polled to completion next run, result merged into a scratch
  placement_results, workbook reflects it. Then set cap/enable to the agreed values.

## Rollout

1. Build client + store + target selector + executor, all tests green. Ship with
   `RETEST_ENABLED=False` (dry-run only). Add the cron (dry-run).
2. Review a dry-run's target list. Adjust caps/threshold.
3. Enable for ONE client, cap=1, run one real test end-to-end, verify merge + self-heal.
4. Enable remaining clients at agreed per-client caps. Monitor credit burn via dashboard.

## Open Items (user input at implementation)

- **Per-client daily caps** — the exact number per client (default 2 each; user tunes;
  PRECISE_LEADS has 92 credits confirmed, others dashboard-only).
- **Inbox threshold** — inbox% at/above which a test counts as "inbox" (default 80).
- **Cron time** — when the executor runs (after the daily sync).
