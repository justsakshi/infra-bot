# Placement Testing Loop — Design

Date: 2026-09-11
Status: approved design, pending implementation plan

## Problem

Placement testing exists in code but the loop does not close.

`retest_executor.py` Pass A writes results to Mongo (`_results`) and to a flat
"API Tests" tab. Target selection reads a *different* place: the per-client
deliverability grid, via `DeliverabilityReader` → `build_health_rows` →
`select_targets`. Nothing writes back into that grid.

Consequences, verified by reading the code on 2026-09-11:

1. `select_targets` filters on `top_problem in {untested, stale}` and applies a
   7-day floor from `row["test_date"]`. That field comes from the sheet grid.
   Since results never reach the grid, `test_date` stays blank,
   `_tested_within_floor` returns `False`, and the same inboxes are re-selected
   every run — capped only by `RETEST_PER_CLIENT_DAILY_CAP`.
2. Coverage never advances past the first N inboxes the ranker picks.
3. Results are invisible to the health view and campaign desk, which read the
   grid.

`RETEST_ENABLED` is `false`, so this has never run live. Turning it on as-is
would burn credits daily on the same two inboxes per client and change no
decision.

## What was verified live (2026-09-10)

A manual end-to-end test was run to establish that the API layer works:

- Test id 531653, sender `aaron@onebettrdata.com`, routed through BettrData's
  standing "Deliverability Test Campaign" (3752513, sequence step 10028994).
- Result: 100% inbox, 0% spam. Office365 7/7, G Suite 2/2. Worst-provider 100%.
- Elapsed ACTIVE → COMPLETED: 4172s (69.5 min), measured at 3-minute poll
  granularity.
- `create_test`, `poll_test`, `get_report` and the worst-provider judging all
  worked against the real API with no code changes.

Two facts established by failure during that run:

- `create_test` returns HTTP 400 `"Sender email accounts X not used in the
  campaign"` unless the sender is already attached to the campaign. The attach
  step is mandatory.
- `SmartDeliveryClient.__init__` takes `(api_key)` only.

## Decisions

| Decision | Choice |
|---|---|
| Source of truth for scheduling | Mongo. Sheet is an output. |
| Rotation record | Mongo; sheet stays domain-level |
| Domain filter | Skip `replaced` and `prewarmed (cancel)` |
| Sending path | Standing test campaign only; never live campaigns |
| Copy freshness | Pull from live campaigns (read-only), write to test campaign |
| Pacing | Fire all Thursday, poll across following days |
| Manual trigger | CLI script |
| Sheet writes | Blank cells only, never overwrite |
| Failure posture | Fail safe, restore warmup, report, resume next run |
| Verification | Dry-run → duplicated tab → live on BettrData only |

### Why the sheet is not the source of truth

The grid is human-maintained and hand-edited. Making it load-bearing means a
formatting change breaks scheduling. Mongo holds the schedule; the sheet is
written to as a report for the team.

One deliberate exception: the **status column** (`old` / `new` / `prewarmed` /
`prewarmed (cancel)` / `replaced`) is read from the sheet. It is a curation
decision the team owns and has no other home.

### Why not live campaigns

Routing tests through live campaigns would require attaching inboxes as senders
to campaigns actively sending to real prospects, changing their sender roster.
The upside — testing the exact copy an inbox really sends — does not justify
the exposure. All tests route through the account's standing test campaign,
matched on `RETEST_TEST_CAMPAIGN_KEYWORD`. This is what
`_test_campaign_fallback_args()` already does; it stops being a fallback and
becomes the only path.

Copy freshness is achieved separately, without sending through live campaigns:
live copy is *read* and written into the *test* campaign.

## Sheet structure (verified)

Per-client tabs in sheet `Deliverability Test results`. BettrData tab, 38 rows:

- Row 0: dates, in merged pairs — `24 June`, `1 July`, ..., `2 September`
- Row 1: `Domains | ESP | Purchased On | <status> | G suite | Outlook | G suite | Outlook | ...`
- Rows 2+: one row per domain, values `Inbox` / `spam` / blank

36 domain rows: 11 `replaced`, 6 `prewarmed (cancel)`, 4 `prewarmed`, rest
`old`/`new`. Testable set is ~19.

The G suite / Outlook pair is the **seed provider**, not the sending ESP —
`onebettrdata.com` is a Google domain with values in both columns. This maps
directly onto `get_report()`'s `by_provider` keys (`G Suite`, `Office365`).

Observed signal the current blended view hides: `heybettrdata.com` has been
`Inbox` / `spam` for four consecutive weeks — healthy at Google, failing at
Microsoft.

## Components

### 1. Schedule store — `placement_schedule` (Mongo)

One document per client+domain:

```
{client, domain, inboxes: [...], last_tested_email, last_tested_date,
 last_result, last_by_provider, rotation_index, consecutive_fails,
 last_campaign_id, last_copy_hash}
```

- `rotation_index` advances **on a recorded result**, not on test creation. A
  failed create leaves the index unmoved so that inbox is retried, not skipped.
- `inboxes` is re-synced from Smartlead each run; the index is clamped to the
  current list length. Deleting or adding an inbox must not corrupt rotation.
- `consecutive_fails` drives escalation.

### 2. Selection

Per client, weekly:

1. Read the client tab once for the status column → testable domain set.
2. Pull live inboxes from Smartlead, group by domain, apply `is_excluded_inbox`.
3. Join against `placement_schedule`. Due if never tested or last tested
   ≥ `PLACEMENT_INTERVAL_DAYS` (7) ago.
4. Per due domain, select `inboxes[rotation_index % len(inboxes)]`. Skip if that
   inbox already has an ACTIVE test.

No per-client daily cap on the weekly run. Instead a **credit pre-check**: if
the account cannot cover the batch, test worst-first (highest
`consecutive_fails`, then oldest `last_tested_date`) and report the shortfall.
A credit shortage degrades into "we tested what mattered most", not an
arbitrary prefix.

The 7-day floor stays and now functions, because `last_tested_date` is written
by us and cannot be blank.

### 3. Copy refresh

Weekly, before the batch:

1. Read the client's newest ACTIVE live campaign, step 1, `sequence_variants[]`.
   Read-only. Live campaigns are never written to.
2. Snapshot the test campaign's current sequence to Mongo (revert path).
3. Write the pulled variants into the **test campaign** via a new
   `save_campaign_sequence_variants()` that accepts a variant list.
4. Skip entirely if resolution is ambiguous or pulled copy is empty. Never write
   a degraded sequence.
5. Record source campaign id and a copy hash for correlation.

**Hazard:** the existing `save_campaign_sequences()` (api.py:242) POSTs a whole
sequence array containing a single variant A. Calling it against the test
campaign would destroy its 4 existing variants. It must not be reused here, and
must be left untouched in case other code depends on its behavior.

Content is judged only after authentication and reputation pass, so copy
freshness is a second-order signal — but spam-word and link changes do matter.

### 4. Test execution

Per selected inbox, reusing the verified 2026-09-10 path:

1. Attach the inbox to the standing test campaign
   (`add_campaign_email_accounts`). Already-attached errors are benign.
2. Optionally disable warmup (`RETEST_DISABLE_WARMUP`), recording the ids.
3. `create_test(campaign_id, sequence_mapping_id, sender_emails=[the one
   inbox], ...)`. Single sender only — one test = one inbox = one domain row.
4. `store.record_created(...)` with `warmup_off_ids`.

`sequence_mapping_id` is the sequence **step's** top-level `id`, not a variant
id — confirmed against `_campaign_launch_args()` and the live test.

### 5. Result handling (Pass A, existing, extended)

Existing behavior is correct and is kept: poll, judge on worst provider,
save result, restore warmup **before** marking done, keep the test ACTIVE
carrying only failed restore ids so the next run retries.

Extended to also:
- Update `placement_schedule` (last tested, result, advance `rotation_index`,
  update `consecutive_fails`).
- Write to the per-client grid via the new writer.

### 6. Grid writer — `placement_grid.py`

1. Parse rows 0–1 for the date row and `G suite`/`Outlook` label row. This
   parsing is **extracted and shared** with `DeliverabilityReader` so reader and
   writer cannot disagree about the format.
2. Find this week's column pair by date label; if absent, append two columns and
   write headers.
3. Find the domain row by **exact** lowercased match on column A.
4. Write only if the target cell is blank. Never overwrite.
5. `by_provider["G Suite"]` → G suite column; `by_provider["Office365"]` →
   Outlook column. Value `Inbox` if that provider's `inbox_pct >=
   RETEST_INBOX_THRESHOLD` (80), else `spam`.
6. Batch a client's writes into one `batch_update`.

Failures are non-fatal and logged, matching `append_api_result`. A sheet problem
must never strand a test or leave warmup off. Mongo is the durable record.

Highest-risk bugs to guard: duplicate column pair for one date, and wrong-row
writes where domains share prefixes (`bettrdatas.com` vs `thebettrdatas.com`).
Exact match only.

### 7. Entry points

**Weekly:** `index.js` cron, Thursday. Fires the batch. The existing daily
`retest_executor` Pass A polls and writes results as they land (Thu/Fri).

**On-demand:** `placement_test.py --client X --inbox Y`, plus `--domain` (tests
next-in-rotation) and `--dry-run`. Same selection, store and writer as the
weekly job, so the manual path cannot drift from the automated one.

### 8. Insights

- **Per-provider divergence** — domains healthy at one provider and failing at
  the other. This is the failure the worst-provider rule exists for, promoted
  from something noticed by eye to a first-class alert.
- **Consecutive-fail escalation** — 1 note, 2 warn, 3 recommend pulling.
- **Fleet coverage** — % tested in last 7 days; inboxes never tested.
- **Copy correlation** — placement change against copy hash change.

Delivered as a Slack digest after the Thursday batch, via
`smartlead.notify.post_digest`.

## Verification plan

1. Unit tests on the grid writer against fixture sheets: new column creation,
   existing column reuse, blank-only writes, exact row matching, missing domain,
   malformed header.
2. Rotation tests: advance-on-result, no-advance-on-failure, inbox list changes,
   index clamping.
3. `--dry-run` against real BettrData data. Logs intended writes, touches
   nothing.
4. Duplicate the BettrData tab; run for real against the copy; diff.
5. Enable live for BettrData only. Observe one Thursday. Then other clients.

## Open risks

**Seed sample size.** The live test drew 9 seeds (7 Microsoft, 2 Google). Two
Google seeds means a single bad landing reads as 50% Google. Whether seed count
is fixed or plan-dependent is unknown, and it bears directly on whether an 80%
per-provider threshold is meaningful. To be determined during implementation.

**Concurrency.** Only one test has ever been run at a time. ~19 concurrent tests
per client may be rate-limited or queued, stretching completion past 70 minutes.
The fire-all-then-poll design tolerates this, but it should be verified with a
3-domain batch before running the full set.

**Credit cost per full run.** ~19 domains per client per week. Cost per test in
credits is not yet established and should be confirmed before enabling.

## Explicitly out of scope

- Fleet register unification (handoff doc D2/D5). This design is a stepping
  stone toward it, not a replacement.
- Any change to `save_campaign_sequences()`.
- Any write to a live campaign.
- Enabling other executors (bounce-protect, warmup, rotation).
