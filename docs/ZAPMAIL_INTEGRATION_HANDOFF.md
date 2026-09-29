# Zapmail Integration — Handoff

Bring a fresh session up to speed on the Zapmail work in infrabot without
re-deriving it. Paste the "Prompt" section to Claude Code, or read this file
directly.

Repo: `C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot`
Do **not** touch `precise-automator`. Do **not** make any live Zapmail call
that could spend money (purchases / wallet / credits) unless a human explicitly
approves a specific payload.

Last updated: 2026-09-28 (spend-path hardening pass — see "Changelog").

---

## Prompt (paste to Claude Code)

```
We're integrating Zapmail into infrabot (the cold-email fleet tool at
C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot). All work is in
that repo. Do NOT touch precise-automator, and do NOT make any live Zapmail
API call that could spend money — purchases/wallet/credits are off-limits
unless a human explicitly approves a specific payload.

Read infra-bot/docs/ZAPMAIL_INTEGRATION_HANDOFF.md first. Summary:
- ZapmailClient (smartlead_sync/smartlead/zapmail.py) wraps ~55 endpoints,
  classified READ / WRITE / SPEND. WRITE needs approve=True; SPEND needs
  approve=True AND ZAPMAIL_ALLOW_SPEND=true, enforced inside the client.
- Domains are bought ONLY via zapmail_buy.py --execute <batch_id> (ledgered,
  staggered, live price re-check, strict account). Slack and domain_generator
  --buy only STAGE.
- Tests: .venv\Scripts\python.exe -m pytest test_zapmail_infrabot.py
  test_zapmail_spend_guards.py test_zapmail_integration.py
  test_domain_naming.py -q  (146 pass). Never run bare pytest over the
  directory (test_smart_delivery.py self-executes).

NEXT STEPS WHEN ASKED:
1. Zapmail support answers (docs/ZAPMAIL_QUESTIONS_2026-09.md): correct
   price parsing, available-bulk status enum, buy/export response shapes.
2. Real client->account map: set ZAPMAIL_CLIENT_ACCOUNTS, the
   ZAPMAIL_API_KEY_<NAME> keys, and ZAPMAIL_PRIMARY_ACCOUNT_NAME.
3. Microsoft/Outlook mailbox support (team needs Outlook inboxes to match
   Outlook leads; everything today defaults to GOOGLE).
4. Delayed purchase: evaluated 28 Sep, not found (see
   docs/ZAPMAIL_DELAYED_PURCHASE_EVAL_2026-09-28.md). Awaiting Zapmail Q18.
5. Scheduler wiring for daily loops (renewal alerts, placement sweeps).
```

---

## What is built

### The end-to-end flow

```
suggest names ──> stage plan ──> buy one batch/day ──> connect ──> mailboxes ──> export to Smartlead
/domains          /domains buy   zapmail_buy.py        zapmail_lifecycle.py      zapmail_export.py
domain_generator  zapmail_buy    --execute (CLI only)  --connect   --mailboxes   --export
                  --plan
```
Plus fleet ops: `/zapmail status|renewals|cross-check`, renewals/tags
(`zapmail_maintenance.py`), placement tests (`zapmail_placement.py`).

### Modules (`smartlead_sync/smartlead/`)
- `zapmail.py` — `ZapmailClient`. Auth (`x-auth-zapmail`), optional
  `x-workspace-key`, `x-service-provider` only where required, retry/backoff
  for reads, single-shot SPEND. Error classes: `ZapmailHTTPError` (4xx —
  definitely not processed), `ZapmailOutcomeUnknown` (network/timeout/5xx —
  may have been processed), `ZapmailSpendBlocked` (a gate refused).
  `spend_allowed()` lives here.
- `zapmail_accounts.py` — accounts from `ZAPMAIL_API_KEY[_<NAME>]`; client →
  account map (`DEFAULT_CLIENT_ACCOUNTS`, overridden by
  `ZAPMAIL_CLIENT_ACCOUNTS="Client=Account,..."`). `strict=True` resolution
  (used by every spend) never falls back to the primary account.
  `ZAPMAIL_PRIMARY_ACCOUNT_NAME` names the plain `ZAPMAIL_API_KEY` account.
- `domain_availability.py` — bulk availability (20 names/request, 10 req/30
  min), DNS pre-filter, blacklist check, disk cache (bulk-sourced rows only;
  unknowns not cached; `use_cache=False` for live checks).
- `domain_ai.py` — AI Domain Finder + site vocabulary scrape.
- `domain_purchase.py` — `stage_purchase` (read-only), `verify_prices` (live
  re-check), `execute_purchase` (all gates; call via `domain_batch` only).
- `domain_batch.py` — THE buy path: `stage_batches`, `execute_one`,
  `reconcile_one`, `BatchStore` (Mongo `zapmail_domain_batches`).
- `domain_lifecycle.py` — `connect_and_wait`, `assign_mailboxes_and_wait`
  (skips existing usernames; `sender_names` for real senders).
- `domain_export.py` — export by exact mailbox ids to one pinned third-party
  account.
- `zapmail_fleet.py`, `zapmail_renewals.py` — read-only fleet status,
  renewals, tracker cross-check.
- `zapmail_maintenance.py` — renewal preview (read), `renew_now` (SPEND,
  gated), auto-renew + tags (WRITE, gated).
- `zapmail_placement.py` — placement status/report (read), `run_test` (SPEND,
  gated).

### CLIs (`smartlead_sync/`)
| CLI | Read-only | Gated |
|---|---|---|
| `zapmail_buy.py` | `--plan`, `--list`, `--reconcile <id>` | `--execute <id> --approve` (+ kill-switch) |
| `zapmail_lifecycle.py` | `--ns`, `--connect-status` | `--connect`, `--mailboxes [--names "A B,C D"]` |
| `zapmail_export.py` | `--accounts`, `--status` | `--ensure-account`, `--export [--third-party-account-id]` |
| `zapmail_maintenance.py` | `--renewals` | `--renew` (+ kill-switch), `--auto-renew`, tags |
| `zapmail_placement.py` | `--status`, `--eligible`, `--report` | `--run` (+ kill-switch) |
| `zapmail_status.py` | `--status`, `--renewals`, `--cross-check` | — |
| `domain_generator.py` | suggestions, `--buy` (stage only; `--approve` is refused) | — |

### Slack
- `/domains ...` — suggestions; `/domains ai ...`; `/domains buy <domains>
  client=X` stages a ledgered plan with batch ids and dates. Never buys.
  Client and domain arguments are whitelisted so nothing can be read as a
  CLI flag.
- `/zapmail status|renewals|cross-check` — read-only.

---

## Infrabot wiring (2026-09-28)

### Slack `/zapmail` (read-only, all accounts, Google + Outlook)
| Command | Shows |
|---|---|
| `/zapmail status` | Wallet, auto-recharge, plan, domains + active mailboxes per provider, placement credits |
| `/zapmail renewals` | Domains expiring in 2 months (both providers); says so if an account could not be checked |
| `/zapmail cross-check` | Every Zapmail domain vs the /infra tracker: untracked, not-on-Zapmail, date mismatches |
| `/zapmail domain x.com` | Which account/provider has it, status, expiry, auto-renew, each mailbox |
| `/zapmail batches` | The staggered purchase plan (ledger) and what is due |
| `/zapmail digest` | Today's action list |

### One-click domain suggestions (`/domains`)
`/domains` → a **Suggest for <client>** button per client in
`smartlead_sync/domain_clients.json` (site + 3-8 keywords; Melior not set up
yet). Click → Zapmail AI Domain Finder (2 keyword windows) + our generator,
screened against naming rules, everything we own (tracker + Smartlead + every
Zapmail account), blacklists, price ceiling → tick boxes (~90s) → **Stage
purchase** → staggered batch plan → **Buy now** on due batches, shown only
when `ZAPMAIL_BUY_APPROVERS` (Slack user ids) is set, confirm dialog, and
still refused by Python unless `ZAPMAIL_ALLOW_SPEND=true` + every purchase
check passes. `/zapmail batches` shows the same Buy button on due batches.
CLI: `domain_suggest.py --client Bettrdata`. Live results:
docs/ZAPMAIL_LIVE_TEST_2026-09-28.md. Free-feature self-test:
`zapmail_selftest.py` (77/79 OK).

### Automation added 2026-09-29 (so nobody updates /infra by hand for Zapmail)
- **Tracker sync** (`zapmail_asset_sync.py`, cron 09:30 IST, Slack `/zapmail sync`):
  Zapmail → /infra `assets`. Expiry dates follow Zapmail; status only
  downgrades (never re-activates); blanks filled, team fields never
  overwritten; new rows only for current clients (no owner/channel → no
  reminder spam); nothing deleted. Dry run unless `--apply` /
  `ZAPMAIL_ASSET_SYNC_ENABLED=true`; applying re-syncs the tracker sheet.
  First live preview: 10 new Melior domains, 58 missing expiry dates filled,
  66 inbox expiries refreshed, 5 lapsed OPSC domains + 3 BettrData inboxes
  (thebettrdatas.com) marked Inactive. ScaledMail stays manual.
- **Webhooks** (`zapmail_webhooks.js`, route `POST /webhooks/zapmail/<account>`,
  mounted before `express.json`): HMAC-verified (`X-Zapmail-Signature`,
  5-min tolerance), answered at once, de-duplicated; alerts (mailbox FAILED,
  domain status change / health ≤30, export failed, placement test done,
  billing change) to `ZAPMAIL_NOTIFY_CHANNEL`; domain/mailbox changes refresh
  that domain in /infra. Register per account with
  `zapmail_webhooks.py --register https://<render-host>/webhooks/zapmail/<account> --client X --approve`
  and store the one-time secret as `ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT>`.
- **Slack** (`zapmail_actions.js`): `/zapmail` alone = home menu (status,
  renewals, purchase plan, digest, suggest domains, look up a domain (form),
  pre-warmed, tracker sync, cross-check). Look-up shows client, provider,
  health, mailboxes + buttons **Set up mailboxes** (form: count, real sender
  names), **Export to Smartlead**, **Auto-renew on/off**, **Update tracker**.
  Renewals: current clients get **Renew / Auto-renew on / Look up**; past
  clients listed as lapsing. Buttons only for current clients; approvers only
  (`ZAPMAIL_APPROVERS`); renew still needs `ZAPMAIL_ALLOW_SPEND=true`.
- **Client ownership** in one place (`zapmail_clients.py`): tracker client →
  Belardi Wong account → brand in name → else unassigned.
- **Pre-warmed** view: our slots (PL: Google 15/15 $105/mo, Outlook 12/12
  $84/mo — ~$7/mailbox/mo, all used) + Zapmail stock (476 Google / 71 Outlook).
- Digest now lists only current-client expiries; past-client lapses are a count.
- Inbox sheet `# Campaigns` now counts ACTIVE+PAUSED campaigns only (Campaign
  Desk's shared-inbox split reads it) — deploy with Campaign Desk.
- Tests: Python 192 (7 files) · JS 13 (`test_zapmail_actions.js`) + 10.

### Daily digest (cron 09:40 IST, `zapmail_digest.py`)
Wallets (flags low balance with auto-recharge off), domains expiring within
14 days, batches due or needing reconcile, unreachable accounts. **Posts only
when `ZAPMAIL_NOTIFY_CHANNEL` is set** — off by default, like the health digest.

### One-step provisioning (CLI)
`zapmail_lifecycle.py --provision a.com --client "Precise Leads" [--names "Jane Doe,John Roe"] [--provider MICROSOFT] [--export] [--approve]`
→ connect (skipped if live) → mailboxes (top-up, skips existing names) →
export (opt-in). Dry run by default; stops at the first failed step.

### Routing rules
- Every client-scoped action uses the client's **own** Zapmail account
  (`open_client` / `require_account`). Unmapped clients are refused — no
  fallback to another client's account. Only domain availability and AI name
  suggestions (account-agnostic) may use any key.
- **Both providers:** Zapmail returns Google data only unless asked for
  `MICROSOFT` (verified live). Fleet reads query both; domain actions detect
  the domain's provider first.
- **Export targets are per client, never guessed.** Set
  `ZAPMAIL_EXPORT_TARGETS="Client=<thirdPartyAccountId>,..."` (ids from
  `zapmail_export.py --accounts --client X`) or pass
  `--third-party-account-id`.

### Environment (local `.env` done; Render still needs these)
| Var | Value / meaning |
|---|---|
| `ZAPMAIL_API_KEY` | Belardi Wong's Zapmail key (the plain key) |
| `ZAPMAIL_PRIMARY_ACCOUNT_NAME` | `Belardi Wong` |
| `ZAPMAIL_API_KEY_PRECISE_LEADS` | Precise Leads' Zapmail key (main fleet) |
| `ZAPMAIL_CLIENT_ACCOUNTS` | optional override, e.g. `Bettrdata=Precise Leads` once confirmed |
| `ZAPMAIL_EXPORT_TARGETS` | optional, per-client Smartlead target in Zapmail |
| `ZAPMAIL_NOTIFY_CHANNEL` | optional — set to turn the daily Slack post on |
| `ZAPMAIL_APPROVERS` | comma-separated Slack user ids allowed to press any Zapmail action button (buy, mailboxes, export, auto-renew, renew, tracker apply). Empty = views only. (`ZAPMAIL_BUY_APPROVERS` still accepted.) |
| `ZAPMAIL_ASSET_SYNC_ENABLED` | `true` = the 9:30 cron and webhooks WRITE the /infra tracker; unset = preview only |
| `ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT>` | signing secret per Zapmail account (`…_PRECISE_LEADS`; plain `ZAPMAIL_WEBHOOK_SECRET` for Belardi Wong) — from `zapmail_webhooks.py --register` |
| `ZAPMAIL_ALLOW_SPEND` | leave UNSET |

### Live facts (read-only checks, 2026-09-28)
- Belardi Wong: wallet $0, auto-recharge off; 22 Google + 6 Microsoft domains;
  48 + 18 mailboxes; 25 placement credits; Smartlead export target
  saml@belardiwong.com.
- Precise Leads: wallet $64, auto-recharge on ($50 below $25); 193 Google + 20
  Microsoft domains; 54 + 15 mailboxes; 31 placement credits; 28 domains
  renewing within 2 months (~$21 each); 2 workspaces (+ "StaffAI").
- The Precise Leads Zapmail account holds BettrData domains
  (`askbettrdata.com`, `bettrdataco.com`, …) and its only Smartlead export
  target is **amanda@bettrdata.io** (BettrData's Smartlead).
- Cross-check: 241 Zapmail domains vs 180 tracked; 167 Zapmail domains are
  not in the /infra tracker.
- `renewal-soon` rejects `null` filters with 422 (fixed: nulls omitted).

---

## Safety model

| Layer | What it enforces |
|---|---|
| Slack | Stage only. Args whitelisted (no `-`-prefixed values reach the CLI). |
| `domain_generator --buy` | Stage only; `--approve` refused with a pointer to `zapmail_buy.py`. |
| Client (`zapmail.py`) | WRITE needs `approve=True`. SPEND needs `approve=True` AND `ZAPMAIL_ALLOW_SPEND=true` — every SPEND method, no exceptions. SPEND never auto-retries. |
| Account | Spends resolve the client strictly: unmapped client or missing key → refused, never the primary wallet. |
| Purchase | Live uncached re-check: every name available, priced, ≤ $25 ceiling (`--price-ceiling`), ≤ staged price. |
| Ledger | Mongo required. Batch bought once (`purchased` refused). Batch client is authoritative (`--client` mismatch refused). `earliest_date` enforced. Atomic claim (`planned/failed → in_progress`) blocks concurrent double-buys. |
| Ambiguity | Timeout/5xx/unknown error → `unknown`, blocked until `--reconcile` reads Zapmail and settles it to `purchased` / `failed` / `partial`. Only a 4xx → `failed` (retryable). |
| Renewals / placement | Kill-switch + strict account + explicit ids (no filter-based renewals). |

Ledger statuses: `planned → in_progress → purchased | failed | unknown`;
reconcile: `unknown/in_progress/failed → purchased | failed | partial`.
`partial` stays blocked for a human.

---

## Known gaps (not bugs — not yet done)

1. **Client → account map is placeholder** until real account names/keys are
   set. Until then strict resolution refuses spends for mapped clients whose
   key is missing (by design).
2. **Response shapes: confirmed** by live tests and Zapmail support
   (2026-09-29; answers + resulting code changes at the top of
   `ZAPMAIL_QUESTIONS_2026-09.md`): NaN-safe prices, wallet must cover a buy
   (a short wallet silently issues an unpaid invoice), 100 searches / 30 min,
   NS_NOT_CHANGED + mailbox FAILED handling, Zapmail's polling intervals.
3. **Export still not trusted live.** Smartlead login (email + password)
   confirmed; where the `exportId` comes from is still unstated; re-export cap
   number (3 vs 10) unconfirmed. PL/Melior have no Smartlead target yet.
4. **Outlook mailbox buying not built.** Fleet reads cover both providers and
   `--provider MICROSOFT` connects to the Outlook side; pre-warmed M365
   mailboxes export like Google ones (price not given).
5. **Zapmail's delayed purchase is UI-only** (support, 2026-09-29): charges
   immediately, staggers only registration. Our ledger stagger stays.
6. **Scheduler:** only the read-only digest (09:40 IST) runs on a cron.
7. **Auto-renew:** API-bought domains default to `autoRenew: false` — decide
   whether provisioning should switch it on for current clients.
7. Long-tail endpoints (workspaces, billing, zapbox, webhooks, zapsites,
   subscriptions, wallet top-up) documented but not wrapped.

---

## Test note

```
cd infra-bot\smartlead_sync
.venv\Scripts\python.exe -m pytest test_zapmail_spend_guards.py test_zapmail_integration.py test_domain_naming.py -q
```
127 tests, no network/Mongo/spend. `test_zapmail_spend_guards.py` covers every
gate above. Do **not** run bare `pytest` over the whole directory:
`test_smart_delivery.py` self-executes at import time.

---

## Changelog

**2026-09-28 — spend-path hardening**
- Slack arg injection closed (`client=--approve` used to reach the buy path).
- `domain_generator --buy --approve` execute path removed; one buy path left.
- Kill-switch moved into the client for every SPEND (renew/placement/DNS
  Shield/prewarmed/aged/quick-setup previously skipped it).
- Strict account resolution for spends (no primary-wallet fallback).
- Live price re-check + ceiling + staged-price check before `/buy`.
- `earliest_date` enforced; atomic claim; `unknown` state + `--reconcile`.
- Batch client authoritative; `--client` mismatch refused.
- `renew_domains` takes explicit ids only.
- Export: exact-domain mailbox ids, pinned third-party account, stricter
  export-id extraction.
- Mailbox top-ups skip existing usernames; `--names` for real senders.
- `x-service-provider` only sent where required.
- Availability cache: bulk-sourced rows only, unknowns not cached.
- Slack staging now records a ledgered batch plan and shows batch ids/dates.
- New `test_zapmail_spend_guards.py` (46 tests).
