# Test checklist — Zapmail (Infrabot) + Campaign Desk, before pushing

Tick each box as you go. Every test says what it touches:

| Tag | Meaning |
|---|---|
| **READ** | Only reads Zapmail / Smartlead / Mongo. Safe any time. |
| **LEDGER** | Writes a *planned* purchase to the test ledger (`zapmail_domain_batches_selftest`). No money. |
| **TRACKER** | Writes the /infra asset tracker (Mongo `assets`). |
| **ZAPMAIL-WRITE** | Changes something on Zapmail (free). |
| **SPEND** | Costs money. Only with `ZAPMAIL_ALLOW_SPEND=true`. |

Nothing here commits, pushes, or deploys.

---

## 0. Setup (once per PowerShell window)

**Infrabot CLI window**
```powershell
cd C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\infra-bot\smartlead_sync
Get-Content ..\.env | Where-Object { $_ -match '^\s*[A-Za-z_][A-Za-z0-9_]*=' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item "env:$($k.Trim())" $v.Trim().Trim('"') }
Remove-Item env:ZAPMAIL_ALLOW_SPEND -ErrorAction SilentlyContinue      # spend OFF
$env:ZAPMAIL_BATCH_COLLECTION = 'zapmail_domain_batches_selftest'       # test ledger
$py = ".venv\Scripts\python.exe"
```

**Infrabot Slack window** (only /domains + /zapmail + webhooks — no crons, no main bot)
```powershell
cd C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\infra-bot
$env:ZAPMAIL_APPROVERS = "<your Slack member id, e.g. U045…>"   # to test action buttons
node dev\zapmail_local.js
```
> **No need to suspend Render.** Test on a separate dev Slack app (one-time,
> ~3 min): follow the steps at the top of `dev\slack_dev_app_manifest.yml`, put
> `DEV_DOMAINS_SLACK_BOT_TOKEN` / `DEV_DOMAINS_SLACK_APP_TOKEN` in `.env`. Then
> in every test below type **`/domains-dev`** and **`/zapmail-dev`** instead of
> `/domains` and `/zapmail`. Render keeps serving the real commands untouched.
> The harness prints which Slack app and switches (spend, tracker writes,
> approvers) are on when it starts; with no dev tokens it runs webhooks only.
>
> **Or, with Render suspended:** `$env:DEV_USE_PROD_SLACK = "true"` before
> `node dev\zapmail_local.js`, and use the real `/domains` / `/zapmail`.
> Resume Render when done: while it is off, `/infra`, the crons and the
> Campaign Desk 10:00 release do not run.

**Campaign Desk window** — the lead release must be OFF locally, or it really
sends leads at 10:00:
```powershell
cd C:\Users\Manveen\Desktop\new_things_to_mess_araound\PL_MCP_Bots\precise-automator
$env:STAGGER_SCHEDULER = 'off'; $env:STAGGER_RELEASE_ENABLED = 'false'
.venv\Scripts\python.exe -m uvicorn app.main:app --port 8000
```

---

## 1. Automated tests (5 min)

- [ ] Infrabot Python — `& $py -m pytest test_domain_quality.py test_renewals_client_filter.py test_zapmail_asset_sync.py test_inbox_campaign_count.py test_domain_suggest.py test_zapmail_infrabot.py test_zapmail_spend_guards.py test_zapmail_integration.py test_domain_naming.py -q` → **231 passed**
- [ ] Infrabot sync wiring — `& $py test_sync_wiring.py` → **ALL PASSED**
- [ ] Infrabot JS — `cd ..; node test_zapmail_actions.js; node test_domain_suggest_command.js; node test_domains_zapmail_route.js; node test_domains_access.js; node test_renewal_labels.js; node test_slack_text.js; cd smartlead_sync` → **13, 10, 7, 9, 8, 9 passed**
- [ ] Campaign Desk — `.venv\Scripts\python.exe -m pytest -q` → **1148 passed**

---

## 2. Zapmail from the command line (Infrabot CLI window)

| ✓ | # | Feature | Command | Expect | Tag |
|---|---|---|---|---|---|
| [ ] | 2.1 | Fleet status | `& $py zapmail_status.py --status` | Both accounts; wallet, auto-recharge, Google/Outlook domains + mailboxes, placement credits | READ |
| [ ] | 2.2 | Renewals | `& $py zapmail_status.py --renewals` | 28 rows (26 Google + 2 Outlook), provider column | READ |
| [ ] | 2.3 | Domain look-up | `& $py zapmail_status.py --domain askbettrdata.com` | PRECISE_LEADS, ACTIVE, 3 mailboxes | READ |
| [ ] | 2.4 | Pre-warmed | `& $py zapmail_status.py --prewarmed` | Stock + our slots (15/15 Google, 12/12 Outlook) | READ |
| [ ] | 2.5 | Tracker vs Zapmail | `& $py zapmail_status.py --cross-check` | 241 vs ~180; only-in-Zapmail / lapsed / no-expiry / only-in-tracker | READ |
| [ ] | 2.6 | Daily digest | `& $py zapmail_digest.py --no-post` | Wallets, current-client expiries only, "past-client domains lapsing" count | READ |
| [ ] | 2.7 | Free-feature self-test | `& $py zapmail_selftest.py` | 77/79 OK (2 = Smartlead fetch-workspaces, expected) | READ |
| [ ] | 2.8 | Domain suggestions | `& $py domain_suggest.py --client Bettrdata` | Up to 10 available names, each shown as its words (`catalog + mark`), Zapmail AI + generator, ~90 s; then "Dropped N Zapmail AI names" with reasons. Every name should be about the business — no flume/sluice/courier names | READ |
| [ ] | 2.9 | Suggestions, other clients | `--client "Belardi Wong"`, `--client "Precise Leads"`; `--client Melior` | Names for the first two; Melior → "needs a main_domain and 3 keywords" | READ |
| [ ] | 2.10 | Stage a purchase plan | `& $py zapmail_buy.py --plan <2 names from 2.8> --client Bettrdata` | Batches with dates + batch ids; "bills: PRECISE_LEADS" | LEDGER |
| [ ] | 2.11 | Purchase ledger | `& $py zapmail_buy.py --list` | The batch from 2.10 | READ |
| [ ] | 2.12 | Buy refused (spend off) | `& $py zapmail_buy.py --execute <batch id> --approve` | Refused: needs `ZAPMAIL_ALLOW_SPEND=true` | READ |
| [ ] | 2.13 | Buy refused (not approved) | `& $py zapmail_buy.py --execute <batch id>` | Refused | READ |
| [ ] | 2.14 | Provision dry run | `& $py zapmail_lifecycle.py --provision gomelior.com --client Melior --export` | connect skipped (already live) · "would create" names · export after mailboxes | READ |
| [ ] | 2.15 | Unmapped client refused | `& $py zapmail_lifecycle.py --connect-status x.com --client Darlean` | "no Zapmail account configured" | READ |
| [ ] | 2.16 | Export dry run | `& $py zapmail_export.py --export askbettrdata.com --client Bettrdata` | DRY RUN → 3 mailboxes → target amanda@bettrdata.io account | READ |
| [ ] | 2.17 | Export target guard | `& $py zapmail_export.py --export askbettrdata.com --client Melior` | Refused: no export target for Melior | READ |
| [ ] | 2.18 | Tracker sync preview | `& $py zapmail_asset_sync.py` | 10 new Melior domains, expiry updates, 5 OPSC + 3 thebettrdatas inboxes → Inactive | READ |
| [ ] | 2.19 | Webhook list | `& $py zapmail_webhooks.py --list` | 0 endpoints on both accounts | READ |
| [ ] | 2.20 | Renewal preview | `& $py zapmail_maintenance.py --renewals --client Melior` | Melior's 5 domains with renew price | READ |

---

## 3. Slack — read-only (Infrabot Slack window running)

> Type **`/domains zapmail …`** everywhere below. `/zapmail` itself is not
> registered in Slack (only the app owner can add it); `/domains zapmail` does
> exactly the same thing.

| ✓ | # | Try | Expect | Tag |
|---|---|---|---|---|
| [ ] | 3.1 | `/domains zapmail` | Home menu with 9 buttons | READ |
| [ ] | 3.2 | Each menu button: Fleet status, Renewals, Purchase plan, Today's digest, Pre-warmed, Tracker sync, Tracker vs Zapmail | Each posts its view (same numbers as section 2) | READ |
| [ ] | 3.3 | Look up a domain → type `askbettrdata.com` | Client BettrData, Outlook, health 90, 3 mailboxes + 4 buttons | READ |
| [ ] | 3.4 | Look up `kombinatorfunds.com` | "not a current client", health 0, **no buttons** | READ |
| [ ] | 3.5 | Look up `google.com` / `not a domain` | "not in any connected Zapmail account" / form error | READ |
| [ ] | 3.6 | Renewals view | 5 Melior rows with a ⋯ menu; 23 past-client domains listed without actions | READ |
| [ ] | 3.7 | `/domains zapmail help`, `/domains zapmail domain askbettrdata.com`, `/domains zapmail prewarmed` | Typed forms work | READ |
| [ ] | 3.8 | `/domains` | Buttons: BettrData, Belardi Wong, Precise Leads; "Melior not set up yet" | READ |
| [ ] | 3.9 | Suggest for BettrData | "Finding domains…", ~90 s later up to 10 names with tick boxes — all about the business (e.g. feedsworks, segmentline) | READ |
| [ ] | 3.10 | Tick 2 → **Stage purchase** | Plan with dates + batch id | LEDGER |
| [ ] | 3.11 | Stage with nothing ticked | "Tick at least one domain first." | READ |
| [ ] | 3.12 | **Suggest again** | New run | READ |
| [ ] | 3.13 | `/domains suggest belardi wong` | Straight to Belardi Wong suggestions | READ |

## 4. Slack — approval guards (still no changes)

| ✓ | # | Try | Expect | Tag |
|---|---|---|---|---|
| [ ] | 4.0 | Every reply in sections 3–5 says **"Only visible to you"** | Nothing is posted for the whole channel | READ |
| [ ] | 4.1 | Restart the harness with `$env:ZAPMAIL_APPROVERS=''` and `$env:DOMAINS_ALLOWED_USERS='<your id>'`, press Export / Auto-renew / Set up mailboxes | ":lock: Only Zapmail approvers…" — nothing runs | READ |
| [ ] | 4.1b | Ask a teammate NOT on either list to type `/domains` | ":lock: This bot is limited to the domains team…" — only they see it | READ |
| [ ] | 4.2 | Staged plan without approvers | "One-click buying is off" — no Buy button | READ |
| [ ] | 4.3 | With approvers set, spend OFF: **Buy now** on a due test batch | Confirm dialog → "Not bought: needs … ZAPMAIL_ALLOW_SPEND=true" | READ |
| [ ] | 4.4 | Renewals ⋯ → **Renew now** (spend OFF) | Refused by the spend switch | READ |

## 5. Slack — real changes (approvers set; pick safe targets)

| ✓ | # | Try | Expect | Tag |
|---|---|---|---|---|
| [ ] | 5.1 | Look up `gomelior.com` → **Turn auto-renew on**, then look up again → **Turn auto-renew off** | ✅ each time; look-up shows the new state. Fully reversible. | ZAPMAIL-WRITE |
| [ ] | 5.2 | Look up a current-client domain → **Update tracker** | "Tracker: N row(s) written" for that domain | TRACKER |
| [ ] | 5.3 | **Tracker sync** → **Apply to tracker** (after reading the preview) | "Tracker updated: N rows"; check /infra shows the 10 Melior domains | TRACKER |
| [ ] | 5.4 | **Set up mailboxes** on a domain with <5 mailboxes (form: 1, your name) | ⚠️ Precise Leads has **0 free purchased mailbox slots** right now, so expect Zapmail to refuse — that tests the error path. A real creation needs a free slot first. | ZAPMAIL-WRITE |
| [ ] | 5.5 | **Export to Smartlead** — only on a domain whose mailboxes are NOT already in that client's Smartlead | "Export started"; mailboxes appear in Smartlead. Skip if unsure. | ZAPMAIL-WRITE |
| [ ] | 5.6 | *(Optional, costs ~$13)* Set `$env:ZAPMAIL_ALLOW_SPEND='true'` in the harness window, restart, stage 1 cheap name, **Buy now** | "Bought … (invoice)"; `/domains zapmail domain <name>` shows PENDING → ACTIVE; wallet down ~$13 | SPEND |
| [ ] | 5.7 | *(After 5.6)* `& $py zapmail_lifecycle.py --provision <name> --client "Precise Leads" --approve` | Connect skipped, mailboxes created (needs free slots) | ZAPMAIL-WRITE |

## 6. Webhooks (harness running)

| ✓ | # | Try (new window, in `infra-bot`) | Expect | Tag |
|---|---|---|---|---|
| [ ] | 6.1 | `node dev\send_test_webhook.js mailbox-failed` | `200 ok`; harness logs the alert (Slack too if `ZAPMAIL_NOTIFY_CHANNEL` set) | READ |
| [ ] | 6.2 | `… domain-critical`, `… export-failed`, `… placement-done` | `200 ok` each; alert text in log | READ |
| [ ] | 6.3 | `… bad-signature` | `401 bad signature` | READ |
| [ ] | 6.4 | Send the same event twice quickly | Handled once | READ |
| [ ] | 6.5 | *(After deploy only)* register real webhooks: `& $py zapmail_webhooks.py --register https://<render-host>/webhooks/zapmail/precise_leads --client "Precise Leads" --approve` | Secret printed once → save as `ZAPMAIL_WEBHOOK_SECRET_PRECISE_LEADS` on Render | ZAPMAIL-WRITE |

## 7. Campaign Desk — Outlook/Gmail capacity (Campaign Desk window running)

| ✓ | # | Try | Expect | Tag |
|---|---|---|---|---|
| [ ] | 7.1 | New window in `precise-automator`: `.venv\Scripts\python.exe scripts\provider_capacity_today.py --campaign 4008470 --outlook-leads 71 --gmail-leads 57` | Per provider: inboxes, capacity/day, follow-ups due, waiting, safe new leads, add/hold (was 0 Outlook / 53 Gmail) | READ |
| [ ] | 7.2 | Same for `--campaign 3912524` (wait ~1 min between runs) | Outlook ~22, Gmail 0 | READ |
| [ ] | 7.3 | `--json` and `--buffer 0` variants | JSON / slightly higher numbers | READ |
| [ ] | 7.4 | Open http://localhost:8000/stagger → a batch on a Melior campaign | Per-provider panel: capacity, follow-ups due, waiting, released today / queued / sent, "growing" badge | READ |
| [ ] | 7.5 | Panel on a huge campaign or with `STAGGER_FOLLOWUP_ESTIMATE=false` | Falls back to follow-up % and **says so** | READ |
| [ ] | 7.6 | Dry-run release: `.venv\Scripts\python.exe -c "import json; from app.workers.stagger_release import run_stagger_release_now as r; print(json.dumps(r(dry_run=True), default=str, indent=1))"` | Per batch: would release N per provider, held surplus, method used; **nothing sent** | READ |
| [ ] | 7.7 | Check the dry run on a Saturday/Sunday date (or read the output's sending-day field) | No release on non-sending days | READ |
| [ ] | 7.8 | Compare 7.1's "safe Outlook" with what Smartlead actually sends tomorrow for that campaign | Outlook leads only from Outlook inboxes | READ |
| [ ] | 7.9 | Dry run (7.6) with the "ZZ TEST stagger" batch still active (campaign 4008562 was deleted) | That batch shows `campaign_deleted: [4008562]`, queued count, `batch_paused: false`; **no** "reply check failed … 404" | READ |
| [ ] | 7.10 | After deploy: next 10:00 run | One Slack warning: "Smartlead campaign 4008562 was deleted — N queued lead(s)… Batch paused"; batch shows paused on /stagger; no warning the day after | WRITE (our Mongo only) |
| [ ] | 7.11 | A morning where nothing is released and nothing happened | **No** Slack message (before: "0 lead(s) released") | READ |

---

## 8. Before pushing

- [ ] All boxes above ticked (or consciously skipped: 5.5–5.7, 6.5)
- [ ] Resume the Render services (or delete the "Domains (dev)" Slack app if you used it)
- [ ] Set on Render: `DOMAINS_ALLOWED_USERS` (who may use /domains — **nobody gets in without it**), `ZAPMAIL_APPROVERS`, alerts channel, tracker sync on (`ZAPMAIL_ASSET_SYNC_ENABLED=true`)
- [ ] Melior website + keywords in `smartlead_sync/domain_clients.json`
- [ ] Deploy Infrabot and Campaign Desk **together** (the `# Campaigns` fix)
- [ ] After deploy: 6.5 (webhooks), then the first real 9:30 tracker sync
- [ ] Optional cleanup: drop Mongo collection `zapmail_domain_batches_selftest`
