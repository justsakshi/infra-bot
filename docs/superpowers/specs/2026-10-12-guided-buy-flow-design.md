# Guided buy flow: suggested domains → provider → inbox mix → volume → stage

Date: 2026-10-12. Approved in chat by Manveen (option 1, Slack modal).

## Goal
One guided path in Slack from the domain suggestions to a staged order, the same shape for ScaledMail and Zapmail, simple enough for Anjali or Avinash without knowing the plumbing. Volume is the RESULT: pick domains and inbox types, see what that sends and costs, then stage.

## Decisions (from the user)
- Volume is shown as a result of the chosen mix (not a target the flow solves for).
- Volume per inbox per day: Google 20, Outlook 8, SMTP 8 (ScaledMail's own calculator numbers; our Google standard). Zapmail Outlook 8 until placement data says otherwise.
- Outreach runs on weekdays: a month = 22 sending days. Show per day and per month.
- Always 3 mailboxes per domain where there is a choice (Google on both providers, Zapmail Outlook). ScaledMail Outlook is fixed at 25 per domain, SMTP at 4.
- Zapmail: Google and Outlook only (no SMTP), both $3.25 per mailbox per month.
- Layout per domain, like ScaledMail's own "Domain Count" calculator: one row per inbox type with − / + domain count and inboxes, per day, per month, $/mo.
- Built inside the existing Slack suggestions (not a web page).

## The offer table (`smartlead_sync/inbox_offer.json`, single source for JS and Python)

| Provider | Type | Inboxes/domain | Per inbox/day | Price | Per domain/day | Per domain/month (22 d) | $/domain/mo |
|---|---|---|---|---|---|---|---|
| ScaledMail | google | 3 | 20 | $3.50/mailbox | 60 | 1,320 | 10.50 |
| ScaledMail | outlook | 25 | 8 | $50/domain | 200 | 4,400 | 50.00 |
| ScaledMail | smtp | 4 | 8 | $3.75/domain | 32 | 704 | 3.75 |
| Zapmail | google | 3 | 20 | $3.25/mailbox | 60 | 1,320 | 9.75 |
| Zapmail | outlook | 3 | 8 | $3.25/mailbox | 24 | 528 | 9.75 |

Also in the file: `sending_days_per_month: 22`, `setup_days` (ScaledMail 3 business days, Zapmail 1), `warmup_days: [21, 30]`. Domain registration is added at the price the suggester showed for each name. `scaledmail_orders.PRICE` reads this file instead of its own constants (the old order form's text labels stay as they are).

## Flow (one modal, redrawn with `views.update`)
Entry: the suggestions message keeps its tick boxes; its buy buttons become one **Buy these** (plus *Suggest again*). The ticked names and the client go into the modal's `private_metadata`. Fewer than 1 ticked → ephemeral "tick at least one domain".

1. **Provider.** Two buttons, each with one line: ScaledMail (Google / Outlook / SMTP; ScaledMail sets up inboxes; support uploads them to Smartlead) and Zapmail (Google / Outlook; the bot creates inboxes and connects them to Smartlead). Names not available on ScaledMail (suggestion `scaledmail.available == false`) are listed as "will be left out" under the ScaledMail button.
2. **Inbox mix.** One row per type of the chosen provider: label, "N inboxes/domain · X/day · Y/mo · $Z/mo per domain", − / + buttons and the current count. The first row starts with every usable ticked name; + on a row takes a name from the last non-empty other row, − gives one back to the first row (counts always sum to the usable ticked names). Each row lists its domains (tick order). Total block: domains, inboxes, ~emails/day, ~emails/month, first charge (domain registrations + first month), monthly, and "ready to send around <date>" = today + setup_days business days + warmup_days (shown as a date range).
3. **Confirm** (same modal, submit button **Stage**): senders (text input, prefilled from `inbox_profiles.json` `default_senders`) and redirect (prefilled from the new `redirect` in `domain_clients.json`; Precise Leads → https://preciseleads.in). On submit:
   - ScaledMail: one `scaledmail_cli.py stage` per non-empty row (one provider per plan), `--per-domain 3` for Google. Each result posts the existing plan card (`stagedBlocks`) with **Place order** for approvers.
   - Zapmail: one inbox job per non-empty row via a new `inbox_jobs.py --create --kind new --provider GOOGLE|MICROSOFT --domains ... --per-domain 3 --names ... --client ...`, posted with the existing job approve card.
   - Errors from staging (taken name, ceiling) are posted per row; other rows still stage.

## Components
- `smartlead_sync/inbox_offer.json` (new): the table above.
- `buy_flow.js` (new): pure functions `rowsFor(provider)`, `rowMath(row, n)`, `split(names, counts)`, `totals(...)`, `readyDates(today, provider)`, `rebalance(counts, row, delta)`, and block builders `providerView`, `mixView`, `confirmView`; Bolt handlers for `buy_open`, `buy_provider`, `buy_plus`, `buy_minus`, `buy_submit`. State lives in `private_metadata` (client, names with prices, provider, counts).
- `domain_suggest_command.js`: replace the two buy buttons with `buy_open`; register `buy_flow` in `startDomainsApp`.
- `smartlead_sync/inbox_jobs.py`: `--create` for kind `new`. Slack today only creates `prewarmed` jobs; reuse the job model, its approval card and its cost estimate.
- `smartlead_sync/domain_clients.json`: `redirect` per client.
- Old paths stay: `/domains sm` order form, Zapmail batches, quote.

## Safety (unchanged)
Staging is free and writes ledgers only. ScaledMail placing: approver click + `SCALEDMAIL_ALLOW_SPEND`. Zapmail: job approval + `ZAPMAIL_ALLOW_SPEND`. The modal refuses a mix that does not cover the usable ticked names. No inbox passwords anywhere.

## Testing
- Node test (`test_buy_flow.js`, like `test_scaledmail_command.js`): offer table loads; row math for all 5 rows (e.g. ScaledMail Google 7 domains = 21 inboxes, 420/day, 9,240/mo, $73.50/mo); split and rebalance keep the sum; totals; ready dates skip weekends; views stay under Slack's 100-block limit for 20 names; private_metadata under 3,000 chars for 20 names.
- Pytest: `inbox_jobs.py --create` makes a pending `new` job with the right fields and never spends; `scaledmail_orders` prices come from the offer file.
- Live: one Slack dry run that stages (no place) for one ScaledMail and one Zapmail mix, then drop the plans / cancel the jobs.

## Out of scope
A web page; volume targets ("I need 1,500/day"); auto-placing; changes to the existing order form and quote.
