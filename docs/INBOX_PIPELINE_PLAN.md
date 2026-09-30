# Inbox pipeline — plan (29 Sep 2026)

Goal: the team buys domains and inboxes, sets them up and keeps them healthy
from one place, with as few manual steps as possible. Slack first (it exists),
then the website on the same engine.

## What one "setup job" does

```
pick → [buy domain] → [buy inbox slots] → create inboxes → wait ACTIVE
     → into Smartlead → name + signature + limits + warmup → tracker → done
```

Three ways to start a job:

| Start | Skips |
|---|---|
| **New domain** (suggested name) | nothing |
| **Domain we already own** | buying the domain |
| **Pre-warmed** (pick from Zapmail's for-sale list) | buying the domain, creating inboxes, 3-4 weeks of warmup |

Each job is saved in the database step by step. A restart or crash resumes where
it stopped; a paid step is never run twice (same ledger rule as domain buying).
The job can run **now** or be **scheduled** for a date.

## Steps and what each touches

| Step | API | Money |
|---|---|---|
| Buy domain | Zapmail `POST /domains/buy` (existing, ledgered) | yes |
| Buy inbox slots (only if none free) | Zapmail `POST /wallet/buy-addon-mailboxes?quantity=N` | yes |
| Buy pre-warmed plan (only if no free pre-warmed slot) | Zapmail `POST /prewarmed-domains/purchase?planType=` | yes |
| Assign pre-warmed domain | Zapmail `POST /prewarmed-domains/assign {domainIds}` | uses slot |
| Create inboxes | Zapmail `POST /mailboxes` (max 5 per domain) | uses slot |
| Into Smartlead | BettrData: Zapmail export (connected). Precise Leads + Melior: add directly with the inbox's app password (option 2). Belardi Wong: Zapmail export (its own account). | no |
| Name, signature, daily cap, warmup | Smartlead email-account update + warmup (policy: warmup always on, 40/day ramp, reply rate 25-30%) | no |
| Melior inboxes filed under the Melior client | Smartlead `client_id` | no |
| Tracker | existing Zapmail → /infra sync (inbox expiry from Zapmail `expireOn`) | no |

## Maintenance actions (on any existing inbox)

- **Change name**: Zapmail first/last name + Smartlead sender name together. The address
  (username) is NOT changed on a warmed inbox — a new address starts reputation from zero.
- **Change signature**: Smartlead, one inbox or all of a client's inboxes at once.
- **Expiry**: tracked automatically; the Daily Renewal Check already lists inboxes.

## Safety (same rules as today)

- One approval per job, showing the total cost before anything is bought.
- Spending still needs `ZAPMAIL_ALLOW_SPEND=true` on the server.
- Wallet checked first; a short wallet stops the job with a plain message, never an unpaid invoice.
- Everything tested against a fake Zapmail + Smartlead (every step, crash at every
  step, double run) before the first real run — and the first real run is ONE cheap
  domain / ONE inbox, with you watching.

## Build order

1. ✅ **Smartlead setup step** (name, signature, warmup, client) — `inbox_setup.py` + Slack
   "Name & signature" button on a domain look-up. Works on existing inboxes. No money.
2. ✅ (engine, fakes) **Option 2** — Precise Leads / Melior inboxes added to Smartlead directly
   (Google: app password). Needs its first live run on a real new inbox.
3. ✅ (engine, fakes) **Buying** — `inbox_jobs.py` jobs: new domain / owned domain, slots,
   create, Smartlead, setup, tracker; one approval per job; now or `--run-on`; ticked every
   10 min by Infra Bot, creator DM'd on each change. New domains can be Google or Outlook (Zapmail
   confirmed the provider header on purchases).
4. ✅ (engine, fakes) **Pre-warmed** — a free pre-warmed slot (buys a starter plan, $39 then
   $24/month, only when none is free), then a free assign of the chosen domain, then 2-3.
   Slack button live.
   Also: failed inboxes are retried once automatically; inboxes can be retired at renewal
   (Slack + `zapmail_inboxes.py`); jobs move as soon as Zapmail reports a change (webhooks).
   First real run of 2-4: ONE cheap job with you watching.
5. **Website** — login first, then Renewals / New domains / Set up inboxes / Health pages on
   the same engine.

## Needed from the team

1. Sender names per client (or "pick realistic names for me").
2. Signature template per client (name, title, company, website, phone?).
3. Approval: one click per job (showing total cost) — OK?
4. Google or Outlook per client (or a mix, e.g. 2 Google + 1 Outlook per domain).
