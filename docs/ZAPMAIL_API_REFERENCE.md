# Zapmail API Reference (complete memory)

Single source of truth for how the fleet talks to Zapmail. Everything the
tools know about Zapmail lives here and in `smartlead/zapmail.py`.

**Status:** researched 2026-09, against Zapmail's published OpenAPI docs
(`docs.zapmail.ai`, `api.zapmail.ai`). Fully-specified endpoints are in the
catalog below; a smaller set is summary-only and marked as such. No endpoint
is listed from guesswork.

---

## 1. Safety policy (non-negotiable)

| Class | Meaning | Allowed without approval? |
|---|---|---|
| **READ** | Pure retrieval. No cost, no side effect. | Yes |
| **WRITE** | Mutates the account (connect, tags, DMARC, mailbox assign, cancel). No new money, but consumes purchased quota. | No — needs `approve=True` |
| **SPEND** | Costs wallet balance / credits / creates an invoice. | No — needs `approve=True` **and** `ZAPMAIL_ALLOW_SPEND=true` |

The three layers of the guard, in order:

1. `ZapmailClient` refuse WRITE/SPEND calls unless the caller passes
   `approve=True` (a deliberate per-call keyword, never sticky state).
2. `domain_purchase.execute_purchase` additionally checks the
   `ZAPMAIL_ALLOW_SPEND` env var is `"true"` — off everywhere else (local,
   CI, Render).
3. The Slack surface (`/domains buy`) **stages only**; it renders a
   read-only price/availability plan and never executes a buy.

Nothing in this repo spends money unless a human flips the switch and confirms
the exact list.

---

## 2. Connection

- **Base URL:** `https://api.zapmail.ai/api` (paths below are relative, e.g.
  `/v2/domains/buy` → `https://api.zapmail.ai/api/v2/domains/buy`).
- **Auth header (required almost everywhere):** `x-auth-zapmail: <API_KEY>`.
  Key location: Zapmail Dashboard → Settings → Integrations → API. Requires the
  Pro plan for API access.

### Optional scoping headers

| Header | When |
|---|---|
| `x-workspace-key` | Operate outside the primary workspace. Listed as **required** on `move-domains-across-workspace`, `GET /v2/domains/connection-requests`, and `GET /v2/exports/status` (set `ZAPMAIL_WORKSPACE_KEY[_<NAME>]` or those calls 400). |
| `x-service-provider` | `GOOGLE` (default) or `MICROSOFT`. **Required** on: purchase DNS Shield, purchase prewarmed subscription, purchase high-reputation domains, purchase placement-test plan, purchase subscription, get eligible mailboxes for placement tests, AI Domain Finder, schedule-mailbox-creation. `zapmail.py` sends it ONLY on those endpoints (or when a client is built with an explicit provider) so it can never filter Microsoft mailboxes out of general list calls. |

> The current on-record API key is Google-only; Microsoft operations need a
> Microsoft-scoped key. See the vendor-eval note (`slack message exporter`) that
> Zapmail is Google-only for this team's purposes.

### Known header quirk
Several wallet/subscription docs spell the provider header with a **leading
space**: `' x-service-provider'`. `zapmail.py` sends the clean name, which is
what the other endpoints accept; if a wallet call 400s on "missing provider",
this is the first thing to re-check.

---

## 3. Rate limits

| Scope | Limit |
|---|---|
| General requests | 5 rps · 200 rpm |
| Domain search (`/available` + `/available-bulk`, shared) | **100 requests / 30 min per API key** (each bulk call ≤ 20 names → ~2,000 names / 30 min) |
| AI Domain Finder | general limit only (not the search bucket) |
| Re-export mailboxes | per mailbox per app, rolling 7 days; number unconfirmed (3 vs 10) |

> Confirmed by Zapmail support 2026-09-29 (all buckets keyed by API key,
> shared across workspaces and IPs). Code: `RATE_LIMIT_CALLS = 100`, one run
> spends at most `DEFAULT_RUN_CALLS = 25`. Full support answers and the code
> changes they drove: `ZAPMAIL_QUESTIONS_2026-09.md` (top section).

---

## 4. Key gotchas discovered

1. **`available` (single-name) returns a nullable `exactMatch` and a marketing
   suggestion list** — you must not read a price off the suggestion list and
   attribute it to the queried name. `available-bulk` fixes this: one clean row
   per requested name (`status`, `isPremiumDomain`, `domainPrice`,
   `renewPrice`). The suggester now uses bulk.
2. **Registrable TLDs are restricted:** `com`, `net`, `org`, `biz`, `live`,
   `info`. The AI Domain Finder can return `.io`/`.co`/etc. — filter to this
   set before a buy plan (`zapmail.REGISTRABLE_TLDS`).
3. **AI Domain Finder is async + cached 5 min:** first call starts generation,
   subsequent calls return progress/results; a rapid re-call returns the same
   batch.
4. **Mailbox assignment caps:** max **5 mailboxes per domain**; a Microsoft
   account enforces a 24-hour rule between batches.
5. **Renewal window:** `renewal-soon` / `renew` / `get-renewal-price` /
   `update-auto-renew` only cover domains expiring in the **next 2 months**, and
   **exclude** high-reputation (aged) and pre-warmed domains.
6. **Endpoints with non-released status:** `upgrade-dns-shield-ltd-plan`
   (`testing`), `get-renewal-price` (`developing`).
7. **Deprecated:** `name-servers`, `name-servers/verify`, and
   `mailboxes/schedule`. Use `connect-domain` (new) and the quick-setup /
   scheduled-flow instead.
8. **`GET /v2/domains`** is the only endpoint where `x-auth-zapmail` is marked
   optional, but it still needs workspace scoping in practice.

---

## 5. Endpoint catalog

Class legend: **READ** · **WRITE** · **SPEND**. Header requirements are under
"Headers" (`auth` = `x-auth-zapmail`).

### Domains — availability & purchase

| Method + path | Class | Headers | Purpose / key fields |
|---|---|---|---|
| `POST /v2/domains/available` | READ | auth | Single-name availability. Body `{domainName, tlds[], years}`. Returns `availableDomains[]` + `exactMatch` (often null). |
| `POST /v2/domains/available-bulk` | READ | auth | **Bulk availability, up to 20 names.** Body `{domainNames[]}`. Returns `domains[]` (`domainName`, `status`, `isPremiumDomain`, `domainPrice`, `renewPrice`) + `total/available/unavailable`. 10 req/30 min. |
| `POST /v2/domains/ai-finder` | READ | auth + provider | AI-generated available domains. Body `{keywords[], tlds[], desiredCount}`. Async poll: `data.status/progress/domains/totalDomains`. Cached 5 min. |
| `POST /v2/domains/buy` | SPEND | auth | **Purchase domains.** Body `{domains:[{domainName, years}], useWallet, enableDnsShield}`. `useWallet=true` deducts wallet; `false` returns a Stripe `paymentLink`. |
| `POST /v2/quick-setup` | SPEND | auth | Domains + mailboxes (+ optional export) in ONE call. Body `{domains[], mailboxes{domain:[{username,firstName,lastName,...}]}, exportApp?, enableDnsShield}`. Charged only for *extra* slots. Returns `{quickSetupBatchId, slotsNeeded, invoiceUrl}`. |

### Domains — listing / connection / DNS / renewal / tags

| Method + path | Class | Headers | Purpose / key fields |
|---|---|---|---|
| `GET /v2/domains` | READ | auth | List domains. Query `contains,page,limit`. |
| `POST /v2/domains` | READ | — | List with filters. Body `{status[], contains, tagIds[], sortBy(ASC/DESC)}`. |
| `GET /v2/domains/assignable` | READ | auth | Domains mailboxes can be assigned to. Query `contains,page,limit`. |
| `POST /v2/domains/connect-domain` | WRITE (free) | auth | Connect registered domain. Body `{domainNames[]}`. **NS must point at** `pns61.cloudns.net`, `pns62.cloudns.com`, `pns63.cloudns.net`, `pns64.cloudns.uk`. Poll via connection-requests. |
| `GET /v2/domains/connection-requests` | READ | auth + workspace | Pending connects + per-domain status enum (PENDING→SUCCESS / DOMAIN_NOT_REGISTERED / BLACKLISTED_DOMAIN / WORKSPACE_ALREADY_EXISTS …). Query `page,limit,status,contains`. |
| `POST /v2/domains/name-servers` | READ | auth | *(deprecated)* Get NS for transfer. Body `{domainName, maskForwarding}`. |
| `POST /v2/domains/name-servers/verify` | READ | auth | *(deprecated)* Verify NS. Body `{domainName}`. |
| `POST /v2/domains/renewal-soon` | READ | auth | Domains expiring ≤2 months (excludes aged/pre-warmed). Body `{contains, tagIds}`; query `page,limit`. Returns `domains[]` with `registeredOn/expireOn/serviceProvider`. |
| `POST /v2/domains/get-renewal-price` | READ | auth | Renewal price for eligible domains *(developing)*. Body `{domainIds[], contains, tagIds}`. |
| `POST /v2/domains/renew` | SPEND | auth | Renew eligible domains. Body `{domainIds[], contains, tagIds}`. Wallet auto-applied. Returns `{paymentLink, domains, domainIds, useWallet}`. |
| `POST /v2/domains/update-auto-renew` | WRITE | auth | Toggle auto-renew. Body `{domainIds[], autoRenew, contains?, tagIds?}`. |
| `POST /v2/domains/dmarc` | WRITE (free) | auth | Set DMARC `rua` address on matching domains. Body `{domainIds[], email, contains, status[], tagIds[]}`. |
| `GET /v2/domains/tags` | READ | auth | List tags → `[{id, name, tagColor}]`. |
| `POST /v2/domains/tags` | WRITE | auth | Create tags. Body `[{name, tagColor}]` → `{tagIds[], domainTagsCreated[]}`. 409 on dup. |
| `POST /v2/domains/tags/delete` | WRITE | auth | Delete a tag. Body `{tagId}`. |
| `POST /v2/domains/tags/remove` | WRITE | auth | Remove tags from domains. Body `{tagIds[], domainIds[], contains, status[]}`. |
| `POST /v2/domains/assign-tag` | WRITE | auth | Bulk assign tags. Body `{tagIds[], domainIds[]}`. |
| `POST /v2/domains/move-workspace` | WRITE | auth + **workspace** | Move domains. Body `{domainIds[], workspaceId, serviceProvider}`. |
| `GET /v2/domains/health-score` | READ | auth | Nameserver reputation. Query `?domainId=`. Returns `{domain, averageScore, isAbused}`. |

### Mailboxes

| Method + path | Class | Headers | Purpose / key fields |
|---|---|---|---|
| `GET /v2/mailboxes/list` | READ | auth | All mailboxes + quota (`purchasedMailboxes`, `availableMailboxes`, `scheduledMailboxes`, …) + per-domain `adminDetails` (password/secret). Query `page,limit,contains`. |
| `POST /v2/mailboxes` | WRITE (quota) | auth + provider | Assign mailboxes keyed by domainId → `[{firstName,lastName,mailboxUsername,domainName}]`. Max 5/domain; Microsoft 24h rule. |
| `POST /v2/mailboxes/schedule` | WRITE | auth + provider | *(deprecated)* Schedule mailbox creation on next renewal. |

### Export

| Method + path | Class | Headers | Purpose / key fields |
|---|---|---|---|
| `POST /v2/exports/mailboxes` | WRITE | auth | Export to app or CSV. Body `{apps[], ids?, excludeIds?, tagIds?, contains?, status?, thirdPartyAccountId?}`. `apps:["MANUAL"]` = CSV. 3 req/mailbox/week. |
| `POST /v2/exports/accounts/third-party` | WRITE | auth | Register a third-party account. Body `{email, password, app}`. `app` ∈ SMARTLEAD / INSTANTLY / … (EmailBison adds `url`/`clientId`). |

### Users / wallet / plans

| Method + path | Class | Headers | Purpose |
|---|---|---|---|
| `GET /v2/users` | READ | auth | Plan, mailbox usage, `walletBalance`. |
| `GET /v2/wallet/balance` | READ | auth | `{walletBalance, autoRechargeEnabled, autoRechargeDetails}`. |
| `POST /v2/wallet/balance` | SPEND | auth | Add wallet balance (Stripe checkout). Body `{amount}`. ~1-min anti-spam. |
| `POST /v2/wallet/enable-auto-recharge` | WRITE | auth | Toggle auto-recharge. Body `{enable, threshold, minimumRechargeAmount}`. |
| `POST /v2/wallet/buy-addon-mailboxes` | SPEND | auth | Buy extra mailboxes. Query `?quantity=`. $3.00–$3.50/mailbox/mo. |
| `GET /v2/subscriptions` | READ | auth | All subscriptions (`status`, `contains` filters). |
| `POST /v2/subscriptions/purchase` | SPEND | auth + provider | Buy plan. Query `?planName=`, `billingCycle`. |
| `POST /v2/subscriptions/upgrade` | SPEND | auth | Upgrade. Body `{lookupKey, subscriptionId}`. |
| `POST /v2/subscriptions/cancel` | WRITE | auth | Cancel/revert. Body `{subscriptionId, revertCancellation}`. |
| `POST /v2/subscriptions/mailboxes` | READ | auth | Mailboxes in a subscription. Body `{subscriptionId}`. |
| `POST /v2/payment/invoices` | READ | auth | Invoice URL. Body `{subscriptionId}`. |

### DNS Shield

| Method + path | Class | Purpose |
|---|---|---|
| `GET /v2/dns-shield/eligible-domains` | READ | Domains eligible for shield (`page,limit,tagIds?,contains?,status?,mailboxIds?`). |
| `GET /v2/dns-shield/available-slots` | READ | `totalAvailableSlots` + per-subscription slots. |
| `GET /v2/dns-shield/subscriptions` | READ | Shield subs (MONTHLY / LTD). |
| `GET /v2/dns-shield/allocated-domains` | READ | Domains on a subscription (`?subscriptionId=`). |
| `POST /v2/dns-shield/allocate-domains` | WRITE | Allocate domains to LTD slots. Body `{domainIds[]}`. |
| `POST /v2/dns-shield/purchase` | SPEND | Buy shield. Body `{planType: LTD|MONTHLY|EXISTING, …}`. Starter $299 / Growth $999 / Pro $2,999 (LTD), $3/domain/mo. |
| `POST /v2/dns-shield/upgrade-plan` | SPEND | Upgrade LTD. Body `{subscriptionId, newPlanName}`. *(testing)* |
| `POST /v2/dns-shield/cancel-subscription` | WRITE | Cancel. Body `{subscriptionId}`. |

### Pre-warmed domains

| Method + path | Class | Purpose |
|---|---|---|
| `GET /v2/prewarmed-domains/get-domains` | READ | Available pre-warmed domains → `domains[]` (`id, domain, isSold, preWarmedUpMailboxes[], price`). |
| `GET /v2/prewarmed-domains/count` | READ | Unsold counts by provider (`data.google`, `data.microsoft`). |
| `GET /v2/prewarmed-domains/subscriptions` | READ | Pre-warm subscriptions (`status?,page?,limit?`). |
| `POST /v2/prewarmed-domains/purchase` | SPEND | Buy pre-warmed plan. `?planType=starter`. Starter $39/$24 … Pro $339/$180. |
| `POST /v2/prewarmed-domains/assign` | WRITE | Assign pre-warmed domains to fill slots. Body `{domainIds[]}`. |

### High-reputation (aged) domains

| Method + path | Class | Purpose |
|---|---|---|
| `GET /v2/aged-domains/available-domains` | READ | Marketplace aged domains → `domains[]` (`domainName, price, qualityScore, domainAgeYears, metrics{domainAuthority,spamScore,…}`). |
| `POST /v2/aged-domains/purchase` | SPEND | Buy aged domains. Body `{domains[]}` (max 50). |

### Placement test

| Method + path | Class | Purpose |
|---|---|---|
| `GET /v2/placement-tests/subscriptions` | READ | Placement subs + credits. |
| `GET /v2/placement-tests/overall-report` | READ | Aggregate inbox/spam/blacklist rates + `byProvider[]`. |
| `GET /v2/placement-tests/orders` | READ | Test orders + results (`page?,limit?`). |
| `GET /v2/placement-tests/report` | READ | Report by `?cartOrderId=`. |
| `POST /v2/placement-tests/eligible-mailboxes` | READ | Eligible mailboxes. Query `page,limit,status?`. Provider header required. |
| `GET /v2/placement-tests/available-slots` | READ | `totalAvailableCredits`, `usedCredits`, subscriptions, `nextResetDate`. |
| `POST /v2/placement-tests/purchase` | SPEND | Run tests. Body `{placementType(MONTHLY/ONE_TIME), testName, mailboxIds[], seedAccounts[]}`. ONE_TIME = $2/mailbox. |
| `POST /v2/placement-tests/purchase-plan` | SPEND | Buy placement plan. Body `{planName}`. Starter $29/mo … Pro $199/mo (LTD $99/$299/$999). |
| `POST /v2/placement-tests/cancel-subscription` | WRITE | Cancel. Body `{subscriptionId, revertCancellation}`. |

### DNS records

| Method + path | Class | Purpose |
|---|---|---|
| `GET /v2/dns/` | READ | DNS records for a domain. Query `?id=<domainId>`. Returns `{records[], disabledRecords[]}`. |
| add / update / delete dns records | WRITE | *(summary-only — see appendix)* |

---

## 6. How the code maps to this

| File | Role |
|---|---|
| `smartlead/zapmail.py` | `ZapmailClient` — auth, headers, retry, and the READ/WRITE/SPEND guard. Core lifecycle methods implemented; long tail via `_request`. |
| `smartlead/domain_availability.py` | Availability via `available-bulk` (+ DNSBL pre-check), cache + rate-limit budget. |
| `smartlead/domain_ai.py` | AI Domain Finder (`ai_suggest`) + homepage vocabulary scrape (`source_vocabulary`). |
| `smartlead/domain_purchase.py` | Staged buy (`stage_purchase`) + gated execute (`execute_purchase`). |
| `domain_generator.py` | CLI: `/domains` generation, `--ai`, `--auto-vocab`, `--buy` (stage), estate bookkeeping. |
| `domains_command.js` | Slack surface: `/domains`, `/domains ai`, `/domains buy` (stage-only). |

---

## 7. Appendix — summary-only endpoints

Fully-specified above, the below are captured from the docs index but not yet
decomposed into exact method/path/fields (fetch on demand before use):

- **Workspaces:** retrieve list, create, update, list members, update role,
  revoke access, send invitation, list invitations, revoke invitation, update
  domain renewal settings.
- **Billing:** add / update billing details (name, company, address, contact).
- **Mailboxes:** update mailbox, remove-mailboxes-on-next-renewal, get
  authenticator code, custom OAuth, retry creation of failed mailboxes.
- **Export:** get export status, fetch workspaces by app, list third-party
  accounts.
- **Domains (misc):** add/remove domain forwarding, enable/remove email
  forwarding, enable/remove catch-all, check DNS records, remove unused
  domains, get domain connection requests / remove them, add Google Client ID
  to domain.
- **Global:** global mailbox-domain search (dashboard search → mailboxes +
  workspace + provider).
- **Zapbox:** send email, list connected accounts, fetch emails, get thread,
  search emails, download attachment, create/delete/rename label.
- **Webhooks:** get event types, list/create/update webhook endpoints.
- **Zapsites:** scan, create, fetch, edit, regenerate, deploy a ZapSite.