# Zapmail live test — every free feature (2026-09-28)

Read-only run of every free Zapmail endpoint on both accounts (Precise Leads,
Belardi Wong) and both providers (Google, Microsoft). Spend switch off. Re-run
any time: `python zapmail_selftest.py --md docs/<file>.md`.

**Result: 77 of 79 checks passed.** The 2 failures are the same documented
limitation: `fetch-workspaces` does not support Smartlead ("Supported apps:
INSTANTLY, …"), so Smartlead exports must name a third-party account id.

## What we learned

| Area | Finding |
|---|---|
| Providers | Every list endpoint returns **Google only** unless the request asks for `MICROSOFT`. Our code now always checks both. |
| Accounts | PL: $64 wallet, auto-recharge ON ($50 below $25), 193 Google + 20 Microsoft domains, 2 workspaces, 12 tags, 10 subscriptions. BW: $0 wallet, auto-recharge OFF, 22 + 6 domains, 4 subscriptions. |
| Domain status | PL Google: only **60 of 193** domains are ACTIVE and 49 can take mailboxes; the rest are lapsed/inactive. |
| Renewals | PL: 26 Google + 2 Microsoft domains renew within 2 months; renewal price works (~$21, Melior $22.42). BW: none. `renewal-soon` rejects `null` filters (fixed). |
| Connection requests | Works **without** `x-workspace-key` (docs said it was required). None pending. |
| DNS records | Works per domain (7–15 records). One call took 8s. |
| Domain health score | Works: `healthy/90` on live domains; `critical/0 — No mailboxes assigned` on a bare BW domain. Useful for the digest. |
| Placement tests | BW: 4 runs, 84 emails, **54.8% inbox / 9.5% spam**, 25 credits left. PL: 4 runs (1 in progress), 456 emails, **63.1% inbox / 2.4% spam**, 31 credits left. Eligible-mailbox call can be slow (22s once). |
| DNS Shield | PL has 1 subscription but **0 free slots**; BW none. 60 PL Google + 17 Microsoft domains are eligible. |
| Pre-warmed | Marketplace: **448 Google + 84 Microsoft** pre-warmed mailboxes available, 113 domains unsold. PL already has 2 pre-warmed subscriptions. |
| Aged domains | Marketplace lists work. |
| Export targets | One Smartlead target per account: PL → amanda@bettrdata.io (BettrData), BW → saml@belardiwong.com. |
| Availability (bulk) | Clean per-name answers; standard `.com` = $12.99 first year, $20.99 renewal. |
| Availability (single) | Returns a suggestion list, `exactMatch: null` — not usable for a yes/no; we use bulk. |
| **AI Domain Finder** | **Works and is the best name source.** ~15–20s per run, returns ~6–12 names per run **with availability and price already attached** (objects, not strings — our first parser dropped them all; fixed). |

## Domain suggestions: AI finder vs our generator (live)

| Client | Zapmail AI usable | Our generator usable | Examples (AI) |
|---|---|---|---|
| BettrData | 24 | 10 | ingestyard.com, parsebatch.com, conduitcraft.com |
| Belardi Wong | 22 | 8 | postalcraft.com, envelopeyard.com, stampdeliver.com |
| Precise Leads | 19 | 12 | quotapeak.com, signalwarm.com, sequencecove.com |

Our generator's names are weaker compounds (`catalogpostal.com`,
`leadsbookedhq.com`), so Slack shows AI names first and keeps ~⅓ of the slots
for ours. A full suggestion run takes ~90s (most of it our generator's DNS +
availability checks).

## Raw results


77/79 checks OK. Read-only; spend switch forced off.

| Feature | Account | Provider | OK | ms | Result |
|---|---|---|---|---|---|
| User + plan | Belardi Wong | - | ✅ | 593 | email=saml@belardiwong.com, firstName=Donna, lastName=Belardi, activePlan=Growth, planEndsOn=2026-05-24T15:10:54.000Z, purchasedMailboxes=48, assignedMailboxes=48, userCreatedAt=2025-12-11T15:09:59.345Z, walletBalance=0 |
| Wallet balance | Belardi Wong | - | ✅ | 218 | walletBalance=0, autoRechargeEnabled=False |
| Workspaces | Belardi Wong | - | ✅ | 218 | totalSearchedCount=1, currentPage=1, nextPage=2, totalPages=1, totalWorkspacesCount=1, totalDomainsCountGoogle=22, totalDomainsCountMicrosoft=6, totalPurchasedMailboxesCountGoogle=48, totalPurchasedMailboxesCountMicrosoft=18, workspaces[0] |
| Subscriptions | Belardi Wong | - | ✅ | 203 | list[4] keys=['id', 'invoiceDetails', 'paymentFailureMessage', 'periodEnd', 'periodStart', 'plan', 'planUpgradeCancelPossible', 'price'] |
| Domain tags | Belardi Wong | - | ✅ | 188 | list[0] |
| Export accounts (Smartlead) | Belardi Wong | - | ✅ | 203 | accounts[1] |
| Export workspaces (Smartlead) | Belardi Wong | - | ❌ | 186 | 400 on /v2/exports/fetch-workspaces: {"status":400,"message":"Invalid app parameter. Supported apps: INSTANTLY, QUICKMAIL, SUPERAGI, REACHINBOX, MANYREACH, MAST _(docs say SMARTLEAD unsupported here)_ |
| Placement subscriptions | Belardi Wong | - | ✅ | 203 | list[1] keys=['billingCycle', 'cancelledByUser', 'couponApplied', 'createdAt', 'daysBeforePreCharged', 'deletedAt', 'discountApplied', 'discountId'] |
| Placement credits | Belardi Wong | - | ✅ | 171 | totalAvailableCredits=25, monthlyCredits=0, ltdCredits=25 |
| Placement overall report | Belardi Wong | - | ✅ | 265 | totalTestRuns=4, totalEmails=84, successfulTestsCount=4, inProgressTestsCount=0, failedTestsCount=0, successfulEmailsCount=84, inboxCount=46, inboxRate=54.76%, spamCount=8, spamRate=9.52% |
| Placement orders | Belardi Wong | - | ✅ | 375 | testGroups[4] |
| DNS Shield slots | Belardi Wong | - | ✅ | 187 | totalAvailableSlots=0, subscriptions[0] |
| DNS Shield subscriptions | Belardi Wong | - | ✅ | 187 | empty |
| Prewarmed: available count | Belardi Wong | - | ✅ | 203 | google=448, microsoft=84 |
| Prewarmed: domains for sale | Belardi Wong | - | ✅ | 281 | domains[5], total=113, totalDomainsUnsold=113, currentPage=1, nextPage=2, totalPages=23 |
| Prewarmed: our subscriptions | Belardi Wong | - | ✅ | 202 | subscriptions[0], total=0, currentPage=1, nextPage=2, totalPages=0 |
| Aged domains marketplace | Belardi Wong | - | ✅ | 344 | domains[5] |
| Domains list | Belardi Wong | GOOGLE | ✅ | 671 | totalSearchedCount=22, currentPage=1, nextPage=2, totalPages=5, domains[5] |
| Domains list (filter ACTIVE) | Belardi Wong | GOOGLE | ✅ | 250 | totalSearchedCount=22, currentPage=1, nextPage=2, totalPages=3, domains[10] |
| Assignable domains | Belardi Wong | GOOGLE | ✅ | 234 | totalSearchedCount=22, currentPage=1, nextPage=2, totalPages=5, domains[5] |
| Mailboxes list | Belardi Wong | GOOGLE | ✅ | 313 | totalSearchedCount=16, currentPage=1, nextPage=2, totalPages=4, purchasedMailboxes=48, totalAssignedMailboxes=48, totalAssignedPreWarmedUpMailboxes=0, totalActiveMailboxes=48, totalActivePreWarmedUpMailboxes=0, availableMailboxes=0 |
| Connection requests | Belardi Wong | GOOGLE | ✅ | 389 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, data[0] _(docs: needs x-workspace-key)_ |
| Renewal soon (<=2 months) | Belardi Wong | GOOGLE | ✅ | 235 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, domains[0] |
| DNS records (one domain) | Belardi Wong | GOOGLE | ✅ | 8139 | domainDnsRecords[7] |
| Domain health score (one domain) | Belardi Wong | GOOGLE | ✅ | 235 | error=No mailboxes assigned, label=critical, score=0, reasons[1], recommendedActions[1] |
| Placement-eligible mailboxes | Belardi Wong | GOOGLE | ✅ | 22156 | domains[5] |
| DNS Shield eligible domains | Belardi Wong | GOOGLE | ✅ | 5202 | totalSearchedCount=22, currentPage=1, nextPage=2, totalPages=5, domains[5] |
| Domains list | Belardi Wong | MICROSOFT | ✅ | 639 | totalSearchedCount=6, currentPage=1, nextPage=2, totalPages=2, domains[5] |
| Domains list (filter ACTIVE) | Belardi Wong | MICROSOFT | ✅ | 265 | totalSearchedCount=6, currentPage=1, nextPage=2, totalPages=1, domains[6] |
| Assignable domains | Belardi Wong | MICROSOFT | ✅ | 234 | totalSearchedCount=6, currentPage=1, nextPage=2, totalPages=2, domains[5] |
| Mailboxes list | Belardi Wong | MICROSOFT | ✅ | 328 | totalSearchedCount=6, currentPage=1, nextPage=2, totalPages=2, purchasedMailboxes=18, totalAssignedMailboxes=18, totalAssignedPreWarmedUpMailboxes=0, totalActiveMailboxes=18, totalActivePreWarmedUpMailboxes=0, availableMailboxes=0 |
| Connection requests | Belardi Wong | MICROSOFT | ✅ | 406 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, data[0] _(docs: needs x-workspace-key)_ |
| Renewal soon (<=2 months) | Belardi Wong | MICROSOFT | ✅ | 233 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, domains[0] |
| DNS records (one domain) | Belardi Wong | MICROSOFT | ✅ | 437 | domainDnsRecords[15] |
| Domain health score (one domain) | Belardi Wong | MICROSOFT | ✅ | 234 | label=healthy, score=90, reasons[0], recommendedActions[0] |
| Placement-eligible mailboxes | Belardi Wong | MICROSOFT | ✅ | 360 | domains[5] |
| DNS Shield eligible domains | Belardi Wong | MICROSOFT | ✅ | 483 | totalSearchedCount=6, currentPage=1, nextPage=2, totalPages=2, domains[5] |
| User + plan | PRECISE_LEADS | - | ✅ | 296 | email=avinash@preciseleads.in, firstName=Avinash, lastName=Haridas, activePlan=Growth, planEndsOn=2026-07-09T08:26:50.000Z, purchasedMailboxes=54, assignedMailboxes=69, userCreatedAt=2024-10-02T09:19:02.323Z, walletBalance=64 |
| Wallet balance | PRECISE_LEADS | - | ✅ | 218 | walletBalance=64, autoRechargeEnabled=True |
| Workspaces | PRECISE_LEADS | - | ✅ | 265 | totalSearchedCount=2, currentPage=1, nextPage=2, totalPages=1, totalWorkspacesCount=2, totalDomainsCountGoogle=193, totalDomainsCountMicrosoft=20, totalPurchasedMailboxesCountGoogle=54, totalPurchasedMailboxesCountMicrosoft=15, workspaces[1] |
| Subscriptions | PRECISE_LEADS | - | ✅ | 234 | list[10] keys=['id', 'invoiceDetails', 'paymentFailureMessage', 'periodEnd', 'periodStart', 'plan', 'planUpgradeCancelPossible', 'price'] |
| Domain tags | PRECISE_LEADS | - | ✅ | 235 | list[12] keys=['id', 'name', 'tagColor'] |
| Export accounts (Smartlead) | PRECISE_LEADS | - | ✅ | 233 | accounts[1] |
| Export workspaces (Smartlead) | PRECISE_LEADS | - | ❌ | 234 | 400 on /v2/exports/fetch-workspaces: {"status":400,"message":"Invalid app parameter. Supported apps: INSTANTLY, QUICKMAIL, SUPERAGI, REACHINBOX, MANYREACH, MAST _(docs say SMARTLEAD unsupported here)_ |
| Placement subscriptions | PRECISE_LEADS | - | ✅ | 235 | list[1] keys=['billingCycle', 'cancelledByUser', 'couponApplied', 'createdAt', 'daysBeforePreCharged', 'deletedAt', 'discountApplied', 'discountId'] |
| Placement credits | PRECISE_LEADS | - | ✅ | 218 | totalAvailableCredits=31, monthlyCredits=0, ltdCredits=31 |
| Placement overall report | PRECISE_LEADS | - | ✅ | 578 | totalTestRuns=4, totalEmails=456, successfulTestsCount=3, inProgressTestsCount=1, failedTestsCount=0, successfulEmailsCount=455, inboxCount=287, inboxRate=63.08%, spamCount=11, spamRate=2.42% |
| Placement orders | PRECISE_LEADS | - | ✅ | 296 | testGroups[4] |
| DNS Shield slots | PRECISE_LEADS | - | ✅ | 234 | totalAvailableSlots=0, subscriptions[0] |
| DNS Shield subscriptions | PRECISE_LEADS | - | ✅ | 218 | list[1] keys=['billingCycle', 'cancelledByUser', 'couponApplied', 'createdAt', 'id', 'invoiceLink', 'isFreeTrial', 'lookupKey'] |
| Prewarmed: available count | PRECISE_LEADS | - | ✅ | 250 | google=448, microsoft=84 |
| Prewarmed: domains for sale | PRECISE_LEADS | - | ✅ | 313 | domains[5], total=113, totalDomainsUnsold=113, currentPage=1, nextPage=2, totalPages=23 |
| Prewarmed: our subscriptions | PRECISE_LEADS | - | ✅ | 639 | subscriptions[2], total=2, currentPage=1, nextPage=2, totalPages=1 |
| Aged domains marketplace | PRECISE_LEADS | - | ✅ | 328 | domains[5] |
| Domains list | PRECISE_LEADS | GOOGLE | ✅ | 281 | totalSearchedCount=193, currentPage=1, nextPage=2, totalPages=39, domains[5] |
| Domains list (filter ACTIVE) | PRECISE_LEADS | GOOGLE | ✅ | 233 | totalSearchedCount=60, currentPage=1, nextPage=2, totalPages=6, domains[10] |
| Assignable domains | PRECISE_LEADS | GOOGLE | ✅ | 250 | totalSearchedCount=49, currentPage=1, nextPage=2, totalPages=10, domains[5] |
| Mailboxes list | PRECISE_LEADS | GOOGLE | ✅ | 453 | totalSearchedCount=23, currentPage=1, nextPage=2, totalPages=5, purchasedMailboxes=54, totalAssignedMailboxes=54, totalAssignedPreWarmedUpMailboxes=15, totalActiveMailboxes=54, totalActivePreWarmedUpMailboxes=15, availableMailboxes=0 |
| Connection requests | PRECISE_LEADS | GOOGLE | ✅ | 406 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, data[0] _(docs: needs x-workspace-key)_ |
| Renewal soon (<=2 months) | PRECISE_LEADS | GOOGLE | ✅ | 250 | totalSearchedCount=26, currentPage=1, nextPage=2, totalPages=1, totalCount=26, domains[26] |
| Renewal price | PRECISE_LEADS | GOOGLE | ✅ | 2485 | list[5] keys=['domainId', 'domainName', 'domainPrice', 'isPremiumDomain', 'renewPrice', 'status'] |
| DNS records (one domain) | PRECISE_LEADS | GOOGLE | ✅ | 453 | domainDnsRecords[11] |
| Domain health score (one domain) | PRECISE_LEADS | GOOGLE | ✅ | 218 | label=healthy, score=90, reasons[0], recommendedActions[0] |
| Placement-eligible mailboxes | PRECISE_LEADS | GOOGLE | ✅ | 360 | domains[5] |
| DNS Shield eligible domains | PRECISE_LEADS | GOOGLE | ✅ | 468 | totalSearchedCount=60, currentPage=1, nextPage=2, totalPages=12, domains[5] |
| Domains list | PRECISE_LEADS | MICROSOFT | ✅ | 250 | totalSearchedCount=20, currentPage=1, nextPage=2, totalPages=4, domains[5] |
| Domains list (filter ACTIVE) | PRECISE_LEADS | MICROSOFT | ✅ | 250 | totalSearchedCount=17, currentPage=1, nextPage=2, totalPages=2, domains[10] |
| Assignable domains | PRECISE_LEADS | MICROSOFT | ✅ | 250 | totalSearchedCount=13, currentPage=1, nextPage=2, totalPages=3, domains[5] |
| Mailboxes list | PRECISE_LEADS | MICROSOFT | ✅ | 405 | totalSearchedCount=9, currentPage=1, nextPage=2, totalPages=2, purchasedMailboxes=15, totalAssignedMailboxes=15, totalAssignedPreWarmedUpMailboxes=12, totalActiveMailboxes=15, totalActivePreWarmedUpMailboxes=12, availableMailboxes=0 |
| Connection requests | PRECISE_LEADS | MICROSOFT | ✅ | 437 | totalSearchedCount=0, currentPage=1, nextPage=2, totalPages=0, totalCount=0, data[0] _(docs: needs x-workspace-key)_ |
| Renewal soon (<=2 months) | PRECISE_LEADS | MICROSOFT | ✅ | 235 | totalSearchedCount=2, currentPage=1, nextPage=2, totalPages=1, totalCount=2, domains[2] |
| Renewal price | PRECISE_LEADS | MICROSOFT | ✅ | 1405 | list[2] keys=['domainId', 'domainName', 'domainPrice', 'isPremiumDomain', 'renewPrice', 'status'] |
| DNS records (one domain) | PRECISE_LEADS | MICROSOFT | ✅ | 469 | domainDnsRecords[15] |
| Domain health score (one domain) | PRECISE_LEADS | MICROSOFT | ✅ | 218 | label=healthy, score=90, reasons[0], recommendedActions[0] |
| Placement-eligible mailboxes | PRECISE_LEADS | MICROSOFT | ✅ | 453 | domains[5] |
| DNS Shield eligible domains | PRECISE_LEADS | MICROSOFT | ✅ | 453 | totalSearchedCount=17, currentPage=1, nextPage=2, totalPages=4, domains[5] |
| Availability, bulk (3 names) | any | - | ✅ | 3296 | domains[3], total=3, available=2, unavailable=1 _(1 of 10 searches / 30 min)_ |
| Availability, single name | any | - | ✅ | 1953 | exactMatch=None, availableDomains[50] _(1 of 10 searches / 30 min)_ |
| AI Domain Finder | any | GOOGLE | ✅ | 16358 | status=generating, progress=100, domains[6], totalDomains=6 _(async ~15-20s; cached 5 min)_ |

