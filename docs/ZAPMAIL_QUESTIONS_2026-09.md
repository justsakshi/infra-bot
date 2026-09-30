# Zapmail API — open questions for support (2026-09)

For the team building Zapmail automation on the public API. Answers make the
purchase/lifecycle code exact rather than inferred. Each question carries our
**current assumption** so a one-word correction is enough.

Split into **Blocking** (affects whether buy/automation money moves correctly
and safely) and **Sharpening** (makes the reference exact).

---

## ANSWERED — Zapmail support, 2026-09-29 (and what changed in our code)

| # | Answer | Code change |
|---|---|---|
| 1 | status is only AVAILABLE / UNAVAILABLE; premium is `isPremiumDomain`; `originalPrice` only on a .com promo. UNAVAILABLE also = banned phrase, already in Zapmail, or existing Google/MS workspace. | none (as assumed) |
| 2 | Prices are numeric strings in USD, no symbol. **Unsupported TLDs can return "NaN".** | `parse_price()`: NaN/inf/negative → unknown. Before, NaN passed every price check. |
| 3 | `/buy` with wallet → Stripe invoice, wallet applied; if fully covered, paid at $0 and registration starts. Returns `{message:"Domains purchased", invoiceLink}` — no domain ids; poll domains PENDING → ACTIVE. | ledger records `invoice_link`; Slack says registration is async |
| 4 | **Short wallet is NOT an error**: invoice for the remainder, nothing registers until paid, order dropped after ~5 min. | `require_wallet_covers()` refuses before `/buy` unless the wallet alone covers the batch |
| 5 | `/buy` does not check the registry first; a taken name is refunded after. It rejects names already in the account, banned phrases, and (400) existing workspaces. | our live `available-bulk` re-check right before buy stays mandatory |
| 6 | Quick-setup: wallet-only, 400 "Insufficient wallet balance…"; needs billing details + base plan; `invoiceUrl` = paid record; track `GET /quick-setup?domain=` (PENDING … COMPLETED). | `quick_setup_status()` added; docstring |
| 7 | Smartlead export uses the **Smartlead login** (email + password): add third-party account → export by ids/tags/contains (ACTIVE only) → poll `exports/status?exportId`. Or `exportApp:"SMARTLEAD"` in quick-setup. | matches our code; where `exportId` comes from is still unstated |
| 8 | connect-domain is async: re-call to poll (207 in progress, 200 final); final = SUCCESS or NS_NOT_CHANGED / WORKSPACE_ALREADY_EXISTS / DOMAIN_NOT_REGISTERED / BLACKLISTED / BANNED / PERMISSION_REQUIRED / FAILED. Poll 60s; allow 24–48h for NS. Mailboxes IN_PROGRESS → ACTIVE / FAILED, poll 2–5 min, Google ~1h, Microsoft hours, escalate at 24h. | connect polls 60s, reports NS_NOT_CHANGED (not TIMEOUT); mailboxes poll 180s, stop on FAILED, say "check later" |
| 9 | 5 mailboxes/domain default (configurable per account); **no 24h rule** on either provider. | docstrings corrected |
| 10 | `/available` + `/available-bulk` share **100 req / 30 min per API key** (+ 200/min, 5/s). | budget 10 → 100; per-run cap 25; suggestion generator 3 → 10 |
| 11 | TLDs: com, net, org, biz, live, info (no .co/.io). | as assumed |
| 12 | AI finder free, general limit only (not the search bucket), cached 5 min per keyword set. | as built |
| 13 | API-bought domains default `autoRenew: false` unless the workspace default is set (`POST /workspaces/update-domain-renewal-settings`). | open decision: auto-renew current clients' new domains? |
| 14 | `GET /workspaces` lists all; id = `x-workspace-key`; omitted = default workspace. | as built |
| 15 | Header is `x-service-provider` (no space), defaults GOOGLE; `x-auth-zapmail` required everywhere. | as built |
| 16 | Export cap per mailbox per destination app, rolling 7 days, successful exports only, first counts. **Number unconfirmed (3 vs 10).** | still open |
| 17 | Health score 0–100; 0–30 = critical. Nameserver reputation follows Spamhaus; Zap Shield ranks top. | could flag ≤30 in the digest |
| 18 | Delayed purchase is **UI-only**; charges immediately, registration staggered per domain. | our ledger stagger stays (it staggers the charge too) |
| 19 | Zap Shield gives an isolated IP pool on activation and rotates it; the staggered UI mode varies purchase timing. | — |
| 20 | Pre-warmed Microsoft 365 mailboxes export to Smartlead like Google ones; inventory: `get-prewarmed-domains`. **Price not given.** | still open |

Still open: where the export id comes from, the re-export cap number, pre-warmed M365 price.

---

## Blocking — money + lifecycle correctness

1. **`available-bulk` `status` enum.**
   What exact strings does `status` take (`AVAILABLE` / `UNAVAILABLE` /
   `TAKEN` / `PREMIUM`, …)? We only treat `status == "AVAILABLE"` as buyable.

2. **`domainPrice` / `renewPrice` format.**
   Are they strings (`"$12.99"`) or numbers? We currently strip `$`/`,` and
   parse a float. Need to know so the pre-purchase total is exact.

3. **`buy` with `useWallet=true`.**
   Does it purchase immediately (returning bought domain IDs), or still return
   a payment link? What is the success response when wallet covers the total?

4. **`buy` + insufficient wallet balance.**
   Does it fail cleanly, partial-buy, or fall back to a payment link? We never
   want a half-completed cart.

5. **Does `buy` self-verify availability?**
   Or must we pre-verify with `available-bulk`? (We assume it self-checks and
   400s on taken names.)

6. **`quick-setup` funding.**
   Is it Stripe-invoice-only (`invoiceUrl` in the response), or can it be
   wallet-funded?

7. **The documented Smartlead export path.**
   To buy domain → create 2 inboxes → auto-export to a **specific Smartlead
   account**: is `add-third-party-account` required first, and what goes in
   `email`/`password` (the Smartlead API key?). What's the simplest correct
   sequence?

8. **Post-purchase timing.**
   Recommended waits: NS propagation → `connect-domain` success → mailbox
   creation (Google) → export availability, and any delay between mailbox
   assignment and the mailbox reaching `ACTIVE`.

9. **Mailbox cap + 24-hour rule.**
   Confirm **5 mailboxes/domain** max, and that the 24-hour rule is
   **Microsoft-only** (we're Google) — we assume it doesn't apply.

10. **Domain-search rate limit truth.**
    Docs list both "Domain Search: 100/30min" and "10 requests/30min per
    client" (on `available-bulk`). Which is authoritative? Is the budget **per
    key, per workspace, or per account**, and do `available` (single) and
    `available-bulk` share one budget? We budget conservatively at 10 bulk
    requests/30min.

---

## Sharpening — exactness only

11. **Registrable TLDs** — is it exactly `com, net, org, biz, live, info`?
    Will `.io`/`.co` become registrable? (The AI finder returns them, so we
    clamp to the six.)

12. **AI Domain Finder** — free (within the search budget) or does it spend
    credits? And within the 5-minute cache window: repeated calls return the
    same cached result, an error, or a charge?

13. **Auto-renew default** on domains bought via API — on or off? (Affects
    whether `update-auto-renew` must run after every buy.)

14. **Workspace enumeration** — can one API key list its workspaces, and how do
    we get workspace IDs for `x-workspace-key` scoping?

15. **Doc QA** — the provider header is listed with a leading space
    (`' x-service-provider'`) on several wallet/subscription endpoints; doc bug
    or literally required? And `GET /v2/domains` marks `x-auth-zapmail` as
    *optional* — doc bug?

16. **Re-export limit** — "3/mailbox/week" applies only to *re*-exporting an
    already-exported mailbox (first export unlimited), or to all exports?

17. **`health-score` scale** — what range is `averageScore`, and is there a
    documented `isAbused` threshold that maps to "don't buy/send"?

---

## Answered by a live read-only test (2026-09-28)

- **Q1 status enum:** `AVAILABLE` / `UNAVAILABLE` (confirmed on `google.com`,
  a random free name, and a taken name).
- **Q2 price format:** plain decimal strings, no `$`: `"12.99"` first year,
  `"20.99"` renewal for a standard `.com`. Taken names return `"0.00"`.
  Note the renewal is ~60% dearer than year one.
- **Mailbox rows** carry `id`, `email`, `status`, `isWarmedUp` (export by id
  works as coded). **Domain rows** carry `id`, `domain`, `status`,
  `assignedMailboxesCount`, `autoRenew`, `dnsShieldEnabled`, `isWarmedUp`.
- **Wallet:** balance 0, auto-recharge off on the primary key — a wallet buy
  would currently fail; top up or use payment links.

---

## Added 2026-09-28 — delayed / alternative purchasing

18. **Delayed or scheduled domain purchase.** We were told Zapmail recently
    added a way to delay domain purchases so a batch is not bought all at
    once. We can't find it in the API reference, the MCP docs ("Purchases
    charge immediately"), or the help center. Does it exist? If so: is it
    UI-only or on the API, and does it delay the *registration* date itself
    (what registries and filters see), or only the charge?

19. **Registration spread.** When 10 domains are bought in one `buy` call,
    are they registered with the same registrar, at the same timestamp, on
    the same nameserver set / IP? Does Zap Shield's "DNS zone rotation" and
    isolated IP pool change that for domains bought together?

20. **Pre-warmed Microsoft 365 mailboxes.** Available counts, price per
    mailbox, and whether they can be exported to Smartlead like regular
    mailboxes. (We need more Outlook senders.)

## Added 2026-09-29 (inbox pipeline)

- **Buying a domain straight into Outlook:** `POST /v2/domains/buy` takes no provider. Does it honour `x-service-provider: MICROSOFT` so the domain lands in the Microsoft workspace? Until answered, the bot only buys new domains for Google.
- **Add-on inbox slots (`/wallet/buy-addon-mailboxes`):** is it paid from the wallet (like domains with useWallet) or always by card through the returned invoice link? Is the quantity per provider?
- **Pre-warmed assign:** does an assigned pre-warmed domain keep its inboxes' warmup history and existing first/last names? Can those names be changed (PUT /mailboxes) without hurting the warmup?


## Batch 3 — sent 2026-09-30 (inbox pipeline, before the first live run)

Supersedes the "Added 2026-09-29" bullets above. See the chat message of 30 Sep for the send-ready wording.

Blocking: B1 Outlook domain buy provider header · B2 add-on slots (wallet vs card, per provider, when they appear) · B3 /mailboxes with no free slot (refuse vs charge) · B4 pre-warmed plan purchase funding + adding single slots · B5 pre-warmed assign (warmup kept, names, renewal) · B6 Microsoft SMTP AUTH / app passwords for direct Smartlead add.
Sharpening: S1 rename via PUT /mailboxes · S2 retry-failed + remove-on-renewal · S3 one Smartlead per workspace / move domains across workspaces · S4 export id + re-export cap · S5 webhook payload samples + registration-complete event · S6 planEndsOn in the past.


## ANSWERED — Zapmail support, 2026-09-30 (batch 3) and what changed

| Topic | Answer | Code change |
|---|---|---|
| Outlook domain buy | `x-service-provider` on `/domains/buy` files the domain under that provider | Batches carry `provider`; purchases send it; new-domain Outlook jobs allowed |
| Add-on slots | Wallet ONLY; slots are locked to the provider they were bought for | Wallet checked before buying slots (refuse with the amount); message no longer mentions an invoice |
| No free slot | `/mailboxes` refuses; buy slots first | as assumed |
| Pre-warmed | Pick from `get-prewarmed-domains`, then `assign` — **no plan needed**; charges wallet, else the card on file | Plan purchase removed; `assign_prewarmed` is the paid step, wallet must cover the price first, ordered at most once |
| Pre-warmed life | Domains last **one year and then expire** (cannot be renewed) | Cost line says so |
| Pre-warmed names | First/last can be reset; changing the username loses warmup history | We never change usernames |
| Rename sync | Zapmail pushes the new sender name to the sending platform; a username change needs a re-export | nothing (we keep usernames) |
| Smartlead | Direct integration uses OAuth; **several Smartlead accounts can be connected** (with correct credentials); domains cannot move workspaces via API | Recommend connecting the Precise Leads Smartlead too → Zapmail export (OAuth, works for Outlook); direct add stays the fallback |
| Export id | In the export request's response | as built |
| Domain connection status | No API; UI only | we keep polling the domain list for ACTIVE |
| Webhooks | No sample payloads | alert text reads either shape |
| planEndsOn | They asked which endpoint: `GET /v2/users` | reply sent back |

Follow-ups: (a) is $14.99 the whole pre-warmed charge, or are its inboxes also billed monthly? (b) `planEndsOn` on `GET /v2/users`.
