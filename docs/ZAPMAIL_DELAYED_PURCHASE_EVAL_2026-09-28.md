# Does Zapmail have a delayed domain purchase feature?

> **Update 2026-09-29 — Zapmail support answered:** yes, but **UI-only** (not in
> the API). It **charges immediately**; only the registration at the registrar
> is staggered per domain. So for API buying, our own ledger stagger stays —
> it staggers both the charge and the registration. Zap Shield gives an
> isolated IP pool on activation and rotates it. Pre-warmed M365 mailboxes
> export to Smartlead like Google ones (price not given).

Checked 2026-09-28, after the standup where we heard Zapmail had added a way to
delay domain purchases and other new ways to buy domains.

## Short answer

**We found no delayed or scheduled domain purchase feature in anything Zapmail
has published.** Every buy we can see charges and registers right away.
The only thing on our side that spreads purchases over days is our own batch
plan (`zapmail_buy.py`), so we keep it.

It may exist in the dashboard without being documented yet. Question 18 in
`ZAPMAIL_QUESTIONS_2026-09.md` asks Zapmail directly. If anyone has a
screenshot or link, that settles it faster.

## What we checked

| Source | What it says about buying |
|---|---|
| API reference (all 124 endpoints) | One buy call: `POST /v2/domains/buy`. Fields: domain name, years, pay from wallet or get a payment link, DNS Shield on/off. No date or schedule field. `quick-setup` (buy + mailboxes in one call) has no schedule field either. |
| MCP server docs | "Purchases charge immediately, not on preview." "If your wallet balance covers the cost, a purchase or renewal tool completes the charge in that same call." |
| Help center, buy-a-domain guide (updated Apr 2026) | Search, add to cart, pay. Nothing about scheduling. |
| Help center, Bulk Domain Finder (updated Apr 2026) | Upload a CSV of up to 100 names; available ones go into one cart; one payment. |
| Help center, domain FAQs (updated Jan 2026) | Nothing about timing. |
| Webhooks | Events for domain/mailbox status changes, exports, placement tests. None for a scheduled purchase. |

The one "scheduled" thing in Zapmail is `POST /v2/mailboxes/schedule`, which
creates **mailboxes** on the next subscription renewal date. It is deprecated,
and it is about mailboxes, not domains.

## What Avinash may have meant

These are real, current Zapmail features that fit "don't buy everything
together" or "buy domains in other ways":

1. **Zap Shield (DNS Shield).** Zapmail says it uses an isolated IP pool and
   rotates DNS zones so your domains don't share a "bad neighbourhood". That
   targets the same problem as spreading purchases: domains that look like
   one batch. It also speaks to our open concern that 68 of 97 domains sat on
   one forwarding IP. **Worth testing on the next batch.** Costs money (monthly
   or lifetime plan); our code can already buy it, gated like every other spend.

2. **Pre-warmed domains and mailboxes.** Already aged and warmed, available
   for **Google and Microsoft 365**, delivered instantly, 12-month life, can't
   be renewed. Skips the warmup wait, and is a quick way to add the
   **Outlook senders** we need for Outlook-heavy lists like accounting. **Worth
   pricing.**

3. **High-reputation (aged) domains.** A marketplace of older domains, up to 50
   per purchase. Older registration dates, so they don't look like a fresh
   batch.

4. **Bulk Domain Finder.** A CSV upload of up to 100 names, straight into a cart.
   Faster for buying many domains at once, which is the opposite of spreading
   them out.

5. **Buy elsewhere, then connect.** Domains registered at another registrar
   (e.g. Cloudflare) can be connected to Zapmail. This is the only documented
   way to actually spread registrars, and our connect flow
   (`zapmail_lifecycle.py --connect`) already supports it.

## Recommendation

- **Keep our spread-out batch plan.** It is the only thing that delays
  purchases, and it now enforces each batch's date.
- **Ask Zapmail** questions 18–20 (delayed purchase, whether domains bought
  together share a registrar and IP, and pre-warmed Microsoft pricing).
- **Try Zap Shield on the next small batch** and compare placement against
  domains without it.
- **Price pre-warmed Microsoft 365 mailboxes** for the Outlook shortfall. Our
  code can read available counts for free (`prewarmed_count`).
- **If Zapmail confirms a native delay feature on the API,** we can point our
  batch plan at it and drop our own date check. Until then there is nothing
  to switch to.

## Sources

- API index: https://docs.zapmail.ai/llms.txt
- Buy endpoint: https://docs.zapmail.ai/get-domains-purchase-payment-link-13521209e0.md
- Quick setup: https://docs.zapmail.ai/quick-setup-33476112e0.md
- Schedule mailbox creation (deprecated): https://docs.zapmail.ai/schedule-mailbox-creation-26753639e0.md
- MCP: https://docs.zapmail.ai/mcp-2408514m0.md
- Webhook events: https://docs.zapmail.ai/get-event-types-43298285e0.md
- Buy a domain: https://help.zapmail.ai/en/articles/9527479-how-to-buy-a-new-domain-in-zapmail-a-step-by-step-guide
- Bulk Domain Finder: https://help.zapmail.ai/en/articles/12432974-how-to-use-bulk-domain-finder-in-zapmail
- Domain FAQs: https://help.zapmail.ai/en/articles/9539080-faqs-domain-management-in-zapmail
- Pre-warmed guide: https://help.zapmail.ai/en/articles/13358418-pre-warmed-mailboxes-in-zapmail-complete-guide
- Zap Shield: https://zapmail.ai/features/zapshield
- Move domains between workspaces/providers: https://help.zapmail.ai/en/articles/13242432-how-to-move-domains-between-workspaces-or-providers-in-zapmail
