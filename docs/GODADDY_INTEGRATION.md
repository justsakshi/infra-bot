# GoDaddy integration (domains)

Decided 2026-10-09: sending domains are bought on GoDaddy (cheaper than ScaledMail's $15.50 / $17 renewal), and the mailboxes come from ScaledMail on those domains. ScaledMail's team sets up DNS once the nameservers point to them.

## Prices (GoDaddy v3 API, docs read 2026-10-09)
| .com | First year | Auto-renewal |
|---|---|---|
| GoDaddy v3 API | $9.79 | $14.99 |
| ScaledMail (for comparison) | $15.50 | $17 |

The discounted rate applies only to purchases made through the v3 API, not the GoDaddy website.

## Setup
| Var | Value |
|---|---|
| `GODADDY_PAT` | Personal Access Token from the GoDaddy developer dashboard, scopes `domains.domain:read`, `domains.domain:create` and update (nameservers). v3 does not accept the old sso-key |
| `GODADDY_ALLOW_SPEND` | leave unset; set to `true` on the command line for the one run that buys |
| `GODADDY_ASSET_SYNC_ENABLED` | `true` lets the 9:37 sync write the tracker |
| `GODADDY_DOMAIN_PRICE_CEILING` | default `15` dollars a domain (quote above it is refused) |
| `GODADDY_PLAN_CEILING` | default `300` dollars a plan |

The GoDaddy account needs a payment method on file (or a Good as Gold balance) to register through the API.

## Buying (runbook)
1. Names from the suggester only. `godaddy_cli.py check a.com b.com` shows availability, first-year and renewal price, and any naming problem (client brand, cold-email cliche, not .com).
2. Stage (free): `python3 godaddy_cli.py stage --client "Precise Leads" --domains a.com,b.com --ns <ScaledMail nameservers>`
3. Place (charges the account; a human said "place <id>"):
   `GODADDY_ALLOW_SPEND=true python3 godaddy_cli.py place <plan_id> --approve --user <name>`
   For each domain: quote (locks the price; refused above the ceiling or with premium fees) → register with a fixed Idempotency-Key → poll → set the nameservers.
4. A timeout leaves the domain `unknown`. Placing the plan again first looks the domain up in our account, then replays the same key (GoDaddy dedupes), so nothing is bought twice. A definite failure retries with a new key.
5. Order the mailboxes on ScaledMail for those domains (see `SCALEDMAIL_INTEGRATION.md`).

## Live lessons
- 2026-10-09: a quote succeeds even with no card on the account. The first purchase then returned `422 no chargeable payment profile found for shopper`. Nothing was charged. A successful quote does not prove the account can pay, so the bot stops a plan at the first billing refusal.
- The live quote puts `price` and `fees` at the top level, not under `items[0]` as the docs show. `quote_terms` reads both.
- Payment: GoDaddy's API purchases cannot do a card's extra authentication step (their docs say so for EEA cards). Indian cards normally ask for an OTP, so use a prepaid **Good as Gold** USD balance for API purchases.

## Tracking
`godaddy_cli.py sync` (cron 9:37 IST): every GoDaddy domain's expiry follows GoDaddy; domains the bot bought get a tracker row with the client from the purchase ledger (`godaddy_domain_plans`). Domains with no known client are reported, not added.

## Tests
`cd smartlead_sync && python3 -m pytest test_godaddy.py -q` (15: gates, name rules, never buying twice, price jumps, sync).
