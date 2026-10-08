# ScaledMail integration

ScaledMail is our second mailbox provider, after Zapmail. It sells Google, Outlook and SMTP mailboxes as monthly orders. This page covers what the bot does with ScaledMail, the safety gates, and the setup. Built on 2026-10-08.

## What the team can do

Slack: `/domains sm` (or `/domains scaledmail`) opens the menu. `/scaledmail` works too, but only after someone registers that command in Slack and sets `SCALEDMAIL_SLASH_COMMAND=/scaledmail`. The Zapmail menu also has a **ScaledMail →** button.

| View (everyone on the domains app) | Shows |
|---|---|
| Fleet & cost | Monthly total, every active order (client, mailboxes, billing day), domains and mailboxes per provider and per client, unused registered domains |
| Renewals & billing | Orders billing in the next 14 days, domain registrations renewing in the next 60 |
| Orders | Every order. Approvers get an overflow menu with Set client and Cancel order |
| Look up a domain | Client, order, billing day, renewal, redirect and mailboxes, plus action buttons |
| Price a volume | ScaledMail's own calculator: domains, mailboxes and $/month for N emails a month, any Google/Outlook/SMTP split, at the low, medium or max sending level |
| Find domains | Available names for a keyword, with first-year and renewal prices (blacklisted names are dropped) |
| Order mailboxes | A form that checks availability, prices the order and records a **plan**. Nothing is bought |
| Purchase plans | The ledger. Planned plans get **Place order**; plans with an unknown result get **Reconcile** |
| Placement reports | ScaledMail's weekly placement test. It is a paid add-on and is off today |
| Pre-warmed | ScaledMail's pre-warmed stock and prices (0 in stock on 2026-10-08) |
| Tracker sync | Preview of the ScaledMail → /infra tracker changes. Approvers get **Apply** |

| Action (approvers only, each asks first) | Cost | Notes |
|---|---|---|
| Set client | free | Sets the order tag. Every domain in that order then counts as that client |
| Rename senders | free | Changes the display name only; the addresses stay, so no re-warm. ScaledMail reviews and applies it |
| Change redirect | free | ScaledMail reviews and applies it |
| Replace domain | free | Swaps a burnt domain for an unused one we already bought on ScaledMail. The mailboxes need warming again |
| Place order | **charges the card** | Also needs `SCALEDMAIL_ALLOW_SPEND=true`. Re-checks availability first. Sent once and never retried |
| Cancel order | stops every mailbox in the order | Also needs `SCALEDMAIL_ALLOW_CANCEL=true`. ScaledMail can only cancel a whole order, never one domain |

CLI: `smartlead_sync/scaledmail_cli.py`. Run `python scaledmail_cli.py -h` for every command. Add `--json` for one machine-readable line.

## Daily automation
- **09:35 IST, tracker sync** (`scaledmail_cli.py sync`). It writes only when `SCALEDMAIL_ASSET_SYNC_ENABLED=true`; otherwise it logs a preview.
  - Domain expiry follows ScaledMail's `renewal_at`.
  - Inbox expiry moves to the order's next billing day, but only when the tracker's date is missing or already past.
  - Status only goes down: a cancelled order or a domain that is gone becomes Inactive.
  - New rows are added only for current clients, and only once the domain is Active.
  - Nothing is deleted, and team fields are never overwritten.
- **09:45 IST, digest** (`scaledmail_cli.py digest --post`). It posts to `SCALEDMAIL_NOTIFY_CHANNEL` (or `ZAPMAIL_NOTIFY_CHANNEL`), and only when one of them is set. It covers:
  - orders billing in the next 3 days
  - domain registrations renewing in the next 14 days
  - domains still being set up
  - domains with no client
  - flagged placement results
- Both jobs skip quietly when `SCALEDMAIL_API_KEY` is missing.

## Safety model
| Layer | Enforces |
|---|---|
| `ScaledMailClient` | WRITE needs `approve=True`. CANCEL also needs `SCALEDMAIL_ALLOW_CANCEL=true`. SPEND (`create-custom-order`, `create-order`, `buy-domains`, `buy-pre-warm-inboxes`) also needs `SCALEDMAIL_ALLOW_SPEND=true` and is single-shot |
| Order ledger (Mongo `scaledmail_order_plans`) | `planned → in_progress → placed / failed / unknown`. The claim is atomic, so two clicks cannot order twice. `unknown` (timeout or 5xx) stays blocked until **Reconcile** finds the order's tag. Not finding the tag never marks a plan failed; after checking the ScaledMail UI, a human runs `scaledmail_cli.py mark-failed <plan>` |
| Ceilings | `SCALEDMAIL_DOMAIN_PRICE_CEILING` (default $25 a domain) and `SCALEDMAIL_ORDER_MONTHLY_CEILING` (default $500 a month per order) |
| Slack | Only approvers can press the buttons. Typed commands are read-only, and arguments are checked against strict patterns, so nothing can be passed as a flag |
| Passwords | The bot never asks ScaledMail for mailbox passwords (`password=true` is never sent) |

The order tag is `<client>-<plan id>`, for example `melior-a1b2c3`. ScaledMail's order list therefore shows which client and which plan every bot-placed order came from.

## Client of a domain
The bot decides in this order:
1. the /infra tracker's client;
2. the order tag;
3. the brand in the redirect (`getmelior.com` means Melior, `preciseleads.in` means Precise Leads);
4. the brand in the domain name;
5. otherwise **unassigned**. The bot never guesses a client.

On 2026-10-08, order `recLP7iFrjZ27pq94` (agencyforumco.com, peerplaybook.com, peeragencies.com; 75 Outlook mailboxes, $150 a month) is unassigned. Use **Set client** on it.

## Live facts (2026-10-08)
- **Account:** organization `recIzq3Ln9Dc8B5Wm` ("preciseleads").
  - 4 active orders, $491.50 a month in total.
  - 28 domains: Melior has 10 Google and 2 Outlook, Precise Leads has 13 Google, and 3 Outlook are unassigned and still being set up.
  - 1 unused registration: `preciseleadshq.com`.
- **Prices** (the API returns dollars, even though its docs say cents):
  - Google: $3.50 a mailbox, 2–4 mailboxes per domain
  - Outlook: $50 a domain of 25 mailboxes
  - SMTP: $3.75 a domain of 4 mailboxes
  - New .com domain: $15.50 the first year, $17 to renew
- **ScaledMail's own sending assumptions per mailbox per day** (low / medium / max level):
  - Google: 15 / 20 / 25
  - Outlook: 5 / 8 / 10
  - SMTP: about 7 / about 8 / 10
- **Mailbox lists can be wrong.** The Outlook mailbox list ScaledMail returns does not always match reality. For `growmelior.com` it lists 25 addresses that exist nowhere, while Smartlead and the tracker agree on 25 others. So the sync never adds new inbox rows on a domain the tracker already has inboxes for.

## Not possible through ScaledMail's API (as of 2026-10-08)
- Cancelling or removing a single domain or mailbox. Only whole orders can be cancelled.
- Pushing mailboxes into Smartlead. The `sequencer` field on an order is documented for Instantly only. Connect Smartlead the way the team does today, or ask ScaledMail support whether Smartlead works there.
- Webhooks. There are none, so the daily sync and digest do the watching.
- Mailbox health or warmup data. Our Smartlead placement tests (Mon/Tue) cover this.

## Setup (Render)
| Var | Value |
|---|---|
| `SCALEDMAIL_API_KEY` | the API token from app.scaledmail.com/settings (a JWT starting `eyJ`) |
| `SCALEDMAIL_ORG_ID` | optional; with one organization the bot finds it |
| `SCALEDMAIL_APPROVERS` | Slack member ids allowed to press ScaledMail buttons (falls back to `ZAPMAIL_APPROVERS`); these users are also let into the domains app |
| `SCALEDMAIL_ASSET_SYNC_ENABLED` | `true` lets the 9:35 sync write the tracker |
| `SCALEDMAIL_NOTIFY_CHANNEL` | channel id for the 9:45 digest |
| `SCALEDMAIL_ALLOW_SPEND` | leave unset until the team wants Slack ordering |
| `SCALEDMAIL_ALLOW_CANCEL` | leave unset until needed |
| `SCALEDMAIL_SLASH_COMMAND` | `/scaledmail`, only if that command is registered on the domains Slack app |

## Tests
```
cd infra-bot\smartlead_sync && python3 -m pytest test_scaledmail.py -q     # 44: gates, routing, billing, ledger, sync
cd infra-bot && node test_scaledmail_command.js                             # 29: routing, rendering, approver gates, form checks
```
