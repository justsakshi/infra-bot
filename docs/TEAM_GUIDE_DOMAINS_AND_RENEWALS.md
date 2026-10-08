# Domains, inboxes and renewals — team guide

One page: where to look, what each Slack message means, and what to do. Zapmail and ScaledMail are handled together; you never need to know which provider a domain is on.

## One place: `/domains`

Type `/domains`, or DM the **Domain Suggester** app "hi". You get one menu:

| Button | Does |
|---|---|
| Suggest for <client> | Available sending-domain names, with Zapmail **and** ScaledMail prices (2-4 min). Tick names → **Buy on Zapmail** or **Buy on ScaledMail (with inboxes)**. |
| Fleet & cost | Every account and order on both providers: domains, inboxes, monthly cost |
| Renewals & billing | What bills or renews soon on both providers: inbox subscriptions (with any **payment failed**), orders, domain registrations |
| Look up a domain | Finds it on whichever provider has it, with that provider's buttons (mailboxes, retire inboxes, rename senders, redirect, export, auto-renew…) |
| Purchases | Staged and placed buys on both providers |
| Order mailboxes / Price a volume | Price and stage new inboxes; nothing is charged until an approver confirms |
| Pre-warmed · Tracker sync · Today's digest | Both providers in one message |
| Infra audit | Every setup check in one run (~1 min): MX + DMARC policy, many domains on one name-server set, redirects to the client's site, sending from the main domain, >3 mailboxes or >100/day per domain, mailbox caps, inboxes under 21 days in live campaigns, warmup, signatures, sender names, provider mix, ESP matching, SMTP IP blacklists |
| Tracker: Expiring today / Next 7 days / Add / Renew / List | What Infra Bot's `/infra` does |

Typed versions: `/domains status` · `renewals` · `domain x.com` · `purchases` · `prewarmed` · `sync` · `digest` · `audit` · `suggest bettrdata` · `quote 30000 google,outlook 70,30 low` · `infra expiring 7`. The same words work in a DM to the app, as do CSV uploads and `renew` / `delete` + one name per line.

## How inboxes are paid for

Inboxes are **never renewed one by one**. They are seats in a monthly subscription (Zapmail) or order (ScaledMail), charged to the card on its bill date; every inbox in it renews. To stop paying for some inboxes:
- **Zapmail:** look up the domain → **Retire inboxes** (removed at the next bill; the domain stays; can be undone before then).
- **ScaledMail:** only a whole order can be cancelled (**Cancel its order**), never one domain.

Domains themselves renew yearly; **Renewals & billing** lists both.

## The daily Slack messages

| Message | Covers | What to do |
|---|---|---|
| **📬 Domains & inboxes daily** (9:40) | Zapmail + ScaledMail | Act on anything under a heading with a count > 0 — especially *inbox subscriptions billing in 3 days* and any **payment failed** line (those inboxes can be suspended). |
| **⏰ Daily Renewal Check** (10:00) | Everything in the /infra tracker, all providers incl. Inboxkit | Renew or let lapse each item. *Estimated* = no expiry saved; check the provider, then save the real date. |
| **🔎 Infra audit** (10:20 Mon-Fri) | Setup checks | Fix P0 today, P1 this week. |
| **🔔 Reminder** (16:00) | Nudge only | Finish anything from the morning. |

**If they disagree:** run `/domains domain <name>`. Zapmail's or ScaledMail's date wins; the 9:30 / 9:35 syncs correct the tracker. Inboxkit is not readable by the bot — the tracker is right for it.

## Past clients

Current clients: **Belardi Wong, Precise Leads, BettrData, Melior**. Anything for another client is left to lapse. Don't renew it; set it to *Inactive* on the dashboard.

## Who can do what

Views are open to everyone on the allowed list; replies are visible only to you. Buying, renewing, creating or retiring inboxes, exporting, cancelling and tracker writes are **approver-only** buttons and each asks first. Spending also needs the server switch (`ZAPMAIL_ALLOW_SPEND`, `SCALEDMAIL_ALLOW_SPEND`); ScaledMail cancelling needs `SCALEDMAIL_ALLOW_CANCEL`.

**Access:** send your Slack member ID (profile → ⋯ → Copy member ID) to an admin, who adds it to `DOMAINS_ALLOWED_USERS`. Approvers: `ZAPMAIL_APPROVERS` / `SCALEDMAIL_APPROVERS`.

_Old commands still work: `/domains zapmail …`, `/domains sm …`, `/infra …`._
