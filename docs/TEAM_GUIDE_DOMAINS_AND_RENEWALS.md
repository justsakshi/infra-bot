# Domains, inboxes and renewals — team guide

One page. What each Slack message means, which one to trust, and what to do.

## Where things live

| What | Where |
|---|---|
| Every domain and inbox we pay for, any provider | **/infra tracker** (dashboard: infra-bot-1.onrender.com) |
| Zapmail domains, mailboxes, wallets, renewals | **Zapmail** — the bot reads it directly |
| Inboxkit, ScaledMail | Their own dashboards — the bot cannot read them; the tracker is updated by hand |

Zapmail is the truth for Zapmail assets. The tracker is the truth for everything else.

## The daily Slack messages

| Message | Covers | What to do |
|---|---|---|
| **⏰ Daily Renewal Check** (10:00) | Everything in the /infra tracker, all providers | Renew or let lapse each item listed. A line marked *estimated* has no expiry date saved — check the provider first, then save the real date on the dashboard. |
| **📬 Zapmail daily** | Zapmail accounts only | Act on anything under a heading with a count > 0. It will not list Inboxkit / ScaledMail items — that is expected. |
| **🔔 Reminder** (16:00) | Nudge only | Nothing new — finish anything from the morning check. |

**If the two disagree** (e.g. "5 expire today" vs "0 expiring"):
1. Open *Read more* on the Daily Renewal Check. Look at the provider on each line.
2. Not Zapmail (Inboxkit, ScaledMail) → the Zapmail digest can't see it; the Renewal Check is right. If it says *estimated*, confirm the date in that provider.
3. Zapmail → run `/domains zapmail domain <name>`. Zapmail's date wins; the 9:30 tracker sync corrects the tracker (or press **Update tracker** on the look-up).

## Past clients

Current clients: **Belardi Wong, Precise Leads, BettrData, Melior**. Anything for another client is being left to lapse. Don't renew it. Set it to *Inactive* on the dashboard so it stops appearing.

## Slack commands (only people on the allowed list; replies are visible only to you)

| Type | Does |
|---|---|
| `/domains` | Pick a client → get available sending-domain names → tick → **Stage purchase** |
| `/domains zapmail` | Zapmail menu: status, renewals, purchase plan, digest, look up a domain, tracker sync |
| `/domains zapmail domain x.com` | One domain: account, client, health, expiry, mailboxes, action buttons |

Buying, renewing, creating mailboxes and exporting are buttons for approvers only, and each asks before doing anything.

## Asking for access

Send your Slack member ID (profile → ⋯ → Copy member ID) to an admin. They add it to `DOMAINS_ALLOWED_USERS`. Approvers are listed in `ZAPMAIL_APPROVERS`.
