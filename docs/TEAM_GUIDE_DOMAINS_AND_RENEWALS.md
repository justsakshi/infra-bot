# Domains, inboxes and renewals — team guide

One page. What each Slack message means, which one to trust, and what to do.

## Where things live

| What | Where |
|---|---|
| Every domain and inbox we pay for, any provider | **/infra tracker** (dashboard: infra-bot-1.onrender.com) |
| Zapmail domains, mailboxes, wallets, renewals | **Zapmail** — the bot reads it directly |
| ScaledMail orders, domains, mailboxes, billing days | **ScaledMail** — the bot reads it directly (`/domains sm`) |
| Inboxkit | Its own dashboard — the bot cannot read it; the tracker is updated by hand |

Zapmail and ScaledMail are the truth for their own assets. The tracker is the truth for everything else.

## The daily Slack messages

| Message | Covers | What to do |
|---|---|---|
| **⏰ Daily Renewal Check** (10:00) | Everything in the /infra tracker, all providers | Renew or let lapse each item listed. A line marked *estimated* has no expiry date saved — check the provider first, then save the real date on the dashboard. |
| **📬 Zapmail daily** | Zapmail accounts only | Act on anything under a heading with a count > 0. It will not list Inboxkit / ScaledMail items — that is expected. |
| **ScaledMail today** (9:45) | ScaledMail only | Orders billing in 3 days, domains renewing in 14 days, domains still being set up, domains with no client. Posts only when `SCALEDMAIL_NOTIFY_CHANNEL` is set. |
| **🔔 Reminder** (16:00) | Nudge only | Nothing new — finish anything from the morning check. |

**If the two disagree** (e.g. "5 expire today" vs "0 expiring"):
1. Open *Read more* on the Daily Renewal Check. Look at the provider on each line.
2. Inboxkit → the bot can't see it; the Renewal Check is right. ScaledMail → `/domains sm domain <name>` (the 9:35 sync keeps the tracker in step). If it says *estimated*, confirm the date in that provider.
3. Zapmail → run `/domains zapmail domain <name>`. Zapmail's date wins; the 9:30 tracker sync corrects the tracker (or press **Update tracker** on the look-up).

## Past clients

Current clients: **Belardi Wong, Precise Leads, BettrData, Melior**. Anything for another client is being left to lapse. Don't renew it. Set it to *Inactive* on the dashboard so it stops appearing.

## Easiest: message the Domain Suggester app

Open **Domain Suggester** in Slack (Apps → Domain Suggester → Messages) and type **hi**. You get one menu:

| Button | Does |
|---|---|
| Suggest domains | Pick a client → available sending-domain names (takes 3-4 min) |
| Zapmail / ScaledMail | Fleet, renewals, look-ups and their action buttons |
| Infra audit | Every setup check in one run (~1 min): MX + DMARC policy, many domains on one name-server set (Cloudflare footprint), redirects to the client's site, sending from the main domain, >3 mailboxes or >100/day per domain, mailboxes over their cap, inboxes under 21 days in live campaigns, warmup off / below cold volume, missing signature or sender name, one-provider-only, ESP matching off, SMTP IPs on blacklists. Also posts Mon-Fri 10:20 IST when `INFRA_AUDIT_CHANNEL` is set. |
| Expiring today / Next 7 days | Tracker rows about to expire, per client, with **Mark renewed** (tracker only) |
| Add asset / Renew asset / List all | Same as Infra Bot's `/infra add`, `/infra renew`, `/infra list` |

You can also just type what you want: `zapmail renewals`, `sm status`, `infra expiring 7`, `suggest bettrdata`, `zapmail domain x.com`. Sending a CSV (`domains.csv`, `renew_inboxes.csv`, `delete_domains.csv`) or a message that starts with `renew` / `delete` followed by one name per line works exactly as it does with Infra Bot.

## Slack commands (only people on the allowed list; replies are visible only to you)

| Type | Does |
|---|---|
| `/domains` | Pick a client → get available sending-domain names → tick → **Stage purchase** |
| `/domains menu` | The one menu above (domains, Zapmail, ScaledMail, tracker) |
| `/domains infra add · renew · list · expiring 7` | The /infra tracker features |
| `/domains zapmail` | Zapmail menu: status, renewals, purchase plan, digest, look up a domain, tracker sync |
| `/domains zapmail domain x.com` | One domain: account, client, health, expiry, mailboxes, action buttons |
| `/domains sm` | ScaledMail menu: fleet & monthly cost, renewals & billing, orders, look up a domain, price a volume, find domains, order mailboxes, purchase plans, tracker sync |
| `/domains sm domain x.com` | One ScaledMail domain: client, order, billing day, mailboxes; Set client / Rename senders / Change redirect / Replace domain / Cancel its order |
| `/domains sm quote 30000 google,outlook 70,30 low` | Domains, mailboxes and monthly price for a volume (nothing is ordered) |

Buying, renewing, creating mailboxes and exporting are buttons for approvers only, and each asks before doing anything.

ScaledMail: anyone on the list can price an order (**Order mailboxes** → staged plan). Only approvers can **Place order** (charges the card; the server must also have `SCALEDMAIL_ALLOW_SPEND=true`) or **Cancel order** (stops every mailbox in that order; needs `SCALEDMAIL_ALLOW_CANCEL=true`). ScaledMail cancels whole orders only, never one domain.

## Asking for access

Send your Slack member ID (profile → ⋯ → Copy member ID) to an admin. They add it to `DOMAINS_ALLOWED_USERS`. Approvers are listed in `ZAPMAIL_APPROVERS` and `SCALEDMAIL_APPROVERS`.
