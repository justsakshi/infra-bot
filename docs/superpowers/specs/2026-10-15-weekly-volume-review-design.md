# Weekly volume review: raise or lower each inbox's daily limit, one click to apply

Date: 2026-10-15 · Status: draft for Manveen's review

## Why

Inbox intake sets each new inbox's daily campaign limit once (Google 15,
Outlook 5, SMTP 7). Support from all three providers (15 Oct) says to raise it
slowly while the inbox stays healthy and to step back when it slips. Nobody
does this today, so limits stay at their starting value forever.

## The rule (ScaledMail's, adopted 15 Oct; Outlook maximums set by Manveen)

From `smartlead_sync/inbox_standard.json`:

| Type | Start | Weekly step | Maximum |
|---|---|---|---|
| Google | 15 | +2 | 20 |
| Outlook, 3 per domain (Zapmail) | 5 | +1 | 10 |
| Outlook, 25 per domain (ScaledMail) | 5 | +1 | 8 |
| SMTP | 7 | none yet | 7 (until ScaledMail answers) |

Signals per inbox, from Smartlead support:

- **placement**: its last two counting placement tests (kill rule) both passed (80%+), a week or more apart;
- **bounces**: under 2% over the last 14 days;
- **warmup reputation**: 90% or more.

Decision per inbox, once a week:

- **raise** by the type's step (never above the maximum) when all three
  signals are good and its limit has not changed in the last 7 days;
- **hold** when one signal slips (or there is not enough data yet);
- **step back** by one step (never below the start) when two or more slip.

Two failed tests in a row is already "stop using it" in Kill warnings; the
review leaves those inboxes to that.

## Which inboxes

Only inboxes intake set up (tag `Bot-Setup`), past their ready date, and in a
campaign that sent in the last 7 days. Older inboxes and types with no step
are never touched.

## What the team sees

Tuesday after Kill warnings (about 13:45 IST), one message in the infra
channel:

```
Volume review, 21 Oct: raise 18 inboxes, hold 4, step back 1
Precise Leads: raise 15 (Google 15 -> 17) · hold 2      [Apply Precise Leads]
Bettrdata: raise 3 (Outlook 5 -> 6) · step back 1 (thebettrdatas.com 6 -> 5)   [Apply Bettrdata]
Held: 2 inboxes bounced 2-4% this fortnight; 2 have one failed test
```

Apply buttons are per client, approvers only, with a confirm dialog. Nothing
changes until someone clicks. Clicking writes each inbox's new daily limit,
reads it back from Smartlead, and replies with what changed.

## What the bot remembers

Each inbox's approved limit and its history go into the intake ledger
(`inbox_intake`). The drift check compares an inbox to its last approved
limit, not the starting value, so an approved raise is never reported as
"changed since setup".

## Data

- Placement tests: the same read Kill warnings uses (`renewal_review.smartlead_facts`).
- Bounces and sends per inbox over 14 days: Smartlead's
  `GET /analytics/mailbox/name-wise-health-metrics` (start_date, end_date,
  timezone), named by Smartlead support on 15 Oct. Its exact fields are
  checked live before building.
- Warmup reputation: the email account list.

## Out of scope

Warmup settings (auto-adjust handles warmup as limits rise, per Smartlead),
adding inboxes, and campaign assignment.

## Tests

- Pure: the decision for every signal combination, step and maximum per type,
  the 7-day cooldown, types with no step, ready-date and in-campaign filters,
  the message text.
- Apply: writes only the approved inboxes of that client, reads back, records
  the approved limit; a failed write is reported, not retried silently.
- Drift: an approved raise is not drift; a manual change still is.
- Live, before enabling: a dry run printed for this week's inboxes.
