# Weekly volume review: raise or lower each inbox's daily limit, one click to apply

Date: 2026-10-15 · Status: REPORT ONLY (Manveen 15 Oct: "right now I just want you to report … make sure that we are completely sure before turning that on"). The Tuesday post (VOLUME_REVIEW_ENABLED=true) says what the rule would do and changes nothing; each week's suggestions are saved so they can be checked against what happened. Apply needs VOLUME_REVIEW_APPLY=true, which stays off until the rule is reviewed against research and proven on a test group.

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
| SMTP (ScaledMail) | 7 | +1 | 10 per inbox (ScaledMail, 15 Oct) |

Signals per inbox, from Smartlead support:

- **placement**: its last two counting placement tests (kill rule) both passed (80%+), a week or more apart;
- **bounces**: under 2% over the last 14 days;
- **warmup reputation**: 90% or more; SMTP 70% or more (ScaledMail 15 Oct:
  SMTP reputation runs lower than Google/Microsoft; under 50% they rotate the
  order's IP on request, so the review lists those domains).

Warmup is never switched off, also while sending (ScaledMail, all providers);
the drift check already reports "warmup off".

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

## Research, 15 Oct, and the revised rule (PROPOSED, not built)

Three research passes (mailbox providers' own docs, cold-email practitioners
and tools, and Smartlead's API checked live, read-only). Full reports with
sources: Obsidian vault `memory context/research/2026-10-15-inbox-volume/`.
Source labels: OFFICIAL = Google/Microsoft/Yahoo docs; VENDOR = sells inboxes,
warmup or sending; INDEPENDENT = no product.

What changes the design:

1. **Reputation lives on the domain (and on the IP for SMTP), not the inbox**
   (Google, Yahoo: OFFICIAL). Decisions must be per domain, with a per-domain
   daily cap. A ScaledMail 25-inbox Outlook domain at 5/day already sends 125;
   at 8/day it is 200, above ScaledMail's own blog figure of 50-75 per domain
   (VENDOR, conflicting with what they sell).
2. **No tool found raises volume automatically because an inbox looks
   healthy.** Vendor ramps run on a timer to a fixed cap; their health
   automation only pauses or cuts. Smartlead's Insights asks a person to
   approve every change.
3. **At 5-20 sends a day, per-inbox signals are mostly noise.** A 20-seed test
   at 16/20 fits a true placement anywhere from 58% to 92%. A healthy inbox
   (0.8% bounces, our fleet's rate) trips "2% bounces" 12-33% of weeks.
   Replies per inbox per week are 0-1. Pool by domain and over 2-4 weeks.
4. **Warmup reputation is Smartlead's own warmup-network placement** (it
   matched sent-minus-spam over the last 7 days on all 25 inboxes checked).
   Google and Outlook sit at 98-100%, so it never moves; SMTP swings about
   ±6-7 points from sampling alone. It should be an alarm, not a vote.
   Validity's Heatwave blocklist (3 Sep 2026) lists domains warmed with
   synthetic engagement; a listing is reportedly permanent.
5. **Bounces have two causes.** "Address does not exist" is a list problem;
   "blocked/refused" is a sender problem. Smartlead's
   `/campaigns/{id}/mailbox-statistics` gives `sender_bounce_count` per inbox
   and is current; the name-wise analytics run about 6 days behind.
6. **Lower fast, raise slowly.** Gmail reputation lags behaviour by 3-4 weeks;
   Google warns against sudden doubling (OFFICIAL). Outlook 5 -> 10 is a doubling.
7. **Total volume matters, not only the campaign limit.** Smartlead support:
   campaign + warmup under 25/day for Google, 10-15 for Outlook; warmup drops
   to 7-9/day in a live campaign when auto-adjust is on. To verify on our
   first ready inboxes (27 Oct) with `/email-accounts/{id}/warmup-stats`.
8. **New Microsoft tenants are capped** on external recipients (10% of normal
   under 31 days, 25% at 31-60, 500/day for trial tenants; rolling out
   Sep-Nov 2026, OFFICIAL).

Revised rule (proposal):

- **Protect first (alerts, then later automatic):** any one of these pauses
  sending on the domain: a blocklist or Heatwave listing; policy or
  spam-block bounces; sender bounces 4%+ on 100+ sends; a placement test
  under 50%; a Microsoft account restriction; two of its inboxes failing.
- **Step back by domain:** cut the domain's inboxes by a third for a week
  (Smartlead) up to half for two weeks (our playbook), not one small step.
- **Raise only by domain, only for Google and Zapmail Outlook:** every inbox
  on the domain clean, two placement tests both taken at the current limit,
  100+ sends with sender bounces under 2%, reply rate not in the bottom of
  same-provider domains, 2-week cooldown after a raise, 4 weeks after a step
  back, and the domain stays under its daily cap. ScaledMail 25-per-domain
  Outlook is never raised until ScaledMail confirms a per-domain ceiling and
  the tenant model in writing.
- **Proof before switching on:** 4-6 weeks report-only with every suggestion
  and its inputs saved; then compare it with what happened (would it have
  raised inboxes that later failed? how often did it flip?). Then a small test
  group with matched domains held flat as a control, step-backs before raises.
