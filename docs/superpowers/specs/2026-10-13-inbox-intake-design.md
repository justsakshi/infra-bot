# Inbox intake: set every new Smartlead inbox up once, then only watch

Date: 2026-10-13 · Status: approved by Manveen 2026-10-13 (option 1); ready-to-send tracking added on his ask

## Why

When ScaledMail or Zapmail delivers inboxes into Smartlead, nobody sets the
daily limit or warmup, and the three intake paths that exist each set
something different:

| How inboxes arrive | Set today |
|---|---|
| ScaledMail upload (`inbox_ready`, every 2 h) | signature, client, tags; no limit, no warmup |
| Zapmail job from the bot (`smartlead_setup` step) | name, signature, client; warmup 40/day, reply 25%, auto-adjust off |
| Zapmail export or pre-warmed done in the Zapmail app | nothing |

Live 2026-10-13: the 73 ScaledMail inboxes delivered today kept ScaledMail's
defaults (reply rate 65%, 16 warmups/day on 25-per-domain Outlook).

This replaces the 2026-10-08 rule "the bot never writes warmup" with: **the bot
writes each inbox's setup once, when it arrives, and never again.** The old
rule existed because a recurring job overwrote settings people made in
Smartlead; a one-time write cannot do that.

## The standard (one row per inbox type)

| Type | Daily limit | Warmup/day | Ramp/day | Reply rate |
|---|---|---|---|---|
| Google (ScaledMail or Zapmail) | 15 | 25 | +5 | 30% |
| Outlook, under 10 inboxes per domain (Zapmail) | 10 | 20 | +5 | 30% |
| Outlook, 10+ inboxes per domain (ScaledMail, 25 per domain) | 5 | 5 | off | 30% |
| SMTP (ScaledMail) | 7 | 20 | +5 | 30% |

Every type: warmup on, warmups on weekdays only, auto-adjust warmup/sending on,
signature from the client's profile (`inbox_profiles.json`), the Smartlead
client, a vendor tag (`ScaledMail-Google` …, `Zapmail-Google` …) and the client
tag. ScaledMail inboxes: no custom tracking domain (left empty).

Sources: daily limits from Manveen; Google/Outlook warmup and the 25-per-domain
Outlook cap from the InboxKit warmup guide (shared tenants: warmup at most 5
per mailbox); reply rate 30% from Smartlead (its warmup guide: 30-40%; our
2026-07 plan: never above 30, "an unnaturally high reply rate is itself a spam
signal"); SMTP limit 7 = ScaledMail's own planning number. Smartlead's API
accepts a ramp of 5-20 only, so ramps are +5 (or off).

The table lives in one file (`smartlead_sync/inbox_standard.json`) so it can be
changed without code.

## How intake works

Every 2 hours (same cron slot as `inbox_ready`, which this replaces), per
current-client Smartlead account (Precise Leads, BettrData; Melior's inboxes
sit in Precise Leads under client 12256):

1. **Find new inboxes:** in the account, created on or after `INTAKE_SINCE`
   (first run: 2026-10-12, which catches today's 73), not tagged `Bot-Setup`.
   Older inboxes are never touched.
2. **Wait for the whole delivery:** if the domain belongs to a ScaledMail order
   or a Zapmail job the bot knows, act only when all its inboxes are in
   Smartlead (today's `inbox_ready` rule). Unknown domains (manual exports,
   pre-warmed) are set up as they appear.
3. **Work out the type:** Google / Outlook / SMTP from the account's type and
   SMTP host; Outlook density from how many inboxes the domain has in
   Smartlead; vendor from the ScaledMail ledger, the Zapmail job, or the
   tracker.
4. **Write once:** update-account (`max_email_per_day`, `signature`,
   `client_id`, `custom_tracking_url` empty for ScaledMail), warmup
   (`warmup_enabled`, `total_warmup_per_day`, `daily_rampup`/`is_rampup_enabled`,
   `reply_rate_percentage`, `auto_adjust_warmup`, `send_warmups_only_on_weekdays`),
   then the tags incl. `Bot-Setup`.
5. **Prove it:** read each inbox back from Smartlead and compare every field
   with what was written. An inbox counts as set up only when the read-back
   matches; a mismatch is reported and the inbox is not tagged `Bot-Setup`,
   so the next run tries again (at most 3 times, then it is reported as stuck).
6. **Record and report:** each inbox's before/after goes to a Mongo ledger
   (`inbox_intake`), and one channel post says what was set up
   ("✅ Set up 50 Outlook inboxes for Precise Leads: limit 5, warmup 5, reply
   30%, signature, tags"), details in the thread.

Gate: the cron runs intake only with `INBOX_INTAKE_ENABLED=true`; until then
the old ScaledMail readiness job keeps running. A preview (writes nothing) is
run by hand: `python inbox_intake_cli.py`. The first live run is the catch-up
of today's 73, started by hand after Manveen reads its preview.

## Ready to send: tracked from the day warmup starts

Each inbox's warmup start is Smartlead's own `warmup_created_at` (for today's
ScaledMail inboxes: 13 Oct, when ScaledMail switched warmup on); intake also
records it in the ledger. An inbox is **ready to use** when both hold:

| Type | Warmup before use | And |
|---|---|---|
| Google | 14 days | Smartlead warmup reputation 90%+ |
| Outlook (either density) | 21 days | same |
| SMTP | 14 days | same |

Google 14 and Outlook 21 are Manveen's; SMTP 14 from Avinash (standup
2026-10-14: "at least two weeks, similar to Google"). The reputation check stops a date alone from clearing
an inbox that is still landing in spam. The days live in the same standard
file (`ready_after_days`), so they can be changed without code.

What the team sees:
- the intake post says each domain's ready date ("warmmeetings.com, Google:
  ready from 27 Oct");
- the 09:40 digest gets a line on the day inboxes become ready ("Ready to use
  today: 3 Google inboxes on warmmeetings.com"), and lists inboxes past their
  date but still under 90% reputation;
- the infra audit's "young inbox in a campaign" check uses each type's days
  instead of a flat 21.

## After intake: watch only

The infra audit already flags warmup off, missing signature, warmup below cold
sends and young inboxes in campaigns. It gains one check, `setup_drift` (P2):
an inbox tagged `Bot-Setup` whose limit, warmup/day, reply rate, weekdays or
auto-adjust no longer matches its type's row. It says what differs and what to
do; it never changes anything. A change someone made on purpose stays; the
report just keeps it visible.

## Also changes

- The Zapmail job step `smartlead_setup` uses the same standard instead of its
  own 40/day, 25%, auto-adjust-off values (one standard, two callers).
- `inbox_ready` is folded into intake; its order-history logging
  (`in_smartlead`, `ready`) stays.
- `SmartleadClient.set_warmup` gains `weekdays_only`.

## Out of scope

Campaign assignment, sender-name changes, Zapmail/ScaledMail ordering, and
warmup changes after intake (watch only).

## Tests

- Pure: type detection (account type, host, density), the standard lookup,
  the planned writes for each type, `Bot-Setup` and `INTAKE_SINCE` filtering,
  "wait for the whole delivery".
- Fake Smartlead: writes then read-back; a read-back mismatch leaves the inbox
  untagged and reported; retry limit; preview mode writes nothing.
- Live, before enabling: the preview on today's 73 inboxes, then the catch-up
  run, then a read-back of every inbox from Smartlead (the same fields shown in
  the Smartlead Bulk Update screen).
