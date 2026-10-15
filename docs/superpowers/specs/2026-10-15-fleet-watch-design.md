# Fleet watch: a daily record of every inbox, what to do when one goes bad, and whether it helped

Date: 2026-10-15 · Status: approved in chat by Manveen ("write the script, build, and everything")

## Why

Manveen wants to know, for every inbox, when something goes bad and what to do,
and to learn from the data which actions work. Today the bot keeps a daily
health score per inbox (`inbox_health_history`) and every placement test
(`placement_results`), but not what each inbox sent, bounced and got back each
day, nor which campaigns it was in. The "All Inboxes" sheet tab has those, and is
overwritten every day.

## The flow (Manveen, 15 Oct)

1. **Monitoring, now until the first batch goes out (27-28 Oct):** the bot posts
   problems with a suggested action, changes nothing; Manveen reads them.
2. **Test on the batch (27 Oct - 18 Nov):** the 11 new Precise Leads Google
   domains; on the 6 pilot domains the bot acts (volume review), the other 5
   are the comparison. Manveen runs their campaigns.
3. **Decide (~18 Nov):** a scorecard says, per kind of suggestion, how often it
   fired, how often it was acted on, and what happened after; Manveen decides
   which become automatic.

## Parts

### 1. Daily record (`inbox_daily`, one document per inbox per day)

Every inbox in the current-client Smartlead accounts (Precise Leads, BettrData;
past clients excluded as elsewhere). Daily at 10:30 IST:

- state as of the run: client, domain, type, vendor tag, daily limit, warmup on,
  warmup reputation, connected (SMTP and IMAP), campaigns it is in and whether
  they sent in 7 days, `Bot-Setup`, ready date, newest placement test and the
  kill-rule verdict;
- that day's campaign numbers: sent, bounced, replied, unsubscribed, from
  Smartlead's per-inbox analytics. They arrive about 6 days late, so each run
  re-reads the last 10 days and fills them in (upsert by inbox and day).

Mongo through the same `_db()` helper as the intake ledger.

### 2. Problems and what to do (`fleet_cases`)

Checks run on the record each day. Each problem opens one case (inbox or
domain, problem kind, opened on, evidence, suggested action); the case closes
the day the check stops firing.

| Problem | Judged on | Suggested action |
|---|---|---|
| Warmup off | inbox | Turn warmup back on; it stays on while sending |
| Disconnected | inbox | Reconnect it in Smartlead |
| Bounces 4%+ of 100+ sends (14 days) | domain | Lower the domain's limit and check the lead list |
| Bounces 2-4% of 100+ sends | domain | Keep volume, check the lead list |
| One failed placement test | inbox | Keep volume, retest next Monday |
| Kill list (two failed tests, under 50%, dead domain) | inbox | Take it out of campaigns, do not renew |
| Warmup reputation under its type's level 7 days running | inbox | Keep volume, check SPF/DKIM/DMARC |
| SMTP reputation under 50% after 7 days of warmup | domain | Ask ScaledMail to rotate the IP |
| On a blocklist (`blacklist_checks`) | domain | Pause the domain, request delisting |
| In a campaign before its ready date | inbox | Take it out until it is ready |
| Sending but no placement test in 21 days | domain | Add it to Monday's test |
| Daily limit differs from the approved one | inbox | Confirm the change was meant |

### 3. Was it acted on, and did it help

Each case records the evidence when it opened and the same numbers 14 days
later. "Acted on" is read from what changed, not from a click:

- warmup back on; reconnected;
- limit lowered, or removed from campaigns (bounces, kill list, early campaign);
- retested within 10 days (failed test, no test);
- reputation back over its level (low reputation, SMTP IP).

### 4. Messages

- **Fleet watch, daily 10:45 IST** (`FLEET_WATCH_CHANNEL`, default #test-channel
  C0AV3HTPP0S): new cases only, grouped by action, plus counts of open cases.
  Nothing new = no post.
- **Scorecard, Mondays and on demand** (`fleet_watch.py --scorecard`): per
  problem kind, cases opened, acted on, cleared within 14 days (acted vs not),
  median days to clear; the pilot vs comparison domains side by side.

Nothing in Fleet watch writes to Smartlead. The only automatic changes stay
those already approved: intake setup, the test-campaign attach, and the volume
review on the pilot domains.

## Out of scope

Automatic fixes (decided after the scorecard), reply sentiment, client-level
campaign analytics, a dashboard UI.

## Tests

Pure: each check (fires, does not fire, edge of each threshold), case open and
close, "acted on" inference, the 14-day outcome, message and scorecard text.
Store: upsert by inbox and day, backfill overwrite. Live: one dry run printed
before the first post.
