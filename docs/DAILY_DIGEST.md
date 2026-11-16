# Daily digest

The inbox channel gets one post a day, at 11:00 IST, addressed to Manveen. Before 16 Nov 2026 every job posted its
own report (about 15 posts a day). Manveen, 16 Nov: "if you would just spam that every check 15 minutes or 30
minutes throughout the day nobody will read that and nothing will happen".

## What the post says

1. **Things that need you today**, each with the exact step ("Do: ..."). Up to 12 in the post, the rest in the thread.
   An ask open for more than a day says since when.
2. **Done by the bot since the last digest**: what the pipeline changed by itself (inboxes pulled from campaigns,
   tagged, returned, test campaigns filled, copy refreshed, inboxes set up).
3. **Checks that broke**: a job that could not read a source or crashed, the PC's v2 job silent for 3 hours, Render
   behind main.
4. **Thread**: each job's full report from the last 30 hours.

No asks, nothing done and nothing broken: no post that day.

**Urgent** items post at once and stay in the digest until fixed: a payment failed, a subscription not renewed or
cancelled, a Smartlead key rejected, a live campaign losing half its inboxes or left with only failed ones. Each
posts once; if it is fixed and comes back later, it posts again.

The noon "Daily Renewal Check" card (only when something expires within 3 working days) stays as it is: its
buttons are the step.

## Every job, before and now

| Job (digest source) | When (IST) | Before | Now |
|---|---|---|---|
| v2 weekly (`v2-weekly`, PC) | hourly | post per run with "Needs you" | asks; pulls, tags, returns are done lines |
| Inbox list (`inbox-status`) | 10:00 | daily post | asks (disconnected, half lost: urgent) |
| Billing watch (`billing-watch`) | every 2 h from 10:05 | post per alert | urgent ask per failed / not renewed / cancelled bill; new and gone kept 3 days |
| Renewal review (`renewal-review`) | 09:50 | post when bills are due | one ask per bill with inboxes to drop or check |
| Zapmail digest (`zapmail-digest`) | 09:40 | daily post | asks (wallet, expiring domains, batches, ScaledMail orders); status lines in the thread |
| API keys (`key-health`) | 09:45 | alert | rejected key: urgent ask; unreachable: broken line |
| Campaign stats job (`campaign-metrics`) | 09:00 | failure post | broken line naming the source and the error type |
| Daily sheet sync (`smartlead-sync`) | 10:00 | failure post | broken line |
| Test campaigns (`dt-senders`) | 06:00 | post | done lines; untracked domains: ask |
| Test copy (`copy-sync`) | Fri-Sun 18:30, Mon/Tue 05:00 | post | done line; fixable problems: ask |
| Blocklists (`blacklist`) | Mon 09:00 | post | thread only; a check that could not run: broken line. A listing is never an ask |
| Fleet watch (`fleet-watch`) | 10:30 | post per new case | one ask per open kind; blocklist listings thread only |
| Fleet scorecard (`fleet-scorecard`) | Mon 10:50 | post | thread only |
| Warm-up repair (`warmup-repair`, inside fleet watch) | 10:30 | new (17 Nov 2026) | Mondays one done line per client ("Warm-up repair, BettrData: N repairing, N recovered this week, N worse."); inboxes 3 weeks in repair without improving: one ask (cancel candidates, his call); per-inbox lines in the thread. Report-only: changes no warm-up setting. Mongo `warmup_repair`; seed and preview: `warmup_repair_cli.py` |
| Gate self-test (`gate-selftest`) | 11:30 | Monday post, and on a disagreement | silent when all agree; held-back inboxes still sending: ask; other disagreements: broken line |
| Reply check (`reply-monitor`) | 13:00 | ALERT posts, Monday summary | ALERT domains: one ask; summary in the thread |
| Kill warnings (`kill-report`) | Tue 13:30 | post | asks (still sending, idle in live campaigns, domains to stop using) |
| Real-recipient check (`real-recipients`) | Sun 07:00 | post | trust change: done line; report in the thread |
| Classic seed test (`placement-report`) | Mon/Tue 08:00-13:00 | post per client | thread only (being retired, see below) |
| New inboxes (`inbox-intake`, `inbox-ready`) | every 2 h | post | done lines; problems a person fixes: ask |
| Bot health, restart, 4 PM reminder, v2 watch (bot side) | various | posts | silent; a failed run is a broken line |

Jobs that run after 11:00 (gate self-test, reply check, kill warnings, the later billing-watch runs) appear in the
next morning's digest, unless urgent.

Not in the digest, on purpose: campaign stats and the Microsoft/Proofpoint watch post to the team's daily channel,
not the inbox channel. Capacity, placement summary/batch/collector, bounce protect, auto retest and the tracker
syncs post nothing themselves.

## Classic seed test: retired as a signal (16 Nov 2026)

The v2 weekly test is the one the pipeline acts on. The October Google batch passed the classic test and warm-up
and was in Google spam on v2, so the classic test is not trusted. Its reports go to the thread only. Melior's and
BettrData's classic schedules end 29 Nov 2026 and are not recreated; stopping PL's (they run to Oct 2027) is
Manveen's call, after the Saturday v2 runs prove themselves.

## Undo

`DIGEST_MODE=off` on Render (Environment) and in the PC job's `.env`: every job posts exactly as before and the
11:00 digest job is not scheduled. When Mongo cannot be written, a job posts the old way for that run, so an ask
is never lost.

## Go-live order

1. Render deploys main (the 11:00 `digest` job lives there).
2. Then the PC's v2 checkout moves to main. The other way round, the PC job would hand its asks to a digest that
   nobody posts yet.

## Code

- `smartlead_sync/smartlead/digest.py`: `Ask`, `Done`, `report(source, asks, done, detail)`; urgent posting.
- `smartlead_sync/digest_post.py`: builds and posts the digest (`--post`; without it a dry run).
- Mongo: `digest_asks` (one doc per source), `digest_done`, `digest_urgent`, `digest_meta`.
- Each job's mapping: its `digest_items` / `digest_asks` function, with tests for digest on, fallback and off.
