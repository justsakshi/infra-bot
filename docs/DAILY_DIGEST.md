# Daily digest

The inbox channel gets one post a day, at 11:00 IST, addressed to Manveen. Before 16 Nov 2026 every job posted its
own report (about 15 posts a day). Manveen, 16 Nov: "if you would just spam that every check 15 minutes or 30
minutes throughout the day nobody will read that and nothing will happen".

## What the post says (since t409, 23 Nov 2026)

Manveen, 23 Nov 2026: "why does it fail to be readable again and again", "make sure you fix them from root". The post
now holds only things a person must do, each one of eight sentences he approved (`smartlead/digest_sentences.py`);
code fills only their [brackets]:

```
Inbox digest · Mon 23 Nov · 4 things to do

1. BettrData: retire 3 Google inboxes on bill.example in Zapmail before 27 Nov, or they renew ($14.25).
2. Melior: forum.example failed its weekly test (0 of 100 reached the inbox at Outlook). Keep its 25 inboxes out of campaigns and don't renew the domain.
3. BettrData: delete 8 inboxes in Smartlead that Zapmail already removed (list in thread).
4. BettrData: sam@fixed.example finished warm-up repair. Remove its DO NOT USE tag if you want it back.

Watching, nothing to do: peer.example replies are low (6 vs 21 expected); the next weekly test on Sat 28 Nov decides.
```

- At most 5 numbered items; dated ones (bills) first, soonest first. The rest are in the thread, the one opened first
  first, then the person-only asks that have no approved sentence yet, in the closest sentence's shape and marked
  "(wording not approved yet)". "N more in the thread." says how many.
- One line per item: client, what, by when. Lists, steps ("How: ...") and where to do it are in the thread, in item
  order ("1 above:" ...). A list of more than 3 addresses is "N inboxes, list in thread".
- A day with nothing to do posts one line, "Inbox digest · <day> · nothing to do today", so a missing post always
  means the bot did not run. No @mention.
- Never in the post: "For info" facts and "Done by the bot" (thread, last reply), what was left out on purpose
  (thread), inbox counts (the All Inboxes tab), and the bot's own problems.

Where every ask kind goes is one table, `smartlead/digest_items.py` (`KEYMAP`, `TEXTMAP`, `KINDS`;
`mapping_table()` prints it): a sentence, a Watching line, a read-only lookup the bot does itself (the domain's latest
weekly verdict, Mongo `v2_verdicts`; its next weekly test, Mongo `test_coverage`), the dev channel, or the thread. A
lookup that cannot read its data never guesses: the ask goes to the thread as the job wrote it. A new ask kind with no
row goes to the thread, never the post.

**The bot's own problems** (a job failed or never finished, a check could not read, Render behind main, the PC job
silent or in warn mode) are one separate message, "Bot jobs · <day> · N problems", in the bot-health channel
(`BOT_HEALTH_CHANNEL`, else the infra channel, as `index.js` healthChannel), never in Manveen's digest.

**Facts for the blind test**: with `--post`, the digest saves what it was made from to Mongo `digest_facts` (one doc per
IST day). `digest_blind_test.py` (a PC scheduled task, 7 days from 24 Nov 2026) has headless Claude write the same
digest from those facts, checks it against them (`digest_sentences.check`), and DMs Manveen both, unlabelled: "Which
would you act on faster? Reply 1 or 2." The AI design replaces the templates only if it wins 4 of 7 and he says yes.

The digest posts once a day: a second `--post` run the same IST day does nothing (`--again` to repeat it).
Since t409 a day with nothing to do still posts its one-line header.

**Urgent** items post at once and stay in the digest until fixed: a payment failed, a subscription not renewed or
cancelled, a Smartlead key rejected, a live campaign losing half its inboxes or left with only failed ones. Each
posts once; if it is fixed and comes back later, it posts again.

Every ask carries its own list: which campaigns, inboxes, domains, bills or batches it is about (17 Nov 2026,
Manveen, on an urgent post that named no campaign: "what was the point of this message if it's not gonna give me
the complete info"). The digest shows its list in the thread (12 lines, "and N more"); an urgent post shows up to 10 ("and N more"). A step says "each campaign listed", never "in the thread": an urgent post has no thread.
A broken check shows its error under it (in the dev-channel message since t409). A bill ask names the vendor, bill date, amount, inboxes and domains, and
one bill is one ask (a failed payment is not asked about again as "not renewed").

An urgent post reads: what is wrong, the list, then `@Manveen` and the step, then "It stays in the daily digest
until it is fixed.", each part apart.

The noon "Daily Renewal Check" card (only when something expires within 3 working days) names each domain or inbox
on the card (client, vendor, cost, the renewal review's flag; at most 8 per group, Read more for the rest) and ends
with the decision: renew each one before its date or let it go on purpose; a ⛔ one should not be renewed.

Zapmail webhooks: with the digest on, a subscription status change or failed payment is not posted by the webhook
(billing watch posts it at once with the bill's details) and neither is a finished Zapmail placement test (nothing
to do). An export failure or a billing change still posts at once, with the step.

Bot health leaves out a missed run nobody can act on: the digest's own slot, and a job that has run fine since.

## Every job, before and now

| Job (digest source) | When (IST) | Before | Now |
|---|---|---|---|
| v2 weekly (`v2-weekly`, PC) | hourly | post per run with "Needs you" | asks; pulls, tags, returns are done lines |
| Inbox list (`inbox-status`) | 10:00 | daily post | asks (disconnected, half lost: urgent; sending untested and on no test campaign); the numbers per client |
| Billing watch (`billing-watch`) | every 2 h from 10:05 | post per alert | urgent ask per failed / not renewed / cancelled bill; new and gone kept 3 days |
| Renewal review (`renewal-review`) | 09:50 | post when bills are due | one ask per bill with inboxes to drop or check |
| Zapmail digest (`zapmail-digest`) | 09:40 | daily post | asks (wallet, expiring domains, batches, ScaledMail orders); status lines in the thread |
| API keys (`key-health`) | 09:45 | alert | rejected key: urgent ask; unreachable: broken line |
| Campaign stats job (`campaign-metrics`) | 09:00 | failure post | broken line naming the source and the error type |
| Daily sheet sync (`smartlead-sync`) | 10:00 | failure post | broken line |
| Test campaigns (`dt-senders`) | 06:00 | post | done lines (also: pulled inboxes put on a DT campaign for the weekly retest); untracked domains, pulled inboxes with no room on a DT campaign, inboxes in no active weekly v2 test or in two, two active tests with one name: ask; v2 tests or asset tracker unreadable: broken line |
| Test copy (`copy-sync`) | Fri-Sun 18:30, Mon/Tue 05:00 | post | done line; fixable problems: ask |
| Blocklists (`blacklist`) | Mon 09:00 | post | thread only; a check that could not run: broken line. A listing is never an ask |
| Fleet watch (`fleet-watch`) | 10:30 | post per new case | one ask per open kind; blocklist listings thread only |
| Fleet scorecard (`fleet-scorecard`) | Mon 10:50 | post | thread only |
| Inbox evidence (`inbox-evidence`) | Sun 09:00 | new (17 Nov 2026) | one done line per client (emails, sender known %, replies, out-of-office, team reply log matched, quiet, not enough data); a partial read: broken line; base rates, reply-log misses and quiet inboxes in the thread. Report only. Rules: `docs/INBOX_EVIDENCE.md` |
| Warm-up repair (`warmup-repair`, inside fleet watch) | 10:30 | new (17 Nov 2026) | Mondays one done line per client ("Warm-up repair, BettrData: N repairing, N recovered this week, N worse."); inboxes 3 weeks in repair without improving: one ask (cancel candidates, his call); per-inbox lines in the thread. Report-only: changes no warm-up setting. Mongo `warmup_repair`; seed and preview: `warmup_repair_cli.py` |
| Gate self-test (`gate-selftest`) | 11:30 | Monday post, and on a disagreement | silent when all agree; held-back inboxes still sending: ask; other disagreements: broken line |
| Reply check (`reply-monitor`) | 13:00 | ALERT posts, Monday summary | ALERT domains: one ask; summary in the thread |
| Kill warnings (`kill-report`) | Tue 13:30 | post | asks (still sending, idle in live campaigns, domains to stop using) |
| Volume review (`volume-review`) | Tue 13:45 (when `VOLUME_REVIEW_ENABLED=true`) | own post (a count of idle inboxes named nobody) | a pilot domain stepped back: done line; a pilot change that did not write, or a domain outside the pilot that should step back: ask, each domain or inbox listed with its client; an account it could not read: broken line. A domain on hold and the inboxes that sent nothing in 7 days (by client and domain, DO NOT USE ones marked) are thread only: nothing to do. SMTP IP and hand-changed limits point at fleet watch, which asks about them. Never raises a limit |
| Real-recipient check (`real-recipients`) | Sun 07:00 | post | trust change: done line; report in the thread |
| Classic seed test (`placement-report`) | Mon/Tue 08:00-13:00 | post per client | thread only (being retired, see below) |
| New inboxes (`inbox-intake`, `inbox-ready`) | every 2 h | post | done lines; problems a person fixes: ask |
| Bot health, restart, 4 PM reminder, v2 watch (bot side) | various | posts | silent; a failed run is a broken line |

Jobs that run after 11:00 (gate self-test, reply check, kill warnings, volume review, the later billing-watch runs) appear in the
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
- `smartlead_sync/digest_post.py`: builds and posts the digest (`--post`; without it a dry run): `plan` (where
  every ask goes), `compose` (the post and thread), `dev_post` (the bot's own problems), `day_facts`.
- `smartlead_sync/smartlead/digest_sentences.py`: the approved sentences, the header, the blind test's checker.
- `smartlead_sync/smartlead/digest_items.py`: where every ask kind goes, and the read-only lookups.
- `smartlead_sync/digest_blind_test.py`: the 7-day blind test (PC task).
- Mongo: `digest_asks` (one doc per source), `digest_done`, `digest_urgent`, `digest_meta`, `digest_facts`,
  `digest_blind`.
- Each job's mapping: its `digest_items` / `digest_asks` function, with tests for digest on, fallback and off.
