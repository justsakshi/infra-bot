# v2 weekly job

Keeps every inbox that sends for PL, Melior and BettrData proven in the inbox, week after week, without anyone running it.

Code: `smartlead_sync/v2_weekly.py` (files, state, Smartlead, Slack, Mongo), `smartlead_sync/smartlead/v2_results.py` (the rules) and `smartlead_sync/smartlead/v2_api.py` (results from the Smart Delivery v2 API). Tests: `test_v2_results.py`, `test_v2_api.py`, `test_v2_weekly.py`, and `test_v2_weekly_run.py` (whole hourly runs against a fake Downloads folder, v2 API, fleet, Slack and Mongo).

## The week

| When | What happens | Who |
|---|---|---|
| Sun 00:00 IST | Smart Delivery v2 tests DT PL #1-3, DT Melior #1-4 and DT BettrData #1-2 (weekly, one by one, 5-minute gap, 4 emails per provider). The gap is for the whole test and 5 minutes is the least Smartlead allows, so a 50-mailbox test (400 emails) takes about 33 hours and is in on Monday morning. A test that starts on a Saturday, Sunday or Monday counts as the week's test | Smartlead, automatic |
| Each hour | The job reads every finished run from the Smart Delivery v2 API, and any v2 CSV in `C:\Users\Manveen\Downloads` | Job |
| About 2 hours after the last run of a night finished | The job acts on the night, then posts in the infra channel | Job |
| Tue 12:00 IST | If any DT test has no result for the week, one reminder naming each missing test. When the API's test list was read, only a DT campaign whose ACTIVE v2 test should have run is named, with why, and nothing asks for a download; a DT campaign with no v2 test is left to the coverage check | Job |

Nothing needs downloading any more. Smartlead support said on 15 Nov 2026 that v2 has an API, and it was checked read-only the same day with both keys. Each hourly run reads every finished run of both accounts (PRECISE_LEADS, which holds the PL and Melior tests, and BETTRDATA) with the same keys the job already uses.

- **What it reads.** Runs that finished on or after 13 Nov 2026, the day the weekly "synced" tests began, and within the last 21 days. Older runs are never read from the API: they include stopped, one-off and duplicate tests from 7-11 Nov that were never downloaded on purpose, and reading them now would act on old results.
- **Same as a download.** Each run becomes the rows its CSV would have, under the CSV's own file name (`<test name>-run-<n>.csv`), so everything after that is the same: the night, the copy check by name, the verdicts. On the 14 Nov runs the API rows equal the downloaded CSVs row for row. A copy of each run is kept in `runs\` as a CSV.
- **Read once.** A run is fetched once. A run whose CSV was already read is not fetched, and a CSV of a run already read from the API is filed as `duplicate` (matched by the run's file name, `(1)`-style copies included, and its night).
- **Seeds that never arrive.** A file or run with more than a fifth of its results not in yet is filed as pending and read again every hour by itself (an API run is fetched again; nothing is posted for it). One that is still not readable 48 hours after sending (a result other than "Never arrived") is posted once per run, however many times it is fetched or downloaded. Of two copies of one run, the one with fewer seeds still out is read. A run that sent no test emails at all (the account had no test runs left, inboxes disconnected) is posted once as such and never fetched again. A seed still "Never arrived" 48 hours after it was sent will not arrive (seen well over a day after sending; support says v2 watches a run for about 2.5 hours), so it then counts as final: the file is read on what arrived, no re-download is asked for, and the inbox is not judged at that provider. A copy check is read the same way.
- **Rate limit.** The account allows 200 calls a minute, shared with other tools. A call answered with HTTP 429 waits (15 s, then 30 s, or what the server asks, at most 60 s) and tries again twice before it counts as an error.
- **Errors.** One account or one test failing never stops the other account or the CSVs. The error is logged and listed in the heartbeat's `errors` as what was read plus the HTTP status or the error type only, because the key is in the URL.

**Fallback: the download.** Downloading a test's CSV into Downloads still works. If the API is down, or switched off with `V2_API=off` in the `.env` or `--no-api` on the task, download each test's CSV (the copy checks too) on Tuesday morning; the job reads it within the hour.

## The rules

- Per provider, 4 seeds: **pass** = all 4 in the inbox, **fail** = 3 or more of 4 in spam, **watch** = all 4 answered with 2-3 in the inbox.
- **Own provider** (Manveen, 11 Nov 2026: Google to Google and Outlook to Outlook first): an inbox is judged at its own provider. A Google inbox (Smartlead type `GMAIL`) is judged on its Google seeds only, an Outlook inbox (`OUTLOOK`) on its Microsoft seeds only. An `SMTP` inbox, or one whose type the job does not know, is judged on both as before: it passes only if it passes at both, and fails if either fails. The post still shows both seed counts.
- **Failed domain**: per client and sending domain, if half or more of its inboxes failed at their own provider that week, the domain has failed. The share is of every inbox the job knows on the domain, tested or not, so a one-off test of one inbox cannot rest a whole domain. Every other inbox on it, tested or not, is taken out the same way a failed inbox is, and the post gives the domain its own line ("domain failed · <domain> · 2 of 4 inboxes in spam → 2 more inboxes taken out of ..."). None of its inboxes comes back while the domain still fails. An inbox that was never in a test is put on a DT campaign by the next `dt_senders.py` run (rule below), so it can earn its passes; with no room, the digest asks for it.
- **Not judged, never acted on**: a seed that had not answered when the file was downloaded, a seed that BOUNCED (a problem at the seed address, not the sender), or a test that sent fewer than 4 emails per provider (shown as "Google 1/1 (only 1 sent)", not "1/4"). An inbox still out or tagged because of an earlier fail that a short test can't clear says so ("failed Mon 9 Nov, and this test can't clear it"). A DT test with no result says why when the job knows: its run is still coming in (For info) or sent no test emails (Needs you); else "no finished v2 run found". The watch-week line shows only before its date. The post says which, and always lists it under Needs you: a fresh download when a CSV's seeds were still pending (once; after 48 hours the job reads it as it is), runs that sent fewer than 4 (one line: Smartlead's test inboxes are a shared pool that refills once a day at 05:30 IST, so a run started late in that day comes out short, and a few minutes' gap does not help; run it again after 05:30 IST, spread through the day), or, when a seed bounced or a provider has no seed rows in a file, a check of that test's seed list and a re-run (until then those inboxes can't be pulled or come back).
- **Did not send.** An inbox with no email sent in a test is checked against Smartlead's own login status. Smartlead can't log in: one Needs-you line for all of them, by domain: "reconnect, or delete if cancelled". Smartlead shows it connected: one For-info line: the run came back short (the test inboxes run low late in the day); look at the inbox if it misses next week's test too. Status unknown: one line, "check they are connected".
- **Fail**, or **watch in two weekly tests 6+ days apart**: out of every live campaign that is not a test campaign. Its leads move to the campaign's other senders, leads the removal stalled are resumed, and it gets the tag `DO NOT USE - failed v2 (auto)`. gtm presend refuses any `DO NOT USE` tag. Warm-up stays on, and the inbox stays in its DT campaign, so the next Monday tests it again. A one-off midweek test or a history file never counts toward "two weeks running"; a one-off fail still pulls.
- **Melior acts like PL and BettrData** (Manveen, 16 Nov 2026: "Yes, Outlook only"). Its Outlook inboxes are taken out, tagged and returned by the same rules; its Google inboxes stay report-only under the next rule, as every client's do. The report-only stage for a whole client is still in the code (`REPORT_ONLY_CLIENTS`, empty now): a client listed there is only reported on ("would be taken out"), and `--act-clients <name>` overrides it for one run.
- **Inbox types whose test is not trusted** (Google inboxes since 12 Nov 2026): a failed v2 test is reported, never acted on, and a campaign whose remaining senders failed only such a test is not called stuck. Which types are not trusted is decided each Sunday by the real-recipient check (`real_recipients.py`): a type whose inboxes v2 fails mostly, while real recipients at its own provider answer it at least as well as the other type in the same campaigns, is not trusted; one real recipients agree is worse, or that v2 mostly passes, is trusted again; anything in between, or too little data, leaves it as it was. The decision is kept in Mongo `v2_trust` and read by this job, the inbox list, the daily inbox post and the gate self-test (`usable.refresh_trust`). Overrides: `V2_UNTRUSTED_TYPES=GMAIL` (or `none`) on a reader, `V2_TRUST_AUTO=off` on the check, `--act-types GMAIL` for one run.
- **A pulled inbox stays in a weekly test** (Manveen, 16 Nov 2026) until it is back in use (returned, or a person removed the tag) or cancelled (gone from Smartlead, or Smartlead can't log in to it). `dt_senders.py` (daily 06:00 IST, Render) keeps every connected inbox with the job's tag on one of its client's DT campaigns, even when its domain is not in the deliverability sheet, and puts one that is on none on the first with room (a done line in the digest). It only takes room left in the DT campaigns the client already needs: never more than 50 per campaign, never a new campaign for it. With no room, the digest asks a person to make room or create the next DT campaign and its test. The tag is the truth here, as in `reconcile`: dt_senders runs on Render and reads Smartlead, not the PC's `state.json`.
- **Coverage rule** (Manveen, 16 Nov 2026: "make sure we have all inboxes in test except scaled mail canceld orders"): every connected inbox we use (its domain in the deliverability sheet, or pulled by this job) sits in exactly one ACTIVE weekly v2 test, at most 50 senders a test. ScaledMail cancelled orders are left out: the asset tracker's domain row is no longer Active. `dt_senders.py` checks it every morning from the v2 API (`GET /tests`, `GET /tests/{id}`): an inbox in no active test, or in two or more, is a digest ask per client, and so are two active tests with one name. The weekly copy check ("... synced copy") reuses a few inboxes on purpose and is never counted as a duplicate. A v2 test keeps the senders it was created with, so a missing inbox needs a new test (created Saturday after 05:30 IST, when the seed pool refills), and a sender of an active test is never moved off that test's DT campaign by the fill-first layout, or taken off it. If the tests can't be read, nobody on a DT campaign moves that day and the digest says the check is broken.
- **Back in**: after two passes at least 6 days apart, both after the pull, it goes back into the same campaigns and the tag comes off. Not while its domain is failed (above): then it waits, and the post says so under For info.
- **Never the last good sender**: a live or scheduled campaign that would be left with no good sender is not changed, and the post asks a person. A paused or draft one is left as it is, because it sends nothing.
- **Broken test brake**: if 80% or more of a night's inboxes (10+) are below 4/4 at one provider, not counting inboxes already known bad, that provider's results are held and nothing is pulled for them. The same check runs per test file, so one broken test among good ones is held on its own.
- **Waits for the whole night**: it acts only after 90 minutes with no new file for that night.
- **A person wins**: remove the job's tag in Smartlead and the job leaves that inbox alone until its next result.
- **History**: the 5 Nov audit files are filed as history. They are kept and sent to Mongo, but never start an action.
- **A CSV it can't read**: a file with a Mailbox column but another header (Smart Delivery changed its download) is posted once as "unrecognised v2 CSV format: <file>" instead of being skipped silently.

## Copy check

An inbox test sends a plain note, so it says whether the inbox is fine. It says nothing about the email. On 11 Nov 2026 the same Google inbox delivered the plain note to a Gmail inbox and its campaign email to spam, with tracking on and with tracking off. The copy check tests the email.

**The campaign.** One small campaign per client: `DT PL copy check`, `DT Melior copy check`, `DT BettrData copy check`. It holds:

- the client's real first email: the campaign that sent the most this week, the same rule the standing test campaigns follow. Its tracking settings are left as they are.
- up to 10 senders: 5 Google-hosted and 5 Outlook-hosted inboxes of that client, at most 2 per domain. Each is connected, has no `DO NOT USE` tag, and its newest v2 verdict is a pass from the last 10 days.

`copy_sync.py` refreshes both on every run (Fri, Sat and Sun 18:30, Mon and Tue 05:00 IST): it attaches the inboxes that qualify and are missing, detaches the ones that no longer qualify, and reads the list back. If no inbox qualifies, or the verdicts in Mongo cannot be read, the senders stay as they are and the run says so. The campaign stays paused and is never started.

**Create it, once per client.** From `smartlead_sync` in a checkout that has this code and the `.env` (the keys and `MONGO_URI`):

```powershell
python copy_sync.py --create-copy-check "DT PL"              # dry run: says which campaign it would clone
python copy_sync.py --create-copy-check "DT PL" --apply
python copy_sync.py --create-copy-check "DT Melior" --apply
python copy_sync.py --create-copy-check "DT BettrData" --apply
```

It clones the client's `DT <client> #1` campaign (one step, same settings and schedule, the placeholder lead, paused), then loads the email and attaches the senders. It refuses to create a second one.

**Each week (Manveen).** In Smart Delivery v2, one more test per client, on the copy-check campaign: 4 emails per provider, 5-minute gap, and the words "copy check" in the test's name (for example `PL copy check`). The job reads its result from the API like the others (or from its CSV in Downloads).

The name matters. The CSV's file name comes from the test's name (a run read from the API gets the same name), and "copy check" in it (also `copy-check`, `copy_check`, `copycheck`) is how the job knows the file. Without it the job can still tell when every inbox in the file is a sender of a copy-check campaign, but it only knows the senders as of its last weekly run, so a sender attached since then makes an unnamed file look like an inbox test.

**What the job does with it.**

- Nothing in the file is an inbox result: no verdict, no pull, no return, no domain rule, no hold, no `v2_verdicts` row. The headline's inbox counts leave it out.
- It judges the email per sender type, at that type's own provider: Google-hosted senders on the Google seeds, Outlook-hosted senders on the Microsoft seeds. Only emails that got a placement count, and only senders whose newest judged inbox-test result is a pass; the others are left out and counted as left out.
- Share in the inbox: 75% or more = the email is fine, 50% or less = the email is the problem, in between = borderline. Fewer than 8 counted emails for a sender type = not judged.
- The post has one line per sender type under the client, for example `copy check · Google inboxes · 3 of 20 emails in the Google inbox, from 5 inboxes that pass the plain note → the email is the problem: rewrite it before it sends from Google inboxes`. A failed email also gets a line under Needs you.
- With the week's inbox files it is part of their post. On its own (a one-off check) it is posted once the file has been in Downloads for 90 minutes; that run does not read or change Smartlead.
- The client is the one most of the file's inboxes belong to.
- A copy-check campaign with no copy-check file for the week is named in "No result this week from ..." and in the Tuesday reminder, like any other DT test.

It only reports. Refusing a launch on a failed copy check is a later step.

**Check a drafted campaign before launch.** Load its first email into the client's copy-check campaign, then run a test on it:

```powershell
python copy_sync.py --only-copy-check "DT Melior" --source 1234567          # dry run
python copy_sync.py --only-copy-check "DT Melior" --source 1234567 --apply
```

`--source` is the Smartlead id of the campaign to check; a draft works. A campaign of another client is refused and the run exits 1. Then create a one-off v2 test on `DT Melior copy check` (4 emails per provider, "copy check" in its name) and download its CSV when it is done. The next scheduled copy sync puts the most active campaign's email back, so start the test before then.

## What reaches Render

After every saved run the job writes to Mongo (database `HEALTH_HISTORY_DB`, `MONGO_URI` from the same `.env`). A Mongo problem never stops the run: it is printed in `task.log` and listed in the heartbeat's `errors` when the heartbeat itself can be written.

- `v2_heartbeat`, one doc `_id: "pc"`: `last_run` (UTC), `newest_night`, `files_read`, `mode` (act / warn), `last_post_ok`, `commit`, `behind_main` (commits behind `origin/main` as last fetched), `errors`.
- `copy_checks`, one doc per client per night, `_id: "<client>|<night>"`: `client`, `night`, `file`, and per sender type (`google`, `outlook`) `inbox`, `sent`, `senders`, `left_out`, `verdict` (PASS / WATCH / FAIL / NOT_JUDGED), `updated_at`. Written once the copy check has been reported; rewritten only when it changes.
- `v2_verdicts`, one doc per inbox per night, `_id: "<email>|<night>"`: `email`, `domain`, `client`, `night`, `verdict` (the one the job acts on: at the inbox's own provider, with held providers set aside; `held` names them. A Google inbox in spam at Microsoft only reads PASS here, so the tools that read these rows (usable list, gate self-test, renewal review, grid) see the verdict the job acted on), `google` and `microsoft` (`inbox`, `reported`, `sent`), `history`, `weekly`, `pulled_on`, `updated_at`. Only nights the job has acted on (so holds are worked out) from the last 42 days; a doc is rewritten only when it changes.

## Where it runs

- Windows scheduled task **InfraBot v2 weekly**: hourly, no window, on Manveen's PC.
- Code: worktree `C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\infra-bot-v2weekly`, checked out at `origin/main`. The task does not use the shared `infra-bot` checkout, so switching branches there cannot break it. Each run logs how many commits it is behind `origin/main`, and the weekly post says so.
- Keys: read from `infra-bot\smartlead_sync\.env` via `--env-dir`. The keys are never printed.
- State: `C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\v2-weekly\`, with `state.json`, `runs\` (a copy of every CSV), `reports\` (every post) and `task.log`.

## Install (once, PowerShell)

First check that everything the job needs is reachable. It posts nothing and changes nothing; stop here if any line says FAIL.

```powershell
$repo = "C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot"
cd "$repo\infra-bot-v2weekly\smartlead_sync"
& "$repo\infra-bot\smartlead_sync\.venv\Scripts\python.exe" v2_weekly.py --self-check --env-dir "$repo\infra-bot\smartlead_sync"
```

It checks the Slack channel exists and the bot is a member, both Smartlead keys work, the v2 API answers for both accounts (it prints how many tests each has), the Downloads folder is readable and Mongo answers.

Then file the old audit CSVs as history, so they can never start an action. (The job also files any test from before its first day as history, so doing this in the wrong order is safe.)

```powershell
& "$repo\infra-bot\smartlead_sync\.venv\Scripts\python.exe" v2_weekly.py --history-only --env-dir "$repo\infra-bot\smartlead_sync" --log "$repo\v2-weekly\task.log"
```

Then register the hourly task:

```powershell
$py = "$repo\infra-bot\smartlead_sync\.venv\Scripts\pythonw.exe"
$taskArgs = "v2_weekly.py --apply --post --act-from 2026-11-16 --env-dir `"$repo\infra-bot\smartlead_sync`" --log `"$repo\v2-weekly\task.log`""
$a = New-ScheduledTaskAction -Execute $py -Argument $taskArgs -WorkingDirectory "$repo\infra-bot-v2weekly\smartlead_sync"
$t = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddHours((Get-Date).Hour + 1) -RepetitionInterval (New-TimeSpan -Hours 1)
$s = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 45) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "InfraBot v2 weekly" -Action $a -Trigger $t -Settings $s -Description "Reads Smart Delivery v2 CSVs from Downloads and keeps failing inboxes out of campaigns (infra-bot docs/V2_WEEKLY.md)"
```

`--act-from 2026-11-16`: the first week (the 9 Nov tests) only posts what it would do. From 16 Nov it acts on its own. Melior's Outlook inboxes act too since 16 Nov 2026 (code change, no task change); Google inboxes stay report-only.

## Everyday commands

| Want to | Run (from the worktree's `smartlead_sync`) |
|---|---|
| Check it can reach everything | `python v2_weekly.py --self-check --env-dir ...\infra-bot\smartlead_sync` (posts nothing) |
| Read CSVs only, not the v2 API | add `--no-api`, or set `V2_API=off` in the `.env` |
| See what it would do now | `python v2_weekly.py --env-dir ...\infra-bot\smartlead_sync` (dry run: no state, no Smartlead, no Slack) |
| Act again on stored results | `python v2_weekly.py --replan --apply --post --env-dir ...` |
| Let it act on a report-only client for one run | add `--act-clients <name>` (none is report-only since 16 Nov 2026) |
| Pause it | `Disable-ScheduledTask "InfraBot v2 weekly"` (`Enable-ScheduledTask` to resume) |
| Ship a code change to it | `git -C ...\infra-bot-v2weekly fetch origin main` then `git -C ...\infra-bot-v2weekly checkout --detach origin/main`, then run the self-check |
| Undo one pull | Remove the `DO NOT USE - failed v2 (auto)` tag in Smartlead and add the inbox back by hand |

## If something goes wrong

- Exit codes (Task Scheduler's "Last Run Result"): 0 fine, 1 a Smartlead step failed (the post lists it), 2 crashed, 3 a Slack post was refused.
- A refused post writes `UNPOSTED: <headline>` to `task.log`. The weekly report is queued (last 5 kept) and posted again on the next run; the reminder is tried again every hour.
- A crash posts "v2 weekly job failed" once a day while it keeps failing. If that post is refused too, it is tried again the next hour. Details are in `task.log`.
- State is saved after every change. A half-done lead move is retried every run, or daily once there are no new files, for up to 14 days.
- No post on Tuesday at all: the PC was off or the task is disabled. Check `Get-ScheduledTask "InfraBot v2 weekly"` and `task.log`. The `v2_heartbeat` doc in Mongo shows when it last ran, so a check on Render can notice too.

## Rules review: are the rules themselves still right?

The job adapts what it does with each inbox every week. Its rules are fixed numbers, and a fixed rule can be wrong for a long time without anyone seeing it. `smartlead_sync/rules_review.py` holds each rule up against an outcome the rule did not use:

- **The test decides.** Each sending domain's test result is compared with the daily reply check of the same domain, per client and inbox type. A group where the test fails domains that are answered like the client's other domains, or passes domains that get far fewer replies, is "in doubt".
- **Back after two passes.** Counted from the stored results: how many inboxes came back, and how many of them failed again.

Each rule comes out as holds, in doubt, or not enough data yet. A rule in doubt comes with a proposal. Nothing changes by itself: a rule changes only on a person's yes, through a normal code change.

```
python rules_review.py            # print it
python rules_review.py --json     # with the evidence table
python rules_review.py --post     # post it to the infra channel
```

It is read-only and is not on a schedule yet. "Answered" means the reply check says OK, the domain has at least two replies, and it has at least 70% of the replies its peers' rate predicts (`reply_stats.answered`). The reply check counts replies from every recipient provider, so it shows whether a domain reaches people, not where one provider files its mail.

