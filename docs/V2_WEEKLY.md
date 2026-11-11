# v2 weekly job

Keeps every inbox that sends for PL, Melior and BettrData proven in the inbox, week after week, without anyone running it.

Code: `smartlead_sync/v2_weekly.py` (files, state, Smartlead, Slack, Mongo) and `smartlead_sync/smartlead/v2_results.py` (the rules). Tests: `test_v2_results.py`, `test_v2_weekly.py`, and `test_v2_weekly_run.py` (whole hourly runs against a fake Downloads folder, fleet, Slack and Mongo).

## The week

| When | What happens | Who |
|---|---|---|
| Sun 00:00 IST | Smart Delivery v2 tests DT PL #1-3, DT Melior #1-4 and DT BettrData #1-2 (weekly, one by one, 5-minute gap, 4 emails per provider). The gap is for the whole test and 5 minutes is the least Smartlead allows, so a 50-mailbox test (400 emails) takes about 33 hours and is in on Monday morning. A test that starts on a Saturday, Sunday or Monday counts as the week's test | Smartlead, automatic |
| Tue morning | Download each test's CSV into `C:\Users\Manveen\Downloads` | Manveen, about 2 minutes |
| Within 2 hours of the last download | The job reads them and acts, then posts in the infra channel | Job |
| Tue 12:00 IST | If any DT test has no result for the week, one reminder post naming each missing test | Job |

The download is the only manual step: v2 has no API, webhook or email (Smartlead support, 5-6 Nov 2026). A request for one went to Smartlead on 6 Nov.

## The rules

- Per provider, 4 seeds: **pass** = all 4 in the inbox, **fail** = 3 or more of 4 in spam, **watch** = all 4 answered with 2-3 in the inbox.
- **Own provider** (Manveen, 11 Nov 2026: Google to Google and Outlook to Outlook first): an inbox is judged at its own provider. A Google inbox (Smartlead type `GMAIL`) is judged on its Google seeds only, an Outlook inbox (`OUTLOOK`) on its Microsoft seeds only. An `SMTP` inbox, or one whose type the job does not know, is judged on both as before: it passes only if it passes at both, and fails if either fails. The post still shows both seed counts.
- **Failed domain**: per client and sending domain, if half or more of the inboxes tested that week failed at their own provider, the domain has failed. Every other inbox on it, tested or not, is taken out the same way a failed inbox is, and the post gives the domain its own line ("domain failed · <domain> · 2 of 4 inboxes in spam → 2 more inboxes taken out of ..."). None of its inboxes comes back while the domain still fails. An inbox that was never in a test has no passes to come back on: add it to a DT test, or remove its tag by hand.
- **Not judged, never acted on**: a seed that had not answered when the file was downloaded, a seed that BOUNCED (a problem at the seed address, not the sender), or a test that sent fewer than 4 emails per provider. The post says which, and always lists it under Needs you: a fresh download when seeds were still pending, or, when a seed bounced or a provider has no seed rows in a file, a check of that test's seed list and a re-run (until then those inboxes can't be pulled or come back).
- **Fail**, or **watch in two weekly tests 6+ days apart**: out of every live campaign that is not a test campaign. Its leads move to the campaign's other senders, leads the removal stalled are resumed, and it gets the tag `DO NOT USE - failed v2 (auto)`. gtm presend refuses any `DO NOT USE` tag. Warm-up stays on, and the inbox stays in its DT campaign, so the next Monday tests it again. A one-off midweek test or a history file never counts toward "two weeks running"; a one-off fail still pulls.
- **Melior is report-only** until Ryan agrees to automatic pulls: its inboxes are listed as "would be taken out (Melior: report-only until Ryan agrees)" and nothing in Smartlead changes for them. The post counts them by what would happen (taken out, left as the last good sender of a live campaign, left in a paused/draft one, or tagged only), and a live Melior campaign left with only failing senders gets its own line asking Ryan to pause it or add a good sender. To act on Melior too, add `--act-clients Melior` to the task.
- **Back in**: after two passes at least 6 days apart, both after the pull, it goes back into the same campaigns and the tag comes off. Not while its domain is failed (above): then it waits, and the post says so under For info.
- **Never the last good sender**: a live or scheduled campaign that would be left with no good sender is not changed, and the post asks a person. A paused or draft one is left as it is, because it sends nothing.
- **Broken test brake**: if 80% or more of a night's inboxes (10+) are below 4/4 at one provider, not counting inboxes already known bad, that provider's results are held and nothing is pulled for them. The same check runs per test file, so one broken test among good ones is held on its own.
- **Waits for the whole night**: it acts only after 90 minutes with no new file for that night.
- **A person wins**: remove the job's tag in Smartlead and the job leaves that inbox alone until its next result.
- **History**: the 5 Nov audit files are filed as history. They are kept and sent to Mongo, but never start an action.
- **A CSV it can't read**: a file with a Mailbox column but another header (Smart Delivery changed its download) is posted once as "unrecognised v2 CSV format: <file>" instead of being skipped silently.

## What reaches Render

After every saved run the job writes to Mongo (database `HEALTH_HISTORY_DB`, `MONGO_URI` from the same `.env`). A Mongo problem never stops the run: it is printed in `task.log` and listed in the heartbeat's `errors` when the heartbeat itself can be written.

- `v2_heartbeat`, one doc `_id: "pc"`: `last_run` (UTC), `newest_night`, `files_read`, `mode` (act / warn), `last_post_ok`, `commit`, `behind_main` (commits behind `origin/main` as last fetched), `errors`.
- `v2_verdicts`, one doc per inbox per night, `_id: "<email>|<night>"`: `email`, `domain`, `client`, `night`, `verdict` (both providers combined, with held providers set aside; `held` names them. It is not yet the own-provider verdict the job acts on, so a Google inbox in spam at Microsoft only still reads FAIL here), `google` and `microsoft` (`inbox`, `reported`, `sent`), `history`, `weekly`, `pulled_on`, `updated_at`. Only nights the job has acted on (so holds are worked out) from the last 42 days; a doc is rewritten only when it changes.

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

It checks the Slack channel exists and the bot is a member, both Smartlead keys work, the Downloads folder is readable and Mongo answers.

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

`--act-from 2026-11-16`: the first week (the 9 Nov tests) only posts what it would do. From 16 Nov it acts on its own, except for Melior (report-only, above).

## Everyday commands

| Want to | Run (from the worktree's `smartlead_sync`) |
|---|---|
| Check it can reach everything | `python v2_weekly.py --self-check --env-dir ...\infra-bot\smartlead_sync` (posts nothing) |
| See what it would do now | `python v2_weekly.py --env-dir ...\infra-bot\smartlead_sync` (dry run: no state, no Smartlead, no Slack) |
| Act again on stored results | `python v2_weekly.py --replan --apply --post --env-dir ...` |
| Let it act on Melior too | add `--act-clients Melior` to the task's arguments |
| Pause it | `Disable-ScheduledTask "InfraBot v2 weekly"` (`Enable-ScheduledTask` to resume) |
| Ship a code change to it | `git -C ...\infra-bot-v2weekly fetch origin main` then `git -C ...\infra-bot-v2weekly checkout --detach origin/main`, then run the self-check |
| Undo one pull | Remove the `DO NOT USE - failed v2 (auto)` tag in Smartlead and add the inbox back by hand |

## If something goes wrong

- Exit codes (Task Scheduler's "Last Run Result"): 0 fine, 1 a Smartlead step failed (the post lists it), 2 crashed, 3 a Slack post was refused.
- A refused post writes `UNPOSTED: <headline>` to `task.log`. The weekly report is queued (last 5 kept) and posted again on the next run; the reminder is tried again every hour.
- A crash posts "v2 weekly job failed" once a day while it keeps failing. If that post is refused too, it is tried again the next hour. Details are in `task.log`.
- State is saved after every change. A half-done lead move is retried every run, or daily once there are no new files, for up to 14 days.
- No post on Tuesday at all: the PC was off or the task is disabled. Check `Get-ScheduledTask "InfraBot v2 weekly"` and `task.log`. The `v2_heartbeat` doc in Mongo shows when it last ran, so a check on Render can notice too.
