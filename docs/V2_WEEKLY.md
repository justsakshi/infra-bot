# v2 weekly job

Keeps every inbox that sends for PL, Melior and BettrData proven in the inbox, week after week, without anyone running it.

Code: `smartlead_sync/v2_weekly.py` (files, state, Smartlead, Slack) and `smartlead_sync/smartlead/v2_results.py` (the rules). Tests: `test_v2_results.py`, `test_v2_weekly.py`.

## The week

| When | What happens | Who |
|---|---|---|
| Mon 20:00 IST | Smart Delivery v2 tests DT PL #1-3, DT Melior #1-4 and DT BettrData #1-2 (weekly, one by one, 1-minute gap) | Smartlead, automatic |
| Tue morning | Download each test's CSV into `C:\Users\Manveen\Downloads` | Manveen, about 2 minutes |
| Within 2 hours of the last download | The job reads them and acts, then posts in the infra channel | Job |
| Tue 12:00 IST | If no file for Monday came in, one reminder post | Job |

The download is the only manual step: v2 has no API, webhook or email (Smartlead support, 5-6 Nov 2026). A request for one went to Smartlead on 6 Nov.

## The rules

- Per provider: **pass** = 4 of 4 seeds in the inbox, **watch** = 2-3, **fail** = 0-1. An inbox passes only if it passes at both Google and Microsoft.
- **Fail**, or **watch two weeks running**: out of every live campaign that is not a test campaign. Its leads move to the campaign's other senders, leads the removal stalled are resumed, and it gets the tag `DO NOT USE - failed v2 (auto)`. gtm presend refuses any `DO NOT USE` tag. Warm-up stays on, and the inbox stays in its DT campaign, so the next Monday tests it again.
- **Back in**: after two passes at least 6 days apart, both after the pull, it goes back into the same campaigns and the tag comes off.
- **Never the last good sender**: a live or scheduled campaign that would be left with no good sender is not changed, and the post asks a person. A paused or draft one is left as it is, because it sends nothing.
- **Broken test brake**: if 80% or more of a night's inboxes (10+) are below 4/4 at one provider, not counting inboxes already known bad, that provider's results are held and nothing is pulled for them.
- **Waits for the whole night**: it acts only after 90 minutes with no new file for that night.
- **A person wins**: remove the job's tag in Smartlead and the job leaves that inbox alone until its next result.
- **History**: the 5 Nov audit files are filed as history. They are kept for "two weeks running" but never start an action.

## Where it runs

- Windows scheduled task **InfraBot v2 weekly**: hourly, no window, on Manveen's PC.
- Code: worktree `C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\infra-bot-v2weekly`, checked out at `origin/main`. The task does not use the shared `infra-bot` checkout, so switching branches there cannot break it.
- Keys: read from `infra-bot\smartlead_sync\.env` via `--env-dir`. The keys are never printed.
- State: `C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot\v2-weekly\`, with `state.json`, `runs\` (a copy of every CSV), `reports\` (every post) and `task.log`.

## Install (once, PowerShell)

First file the old audit CSVs as history, so they can never start an action. (The job also files any test from before its first day as history, so doing this in the wrong order is safe.)

```powershell
$repo = "C:\Users\Manveen\Desktop\new_things_to_mess_araound\infrabot"
cd "$repo\infra-bot-v2weekly\smartlead_sync"
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

`--act-from 2026-11-16`: the first week (the 9 Nov tests) only posts what it would do. From 16 Nov it acts on its own.

## Everyday commands

| Want to | Run (from the worktree's `smartlead_sync`) |
|---|---|
| See what it would do now | `python v2_weekly.py --env-dir ...\infra-bot\smartlead_sync` (dry run: no state, no Smartlead, no Slack) |
| Act again on stored results | `python v2_weekly.py --replan --apply --post --env-dir ...` |
| Pause it | `Disable-ScheduledTask "InfraBot v2 weekly"` (`Enable-ScheduledTask` to resume) |
| Ship a code change to it | `git -C ...\infra-bot-v2weekly fetch origin main` then `git -C ...\infra-bot-v2weekly checkout --detach origin/main` |
| Undo one pull | Remove the `DO NOT USE - failed v2 (auto)` tag in Smartlead and add the inbox back by hand |

## If something goes wrong

- A crash posts "v2 weekly job failed" once a day while it keeps failing. Details are in `task.log`.
- State is saved after every change. A half-done lead move is retried every run, or daily once there are no new files, for up to 14 days.
- No post on Tuesday at all: the PC was off or the task is disabled. Check `Get-ScheduledTask "InfraBot v2 weekly"` and `task.log`.
