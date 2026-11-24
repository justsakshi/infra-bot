# Registers the one-time Dynadot Black Friday buy (docs/DYNADOT.md step 4).
# Usage: powershell -NoProfile -File scripts\dynadot_schedule.ps1 -CodeDir <...\smartlead_sync> -VenvDir <...\.venv> -EnvFile <...\infra-bot\.env>
param(
    [Parameter(Mandatory = $true)][string]$CodeDir,
    [Parameter(Mandatory = $true)][string]$VenvDir,
    [Parameter(Mandatory = $true)][string]$EnvFile,
    [string]$At = "2026-11-27 05:35",
    [int]$Count = 100,
    [string]$Names = "$env:USERPROFILE\.infrabot\dynadot\names.txt"
)
$py = Join-Path $VenvDir "Scripts\pythonw.exe"
if (-not (Test-Path $py)) { throw "no pythonw at $py" }
if (-not (Test-Path $Names)) { throw "no names file at $Names" }
$taskArgs = "dynadot_cli.py --env-file `"$EnvFile`" buy --names `"$Names`" --count $Count --approve --allow-spend --require-sandbox-pass --wait-minutes 180 --notify"
$a = New-ScheduledTaskAction -Execute $py -Argument $taskArgs -WorkingDirectory $CodeDir
$t = New-ScheduledTaskTrigger -Once -At ([datetime]$At)
$s = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 5) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName "InfraBot Dynadot BF buy" -Action $a -Trigger $t -Settings $s -Force -Description "t406: buys up to $Count .co on Dynadot from the prepaid balance (infra-bot docs/DYNADOT.md)" | Out-Null
Get-ScheduledTask "InfraBot Dynadot BF buy" | Get-ScheduledTaskInfo | Select-Object TaskName, NextRunTime
