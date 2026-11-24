# Dynadot (domains paid from a prepaid balance)

Built 24 Nov 2026 (t406) to buy about 100 `.co` domains in Dynadot's Black Friday sale ($15.50 → about $2.60 first year, 27 Nov 00:00 UTC to 5 Dec 16:00 UTC per the community deal tracker), kept as backup for any client.

Why Dynadot works where GoDaddy didn't: Dynadot API orders are paid **only from the account balance**. A person tops the balance up once by hand (the card OTP happens then), and the script spends it. A register call without enough balance is refused; no card is ever charged. GoDaddy's API had to charge the card and failed on the OTP (see `GODADDY_INTEGRATION.md`).

Code: `smartlead_sync/smartlead/dynadot.py` (client), `smartlead_sync/smartlead/dynadot_orders.py` (buy run), `smartlead_sync/dynadot_cli.py`. Tests: `cd smartlead_sync && python -m pytest test_dynadot.py -q`.

## Safety
- Reads are free (`balance`, `check`, `ledger`). Buying needs `--approve`, and on the live API also `DYNADOT_ALLOW_SPEND=true` or `--allow-spend` for that one run.
- Never above `DYNADOT_PRICE_CEILING` (default $2.60) a domain, never above `DYNADOT_PLAN_CEILING` (default $260) in total. Premium names are skipped.
- If the price is still above the ceiling (sale not live yet), `--wait-minutes N` keeps checking every `--poll-seconds` and buys once it drops. After N minutes it stops and buys nothing above the ceiling.
- The ledger is written after every domain. A lost answer is looked up in the account (`domain_info`), never re-ordered. Running the same command again continues where it stopped.
- The API key travels in the URL. The client never prints a URL and never calls `raise_for_status()`.
- Names and the ledger live outside the repo, in `C:\Users\<you>\.infrabot\dynadot\` (this repo is public).

## 1. Account (by hand, once)
1. Sign up at dynadot.com with the services@ email. Turn on 2-step login.
2. Account settings: set the currency to **USD**. Fill in the default contact (registrant, admin, technical, billing). Use letters, digits and `- . , : ( ) ' # * @ /` only, with no double quotes (a quoted name broke GoDaddy's registry step).
3. Top up the balance: about **$265** (100 × $2.60 plus a margin). Do this by card or PayPal, with the OTP done by you.
4. Tools → API: create the API key and the **sandbox** key. If it asks for allowed IPs, add this PC's public IP.
5. Put the keys in `infra-bot\.env` as `DYNADOT_API_KEY=` and `DYNADOT_SANDBOX_KEY=`. Never paste them in chat.

## 2. Free checks
From `smartlead_sync`, using the venv's `python.exe`:

```
python dynadot_cli.py balance                     # should show the USD top-up
python dynadot_cli.py check --names C:\Users\<you>\.infrabot\dynadot\names.txt
```

`check` shows availability and the price per name. Before 27 Nov it will show the regular `.co` price, which is expected. To see whether search shows sale prices at all, `check` one `.xyz` or `.club` name: those are already on sale at $1.50.

## 3. Sandbox test (no real money)
```
python dynadot_cli.py --sandbox balance
python dynadot_cli.py --sandbox buy --names C:\Users\<you>\.infrabot\dynadot\names.txt --count 3 --approve --total-ceiling 100 --price-ceiling 100
```

The sandbox has its own prices, so its ceilings are raised for the test. It should report `3 of 3 bought`. The live run refuses to start until a sandbox run has bought at least one domain (`--require-sandbox-pass`).

## 4. The live run (Task Scheduler, 27 Nov 05:35 IST)
`scripts\dynadot_schedule.ps1` registers a one-time task named "InfraBot Dynadot BF buy". The task:
- runs `pythonw dynadot_cli.py buy --count 100 --approve --allow-spend --require-sandbox-pass --wait-minutes 180 --notify`
- wakes the PC to run (the PC must be plugged in and not shut down; sleep is fine)
- DMs Manveen the result on Slack and writes `buy.log` next to the ledger

To pause it: `Disable-ScheduledTask "InfraBot Dynadot BF buy"`. To run it by hand instead, use the same command without `pythonw`.

If it stops early (ran out of balance, ran out of names, unclear answers), fix the cause and run the same command again. The sale runs until 5 Dec.

## After buying
The domains sit unused in Dynadot. When a client needs them, point the nameservers at the mailbox provider (Zapmail or ScaledMail) and add them to the tracker. Year two costs about $15.50 a `.co`; turn auto-renew off unless a domain is still in use.
