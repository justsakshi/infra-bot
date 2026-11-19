# infra-bot: notes for agents

This repo is public: no sending domains, inboxes or test results in code, tests, commits or PR text.

## Which inboxes may send (one answer for every agent)

- The answer is the All Inboxes tab this repo writes every day (`smartlead_sync/smartlead/processing.py` + `usable.py`). Read its Busy Reason with `usable.send_verdict`: `never` (tagged, failed v2), `cannot` (no connection, DNS, warm-up blocked), `hold` (no v2 pass in 10 days, listed domain; Google: quiet, low warm-up), `send`.
- gtm reads the same tab through `tools/lib/inbox_list.py` (presend, sl_guard); Campaign Desk through its sender gate. Report and chart scripts group rows with `usable.team_group` and add no rule of their own.
- Never judge an inbox from a chart, a table, memory or an old report. If one of them disagrees with the tab, the tab wins; changing the answer means changing `usable.py`, and a change that frees inboxes needs Manveen's yes.
