# Inbox evidence: how an inbox is judged

Manveen, 16 Nov 2026: "can't really trust that you can see all results replies everything", then "so complete your data we can't take chances we should do everything we can do".

No single source says whether an inbox's email reaches the inbox. This page lists the four we use, what each one proves, and how they combine. The rules apply to any client.

## The four sources

| Source | Job | What it proves | What it cannot prove |
|---|---|---|---|
| Smart Delivery v2 test (seed inboxes) | `v2_weekly.py`, Saturdays | Where the test email landed at Google and Microsoft seed inboxes, per sending inbox | How real recipients' filters treat it. Google results are report-only (`v2_trust`) |
| Own-recipient placement check | planned (t17 item 4) | Where a plain email from the inbox lands in mailboxes we control and read over IMAP | Anything about other recipients' history with us |
| Warm-up spam % | `fleet_watch.py`, daily | The share of warm-up emails in spam over 7 days | Real campaign mail, which has other copy and other recipients |
| Real sends: replies and out-of-office | `inbox_evidence.py`, Sundays | That an email from this exact inbox reached a real recipient's inbox | Silence is not spam. Most cold emails get no answer |

## Real sends (`inbox_evidence.py`)

- **Every lead, no sample.** The job takes every lead emailed in the last 28 days in a live campaign, skipping DT and test campaigns. It reads each lead's message history, which names the inbox that sent each email (`stats_id` → `from`). Each email counts for the inbox that sent it. A reply counts for the inbox that sent the step that was replied to. The share of sends whose inbox is known is printed as "sender known for N%".
- **Per recipient provider.** The lead's MX decides it: Gmail, Microsoft, Proofpoint or other.
- **Reply**: a reply that is not out-of-office or a delivery-failure notice. "Wrong person" counts: a person answered, so the email reached the inbox (provider_watch still leaves it out of reply rates).
- **No known sender**: a lead removed from Smartlead after it was emailed keeps its statistics rows but has no message history, so its sends have no sender. They are counted per client and named in the digest detail.
- **Out-of-office (OOO)**: proves the email reached the inbox. Gmail and Exchange send no auto-reply for mail in spam or junk.
- **Bounce**: the email was marked bounced, or the "reply" is a delivery-failure notice. Bounces are kept apart, because a bounce says the address was bad (the lead list), not where the email landed.
- **Labels**, per inbox per provider, over 28 days. The base rate is the client's (replies + OOO) per email at that provider, across all its inboxes.
  - **reached the inbox**: at least one reply or OOO.
  - **quiet (0 of ~N expected)**: at the base rate, 3 or more were expected and none came. If an inbox truly matched the client's rate, the chance of 0 when 3 are expected is 5%. So quiet is a warning to check with the other sources, not a verdict.
  - **not enough data**: fewer than 3 expected. This is never read as "fine".
- **Reply-log check.** The team's trusted reply logs (`REPLY_LOGS`: the sheet, tab and columns per client) are compared with what the run found. Every logged reply in the window counts, one per lead. The result is a coverage % and a reason for each miss:
  - Smartlead shows no reply from this lead;
  - the lead is in no campaign read in the window;
  - the reply was found, but the sending inbox is unknown.

  A log the service account cannot open is reported, never worked around.
- **Where it shows.** The week is kept in Mongo `inbox_evidence`, and the per-lead sender map in `lead_senders` (lead ids only, no addresses). All Inboxes gets two columns, "Real sends (28d)" and "Real-send evidence". The digest gets one done line per client.
- **Report only.** None of this makes an inbox BUSY or FREE, takes it out of a campaign, or brings it back. Acting on it is a separate decision for Manveen.

## Combining them

- An Outlook inbox is judged by v2 at its own provider (`docs/V2_WEEKLY.md`), and only v2 takes it out or brings it back.
- When v2 and real sends disagree, it is shown, never settled in silence. For example, a v2 fail on an inbox whose real sends "reached the inbox" at the same provider in the same weeks. The real-recipient check (`real_recipients.py`) settles disagreement per inbox type through `v2_trust`.
- A Google inbox has no trusted v2 result. Real-send evidence, warm-up spam % and the own-recipient check are its evidence. The rule for acting on them is not decided yet; it is parked for Manveen's yes.
- A blocklist listing is never an ask (Manveen, 16 Nov 2026).
