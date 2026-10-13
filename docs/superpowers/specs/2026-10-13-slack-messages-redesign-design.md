# Slack messages: headline in the channel, details in the thread

Date: 2026-10-13. Approved in chat by Manveen (details in a thread, option 1: posted into the thread at once).

## Problem
The bot's 15 scheduled channel posts and the `/domains` screens are walls of plain text. Every post repeats the same boilerplate ("Should kill = the team's rule: …", "To stop paying for an order …", price lists, "Views are read-only …"). The lists (every mailbox, every domain, 34 past-client domains) sit in the main message. A person cannot tell in five seconds whether anything needs doing.

## The rule (every message)
1. **Headline:** one line, starting with ✅ (nothing to do), ⚠️ (look at it) or 🚨 (act today), plus the date.
2. **The numbers that matter:** at most 3 short lines (counts per client, totals). No lists.
3. **Details in the thread:** the lists, the per-item reasons and the team rule are posted as thread replies right away. Slack collapses them under "N replies", which works as the "show details" button. The team acts from the thread and it stays as the record.
4. **Said once:** boilerplate (what a rule means, how to stop paying, prices) goes in the thread's last reply at most, or in one pinned "How to read the bot" note. Never in the headline.
5. **Plain words:** no internal names (`/domains review`, `ZAPMAIL_APPROVERS`) in the headline. An error says what happened and what to do ("Suggestions took over 10 minutes. Zapmail's search limit, try again at 14:30"), never a bare ":x: timed out".

## How (one helper)
`smartlead/notify.py` gets `post_with_thread(token, channel, headline_lines, thread_parts)`:
- posts the headline (headline + up to 3 lines) as the parent message,
- posts each thread part as a reply (`_post(..., thread_ts=ts)`), splitting any part over 40 lines,
- returns the parent ts; if the parent fails, nothing else posts; if a reply fails, the parent stays and the failure is logged.
Every formatter changes from "one long string" to `(headline_lines, thread_parts)`. The posting scripts call the helper. JS-posted channel messages (`index.js`) use the same shape through `chat.postMessage` + `thread_ts`.

## Phase 1: channel posts (before → after)

| Post | Headline + numbers (channel) | Thread |
|---|---|---|
| Deliverability test results (per client) | `✅ Precise Leads, 13 Oct: all 30 mailboxes land in the inbox` or `⚠️ Bettrdata, 13 Oct: 11 of 71 mailboxes in spam (3 domains)` + one line "Google→Google 16% spam, Outlook→Google 14%" only when a route is over 5% | 1) mailboxes in spam with action (remove from X / not in a campaign), 2) domains to keep out of new campaigns, 3) the full route table |
| Kill warnings (Tue) | `⚠️ Kill warnings, 13 Oct: 19 inboxes should come out of campaigns (6 domains), 2 to watch` + `Bettrdata 13 · Precise Leads 6 · Melior 0` | one reply per client (inbox, domain, reason), then one reply: the rule + what to do |
| Renewal review (09:50) | `⚠️ Bills in the next 7 days: $X across N bills, retiring the flagged inboxes saves $Y/mo` | one reply per bill (keep / retire / check lists), then the copy-ready support message |
| Domains & inboxes daily (09:40) | `✅/⚠️ Domains & inboxes, 13 Oct` + up to 3 lines: what bills soon (sum), what is being set up / ready, anything late | Zapmail section, ScaledMail section, purchase batches, past-client domains lapsing (one line each, full list) |
| Infra audit (10:20) | `⚠️ Infra audit: N problems (P0 x, P1 y)` | one reply per problem with examples + fix |
| Message sequence refreshed | `✅ Message sequences refreshed` (only problems get a second line) | problems list |
| Inboxes ready in Smartlead | `✅ 20 SMTP inboxes ready in Smartlead (Precise Leads)` | per order: domains, what was set |
| Inbox billing watch | unchanged (already one line per event); drop the repeated "Unpaid inboxes get suspended …" to the thread | — |
| Daily Renewal Check (10:00) | already short with Read more buttons; only wording: headline with ⚠️ and the count | unchanged |
| Short alerts (API key, blacklist, reply rate) | unchanged structure; trim the repeated advice sentence to the thread | the advice |

A pinned note "How to read the bot" (posted once by hand from a text in `docs/`) explains ✅/⚠️/🚨, the kill rule, retire vs check, and where to act.

## Phase 2: `/domains` screens (same rule, in place)
- **Home:** buttons only, grouped (Buy · Check · Tracker), one line of help. Drop the read-only/approver sentence (the buttons ask anyway).
- **Fleet status:** headline (`$662.75/mo · 8 orders · 43 domains`) + per-provider line; orders behind **Show orders** (replaces the screen, ← Back).
- **Renewals:** headline with what bills in 14 days; past-client domains as one count line with **Show** instead of 34 names inline.
- **Before the bill (review):** one line per bill (keep/retire/check counts + saving) with **Show inboxes** per bill.
- **Purchase plans:** active and placed plans only; dropped/failed older than 7 days behind **Show old plans**.
- **Suggestions:** already trimmed (common price said once).
- **Errors:** human sentences with the next step (see rule 5).

## Out of scope
Changing what the checks compute; the Infra Bot app's settings (its owner is deactivated; threads need no new permissions); email digests.

## Testing
- Each formatter: pure tests on today's real shapes (13 Oct deliverability, kill warnings, renewal review) asserting the headline is ≤ 4 lines with no mailbox address in it, every listed item appears in the thread parts, and boilerplate appears at most once.
- `post_with_thread`: fake `_post` asserting parent first, replies carry its ts, 40-line split, parent failure stops replies.
- Phase 2 screens: block tests like `test_slack_reply.js` (headline block, Show button replaces in place, nav row).
- Live: one manual run of each phase-1 script into a test channel (env var pointed at it) before the channels.
