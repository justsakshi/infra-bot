# Today's Work Explained — Plain English

**What we did, why, how it works, and how to check it. Written for a non-engineer to follow.**
Date: 2026-07-07. Full technical detail in [DELIVERABILITY_MASTER_PLAN.md](DELIVERABILITY_MASTER_PLAN.md), [OPERATOR_PLAYBOOK.md](OPERATOR_PLAYBOOK.md), [SCALE_ROADMAP.md](SCALE_ROADMAP.md).

---

## 1. Where this started

You and Avinash had a call reviewing the Inbox Health workbook. Three things came out of it:
1. The workbook's warmup automation had a rule that turns out to be **wrong** ("turn warmup off once an inbox is actively sending").
2. When boosting a struggling inbox, the instinct was "send more emails" — Avinash said the real lever is the **reply rate**, not volume.
3. DNS problems (SPF/DKIM/DMARC) are **Zapmail's job to fix**, not ours — we should detect and ask them, never edit DNS ourselves.

You asked me to: research what Smartlead and Instantly themselves recommend, fix anything we were doing wrong, build the automation, and test it end-to-end.

---

## 2. What we found in research (why the rule was wrong)

I read Smartlead's and Instantly's own official help docs and blog posts (not third-party guesses). Both companies say the same thing in different words:

> **"Never turn warmup off while an inbox is sending real campaigns."**

Reasoning: warmup email traffic is what tells Gmail/Outlook "this is a real, active, engaged inbox." If you switch it off the moment real campaign emails start, the inbox loses that signal and its reputation quietly erodes over 6-8 weeks — by the time you notice, it's already landing in spam.

The fix both companies use: **keep warmup on always, just send fewer warmup emails while campaigns are running** (because the inbox now has a daily sending limit shared between warmup and real emails). Smartlead even has a built-in feature for this called "auto-adjust" that automatically dials warmup down when a campaign is active.

We also found the exact numbers Smartlead recommends:
- Warmup volume should never exceed **40 emails a day** for an inbox (we had this at 50 in one place, wrong).
- While an inbox has a live campaign attached, warmup should drop to about **20 emails a day** (with auto-adjust handling the exact number).
- A brand-new inbox needs **21-30 days** of warmup before it's safe to send real campaigns — not 2 weeks like we assumed.
- The "reply rate" setting (how often the warmup network replies to this inbox's warmup emails) should sit at **25-30%**, never higher — an inbox that gets replied to too often looks fake to spam filters.
- When an inbox's reputation drops, the fix is to **raise the reply rate to 30%**, not to blast more volume at it. This directly confirms what Avinash said on the call.

---

## 3. What we built (the automation)

Think of it as a robot that visits every inbox, every client, every day, and asks: *"what state is this inbox in, and is its warmup setting correct for that state?"*

### The four states every inbox can be in

| State | Meaning | What the robot sets |
|---|---|---|
| **NEW** | Just provisioned, still ramping up | Warmup 40/day, increasing +5/day |
| **ACTIVE** | Attached to a live campaign, actually sending | Warmup 20/day + Smartlead's auto-adjust turned on |
| **IDLE** | Sitting on the bench / between campaigns | Warmup 20/day, steady |
| **RECOVERING** | Reputation has dropped below 90% | Warmup 15/day, but **reply rate boosted to 30%** |

The robot never turns warmup fully off — the only inbox with warmup off is one that's been officially retired.

### How it decides

It reads the same data the daily sync already pulls (which campaign an inbox is on, whether that campaign is actually alive or a "zombie", the inbox's current reputation number) and picks a state for each inbox. If the inbox's current setting doesn't match what its state calls for, the robot logs a planned change. It does NOT touch the live inbox unless we've explicitly told it to (see "dry-run" below).

### Other pieces built the same day

- **Bounce protection sweep** — Smartlead has a feature that auto-pauses a campaign the moment its bounce rate crosses a threshold, so a bad list can't quietly wreck a domain's reputation while nobody's watching. We built a robot that checks every active campaign and turns this on (3% threshold) if it's missing. We ran it — **found 24 live campaigns across all 4 clients with this protection missing.**
- **Blacklist monitor** — checks every domain we send from against the serious spam blacklists (the kind that actually matter, not the pay-to-remove-yourself scam lists). Runs every Monday.
- **DNS message fix** — when the workbook flags a broken SPF/DKIM/DMARC record, the instruction now correctly says "tell Zapmail" instead of implying we should fix it ourselves. Also: Outlook inboxes often don't have a DKIM record by design, so we downgraded that specific case from "urgent, broken" to "check with Zapmail first" so it doesn't cry wolf.

---

## 4. What "dry-run" means (important — nothing live changed yet)

Every one of these robots has a safety switch. Right now they're all set to **look and report only** — they calculate exactly what they would change, print it out, and touch nothing. This is standard practice for anything that could mess with a client's sending: you watch what it WANTS to do for a few days, confirm it's sane, then flip a switch to let it actually act.

We ran all three in this "look only" mode against your real, live account data today to prove they work correctly. Results below.

---

## 5. How we tested it (end-to-end, against real data)

1. **Logic test** — fed the warmup robot 8 made-up example inboxes covering every situation (new, active, idle, recovering, blocked, already-correct) and checked it made the right call on all 8. Passed.
2. **Live warmup dry-run** — pointed it at your actual Smartlead accounts (all 4 clients). It looked at every real inbox and proposed **325 changes**: 16 inboxes that had warmup fully off, 274 that had the wrong volume for their state, 35 that needed the reputation-recovery treatment. Zero inboxes told to turn off — correct, matches the "always on" rule.
3. **Live bounce-protection dry-run** — found the 24 unprotected campaigns listed above.
4. **Live blacklist scan** — this is the one that turned up something serious (next section).
5. **Code checks** — every changed file was syntax/compile-checked before and after.

---

## 6. The thing we found while testing: a real problem

Running the blacklist checker against every domain across all 4 clients (118 domains total), **105 of them are listed on SURBL**, a blacklist that spam filters check.

At first glance this looks like the whole fleet is on fire. We dug into how SURBL actually works before jumping to conclusions: it doesn't blacklist your *sending* domain directly — it blacklists domains that show up as **links inside the email body** (a tracking link, a signature URL, an unsubscribe link). So the real question isn't "is our domain listed" — it's "do our emails contain links pointing at a listed domain." That's the next thing to check before deciding whether this needs urgent action or is lower-stakes than it looks.

We did NOT take any action on this (no delisting requests, no domain changes) — flagging it for you and Avinash to decide, with the correct context so nobody overreacts or underreacts.

---

## 7. What's NOT done yet

- Nothing built today is live — it's all sitting on this computer, not deployed to the server that actually runs your crons (Render). Someone needs to push it.
- Even after deploying, the safety switches stay off until you decide to flip them (there's a recommended week-by-week order for turning them on safely — bounce protection first, then warmup, then the retest system).
- The SURBL finding needs a decision, not just data.
- A "tell us when to buy more inboxes" feature does not exist yet — that's the next thing to build if you want it (we scoped it, didn't build it).

---

## 8. How to check any of this yourself, going forward

- Every robot prints a plain-English log line every time it runs (in Render's logs) — e.g. "16 enable, 274 retune, 35 boost" for warmup, or "24 campaigns missing bounce protection."
- The Inbox Health Google Sheet workbook is the human-facing view — it's refreshed by the daily sync (see below) and shows one row per inbox with a grade, the top problem, and exactly what to do about it.
- Full "what do I check and when" routine is written out in [OPERATOR_PLAYBOOK.md](OPERATOR_PLAYBOOK.md) — daily/weekly/monthly manager checklist.

---

## 9. The Google Sheets sync

The daily sync (`run.py`) is the thing that actually populates your Google Sheets workbook — it logs into Smartlead for all 4 clients, pulls every inbox and campaign, checks DNS, computes each inbox's health score, and writes it all to the sheet. It normally runs automatically once a day at 10 AM. We ran it manually just now, on demand, so the workbook reflects the corrected DNS-flag wording and Outlook-DKIM handling from today's changes.
