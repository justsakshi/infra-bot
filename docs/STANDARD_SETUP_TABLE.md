# Standard Inbox Setup — Every Number, Every State (for Avinash's review)

**Date:** 2026-07-10 · **Requested on today's call** ("create a standard setup and share that with me, all these figures")

One open question changes one column of this table — flagged at the bottom.

---

## 1. The four states and what triggers them

| State | Trigger (automatic, re-evaluated daily) |
|---|---|
| **NEW** | Warmup younger than 21 days |
| **ACTIVE** | Attached to a live campaign and actually sending |
| **IDLE** | On the bench — no live campaign (sub-state LONG_IDLE after 30+ days without a send) |
| **RECOVERING** | Warmup reputation < 90%, or post-incident (failed placement test) |

## 2. The numbers per state

| Setting | NEW | ACTIVE | IDLE | LONG_IDLE (30d+) | RECOVERING |
|---|---|---|---|---|---|
| Warmup on? | ✅ always | ✅ always | ✅ always | ✅ always | ✅ always |
| Warmup emails/day (max) | **40** (ramping +5/day from 5) | **20** | **20** | **20** | **15** |
| Warmup reply rate % | 25 | 25 | 25 | **28** | **30** |
| Smartlead auto-adjust | off | **on** (trims warmup 7-10 on send days) | off | off | off |
| Cold emails/day (hard cap in code) | 0 (not campaign-eligible) | **Outlook 10 · Gmail 25 · SMTP 15** | 0 | 0 | 0 |
| Campaign-eligible (precise-automator)? | no | yes | yes, if rep ≥ 90 + fresh inbox test | no — flagged for retest first | no |
| Extra | — | — | — | flagged for placement retest | boosted reply rate rebuilds rep fastest |

**Warmup never turns off** in any state — the old off-when-sending behavior rots reputation in 6-8 weeks (Smartlead's + Instantly's own docs). Only exception: inbox formally retired.

## 3. Fleet-wide guardrails (state-independent)

| Guardrail | Value | Why |
|---|---|---|
| Cold send cap per provider | Outlook 10 · Gmail 25 · SMTP 15 /day | Enforced in the capacity number every tool reads — no tool can assign more, regardless of the Smartlead daily-limit setting |
| Daily-limit ceiling | 45/day total | Smartlead's own optimal band is 20-49/day (88% inbox placement above 50 drops) |
| Bounce auto-pause | 3% on every active campaign | Campaign pauses itself before a bad list burns the domain |
| Placement retest floor | never < 7 days since last test | Reputation moves slowly; earlier retest wastes a credit |
| Retest trigger | no test on record, or test > 14 days old | Worst-first, 2/client/day, ~120 PL credits/month ≈ 2-4 tests/day fleet-wide |
| Warmup reputation floor | 90% | Below → RECOVERING profile + pulled from campaign eligibility |

## 4. Match against the numbers you gave on the call

| Your number (call, 2026-07-10) | Our setting | Match? |
|---|---|---|
| Cold: 10 (Outlook) to 30 (Gmail) | 10 / 25 | ✅ inside your band (Gmail 25, can raise to 30 on your word) |
| Warmup: 30-50/day separate | NEW 40, others 15-20 | ⚠️ NEW fits; ACTIVE/IDLE at 20 sits below your 30-50 — intentional while inboxes also carry campaign volume; say the word and the standard becomes 30 |
| Reply rate standard 20% | 25% (28-30 when recovering) | ⚠️ ours slightly higher; happy to standardize at 20 with raised-when-bad kept |
| Randomize daily warmup count | Smartlead treats our number as a MAX and varies the actual daily count itself | ✅ **verified with live data** (one BW inbox's last 7 days: 10, 15, 20, 28, 33, 35, 25 — never flat) |

## 5. The one open question (support ticket pending)

**Is `max_email_per_day` campaign-only or campaign + warmup combined?**
The API reference says *"including warmup and campaign emails"*; the app UI says *"warmup not included."* These cannot both be true.

- If **combined** (API doc): our BW daily-limit raises (30→45 etc.) were necessary and stay.
- If **separate** (UI): the raises bought nothing and we roll all 15 inboxes back to their original limits from the timestamped snapshot — a 10-minute, fully-scripted revert. Warmup profiles, provider caps, and everything else in this table are correct either way.

Also asked: whether SmartDelivery test sends count against the same limit (affects how many tests/day per inbox are safe).
