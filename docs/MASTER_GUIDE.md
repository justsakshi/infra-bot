# Master Guide — Deliverability System

**One document: what we built, how to use it, what everything means, and what a campaign manager does day to day.**

Created: 2026-07-08. Sheet: https://docs.google.com/spreadsheets/d/197vytufJI-r2ruOrkmox4tm_r1FNoE_rihuUE254zhU

Deployed commit: `4443a3f` on `main` (Render auto-deploys from this branch).

---

## 1 — What we built (in one paragraph)

A system that watches every inbox across every client, every day: scores it, tells a manager exactly what's wrong and what to do about it, and automatically maintains warmup and bounce-safety settings so inboxes don't quietly go bad. It also checks every sending domain against real spam blacklists weekly. Everything new ships in "watch only" mode — it calculates and logs what it would do, but changes nothing live until a switch is flipped on Render.

---

## 2 — The Google Sheet: every tab, what it's for, how to read it

**URL:** https://docs.google.com/spreadsheets/d/197vytufJI-r2ruOrkmox4tm_r1FNoE_rihuUE254zhU

### Tab: "All Inboxes" — the master census
One row per inbox, every client, deduplicated (an inbox can be attached to more than one campaign — this tab merges those into one row).

| Column | What it means |
|---|---|
| Email / Client / Campaign | which inbox, which client, what it's currently sending for (or "N/A" if idle) |
| Daily Limit / Leads Remaining | how many it can send today, how much work is left on its current campaign |
| Availability | **FREE** = passed every safety check, usable right now. **BUSY** = something's wrong |
| Busy Reason | exact reason(s) when BUSY — disconnected, low reputation, failed test, DNS broken, no capacity. Comma-separated if more than one |

**Use it for:** "I need a healthy inbox for a new campaign" → filter Availability=FREE, pick one. Not for diagnosing problems — that's the next tab.

### Tab: "Inbox Health" — the one you work from daily
Same inboxes, scored and prioritized. **This is the tab every manager opens first, every day.**

| Column | What it means |
|---|---|
| Priority | P0 (fix today, actively broken) / P1 (this week) / P2 (routine) |
| Score / Grade | 0-100, A/B/C/D. See scoring formula below |
| Trend | ↑/↓/→ vs 7 days ago — **this is your early warning system** |
| Top Problem | the single worst thing wrong with this inbox right now |
| What To Do | the exact fix, in plain language |
| Owner | 🤖 = system handles automatically, ignore it. 👤 = you need to act |
| Manager | who's responsible for this client |

**The score formula (out of 100):**
- **Placement — 40 pts.** Did its last spam-placement test land in the inbox or the spam folder? Biggest weight, most direct signal.
- **Warmup reputation — 25 pts.** Smartlead's own trust score for the mailbox (needs ≥90% to score well).
- **Bounce rate — 20 pts.** How many of its sends are bouncing.
- **Connection — 15 pts.** Is it actually connected, and does its domain have valid SPF/DKIM/DMARC.

90+ = A · 70+ = B · 50+ = C · below 50 = D.

**How you actually use this, daily (5 min):** open the tab, filter to your client, sort by Priority. Every P0 row — fix today, "What To Do" tells you how. P1 — this week. Skip anything marked 🤖. Watch the Trend column for anything sliding down before it becomes a P0.

### Tab: "Campaign Metrics" — is the campaign itself working
Per-campaign send/reply/positive-reply numbers, last 7 days. Used Fridays: 21-day-old campaign at ≥2× baseline reply rate → scale it; near baseline → iterate copy; <50% of baseline → kill it.

### Client deliverability test tabs (Belardiwong, Melior, Precise Leads, Darlean new, Mythic)
Raw spam-placement test results — one row per test, which domain, when, inbox or spam. This is the raw material that feeds the Placement score in Inbox Health. You don't read this daily; the score is the finished product.

---

## 3 — What we built behind the scenes (the automation)

Six jobs run on a schedule (Render, all times IST). Each one reads the Sheet data above, or writes into it.

| Job | When | What it does | What to check |
|---|---|---|---|
| **Daily sync** (`run.py`) | 10:00 daily | Pulls every inbox/campaign from Smartlead, checks DNS, scores everything, writes all 4+ tabs above | Sheet has today's date; row counts look normal (~376 inboxes) |
| **Auto placement-test** | 11:00 daily | Runs a real spam-placement test on stale/untested inboxes | Log shows test targets picked |
| **Auto-warmup** | 11:30 daily | Sets the correct warmup volume/settings per inbox (see §4) | Log shows change counts |
| **Headroom fix** (NEW) | 12:15 daily | Raises an inbox's daily send limit so warmup actually has room to run (see §4) | Log shows inboxes fixed |
| **Bounce auto-protect** (NEW) | 12:30 daily | Turns on Smartlead's own bounce-safety switch on any campaign missing it | Log shows campaigns fixed |
| **Blacklist monitor** (NEW) | Monday 9:00 | Checks every domain against real spam blacklists (Spamhaus, SURBL, URIBL) | Log shows any listed domains |

**Every one of these is currently in "watch only" mode** — they calculate and print what they'd do, but change nothing live. Turning them on is a deliberate, one-at-a-time process (§6).

---

## 4 — Warmup, explained properly (the core fix)

**The bug we fixed:** the old system turned an inbox's warmup completely OFF the moment it started sending a real campaign. Both Smartlead and Instantly (the platforms themselves) say this is wrong — it lets reputation quietly rot over 6-8 weeks.

**The fix — 4 states, warmup always stays on, only the volume changes:**

| State | Meaning | Warmup setting |
|---|---|---|
| **NEW** | Just provisioned | Climbs from low up to a 40/day ceiling, +5/day |
| **ACTIVE** | Sending a real campaign right now | 20/day + Smartlead's own auto-adjust feature turned on |
| **IDLE** | Sitting on the bench, no live campaign | Flat 20/day |
| **LONG_IDLE** | Idle 30+ days | Same 20/day, but reply rate nudged up + flagged for a re-test before it's ever reused |
| **RECOVERING** | Reputation dropped below 90% | 15/day, but reply rate boosted to 30% — the fix is MORE REPLIES, not more volume |

**Honesty note, because this matters:** the guardrails here (40/day ceiling, never fully off, auto-adjust exists) are confirmed directly from Smartlead's own documentation. Two specific numbers — the exact reply-rate percentage and the ramp increment — come from one of two numbers Smartlead's own pages disagree on; we picked the more conservative one. The IDLE and RECOVERING volume numbers are our own engineering judgment — no vendor publishes a number for those situations.

**The headroom problem we also found and fixed:** Smartlead's daily-send-limit field is ONE shared bucket for both warmup and campaign emails. Every real inbox we checked was capped at 30 or less — meaning if a campaign used the full 30, warmup had zero room left, even with "warmup on." The headroom-fix job raises that ceiling (+20) specifically for inboxes in a live campaign, confirmed live on **116 real inboxes**.

**Order matters:** the headroom fix must be turned on BEFORE the warmup automation, or warmup gets a target with no room to actually run.

---

## 5 — What "going bad" looks like, and how the system flags it

This is the direct answer to "how will it flag something before it goes bad":

1. **Trend arrow in Inbox Health** — score dropping over 7 days, before it's even a P0. This is the earliest visible signal.
2. **Grade slipping** (A→B→C→D) — same idea, coarser.
3. **P0 appears** — something crossed a hard line: failed placement test, DNS broken, warmup blocked, landing in spam during warmup.
4. **Bounce auto-protect** (once enabled) — Smartlead itself pauses a campaign the instant its bounce rate crosses 3%, no human needed to catch it.
5. **Blacklist monitor** — weekly check catches a domain getting blacklisted, even if nothing else changed yet.
6. **Reply rate reality check** (not yet built) — the single best leading indicator (a reply-rate drop shows up ~48 hours before opens/bounces move) isn't automated yet. Currently a manual Monday check. Flagged as the next thing worth building.

---

## 6 — What a campaign manager does, day by day

### Every day (5 min)
1. Open **Inbox Health**, filter to your client, sort by Priority.
2. Any P0 row — fix TODAY:
   - Failed placement test → pause inbox, check copy vs a healthy sibling, wait 7 days, re-test.
   - SPF/DKIM/DMARC broken → **escalate to Zapmail support, never edit DNS yourself.** Outlook DKIM warnings specifically may be a false alarm — ask Zapmail to confirm.
   - Warmup blocked → young inbox (<30 days), retire it. Established, wait 48h then re-enable at low volume.
3. Ignore every row marked 🤖.

### Monday (30 min)
1. Any grade that dropped since last week (Trend ↓) — look before it turns red.
2. Reply rate <1% after 200+ sends → compare copy to a healthy sibling inbox, or rotate it out.
3. Bounce >3% → clean the list. >5% → pause that campaign now.
4. Read the blacklist report — any of your client's domains newly listed.

### Wednesday (15 min)
Respond to positive/interested replies fast. Categorize replies correctly in Smartlead — your manual tags feed the metrics.

### Friday (20 min)
21-day-old campaigns: ≥2× baseline → scale. Near baseline → iterate copy. <50% → kill.

### Every other Monday (30 min)
Retire bad inboxes. Promote warmed bench inboxes to active (only if rep ≥90% and old enough). <5 spares → order new domains now (4-6 week lead time).

### Monthly (1st)
Fleet-wide placement test sweep, target ≥85% landing in inbox.

---

## 7 — The SURBL finding (what it is, what it means, what to do)

**What we found:** 105 of 120 sending domains are listed on SURBL, a blacklist that spam filters check.

**What SURBL actually does — this matters, don't skip it:** SURBL doesn't blacklist your sending address. It blacklists domains that appear as **links inside the email body** — a tracking link, a signature URL, an unsubscribe link. Being listed as a pure FROM domain, with no links pointing at it, has limited direct impact.

**What we checked live:** almost every campaign across the fleet already has link-click tracking turned OFF — meaning most of the 105 listed domains aren't actively creating a link-based penalty right now.

**The 3 campaigns that ARE exposed** (click-tracking on + sending from a listed domain):
- `Fractional CFO Firms | Precise Leads`
- `Melior -Executive Search- Round Table`
- `Melior - Roundtable Chemicals (Sales Leader)`

**Important correction:** Smartlead locks a campaign's tracking setting once it starts sending — you cannot flip it off mid-flight on a live campaign. So the fix for these 3 is NOT "turn off tracking." Real options:
1. **Clone the campaign** with tracking off from the start, move leads over.
2. **Replace the sending domain(s)** on these campaigns with clean, non-listed ones.
3. **Delist the domain** at SURBL directly (evidence-based, per-domain — don't mass-request, it looks like spam behavior and gets ignored).

**What NOT to do:** don't panic-delist all 105 domains. Most aren't actively hurting you because tracking is off. Only the 3 above need action.

---

## 8 — Turning the automation on (once you're ready)

Never flip two at once. Wait 2-3 days between each, watching the Render logs.

```
STEP 1   HEADROOM_FIX_ENABLED=true       (raises daily limits so warmup has room)
STEP 2   BOUNCE_PROTECT_ENABLED=true     (turns on the 3% auto-pause safety net)
STEP 3   WARMUP_AUTO_ENABLED=true        (applies the 4-state warmup fix)
LATER    RETEST_ENABLED, ROTATION_ENABLED (pre-existing systems, separate rollout)
```

No env variables are required to deploy — every new setting defaults to OFF/dry-run. You only add a variable on Render when you're ready to flip that specific switch.

---

## 9 — What this does NOT cover yet (be honest about this)

- Campaign copy quality (spam words, links, HTML vs plain text) — not automated.
- List verification before upload — not automated.
- "You need N more inboxes, order them now" capacity planning — scoped, not built.
- Reply-rate-drop early warning — the best leading indicator, not built yet.
- Rest-based domain rotation (resting a tired domain instead of only replacing it) — not built.
- The SURBL delisting itself — detected, not fixed. That's a manual per-domain decision.

This system makes the automation correct and adds real detection that didn't exist before. It is not the finish line for deliverability — it's the foundation the rest gets built on.
