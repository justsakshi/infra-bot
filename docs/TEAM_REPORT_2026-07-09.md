# Team Report — Inbox Deliverability System: What It Does & What Changed (Belardi Wong pilot)

**Date:** 2026-07-09 · **Scope of live changes:** Belardi Wong only (pilot client) · **Shareable:** yes, written for the whole team

---

## The one-paragraph version

We built and tested a system that watches every inbox daily, scores its health, fixes the mechanical stuff automatically (warmup settings, send-limit headroom, bounce safety switches), and tells the right manager on Slack exactly what needs a human. Today it went live on one client — Belardi Wong — as a pilot. Every change was recorded with before/after values and timestamps, verified by reading the values back from Smartlead, and can be rolled back. Nothing sends more cold email than before; what changed is that reputation-building warmup traffic now actually runs alongside campaigns, and safety nets that didn't exist now do.

---

## What the system does (the 10 background jobs)

| Job | What it does | Runs |
|---|---|---|
| Daily sync | Pulls every inbox/campaign, scores health 0-100, writes the Google Sheet workbook | 10:00 daily |
| Blacklist monitor | Checks every sending domain against real spam blacklists (SURBL/Spamhaus/URIBL) | Mondays |
| Capacity planner | "Do we have enough healthy inboxes? Order N domains by DATE" advisory | Mondays |
| Reply-rate monitor | Watches per-domain reply rates — the earliest warning (~48h ahead) that deliverability is slipping | Daily |
| Auto-retest | Requests fresh spam-placement tests for inboxes with stale results | Daily |
| Warmup manager | Keeps warmup ON at the right volume for every inbox's state — never turns it off | Daily |
| Headroom fix | Makes sure each inbox's daily limit has room for warmup beside campaign sends (capped at 45 total) | Daily |
| Bounce protection | Turns on Smartlead's auto-pause (3% bounce) on any campaign missing it | Daily |
| Rotation | Would swap broken senders for healthy spares — **kept OFF by decision** (riskiest, moves leads) | — |
| Slack digest | Daily action list to the team channel, @-mentioning each client's manager | With sync |

---

## What actually changed today (Belardi Wong, all verified live)

**1. Bounce auto-pause: 3 campaigns, nothing → 3%**
Hospitality Multi-Property Chains (all 3 variants). If bounce rate spikes, Smartlead now pauses the campaign itself before the domain's reputation takes damage. Before: nothing would have stopped it.

**2. Daily send limits: 15 inboxes raised**
- 6 inboxes: 10 → 30/day
- 9 inboxes: 30 → 45/day
Why: Smartlead's daily limit is ONE shared bucket for campaign sends AND warmup emails. At the old limits, campaigns used the whole bucket and warmup silently sent **zero** — cold email was going out with no reputation-building traffic beside it. The raise gives warmup real room. **Campaign volume itself is unchanged** — those settings weren't touched. Hard cap at 45/day total keeps every inbox inside Smartlead's own documented optimal band (20-49/day = 88% inbox placement; above that placement drops).

**3. Warmup settings: 55 inboxes corrected, 0 turned off**
Every inbox now runs the warmup profile matching its state: bench inboxes 20/day, campaign-attached inboxes 20/day with Smartlead's auto-adjust on, new inboxes ramping to 40/day. Reply-rate setting standardized at 25%. The old system's biggest flaw — turning warmup OFF the moment an inbox started sending (which quietly rots reputation over 6-8 weeks, per Smartlead's and Instantly's own docs) — is gone: the new rule is warmup **never** turns off.

**4. Slack digest wired**
New channel connected and test-posted successfully. Daily digest will @-mention Anjali (Belardi Wong, Melior), Varsha (Bettrdata, Precise Leads), Balasankar (DARLEAN, Mythic) with their clients' action items.

---

## Why it's safe (what we did to make sure)

- **Everything ran in "report-only" mode first** — every job printed exactly what it *would* change, reviewed before any live write.
- **A timestamped snapshot of every inbox's original settings was taken before any change** (`rollback_snapshot_belardiwong_2026-07-09T14-56-58.json`). We can restore any value to exactly what it was.
- **Every write was verified** by reading the value back from Smartlead afterward — not trusting the script's own success claim.
- **The live run caught 2 real bugs** that testing-without-writing could never find (an API rejection on one parameter; a capacity number that another tool reads which would have over-allocated volume). Both fixed, verified, and shipped the same hour. This is exactly why we piloted on one client first.
- **Rotation — the only genuinely risky job (it moves leads between senders) — stays OFF.**
- Nothing increased cold-email volume. The only new sends are warmup emails, which land in a friendly network and *build* reputation.

---

## How deliverability improves over time (what to expect)

1. **Weeks 1-2:** warmup traffic resumes on all sending inboxes → sender reputation signals (opens/replies from the warmup network) start accruing where there were none.
2. **Continuous:** bounce auto-pause prevents the classic "bad list burns a domain overnight" incident.
3. **Weekly:** blacklist scan + capacity advisory catch fleet-level problems before they show up as spam-foldering.
4. **From ~week 2:** the reply-rate monitor has enough history to flag a domain whose replies drop >30% below its own baseline — the earliest available warning, ~48h before opens/bounces move.
5. **The workbook** (Inbox Health tab) remains the daily human view: P0 rows = act today, with the exact fix written in the row.

**What stays human:** fixing flagged inboxes (DNS escalations to Zapmail, blacklist delistings, retiring dead domains), responding to replies, and campaign judgment calls. The system detects and maintains; it doesn't make those decisions.

---

## Known open items (tracked, not blockers)

- **SURBL listings:** 23/23 Belardi Wong domains (107/120 fleet-wide) are on the SURBL blacklist. Impact is via links-in-emails, not the sender name itself; most campaigns already have link tracking off. Needs per-domain human triage — the system detects, humans decide.
- **Bench-inbox testing:** placement tests can only run through an active campaign, so idle spare inboxes can't be auto-retested yet. Fix: a standing test campaign (planned).
- **Capacity "ORDER NOW" label** currently also fires when it just means "replace N broken inboxes," not a true shortage — wording fix planned.
- **Other 3 clients** (DARLEAN, Mythic, Precise Leads) get the same treatment when the flags are enabled on the server — first run will be their "big wave," same as Belardi Wong's today.

---

## Rollback (if anything ever looks wrong)

1. Turn the job's flag off — it stops touching anything further.
2. Restore original values from the timestamped snapshot (exact per-inbox numbers on file).
3. Every change today also has a per-inbox timestamped change log and a git history explaining why.

---

## Appendix — every job's actual Belardi Wong run data (2026-07-09)

This is what each job did or found when run against the real account today. This is exactly what "live" looks like.

### 1. Daily sync
- 69 unique inboxes (deduped from 241 campaign-rows), 31 campaigns
- All 69 scored and written to the Inbox Health tab; 69 history records saved
- 23 domains DNS-audited (SPF/DKIM/DMARC) — audit now completes in seconds (was a 20+ minute stall before this week's fix)
- Health findings: **9 inboxes on 3 domains currently failing placement tests** (belardiwongs.com, bwdirectmail.com, heybelardiwong.com — likely tied to their blacklist listings, human action needed: pause + investigate links/DNS + delist); 6 more inboxes with stale tests (reachbw.com, sendbw.com)

### 2. Blacklist monitor
- **23 of 23 sending domains listed on SURBL (ABUSE)** — 100% of this client's fleet
- Zero on Spamhaus DBL or URIBL
- Detection only — delisting/replacement is a human decision, tracked separately

### 3. Capacity planner
- Demand: 20.4 sends/day (7-day average of actual campaign sends)
- Safe capacity: 330/day across healthy campaign-attached inboxes — **16× demand, plenty of room**
- Bench: 39 warm spare inboxes vs. a target of 5 — deep bench
- Advisory: "order 5 domains" — driven entirely by the 5 currently-broken inboxes needing replacement, NOT a capacity shortage (label wording fix planned)
- Also seeded the domain-age registry (130 domains fleet-wide on record)

### 4. Reply-rate monitor
- 5 domains had active-campaign sending stats; snapshots stored to Mongo
- 0 alerts — expected on early runs; its main "drop vs. own baseline" alert needs ~a week of daily history before it can fire

### 5. Auto-retest
- 2 stale-test inboxes selected (samlonsdale@reachbw.com, saml@sendbw.com)
- **0 tests created, 0 credits spent** — both inboxes are benched with no campaign attached, and a placement test physically sends through a campaign's sequence. The executor correctly refused rather than forcing it. Follow-up: standing test campaign for bench inboxes.

### 6. Warmup manager — 55 inboxes changed live, 0 turned off
Breakdown of the 55 (all "retune" — volume corrected to state profile):
- ~44 bench/idle inboxes: mixed 10 or 40/day → **20/day** (maintenance level)
- 6 campaign-attached inboxes: 40/day → **20/day + auto-adjust ON** (warmup yields to campaign volume in real time)
- 5 new/ramping inboxes (reachbw.com, sendbw.com): 10/day → **ramping to 40/day cap**
- Reply-rate setting standardized: 20% → **25%** everywhere
- Verified by reading values back from Smartlead after writing
- (First attempt hit a Smartlead API quirk — 50 writes rejected over one parameter; diagnosed, fixed, retried clean the same hour. The fix is now permanent in the code.)

### 7. Headroom fix — 15 inboxes, exact before → after

| Inbox | Before | After |
|---|---|---|
| sam / saml / samlonsdale @ swiftbybelardiwong.com | 10 | 30 |
| sam / saml / samlonsdale @ newbelardiwong.com | 10 | 30 |
| sam / saml / samlonsdale @ reachbelardiwong.com | 30 | 45 |
| sam / saml / samlonsdale @ realbelardiwong.com | 30 | 45 |
| sam / saml / samlonsdale @ justbelardiwong.com | 30 | 45 |

(The 30→ group was initially set to 50 by the original formula; caught in review the same hour and corrected to 45 — the cap is now permanent so no inbox's total can exceed 45/day.)

### 8. Bounce protection — 3 campaigns, before: none → after: auto-pause at 3%
- Hospitality - Multi-Property Chains (3375601)
- Hospitality - Multi-Property Chains Revised (3422152)
- Hospitality - Multi-Property Chains manager Level (3512952)

### 9. Rotation
- 0 swaps needed today (no broken sender currently attached to a live campaign meets the swap criteria)
- **Kept OFF by decision** — it moves leads between senders, the hardest action to reverse

### 10. Slack digest
- New channel wired and confirmed with a live test post
- Daily digest will list each client's action items, @-mentioning the responsible manager (Anjali / Varsha / Balasankar)

### Cross-check that ties it together
The 9 failing-placement inboxes (job 1) sit exactly on 3 of the SURBL-listed domains (job 2) — the system's signals corroborate each other and point to one root cause per domain, which is what makes the daily workbook actionable rather than noisy.

---

## Placement testing — what we ran, what we learned, and the account/credit reality

**Tests created so far: 0. Credits spent: 0.** Three live attempts, each informative:

1. **Belardi Wong, attempt 1:** both auto-selected targets were bench inboxes with no campaign attached — and a placement test physically sends through a campaign's sequence. The executor correctly refused. This surfaced the gap.
2. **Belardi Wong, attempt 2 (after building the fix):** the executor now falls back to the account's standing "Deliverability test Campaign" — it found the campaign, attached the bench inbox, and issued the test-creation call. Smartlead rejected it with **"Insufficient sequence credits"** — the Belardi Wong account has **zero SmartDelivery credits**. The whole code path is proven right up to the credit wall.
3. **PRECISE_LEADS (in progress at time of writing):** running the full end-to-end flow on the PL account, which has **92 confirmed credits** — up to 2 tests on its worst inboxes. When they finish (~30 min after creation), results are auto-written to Mongo and appended to the deliverability sheet, then flow into the workbook's Test Status column — which is exactly what the campaign-creation tool (precise-automator) reads when picking inboxes. Same loop as a human-run test, fully automated.

**Account structure fact (verified today, worth everyone knowing):** we run **4 separate Smartlead accounts** — Belardi Wong, DARLEAN, MYTHIC, and PRECISE_LEADS (which itself hosts the Melior / Bettrdata / OSC / Monarch / Capsule / VOC family, 345 inboxes). Checked directly: **no Belardi Wong / Darlean / Mythic inboxes exist inside the PRECISE_LEADS account.** SmartDelivery credits are per-account and don't transfer. Each account also already has its own "Deliverability test Campaign" — so the plumbing for per-account testing exists everywhere; only credits are missing in 3 of 4 accounts.

## ❓ Questions to resolve tomorrow (for the team / Smartlead support)

1. **How do we test Belardi Wong / Darlean / Mythic inboxes?** Their own accounts have no SmartDelivery credits. Options, in order of preference:
   - **(a) Buy credits in each account** — simplest, no side effects; each account's test campaign is already set up.
   - **(b) Ask Smartlead support whether credits can be pooled/shared across workspaces** under one billing relationship — if yes, cheapest.
   - **(c) Connect those clients' mailboxes into the PRECISE_LEADS account too** (Smartlead allows the same mailbox in multiple workspaces) — technically possible but NOT recommended: it duplicates mailbox billing, risks two accounts running conflicting warmup on the same inbox (dangerous now that warmup is automated), and splits test results away from where the client's data lives.
2. **Confirm with the team:** when they say they "test all clients from the Precise Leads Smartlead" — do they mean the PL family of sub-clients (true), or do they have a separate process for BW/Darlean/Mythic we haven't seen (their manual test results do exist in the per-client sheet tabs, so tests happened somewhere — worth asking exactly how)?
3. **Top-up decision:** how many credits per account per month? At the auto-tester's cap (2/client/day, only when stale) actual usage is modest — likely 20-40/month per account.
