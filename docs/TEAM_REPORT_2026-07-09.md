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
