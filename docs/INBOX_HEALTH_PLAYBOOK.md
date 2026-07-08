# Inbox Health Playbook

**The single reference for keeping all client inboxes healthy, landing in the inbox, and never going bad.**

Last updated: 2026-07-02

---

## Part 1 — The Goal (plain English)

Every inbox we send from has a **reputation**. Good reputation → emails land in the inbox. Bad reputation → emails land in spam and the inbox is useless. Reputation is *earned* over time (warmup + good behavior) and *lost* fast (bad lists, spammy copy, over-sending).

Software can't *force* an inbox to land in the inbox — but it CAN:
1. **Measure** each inbox's health daily
2. **Warn** before an inbox goes bad
3. **Tell** the right person exactly what to fix
4. **Auto-do** the safe maintenance (tests, warmup)

That's what this system does. This playbook is the map.

---

## Part 2 — What We've ALREADY Built (shipped + on `main`)

| # | Feature | What it does | Status |
|---|---|---|---|
| 1 | **Daily Sync** | Pulls all inboxes/campaigns from Smartlead + HeyReach → Google Sheets, runs 10 AM IST daily | ✅ LIVE |
| 2 | **Campaign Metrics dashboard** | DARLEAN campaign performance (leads, responses, connections) — Smartlead + HeyReach | ✅ LIVE |
| 3 | **All Inboxes master tab** | Every inbox across all clients, deduped, one row each (availability, capacity, warmup, test) | ✅ LIVE |
| 4 | **Inbox Health workbook** | Scores every inbox 0–100 daily, grade A–D, trend arrow, top problem, exact "what to do", owner (🤖/👤), manager | ✅ LIVE |
| 5 | **Health history (MongoDB)** | Saves each inbox's daily score → trend tracking + early warning | ✅ LIVE |
| 6 | **Deliverability test mapping** | Reads placement-test results (inbox/spam) per domain from the deliverability sheet, all clients | ✅ LIVE |
| 7 | **Slack digest** | Daily message grouped by client, @-mentions each manager, lists their action items | ⏳ CODE READY (needs live channel + manager handles) |
| 8 | **Auto placement-test executor** | Auto-runs SmartDelivery tests for stale/untested inboxes, worst-first, per-client capped | ✅ BUILT — DRY-RUN (spends nothing until enabled) |
| 9 | **Auto-warmup executor** | Keeps warmup ON unless an inbox is actively sending in a live campaign; flips any inbox whose state is wrong | ✅ BUILT — DRY-RUN (changes nothing until enabled) |

**The health score (how #4 works):** 4 signals, 0–100:
- Placement test result — 40 pts (landed inbox vs spam — strongest signal)
- Warmup reputation % — 25 pts
- Bounce rate — 20 pts
- Connection + reply — 15 pts
→ 90–100 A (green), 70–89 B, 50–69 C (yellow), 0–49 D (red)

---

## Part 3 — The Thresholds That Matter (the rules)

These are the industry-standard numbers the system uses / should use. **Reference card.**

### Health & performance
- **1% rule:** healthy inbox = reply rate ≥1% after 200+ sends. Below that after 200 sends → something's broken.
- **Bounce:** <1% excellent · 1–2% normal · 2–3% watch list quality · **>3% red** · **>5% STOP** (ISP suspension risk).
- **Spam placement (inbox %):** >90% great · 80–90% ok · 70–80% inspect · **<70% pause & fix**. Monthly target ≥85%.
- **Warmup reputation:** >80 good · 50–80 keep warming, don't send critical · **<50 do not send**.
- **Reply-rate drop:** >30% week-over-week → run full audit. This week <50% of last-4-week avg → real problem (else noise).

### Warmup ramp settings (UPDATED 2026-07-07 — see docs/DELIVERABILITY_MASTER_PLAN.md §1)
**Warmup is ALWAYS ON, in every state — only volume changes.** (Smartlead + Instantly
official: turning warmup off while sending erodes deliverability within 6–8 weeks.)
- **New inbox:** 40/day cap (Smartlead official max — not 50), ramp +2–5/day, 25% reply rate. 21–30 days before first campaign send.
- **Active (sending) inbox:** warmup stays ON at ≤20/day + auto-adjust ON (Smartlead trims 7–10/day automatically while campaigns run).
- **Insurance (idle) inbox:** 20/day, ramp 0, warmup ON.
- **Recovering (low rep / post-incident):** 15/day, ramp 0, **reply rate 30%** — boost the REPLY RATE, never the volume (replies are the reputation signal; volume spikes look like spam).
- Reply rate band: 25–30%, never above 30 (unnaturally high reply rate is itself a spam signal).
- Fleet-wide: "send warmup emails only on weekdays" ON (UI setting — not exposed via API, set manually).

### Infrastructure
- **2 inboxes per domain** (Gmail flags domains with >3–5 inboxes).
- **DNS auth required per domain:** SPF (`v=spf1 include:... ~all`, never `+all`), DKIM (`default._domainkey`), DMARC (`p=none` first 2 weeks → `quarantine` long-term → `reject` after 30d clean).
- **Physical mailing address in signature footer** (CAN-SPAM).
- Provisioning → sendable: **~2 weeks warmup**.

### Timing
- Campaign reply signal stabilizes at **21 days**.
- Re-test/re-audit only after **7 days** (reputation moves slowly).

---

## Part 4 — The Inbox Lifecycle (every inbox moves through this)

```
NEW (just provisioned)
  → enable warmup (40/day, ramp +2-5, reply 25%), set signature, tag "insurance"
  → WARMUP (21-30 days for new domains; 14 days only if domain is aged) — reputation builds
  → ACTIVE — promote to a live campaign; warmup STAYS ON at ≤20/day + auto-adjust
  → MONITOR — daily health score; watch 1% rule, bounce, reputation, warmup-blocked
  → if healthy → keep sending
  → if going bad → PULL from campaign (RECOVERING: warmup 15/day, reply 30%), alert manager, fix or RETIRE
  → RETIRE (dead/unrecoverable) → tag retired, disable warmup (the ONLY state with warmup off), swap in an insurance inbox
```

**Signals an inbox is going bad + the fix:**
| Signal | Fix |
|---|---|
| Reply <1% after 200 sends | Check placement, compare copy vs a passing sibling, rotate out if reputation bad |
| Bounce >3% (hard) | List problem — verify list, remove bad addresses |
| Bounce >3% (soft) | Reputation — cut volume 50% for 2 weeks |
| Bounce >5% | STOP the campaign immediately |
| Warmup blocked | Young inbox → retire; established → re-warm at 15/day |
| Failed placement test | Pause, check SPF/DKIM/DMARC + copy + list, retest |
| Gmail → Promotions | Remove links/images/marketing phrases, text-only |
| Domain blacklisted | <30d old → replace; >90d → pause 7d, submit delisting, slow resume |

---

## Part 5 — Recommended Cadence

| When | Task |
|---|---|
| **Daily (automated)** | Health score + workbook + history + Slack digest — the system does this |
| **Monday** | Review workbook P0/P1 rows; run deliverability audit (7-day) if reply <1% anywhere |
| **Wednesday** | Positive-reply sweep — respond to interested replies fast |
| **Friday** | Campaign retrospectives at 21-day mark (keep/iterate/kill) |
| **Every other Monday** | Inbox rotation — retire bad, promote insurance→active |
| **Monthly (1st)** | Placement tests across the fleet (target ≥85% inbox) |
| **Ad hoc** | Reply drop >30% WoW → audit. Bounce >2% → auth+spam check |

**Skip:** daily reply-rate obsessing (use 7-day averages), daily manual Smartlead checks.

---

## Part 6 — What's LEFT To Do (the plan)

### A. Finish + deploy what's built (small, do first)
1. **Slack notifications** — provide a live channel + manager name/handle per client (DARLEAN, Melior, Precise Leads, Bettrdata, Mythic, Belardi Wong, Avench, OSC, StaffAI). I wire `manager_map.py` + `HEALTH_NOTIFY_CHANNEL`.
2. **Schedule cron jobs on Render** — add cron entries (like the existing 10 AM sync) for:
   - `retest_executor.py` — daily, after the sync (ships in DRY-RUN — spends nothing)
   - (health workbook already runs inside the daily sync — no new cron)
3. **Redeploy** from `main` → everything runs automatically on the server.

### B. Auto-warmup automation (BUILT 2026-07-07 — state machine, dry-run)
**Rule (final, vendor-confirmed): warmup ALWAYS ON; only the volume profile changes.**
```
For each inbox (smartlead/warmup_planner.py):
  rep < 90                          → RECOVERING: 15/day, reply rate 30% (boost replies, not volume)
  in live (ACTIVE + fresh) campaign → ACTIVE: 20/day, auto-adjust ON
  still in ramp window ("warming")  → NEW: 40/day, ramp +5
  everything else                   → IDLE: 20/day
  blocked                           → skip (human-only)
  → apply profile when warmup is off OR volume is off-target (±10 tolerance on auto-adjusted inboxes)
```
- Ships **DRY-RUN** (`WARMUP_AUTO_ENABLED=false`) — cron at 11:30 AM IST logs enable/retune/boost plans.
- Also built: **bounce auto-protection sweep** (`bounce_protect_executor.py`, 12:30 PM IST, dry-run via `BOUNCE_PROTECT_ENABLED`) — sets Smartlead's `bounce_autopause_threshold=3%` on every ACTIVE campaign missing it; and **blacklist monitor** (`blacklist_monitor.py`, Mondays 9 AM IST, read-only) — Spamhaus DBL / SURBL / URIBL for every sending domain → Mongo `blacklist_checks`.

### C. Auto-placement-test — turn on when ready
- Currently DRY-RUN (`RETEST_ENABLED=false`) — logs targets daily, spends nothing.
- To go live: set `RETEST_ENABLED=true` + per-client caps. Start small (1 client, cap 1, one real test) → verify → scale.
- PRECISE_LEADS has 92 credits confirmed; other accounts' balances are dashboard-only but the code catches credit errors safely.

### D. Longer-term (optional)
- Auto-pull unhealthy inboxes from campaigns (risky — recommend-only for now).
- DNS auth checker (SPF/DKIM/DMARC) built into the sync → flag misconfigured domains automatically.
- New-inbox provisioning pipeline (Zapmail → auto-configure → warmup → ready).

---

## Part 7 — How It's Deployed

- **Where:** Render (`infra-bot-1.onrender.com`), builds from GitHub `main`.
- **How the sync runs:** `index.js` (Node) has `node-cron` → spawns `python run.py` at 10 AM IST daily.
- **To add a job (e.g. auto-test, auto-warmup):** add a `cron.schedule(...)` entry in `index.js` that spawns the new Python script — same pattern as the sync. On deploy, it runs automatically.
- **Redeploy:** push to `main` → trigger Render deploy → new build runs the crons.

---

## Part 8 — Campaign Manager's Weekly Action Calendar

**The exact routine each campaign manager follows to keep their client's inboxes landing in the inbox.** The system does the tracking; the manager does the judgment fixes. Open the **Inbox Health workbook**, filter to your client, and work top-down (P0 first).

### Every DAY (5 min)
1. Open the workbook → your client → sort by Priority.
2. Any **P0 (red / D-grade)?** Do them TODAY — these are actively landing in spam:
   - *Failed placement test* → pause the inbox in Smartlead, check the domain's SPF/DKIM/DMARC, simplify the copy if spammy, then let it retest.
3. **🤖 Auto rows** — ignore, the system handles them.

### MONDAY (30 min) — Health sweep
1. Scan all your inboxes' grades. Note anything that dropped a grade since last week (Trend column ↓).
2. Any inbox with **reply rate <1% after 200+ sends** → it's underperforming: compare its copy to a healthy sibling inbox, or rotate it out.
3. Any **bounce >3%** → list problem: clean the list (remove bad addresses). **>5% → pause that campaign now.**
4. Any **P1** rows (disconnected, low-rep, stale/untested) → work them:
   - *Disconnected* → reconnect the inbox (re-login).
   - *Low warmup rep (<90%)* → keep it OUT of campaigns, let warmup recover.
   - *Stale/untested* → the auto-tester handles it (or run a manual placement test).

### WEDNESDAY (15 min) — Reply handling
1. Check for positive/interested replies → respond fast (within the hour for hot ones).
2. Categorize replies correctly in Smartlead (the auto-tagging isn't perfect — your manual tags feed the metrics).

### FRIDAY (20 min) — Campaign review
1. For campaigns hitting the **21-day mark**, review reply rate:
   - **≥2× baseline** → scale it up.
   - **near baseline** → iterate the copy.
   - **<50% of baseline** → kill it.
2. Note any inbox that's been sending heavily — flag for a placement retest.

### EVERY OTHER MONDAY (30 min) — Inbox rotation
1. **Retire** inboxes that are bad/blocked/failing the 1% rule (tag retired, warmup off).
2. **Promote** warmed insurance inboxes → active to backfill.
3. If you have **<5 spare warm inboxes**, start a new domain purchase (2-week lead time).

### MONTHLY (1st of month, 45 min) — Fleet placement test
1. Run placement tests across the client's inboxes (target **≥85% inbox**).
2. Anything **<70%** → pause + incident response (auth + copy).
3. Update the deliverability sheet with results.

### Stale campaigns — the silent inbox killer
A campaign can show **ACTIVE** in Smartlead but be effectively **dead**: no new
lead added AND no emails sent in **14+ days**. This is dangerous because:
- The inboxes on it aren't sending (not working) *and* were going cold (not warming).
- It hides — the dashboard says ACTIVE, so nobody looks.
- It traps good inboxes on a zombie campaign.

**What the system does automatically:** detects stale campaigns, turns warmup back
ON for their inboxes (rescue), and flags them.

**What YOU (the manager) must do when a campaign is flagged stale — pick one:**
| Situation | Action |
|---|---|
| Still valuable, just out of leads | **Feed it new leads** (build + upload a list) → campaign goes fresh |
| Done / not worth continuing | **Pause or complete it** → frees the inboxes |
| Inboxes needed elsewhere | **Reassign** them to a live campaign |

Rule of thumb: a stale campaign shouldn't sit stale more than a few days. Revive it
(leads) or retire it (pause). Never leave good inboxes trapped on a zombie campaign.

### The manager's mental model
- **Green (A/B)** = healthy, use freely.
- **Yellow (C)** = degrading, watch + fix this week.
- **Red (D)** = broken, pull from campaigns NOW.
- **Trend ↓** = catch it before it turns red.
- **Warmup ON** unless the inbox is actively sending — the system now manages this automatically (once enabled).

### What the system does FOR the manager (so they don't have to)
- Scores + grades every inbox daily
- Warns before decline (trend)
- Runs placement tests (when enabled)
- Manages warmup on/off (when enabled)
- Slacks them their action list each morning

The manager only does what needs a human: **DNS fixes, copy rewrites, reconnects, retire/scale decisions, reply handling.**

---

## Part 9 — Current Status Summary

**✅ Working now (daily, automatic):** inbox scoring, workbook, history, trend, test-mapping, campaign metrics.
**⏳ One step from live:** Slack digest (needs channel + managers), cron for auto-test (dry-run).
**🔨 Designed, not built:** auto-warmup (rule being refined).
**⏸ Off by choice:** real auto-testing (dry-run until you enable + fund).

**Bottom line:** the *tracking + telling-you-what-to-do* half is DONE and running. The *auto-fixing* half (warmup, tests) is built or designed and ships safely in dry-run first. The human still owns the judgment fixes (DNS, copy, retire) — with the workbook telling them exactly what + when.
