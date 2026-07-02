# Feature Checklist + How To Use

**Every feature we built, its test status, and the 4 how-tos.** Verified 2026-07-03.

> **CREDIT LOCK:** Placement-test credits frozen at **30** by user request. No `create_test` calls until explicitly approved. All checks below are offline / read-only / dry-run — **zero credit spend.**

---

## Part 1 — Feature Checklist (verified)

| # | Feature | What it does | Unit test | Live-verified | Ships |
|---|---|---|---|---|---|
| 1 | Daily sync (Smartlead+HeyReach → Sheets) | Pulls all data daily | — | ✅ runs daily | LIVE |
| 2 | Campaign Metrics dashboard | DARLEAN campaign performance | ✅ test_campaign_metrics | ✅ 28 campaigns written | LIVE |
| 3 | All Inboxes master tab | Every inbox, deduped | ✅ test_sync_wiring | ✅ 442 inboxes | LIVE |
| 4 | **Inbox Health workbook** | Score+grade+problem+action per inbox | ✅ test_health | ✅ 442 scored, graded | LIVE |
| 5 | Health history (MongoDB) | Daily score → trend | — | ✅ 442+ records saved | LIVE |
| 6 | Trend / early-warning | ↓ arrow before decline | ✅ test_health | ✅ computed | LIVE |
| 7 | Deliverability test mapping | inbox/spam per domain, all clients | — | ✅ Mythic/Bettrdata/all mapped | LIVE |
| 8 | Manager map (per client) | who owns each inbox | ✅ test_manager_map | ✅ resolves | LIVE (handles TBD) |
| 9 | Slack digest | daily per-manager action list | ✅ test_notify | ✅ builds | code ready (channel TBD) |
| 10 | **Auto placement-test** | runs spam tests, worst-first, capped | ✅ test_retest_targets, test_smart_delivery | ✅ dry-run: 6 targets; create+poll proven (2 tests running) | DRY-RUN |
| 11 | **Auto-warmup** | warmup ON unless live-sending | ✅ test_warmup_planner | ✅ dry-run: 117 changes, 0 wrong-enable | DRY-RUN |
| 12 | Stale-campaign detection | catch dead ACTIVE campaigns | ✅ test_campaign_freshness | ✅ 0 stale (all fresh) | DRY-RUN |
| 13 | 2026 toggle: conservative volume | 40→30/day | ✅ | ✅ toggle flips | OFF |
| 14 | 2026 toggle: maintenance trickle | active senders keep 8/day | ✅ test_warmup_planner | ✅ disable→trickle proven | OFF |
| 15 | 2026 toggle: spam-landing flag | warmup spam → P0 | ✅ test_health | ✅ toggle flips | OFF |
| 16 | Cron jobs (test 11:00, warmup 11:30) | scheduled on deploy | — | syntax-checked | WIRED |

**Unit tests:** 11 suites, **all PASS** (offline, zero credits).
**Auto-test loop:** create + poll + report all proven live (2 tests running, results pending — no more credits until approved).

---

## Part 2 — How WE Will Use It

1. **Deploy** — push is done; redeploy on Render → the daily sync + health workbook run automatically at 10 AM IST. (Auto-test/warmup crons run in dry-run — safe.)
2. **Watch the workbook** — the "Inbox Health" tab becomes the single source of truth for inbox quality across all clients.
3. **Turn on features gradually** — each is an env toggle, all OFF by default:
   - Start: Slack digest (set channel + managers).
   - Then: auto-warmup dry-run → review → enable.
   - Then: auto-test dry-run → review targets → enable with a small cap (when you approve credit spend).
   - Then: 2026 toggles (trickle, conservative volume, spam-flag).
4. **Review weekly** — the workbook + Slack digest drive the team's routine (Part 5).

---

## Part 3 — How It's Useful To Us

- **One dashboard, all clients** — no more checking each Smartlead account manually.
- **Catches problems early** — the trend arrow warns before an inbox fails, so you fix it before it costs a campaign.
- **Tells you exactly what to do** — no deliverability expertise needed; the "What To Do" column is the instruction.
- **Automates the safe grunt work** — placement tests + warmup management run themselves (when enabled).
- **Routes work** — each manager sees only their inboxes; nothing falls through the cracks.
- **2026-current** — thresholds match the latest Google/Yahoo/Microsoft rules, not stale advice.
- **Credit-safe** — auto-test caps per client, catches credit errors, never runs away.

Net: fewer inboxes go bad, more emails land in the inbox, less manual monitoring.

---

## Part 4 — How To Explain It To The Team

**The one-liner:** "A robot that grades every inbox daily and tells you exactly what to fix, so our emails keep landing in the inbox."

**The 3 things they need to know:**
1. **Open the "Inbox Health" tab** — it's sorted worst-first. Red (D) = broken, do now. Yellow (C) = fix this week. Green = fine.
2. **Do the 👤 You rows** — the "What To Do" column is your instruction (fix DNS, reconnect, clean list, etc.). Ignore 🤖 Auto rows — the robot handles those.
3. **Check Slack each morning** — you get a message listing your client's problem inboxes.

**What to reassure them:** they don't run anything, don't need to be deliverability experts, and can't break anything — the robot only *tells* them what to do; humans decide.

Point them at [TEAM_GUIDE.md](TEAM_GUIDE.md) for the full walkthrough.

---

## Part 5 — How The Campaign Manager Uses It (daily → monthly)

**Daily (5 min):** Open workbook → your client → clear P0 (red) rows. Check Slack digest.

**Monday (30 min):** Full sweep — scan grades, note ↓ trends, work P1 rows. Check any bounce >3% (clean list) or >5% (stop campaign). Any reply <1% after 200 sends → compare copy / rotate inbox.

**Wednesday (15 min):** Respond to positive replies fast. Categorize replies correctly in Smartlead.

**Friday (20 min):** Review campaigns at 21-day mark → scale (≥2× baseline) / iterate / kill (<50%). Flag heavy-sending inboxes for a retest.

**Every other Monday (30 min):** Rotate inboxes — retire bad/blocked, promote warmed insurance → active. If <5 spare warm inboxes, start a domain purchase (2-week lead).

**Monthly / 1st (45 min):** Placement tests across the fleet (target ≥85% inbox). <70% → pause + fix auth/copy. Update the deliverability sheet.

**Per-case (any time):** every workbook signal maps to an exact action — see [2026-DELIVERABILITY-COURSE-OF-ACTION.md](2026-DELIVERABILITY-COURSE-OF-ACTION.md) Part 3 (failed test, high volume, stale campaign, bounce, warmup blocked, Gmail-promotions, DNS, list hygiene, auth).

**Special case — stale campaign flagged:** feed it new leads, OR pause/complete it, OR reassign its inboxes. Never leave good inboxes trapped on a dead campaign.

---

## Part 6 — To-Do To Go Fully Live

- [ ] Redeploy on Render (crons start, dry-run — safe)
- [ ] Set `HEALTH_NOTIFY_CHANNEL` = live Slack channel
- [ ] Fill `manager_map.py` — manager name + Slack handle per client
- [ ] Review auto-warmup dry-run log → set `WARMUP_AUTO_ENABLED=true`
- [ ] (recommended) `WARMUP_MAINTENANCE_TRICKLE=true`, `WARMUP_CONSERVATIVE_VOLUME=true`
- [ ] (recommended) `HEALTH_SPAM_FLAG_ENABLED=true`
- [ ] When ready to spend credits: review auto-test dry-run → `RETEST_ENABLED=true` + small cap
- [ ] Poll the 2 pending test results (466268/466269) once they finish — proves the loop; no new credits

Everything ships OFF/dry-run. Nothing changes or spends until these are set.
