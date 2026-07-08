# Operator Playbook — Who Does What, How It's Checked, How To Turn It All On

**The single runbook: what the SYSTEM does automatically, what the MANAGER does by hand, how each piece is verified, and the exact enablement sequence.**

Created: 2026-07-07. Companions: [INBOX_HEALTH_PLAYBOOK.md](INBOX_HEALTH_PLAYBOOK.md) (rules + thresholds) · [DELIVERABILITY_MASTER_PLAN.md](DELIVERABILITY_MASTER_PLAN.md) (why + sources).

---

## 1 — The System (background automation)

Six cron jobs on Render, all IST. Each row: what it does, how we check it works, current state.

| # | Cron (IST) | Job | What it does | How we check it | State |
|---|---|---|---|---|---|
| 1 | 10:00 daily | `run.py` sync | Pulls all inboxes/campaigns → Sheets (All Inboxes, Inbox Health, Campaign Metrics), DNS audit, health scores → Mongo history | Sheets tabs have today's date; Render logs `[smartlead] sync finished with code 0` | ✅ LIVE |
| 2 | 11:00 daily | `retest_executor.py` | Auto placement-tests for stale/worst inboxes, per-client cap 2 | Log line `[retest]`; results land in `placement_tests`/`placement_results` Mongo + API test tab | ⏸ DRY-RUN (`RETEST_ENABLED=false`) |
| 3 | 11:30 daily | `warmup_executor.py` | State-machine warmup: RECOVERING 15/reply-30 · ACTIVE 20+auto-adjust · NEW 40+ramp · IDLE 20 · LONG_IDLE 20+reply-28 (30d+ no send, retest flagged); warmup ALWAYS ON | Log `[Warmup] N change(s): X enable, Y retune, Z boost`; spot-check 2-3 inboxes in Smartlead UI warmup settings | ⏸ DRY-RUN (`WARMUP_AUTO_ENABLED=false`) |
| 3b | 12:15 daily | `headroom_fix_executor.py` | Raises `max_email_per_day` on ACTIVE inboxes to (current + 20) — Smartlead confirms this field is ONE shared bucket for warmup + campaign sends, and every real inbox caps at ≤30, leaving zero room for the ACTIVE profile's 20/day warmup. Fixes the squeeze | Log `[Headroom] N inbox(es) need raised`; verify one inbox's daily limit in Smartlead UI → Settings | ⏸ DRY-RUN (`HEADROOM_FIX_ENABLED=false`) — NEW, deploy pending |
| 4 | 12:00 daily | `rotation_executor.py` | Swaps P0-broken senders for healthy bench inboxes, cap 2/client | Log `[rotation]`; `rotation_log` Mongo collection | ⏸ DRY-RUN (`ROTATION_ENABLED=false`) |
| 5 | 12:30 daily | `bounce_protect_executor.py` | Sets Smartlead bounce auto-pause (3%) on every ACTIVE campaign missing it | Log `[bounce-protect] N campaigns missing`; after enable, N should drop to 0 and stay 0; verify one campaign in Smartlead UI → Setup | ⏸ DRY-RUN (`BOUNCE_PROTECT_ENABLED=false`) — NEW, deploy pending |
| 6 | Mon 09:00 | `blacklist_monitor.py` | Every sending domain vs Spamhaus DBL / SURBL / URIBL; hits → console + Mongo `blacklist_checks` | Log `[blacklist] ✅ all domains clean` or 🚨 list; read-only, no enable flag | ✅ READY — NEW, deploy pending |
| 7 | daily (inside #1) | Slack digest | Per-client action list @-mentioning managers | Message appears in channel | ⏳ NEEDS channel + manager handles |

**Deploy note:** jobs 3b, 5, 6 + the new warmup logic exist on this machine only until pushed to `main` → Render redeploy.

**Enablement order note:** enable the headroom fix (3b) BEFORE warmup auto (3) — raising the daily cap first means warmup actually has room to use once its profile is applied. Doing it the other way round means warmup targets get set but silently can't send.

---

## 2 — The Manager (human judgment work)

The system detects and tells; the manager fixes what needs judgment. Workbook column "Owner": 🤖 = ignore (system handles), 👤 = yours.

### Daily (5 min)
1. Open Inbox Health workbook → your client → sort by Priority.
2. **Every P0 row, act today:**

| P0 problem in workbook | Your fix | How you verify the fix |
|---|---|---|
| Failed placement test | Pause the inbox in Smartlead. Check copy vs a passing sibling (links? images? spam words?). Fix copy. DNS flags on the row → already escalated (see below). Wait 7 days → auto-retester re-tests | Next test shows "inbox"; workbook row goes green |
| Landing in spam during warmup | Pause inbox; audit copy + auth; check complaint rate in Google Postmaster | `warmup_spam_count` stops rising; rep recovers ≥90 |
| SPF/DKIM/DMARC misconfigured | **Ticket to Zapmail support with the domain list. NEVER edit DNS yourself** (it's what we pay them for). Outlook DKIM-missing = P1 "may be expected" — ask Zapmail to confirm | Next daily sync re-audits DNS; flag clears |
| Warmup blocked | Young inbox (<30d) → retire + provision replacement. Established → wait 48h, re-enable at 15/day (RECOVERING profile does this automatically once warmup auto is on) | `warmup_state` leaves "blocked" |
| Blacklisted domain (Monday blacklist report) | Pause campaigns on that domain. Spamhaus/SURBL → submit delisting request at their portal. Domain <30d old → replace instead (cleanup not worth it) | Next Monday's run shows domain clean |

### Monday (30 min) — health sweep
1. Grades that dropped since last week (Trend ↓) — investigate before they hit red.
2. Reply <1% after 200+ sends → compare copy vs healthy sibling, or rotate out.
3. Bounce >3% → clean the list (re-verify, remove bad). **>5% → pause campaign now** (once bounce auto-pause is enabled Smartlead does this at 3% by itself).
4. Read the Monday blacklist report (Render logs / Slack once wired).
5. P1 rows: disconnected → re-login; low rep → keep out of campaigns (auto-warmup runs RECOVERING); stale test → auto-tester handles.

### Wednesday (15 min) — replies
1. Respond to positive/interested replies within the hour.
2. Fix miscategorized replies in Smartlead (your manual tags feed the metrics).

### Friday (20 min) — campaigns
1. 21-day-old campaigns: ≥2× baseline → scale · near baseline → iterate copy · <50% → kill.
2. Stale-flagged campaigns (ACTIVE but dead 14d+): feed new leads, or pause it, or reassign its inboxes. Never leave inboxes trapped — system already flips their warmup back on, but the campaign decision is yours.

### Every other Monday (30 min) — rotation
1. Retire bad inboxes (tag retired — the ONLY state where warmup goes off).
2. Promote warmed insurance inboxes (only if warmup rep ≥90% AND 21-30 days old for new domains).
3. <5 spare warm inboxes → order new domains now (2-week min lead time, 30 days for campaign-ready).

### Monthly (1st, 45 min)
1. Fleet placement tests, target ≥85% inbox. <70% → incident response.
2. Check Google Postmaster Tools (spam complaint rate <0.1%, target) for Gmail-heavy clients.

### One-time manual settings (API can't set these — do once in Smartlead UI)
- [ ] **"Send warmup emails only on weekdays" ON** for every inbox (Email accounts → warmup settings). Not exposed via API.
- [ ] Campaign-level: disable open tracking on cold campaigns; ESP matching ON (Setup → `enable_ai_esp_matching` — sweepable later if wanted).

---

## 3 — Enablement sequence (dry-run → live, one system at a time)

Never flip two at once — you can't attribute problems. Between steps: 2-3 days of clean logs.

```
WEEK 1
  Day 1  Deploy to main → Render. All new jobs run DRY-RUN — zero risk.
  Day 1  Review dry-run logs: [Warmup] plan counts, [bounce-protect] campaign list.
         Sanity: no inbox planned ABOVE 40/day; live senders planned at 20 not 40.
  Day 2  Re-check logs. Warmup plan should be near-identical day-over-day
         (idempotent). If it re-plans the same inboxes daily → volume isn't
         sticking → investigate before enabling.
  Day 3  ENABLE bounce protect:  BOUNCE_PROTECT_ENABLED=true  (cap 10/run,
         ~3 days to cover all 24 campaigns). Verify in Smartlead UI: one
         campaign → Setup → bounce threshold = 3.
WEEK 2
  Day 1  ENABLE headroom fix FIRST:  HEADROOM_FIX_ENABLED=true. Raises
         max_email_per_day on ACTIVE inboxes so warmup has actual room
         (every real inbox caps at <=30 today — 0 headroom for warmup).
         Verify in Smartlead UI: one ACTIVE inbox's daily limit went up.
  Day 2  ENABLE warmup auto:  WARMUP_AUTO_ENABLED=true. (Must come AFTER
         headroom fix, or warmup targets get set with nowhere to send.)
         Day 1 applies the big first wave (every off-profile inbox).
  Day 2  Log should show near-zero changes (steady state). Spot-check 3 inboxes
         in UI: volume matches profile, reply rate 25/30.
  Day 4  Check warmup reps didn't drop after the retune wave.
WEEK 3
  Day 1  ENABLE auto-retest:  RETEST_ENABLED=true, RETEST_PER_CLIENT_DAILY_CAP=1,
         one client first (PRECISE_LEADS — 92 credits confirmed). Verify one
         real test completes + result lands in the workbook. Then raise caps.
LATER   ROTATION_ENABLED — only after warmup + retest have run clean for 2+
         weeks (it mutates campaigns; test on a dummy campaign first).
```

**Slack digest go-live (independent, any time):** give Avinash's channel ID + manager handle per client (DARLEAN, Melior, Precise Leads, Bettrdata, Mythic, Belardi Wong) → set `HEALTH_NOTIFY_CHANNEL` + fill `manager_map`.

---

## 3b — Full-repo audit (2026-07-08): bugs found + fixed before enablement

Three independent review passes over the whole pipeline, every finding verified
against the code (and live DNS) before fixing:

| Bug | Impact | Fix |
|---|---|---|
| DKIM checked only at `default` selector | **Every Gmail/Outlook "DKIM misconfigured" P0 in the workbook was a FALSE ALARM** — verified live: all flagged domains have valid DKIM at `google`/`selector1`. **The planned Zapmail DKIM ticket is CANCELLED — nothing was broken** | Checker now tries google/selector1/selector2/default; passes on any hit |
| DNS lookup errors reported as "missing" or as green | DoH timeout could fabricate a P0, or hide a real failure as verified-OK | Errors now report "status unknown", never cached, never flagged |
| Inbox on 2+ campaigns: dedup kept a random row | `campaign_status` in the workbook (and warmup state downstream) could show a completed campaign instead of the ACTIVE one | Dedup + warmup planner now always prefer the live-ACTIVE campaign row |
| Retest executor could strand warmup OFF (3 paths: abandoned tests, failed test-creation, failed restore) | The #1 "silently lose an inbox" mechanism — inboxes left warmup-off forever with only a log line | Restore-before-done ordering; abandoned tests restore warmup first; failed creations restore immediately; failed restores retry next run |
| 7-day retest floor implicit only | Config change could silently burn credits on too-fresh re-tests | Explicit floor in target selection |

Also audited clean: rotation executor (no dry-run mutation, no capacity-loss path), all 9 crons (schedules correct, IST), campaign staleness logic, Mongo stores, account discovery, client exclusion.

## 4 — E2E test status (2026-07-07)

| Component | Test | Result |
|---|---|---|
| Warmup planner logic | 8-case unit test (enable/retune/boost/tolerance/blocked/stale) | ✅ PASS |
| Warmup executor vs live API | Dry-run all 4 accounts | ✅ PASS — plans **325 changes: 16 enable, 274 retune, 35 boost, 0 disable** (always-on respected; IDLE 10/40→20, ACTIVE 40→20, NEW→40 all correct). First-wave size is expected; day-2 run should plan ~0. Run takes ~1h (fetches every campaign's leads — optimization candidate) |
| Bounce protect vs live API | Dry-run all 4 accounts | ✅ PASS — found 24 ACTIVE campaigns, 0 protected |
| Headroom fix vs live API | Dry-run all 4 accounts | ✅ PASS — **116 ACTIVE inboxes** have `max_email_per_day` raised (+20). Confirms the theory: every affected inbox was capped at ≤30 (10→30, 25→45, 30→50 etc.) — 0 room for warmup once campaign sends used the daily budget |
| LONG_IDLE sub-state logic | 3-case unit test (fresh idle / 45d idle / unknown date) | ✅ PASS — volume stays 20 (no evidence-free change), reply rate nudges to 28%, retest flagged; unknown-date defaults safely to plain IDLE |
| LONG_IDLE vs live fleet | Full dry-run, all 4 clients (375 total changes) | 0 hits today (verified twice, two code paths, same 375 total). Real result, not a bug — this fleet likely has nothing sitting idle 30+ days yet (fast rotation + today's big warmup correction). Will start firing once inboxes accumulate idle time; re-check in a few weeks |
| Blacklist single domain | reachbw.com | ✅ WORKS — LISTED on SURBL (ABUSE, code 127.0.0.64) |
| Blacklist false-positive check | fake domain + google.com + clean domain | ✅ all clean (sentinel IPs filtered, answer codes decoded — not a resolver artifact) |
| Blacklist full fleet | all 118 client domains | 🚨 **105 of 118 domains LISTED on SURBL ABUSE** (all 4 clients). THE systemic root cause of failing placement tests + short inbox lifespans |
| index.js crons | `node --check` | ✅ PASS |
| Python compile | all edited/new files | ✅ PASS |
| Daily sync (`run.py`) | runs daily in prod already | ✅ LIVE (unchanged core; health message text updated) |
| Retest executor | unchanged this round; already dry-running in prod | ✅ (existing) |
| Rotation executor | unchanged this round; dry-run in prod | ✅ (existing) |

### 🚨 P0 INCIDENT — fleet-wide SURBL listing (found 2026-07-07)
105/118 sending domains are on SURBL ABUSE (full list: run `python3 blacklist_monitor.py`).
SURBL is a URI blacklist used by SpamAssassin and most spam filters — any email FROM
or LINKING TO these domains gets score penalties → spam folder. This explains the
failing placement tests and why inboxes "keep going bad."

**IMPORTANT NUANCE (researched 2026-07-07, full detail in SCALE_ROADMAP.md §2):**
SURBL is a URI blacklist — it penalizes domains appearing in message LINKS/BODIES,
not FROM addresses. A listing hurts when the domain shows up in links (signature
URLs, unsubscribe links, tracking domains) — filters dock ~10-15 points, and
Gmail/Outlook often silently DISABLE the links rather than spam-folder the mail.

What to do (in order):
1. **Audit bodies first:** do campaign emails link to their own sending domain
   (signature, unsubscribe, tracking CNAME)? Those campaigns are the ones actually
   hurt. Move links to a clean dedicated tracking/landing domain, or drop links
   from email 1 entirely (already our copy rule).
2. **Tracking domains:** every client needs a custom tracking domain on a CLEAN
   (non-listed) domain — never the Smartlead shared default. One per client.
3. **Don't mass-delist.** Evidence-based, per-domain, AFTER remediation — bulk
   blind requests create repeat-offender status. Prioritize domains whose URLs
   actually appear in mail bodies. https://www.surbl.org/surbl-analysis
4. **Root cause:** mass listing points to list hygiene (spamtraps in unverified
   lists) + shared infrastructure. Enforce ≥95% verified lists; raise with Zapmail.
5. The Monday blacklist cron tracks the listed-count trend.

### Not done yet (the honest list)
1. **Deploy** — new code is local only. Push to `main` → Render.
2. **Warmup dry-run review** — run takes 1h+ (fetches every lead of every campaign for
   staleness checks — optimization candidate); eyeball the plan before Week-2 enable.
4. **Slack digest** — blocked on channel + manager handles (Avinash).
5. **Zapmail ticket** — Outlook DKIM question + flagged domain list (Manveen).
6. **Smartlead ticket** — substitution subject-line question (blocks rotation enable).
7. **Weekday-only warmup toggle** — manual UI sweep, not API-exposed.
8. **SpamAssassin factors → health score** — Smart Delivery report parsing, next build round.
9. **Open-tracking-off / ESP-matching sweep** — optional future executor (fields confirmed on same settings endpoint).
```
