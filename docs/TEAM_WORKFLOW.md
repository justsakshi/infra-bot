# Deliverability Pipeline — Team Workflow

**Share this with the team. This is the one doc to follow.**
Goal: every inbox tested, maintained, and landing in the inbox — always.

---

## The pipeline (what runs every day, automatically)

```
10:00 AM IST  DAILY SYNC
              pulls every inbox + campaign (all clients, old clients excluded)
              → scores each inbox 0–100 → writes the "Inbox Health" workbook
              → saves history (trend arrows) → merges latest test results

11:00 AM IST  AUTO PLACEMENT-TEST
              picks the worst inboxes (untested / stale test) per client
              → turns warmup off on those senders → runs a Smartlead spam test
              → reads yesterday's finished tests → writes inbox/spam % back
              → turns warmup back ON when the test completes

11:30 AM IST  AUTO-WARMUP
              warmup ON permanently, on every inbox (Avi's policy)
              → enables any inbox with warmup off
              → BOOSTS warmup on low-reputation inboxes (<90%) — never reduces it
              → flags "zombie" campaigns (ACTIVE but no leads/sends in 14 days)

12:00 PM IST  AUTO-ROTATION
              a broken (spam-landing) inbox in a live campaign gets SWAPPED OUT
              → picks a healthy same-client inbox, same sender name if possible
              → adds it to the campaign FIRST, then removes the broken one
              → new leads: zero change · in-flight leads: RESUMED on the
                same-name inbox in a fresh thread (or left paused if no name
                match) — same-thread handover is technically impossible
                (confirmed by Smartlead support)
              → no healthy spare available? → alert: "provision domains"
              → the broken inbox stays connected (replies still arrive), gets
                fixed + retested, and returns to the bench when green
```

**Where you see everything: the Google Sheet → "Inbox Health" tab.**
One row per inbox: Health Score (0–100), Grade (A–D color), Trend (↑↓), Top Problem, **What To Do**, Owner (🤖 robot / 👤 you), Manager, Test Status.

---

## What is ALREADY AUTOMATED (don't do these by hand)

| The robot does | Where you see the result |
|---|---|
| Scores + grades every inbox daily | Inbox Health tab — Score/Grade columns |
| Detects decline before failure | Trend column (↓ = getting worse) |
| Runs placement (spam) tests on stale/untested inboxes | Test Status + Test Date columns update; grade recomputes |
| Turns warmup off for the test, back on after | automatic — nothing to do |
| Keeps warmup ON on every inbox, permanently | Warmup state in Smartlead; "enable/boost" lines in run logs |
| Boosts warmup on low-rep inboxes | Warmup Rep % column recovers over days |
| Flags campaigns that look ACTIVE but are dead (14d no leads/sends) | flagged in run logs + inboxes rescued to warmup |
| Flags inboxes sending above the safe volume (>50/day) | P2 row: "Sending above 2026 safe volume" |
| Flags spam landings during warmup | P0 row: "Landing in spam during warmup" (when enabled) |
| Swaps broken senders out of live campaigns for healthy ones | rotation log lines + Slack digest; campaign keeps running |
| Alerts when a client has no healthy spare inboxes | "NO HEALTHY BENCH — provision domains" in logs/digest |
| Ignores old clients (Avench, Monarch, Capsule, Gofloaters) | they don't appear anywhere |

**Rule of thumb: if the row says 🤖 Auto — skip it, the robot has it.**

---

## What YOU do (the human workflow)

### Every day — 5 minutes
1. Open the **Inbox Health** tab. It's sorted worst-first.
2. Filter to **your clients** (Manager column):
   - **Balasankar** — DARLEAN, Mythic, OSC, StaffAI
   - **Anjali** — Melior, Belardi Wong
   - **Varsha** — Better Data (Bettrdata), Precise Leads
3. Work every **P0 (red)** row that says **👤 You** — the "What To Do" column is your exact instruction. Do these TODAY.

### Every Monday — 30 minutes
- Scan your grades. Anything that **dropped a grade** or shows **↓ trend** → treat as this week's fix.
- Work the **P1** rows (disconnected, low-rep, etc.).
- Bounce check: any campaign **>3% bounce** → clean the list. **>5% → pause the campaign now.**
- Reply check: any inbox **<1% reply after 200+ sends** → copy problem or burned inbox — compare with a healthy sibling, rotate out if needed.

### Every Friday — 20 minutes
- Campaigns at the **21-day mark**: reply ≥2× baseline → scale · near baseline → iterate copy · <50% of baseline → kill.
- **Zombie campaigns** (flagged stale): pick one — feed it new leads / pause it / reassign its inboxes. Never leave inboxes trapped on a dead campaign.

### Every other Monday — 30 minutes
- Retire dead inboxes (failed + unrecoverable), promote warmed spares into campaigns.
- Fewer than **5 spare warm inboxes** for your client? Start a domain purchase now (2-week lead time).

---

## Per-case: exact action for every flag

| Workbook says | You do |
|---|---|
| **Failed placement test** (P0) | Pause the inbox in Smartlead → check SPF/DKIM/DMARC on the domain → simplify copy (cut links/images) → verify the list → let the robot retest. |
| **Landing in spam during warmup** (P0) | Same as above + check complaint rate in Google Postmaster Tools. |
| **Warmup blocked** (P0) | Inbox <30 days old → retire it. Established → investigate reason; robot re-warms at low volume. |
| **SMTP/IMAP disconnected** (P1) | Reconnect the inbox (usually re-login/OAuth). Minutes. |
| **Low warmup reputation** (P1) | Keep it out of campaigns. Robot boosts warmup automatically — just wait for rep >90%, then it's usable. |
| **No test / stale test** (P1, 🤖) | Nothing — robot tests it. |
| **Sending above safe volume >50/day** (P2) | Lower that inbox's campaign daily cap to 30–40. Scale by adding inboxes, never volume. |
| **Zombie campaign flagged** | Feed leads / pause / reassign — within days, not weeks. |
| **Bounce >3%** | Hard bounces → clean the list. Soft → cut volume 50% for 2 weeks. **>5% → stop immediately.** |
| **Gmail → Promotions tab** | 0–1 links in email 1, no images/heavy HTML, plain-text signature. |
| **New domain setup** | SPF (`~all`, never `+all`) · DKIM 2048-bit · DMARC `p=none` → after 2 clean weeks `quarantine` → after 30 days `reject`. Physical mailing address in the signature footer. |
| **Before ANY list upload** | Verify it (catch-all scoring, not just syntax). Remove invalid + risky addresses. |

**Golden rules (Avi):** warmup is NEVER paused or reduced — if an inbox underperforms, lower its **campaign** volume and **increase** warmup. Scale with more inboxes (2 per domain), never more volume per inbox.

---

## Switches (admin) — current state

Everything ships safe. Flip on Render env vars to activate:

| Switch | Now | What turning it ON does |
|---|---|---|
| `WARMUP_AUTO_ENABLED` | **off (dry-run)** | warmup executor actually enables/boosts (logs-only until then) |
| `RETEST_ENABLED` | **off (dry-run)** | auto-tests actually run (spends Smartlead credits, per-client daily cap) |
| `RETEST_DISABLE_WARMUP` | off | tests run warmup-off (fast, true placement), warmup auto-restored after |
| `ROTATION_ENABLED` | **off (dry-run)** | broken-sender swaps actually happen (dummy-campaign validation first!) |
| `WARMUP_ALWAYS_ON` | **ON** | never-pause-warmup policy (Avi) |
| `WARMUP_CONSERVATIVE_VOLUME` | off | warmup ramp 30/day instead of 40 |
| `HEALTH_SPAM_FLAG_ENABLED` | off | spam-during-warmup P0 flag |

Dry-run = the robot prints exactly what it *would* do in the deploy logs, changes nothing, spends nothing. Recommended go-live order: deploy → read one day of dry-run logs → flip `WARMUP_AUTO_ENABLED` → flip `RETEST_ENABLED` + `RETEST_DISABLE_WARMUP` with a small cap.

---

## Slack notifications — the plan (not built yet)

What exists: the daily digest is **coded and tested** — it groups action items by client, tags the manager, links the workbook. What's left to wire:

1. **Pick a live channel** (e.g. `#deliverability`) → set `HEALTH_NOTIFY_CHANNEL` env var. (Current fallback channel is archived — must be replaced.)
2. **Add Slack member IDs** for Balasankar, Anjali, Varsha in `manager_map.py` (names are already set; IDs make the @-mention actually ping).
3. **Invite the bot** to the channel.
4. Redeploy — digest posts automatically every morning after the sync.
5. (Later, optional) P0-only instant alerts, and a weekly summary thread.

Until then: the workbook is the source of truth — check it daily.

---

## Where everything lives

- **Dashboard sheet:** the shared Google Sheet → tabs: *Inbox Health* (main), All Inboxes, Campaign Metrics, per-client tabs
- **Deliverability test sheet:** manual test results (robot merges these with its own API test results — newest wins)
- **Run logs:** Render → infra-bot service → Logs (`[smartlead]`, `[retest]`, `[warmup]` prefixes)
- **Deep-dive docs:** `INBOX_HEALTH_PLAYBOOK.md` (thresholds/lifecycle) · `2026-DELIVERABILITY-COURSE-OF-ACTION.md` (why behind each rule) · `FEATURE_CHECKLIST.md` (what's built + verified)
