# Deploy & Verify — Final Steps

---

## Update 2026-08-06 — Expandi (BettrData) on the Campaign Metrics tab

### Render env vars — two required, one already set

| Variable | Required? | Notes |
|---|---|---|
| `EXPANDEE_API_KEY` | **yes** | Expandi/Liaufa key. Without it the Expandi block is skipped entirely — no error, no rows. |
| `EXPANDEE_SECRET` | **yes** | Both are needed; a key without its secret is skipped with a warning. |
| `MONGO_URI` | already set | Reused for snapshots. If it were missing, Expandi rows would still appear but every month-to-date cell would read `?`. |

Add `BETTRDATA` to `CAMPAIGN_METRICS_CLIENTS` as well, or the workspace is
discovered and then filtered straight back out.

### Why the first days look wrong — and why that is correct

Expandi reports **cumulative lifetime counters and has no working date filter**
(every spelling of `start_date` / `date_from` is accepted and silently ignored).
Month-to-date is therefore computed by differencing a stored daily snapshot
against today's counters.

Consequences, in order of when you will notice them:

1. **Day 1: month columns show `?`.** No baseline exists yet. This is
   deliberate — printing the all-time figure there would inflate the Total row
   and read as a real month.
2. **Day 2 onward: `?` becomes a real number**, covering the period since the
   first snapshot.
3. **Full month-to-date is only correct from the first snapshot of a month.**
   There is no history to backfill, so August will under-report if snapshots
   started mid-month. September onward is exact.

`total_leads`, `leads_in_progress`, and `status` are correct immediately — they
are standing totals, not activity windows, so they need no differencing.

### Verify

```bash
python3 metrics_only.py --dry-run     # look for the "Expandi BETTRDATA" line
```

Expect `Expandi BETTRDATA: 2 account(s), N campaign(s)` and rows with
`platform=Expandi`. On the first run the month columns will be `?`; run it again
the next day and they become numbers.

---

**What to add on Render, how to verify nothing broke, and the exact script to run yourself for a live check. Written so you can do this without me.**

Created: 2026-07-08. Deployed commit: `4443a3f` (pushed to `main`, Render should auto-deploy).

---

## 1 — Env variables: what to add on Render

**Short answer: nothing is required right now.** Every new setting has a safe default baked into the code (`smartlead_sync/smartlead/config.py`) — dry-run / off / conservative. Render will run the new code with zero behavior change until you explicitly add a variable to override a default.

Add variables ONLY when you're ready to flip a switch. Here's the full reference:

### New variables from today (all optional — defaults shown)

| Variable | Default | What it does | When to set it |
|---|---|---|---|
| `HEADROOM_FIX_ENABLED` | `false` | When `true`, actually raises `max_email_per_day` on ACTIVE inboxes (currently: dry-run, just logs) | Step 1 of enablement (see §3) |
| `WARMUP_HEADROOM` | `20` | How much extra daily-send room to add for warmup | Leave default |
| `HEADROOM_FIX_PER_RUN_CAP` | `50` | Max inboxes fixed per day (safety cap) | Leave default |
| `BOUNCE_PROTECT_ENABLED` | `false` | When `true`, actually sets the 3% bounce auto-pause on campaigns missing it | Step 2 of enablement |
| `BOUNCE_PROTECT_THRESHOLD` | `3` | Bounce % that triggers Smartlead's auto-pause | Leave default |
| `BOUNCE_PROTECT_PER_RUN_CAP` | `10` | Max campaigns fixed per day | Leave default |
| `WARMUP_AUTO_ENABLED` | `false` | When `true`, actually applies the warmup state-machine changes (currently: dry-run) | Step 3 of enablement |
| `WARMUP_ACTIVE_PER_DAY` | `20` | Warmup volume for inboxes in a live campaign | Leave default |
| `WARMUP_IDLE_PER_DAY` | `20` | Warmup volume for bench/insurance inboxes | Leave default |
| `WARMUP_RECOVERY_PER_DAY` | `15` | Warmup volume for low-reputation inboxes | Leave default |
| `WARMUP_LONG_IDLE_DAYS` | `30` | Days idle before the LONG_IDLE reply-rate nudge kicks in | Leave default |
| `WARMUP_REPLY_RATE` | `25` | Normal warmup reply-rate setting | Leave default |
| `WARMUP_BOOST_REPLY_RATE` | `30` | Reply rate for recovering inboxes | Leave default |
| `WARMUP_AUTO_ADJUST_ACTIVE` | `true` | Turns on Smartlead's own auto-adjust for ACTIVE inboxes | Leave default (already correct) |

**Nothing else needs to change.** All existing env vars (Smartlead API keys, Mongo URI, Google service account, Slack tokens) stay exactly as they are — nothing in today's work touches those.

---

## 2 — Verify the deploy didn't break anything

Do these checks in order, right after Render finishes redeploying:

1. **Render build log** — confirm the deploy succeeded (no red errors, process started).
2. **Wait for the next scheduled sync (10:00 AM IST)** or trigger it manually if Render allows — confirm:
   - Google Sheet "All Inboxes" and "Inbox Health" tabs got today's date and row counts look normal (should be similar to yesterday: ~376 inboxes, ~1212 campaign-rows).
   - No new errors in the Render log for the sync job.
3. **Check the 3 new cron jobs are registered** — Render log should show these lines appear at their scheduled times (IST):
   - `[CRON] Headroom fix firing at ...` — 12:15 PM
   - `[CRON] Bounce-protect sweep firing at ...` — 12:30 PM
   - `[CRON] Blacklist monitor firing at ...` — Monday 9:00 AM only
4. **Check each new job's dry-run output matches what we saw locally** (numbers may shift slightly day to day, but should be in the same ballpark):
   - Headroom: `[Headroom] ~116 ACTIVE inbox(es) need max_email_per_day raised`
   - Bounce-protect: `[bounce-protect] ~24 campaigns missing`
   - Warmup: `[Warmup] ~375 change(s)`
   - All should end with `(DRY-RUN - not applying)` — if you see anything applying without you setting the ENABLED flag, STOP and tell me immediately (would mean a default got flipped wrong).

If all 4 match — the deploy is verified working, nothing is live-changing yet, and it's operating on the same real data we tested against locally.

---

## 3 — Run it yourself (the live check, step by step)

You can run any of these directly from your machine right now to see live output without waiting for the cron:

```bash
cd infra-bot/smartlead_sync

# 1. See current campaign/inbox state get pulled + written to Sheets (safe, already run today)
python3 run.py

# 2. See exactly which inboxes need their daily limit raised (dry-run, changes nothing)
python3 headroom_fix_executor.py

# 3. See exactly which campaigns are missing bounce protection (dry-run, changes nothing)
python3 bounce_protect_executor.py

# 4. See exactly which inboxes need warmup changes (dry-run, changes nothing — takes ~1hr, it's thorough)
python3 warmup_executor.py

# 5. See which domains are on real blacklists (read-only, always safe to run)
python3 blacklist_monitor.py
```

Every one of these is safe to run as many times as you want — none of them write anything unless you've set the matching `_ENABLED=true` variable, which you haven't yet. Running them is exactly how you build your own confidence, independent of me.

**What "everything is working" looks like:** each script prints a clear summary line (like the ones in §2) and ends cleanly. What would mean something's wrong: a Python error/traceback, or a count that's wildly different from what's listed above (e.g. 0 inboxes found, or 3000 changes instead of ~375).

---

## 4 — The actual enablement order (once you've verified §2 and §3 look right)

**Never flip two at once.** Wait 2-3 days between each, watching the logs.

```
STEP 1 (this week)   Set HEADROOM_FIX_ENABLED=true on Render.
                      Why first: warmup can't get its volume if there's no
                      room in the daily limit. This just raises the ceiling.
                      Verify: check one inbox's daily limit went up in Smartlead UI.

STEP 2 (2-3 days later)   Set BOUNCE_PROTECT_ENABLED=true.
                      Independent of warmup — safe to do any time, but doing
                      it early means a bad list can't quietly damage domains
                      while you're still testing warmup changes.
                      Verify: one campaign's Setup tab shows bounce threshold = 3.

STEP 3 (2-3 days later)   Set WARMUP_AUTO_ENABLED=true.
                      This is the big one — applies the 375 warmup changes.
                      First day will be a big wave (expected). Day 2 onward
                      should show close to 0 new changes (means it "stuck").
                      Verify: spot-check 3 inboxes' warmup settings in Smartlead UI.

LATER                 RETEST_ENABLED and ROTATION_ENABLED — these already
                      existed before today, still dry-run, not part of
                      today's changes. Enable per the existing plan
                      (one client, low cap, expand from there).
```

---

## 5 — What to tell every campaign manager tomorrow

Read this to them, or forward it as-is.

> **What changed:** The system that manages inbox warmup was fixed. It used to turn warmup completely OFF the moment an inbox started sending a real campaign — that was actually wrong and could quietly hurt deliverability over time. Now warmup stays on always, just at a lower, smarter volume while a campaign is running.
>
> **What this means for you day-to-day: nothing changes yet.** All of this is running in "watch only" mode right now — it's calculating what it WOULD do and logging it, but not touching any live inbox settings. You will not see any inbox settings change in Smartlead this week because of this.
>
> **What you should keep doing:** exactly what's in the Inbox Health workbook, same as before — check your P0/P1 rows daily, same rules as always. Nothing about your daily routine changes.
>
> **What's new for you to know:**
> - If you see a DNS problem (SPF/DKIM/DMARC) flagged in the workbook, the instruction now correctly says to escalate it to Zapmail support — never edit DNS yourself, that's their job.
> - We found a real issue: **105 of our 118 sending domains are listed on a spam blacklist called SURBL.** This mainly matters if links inside an email (tracking links, signature links, unsubscribe links) point to one of the listed domains — not just because the domain sends from there. If your client's emails have that kind of link, flag it — we're triaging this centrally, don't try to fix it yourself yet.
> - Over the next 1-2 weeks, you may start seeing warmup settings actually change on inboxes (once it's turned on for real) — that's expected and automatic, no action needed from you unless something looks clearly wrong (e.g., an inbox you know is dead suddenly shows high warmup volume — flag that).
>
> **Who to ask if something looks off:** [Avinash / whoever owns this rollout].

---

## 6 — Summary: what's actually true right now

- Code is deployed (pushed to `main`, commit `4443a3f`).
- **Nothing live has changed for any client, any inbox, any campaign.** Everything new is dry-run.
- You can verify this yourself anytime with the commands in §3 — no dependency on me.
- The path from here is: verify §2 → run §3 yourself → follow the enablement order in §4, one step at a time, watching logs between each.
- Even at the end of §4, this covers warmup + bounce protection + blacklist detection — it does NOT mean every deliverability lever is covered (copy quality, list hygiene, capacity planning are still open — tracked in `SCALE_ROADMAP.md`).
