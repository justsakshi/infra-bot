# Inbox Health System — Team Guide

**What we built + how to use it.** Plain language, for campaign managers.

---

## What this system is

A robot that watches every inbox we send from, across every client, and every day:
1. **Scores** each inbox 0-100 on how healthy it is
2. **Tells** you exactly what's wrong + what to do
3. **Runs tests** to check if emails land in the inbox (not spam)
4. **Manages warmup** so inboxes stay warm
5. **Messages** you on Slack with your action list

You don't run anything. It runs itself daily. You just read the workbook + do the human fixes.

---

## The one place you look: the Inbox Health workbook

A Google Sheet tab called **"Inbox Health"**. One row per inbox. Sorted worst-first (P0 at top).

**What each column means:**
| Column | Meaning |
|---|---|
| Priority | P0 = urgent (do today), P1 = this week, P2 = routine, blank = healthy |
| Health Score / Grade | 0-100, A(green)/B/C(yellow)/D(red) |
| Trend | ↑ improving, ↓ getting worse, → flat |
| Top Problem | the #1 thing wrong |
| **What To Do** | your exact next step |
| Owner | 🤖 Auto (robot handles it) or 👤 You (needs you) |
| Manager | who owns this inbox |

**Your daily job:** open it, work the 👤 You rows top-down. Ignore 🤖 Auto rows.

---

## What we built TODAY (the features)

| Feature | What it does for you |
|---|---|
| **Health scoring** | Every inbox graded daily. See problems before they blow up. |
| **The workbook** | One place: score + problem + exact fix + who owns it. |
| **Trend tracking** | ↓ arrow warns you an inbox is declining BEFORE it fails. |
| **Auto placement-test** | Robot runs spam-placement tests on stale/untested inboxes. Results flow into the score. |
| **Auto-warmup** | Keeps warmup ON unless an inbox is actively sending. Rescues "stale campaign" inboxes going cold. |
| **Stale-campaign detection** | Catches campaigns that look ACTIVE but are dead (no leads/sends 14d) — so their inboxes don't rot. |
| **Slack digest** | Every morning, a message lists your client's problem inboxes + links the workbook. |
| **2026 safety toggles** | Conservative volume (30/day), maintenance warmup trickle, spam-landing red flag. |

Plus two reference docs: the **Playbook** (thresholds + lifecycle) and the **2026 Course of Action** (what changed + per-case actions).

---

## How the auto-test works (getting results)

You asked how results come back. Here's the flow:

```
Day 1: robot picks a stale/untested inbox's campaign -> creates a placement test
        (Smartlead sends seed emails to test inboxes) -> takes 5-20 min
Day 2: robot checks the test -> reads the result (X% inbox, Y% spam)
        -> writes it into the inbox's health data
        -> next score uses the fresh result -> workbook updates
```

**You see the result in the SAME workbook** — the inbox's grade updates and its "stale/untested" flag clears. No new place to check.

- Tests cost Smartlead credits (PRECISE_LEADS has ~92). The robot caps how many it runs per client per day, and stops safely if credits run out.
- **This is OFF by default** (dry-run) — it lists what it *would* test but spends nothing until turned on.

---

## What YOU (the manager) do vs what the ROBOT does

**🤖 Robot (automatic, no action from you):**
- Scores + grades every inbox daily
- Tracks trends, warns of decline
- Runs placement tests (when enabled)
- Manages warmup on/off (when enabled)
- Slacks you your list

**👤 You (needs human judgment):**
- Fix DNS (SPF/DKIM/DMARC) on flagged domains
- Rewrite spammy copy
- Reconnect disconnected inboxes
- Clean/verify lists before upload
- Decide: revive a stale campaign (add leads) or pause it
- Retire dead inboxes, promote fresh ones
- Respond to positive replies

---

## Your weekly rhythm (quick version)

- **Daily:** open workbook, clear P0s (red).
- **Monday:** full sweep — grades, bounces, P1s.
- **Wednesday:** respond to replies.
- **Friday:** review campaigns at 21-day mark (keep/iterate/kill).
- **Every 2 weeks:** rotate inboxes (retire bad, promote fresh).
- **Monthly:** placement tests across the fleet (target ≥85% inbox).

(Full detail: the Playbook doc, Part 8.)

---

## To turn it all on (admin, one-time)

Set these env vars on Render + redeploy:
- `HEALTH_NOTIFY_CHANNEL` — Slack channel for the digest
- fill `manager_map.py` — each client's manager name + Slack handle
- `RETEST_ENABLED=true` — real placement tests (spends credits)
- `WARMUP_AUTO_ENABLED=true` — apply warmup changes
- `WARMUP_MAINTENANCE_TRICKLE=true` — 2026 trickle (recommended)
- `WARMUP_CONSERVATIVE_VOLUME=true` — 30/day volume
- `HEALTH_SPAM_FLAG_ENABLED=true` — spam-landing red flag

Everything ships OFF/dry-run — nothing changes or spends until these are set.

---

## Bottom line

The robot does the **watching + safe auto-maintenance**. You do the **judgment** — with the workbook telling you exactly what, where, and when. Result: inboxes stay healthy, land in the inbox, and problems get caught before they cost you a client's campaign.
