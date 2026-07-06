# Our Email Deliverability System

*One document, everything you need to know. Written for everyone — no technical background needed.*

---

## 1. The background — why this exists

We send cold email for our clients. Every email we send comes from an **inbox** (like `sam@swiftbybw.com`), and every inbox has a **reputation** with Google and Microsoft. Good reputation → our emails land in the prospect's **inbox**. Bad reputation → they land in **spam**, and nobody ever sees them.

Reputation is fragile. It's earned slowly (weeks of "warmup") and lost quickly (bad lists, too much volume, spammy copy). And the worst part: **an inbox can go bad silently.** The campaign keeps "sending", the numbers look fine, but everything is going to spam — wasted leads, wasted money, and a burned domain.

Until now, keeping ~400 inboxes healthy across 8 clients meant manual checking: someone had to run tests by hand, watch warmup, notice problems, and remember which inbox belongs to whom. Things slipped through.

**So we built a system that watches every inbox, every day, automatically — and either fixes problems itself or tells the right person exactly what to do.**

---

## 2. The big picture — how it works

Think of it as a **daily health clinic for inboxes**. Every morning, four robots run in sequence:

```
10:00 AM  THE CHECK-UP     every inbox gets examined and scored 0–100
11:00 AM  THE LAB TEST     the weakest inboxes get a real spam-placement test
11:30 AM  THE GYM          warmup is kept running on every inbox, boosted where weak
12:00 PM  THE SUBSTITUTION broken inboxes in live campaigns get swapped for healthy ones
```

Everything lands in **one Google Sheet tab — "Inbox Health"** — which is the team's single place to look.

---

## 3. The features — what each one does, how, and why it matters

### ① The Health Score (the daily check-up)
- **What:** every inbox gets a score out of 100 and a grade — **A (green) / B / C (yellow) / D (red)** — every morning.
- **How it works behind the scenes:** the robot checks four things per inbox: did its last spam-test land in the inbox (40 points)? is its warmup reputation strong (25)? is it bouncing emails (20)? is it even connected (15)? Add them up = the score.
- **Why it helps:** one glance tells you the state of the whole fleet. Red = broken now. Yellow = weakening. Green = safe to use.

### ② The Workbook (the to-do list that writes itself)
- **What:** the "Inbox Health" tab — one row per inbox, sorted worst-first, showing: the problem, **the exact fix instruction**, who owns it (robot 🤖 or human 👤), which manager it belongs to, and the trend.
- **How:** the robot translates every problem into a plain instruction ("Pause inbox; check domain settings; retest") so nobody needs to be a deliverability expert.
- **Why:** the team never has to figure out *what's wrong* or *what to do* — only to do it.

### ③ Trend & Early Warning
- **What:** a ↑/↓ arrow per inbox comparing today's score to last week.
- **How:** every day's score is saved to a database; the robot compares over time.
- **Why:** a green inbox with a ↓ arrow is next week's red inbox. We catch decline **before** it becomes spam.

### ④ Automatic Spam-Placement Tests (the lab test)
- **What:** the robot runs real placement tests — it literally sends test emails and checks whether they land in inbox or spam — targeting the inboxes that need it most (never tested, or test older than 2 weeks).
- **How:** it picks the worst few per client each day (capped, so it never burns through test credits), briefly turns warmup off so the test is fast and accurate, runs the test, reads the result (e.g. "87% inbox"), turns warmup back on, and writes the outcome everywhere it matters.
- **Where results go:** the **"API Tests" tab** in the deliverability sheet (human-readable log, date + domain + result), and into the inbox's score/grade automatically the next morning.
- **Why:** placement tests are the ground truth of deliverability, and now they run themselves.

### ⑤ Always-On Warmup (the gym)
- **What:** every inbox stays in warmup permanently — our policy is warmup is **never paused or reduced**.
- **How:** the robot checks daily: any inbox with warmup off gets it turned on; any inbox with weak reputation (<90%) gets its warmup **boosted** to recover it. If an inbox underperforms, we lower its *campaign* volume — never its warmup.
- **Why:** warmup is what maintains reputation. Turning it off is how inboxes silently rot. Now that can't happen. (First scan already found **41 inboxes** quietly degrading that will get boosted.)

### ⑥ Zombie-Campaign Detection
- **What:** catches campaigns that look "ACTIVE" in Smartlead but are actually dead — no new leads and no sends for 2+ weeks.
- **Why it matters:** inboxes trapped in a dead campaign neither send nor stay warm — they decay invisibly. The robot rescues them back into warmup and flags the campaign so the manager either feeds it leads or closes it.

### ⑦ Automatic Inbox Rotation (the substitution)
- **What:** if an inbox in a **live campaign** fails its spam test, the robot swaps it out for a healthy spare from the **same client** — preferably with the **same sender name** (Sam → Sam) — and the campaign keeps running without interruption.
- **How (carefully ordered):** add the replacement first → hand over the leads mid-conversation to the same-name inbox (so follow-ups keep landing) → only then remove the broken one. It never leaves a campaign without senders, and it never mixes clients.
- **The clever part:** a broken inbox's follow-ups were going to spam anyway — the swap means follow-ups actually get *seen* again, from the same sender name.
- **Aftercare:** the broken inbox isn't deleted — replies still arrive, it stays in warmup, gets fixed, gets retested, and returns to the bench when green.
- **No spare available?** The robot alerts: "provision new domains for this client" (2-week lead time).

### ⑧ Risk Flags (2026 rules baked in)
- Sending **more than 50 emails/day** from one inbox → flagged (high volume = 4× more bounces; we scale by adding inboxes, not volume).
- Landing in **spam even during warmup** → top-priority flag.
- Google/Microsoft's new sender rules (authentication, complaint limits, one-click unsubscribe) are built into the fix instructions.

### ⑨ Focus on Current Clients Only
- Old clients (Avench, Monarch, Capsule Video, Gofloaters — 33 inboxes) are automatically excluded from everything. No wasted checks, tests, or credits on dead accounts.

### ⑩ Manager Routing + Daily Slack Message *(Slack part pending setup)*
- Every inbox is mapped to its owner: **Balasankar** (Darlean, Mythic, OSC, StaffAI), **Anjali** (Melior, Belardi Wong), **Varsha** (Better Data, Precise Leads).
- Once the Slack channel is connected, each manager gets a morning message listing *their* problem inboxes with a link to the workbook.

---

## 4. What's automatic vs. what humans do

**The robot does (nobody touches these anymore):**
daily scoring · trend watching · spam-placement testing · warmup management + boosting · swapping broken senders · zombie-campaign rescue · logging everything

**Humans do (the judgment work — the workbook tells you exactly which and when):**
- Fix domain settings (SPF/DKIM/DMARC) when a domain is misconfigured
- Rewrite spammy copy
- Reconnect logged-out inboxes (2-minute job)
- Verify/clean lead lists before uploading
- Decide: revive a dead campaign or close it
- Buy new domains when a client's bench runs low

---

## 5. The team routine (this is the whole job now)

- **Daily, 5 min:** open the Inbox Health tab → filter Manager = you → do the red **P0 👤** rows. The "What To Do" column is your instruction.
- **Monday, 30 min:** sweep your grades and ↓ trends; handle P1s; check bounce rates.
- **Friday, 20 min:** campaigns at the 21-day mark — scale the winners, kill the losers; deal with any zombie-campaign flags.
- **Every other Monday:** rotate — retire dead inboxes, promote warmed spares; if fewer than 5 spares, order domains.
- **Monthly:** fleet-wide placement check (target: 85%+ landing in inbox).

Rule of thumb: **red = today, yellow = this week, 🤖 = not your problem.**

---

## 6. Where everything lives

| What | Where |
|---|---|
| **The main dashboard** (Inbox Health + All Inboxes + Campaign Metrics) | the shared Google Sheet |
| Manual test results (as always) | deliverability sheet, client tabs |
| **Robot test results** (new) | deliverability sheet → **"API Tests" tab** |
| Robot activity logs | Render service logs (`[retest]`, `[warmup]`, `[rotation]`) |
| This doc + deeper references | project `docs/` folder |

---

## 7. What this gets us

- **No inbox goes bad silently** — everything is scored daily and decline is flagged early.
- **Emails keep landing in inboxes** — tested continuously, warmed permanently, swapped when broken.
- **Campaigns never stall** — broken senders are replaced without stopping anything.
- **The team spends minutes, not hours** — the system finds the problems AND writes the instructions; humans just execute the short list.
- **Nothing depends on memory** — ownership, history, results, and actions are all recorded automatically.
