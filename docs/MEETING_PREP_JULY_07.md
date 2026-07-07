# Deliverability Operations & Inbox Maintenance Prep — July 07

*Use this document to lead the 3:30 PM deliverability sync with Avinash. It maps out the exact division of labor between the Bot and the Campaign Manager, detailing the 5 maintenance pillars and how we keep our 400+ inbox fleet healthy.*

---

## 🎯 The Core Vision: Always-Warm, Ever-Rotated

Our goal is to **defy computational limits and deliverability blockages** by automating the monitoring and management of our inbox fleet. 

Instead of manual verification, we use a daily automated pipeline running on our Render server that scores inboxes, keeps warmup running, test-placements automatically, and swaps out failing senders without interrupting live campaigns.

---

## 🤖 Part 1 — The Bot's Job (What is Automated & How)

The bot runs in sequence every morning (beginning at 10:00 AM IST) on the Render server:

```
10:00 AM ── THE DAILY SCAN (Scores all inboxes & writes to Google Sheets)
11:00 AM ── THE PLACEMENT LAB (Triggers automated spam-placement tests)
11:30 AM ── THE WARMUP GYM (Adjusts warmup status and applies trickles/boosts)
12:00 PM ── THE INBOX ROTATION (Swaps out P0 broken senders in campaign sequences)
```

### 1. Daily Scoring & Workbook Generation
*   **What it does:** Scans every inbox, computes a health score (0-100), assigns a grade (A-D), and writes a worst-first checklist to the **"Inbox Health"** tab.
*   **How it does it:** Calls the Smartlead API to fetch account details, merges them with historical scores from MongoDB, maps the client to a manager, and pushes the rows to Google Sheets.

### 2. Auto Placement-Testing (Dry-run by default)
*   **What it does:** Runs real email placement tests on stale (no test in 14 days) or untested inboxes.
*   **How it does it:** 
    1.  Uses `retest_executor.py` to identify targets.
    2.  Briefly disables warmup on the inbox (so the test measures real send-path deliverability).
    3.  Tells the SmartDelivery API to send test emails to a seed list.
    4.  Polls the result 24 hours later, updates the database, and **re-enables warmup**.

### 3. Always-On Warmup Management (Dry-run by default)
*   **What it does:** Keeps warmup permanently running to maintain domain reputation.
*   **How it does it:** 
    *   **Idle inboxes:** Warmed at 30/day (ramp +5/day).
    *   **Active inboxes (sending):** Warmup is **never** paused. It runs at a low **5-10/day trickle** to prevent reputation decay.
    *   **Low reputation (<90%):** Warmup is **boosted** and campaign sending volume is automatically reduced (campaign volume, not warmup, is lowered).

### 4. Automated Inbox Rotation (Dry-run by default)
*   **What it does:** Swaps out a P0-broken inbox (failed test) in a live campaign for a healthy bench inbox.
*   **How it does it (Load-bearing order):**
    1.  **Add** the healthy replacement (same client, same sender name e.g., Sam $\rightarrow$ Sam) to the Smartlead campaign.
    2.  Verify the replacement is attached.
    3.  Hand over in-flight leads to the replacement inbox to **preserve active conversation threads**.
    4.  **Remove** the broken inbox from the campaign (never leaves a campaign with 0 senders).

### 5. Daily Slack Notifications
*   **What it does:** Summarizes P0/P1 issues per manager and posts a digest to Slack every morning.

---

## 👤 Part 2 — The Manager's Job (What Humans Do & How)

Managers spend **5 minutes daily** checking the workbook and handling tasks where human intervention is required:

### 1. Fixing DNS Authentication
*   **When:** When the bot flags `dns_error` (SPF/DKIM/DMARC) in the workbook.
*   **How:** Managers log in to the DNS registrar (Cloudflare, GoDaddy) and update the TXT records according to the instructions in the workbook's "What To Do" column.

### 2. Reconnecting Logged-Out Inboxes
*   **When:** When the bot flags `disconnected` or `SMTP/IMAP disconnected` (P1).
*   **How:** Re-authenticate the email account credentials inside the Smartlead portal.

### 3. List Hygiene & Sourcing
*   **When:** Before uploading leads, or when bounce rates exceed 3% (P1).
*   **How:** Filter D2C e-commerce exports by **employee count** (10-5,000) or traffic rather than unreliable revenue filters. Use Scrubby to clean catch-all lists and run Clay shipping agents to check US activity.

### 4. Sequence & Copy Tuning
*   **When:** When inboxes land in spam despite clean DNS settings.
*   **How:** Rewrite copy, remove links/images, use spintax (vaying sentence structure), and limit sequences to 3-4 follow-ups.

### 5. Sparing bench inventory (Buying Domains)
*   **When:** When the client's bench has fewer than 5 spare inboxes (bot warns: "provision new domains").
*   **How:** Buy domains via Scaled Mail, setup SPF/DKIM/DMARC, keep Gmail to **3 per domain** and Outlook to **25 per domain**, and wait 2 weeks for warmup.

---

## 🩺 Part 3 — The 5 Maintenance Pillars (Detailed Deep-Dive)

Our deliverability health score (100 points max) and action resolution are driven by 5 core metrics:

```
HEALTH SCORE = Placement (40) + Warmup (25) + Bounce (20) + Connection/Auth (15)
```

| Pillar | How the Bot Checks It | What the Bot Does Automatically | What the Manager Must Do to Fix It |
|---|---|---|---|
| **1. Placement** *(40 pts)* | Reads manual or auto test scores on the deliverability tabs. | Triggers `retest_executor.py` to auto-test untested or stale (>14 days) domains. | If a test fails (<70% inbox placement): **Pause the inbox**, audit copy (cut links/images), re-verify the list, and wait for retest. |
| **2. Warmup** *(25 pts)* | Checks warmup reputation % and status in Smartlead API. | Keeps warmup ON. If rep < 90%, it **boosts** warmup and moves the inbox to the bench. | If warmup is blocked (P0): Investigate the block reason in Smartlead. Retire the inbox if it's young and unrecoverable. |
| **3. Bounce** *(20 pts)* | Monitors campaign analytics bounce rate. | Flags high bounces (>3% P1, >5% P0) in the workbook. | If bounce > 3%: Re-verify the email list (clean catch-alls). If bounce > 5%: **STOP the campaign immediately**. |
| **4. Connection** *(15 pts)* | Inspects `is_smtp_success` and `is_imap_success` flags. | Deducts all 15 points if disconnected, and flags as SMTP/IMAP Disconnected (P1). | Re-authenticate the email account connection inside the Smartlead portal. |
| **5. DNS Auth** *(Penalizes)* | Performs direct TXT queries using our new DoH checker script. | Deducts points (SPF: -5, DKIM: -5, DMARC: -3) and flags SPF/DKIM as P0, DMARC as P1. | Update TXT records in Cloudflare/GoDaddy. (DKIM requires generating a 2048-bit key in Google/Microsoft Workspace admin console). |

---

## 🚀 Part 4 — Alignment Checklist for Today's Meet

Use these points to close out the meeting and get approval to push the system live:

- [ ] **Smartlead Threading API Query:** Check if replacing an inbox via the API preserves active threads.
- [ ] **Tuning Outlook Volume:** Agree on decreasing daily limits to 10-15/day for inboxes flagged with Outlook spam issues.
- [ ] **LinkedIn Finder Scraper:** Confirm we will deprecate 85 Compute due to the $0.02/company fee and source credit-only billing alternatives.
- [ ] **Enable Auto-Warmup:** Set `WARMUP_AUTO_ENABLED=true` in Render to let the bot manage idle warmup and trickle states.
- [ ] **Slack Digest Activation:** Map manager Slack handles in [manager_map.py](file:///c:/Users/Manveen/Desktop/new_things_to_mess_araound/infrabot/infra-bot/smartlead_sync/smartlead/manager_map.py) and input `HEALTH_NOTIFY_CHANNEL`.

---

## 📨 Ready-to-send: Smartlead support question (copy-paste)

> Hi — when one of our sending inboxes has a deliverability drop and we replace it
> with a different inbox on the same campaign **via the API** (add the new
> email-account, remove the old one), how can we make sure leads who are
> mid-sequence receive their next follow-up **in the same email thread**?
> Is there a setting or API field that preserves the thread (subject/references)
> when the sending account changes? If yes, is it available via the public API?

*(If YES → we wire it into the rotation executor: fully seamless swaps. If NO →
current design stands: same-persona reassignment, new thread.)*

---

## 📊 Live numbers for the call (as of July 07)

| Fact | Number |
|---|---|
| Inboxes tracked (current clients only) | **409** (33 old-client inboxes excluded) |
| DARLEAN grades today | 39 A · 6 B · 30 C · 33 D — 63 P0 |
| Low-rep inboxes the warmup-boost will fix on enable | **41** (rep 68–82%) |
| Swaps rotation would make today (dry-run) | **4** — all persona-matched |
| SmartDelivery credits | **30 used / locked — no spend without approval** |
| Auto-test cap | 2 inboxes/client/day, worst-score-first |
| Everything write-capable | ships **dry-run** (env-var gated) |

**Recommended go-live order:** deploy → 1 day of dry-run logs → `WARMUP_AUTO_ENABLED` → `RETEST_ENABLED` + `RETEST_DISABLE_WARMUP` (credit approval) → dummy-campaign validation → `ROTATION_ENABLED`.

---

## ✅ UPDATE: Smartlead support ANSWERED (before the call)

**Same-thread swap = impossible** (protocol-level, no API setting — confirmed).
Their documented workflow is exactly our design: add new sender → remove old
(leads go PENDING, not redistributed) → **Resume Lead API** → follow-ups continue
from the new same-name sender in a new thread. Rotation executor already updated
to this. Tell Avi: question answered, design final, pending only the dummy-campaign
validation before enabling.
