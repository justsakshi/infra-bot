# Deliverability Master Plan

**Concrete execution plan to keep every client inbox landing in the inbox — built from the 2026-07-07 review meeting, Smartlead official docs/blogs, and Instantly official docs/blogs.**

Created: 2026-07-07
Companion to: [INBOX_HEALTH_PLAYBOOK.md](INBOX_HEALTH_PLAYBOOK.md) (this plan supersedes the playbook's warmup rules — see §2)

---

## 0 — The One Big Correction (read this first)

Our current playbook says: *"Active (sending) inbox: warmup OFF."*

**Both vendors say this is wrong:**

- Smartlead: "Warmup continues through campaigns, not instead of them… run it indefinitely alongside your campaigns." Teams that turn warmup off post-launch "see deliverability erosion within 6–8 weeks." ([blog](https://www.smartlead.ai/blog/how-to-warmup-email-address-before-cold-outreach))
- Instantly: "Always keep the warm-up enabled — never turn it off." ([help doc](https://help.instantly.ai/en/articles/5975326-instantly-cold-email-strategy))

**New rule: warmup is ALWAYS ON for every inbox. Only the volume changes by state.**

The mechanism that makes this safe while campaigns run is Smartlead's **auto-adjust** ("Smart-adjusting algorithm"): when a mailbox is in an active campaign it automatically reduces warmup by 7–10/day and manages the send/reply ratio from real-time performance; when campaigns end it restores warmup to optimal. ([helpcenter art. 63](https://helpcenter.smartlead.ai/en/articles/63-smartlead-s-smart-adjusting-algorithm-for-mailbox-warmups))

> This changes the design of the auto-warmup executor (playbook Part 6-B). It is no longer an ON/OFF flipper — it is a **volume-mode setter**. See §5.2.

---

## 1 — Unified Warmup Policy (the numbers, final)

Reconciled from: meeting feedback (Avinash), Smartlead official, Instantly official. Where they disagreed, the more conservative official number wins.

| Inbox state | Warmup/day | Ramp | Reply rate % | Auto-adjust | Notes |
|---|---|---|---|---|---|
| **NEW (just provisioned)** | start ~5, ramp to **40 cap** | **+2–5/day**, enabled at warmup start (won't work if enabled later) | **25–30%** | OFF (no campaign yet) | 21–30 days before first campaign send (see §1.1) |
| **WARMING (pure warmup, no campaign)** | **40 max** | — (at cap) | 25–30% | OFF | Smartlead official: "do not set above 40 for new accounts" |
| **ACTIVE (sending in live campaign)** | **20 max** (auto-adjust will trim to ~10–15) | 0 | 25–30% | **ON — mandatory** | Meeting said ≤30; Smartlead official says 10–20 (art. 52) / 15–25 (API guide). 20 + auto-adjust satisfies both |
| **INSURANCE (warmed, idle, on standby)** | **15–20** | 0 | 25–30% | OFF | Enough to hold reputation without burning warmup-pool trust |
| **RECOVERING (failed test / was blocked)** | **15** | 0 | 25–30% | OFF | Re-enable low; 14–30 days warmup-only, zero campaign sends |
| **RETIRED** | OFF | — | — | — | Only state where warmup is off |

**Fleet-wide toggles (set on every inbox, every client):**
- ✅ **Send warmup emails only on weekdays** — ON (meeting + both vendors: mimics real business behavior)
- ✅ **Auto-adjust warmup sending ratio** — ON for any inbox attached to a campaign
- ✅ Warmup reply rate — 25–30%, **never above 30** (Smartlead: unnaturally high reply rate is itself a spam signal)
- ✅ Custom tracking domain link warmup — ON where a custom tracking domain exists (Smartlead warms the tracking link inside warmup copy so tracked campaign emails don't get filtered)

**Key insight from the meeting, confirmed by Smartlead:** when boosting a struggling inbox, **raise the reply-rate %, not the volume**. Replies are the strongest reputation signal; volume increases without engagement look like spam. "Boost warmup" = reply rate 30% + volume within state cap — never volume beyond cap.

### 1.1 — New-inbox timeline correction

Playbook said "~2 weeks warmup → sendable." Official guidance:

- Smartlead: minimum 14 days for aged/existing accounts; **21–30 days for new domains**; 6–8 weeks for damaged domains
- Instantly: help docs 14 days minimum; blog stance 3–4 weeks; domain itself should be ≥30 days old before cold sends

**New rule: brand-new domain + inbox = 21 days minimum, 30 preferred, before first campaign email.** Aged domain with a new inbox = 14 days OK. Gate promotion on warmup reputation ≥90% (trailing placement of warmup mail), not just calendar days.

---

## 2 — Campaign Sending Limits (per inbox)

Smartlead's own data ([email frequency blog](https://www.smartlead.ai/blog/email-frequency-best-practices-for-cold-emails)):

| Volume/day | Reply rate | Bounce | Inbox placement |
|---|---|---|---|
| **20–49** | 5.7% | 1.2% | **88%** |
| 50–99 | 3.1% | 3.4% | 71% |

| Rule | Value | Source |
|---|---|---|
| Campaign sends per inbox/day, steady state | **20–30 max** | Smartlead optimal band + Instantly official 30/day |
| New inbox first campaign volume | **5–15/day**, ramp +2/day | Smartlead API guide (10–15) + Instantly slow-ramp (+2/day) |
| Provider nuance | Gmail 30–40, Outlook 30–50, SMTP 40–50 | Smartlead |
| Combined (warmup + campaign) per inbox | ≤ mailbox `max_email_per_day`; e.g. 40 = 15 warmup + 25 campaign | Smartlead API guide |
| Scaling | **Add inboxes, never raise per-inbox volume** | Both vendors |
| Send gaps | ≥9 min between sends per inbox, ±random | Instantly |
| ESP matching | ON (Gmail→Gmail, Outlook→Outlook); +16% deliverability claimed | Smartlead art. 72 |

---

## 3 — Thresholds Update (changes to the health score / workbook rules)

Current playbook thresholds mostly hold. Changes/tightenings:

| Metric | Old rule | New rule | Why |
|---|---|---|---|
| Spam placement pause line | <70% pause | **<80% pause + investigate** (Gmail placement specifically: require >80% before any launch) | Instantly pause-rule: IPR <80–85% over 300 sends → pause |
| Bounce steady-state | <1% excellent, >3% red | Same, but **enable Smartlead "High Bounce Rate Auto Protection" at 3% on every campaign** — platform pauses automatically, no human needed | Smartlead art. 274/210 |
| Spam complaints | (not tracked) | **≥0.3% in 7 days → pause mailbox; resume only after <0.1% sustained** | Google/Yahoo bulk-sender rules + Instantly |
| Warmup reputation | <50 do not send | Add: **campaign-ready gate = ≥90%**; week-2 health <55 → pause ramp and diagnose | Instantly readiness gate + Smartlead benchmarks |
| SpamAssassin score (from Smart Delivery report) | (not used) | **<5.0 required, <3.0 target** — score + firing factors now feed the health workbook | Smartlead |
| Resume-after-fix | 7 days then retest | Keep, plus: **3 consecutive days of healthy metrics before scaling volume back up** | Instantly resume protocol |

---

## 4 — DNS & Blacklists (ownership: Zapmail, verification: us)

**Meeting decision: we do NOT edit DNS ourselves. Zapmail owns DNS for ~90% of domains — misconfigurations are support tickets to Zapmail, not fixes we apply.** Our job is detection and escalation.

1. **Outlook DKIM findings:** the "DKIM misconfigured/missing" rows in the workbook are mostly Outlook inboxes — Outlook historically didn't require tenant DKIM. Action: send the list of flagged domains to Zapmail support, ask (a) is this expected for Outlook, (b) can they enable DKIM per domain. Do not treat Outlook-DKIM-missing as P0 until Zapmail answers.
2. **DMARC progression:** p=none first 2–4 weeks → p=quarantine → p=reject after 30 days clean. Publish `rua` tag so aggregate reports flow. SPF must never be `+all`.
3. **SPF+DKIM must authenticate ≥48h before DMARC is enabled** on new domains (Instantly sequencing rule) — add to new-domain checklist.
4. **Custom tracking domain per client** — verify each client has one (never the shared default), CNAME set, and Smartlead's tracking-link warmup toggle ON.
5. **Blacklist checks:**
   - **At domain purchase** (meeting decision — we stopped doing this when we moved to Zapmail; restart it) and **weekly** for active domains.
   - Severity tiers: **serious** = Spamhaus (DBL/ZEN), SURBL, Barracuda, MSRBL → act (pause domain, delist request, escalate). **Noise** = UCEProtect and similar pay-to-delist lists → log, ignore.
   - Blacklist status already appears in the Smart Delivery report — parse it into the workbook instead of a separate lookup where possible.
6. **Zapmail API integration (new):** Zapmail exposes DNS-records retrieve, DMARC add, DNS change, domain purchase, and their own placement test. Build a read-only `zapmail_dns_check.py` first — pull expected records per domain, diff against live DNS, auto-file the discrepancy into the workbook with owner=Zapmail. Write operations stay manual until trust is earned.

---

## 5 — System Changes (the build list, in order)

### 5.1 — Warmup settings enforcement script (NEW — build first, biggest win)
One script, dry-run first, that sweeps every inbox across all clients and enforces §1:
- weekdays-only ON, auto-adjust ON (campaign-attached inboxes), reply rate → 25–30%, `total_warmup_per_day` → state cap (40/20/15), ramp settings for NEW state.
- Smartlead API: `POST /email-accounts/{id}/warmup` (`total_warmup_per_day`, `daily_rampup`, `reply_rate_percentage`, `warmup_key_id`).
- Output: per-inbox diff log ("would change X→Y") → review → enable. Same pattern as the existing executors.

### 5.2 — Auto-warmup executor: redesign (rule change)
Old design: warmup OFF when actively sending. **Replace with state machine:**
```
for each inbox:
  state = NEW | WARMING | ACTIVE | INSURANCE | RECOVERING | RETIRED
     (derived from existing campaign/availability/health data)
  warmup = ALWAYS ON (except RETIRED)
  apply §1 volume/reply-rate/auto-adjust profile for that state
  flip only inboxes whose live settings ≠ profile
```
- Stale/zombie-campaign rescue logic stays (inboxes on zombie campaigns → WARMING profile).
- Ships dry-run; enable after one week of clean diff logs.

### 5.3 — Health score: add signals
- SpamAssassin score + firing factors (DKIM_INVALID, HTML_MESSAGE, LINK_REDIRECT…) from the Smart Delivery report → new sub-signal + "what to do" mapping per factor.
- Spam-complaint rate (where available) → new red-line rule (§3).
- Warmup-blocked / cool-off status from Smartlead → existing signal, keep.

### 5.4 — Bounce auto-protection rollout
Enable Smartlead High Bounce Rate Auto Protection at **3%** on every live campaign (one-time sweep + check in daily sync that new campaigns have it). Wire its webhook → Slack alert.

### 5.5 — DNS checker + Zapmail escalation lane (was Part 6-D "longer-term" — promote)
Read-only SPF/DKIM/DMARC/MX checker inside the daily sync; discrepancies → workbook row with owner 👤 + action "escalate to Zapmail" (never "edit DNS").

### 5.6 — Blacklist monitor
Weekly cron: check active domains against serious lists (§4.5). New-domain purchase flow gets a pre-flight check.

### 5.7 — Placement-test cadence (auto-tester already built — set policy)
- **Pre-launch:** every inbox gets a Smart Delivery test before joining a new campaign (blocking gate: Gmail placement >80%, SpamAssassin <5).
- **Monthly:** fleet-wide sweep (existing plan, keep, target ≥85%).
- **Auto-retest:** stale/worst-first executor (built, dry-run) — enable with per-client caps, start 1 client × cap 1.
- Never retest a fixed inbox before 7 days (reputation moves slowly).

### 5.8 — Inbox substitution — confirm before enabling
Smartlead support confirmed replacement inbox starts a NEW thread. **Open question to close with support: does the follow-up email sent from the new inbox carry the original subject line (Re: …) or the step's subject?** We don't set subject on every step, so verify before turning substitution on. Until then: substitution = recommend-only in the workbook.

### 5.9 — Copy-side guardrails (feed campaign QA, not inbox system)
First email: text-only (or minimal HTML), <150 words, **0 links, no images**, no tracking pixel — **disable open tracking on cold campaigns** (Smartlead claims tracking pixels cut replies ~68%; Instantly recommends open-tracking off platform-wide). Links/images allowed from follow-ups, max 1 link, never shorteners. Spintax: 2–4 options per group, ~5 groups, section-level not word-level. List-Unsubscribe header ON. Lists ≥95% verified before upload.

---

## 6 — Who does what, when (delta to the existing calendar)

Existing daily/Mon/Wed/Fri manager calendar in the playbook stays. Additions:

| When | New task | Owner |
|---|---|---|
| Now (this week) | Zapmail ticket: Outlook DKIM question + flagged-domain list | Manveen |
| Now | Smartlead support: substitution subject-line question (§5.8) | Manveen |
| Now | Slack digest go-live: channel + manager handles per client | Avinash → Manveen |
| Week 1 | Build + dry-run §5.1 (warmup enforcement sweep); review diffs together | Manveen |
| Week 1 | Enable bounce auto-protection sweep (§5.4) | Manveen |
| Week 2 | Redesign auto-warmup executor to state machine (§5.2), dry-run | Manveen |
| Week 2 | Enable auto-retest for 1 client, cap 1 (PRECISE_LEADS has 92 credits) | Manveen |
| Week 3 | DNS checker (§5.5) + blacklist monitor (§5.6) in daily sync | Manveen |
| Week 3+ | SpamAssassin factors into health score (§5.3) | Manveen |
| Ongoing | New-domain purchase: blacklist pre-flight + 48h SPF/DKIM-before-DMARC + 21–30-day warmup gate | Whoever provisions |
| Monthly | Fleet placement sweep target ≥85%; Google Postmaster weekly check where Gmail domains exist | Managers |

---

## 7 — Source Index

**Smartlead official:** [warmup API guide](https://api.smartlead.ai/guides/email-warmup) · [helpcenter 52 — AI warmups](https://helpcenter.smartlead.ai/en/articles/52-how-to-enable-ai-email-account-warm-ups) · [63 — smart-adjusting algorithm](https://helpcenter.smartlead.ai/en/articles/63-smartlead-s-smart-adjusting-algorithm-for-mailbox-warmups) · [164 — daily ramp-up](https://helpcenter.smartlead.ai/en/articles/164-understanding-the-daily-ramp-up-feature-in-smartlead-s-email-warm-up) · [275 — weekday-only warmup](https://helpcenter.smartlead.ai/en/articles/275-how-to-enable-weekday-only-warm-up-emails-in-smartlead) · [72 — ESP matching](https://helpcenter.smartlead.ai/en/articles/72-understanding-esp-matching-in-smartlead) · [211 — tracking-link warmup](https://helpcenter.smartlead.ai/en/articles/211-custom-domain-tracking-link-warmup) · [274 — bounce auto-protection](https://helpcenter.smartlead.ai/en/articles/274-what-is-high-bounce-rate-auto-protection-in-smartlead) · [237/239 — SmartDelivery](https://helpcenter.smartlead.ai/en/articles/239-understanding-your-deliverability-report-in-smartdelivery) · blogs: [warmup before outreach](https://www.smartlead.ai/blog/how-to-warmup-email-address-before-cold-outreach) · [email frequency](https://www.smartlead.ai/blog/email-frequency-best-practices-for-cold-emails) · [deliverability guide](https://www.smartlead.ai/blog/email-deliverability-guide) · [spintax](https://www.smartlead.ai/blog/what-is-spintax) · [SpamAssassin](https://www.smartlead.ai/blog/what-is-spamassassin-score-and-how-to-fix-it)

**Instantly official:** [warmup settings](https://help.instantly.ai/en/articles/7988514-warmup-settings) · [cold email strategy](https://help.instantly.ai/en/articles/5975326-instantly-cold-email-strategy) · [account/campaign limits](https://help.instantly.ai/en/articles/6248612-account-and-campaign-limits) · [high-bounce auto-pause](https://help.instantly.ai/en/articles/9823139-high-bounce-auto-pause-feature) · [MX/SPF/DKIM/DMARC setup](https://help.instantly.ai/en/articles/6222192-how-to-set-up-mx-spf-dkim-dmarc-domain-forwarding) · [avoid spam filters](https://help.instantly.ai/en/articles/6222401-how-to-ensure-your-emails-get-delivered-and-avoid-spam-filters) · [inbox placement automated tests](https://help.instantly.ai/en/articles/10258482-inbox-placement-automated-tests) · blogs: [warmup guide](https://instantly.ai/blog/email-warmup-guide/) · [90% deliverability](https://instantly.ai/blog/how-to-achieve-90-cold-email-deliverability-in-2025/) · [secondary domains](https://instantly.ai/blog/secondary-domains/) · [AI-SDR deliverability](https://instantly.ai/blog/ai-sdr-email-deliverability-guide/) · [cold email spam fix](https://instantly.ai/blog/cold-email-spam-fix/) · [automated pauses](https://instantly.ai/blog/automate-cold-email-campaign-pauses-for-deliverability/) · [DMARC](https://instantly.ai/blog/dmarc/)
