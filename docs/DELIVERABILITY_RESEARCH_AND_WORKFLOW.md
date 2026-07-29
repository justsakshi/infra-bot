# Deliverability: The Research, and the Workflow We Propose

**Date:** 2026-07-29 · **For:** the team · **No technical background needed**
**This is the document to read.** Everything else in `docs/` is supporting detail.

---

## In three sentences

Our email settings are correct, our domains are clean on the blacklists that matter, and our bounce rate is
healthy — yet a large share of our sending lands in spam. The cause is not any individual domain: **our domains
look like one bulk operation** to spam filters, and separately **Microsoft has broken for cold email across the
industry**. So the plan is not to repair domains, it is to **spot decline early, replace dead domains fast, and
stop buying domains that look like each other**.

---

# PART ONE — THE RESEARCH

## 1. What we tested, and what we found

We sent real test emails from our own production mailboxes to seed addresses at Gmail and Outlook, then recorded
where each one landed. This is the first data we have from the mailboxes clients actually send from.

| Domain | Mailbox hosted on | Landed at Gmail | Landed at Outlook |
|---|---|---|---|
| heybelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — spam |
| realbelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — spam |
| reachbw.com | Microsoft 365 | 0 of 4 — spam | 0 of 4 — spam |
| bwdirectmail.com | Microsoft 365 | 0 of 4 — spam | 0 of 4 — spam |

**Google-hosted mailboxes work at Google and fail at Microsoft. Microsoft-hosted mailboxes fail everywhere.**

One failing domain had sent 8 emails in its entire life, so it cannot have earned a bad reputation.

### This overturned a decision we were about to make

heybelardiwong.com and bwdirectmail.com were both on a list to be deleted, based on a manual test from 9 July.
heybelardiwong turns out to be one of our **healthier** domains.

That 9 July test also does not describe our real setup — it was sent from mailboxes that exist in none of our
Smartlead accounts. Two domains it scored 54% and 100% test identically today.

**Rule going forward: we act on measurements taken from our own production mailboxes.**

## 2. What we ruled out, with evidence

| Suspected cause | Verdict |
|---|---|
| Wrong SPF / DKIM / DMARC | **Ruled out** — valid and aligned on all four, `p=reject` in place |
| Blacklisted domains | **Ruled out** — 0 of 121 on Spamhaus DBL, the list that matters |
| Bad sending reputation | **Ruled out** — fleet bounce 1.23%; a failing domain had 8 lifetime sends |
| Poor warmup | **Ruled out** — 100% warmup score on a mailbox landing 0 of 4 at Microsoft |

That last one matters beyond this investigation: **warmup scores do not predict deliverability.** We have been
treating a number as meaningful that demonstrably is not.

## 3. What is actually causing it

**Cause 1 — our domains look like one bulk operation.**
68 of 97 domains share a single IP address across all four clients. Names follow an obvious pattern
(`heybelardiwong`, `getbelardiwong`, `trybelardiwong`, `reachbw`, `sendbw`). Bought in bulk, same time, same
supplier. **99 of 121 were flagged by SURBL**, a blacklist that looks for exactly this. Zapmail confirmed the
SURBL flagging came from their infrastructure rather than our sending, and have moved domains off Cloudflare.

**Cause 2 — Microsoft has broken for cold email generally.**
Industry-wide, Outlook placement has fallen sharply and Microsoft now filters on *sending patterns* rather than
message content. No configuration fixes this. The realistic answer is to depend less on Microsoft-hosted
mailboxes.

## 4. What the vendors told us

We put direct questions to all three. The answers shaped the workflow more than anything else.

**Smartlead:**
- A mailbox is **locked to a lead for that lead's entire sequence.** Remove it and their follow-ups stop. Belardi
  Wong has **321 leads mid-sequence** right now.
- There is a fix — "Reallocate Mailboxes" moves those leads to a new mailbox and keeps their sequence position
  and timing.
- **But it is manual, with no API.** They confirmed there is no workaround.
- They first said it "maintains the email thread", then narrowed that to continuity "from the campaign
  perspective" and would not confirm the recipient sees one conversation. They did confirm **a prospect replying
  to the old mailbox may not be tracked.**

**Zapmail:**
- Belardi Wong's domains are migrated to Google DNS. **But Precise Leads still has 31 domains on Cloudflare and
  12 with no working nameservers.**
- They ran their own test on our Microsoft mailboxes and **found no problem** — the opposite of our result. They
  asked us to test repeatedly over several days before they investigate further. That is a fair request.
- No API for triggering tests, and no health data available programmatically.

**EmailGuard:** full API, but the features we hoped for did not apply (see §6).

## 5. The pattern worth naming

**Nearly every deliverability capability our vendors sell is UI-only** — Smartlead's mailbox reallocation,
Zapmail's placement tests, EmailGuard's non-connected testing. Confirmed with each of them directly.

**So the automation has to live in our own system**, with vendors used as data sources where they permit it. Any
workflow we design must assume a human performs the final action.

---

# PART TWO — THE WORKFLOW

## 6. The tools, and what each is actually for

| Service | What it does for us | Automatable? | Verdict |
|---|---|---|---|
| **Smartlead** | Sends everything. **Smart Delivery is now our testing system** | Testing yes; swaps **UI-only** | Core |
| **Zapmail** | Sells and hosts domains and mailboxes; controls our DNS | **No** | Core, but the source of the fingerprint problem |
| **Google Postmaster** | Free. Google's own view of our spam-complaint rate | Yes, once verified | Worth doing — needs DNS work from Zapmail |
| **Microsoft SNDS** | Would be the Microsoft equivalent | — | **Not available** — we do not own our sending IPs |
| **EmailGuard** | Placement testing, DMARC reports | Yes | **Not proceeding** |
| **Aerosend** | Alternative domain supplier | Provisioning only | Worth a 2-domain test |

**Decision: Smart Delivery, not EmailGuard.** Every client is being moved onto Smart Delivery, so testing happens
where sending already happens. EmailGuard's one real advantage was per-provider testing without per-account
credits, which disappears once every account has credits. For the record, the rest of it did not apply:
domain masking turned out to mean showing a website in a browser, Spamhaus Intelligence returns a score rather
than a reason, and contact verification duplicates what we already do. The trial still earned its keep — it
produced the four-domain finding above.

## 7. The gap we are closing

**We can see what is dead. We cannot see what is dying.**

Everything we have is a snapshot. A placement test says a domain has *already* failed. What we lack is a trend —
a domain quietly worsening over weeks, which is the only warning that arrives in time to act on.

## 8. The proposed workflow

**Detect → Decide → Instruct → Replace.** A human does the final step because Smartlead requires it.

| Stage | What happens | Automated? |
|---|---|---|
| **Detect** | Placement test per domain on a schedule; bounce watched per mailbox; complaint-rate trend | Yes |
| **Decide** | Two consecutive failures = retire. One test is too noisy to act on | Yes |
| **Instruct** | Slack alert: which mailbox died, which spare replaces it, which campaign, the exact steps | Yes |
| **Replace** | Three clicks in Smartlead: remove old, add new, Reallocate Mailboxes | **Human — no API exists** |

### Testing cadence

Testing every mailbox is unaffordable — 558 mailboxes against a 300-test budget is one test every 56 days. **We
test per domain**, which is how placement actually behaves:

- Active domains: **every 14 days**
- Domains that just failed: **every 7 days**
- New domains: **once, before first use**
- Spare domains: **every 45 days**

### On rotation

We considered rotating mailboxes in and out weekly. **We are not doing that.** It strands leads mid-sequence, and
the evidence that a short rest repairs reputation is weak and mostly vendor-sourced. Instead:

- **Control volume continuously** — keep healthy mailboxes active with sensible daily caps
- **Rotate only at batch boundaries** — a lead batch finishes its sequence on one set of mailboxes
- **Shorten sequences** — complaints roughly triple by follow-up 4, and 58% of replies come from email 1
- **Swap only when a domain is genuinely dead**, because swapping has a real cost

## 9. Build status

| # | What | Status |
|---|---|---|
| 0 | Test what a prospect sees after a mid-sequence swap | Not started — 1 hour |
| 1 | Automatic placement testing per domain | **Already built and proven.** Needs credits |
| 1b | Spam-complaint trend tracking (Google Postmaster) | Blocked on Zapmail DNS |
| 2 | Two-strike retirement rule | To build |
| 3 | Test our 390 spare mailboxes | To build |
| 4 | Slack alert naming exactly what to swap | To build |
| 5 | Guardrails — open tracking, link rules | To build |
| 6 | Batch-boundary rotation, volume control | To build |
| 7 | Break the pattern on the next domain batch | Provisioning policy |

## 10. Already live

- **Bounce rate measured per mailbox** — it never was. Fleet is fine at 1.23%; caught one mailbox at 4.3% over
  925 sends
- **Bounce protection on all 21 active campaigns** — none had it, so one bad list could have burned a domain
- **Blacklist checker fixed** — it was silently reporting everything clean. First real scan: 0 of 121 on Spamhaus
- **Health scoring rebuilt** — it was awarding free points and trusting warmup scores we now know are meaningless
- **Per-provider test results** — our code was averaging Google and Microsoft into one number. A real test
  reading "54% inbox" was actually **0% at G Suite and 100% at Office365**. Now judged on the worst provider
- **API-key watchdog** — one client's key changed three times in a day and every job went blind silently

---

# PART THREE — WHAT WE NEED

## 11. Credit status, checked today

| Account | Credits | Evidence |
|---|---|---|
| Belardi Wong | **None** | Rejected. No test since 1 April |
| DARLEAN | **Yes** | Test today: 100% inbox, both providers |
| MYTHIC | **Yes** | Test today returned **no data** — see below |
| PRECISE_LEADS | **Exhausted** | Rejected. Had ~90 credits three weeks ago |

**The purchase covers Belardi Wong and Precise Leads.** Darlean and Mythic can begin automated testing today at
no cost.

### The Mythic result needs a second look

Mythic's test completed with **12 emails sent and none classified** — not spam, simply absent. Mail that lands in
spam still gets counted, so an empty result usually means the messages never arrived.

**We are not calling this broken yet.** Mythic's previous six tests were all 100% inbox, including two last week
with 88 and 67 emails. We ruled out the obvious causes — same campaign, same settings, same configuration as the
Darlean test that worked. One more test settles it.

## 12. Decisions needed

1. **Buy Smart Delivery credits for Belardi Wong and Precise Leads**
2. **Switch automated testing on for Darlean and Mythic now** — free, and gives us real evidence before the
   purchase
3. **Re-test Mythic** to establish whether the empty result is real
4. **Stop sending from reachbw.com and bwdirectmail.com?** 6 mailboxes with confirmed zero placement — hold
   until repeat tests settle the disagreement with Zapmail
5. **Confirm new domains shift toward Google-hosted**
6. **Approve a 2-domain trial with a second supplier** to test the fingerprint theory

## 13. What we need from Zapmail

- **Precise Leads account:** 31 domains still on Cloudflare, 12 with no working nameservers
- **Point DMARC reports somewhere we can read.** Every domain currently sends them to mailboxes that do not
  exist, so months of data has been discarded
- **Add Google Postmaster verification records** so trend tracking can run
- **Confirm the cost coverage** offered on Darlean domains, and whether it extends to Belardi Wong and Mythic

---

## 14. Why this makes us durable

The goal is not to repair today's bad domains. It is to make bad domains **cheap to spot and cheap to replace**.

1. **Measure from our own mailboxes**, so decisions reflect what clients actually experience
2. **Watch trends, not snapshots**, so we see decline instead of discovering death
3. **Require two failures before retiring**, so we do not discard working domains on noise
4. **Keep tested spares ready** — we have 390 idle mailboxes, we just do not know which are healthy
5. **Remove dead domains the same day**, not the same month
6. **Break the pattern on new domains**, so the fleet stops being one identifiable group

**Recovering burned domains is explicitly not the goal.** It is slow, unreliable, and mostly outside our control.
Protect, detect early, replace quickly.

---

## The numbers

- **558 mailboxes · 174 domains · 4 clients**
- **390 mailboxes idle** — the backup pool exists; we do not know which are healthy
- **321 leads mid-sequence** on Belardi Wong alone
- **Fleet bounce 1.23%** — healthy; one mailbox flagged at 4.3%
- **0 of 121 on Spamhaus DBL** — genuinely clean
- **99 of 121 flagged by SURBL** — infrastructure pattern, Zapmail addressing
- **68 of 97 domains on one shared IP** — the fingerprint
