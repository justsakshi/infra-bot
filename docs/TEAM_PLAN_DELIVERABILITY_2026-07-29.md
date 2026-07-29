# Deliverability: What We Found, What We're Doing About It

**Date:** 2026-07-29 · **For:** the whole team · **Written to be read without any technical background**

---

## The short version

Our email settings are correct. Our domains are not blacklisted anywhere serious. Our bounce rate is healthy.
And yet a large part of our sending is landing in spam.

The reason is not something wrong with any individual domain. It is that **our domains look like a bulk
operation to spam filters**, and separately, **Microsoft has broken for cold email across the whole industry**.

Neither is fixed by tweaking a setting. This document explains what we found, what we are building, what we need
from other people, and which outside services are involved.

---

## 1. What we actually measured

We ran real placement tests — sending from our own production mailboxes to test addresses at Gmail and Outlook,
then recording where each one landed.

| Domain | Where the mailbox is hosted | Landed at Gmail | Landed at Outlook |
|---|---|---|---|
| heybelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — all spam |
| realbelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — all spam |
| reachbw.com | Microsoft 365 | 0 of 4 — all spam | 0 of 4 — all spam |
| bwdirectmail.com | Microsoft 365 | 0 of 4 — all spam | 0 of 4 — all spam |

Two things follow from this.

**Google-hosted mailboxes work at Google and fail at Microsoft.**

**Microsoft-hosted mailboxes fail everywhere** — including at Google. One of them had sent only 8 emails in its
entire life, so it cannot have earned a bad reputation through anything we did.

### This overturned a decision we were about to make

heybelardiwong.com and bwdirectmail.com were both on a list to be deleted, based on a manual test from July 9.
heybelardiwong is in fact one of our healthier domains. We would have thrown away something that works.

That July 9 test also turns out not to describe our real setup: it was sent from mailboxes that do not exist in
any of our Smartlead accounts. Two domains it scored 54% and 100% test identically today.

**Lesson: we act on measurements from our own production mailboxes, not on borrowed test data.**

---

## 2. Why it is happening

### Cause one: our domains look like one bulk operation

- **68 of 97 domains share a single IP address** (`52.15.49.97`) across all four clients
- They are named in an obvious pattern: `heybelardiwong`, `getbelardiwong`, `trybelardiwong`, `reachbw`, `sendbw`
- They were bought in bulk, at the same time, from the same supplier
- **99 of 121 were flagged by SURBL**, a blacklist that specifically looks for this kind of pattern

Zapmail confirmed the SURBL part was their infrastructure, not our sending, and have moved domains off Cloudflare
to fix it. But the wider point stands: a filter can see these domains are one group, so trouble on one can affect
all of them.

### Cause two: Microsoft has broken for cold email generally

This is not specific to us. Industry-wide, Outlook inbox placement has fallen sharply, and Microsoft now blocks
based on *sending patterns* rather than message content. That is why a domain with 8 lifetime sends and perfect
settings still lands in spam.

**There is no configuration that fixes this.** The realistic response is to rely less on Microsoft-hosted
mailboxes, not to keep debugging them.

---

## 3. What is already fixed and running

Built and live this week:

- **Bounce rate is now measured per mailbox.** It never was before — every inbox was silently assumed healthy.
  The fleet is fine at 1.2%, but it immediately caught one mailbox at 4.3% over 925 sends.
- **Bounce protection is on for all 21 active campaigns.** None had it before, meaning a single bad lead list
  could have destroyed a domain overnight with nothing to stop it.
- **The blacklist checker was broken** and quietly reporting everything as clean. Fixed. The first real scan
  showed 0 of 121 domains on Spamhaus, which is genuinely good news we could not previously see.
- **Health scoring was rebuilt.** It had been giving every inbox 20 free points and trusting warmup scores — and
  we now have proof warmup can read 100% while that same mailbox lands in spam at Microsoft.
- **An alert for broken API keys.** One client's key changed three times in a day and every automated job went
  blind without anyone noticing.

---

## 4. What we are building next

### The gap: we can see what is dead, not what is dying

Everything above is a snapshot. A placement test tells us a domain has *already* failed. What we do not have is
a trend — a domain slowly getting worse over weeks, which is the warning that actually gives us time to act.

The fix is to track **spam complaint rate per domain over time** and alert when it starts climbing.

### Build order

| # | What | Why it is in this position |
|---|---|---|
| **0** | **A one-hour test** of what a prospect sees when we move them to a new mailbox mid-sequence | Everything about replacing bad domains depends on this answer |
| **1** | **Automatic placement testing per domain**, on a schedule | Every decision downstream needs trustworthy detection |
| **1b** | **Spam-complaint trend tracking** via Google Postmaster Tools | The early warning we are missing. Gmail only — see the honest limit below |
| **2** | **Two-strike rule**: a domain must fail twice before we retire it | One test is too noisy to act on, as July 9 proved |
| **3** | **Test our spare mailboxes** before we need them | We have 390 idle spares but do not know which are healthy |
| **4** | **A Slack alert that tells someone exactly what to swap** | Replacement is manual, so make it a 60-second job |
| **5** | **Guardrails**: no open tracking without a custom domain, link rules | Cheap, and rules we already agreed but do not enforce |
| **6** | **Rotate at batch boundaries, control volume continuously** | The real long-term rotation mechanism |
| **7** | **Break the pattern on the next batch of domains** | Different suppliers, staggered dates, no obvious naming |

### An honest limit

We wanted the same complaint-rate tracking for Microsoft. **It is not available to us.** Microsoft's tool (SNDS)
only works if you own the IP address you send from, and we send through Google's and Microsoft's shared
infrastructure. The same applies to their complaint feedback programme.

So Microsoft stays partly blind to us. That is another reason to reduce how much we depend on it.

---

## 5. The thing we cannot automate

We asked Smartlead directly. Their answer:

- A mailbox is **locked to a lead for that lead's whole sequence**. Remove the mailbox and their follow-ups
  simply stop. Belardi Wong has 321 leads mid-sequence right now.
- There **is** a fix: a "Reallocate Mailboxes" option that moves those leads to a new mailbox and keeps their
  place in the sequence and their timing.
- **But it is manual, with no API.** They confirmed there is no way to automate it and no workaround.

So the workflow is: **the system detects the problem and tells a person exactly what to do; a person does a
three-click swap.** Our job is to make that human step rare, fast, and obvious — not to eliminate it.

**One caution.** Smartlead first said reallocation "maintains the email thread". When pressed, they narrowed that
to continuity "from the campaign perspective" and would not confirm the recipient sees one conversation. They did
confirm that **if a prospect replies to the old mailbox, that reply may not be tracked.** So swapping has a real
cost. We replace domains that are genuinely dead; we do not rotate on a schedule for its own sake.

Build #0 settles this by testing it ourselves.

---

## 6. Outside services, and what each is actually for

| Service | What it does for us | Verdict |
|---|---|---|
| **Smartlead** | Sends everything; where campaigns and mailboxes live | Core, no change |
| **Zapmail** | Sells and hosts our domains and mailboxes; controls our DNS | Core, but the source of the fingerprinting problem |
| **EmailGuard** | Placement testing split by provider; DMARC reports | Worth buying for testing only — see below |
| **Google Postmaster Tools** | Free. Google's own view of our spam-complaint rate | Yes, but needs DNS changes from Zapmail |
| **Microsoft SNDS / JMRP** | Would give the Microsoft equivalent | **Not available to us**, we do not own our IPs |

### On EmailGuard specifically

We trialled it, and it is what found the Microsoft problem. But several features we hoped for do not apply:

- **Domain masking** turned out to mean making a domain show a website in a browser. It does nothing for spam
  placement. This was one of the two features we were most interested in.
- **Spamhaus Intelligence** returns a score, not a reason. It will not explain *why* Microsoft rejects us.
- **Contact verification** duplicates what we already do.

**What is genuinely worth paying for is automated placement testing split by provider** — no other tool we have
shows Google versus Microsoft separately, and that split is what produced every useful finding this week.

At $49/month it covers 25 domains and we have 175, so this becomes a per-client purchase. Before committing, we
should compare its verdict against Smartlead's own Smart Delivery on the same domains — Anjali has that data.

---

## 7. What we need from other people

**Zapmail — Precise Leads account** (these cannot be seen from the Belardi Wong ticket):
- 31 domains are still on Cloudflare and not migrated
- 12 domains have no working nameservers at all
- Ask whether domains can be spread across more than one forwarding IP

**Zapmail — all accounts:**
- They ran their own placement test and reported our Microsoft mailboxes as fine. **We measured the opposite on
  the same domains the same day.** We have asked how their test sends, because if it bypasses Smartlead it is
  not measuring the path our clients' email actually takes. **This is the most important open question we have.**
- Balasankar was offered cost coverage on affected Darlean domains. Worth confirming that in writing and asking
  whether it extends to Belardi Wong and Mythic.

**Zapmail — DNS work needed for the build:**
- Point DMARC reports at an address we can read. Every domain currently sends them to mailboxes that do not
  exist, so months of data has been discarded.
- Add Google Postmaster verification records, so build #1b can run.

**Internal decisions:**
- Approve the EmailGuard purchase, or decide Smart Delivery is enough
- Decide whether to stop sending from reachbw.com and bwdirectmail.com — 6 mailboxes with confirmed zero
  placement, pending the Zapmail contradiction being resolved
- Confirm we are shifting new domain purchases toward Google-hosted rather than Microsoft-hosted

---

## 8. How this makes things durable

The point is not to fix today's bad domains. It is to make bad domains **cheap to detect and cheap to replace**,
permanently:

1. **We measure from our own mailboxes**, not from borrowed tests, so decisions rest on what clients actually
   experience.
2. **We watch trends, not just snapshots**, so we see a domain declining instead of discovering it is dead.
3. **We require two failures before retiring**, so we do not throw away working domains on noise.
4. **We keep tested spares ready**, so replacement takes minutes rather than a purchasing cycle.
5. **We stop the bleeding fast** — a dead domain leaving rotation the same day, not the same month.
6. **We break the pattern on new domains**, so the fleet stops being one identifiable group.

Recovery of a burned domain is explicitly not the goal. It is slow, unreliable, and largely outside our control.
Protect, detect early, replace quickly.

---

## Appendix: the numbers, for reference

- **558 mailboxes, 174 domains, 4 clients**
- **390 mailboxes are idle spares** — the backup pool already exists
- **321 leads are mid-sequence** on Belardi Wong alone
- **Fleet bounce rate 1.23%** — healthy; one mailbox at 4.3% flagged
- **0 of 121 domains on Spamhaus DBL** — genuinely clean
- **99 of 121 flagged by SURBL** — infrastructure pattern, being addressed by Zapmail
