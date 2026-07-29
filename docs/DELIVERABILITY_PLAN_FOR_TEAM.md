# Deliverability: Everything We Know, and the Plan

**Date:** 2026-07-29 · **For:** the whole team · **Written to be readable without technical background**
**This is the single document to read.** Everything else in `docs/` is supporting detail.

---

## The three-sentence version

Our email settings are correct, our domains are clean on the serious blacklists, and our bounce rate is healthy —
yet a large share of our sending lands in spam. The cause is not any individual domain: it is that **our domains
look like one bulk operation** to spam filters, and separately that **Microsoft has broken for cold email across
the whole industry**. So the plan is not to repair domains, it is to **detect decline early, replace dead domains
fast, and stop buying domains that look like each other**.

---

## Part 1 — What we measured

We sent real test emails from our own production mailboxes to seed addresses at Gmail and Outlook, then recorded
where each landed. This is the first data we have from the mailboxes clients actually send from.

| Domain | Mailbox hosted on | Landed at Gmail | Landed at Outlook |
|---|---|---|---|
| heybelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — spam |
| realbelardiwong.com | Google Workspace | 4 of 4 inbox | 0 of 4 — spam |
| reachbw.com | Microsoft 365 | 0 of 4 — spam | 0 of 4 — spam |
| bwdirectmail.com | Microsoft 365 | 0 of 4 — spam | 0 of 4 — spam |

**Google-hosted mailboxes work at Google and fail at Microsoft. Microsoft-hosted mailboxes fail everywhere.**

One of the failing domains had sent 8 emails in its entire life, so it cannot have earned a bad reputation
through anything we did.

### This overturned a decision we were about to make

heybelardiwong.com and bwdirectmail.com were both on a list to be deleted, based on a manual test from 9 July.
heybelardiwong turns out to be one of our **healthier** domains. We were about to throw away something that works.

That 9 July test also does not describe our real setup — it was sent from mailboxes that exist in none of our
Smartlead accounts. Two domains it scored 54% and 100% test identically today.

**Rule going forward: we act on measurements taken from our own production mailboxes.**

---

## Part 2 — Why it is happening

### Cause 1: our domains look like one bulk operation

- **68 of 97 domains share a single IP address** (`52.15.49.97`), across all four clients
- Names follow an obvious pattern: `heybelardiwong`, `getbelardiwong`, `trybelardiwong`, `reachbw`, `sendbw`
- Bought in bulk, same time, same supplier
- **99 of 121 were flagged by SURBL**, a blacklist that looks for exactly this pattern

Zapmail confirmed the SURBL flagging was caused by their infrastructure rather than our sending, and have moved
domains off Cloudflare in response. The broader point stands: filters can see these domains are one group.

### Cause 2: Microsoft has broken for cold email generally

Industry-wide, Outlook inbox placement has fallen sharply, and Microsoft now filters on **sending patterns**
rather than message content. That is why a brand-new domain with perfect settings still lands in spam.

**No configuration fixes this.** The realistic response is to depend less on Microsoft-hosted mailboxes.

### What we ruled out, with evidence

| Suspected cause | Verdict |
|---|---|
| Wrong SPF / DKIM / DMARC | **Ruled out** — valid and aligned on all four domains, `p=reject` in place |
| Blacklisted domains | **Ruled out** — 0 of 121 on Spamhaus DBL, the list that matters |
| Bad sending reputation | **Ruled out** — fleet bounce 1.23%; a failing domain had 8 lifetime sends |
| Poor warmup | **Ruled out** — 100% warmup reputation on a mailbox landing 0/4 at Microsoft |

---

## Part 3 — What is already fixed and running

Built and live this week:

- **Bounce rate is now measured per mailbox.** It never was — every inbox was silently scored as healthy. The
  fleet is fine at 1.23%, but it immediately caught one mailbox at **4.3% over 925 sends**.
- **Bounce protection on all 21 active campaigns.** None had it before, so one bad lead list could have burned a
  domain overnight with nothing to stop it.
- **The blacklist checker was broken**, quietly reporting everything clean. Fixed. First real scan: 0 of 121 on
  Spamhaus — good news we previously could not see.
- **Health scoring rebuilt.** It had been awarding every inbox 20 free points and trusting warmup scores, which
  we now have proof are meaningless.
- **API-key watchdog.** One client's key changed three times in a day and every automated job went blind
  silently.
- **Alerts moved to Slack** — reply-rate decline and blacklist hits used to go to a log nobody read.

---

## Part 4 — The gap, and what we are building

### We can see what is dead. We cannot see what is dying.

Everything above is a snapshot. A placement test says a domain has *already* failed. What we lack is a **trend**
— a domain quietly worsening over weeks, which is the only warning that arrives in time to act on.

The fix is tracking **spam-complaint rate per domain over time**, and alerting when it climbs.

### Build order

| # | What | Status | Why here |
|---|---|---|---|
| **0** | **One-hour test**: what does a prospect see when we move them to a new mailbox mid-sequence? | Not started | Everything about replacing domains depends on this |
| **1** | **Automatic placement testing per domain** on a schedule, via Smart Delivery | **Code already built** (`retest_executor`) — needs credits on all 4 accounts, then switch on | Every decision downstream needs trustworthy detection |
| **1b** | **Spam-complaint trend tracking** via Google Postmaster Tools | Blocked on Zapmail DNS | The early warning we are missing |
| **2** | **Two-strike rule** — retire only after two consecutive failures | To build | One test is too noisy, as 9 July proved |
| **3** | **Test our 390 spare mailboxes** before we need them | To build | An untested spare is a guess, not a spare |
| **4** | **Slack alert naming exactly what to swap** | To build | Replacement is manual — make it a 60-second job |
| **5** | **Guardrails** — no open tracking without a custom domain, link rules | To build | Cheap, and rules we already agreed but do not enforce |
| **6** | **Rotate at batch boundaries, control volume continuously** | To build | The real long-term rotation mechanism |
| **7** | **Break the pattern on the next domain batch** | Provisioning policy | Different suppliers, staggered dates, no obvious naming |

**Good news on #1:** the automated testing system already exists and has been proven end to end — it creates
tests, polls for results, writes them to the database and the sheet, and feeds the campaign tool. It has been
switched off only because credits existed on one account. Once every client has Smart Delivery, it is a
configuration change rather than a build.

### Testing cadence

Testing every mailbox is impossible — 558 mailboxes against a 300-test monthly budget is one test every 56 days.
**We test per domain instead**, which is how placement actually behaves:

- Domains in active campaigns: **every 14 days**
- Domains that failed last time: **every 7 days**
- New domains: **once, before first use**
- Spare domains: **every 45 days**

### An honest limit

We wanted the same complaint-rate tracking for Microsoft. **It is not available to us.** Microsoft's tool only
works if you own the IP you send from, and we send through Google's and Microsoft's shared infrastructure. Same
for their complaint feedback programme.

Microsoft therefore stays partly blind to us — another reason to reduce our dependence on it.

---

## Part 5 — The thing we cannot automate

We asked Smartlead directly:

- A mailbox is **locked to a lead for that lead's whole sequence.** Remove it and their follow-ups stop. Belardi
  Wong has **321 leads mid-sequence** right now.
- There **is** a fix — a "Reallocate Mailboxes" option that moves those leads to a new mailbox and preserves
  their place in the sequence and their timing.
- **But it is manual, with no API**, and they confirmed there is no workaround.

So the workflow is: **the system detects and tells a person exactly what to do; a person does a three-click
swap.** Our job is to make that step rare, fast, and obvious — not to remove it.

**One caution.** Smartlead first said reallocation "maintains the email thread." Pressed, they narrowed that to
continuity "from the campaign perspective" and would not confirm the recipient sees one conversation. They did
confirm **a prospect replying to the old mailbox may not be tracked**. So swapping has a genuine cost: we replace
domains that are actually dead, we do not rotate on a schedule for its own sake. Build #0 settles this.

---

## Part 6 — The services, and what each is actually for

| Service | What it does for us | Can we automate it? | Verdict |
|---|---|---|---|
| **Smartlead** | Sends everything; campaigns and mailboxes live here. **Smart Delivery is now our testing layer** | Testing **yes**; mailbox swaps are **UI-only** | Core. Every client gets Smart Delivery credits |
| **Zapmail** | Sells and hosts domains and mailboxes; controls our DNS | **No** — tests are dashboard-only, no health API | Core, but the source of the fingerprint problem |
| **Google Postmaster** | Free. Google's own view of our complaint rate | Yes, once domains are verified | Yes — needs DNS work from Zapmail |
| **Microsoft SNDS / JMRP** | Would be the Microsoft equivalent | — | **Not available**, we do not own our IPs |
| **EmailGuard** | Placement testing; DMARC reports | Yes, full API | **Not proceeding** — see below |
| **Aerosend** | Alternative domain/mailbox supplier | Provisioning only, no deliverability data | Worth a 2-domain test to break the fingerprint |

### Decision: Smart Delivery, not EmailGuard

**Every client is being moved onto Smartlead Smart Delivery**, so testing happens where the sending already
happens. That removes the reason we were considering EmailGuard: its one real advantage was per-provider testing
without per-account credits, and once every account has credits that advantage disappears.

For the record, the rest of EmailGuard did not apply to us:
- **Domain masking** turned out to mean making a domain display a website in a browser — nothing to do with spam
  placement, and it was one of the two features we were most interested in
- **Spamhaus Intelligence** returns a score, not a reason, so it cannot explain *why* Microsoft rejects us
- **Contact verification** duplicates what we already do

The trial still earned its keep: it produced the four-domain finding that overturned our retirement list.

### A fix this decision forced, already shipped

Smart Delivery reports results per provider, but our code was **averaging them into one number**. Checking a real
past test proved how dangerous that is: a test that reads **54% inbox blended** is actually **0% at G Suite and
100% at Office365**. A domain that reaches one provider and not the other is half-dead, not mediocre — and the
two need opposite responses.

The parser now records every provider separately and **judges pass/fail on the worst one**. Interesting detail:
that Smart Delivery test failed at Google while our EmailGuard test on the same domain failed at Microsoft, which
tells us different seed lists probe different things — another reason to keep the breakdown rather than a single
score.

**A pattern worth naming:** nearly every deliverability capability our vendors sell is **UI-only** — Smartlead's
mailbox reallocation, Zapmail's placement tests, EmailGuard's non-connected mode. Confirmed with each directly.
The automation has to live in our own system, with vendors as data sources where permitted.

---

## Part 7 — What we need from other people

**Zapmail — Precise Leads account** (invisible from the Belardi Wong ticket):
- **31 domains still on Cloudflare**, not migrated
- **12 domains with no working nameservers at all**
- Can domains be spread across more than one forwarding IP?

**Zapmail — the open disagreement:**
They ran their own placement test and reported our Microsoft mailboxes as fine. **We measured the opposite on the
same domains the same day.** They asked us to test repeatedly over several days rather than rely on one snapshot,
and offered to investigate if the pattern holds. That is a fair request, and **moving every client onto Smart
Delivery resolves it for free** — we can run those four domains daily on our own credits and send them the
series. Until that is done we should not pause those mailboxes on the strength of one disputed test.

**Zapmail — DNS work the build depends on:**
- Point DMARC reports at an address we can read. Every domain currently sends them to mailboxes that do not
  exist, so months of data has been discarded.
- Add Google Postmaster verification records so the trend tracking can run.

**Commercial:** Balasankar was offered cost coverage on affected Darlean domains. Worth confirming in writing and
asking whether it extends to Belardi Wong and Mythic.

**Internal decisions needed:**
1. ~~Approve EmailGuard~~ — **decided: Smart Delivery on every client instead**
2. Stop sending from reachbw.com and bwdirectmail.com? (6 mailboxes, confirmed zero placement — hold until the
   repeat tests settle the Zapmail disagreement)
3. Confirm new domain purchases shift toward Google-hosted
4. Approve a 2-domain trial with a second supplier to test the fingerprint theory

---

## Part 8 — How this makes us durable

The goal is not to repair today's bad domains. It is to make bad domains **cheap to spot and cheap to replace**,
permanently.

1. **Measure from our own mailboxes**, so decisions reflect what clients actually experience
2. **Watch trends, not snapshots**, so we see decline instead of discovering death
3. **Require two failures before retiring**, so we do not discard working domains on noise
4. **Keep tested spares ready**, so replacement takes minutes rather than a purchase cycle
5. **Remove dead domains the same day**, not the same month
6. **Break the pattern on new domains**, so the fleet stops being one identifiable group

**Recovering burned domains is explicitly not the goal.** It is slow, unreliable, and mostly outside our control.
Protect, detect early, replace quickly.

---

## The numbers, for reference

- **558 mailboxes · 174 domains · 4 clients**
- **390 mailboxes are idle spares** — the backup pool already exists; we just do not know which are healthy
- **321 leads mid-sequence** on Belardi Wong alone
- **Fleet bounce 1.23%** — healthy; one mailbox flagged at 4.3%
- **0 of 121 domains on Spamhaus DBL** — genuinely clean
- **99 of 121 flagged by SURBL** — an infrastructure pattern, being addressed by Zapmail
- **68 of 97 domains on one shared IP** — the fingerprint
