# Inbox Protection & Replacement Workflow — Plan

**Written:** 2026-07-28 · **Status:** plan only, nothing built (per Avinash: "come with a plan before you build it")
**Spec source:** PL standup 2026-07-28 · **Evidence:** our own fleet data + placement tests + external research

---

## 0. The one finding that reshapes the whole design

**Smartlead assigns a sender to a lead once, at campaign entry, and keeps it for the entire sequence.**

Verified two ways:
- **Our data:** 6 leads sampled on a 4-step BW campaign, 3 sends each, **one sender throughout every time**.
- **Smartlead's own docs:** removing a mailbox mid-campaign leaves already-touched leads "in pending" with no
  further sends. Only leads that have not yet received email 1 get redistributed.
  ([article 101](https://helpcenter.smartlead.ai/en/articles/101-what-happens-to-the-sequence-if-we-assign-an-email-address-and-then-remove-that-email-address-from-the-campaign),
  [article 102](https://helpcenter.smartlead.ai/en/articles/102-what-happens-if-i-disconnectremove-a-mailbox-mid-campaign))

**CONFIRMED BY SMARTLEAD SUPPORT (2026-07-28)** — their exact answer:

> "When a sender mailbox picks up a lead from a campaign with multiple steps, the sender mailbox remains
> assigned to that lead for the entire sequence... If Mailbox A is removed from the campaign after sending the
> first email, the remaining follow-ups will not be sent. The lead will not automatically switch to another
> mailbox."

So removing a mailbox does **not** cause follow-ups to fire from a different sender on a broken thread. They
simply stop. Belardi Wong has **321 leads mid-sequence** today; pulling their senders would freeze all 321.

**BUT there is a supported way through, which we did not know about:** the campaign menu has a
**"Reallocate Mailboxes"** function (three dots → Reallocate Mailboxes). Per support:

> "The affected leads will be assigned to the new mailbox. The remaining follow-up emails will continue in the
> same existing email thread. The sequence will not restart from Email 1."

This changes the design substantially — rotation IS possible without losing leads or breaking threads.

**The catch: it is UI-only, confirmed explicitly by Smartlead support (2026-07-28):**

> "Currently, mailbox reallocation is only available through the Smartlead UI. There is no supported API
> endpoint available to perform lead reallocation or move leads from one sender mailbox to another
> programmatically... There is no supported API alternative to achieve the same behavior without using the UI
> flow."

We had already verified this independently: no reallocate endpoint exists, and `POST /campaigns/{id}/leads/{id}`
explicitly rejects `email_account_id` with "not allowed". Support also confirmed the `resume` endpoint performs
no sender reassignment. They noted the use case is valid and "can be considered for future improvements", so
this may change, but we must design for manual today.

**One useful detail they confirmed:** for leads already waiting between sequence steps, "the existing sequence
timing will continue after reallocation and will not restart from the beginning." So reallocation is genuinely
lossless — thread, sequence position, and timing all survive.

**The supported swap procedure (manual, 3 steps):**
1. Remove/disconnect the old mailbox from the campaign
2. Add the replacement mailbox to the campaign
3. Campaign menu (three dots) → Reallocate Mailboxes → map old sender → new sender

**Design consequence — this is the hinge of the whole workflow.** Rotation cannot be automated end to end. The
bot detects, decides, and hands a human a precise instruction; the human performs a 3-step UI action. Every
build item below is shaped by that constraint: the goal is to make the human step rare, fast, and unambiguous,
not to eliminate it.

---

## 1. What we actually have (measured, 2026-07-28)

| Client | Inboxes | In campaigns | **Bench (idle)** | Domains |
|---|---|---|---|---|
| Belardi Wong | 66 | 34 | **32** | 22 |
| DARLEAN | 108 | 69 | **39** | 36 |
| MYTHIC | 42 | 12 | **30** | 15 |
| PRECISE_LEADS | 342 | 53 | **289** | 101 |
| **Total** | **558** | **168** | **390** | **174** |

**The backup pool Avinash asked for already exists** — 390 idle inboxes, 70% of the fleet. The gap is not
supply. It is that we do not know which spares are healthy, and we have no mechanism to swap one in.

---

## 2. The workflow, stage by stage

Avinash's six stages, with what is real, what is measured, and what is missing.

### Stage 1 — Provision (land in inbox from day one)

Research consensus for 2026: domain aged 2-4 weeks before first send (90+ days for Microsoft), warmup 4-6 weeks
ramping 5-10/day to 25-40/day, **2-3 mailboxes per domain**, 15-25 sends/inbox/day steady state.

We are at **3 mailboxes/domain** — correct. Our provider caps are Outlook 10 / Gmail 25 — inside the band.

**Change needed:** stop buying every domain from one reseller stack. 68 of 97 domains share one A-record IP,
and the naming is sequential brand-permutation (`heybelardiwong`, `getbelardiwong`, `trybelardiwong`). That is
the textbook fingerprint of a bulk cold-email operation. Spread registrars, DNS providers, registration dates,
and stop the permutation naming on the next batch.

### Stage 2 — Protect

| Lever | Status | Evidence |
|---|---|---|
| Volume caps per inbox | **Live** (Outlook 10 / Gmail 25 / SMTP 15, enforced in code) | inside 2026 consensus band |
| Bounce auto-pause at 3% | **Live on all 21 campaigns** as of 2026-07-27 | none had it before |
| Per-inbox bounce monitoring | **Live** — fleet at 1.23% | caught one inbox at 4.3% over 925 sends |
| Open tracking off unless custom tracking domain | **Rule agreed, not enforced in code** | Avinash @0:36; ~15% higher spam-flag risk |
| Warmup always on | Live | but see the warning in §4 |
| Domain masking / link proxy | **Not used** | only helps if the LINK domain is blacklisted, not the sender |

### Stage 3 — Rotate (redesigned — see §3)

### Stage 4 — Detect

Placement testing is the only ground truth we have. Everything else proved to be a weak predictor:
one mailbox had **125 warmup sends, 125 inboxed, 100% reputation — and Microsoft spam-foldered 4/4 real seeds**.

Cadence, sized against a 300-test/month budget and 174 domains: testing every inbox is impossible
(558 inboxes = one test every 56 days, useless). **Test per domain, not per inbox** — placement is
domain-level, and all four of our tests behaved that way.

| Tier | Cadence | Tests/month |
|---|---|---|
| Domains in active campaigns | every 14 days | ~150 |
| Domains that failed last test | every 7 days | ~40 |
| New domains, before first campaign use | once, on entry | ~20 |
| Bench domains | every 45 days | ~60 |
| Reserve for incidents | — | ~30 |

That fits 300/month with headroom and matches Avinash's "once or twice a week, only what's in use."

### Stage 5 — Retire & replace

Trigger: a domain fails placement (<80% inbox) **twice in a row**, or any inbox exceeds 3% bounce.
One failure alone is not enough — our own data shows single tests are noisy (Anjali's July-9 sweep rated two
domains 54% and 100%; both test identically today).

**Retire, do not rehabilitate.** Avinash's call, and the research agrees: recovery is slow (2-4 weeks best case,
8-16 weeks if severe), unreliable, and not in our vendors' hands. EmailGuard's own support said recovery is out
of their scope; their only advice is rest and slow ramp.

### Stage 6 — Backup pool

Already exists (390 inboxes). What is missing is knowing which are healthy — hence "test bench domains every
45 days" above. A spare that has never been tested is not a spare, it is a guess.

---

## 3. Rotation, redesigned

**Rejected: weekly in/out rotation of inboxes.** It strands mid-sequence leads (321 today on BW alone), and the
evidence that short "rest" periods repair reputation is weak — reputation signals take weeks-to-months to decay,
so a one-week rest breaks no pattern and repairs nothing. The claim that resting restores placement comes mostly
from vendor blogs with no published methodology.

**Rejected: duplicate campaigns per inbox set.** Avinash already called this out as error-prone. Agreed.

**Adopted: rotate at the cohort boundary, throttle volume continuously.**

1. **Volume is the primary lever, not rotation.** Keep every healthy inbox continuously active and control
   sends/inbox/day. This is what Smartlead's architecture natively supports (weighted round-robin at lead entry),
   and it never strands anyone. Varsha's blocker on the call — "we have two working domains but they're in a
   running campaign" — is exactly this: volume per campaign per inbox is already controllable and we are not
   using it.
2. **Rotation happens between cohorts, never mid-sequence.** A lead batch runs its full sequence on one sender
   pool. The next batch gets a different pool. Nobody is ever pulled mid-flight.
3. **Shorter sequences shrink the exposure window.** Spam-complaint rate roughly triples by follow-up #4, and
   58% of replies come from email 1. Moving from 4 steps to 2-3 both improves deliverability and makes cohort
   rotation land more often. Our BW campaigns are at 4 steps.
4. **Emergency swap uses Reallocate Mailboxes, and it is a human step.** If a domain is confirmed dead, the
   sequence is: add the replacement mailbox to the campaign → remove the dead one → three dots → Reallocate
   Mailboxes → map old sender to new. Threads and sequence position survive. Because there is no API for it,
   the bot cannot do this. What the bot CAN do is post a Slack message naming the dead mailbox, the recommended
   replacement from the bench pool, and the affected campaign, so the human action is one click and no thinking.

   Note: the existing `resume_lead` path in our rotation executor does **not** reassign senders (support
   confirmed the resume endpoint has no reassignment behaviour). That code should be re-pointed at the
   reallocation instruction flow rather than attempting an automated fix that does not work.

---

## 4. Open risks worth naming

**Warmup may be doing nothing useful.** 100% warmup reputation coexists with 0/4 Microsoft placement on the same
mailbox. Warmup measures a friendly network's acceptance, not the open internet's. Some practitioners now argue
warmup networks are detectable and actively harmful. We should not treat warmup reputation as a health signal
(already reweighted from 25 to 10 points), and we should be open to testing a warmup-off cohort.

**Microsoft is failing industry-wide, not just for us.** Outlook inbox placement reportedly fell from ~49% to
~27% year-over-year, and Microsoft blocked 3M+ outbound accounts in Q1 2026 on *sending patterns* rather than
content. Our measurement — M365-hosted domains scoring 0/8 with clean auth and 8 lifetime sends — matches this
exactly. **There is no configuration fix.** Treat M365 domains as high-risk and de-weight them until this
stabilises. This supports Avinash's push to test alternative providers.

**Dedicated IP is not automatically better.** New dedicated IPs start with *lower* trust than Google's pools, and
single-IP providers reintroduce the same concentration risk we have now, just self-owned. Worth testing on 2
domains, not worth migrating to wholesale.

---

## 4b. A note on the quality of "best practice" numbers

Much of the published 2026 cold-email guidance is vendor SEO content, not practitioner writing. Reddit and the
real community threads were not reachable during this research. So the numbers below carry different weights:

**Trust (our own measurement):** sender stickiness, 321 in-flight leads, 390 bench inboxes, per-inbox bounce
rates, all four placement results, 68/97 shared IP, warmup-vs-placement divergence.

**Trust (official vendor docs):** Smartlead's mailbox-removal behaviour, Google/Microsoft sender requirements.

**Directional only (repeated across sources but unverified):** 2-3 mailboxes/domain, 20-50 sends/day ceilings,
domain-age and warmup-duration guidance, Microsoft's Q1 2026 enforcement figures, domain replacement costs.

**Genuinely contested:** whether continuous warmup helps or hurts. One camp says it is hygiene; the other says
warmup networks are detectable and mailboxes get penalised for exchanging mail with flagged peers. Our own data
leans toward "warmup is not measuring what we think" — 100% reputation alongside 0/4 Microsoft placement.

The one identifiable practitioner source (an operator running a 30-person agency) argued that **list quality and
targeting research matter more than copy, volume, or infrastructure** — "good deliverability doesn't guarantee
success but bad deliverability guarantees failure." That matches what our bounce data shows.

---

## 5. What to build, in order

| # | Build | Why first | Depends on |
|---|---|---|---|
| 1 | **Per-domain placement test scheduler** on the tiered cadence in §4 | Detection is the whole workflow's input; everything else is guesswork without it | EmailGuard paid plan |
| 2 | **Two-strike retirement rule** + Slack alert naming the domain and its replacement | Turns detection into a decision; single tests are too noisy to act on | #1 |
| 3 | **Bench health tracking** — test spares before they are needed | A never-tested spare is not a spare | #1 |
| 4 | **Slack swap instruction** — names the dead mailbox, the recommended bench replacement, and the campaign, with the Reallocate Mailboxes steps inline | Reallocation is UI-only, so the bot's job is to make the human step instant and unambiguous rather than to automate it | #2, #3 |
| 5 | **Open-tracking guard** — flag any campaign with tracking on and no custom tracking domain | Cheap, and it is already a stated rule being violated | none |
| 6 | **Cohort-aware campaign builder** | The real rotation mechanism; largest build, needs #1-4 proven first | all above |

---

## 5b. The swap alert — exact shape

Because the human does the swap, the alert has to carry everything needed to act without investigation. Target:
under 60 seconds from reading to done.

```
🔴 SWAP NEEDED — Belardi Wong

Dead:        sam@reachbw.com  (0/8 inbox, 2 consecutive failed tests)
Campaign:    BW Webinar - August 13, 2026   (4 steps, 162 leads mid-sequence)
Replace with: sam@teambelardiwong.com
             (bench, last tested 2026-07-20: 8/8 inbox, warmup 100%, 0% bounce)

Do this:
1. Campaign → Email Accounts → remove sam@reachbw.com
2. Add sam@teambelardiwong.com
3. Three dots → Reallocate Mailboxes → sam@reachbw.com → sam@teambelardiwong.com

Threads and sequence timing are preserved. 162 leads will resume on the new sender.
[ Mark done ]  [ Not now ]
```

Everything in that message comes from data we already collect. The only thing missing today is the bench
recommendation, which needs bench domains to have been tested (build item #3).

---

## 6. Answers to the standup's open questions

1. **Placement cadence others run:** once or twice a week on in-use domains is the consensus; daily is neither
   affordable nor useful.
2. **EmailGuard vs Smart Delivery:** EmailGuard's advantage is not test quality, it is that one workspace covers
   all clients via API with no per-account credits and no manual step. Its per-provider split is what found the
   Microsoft problem.
3. **Does masking/proxy extend inbox life?** Only for link-domain reputation. Our sending domains are the problem,
   so it would not have helped here. Low priority.
4. **Zapmail DNS migration + SURBL:** they confirmed SURBL re-fingerprinted and caught all Cloudflare-DNS domains
   industry-wide; they have moved to Google DNS. Timeline and whether delisting is automatic are still unanswered.
5. **Can Zapmail spread our IPs?** Asked, unanswered. 68/97 on one IP is the specific concern.
6. **Aerosend / private infra:** worth a 2-domain test, not a migration. See §4.
7. **The big one — rotation vs sequence continuity:** answered in §3. Cohort-level rotation plus volume
   throttling. Mid-sequence rotation is not possible on Smartlead without stranding leads.

---

## 7. What is already live (built 2026-07-27/28)

- Per-inbox bounce rate, scored and alarmed at 3% (fleet 1.23%, one inbox at 4.3% flagged)
- Bounce auto-pause on all 21 active campaigns — none had it before
- Working Spamhaus DBL checking (was silently reporting everything clean; 0/121 listed on the first real scan)
- Health score reweighted onto signals that actually vary
- API-key watchdog (keys rotated 3× in one day and every job went blind silently)
- Reply-rate decay and DBL alerts routed to Slack instead of unread logs
- EmailGuard placement testing integrated end to end, dry-run by default
