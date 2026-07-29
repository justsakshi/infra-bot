# Smart Delivery: Credit Status and First Test Results

**Date:** 2026-07-29 · **For:** the team · **Context:** we are moving every client onto Smartlead Smart Delivery
as our single testing system

---

## Why we ran these tests

Smartlead's API has no way to ask "how many credits does this account have". The only way to find out is to try
creating a test: it either works, or it returns "Insufficient sequence credits".

So checking credits meant creating real tests. Two accounts had credits and two did not, which means two real
tests were spent finding this out.

---

## Credit status, all four accounts

| Account | Credits | How we know |
|---|---|---|
| **Belardi Wong** | **None** | Rejected: "Insufficient sequence credits". No test since 1 April |
| **DARLEAN** | **Yes** | Test created and completed today |
| **MYTHIC** | **Yes** | Test created and completed today |
| **PRECISE_LEADS** | **Exhausted** | Rejected. Had roughly 90 credits three weeks ago; the team has used them |

**The purchase covers Belardi Wong and Precise Leads**, not just Belardi Wong as we assumed. Precise Leads has
run dry since we last checked.

**Darlean and Mythic can start automated testing immediately** — no purchase and no new code needed. The testing
system already exists and is proven; it only needs switching on for those accounts.

---

## Test results

### DARLEAN — healthy

| Provider | Result |
|---|---|
| G Suite | 5 of 5 inbox |
| Office365 | 4 of 4 inbox |
| Spam | none |

Clean pass. Notably it reached **Office365 4 of 4**, where Belardi Wong's Microsoft-hosted domains scored 0 of 8.
That suggests the Microsoft problem is specific to Belardi Wong's domains rather than universal — a useful
narrowing, though it is one mailbox and Balasankar has separately found 7 of 27 Darlean inboxes landing in spam,
so Darlean is not uniformly healthy either.

### MYTHIC — 12 emails sent, none classified

The test completed but returned **no results at all**: 12 emails sent, zero recorded as inbox and zero as spam.

**What that could mean, honestly:** mail that lands in spam still gets counted. An empty result usually means the
messages were never delivered — rejected or dropped before arrival. Microsoft currently issues hard rejections
rather than spam-foldering, which would produce exactly this pattern.

**But we should not conclude that yet.** Mythic's six previous tests were all 100% inbox, including two on 22
July with 88 and 67 emails. This account has a strong recent record, and one empty test is thin evidence — the
same mistake that made the 9 July manual sweep misleading.

We checked the obvious explanations and ruled them out: the test used the same campaign and the same settings as
Mythic's successful 22 July test, and it used the same configuration as the Darlean test that worked.

**Next step: run one more Mythic test.** If it also returns nothing, something real is wrong with that account's
sending and Balasankar has concrete evidence to take to Zapmail. If it comes back normally, the first result was
a one-off.

---

## A fix this shipped along the way

Smart Delivery reports results per provider, but our code was **averaging them into a single number**. Checking a
real past test showed how misleading that is: a result reading **54% inbox blended** was actually **0% at G Suite
and 100% at Office365**.

A domain that reaches one provider but not the other is half-dead, not mediocre, and the two need opposite
responses. Averaging them made a working domain and a broken one look identical — which is how two healthy
Belardi Wong domains ended up on a retirement list.

**Now fixed:** every provider is recorded separately, and pass/fail is judged on the **worst** provider rather
than the average. Validated on today's live tests.

We also found our code records "everything went to spam" and "nothing was delivered" as the same failure. They
need different responses, so that distinction is being added.

---

## What this means for the plan

1. **Buy Smart Delivery credits for Belardi Wong and Precise Leads** — two accounts, not one.
2. **Switch automated testing on for Darlean and Mythic now.** Costs nothing, needs no build, and gives us a few
   days of real evidence before we commit to the purchase.
3. **Re-test Mythic** to establish whether the empty result is real.
4. The Zapmail disagreement about Belardi Wong's Microsoft domains **resolves itself once BW has credits** — we
   can run the repeat tests they asked for on our own budget.

---

## The numbers

- **558 mailboxes · 174 domains · 4 clients**
- **2 of 4 accounts have testing credits** (Darlean, Mythic)
- **Darlean: 100% inbox** across both providers
- **Mythic: 12 sent, 0 classified** — unexplained, needs a second test
- **Belardi Wong: no test possible since 1 April**
