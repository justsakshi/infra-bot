# Smartlead support — new thread, BettrData account (2026-09-17)

Self-contained. The earlier thread was raised on a different account
(`amanda@bettrdata.io`), so this assumes no shared history and restates the two
prior items that still block us.

Paste the block below.

---

Hi — we have two SmartDelivery placement tests on this account, run four hours
apart on the same mailbox fleet, that give opposite verdicts. We've had to make
a live change to campaigns based on the first one and we can't tell which test
to believe. Test IDs are `535093` (morning) and `535281` (afternoon).

| | 535093 | 535281 |
|---|---|---|
| senders | 44 | 38 |
| dispatched | 705 | 570 |
| classified | 514 | 280 |
| coverage | 73% | 49% |
| overall inbox | 51.4% | 88.1% |
| Office365 | 50.9% (116/228) | 100% (222/222) |
| G Suite | 51.7% (148/286) | 30.4% (14/46) |
| senders at 0% inbox | 15 | 0 |

Both report `status: COMPLETED`. The message copy differed between the two runs
(different campaign sequence) — we know that's a variable and we're not hiding
it, but we don't think copy alone explains what's below.

**1. Fifteen senders put every single seed in spam, then passed four hours
later.**

In 535093, fifteen senders scored exactly 0 inbox — not a low rate, zero — with
clean auth on every one (`spf_fail`, `dkim_fail`, `dmarc_fail` all 0):

```
aaron@bettrdatasgroup.com               0/15   spam=15
aaron@bettrdatastech.com                0/14   spam=14
aarond@bettrdatasgroup.com              0/14   spam=14
aarond@bettrdatastech.com               0/13   spam=13
jennifer.kramer@joinbettrdata.com       0/13   spam=13
jennifer@dataingesthq.com               0/14   spam=14
jenniferk@dataingesthq.com              0/14   spam=14
jenniferk@joinbettrdata.com             0/14   spam=14
laurie.donnelly@bettrdatasdesign.com    0/15   spam=15
laurie.donnelly@debettrdatas.com        0/14   spam=14
laurie@bettrdatasdesign.com             0/14   spam=14
laurie@debettrdatas.com                 0/15   spam=15
laurie@thebettrdatas.com                0/13   spam=13
lincoln@gohivecloud.co                  0/6    spam=6
sean@gohivecloud.co                     0/4    spam=4
```

In 535281 no sender had 0% inbox. Thirteen of the above were in that test and
averaged ~80%. For example `laurie@thebettrdatas.com` went 0/13 to 6/6, and
`aaron@bettrdatasgroup.com` went 0/15 to 7/8.

Can a per-sender 100%-spam result ever be a seed-panel or classification
artifact rather than a real placement measurement? A clean 0/N with passing
SPF/DKIM/DMARC looks to us more like a routing or scoring behaviour than like
fifteen mailboxes independently failing in the same instant.

**2. Sibling mailboxes on the same domain, opposite results in the same test.**

Within 535093 alone — same domain, same DNS, same run:

| domain | failing mailbox | healthy sibling |
|---|---|---|
| thebettrdatas.com | laurie@ 0/13 | laurie.d@ 12/13 |
| debettrdatas.com | laurie@ 0/15 | laurie.d@ 11/12 |
| bettrdatasdesign.com | laurie@ 0/14 | laurie.d@ 11/12 |
| bettrdatasgroup.com | aaron@ 0/15 | aaron.dix@ 13/13 |
| bettrdatastech.com | aaron@ 0/14 | aaron.dix@ 11/13 |
| dataingesthq.com | jennifer@ 0/14 | jennifer.kramer@ 12/14 |

The dotted local part survives and the bare/short variants fail, consistently
across six domains. Is there a per-mailbox (not per-domain) signal that would
drive this? Does the seed panel route or score differently per sender within a
single test? Is that local-part pattern meaningful to you, or coincidence?

**3. Coverage — do unclassified seeds inflate the inbox percentage?**

535281 dispatched 570 and classified 280 (49%), and is marked COMPLETED, so we
assume the other 290 aren't still in flight. The gap is provider-skewed: 222
Office365 seeds classified but only 46 G Suite, where 535093 classified 228 and
286 respectively.

This is our leading explanation for the contradiction, so we'd like it
confirmed or ruled out: if unclassified seeds are excluded from the denominator,
then a provider rejecting mail outright rather than spam-foldering it would
*raise* the reported inbox percentage. 535281's 88% would then be an artifact of
Google seeds going missing, not a good result.

- What happens to a dispatched seed that is never classified — dropped,
  rejected at SMTP, blackholed by the receiving provider, or simply not polled?
- Are unclassified seeds excluded from the percentage denominator?
- Can we retrieve the disposition of unclassified seeds?
- Is there a minimum coverage below which a report shouldn't be read as a
  verdict? We gate at 60% internally, which disqualifies 535281.

**4. Does a COMPLETED test keep re-scoring?**

We polled 535281 as coverage rose (16% → 29% → 47% → 49%) and overall inbox
moved 73.6% → 83.1% → 88.1% across those polls. Once a test reports COMPLETED,
is the report final, or do classifications keep arriving and shift the numbers?
If the percentage is always computed over currently-classified seeds, an early
read isn't just noisy but biased toward whichever provider responds fastest —
which in our data is consistently Office365.

**5. Can you tell whether either test was force-completed?**

This is the single answer that would resolve everything above. If 535281 was
closed out at 49% coverage with seeds still pending, we can discard it and act
on 535093. At the moment both tests look identical to us — `status: COMPLETED` —
and there's no field we can find that distinguishes a test that finished
naturally from one that was closed early. Is there such a field, and can you
check these two specific tests?

**6. Account-level check.**

All 21 sending domains on this account show as listed on SURBL (ABUSE) in our
own blacklist checks on 2026-09-07 and 2026-09-14 — including the domains whose
mailboxes tested healthy. Does SmartDelivery's seed classification factor in
sender-domain blocklist status, or is it purely the receiving provider's
verdict? Would a fully SURBL-listed fleet explain 535093's ~49% spam, and if so
how would 535281 show 88% inbox on those same domains four hours later? And do
you see anything else at the account or IP-pool level here that would affect
placement fleet-wide?

**7. Two API questions that bear on this.**

- **Per-sender-per-provider reporting.** `sender-account-wise` gives us
  per-sender placement and `providerwise` gives a test-wide provider split, but
  neither gives both. Our actual failure mode is a mailbox reaching one provider
  and not the other, which is exactly what we can't currently measure — and it
  would have answered question 1 outright, by showing whether those 15 senders
  failed at both providers or only one. Is there a way to get this?
- **`sender_credits`.** Where can we read this for an account, and what is it
  for BettrData? 535281 used 38 senders in a single test and we'd like to know
  whether we exceeded a per-test sender cap, and what happens when we do.

**What we've already ruled out:** authentication (spf/dkim/dmarc all pass on
every sender across both tests, ~740 seeds, including all 15 at 0%);
domain-level DNS (failing and healthy mailboxes share domains, per question 2);
send volume (all mailboxes at `message_per_day: 15`, daily sent 0–3 at test
time); and warmup reputation (no correlation — we have a 100%-reputation mailbox
at 0% inbox and a 60%-reputation mailbox at 100%, warmup ACTIVE on all).

The most useful answers for us are questions 1, 3 and 5. We've detached those 15
mailboxes from live campaigns as a precaution and can reattach them immediately
if this turns out to be a measurement artifact.

Thanks.

---

## Follow-up reply to paste into the same thread

One more data point that we think sharpens question 1, and a distribution
anomaly we can't explain.

**All seventeen 0%-senders recovered together.** Here is every sender that
scored 0% (or near it) in 535093, with its result in 535281 four hours later:

```
mailbox                                 535093        535281
aaron@bettrdatasgroup.com               0/15   (0%)   7/10   (70%)
aaron@bettrdatastech.com                0/14   (0%)   7/8    (88%)
aaron@ingestresolve.com                 1/16   (6%)   7/10   (70%)
aarond@bettrdatasgroup.com              0/14   (0%)   7/9    (78%)
aarond@bettrdatastech.com               0/13   (0%)   7/11   (64%)
jennifer.kramer@joinbettrdata.com       0/13   (0%)   7/10   (70%)
jennifer@dataingesthq.com               0/14   (0%)   6/11   (54%)
jenniferk@dataingesthq.com              0/14   (0%)   6/10   (60%)
jenniferk@joinbettrdata.com             0/14   (0%)   5/9    (56%)
laurie.donnelly@bettrdatasdesign.com    0/15   (0%)   7/9    (78%)
laurie.donnelly@debettrdatas.com        0/14   (0%)   7/12   (58%)
laurie.donnelly@thebettrdatas.com       1/16   (6%)   7/7    (100%)
laurie@bettrdatasdesign.com             0/14   (0%)   6/8    (75%)
laurie@debettrdatas.com                 0/15   (0%)   7/10   (70%)
laurie@thebettrdatas.com                0/13   (0%)   6/6    (100%)
lincoln@gohivecloud.co                  0/6    (0%)   1/1    (100%)
sean@gohivecloud.co                     0/4    (0%)   1/1    (100%)
                                 TOTAL  2/224  (1%)   101/142 (71%)
```

Seventeen mailboxes across seven domains went from 1% to 71% in four hours,
and *every single one* improved. Not a subset recovering — all of them. We
don't have a mechanism that would move seventeen independent mailboxes on
seven different domains in the same direction, in the same window, at once.
That is the main reason we suspect measurement rather than reputation.

Note also the seed counts: 224 classified for these senders in 535093 versus
142 in 535281, i.e. ~36% fewer seeds reaching a verdict for the same mailboxes.

**The distribution anomaly.** In 535281, 13 of 38 senders scored *exactly* 7
inbox:

```
535281 — inbox count distribution across 38 senders
  inbox= 1 ->  2 senders
  inbox= 3 ->  1
  inbox= 4 ->  2
  inbox= 5 ->  2
  inbox= 6 ->  4
  inbox= 7 -> 13   <-- 34% of all senders on one value
  inbox= 8 ->  4
  inbox= 9 ->  7
  inbox=10 ->  2
  inbox=12 ->  1

535093 — same view across 44 senders
  inbox= 0 -> 15   <-- the 0% cluster from question 1
  inbox= 1 ->  3
  inbox= 3 ->  1
  inbox= 5 ->  2
  inbox= 6 ->  2
  inbox= 7 ->  1
  inbox= 8 ->  2
  inbox=10 ->  4
  inbox=11 ->  5
  inbox=12 ->  2
  inbox=13 ->  3
  inbox=14 ->  4
```

To be clear, 7 is not a hard cap — 14 senders in 535281 exceeded it, up to 12.
But a third of the panel landing on exactly 7 is not a shape we'd expect from
independent per-seed classification, and the senders sitting on 7 are largely
the same ones that scored 0 in the earlier test. In 535093 the distribution is
smooth with no comparable spike.

Is there anything in how seeds are allocated or classified per sender that
would produce a cluster like that — a per-sender seed budget, a batch that
completes as a unit, or a partial-panel state? We ask because if the 7s
represent "the first 7 seeds resolved and the rest never did", then 535281 is
not measuring what we think it is, and question 3's denominator issue would
explain both the cluster and the apparent recovery.

---

## Support reply (2026-09-18) — verdict and what it changes

**Verdict: trust 535093, discard 535281.** Our Q3 mechanism was correct.

Findings from support:

- Both tests used the same 15-seed panel (8 Office365, 7 Google). Panel is not
  the variable.
- 535093 ran 183 min; 535281 was closed at **71 min**. Normal window is
  140–160 min. **Tests created through the API close on a shorter timer than
  the in-app flow.** Support says this is their bug and they are looking at it.
- Google seeds report slower than Office365. At 71 min only 46/266 Google
  seeds (17%) had returned in 535281, vs 286/308 (93%) in 535093.
- Unclassified seeds are excluded from the denominator. 535281's 88% is a
  population that was ~80% Office365 (which places at 100%). Nothing about the
  senders changed.
- Unclassified seeds were still in flight, not rejected or blackholed.
- The 0-inbox results in 535093 are real placement. 224 vs 142 classified for
  the same 17 senders explains the apparent "recovery"; the 13-senders-on-
  exactly-7 cluster is the partial panel resolving before close.
- Dotted-vs-bare local-part pattern does not hold (three dotted addresses also
  scored 0; each domain has three mailboxes and exactly one passed).
- No configuration difference across mailboxes. What lines up is the sending
  host: **13 of 15 zero-inbox senders are Google-hosted mailboxes.**

Not answered: Q6 (SURBL), Q7 (per-sender-per-provider, sender_credits).

### What this means for us

1. **Every placement test we have created via the API since 2026-07 has run
   on a ~71-minute window** and systematically under-sampled Google. The
   2026-09-11 batch that all closed at the identical millisecond, the
   Google-only / Office365-only panels on 09-14, the "72-minute close" we
   recorded as an API behaviour — all the same bug on their side. Our 60%
   coverage gate was the right defence and stays.
2. The 17 detached mailboxes are confirmed failing. Removals stand.
3. The health-history backfill from 535093 was correct.
4. The failure is concentrated in Google-hosted **sending** mailboxes (15 of 30
   fail, vs 2 of 27 Outlook-hosted). Support's phrase "Gmail placement problem"
   should be read as sending-host, not receiving provider — 535093 showed both
   receivers at ~51%, consistent with 15/44 senders dead at both.
5. Until their API timer is fixed, a test that must be trusted has to be
   created from the SmartDelivery UI. Automated weekly testing via
   `/spam-test/manual` cannot be relied on for Google verdicts.

### Follow-up sent

See next section.
