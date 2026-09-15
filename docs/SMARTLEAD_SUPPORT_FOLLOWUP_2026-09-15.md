# Follow-up to Smartlead support — SmartDelivery placement testing

Thanks — that was genuinely useful, and several answers corrected assumptions
we'd baked into our integration. We've already changed our code on the back of
it: full-array sequence replacement, the `PUT .../stop` route (verified — it
returns `400 "Test is not in progress"` on a completed test, so the route is
clearly right and our 404s were just the wrong verb), flat 1-credit billing,
and we've reverted `all_email_sent_without_time_gap` to `false` by default
until we can run the controlled comparison ourselves.

Below is the test data you asked for, plus the follow-ups.

---

## 1. The batch of 15 concurrent tests

Account: **BETTRDATA**. All 15 created through campaign **3752513**
("Deliverability Test Campaign", PAUSED), sequence_mapping_id **10028994**,
one sender each, `provider_ids: [20, 21]`, `min_time_btwn_emails: 5`,
`all_email_sent_without_time_gap: false`, `is_warmup: true`.

Created 2026-09-11 at ~09:29:48 UTC (all within ~250ms of each other).
Seed counts below are as of 2026-09-15, four days later.

| spamTestId | sending domain | classified | created (UTC) | updated (UTC) |
|---|---|---|---|---|
| 532017 | debettrdatas.com | 13/15 | 09:29:48.802 | 10:41:23.064 |
| 532018 | useatlasstack.co | 13/15 | 09:29:48.809 | 10:41:23.064 |
| 532019 | bettrdatasgroup.com | 14/15 | 09:29:48.811 | 10:41:23.064 |
| 532020 | askbettrdata.com | 14/15 | 09:29:48.811 | 10:41:23.064 |
| **532021** | **openbettrdata.com** | **0/15** | 09:29:48.816 | 10:41:23.064 |
| **532022** | **myscalegrid.co** | **0/15** | 09:29:48.823 | 10:41:23.064 |
| 532023 | ingestresolve.com | 11/15 | 09:29:48.829 | 10:41:23.064 |
| 532024 | bestbettrdata.com | 7/15 | 09:29:48.813 | 10:41:23.064 |
| **532025** | **bettrdatas.com** | **0/15** | 09:29:48.845 | 10:41:23.064 |
| 532026 | bettrdatasdesign.com | 12/15 | 09:29:48.883 | 10:41:23.064 |
| 532027 | gohivecloud.co | 3/15 | 09:29:48.925 | 10:41:23.064 |
| 532028 | thebettrdatas.com | 13/15 | 09:29:48.949 | 10:41:23.064 |
| 532029 | dataingesthq.com | 11/15 | 09:29:48.953 | 10:41:23.064 |
| 532030 | mymetriccloud.co | 12/15 | 09:29:48.980 | 10:41:23.064 |
| 532031 | bettrdatastech.com | 12/15 | 09:29:49.046 | 10:41:23.064 |

**The thing we'd most like explained:** every one of the 15 has an identical
`updated_at` of `10:41:23.064Z` — the same millisecond — including the three
that had received zero seeds at that point and still have zero four days
later. That looks like a single bulk operation closing all 15 at once, not 15
independent "all triggers fired" completions. Is that the internal manual-test
limit force-completing them? If so, what is the limit, and is there any signal
in the API response we could read to distinguish "completed normally" from
"force-completed while seeds were still pending"? Right now both look
identical to us (`status: COMPLETED`), which is what caused us to nearly write
false spam verdicts into our reporting.

Related: **532021, 532022, 532025** never received a single seed. The sending
inboxes were healthy throughout (SMTP + IMAP connected, warmup active, 15/day
cap, warmup reputation 86–100%). Were sends ever actually triggered for these
three?

## 2. Late-arriving classifications — confirmed, with timings

You asked whether late data keeps flowing. It does, but very slowly, and it
plateaus well short of complete. Combined across those 15 tests:

- ~2 hours after creation: 67 of 225 seeds classified (30%)
- ~5 hours: 81 / 225 (36%)
- 3 days: 135 / 225 (60%)
- 4 days: no further movement; three tests still at 0

So no data is discarded, but in practice a large concurrent batch never
reaches a full seed panel. Is that consistent with what you'd expect, or does
it point at the same limit as above?

## 3. A second, smaller batch showing the inverse provider pattern

Three tests, same campaign 3752513, created 2026-09-14 ~05:59 UTC, one sender
each:

| spamTestId | sending domain | Google | Office365 |
|---|---|---|---|
| 533310 | bettrdatasgroup.com | 8 inbox / 0 spam | **0 seeds** |
| 533311 | bettrdatas.com | 3 inbox / 0 spam | **0 seeds** |
| 533312 | openbettrdata.com | 7 inbox / 0 spam | **0 seeds** |

All three completed with Google seeds only and zero Office365 seeds, four days
on. In the 15-test batch the imbalance ran the other way (Office365 arriving
first, Google lagging). If there's no intentional provider ordering, is it
expected that a test can end up with an entirely single-provider panel? That
matters a lot to us: our placement verdict is judged on the *worst* provider,
so a test that silently returns one provider only isn't a usable result.

## 4. One test that received nothing at all

**533416** — same campaign, created 2026-09-14T09:54:24Z, sender
`jennifer.kramer@bettrdatas.com`, the only test we've run with
`all_email_sent_without_time_gap: true`. It shows `COMPLETED` with 0/15 seeds
classified and has not moved since. Since this was our only no-gap test, we
can't tell whether the no-gap path itself failed to dispatch or whether it hit
the same issue as 532021/532022/532025. Could you check whether sends were
triggered for this one?

## 5. Remaining questions

1. **The manual-test limit.** Can you tell us the actual number, even
   approximately? We currently throttle ourselves to 5 concurrent tests per
   client as a guess. If the real limit is 10 we're leaving throughput on the
   table; if it's 3 we're still over it. A documented number would let us size
   this properly rather than tune by trial and error.
2. **Force-completion signal.** Per above — any field that distinguishes a
   force-completed test from a naturally completed one? Without it, our only
   defence is comparing `overallTotalCount` against classified seeds and
   refusing to record anything below a coverage threshold.
3. **Sender credits.** You mentioned `sender_credits` caps senders per test.
   Where can we see that number for an account, and what is it for BETTRDATA?
4. **Per-sender reporting** — noted you're checking with the product team. For
   planning: if it doesn't exist, we'll stay on one-test-per-domain, which
   costs us one credit per domain per week rather than one per batch. Any
   sense of whether that's on the roadmap would help us decide whether to
   design around it permanently.
5. **Credit balance endpoint.** Also noted as dashboard-only. We'd like to
   register this as a feature request: we run these tests unattended on a
   schedule, and without a balance check the only way we learn we're out of
   credits is a failed test creation mid-run. Even a read-only balance field
   would let us stop cleanly instead.
6. **Smart Agent insights** — yes please, log that as a feature request. Being
   able to pull Campaign/Domain/Mailbox Health programmatically would replace
   a meaningful amount of what we've built ourselves.

## 6. One thing we found that may be worth flagging internally

Because a test can report `COMPLETED` with an empty or partial `result` array,
a naive integration computes 0% inbox placement and reads it as a total
placement failure. We hit exactly that: six healthy domains were about to be
recorded as spam-foldered purely because their seeds hadn't been classified
yet. We've since added a coverage gate, but anyone computing placement
directly from `/providerwise` on `status == COMPLETED` would silently get
false failures. A documented note, or a `classified_count` field, would save
others from the same trap.
