# Questions for Smartlead support — SmartDelivery placement testing

Context for support: we run SmartDelivery placement tests (`/spam-test/manual`)
programmatically via the API to check inbox placement across our cold-email
fleet. This week we ran ~25 tests on account BETTRDATA and hit several
behaviors we can't find documented. Answers here directly decide how we
schedule and size our automated weekly test runs, so we'd rather ask than
guess.

## Test lifecycle / timing

1. We've observed every test we've created gets marked `status: COMPLETED`
   almost exactly 72 minutes after creation, regardless of how many seeds
   have been classified — sometimes with an empty `result` array at that
   point, and classifications continuing to arrive for days afterward. Is 72
   minutes a fixed window per test? Is it configurable, or tied to our
   plan/account tier?
2. When `status` is `COMPLETED` but the `result` array from
   `/spam-test/report/{id}/providerwise` is empty or covers only a fraction
   of `overallTotalCount`, does the report eventually become complete, or is
   that data permanently lost once the test closes?
3. Is there a way to query "how many seeds have been classified so far" more
   directly than diffing `overallTotalCount` against the sum of
   `adjusted_total_email_count` across `result` rows?

## Concurrency / throughput

4. Is there a limit on how many placement tests can run concurrently on one
   account, or one campaign, before seed delivery gets delayed or queued? We
   saw 1 test complete a full 15-seed panel in ~70 minutes, but a batch of 15
   concurrent tests (225 seeds total, one shared campaign) reach only ~60%
   combined coverage after 3 days, with 3 of the 15 tests receiving zero
   seeds the entire time.
5. Does seed delivery get throttled per sending campaign, per sender account,
   or per account-wide daily volume? If there's a recommended max concurrent
   test count, what is it?
6. In that same 15-test batch, Outlook/Office365 seeds consistently arrived
   before Google/G Suite seeds finished, and in a later smaller batch (3
   tests) the opposite happened — Google seeds arrived and Outlook seeds
   never did. Is there a deterministic send order between providers, or is it
   effectively random per test?

## `all_email_sent_without_time_gap`

7. With `all_email_sent_without_time_gap: true`, is `min_time_btwn_emails`
   fully ignored (we get a 400 if we also pass `min_time_unit`, so it seems
   incompatible outright)? Does "no gap" mean all seeds are dispatched
   essentially simultaneously, or is there still some minimum internal
   spacing?
8. Does sending all seeds without a time gap change how the receiving
   providers evaluate the test — i.e., does a burst of 15 emails in quick
   succession from one inbox risk a different (or less representative)
   placement result than a paced send would produce?
9. Is 5 minutes really the enforced minimum for `min_time_btwn_emails`, or
   can we go lower (e.g. 1–2 minutes) and still have a test complete within
   whatever the closing window is?

## Test creation requirements

10. `create_test` (`/spam-test/manual`) returns a 400
    `"Sender email accounts X not used in the campaign"` unless the sender is
    already attached to `campaign_id` as an email account. Is that expected
    and permanent, or could a future API version accept a sender not on the
    campaign?
11. Is there a way to run a placement test for a single sender's real,
    in-use sending sequence without attaching that sender to a separate
    test/staging campaign?

## Sequence write endpoint

12. `POST /campaigns/{id}/sequences` expects the key `seq_variants` on write,
    while `GET /campaigns/{id}/sequences` returns the same data under
    `sequence_variants`. Is that intentional, or a naming inconsistency that
    might get fixed (and break existing integrations) later?
13. When updating a sequence step's variants via that endpoint, is passing
    the step's `id` the correct/only way to update in place rather than
    creating a duplicate step? Is there a safer "patch this step's variants"
    endpoint we're missing?

## Credits / spend visibility

14. Is there any API endpoint to check remaining SmartDelivery test credits
    or overall account credit balance? We could only find the reactive path
    (a 402 or a credit-related message in the `create_test` error body after
    the fact).
15. What determines credit cost per test — is it a flat cost per test
    regardless of seed count/provider count, or does it scale with something
    (seed panel size, `provider_ids` selected, sender count)?

## Cancellation

16. Is there any way to stop, cancel, or delete a placement test after
    creation? We probed `POST /spam-test/{id}/stop`, `/cancel`, `/pause`,
    and `DELETE /spam-test/{id}` — all returned 404. Confirming there's
    genuinely no cancel path, since once a test is created we currently have
    no way to stop sends short of waiting out the ~72-minute window.

## Per-sender / per-domain reporting

17. When `sender_accounts` contains more than one address, is there any
    report endpoint that breaks results down per sender, rather than only
    the blended `/providerwise` totals? We probed several guessed paths
    (`senderwise`, `sender-wise`, `emailwise`, `mailboxwise`, `overall`) —
    all 404. If per-sender reporting doesn't exist, is one-sender-per-test
    the only way to get a domain-level verdict?
