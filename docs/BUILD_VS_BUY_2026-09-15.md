# Build vs buy: what Smartlead already does, and what we should keep

Date: 2026-09-15
Method: Smartlead's own API reference + helpcenter, live probes against the
BETTRDATA account, a codebase audit, and a 30-day community sweep.

Every claim below is tagged **VERIFIED** (we called it and saw the response),
**DOCUMENTED** (in Smartlead's docs, not yet called), **MARKETING** (their
site says so, nothing else confirms it), or **UNKNOWN**.

## The short version

Smartlead does more than we assumed. We wrote a weekly scheduler they already
ship, and we nearly wrote a provisioning pipeline they now ship too. What they
do *not* do is judge results the way our fleet needs them judged, and that is
the part worth keeping.

## 1. Native recurring placement tests — they have this, we rebuilt it

**VERIFIED.** `POST /spam-test/schedule` exists. Probing it with an empty body
walks a required-field chain that ends at a complete schema:

```
test_name, spam_filters, link_checker, campaign_id, sequence_mapping_id,
provider_ids, sender_accounts, every_days, schedule_start_time, test_end_date
```

`sender_accounts` is an array and `every_days` is the interval, so one
scheduled test can carry many senders on a weekly cadence — which is exactly
what `placement_executor.py` does with a cron entry and a wave throttle.

We stopped one field short of creating a live recurring test, deliberately.

**What this means.** Our Thursday cron is duplicated effort. Theirs handles
firing; ours also handles selection, rotation, copy refresh, coverage gating
and sheet writing. The firing half is the part we could drop.

**Recommendation:** evaluate replacing our cron with `/spam-test/schedule`,
keeping our collector. Do not rush it — our scheduler now encodes four hard-won
behaviors (wave throttling, coverage gating, copy refresh, rotation) and theirs
is unproven against the concurrency ceiling that broke our first batch.

## 2. Auto-Replace — promising, unverifiable, do not enable yet

**MARKETING.** Announced as: detect burning or expiring mailboxes → order a
replacement → warm it → swap it into the same campaigns → remove the old one.
It sits under "SmartSenders," their done-for-you mailbox product, provisioned
through partners (InboxKit, Infrainbox) rather than a registrar we choose.

**UNKNOWN, and each of these is a blocker:**

- What counts as "burning"? If it is a blended placement average, it misses our
  actual failure mode. `heybettrdata.com` read Inbox at Google and spam at
  Outlook for four consecutive weeks — a blend calls that healthy.
- What naming scheme do replacement domains use? Our rule is vocabulary-based
  names, never brand permutations; the July placement study flagged
  permutations specifically. A replacement called `bettrdata-7.com` would
  violate it.
- How long does it warm before sending? Our rule is 30 days of domain age
  before first cold send, 14 for a new inbox on an aged domain.
- Is there an API to configure it or read what it did? If not, it mutates the
  fleet while our Mongo schedule and the team's sheet still reference domains
  it retired.

**Community evidence: none.** A 30-day sweep across Reddit, X, YouTube, HN,
LinkedIn and TikTok surfaced zero operator discussion of Auto-Replace — only
vendor tutorials. Nobody has published an opinion on whether it can be trusted.
That is expected for a feature this new, and it is a reason to wait rather
than a reason to worry.

**Recommendation:** do not enable on a client account yet. Ask support the four
questions above. If the detection criteria turn out to be blended rather than
per-provider, our placement loop should drive replacement decisions and
Auto-Replace should stay off.

## 3. What Smartlead does well enough that we should not build it

| Capability | Status | Verdict |
|---|---|---|
| Warmup ramp + reputation scoring | DOCUMENTED | Use theirs. We already do. |
| Bounce auto-pause at a % threshold | DOCUMENTED (UI; API unclear) | Use theirs; ours adds per-client policy |
| Mailbox rotation across senders | DOCUMENTED | Use theirs |
| Reallocate leads off disconnected senders | DOCUMENTED (manual trigger) | Use theirs |
| ESP matching (`enable_ai_esp_matching`) | DOCUMENTED | Use theirs — a setting, not a build |
| Per-mailbox placement history | VERIFIED | Use theirs (`mailboxes-summary`) |
| Per-sender, per-seed placement detail | VERIFIED | Use theirs (`sender-account-wise`) |

`mailboxes-summary` deserves note: it returns each mailbox's placement rolled
up across every test, free and without a test id. Our `placement_summary.py` is
a thin wrapper over it, which is the right shape — read their data, apply our
judgment.

## 4. What we should keep, because Smartlead does not do it

- **Worst-provider judging.** Their reports give provider totals; nothing
  judges a domain on its weakest provider. This is the rule that catches a
  domain alive at Google and dead at Microsoft, and it is the single most
  valuable line of logic we have.
- **Coverage gating.** A test reports `COMPLETED` before its seeds are
  classified. Scoring that reads as 0% inbox — a total failure — for a healthy
  domain. Six domains were nearly written to the sheet as spam because of it.
  Nothing on their side prevents this.
- **Copy freshness.** Placement tests send whatever the test campaign holds.
  Ours held another client's copy, which scored a healthy domain 1 inbox / 7
  spam at Google versus 8 / 0 on its real copy. No vendor feature does this.
- **The deliverability grid.** The team works from a sheet with per-domain,
  per-week, per-provider history. No API replaces it.
- **Rotation across inboxes within a domain.** Ours tests a different inbox
  each week so the fleet is covered over time. Theirs rotates *sending*, not
  *testing*.
- **Status-column curation.** `replaced` / `prewarmed (cancel)` is a human
  decision with no vendor equivalent; skipping those halves the test spend.

## 5. What we built that we should reconsider

From the codebase audit: **four separate placement-testing paths** now exist —
`retest_executor` (connected SmartDelivery), `placement_executor` (batched),
`eg_test_executor` (EmailGuard), `nc_test_executor` (manual-assisted). All four
solve the same problem under different constraints, and all four are off.

Now that the loop closes and per-sender reporting is available, most of those
constraints are gone. Worth deciding which survive rather than maintaining all
four.

Also: `PLACEMENT_ENABLED` is read in `index.js` but never defined in
`config.py` like every sibling flag. Inconsistent, and easy to miss.

Doc sprawl is real too — eight `DELIVERABILITY*.md` files, several claiming to
supersede the others, none removed.

## 6. Open questions for Smartlead

Added to the support thread:

1. Auto-Replace detection criteria — per-provider or blended?
2. Replacement domain naming and registrar
3. Warmup duration before a replacement sends
4. API to configure Auto-Replace or read its actions
5. `/spam-test/schedule` — does it hit the same concurrency ceiling that
   stalled our 15-test batch?

## Bottom line

Use their infrastructure; keep our judgment. Smartlead is good at sending,
warming, rotating and measuring. It does not know that a domain reaching Google
but not Microsoft is half-dead, that a `COMPLETED` test with no classified
seeds is not a failure, or that the copy under test has to be the client's own.
That is the layer worth owning, and it is small.
