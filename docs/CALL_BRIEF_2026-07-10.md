# Call Brief — Warmup/Sending-Limit Discussion with Avinash (2026-07-10)

**The one thing to land in this call:** Smartlead's daily sending limit is ONE shared pool for campaign emails AND warmup emails. This is Smartlead's design, not our configuration — and it means our inboxes were sending zero warmup whenever they were on campaigns. We fixed it on Belardi Wong yesterday, safely, with receipts.

> **⚠ CORRECTION (2026-07-10, later the same day):** the shared-bucket premise
> below is WRONG. Smartlead's API reference says `max_email_per_day` includes
> warmup, but the live data disproves it: each account carries a SEPARATE
> `warmup_details.max_email_per_day`, and inboxes capped at 10 campaign
> emails/day were observed sending 41-43 warmup emails/day. Warmup was never
> being squeezed. The 15 daily-limit raises were rolled back to their snapshot
> values (verified per inbox), the headroom job was deleted, and the capacity
> calculation no longer reserves warmup out of campaign budget. Everything else
> in this document — warmup profiles, provider cold caps, bounce protection,
> placement testing — is unaffected.



---

## 1. The proof that the pool is shared (Avinash: "that's not how we had configured")

Three independent pieces of evidence — none of them our opinion:

1. **Smartlead's own API documentation** for the email-account update endpoint describes the field verbatim as:
   > `max_email_per_day` — "Maximum emails allowed per day **(including warmup and campaign emails)**"
   (Source: api.smartlead.ai/reference/update-email-account — screen-share this on the call.)

2. **Smartlead's help center (article 63, "smart-adjusting algorithm")** explains their auto-adjust feature exists precisely because the pool is shared:
   > when a mailbox is in an active campaign, "the AI will automatically **decrease the warmup count by 7-10** and manage the sending/reply ratio"
   — you don't need to trim warmup to make room for campaigns unless they draw from the same budget.

3. **Our live data:** every one of Belardi Wong's campaign-attached inboxes had `max_email_per_day` = 30 (Gmail) or 10 (Outlook) — exactly the campaign volume, zero room left. Warmup was configured "on" but had no budget to actually send from on active senders.

**On "Gmail was 30 / 25":** both true — the fleet was inconsistent (BW had Gmail inboxes at 30 AND at 10; PL is a mix of 5/10/15/20/25/30). Part of yesterday's fix was normalizing this per policy.

## 2. What we changed on Belardi Wong (live yesterday, all verified + reversible)

| Change | Numbers | Why |
|---|---|---|
| Raised the shared bucket on 15 campaign-attached inboxes | Gmail 30→45, Outlook 10→30 | Creates room so warmup can actually send alongside campaigns. Total hard-capped at 45 (inside Smartlead's 20-49/day optimal band; also Avi's own stated max from Tuesday) |
| **Cold volume unchanged — enforced in code** | Capacity the campaign tools see: **Outlook 10, Gmail 25, SMTP 15 max** | The extra bucket room is warmup-only by construction; no tool can assign more cold sends than before |
| Warmup profiles on 55 inboxes | bench 20/day, active 20/day + auto-adjust, new ramping to 40; reply-rate 25% | Warmup never turns off anymore (the old off-when-sending behavior rots reputation in 6-8 weeks per Smartlead's + Instantly's own docs) |
| Bounce auto-pause on 3 campaigns | trigger at 3% bounce | Safety net that didn't exist |

Rollback: timestamped snapshot of all 69 inboxes' pre-change values + per-change logs. Any value restorable.

## 3. The corroboration to mention (from today's standup)

Anjali manually tested 23 BW mailboxes → **3 landing in spam**. Our system, independently, the same day: **exactly 3 BW domains failing placement** (belardiwongs.com, bwdirectmail.com, heybelardiwong.com — 9 inboxes). Two different methods, same answer — the automated scoring matches manual reality.

## 4. Placement testing — proof the automation works end to end

- **2 real SmartDelivery tests run on PRECISE_LEADS: both 100% inbox** (test 475859: 35/35 Office365 + 25/25 G Suite; test 475933: same result). Results flowed automatically: Smartlead → our poller → database → deliverability sheet → (next sync) → Test Status column → precise-automator eligibility. Zero human touches.
- 2 of 92 PL credits used.
- **Testing-process clarification (from the standup):** manual tests are done per-account by the managers (Anjali for BW, Varsha via provider support / Mailreach, etc.) — NOT by importing inboxes into PL (Avinash: "don't import"). SmartDelivery-credit automation currently works only where credits exist (PL). For BW/Darlean/Mythic the manual per-account testing continues; if we want those automated too, those accounts need their own SmartDelivery credits (~20-40/month each at the auto-tester's pace).
- One free prep item: each account's "Deliverability test Campaign" needs one dummy lead added (Smartlead refuses to test through an empty campaign — found live).

**SOLVED — how Anjali tested (verified from the API, no need to ask):** She used Smartlead SmartDelivery on the **PRECISE_LEADS account** in *non-connected mode* — 21 manual tests on July 9 (04:16–12:25), one per BW domain, each named after the domain. Non-connected mode = Smartlead hands you a seed list + a Test-ID string; she logged into each BW mailbox and sent the test email by hand (no import — consistent with "don't import"). Her full per-domain results are in the API and her 3 spam domains match our automated scoring exactly: **heybelardiwong.com 46% spam, bwdirectmail.com 46%, belardiwongs.com 21%** (borderline: topbelardiwong 17%, mybelardiwong 12%, mailbelardiwong 4%).

**Automation SHIPPED same day (`nc_test_executor.py`, proven live):**
1. **Anjali's flow, 8 hours → 2 minutes per test.** She still creates the non-connected test in the PL UI (the only step the API refuses) and pastes the seed list + Track-ID into a new **"NC Tests" sheet tab**. The robot does the rest: sends the test email from the inbox via a one-off campaign in the inbox's OWN account (OAuth send — no imports, honoring "don't import"), polls the PL test, writes the result to the sheet + database, cleans up after itself.
2. **Her manual tests auto-ingest with zero process change:** if she sends by hand as today, the robot just records the result. Proven live on her real test 475632 (heybelardiwong.com → fail, 54% inbox / 46% spam) — flowed to the NC Tests tab, API Tests tab, and Mongo.
3. **Credit map (from the API):** PRECISE_LEADS ~90 · **DARLEAN has its own credits** (connected tests July 6) · **MYTHIC has its own credits** (connected test July 1) · BW exhausted (last test April). So the existing connected auto-tester works on Darlean + Mythic today; BW runs on the NC flow (PL credits) or gets its own credits.

**MYTHIC's API key was invalid (401) — rotated 2026-07-10, new key verified locally (42 accounts visible). Render still needs the new key.**

## 5. Bugs the live pilot caught (why the careful rollout was worth it)

1. Smartlead 400s on warmup ramp=0 (50 writes failed → diagnosed → fixed → clean retry same hour)
2. Capacity column would have leaked warmup headroom into precise-automator's cold-send budget (Manveen caught it)
3. Outlook's intended 10/day cold limit would have been erased by the bucket raise (Manveen caught it — now hard-capped in code)
4. Test-report parser read the wrong fields — every completed test would have scored a **false FAIL** (caught against the first real test's raw data; that test then correctly parsed as 100% inbox)

## 6. What's needed to go fleet-wide (the ask)

On Render, set: `HEALTH_NOTIFY_CHANNEL=C0AGVSUNEFP`, `BOUNCE_PROTECT_ENABLED=true`, `HEADROOM_FIX_ENABLED=true`, `WARMUP_AUTO_ENABLED=true`, `RETEST_ENABLED=true`, `NC_TEST_ENABLED=true`, and the **new MYTHIC API key** (`SMARTLEAD_API_KEY_MYTHIC`). Rotation stays OFF (agreed). First live day: DARLEAN/Mythic/PL get the same correction wave BW got. Day-2 check: BW's warmup log plans ~0 changes (converged).

One decision to take on the call: **BW testing going forward** — (a) NC flow on PL credits (shipped, Anjali 2 min/test), or (b) buy BW its own SmartDelivery credits (~20-40/month) and the fully-automatic tester runs there with zero human steps. Both keep "don't import".

Full details: TEAM_REPORT_2026-07-09.md (shareable) · OPERATOR_PLAYBOOK.md (runbook).
