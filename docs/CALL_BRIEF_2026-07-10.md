# Call Brief — Warmup/Sending-Limit Discussion with Avinash (2026-07-10)

**The one thing to land in this call:** Smartlead's daily sending limit is ONE shared pool for campaign emails AND warmup emails. This is Smartlead's design, not our configuration — and it means our inboxes were sending zero warmup whenever they were on campaigns. We fixed it on Belardi Wong yesterday, safely, with receipts.

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

**Automation picture (what the API recon established):**
1. **Zero-cost, available now:** the bot can read every manual test Anjali runs (list + results are on the API). We can auto-ingest them into the same pipeline as our auto-tests — domain from the test name → inbox rows → sheet → Test Status → precise-automator. Anjali changes nothing; the recording/eligibility half of her work disappears.
2. **DARLEAN can be automated today:** its account ran *connected* campaign-mode tests as recently as July 6 — it has its own SmartDelivery credits. Our proven auto-tester works there as-is.
3. **BW full automation needs credits on the BW account.** Anjali's manual-send step cannot be botted: all 69 BW mailboxes are OAuth-connected (no SMTP credentials exposed), and non-connected test creation is UI-only (not on the public API — `test_with_sl_account` is rejected on the create endpoint).

**⚠ Found during recon: MYTHIC's API key returns 401 Invalid — every bot job for Mythic is currently broken. Need a fresh key from the Mythic Smartlead account (Settings → API).**

## 5. Bugs the live pilot caught (why the careful rollout was worth it)

1. Smartlead 400s on warmup ramp=0 (50 writes failed → diagnosed → fixed → clean retry same hour)
2. Capacity column would have leaked warmup headroom into precise-automator's cold-send budget (Manveen caught it)
3. Outlook's intended 10/day cold limit would have been erased by the bucket raise (Manveen caught it — now hard-capped in code)
4. Test-report parser read the wrong fields — every completed test would have scored a **false FAIL** (caught against the first real test's raw data; that test then correctly parsed as 100% inbox)

## 6. What's needed to go fleet-wide (the ask)

On Render, set: `HEALTH_NOTIFY_CHANNEL=C0AGVSUNEFP`, `BOUNCE_PROTECT_ENABLED=true`, `HEADROOM_FIX_ENABLED=true`, `WARMUP_AUTO_ENABLED=true`, `RETEST_ENABLED=true`. Rotation stays OFF (agreed). First live day: DARLEAN/Mythic/PL get the same correction wave BW got. Day-2 check: BW's warmup log plans ~0 changes (converged).

Full details: TEAM_REPORT_2026-07-09.md (shareable) · OPERATOR_PLAYBOOK.md (runbook).
