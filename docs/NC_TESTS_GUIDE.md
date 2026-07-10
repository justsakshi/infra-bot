# NC Tests — Automating Anjali-Style Placement Tests (2026-07-10)

Non-connected SmartDelivery tests on the PRECISE_LEADS account, for inboxes in
ANY client account (Belardi Wong, DARLEAN, MYTHIC) — no imports, PL credits.

## Why this exists

Anjali's July-9 testing (21 domain tests, ~8 hours) = for each domain: create a
non-connected test in the PL UI, log into the mailbox, send the seed email by
hand, then record the result. The API recon (2026-07-10) proved the create step
is UI-only and the manual send can't be botted for OAuth mailboxes — but
everything else can. This flow cuts her per-test work to ~2 minutes of UI +
paste; the robot does the rest.

## The human's 2 minutes (per test)

1. PRECISE_LEADS Smartlead → SmartDelivery → **Start Spam Test → Manual Test**,
   pick "non-connected accounts" at mailbox selection.
2. **Name the test EXACTLY the inbox email** (e.g. `sam@heybelardiwong.com`).
   Naming it the domain also works (Anjali's current convention).
3. Copy the **seed list** and the **Test-ID string** from the UI.
4. Open the deliverability sheet → **"NC Tests" tab** → new row: inbox email,
   Track-ID, seed list (comma/newline separated — paste as-is). Done.

## What the robot does (hourly 9:00–21:00 IST)

- Matches the row to its PL test by name (email, then domain).
- Builds a one-off campaign **in the inbox's own Smartlead account**:
  single-step sequence whose body carries the Track-ID, seeds as leads, inbox
  attached, inbox daily limit temporarily raised to seed-count + 5 (old value
  snapshotted in the row), campaign STARTED. Smartlead sends via the inbox's
  OAuth connection — no passwords, no imports.
- Polls the PL test; on completion: result → Mongo (`placement_results`,
  source `nc`) + "API Tests" tab, inbox limit restored, campaign paused, row
  marked DONE with Inbox % / Spam %.
- **Pure ingestion:** if the test is already COMPLETED (human sent manually,
  Anjali's current flow unchanged), the robot just records the result — proven
  live on her test 475632 (heybelardiwong.com → fail, 54% inbox / 46% spam).

## Row states

blank/PENDING → SENT → DONE. Problems land in Notes (`no PL test named…`,
`missing Track ID or Seed Emails`, `send failed: …`, cleanup instructions).

## Enable

`NC_TEST_ENABLED=true` on Render (ships DRY-RUN: logs the plan, mutates
nothing). Ingestion-only usage — recording manual tests automatically — is safe
to enable immediately; the campaign-send path additionally sends ~60 seed
emails from the target inbox on test day (same volume Anjali's manual send
produced in one blast).

## Limits & notes

- Seed sends go through the shared daily bucket: the temporary limit raise is
  restored on completion; a failed cleanup writes manual-restore instructions
  into the row's Notes.
- One Track-ID = one test (Smartlead rule) — never reuse a row's Track-ID.
- Credits: each non-connected test costs PL SmartDelivery credits at creation
  (UI shows the balance; API does not expose it).
- Senders needn't exist in Smartlead at all (Anjali sent zacharyr@… from
  webmail — that address isn't in any account). For those, only manual send +
  robot ingestion applies; the campaign-send path requires the inbox to live in
  one of our four Smartlead accounts.

## Suggestion phase (added same day)

Once a day (~9:15 IST run) the executor writes worst-first **NEEDS TEST** rows
into the tab for the credit-poor clients (`NC_SUGGEST_CLIENTS`, default
`Belardi Wong,MYTHIC` — BW's SmartDelivery credits are confirmed exhausted,
Mythic's balance is unknown/low; Darlean + PL keep the fully-automatic
connected tester). Cap 5 per client (`NC_SUGGEST_CAP`), untested before stale,
never anything tested <7 days ago, never a duplicate of a row already in the
tab. The human works top-down: create the PL test for each NEEDS TEST row,
paste Track-ID + seeds, done — the robot takes it from there. Proven live
2026-07-10: 10 suggestions written (5 BW stale, 5 Mythic untested).
