# Auto Inbox Rotation — Design Spec

**Date:** 2026-07-03 · **Status:** Approved (in-conversation)
**Depends on:** Inbox Health workbook, warmup executor, client filter, manager map.

## Goal

When an inbox in a live campaign goes **P0-broken** (failed placement / spam-landing),
automatically swap it out for a **healthy bench inbox of the same client** — keeping the
campaign running at the same volume with minimal visible change — and route the broken
inbox through the existing fix-and-retest loop until it returns to the bench.

## The follow-up problem (core design driver)

Smartlead pins each lead to the inbox that sent email 1; follow-ups go from that same
address. Removing an inbox makes Smartlead redistribute its leads uncontrolled.
**Key insight:** a burned inbox's follow-ups land in spam anyway — thread "continuity" on
a failed inbox is continuity into the spam folder. Policy (approved):

- **NEW leads** (email 1 not yet sent): send from the replacement. Zero visible change.
- **IN-FLIGHT leads** (mid-sequence on the victim):
  - **(A)** if a **persona-matched** replacement exists (same `from_name`), explicitly
    reassign each lead victim→replacement (`update lead email-account` API). Same sender
    name; new address; follow-ups actually land.
  - **(B)** no persona match → **pause those leads' sequence** (lose the tail cleanly)
    and alert the manager. Never leave follow-ups flowing from a spam-landing inbox.

## Selection rules

**Victims:** health rows with `status=broken` (P0 failed test / spam-landing) AND
`campaigns > 0`. Old clients excluded (existing filter).

**Bench (replacement) — same client HARD requirement, then best-first:**
grade A/B · warmup rep ≥90% · warmup age ≥14d · placement test = `inbox` (fresh) ·
`connection_ok` · has capacity · not already in that campaign. Persona match
(`from_name`) is a **soft** preference for the swap, **hard** for in-flight reassignment.

**No eligible bench inbox → NO swap.** Log + digest alert: "P0 inbox in campaign X,
zero healthy spares for <client> — provision domains (2-week lead)."

## Swap procedure (per victim per campaign — order is load-bearing)

1. **ADD** replacement to the campaign. Verify it's attached.
2. Reassign victim's **in-flight leads** → replacement (persona-matched only), else pause them.
3. **REMOVE** victim from the campaign (now empty of controlled leads — no random redistribution).
4. Victim: stays **connected** (replies keep arriving), warmup stays ON (always-on policy),
   manager works the fix, robot retests → test passes → back on the bench automatically.
5. Record the swap in Mongo (`rotation_log`) + surface in the Slack digest.

## Safety rails

- `ROTATION_ENABLED=false` default → **dry-run**: logs every intended add/reassign/remove, mutates nothing.
- `ROTATION_PER_CLIENT_DAILY_CAP` (default 2).
- Add-before-remove; **never** remove a campaign's last sender; verify after each step.
- Same-client hard match; in-flight reassignment only on persona match.
- **Dummy-campaign validation before first real enable:** a throwaway campaign with 2 of
  our own inboxes + ourselves as leads; run one real swap end-to-end; confirm follow-up
  behavior matches this spec. (Normal sends only — no SmartDelivery credits.)
- Idempotent: swaps logged; a re-run never re-swaps the same pair.

## Components

- `smartlead/rotation_planner.py` (pure): `select_swaps(health_rows, raw_inboxes, caps, log)`
  → list of `{campaign_id, victim, replacement, inflight_policy}` — unit-testable.
- `smartlead/api.py` (+3 methods): `add_campaign_email_accounts`, `remove_campaign_email_accounts`,
  `update_lead_email_account` (exact routes per probe), `pause_lead` if needed for (B).
- `smartlead/rotation_store.py`: Mongo `rotation_log` (dedupe + audit).
- `rotation_executor.py`: cron entry (after warmup, e.g. 12:00 IST), dry-run default.
- Config: `ROTATION_ENABLED`, `ROTATION_PER_CLIENT_DAILY_CAP`, bench thresholds reuse health constants.

## Honest guarantees

Same: campaign runs, volume, schedule, sequences, new-lead experience, sender *name*, replies to old mail.
Changes: From *address* on reassigned in-flight follow-ups; Gmail may split the conversation view.
Strictly better: follow-ups land in the inbox instead of spam.
