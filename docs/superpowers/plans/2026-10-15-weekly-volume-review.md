# Weekly volume review: plan

Spec: `docs/superpowers/specs/2026-10-15-weekly-volume-review-design.md`.

Checked live 15 Oct: `GET /analytics/mailbox/name-wise-health-metrics` returns
`data.email_health_metrics[]` with `from_email, sent, opened, clicked, replied,
unsubscribed, bounced` (74 Precise Leads inboxes over 14 days).

Decisions the spec left open:

- Bounces count only with 20+ sends in 14 days; fewer is "not enough data" (hold), not a slip.
- Placement is good when the newest counting test passed AND an older counting
  test 7+ days before it also passed. Newest failed = slip. Anything else = wait.
- Step back has no cooldown; a raise needs 7 days since the last approved change.
- An inbox whose Smartlead limit differs from its approved limit (or the start,
  if never approved) was changed by hand: left alone and listed.
- Inboxes on the kill list (SHOULD KILL) are left to Kill warnings.
- Posted as Infra Bot (it is in the Kill warnings channel; Domain Suggester's
  token cannot read channels), so the Apply handler sits on Infra Bot's app.
- Apply refuses a review older than 7 days, a client already applied, and any
  inbox whose limit moved since the review.

## Tasks (test first each)

1. `smartlead/volume_rule.py` (pure): signals, `decide`, `eligible`, `review_text`, `review_blocks`.
2. Ledger: `IntakeStore.approved_limits / approve_limit / save_review / review / mark_applied`.
3. Drift compares to the approved limit (`st.drift(..., want_limit)`, `_drift_sweep(..., approved)`).
4. `apply_client` (async, fake Smartlead): writes, reads back, records; failures reported.
5. `volume_review.py` CLI: dry run (default), `--post`, `--apply-client C --review D --by U --json`.
6. `volume_slack.js`: Apply button handler, approvers only; registered in `index.js`;
   Tuesday 13:45 IST cron behind `VOLUME_REVIEW_ENABLED=true`.
7. Live dry run; full suites; push.
