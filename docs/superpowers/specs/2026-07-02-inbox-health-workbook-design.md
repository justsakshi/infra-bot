# Inbox Health Workbook — Design Spec

**Date:** 2026-07-02
**Status:** Approved (pending written-spec review)
**Owner:** infra-bot / smartlead_sync

## Goal

A daily-maintained **workbook** (Google Sheet tab) that scores the health of **every**
inbox across **every** client, tells whoever owns it exactly **what to do** to keep it
landing in the inbox, **auto-runs** whatever maintenance is safely automatable, and
**notifies the responsible client manager** each day via Slack.

The product is not the score — it is the *action guidance*: "here is the score, here is
what is wrong, here is the exact next step, and here is who owns it."

## Non-Goals

- Not a replacement for human deliverability judgment (DNS fixes, copy rewrites, retire
  decisions stay with humans).
- Not a real-time system — it runs once per daily sync.
- Not auto-pausing/retiring/reconnecting inboxes (too risky; recommend-only).

## Background

`smartlead_sync` already runs daily on Render, pulls Smartlead + HeyReach data, and
writes Google Sheets tabs (per-client Campaign Summary / Inboxes / Warmup, plus the
cross-client **All Inboxes** master tab, **Campaign Metrics** dashboard, and the current
**Deliverability Queue**). Per inbox it already computes: `warmup_rep_pct`,
`warmup_state`, `connection_ok` (smtp+imap), `bounced`, and a placement `test_sheet_status`
+ `test_date` read from a **manually-maintained** deliverability Google Sheet
(ID `1CgxN8hKgqL2rouCOkQKRAyIzessMVGbt81RSc0Itgyg`, one tab per client).

infra-bot also has MongoDB connected and a Slack Bolt bot (socket-mode) already running.

This feature adds: a daily **health score**, **history** in MongoDB, a **workbook** Sheets
tab that supersedes the Deliverability Queue, an **auto-retest** executor using Smartlead's
Smart Delivery API, and a **daily Slack notification** grouped by client manager.

## Architecture

Built in **three phases**, each an independent commit/deploy. Stop after any phase if it
is enough. Phases 1–2 are pure read-only (no external side effects). Phase 3 is the only
one that writes to Smartlead / spends credits.

```
DAILY SYNC (existing run.py)
  └─ per-inbox snapshot (rep, warmup, bounce, conn, test)  ── already computed today
       └─ [P1] compute_health_score(snapshot) -> {score 0-100, grade, drivers}
            ├─ [P1] store daily record  -> MongoDB  inbox_health_history
            ├─ [P2] load 7d/30d-ago records -> trend (delta, arrow) + early-warning flag
            ├─ [P1/P2] resolve action (top problem, what-to-do, owner 🤖/👤, priority)
            └─ [P1] write "Inbox Health" workbook tab (all inboxes, scored, actioned)
  └─ [P2] Slack: one shared-channel daily post, action items grouped by client,
            each line @-mentioning that client's manager (from Manager Map)

SEPARATE SCHEDULED JOB (P3, retest_executor.py)
  └─ select campaigns whose inboxes are stale / untested / high-volume
       └─ Smartlead Smart Delivery API: create -> poll -> pull placement result
            └─ merge into placement source (API vs manual sheet: newer wins)
                 └─ next daily sync re-scores; workbook row may self-heal to green
```

### Storage (Both)

- **MongoDB `inbox_health_history`** — source of truth for history/trends. One document
  per (client, email, date):
  `{ client, email, domain, date (YYYY-MM-DD), score, grade, drivers{test,warmup,bounce,conn}, placement_status, placement_date, warmup_rep_pct, bounce_rate, connection_ok, campaigns }`.
  Indexed on `{client, email, date}`. Retention: keep ≥90 days (enough for 30-day trend).
- **Google Sheets "Inbox Health" tab** — human-readable current view: current score +
  trend + action per inbox. One row per inbox (deduped), all clients on the shared sheet;
  clients on separate sheets get their own tab (same per-sheet pattern as All Inboxes).

## Components (files)

- `smartlead/health.py` (NEW) — pure scoring + action-resolution logic:
  - `compute_health_score(snapshot: dict) -> dict` → `{score, grade, drivers}`
  - `resolve_action(snapshot, score) -> dict` → `{top_problem, what_to_do, how_long, priority, owner ("auto"|"human"), owner_skill}`
  - `trend(today_score, prior_scores) -> dict` → `{delta_7d, arrow, declining}`
  - Weights/thresholds are module constants (tunable). No I/O — unit-testable.
- `smartlead/health_store.py` (NEW) — MongoDB read/write for `inbox_health_history`.
  `save_daily(records)`, `load_prior(client, email, days_ago) -> score|None`.
  Mongo URI from env (`MONGODB_URI`), same as the Node app.
- `smartlead/sheets.py` (MODIFY) — add `write_inbox_health(rows)` + header labels/widths/
  grade coloring. Reuses `_dedupe_inbox_rows`.
- `smartlead/manager_map.py` (NEW) — `MANAGER_MAP: dict[client -> {name, slack}]` +
  `resolve_manager(client)`. Config-only; unknown client → "unassigned" (still listed).
- `smartlead/notify.py` (NEW) — build the daily Slack message (grouped by client,
  @-mention manager, P0s first, workbook link) + post via Slack API
  (`SLACK_BOT_TOKEN`, `HEALTH_NOTIFY_CHANNEL`).
- `smartlead/smart_delivery.py` (NEW, P3) — Python port of the Smart Delivery API flow
  (create → poll → pull), ported from the `email-deliverability-audit` TS skill.
- `retest_executor.py` (NEW, P3) — standalone entry: select targets, run tests (rate-
  limited, capped per run, logged), merge results.
- `run.py` (MODIFY) — call scoring + history + workbook write after the master tab; call
  notify at the end. All wrapped in try/except so health failures never break the sync.
- `smartlead/config.py` (MODIFY) — new constants (tab name, channel, weights, caps).

## The Health Score (0–100)

Sum of four signal sub-scores. Missing data for a signal → **neutral (half credit)**, not
zero (don't punish absence of data).

| Signal | Max | Full credit | Zero credit |
|---|---|---|---|
| Placement test | 40 | `inbox` & fresh (<14d) | `fail`/`spam` |
| Warmup reputation | 25 | rep ≥ 99% | rep < 90% |
| Bounce rate | 20 | bounce < 1% | bounce > 5% |
| Connection + reply | 15 | connected & reply-rate ≥ 1% (or too-few-sends) | disconnected |

- **Placement staleness:** `inbox` but test 14–28d old → linear decay of the 40 toward
  ~half; >28d → treated as untested (neutral 20/40). Keeps confidence honest.
- **Grades:** 90–100 **A** (green) · 70–89 **B** (light green) · 50–69 **C** (yellow) ·
  0–49 **D/F** (red).
- Weights + thresholds are config constants, tunable after seeing real distributions.

## The Workbook Tab ("Inbox Health")

One row per inbox (deduped by client+email), covering **all** inboxes including those in
**no** campaign. Columns:

| Column | Source |
|---|---|
| Priority (P0/P1/P2/—) | `resolve_action` |
| Client · Email · Domain · Provider | snapshot |
| Health Score · Grade | `compute_health_score` |
| Trend (↑/↓/→ ±N over 7d) | `trend` (P2) |
| Status (🔴 Broken / ⚠ Needs Action / ✅ Healthy) | grade + priority |
| Top Problem | `resolve_action` |
| What To Do | `resolve_action` |
| Owner (🤖 Auto / 👤 You) | `resolve_action` |
| How Long | `resolve_action` |
| Manager | `resolve_manager(client)` |
| Score drivers (test/warmup/bounce/conn breakdown) | `compute_health_score` |
| Warmup Rep % · Bounce % · Test Status · Test Date · # Campaigns | snapshot |

Sorted: Priority (P0 first) → Client → Score asc. Grade-colored. This tab **supersedes the
Deliverability Queue** (same detection rules, upgraded to scored + trended + all-inbox +
manager-attributed). The Queue tab is removed once the workbook ships.

## Action Resolution — Auto vs Human

`resolve_action` maps the worst active problem to a fix, an owner, and a priority. Same
first-match-wins order the current queue uses.

| Problem (priority) | Owner | What To Do | Automatable? |
|---|---|---|---|
| Failed placement / spam (P0) | 👤 | Pause, check SPF/DKIM/DMARC + copy + list, retest | No — root cause varies, 1–30d |
| Warmup blocked (P0) | 👤 | Investigate block; pause/retire if no recovery | No |
| Disconnected SMTP/IMAP (P1) | 👤 | Reconnect inbox (often OAuth re-login) | No |
| Low warmup rep <90% (P1) | 👤 | Hold out of campaigns; continue/adjust warmup | Partly (not auto) |
| Untested (P1) | 🤖 | Auto-run placement test | **Yes (P3)** |
| Stale test >14d (P1) | 🤖 | Auto-run fresh placement test | **Yes (P3)** |
| High-volume + old test (P2) | 🤖 | Auto-retest before scaling | **Yes (P3)** |
| Healthy (—) | — | None | — |

**Automated (🤖) — script does it, no asking:** daily scoring, history logging, trend +
early-warning, and (P3) placement **retests** (read-only measurement) with auto-refresh of
the result into the workbook. **Human (👤):** DNS/auth fixes, copy rewrites, reconnect,
pause/retire, warmup-vs-retire judgment.

### Phase 3 Auto-Retest guardrails

- Uses Smartlead **Smart Delivery API** (`smartdelivery.smartlead.ai/api/v1`), ported to
  Python from the `email-deliverability-audit` skill's `run-spam-test.ts`.
- Tests are **per campaign** (seed the campaign's senders), not per single inbox — one
  test covers all inboxes on that campaign. Only G Suite (20) + Office365 (21) seed pools;
  `is_warmup=true`; ≤~300 senders/test; 5–20 min each; **costs Smartlead credits**.
- **Rate-limited:** only stale/untested/high-volume targets; **capped at N tests/run**
  (config, default small); every run logged (what ran, cost, result). Never "retest all."
- **Merge rule (API supplements manual):** placement for an inbox = the **newer** of the
  manual-sheet result and the latest API result. Manual stays authoritative while fresh;
  API fills gaps + refreshes stale ones. Stored so the source of each result is visible.

## Manager Notifications

- **Manager Map** (`manager_map.py`) — **per client** (ownership is per client, not per
  Smartlead account; one account like PRECISE_LEADS holds several clients — Melior,
  Precise Leads, Better Data — each with its own manager). The workbook's existing
  `client` field distinguishes them. Entry: `client -> {name, slack_handle}`. Unknown
  client → "unassigned" (still shown, pinged to a fallback).
- **Channel:** **one shared Slack channel** post per day (`HEALTH_NOTIFY_CHANNEL`).
  Message groups action items by client, P0s first, each client block @-mentioning its
  manager, with a link to the workbook. Everyone sees everything; owners get pinged.
- Posted via the existing Slack bot token. Sending is wrapped so a Slack failure never
  breaks the sync.

## Data Flow / Error Handling

- Health scoring + history + workbook write run **after** the master-inbox write, inside
  their own try/except — a health failure logs and is skipped; the core sync still
  succeeds.
- Notify runs last, also isolated.
- P3 retest executor is a **separate** scheduled job (its own cron), so a slow/failed test
  batch never delays the daily sync.
- Mongo unavailable → scoring still writes the workbook with score but no trend (trend
  shows "—"); history simply isn't logged that run.

## Testing

- `test_health.py` (plain runnable script, like existing tests) — unit tests for
  `compute_health_score` (each signal at full/zero/neutral, staleness decay, grade
  boundaries), `resolve_action` (each problem → correct owner/priority/what-to-do), and
  `trend` (up/down/flat, missing prior).
- `test_manager_map.py` — client → manager resolution incl. unknown-client fallback and
  the multi-client-under-one-account case (Melior vs Precise Leads vs Better Data).
- Live read-only validation script for P1/P2 (score all DARLEAN inboxes, print
  distribution) before wiring the sheet write.
- P3: dry-run mode (select + log targets, don't call the API) validated before enabling
  real tests.

## Rollout

1. **P1** — `health.py` (score + action) + `health_store.py` + workbook tab + wire into
   `run.py`. Replace Deliverability Queue. Ship. (Read-only; safe.)
2. **P2** — trend from history + early-warning flag + Manager Map + Slack daily post. Ship.
   (Read-only; safe.)
3. **P3** — `smart_delivery.py` + `retest_executor.py` + merge rule + separate cron.
   Dry-run first, then enable with a small daily cap. Ship.

Each phase is independently valuable and independently reversible.

## Open Items (need user input before/at implementation)

- **Manager Map values** — the actual manager name + Slack handle per client (DARLEAN,
  Melior, Precise Leads, Better Data, Mythic, Belardi Wong, Avench, OSC, StaffAI).
- **`HEALTH_NOTIFY_CHANNEL`** — which Slack channel for the daily post.
- **P3 daily test cap** — how many auto-retests/day is acceptable on Smartlead credits.
