# Scale Roadmap — Everything Needed To Run Inboxes At Scale

**Gap analysis + build plan: what the system has, what's missing, and the exact specs to close each gap — so the system maintains any number of inboxes, tells us when to buy more, and keeps mail in the inbox.**

Created: 2026-07-07. Companions: [OPERATOR_PLAYBOOK.md](OPERATOR_PLAYBOOK.md) (runbook) · [DELIVERABILITY_MASTER_PLAN.md](DELIVERABILITY_MASTER_PLAN.md) (rules + sources) · [INBOX_HEALTH_PLAYBOOK.md](INBOX_HEALTH_PLAYBOOK.md) (thresholds).

---

## 0 — Direct answer: do we have everything?

**No. We have the MONITOR + FIX half. The SUPPLY half (capacity planning, "buy N domains now" advisories, provisioning) doesn't exist, and four quality gaps remain in monitoring.**

| Lifecycle stage | Status |
|---|---|
| MONITOR — score, grade, trend, DNS, blacklist, zombie campaigns | ✅ Built |
| FIX — warmup profiles, placement retests, rotation, bounce auto-pause | ✅ Built (dry-run, enablement pending) |
| **SUPPLY — know when we need inboxes, order them, warm them, bench them** | ❌ **Not built — biggest gap** |
| ADVISE — tell each manager what to do daily | ✅ Built (Slack digest pending config) |
| PREDICT — catch decay before it's visible | ⚠️ Partial (trend arrow only; leading indicators missing) |

---

## 1 — SUPPLY: Capacity planner (build first — the thing you asked for)

**Goal: every Monday the system says, per client: "you have X days of healthy sending capacity left; order N domains / M inboxes THIS WEEK."**

### The math (industry-benchmarked)

```
DEMAND  (per client)
  required_sends_per_day = Σ active campaigns' daily lead targets
                           (fallback: 7-day avg of actual sent_count)

SUPPLY  (per client)
  sendable = inboxes where grade A/B AND rep ≥90 AND test=inbox AND connected
  safe_capacity = Σ min(message_per_day, 30) over sendable inboxes

BENCH   (insurance pool)
  bench = warmed idle inboxes (rep ≥90, ≥21 days old, not in campaign)
  bench_target = max(5, 25% of active inbox count)        ← industry: 20-25%

CHURN   (from our own Mongo history — this is the key input)
  churn_rate = inboxes retired-or-red per month / fleet size
               (industry baseline: 10-20% of domains burn per month —
                at 118 domains expect 12-24/month; our SURBL data suggests high end)

TRIGGERS (any true → "ORDER NOW" advisory)
  1. bench < bench_target
  2. safe_capacity < required_sends_per_day × 1.2          (20% headroom)
  3. forecast: capacity_in_3_weeks < demand
     (subtract inboxes trending ↓ toward red + monthly churn)

ORDER SIZE
  deficit_sends = max(shortfalls above)
  new_inboxes  = ceil(deficit_sends / 30) + churn_month_estimate
  new_domains  = ceil(new_inboxes / 2)                     (our 2-per-domain rule; industry max 3)

LEAD TIME (why we order EARLY)
  purchase + DNS:      1-2 days (Zapmail)
  warmup to sendable:  21-30 days (new domain)
  ⇒ TOTAL 4-6 weeks from order to production — order at trigger, not at crisis
```

### Implementation spec — `capacity_planner.py` (new, read-only)
- Shipped as its own standalone Monday 9:30 AM IST cron (not folded into the daily sync — it re-fetches account/campaign data itself, including a real deliverability-tab read, rather than reusing the sync's in-memory state).
- Domain age: start tracking NOW — first-seen date per domain in Mongo (`domain_registry` collection); backfill via WHOIS once. Without this, the 21-30-day gate and replace-vs-repair calls stay guesswork.
- Output: new "Capacity" tab in the workbook, one row per client:
  `client | demand/day | safe capacity | headroom % | bench | bench target | churn/mo | days of runway | ORDER: N domains, M inboxes | by date`
- Slack digest gets the ORDER line when a trigger fires.
- Effort: ~1 day. No API writes. **This closes "suggest new inboxes whenever we need them."**

### Provisioning pipeline (phase 2, after planner proves itself)
Zapmail API (Pro plan $299/mo gates API access — confirm with Zapmail; endpoints for domain search/purchase, mailbox creation, DNS retrieve, export-to-Smartlead exist; rate limits: 5 req/s, domain search 10 req/30min):
1. **Phase A (read-only, build with planner):** `zapmail_dns_check.py` — pull expected DNS per domain, diff vs live, flag mismatch as "escalate to Zapmail" workbook row.
2. **Phase B (semi-auto):** planner says ORDER → script generates domain-name candidates (existing /zapmail-domain-setup-public naming patterns), human clicks approve → API purchases + creates inboxes + exports to Smartlead.
3. **Phase C (closing the loop):** new inboxes auto-detected by sync → auto-tagged "insurance" → NEW warmup profile applied by warmup executor (already built) → blacklist pre-flight (already built, run ad-hoc: `python3 blacklist_monitor.py <domain>`) → after 21-30d + rep ≥90 → auto-promoted to bench.
- Alternatives if Zapmail API disappoints: Maildoso ($1.90-2.50/inbox, API + one-click Smartlead export, shared IPs), Hypertide ($1/inbox, isolated tenants), Mailreef (dedicated server, $240/mo + usage). Keep as fallback, don't switch mid-incident.

---

## 2 — SURBL incident: corrected understanding (research update)

SURBL is a **URI blacklist — it flags domains appearing in message LINKS/BODIES, not FROM addresses.** Being listed as a pure sender domain does not by itself route mail to spam. Impact path:
- Filters knock ~10-15 points off composite spam score **when a listed domain appears in the body** (links, signatures, tracking URLs).
- Gmail/Outlook often **disable the links silently** rather than spam-folder the message — a silent failure mode.

**Revised action list (replaces "panic delist everything"):**
1. **Audit campaign bodies + signatures:** do emails link to the sending domain (e.g. unsubscribe links, signature URLs, tracking domains on the sending domain)? If YES → those campaigns ARE hurt by the listing → fix first (move links to a clean, dedicated tracking/landing domain, or drop links from email 1 entirely — already our copy rule).
2. **Tracking domains:** every client must use a **dedicated custom tracking domain on a CLEAN domain** (never the Smartlead shared default, never a SURBL-listed sending domain). One tracking domain per client, isolated blast radius. → Add a tracking-domain audit column to the sync (campaign settings expose it).
3. **Delisting:** evidence-based, per-domain, AFTER remediation — bulk blind requests create repeat-offender status and get ignored. Prioritize only domains whose URLs actually appear in mail bodies.
4. **Root cause at fleet level:** mass listing pattern points to list hygiene (unverified addresses hitting spamtraps) + shared infrastructure. Enforce ≥95% list verification pre-upload, and raise with Zapmail.

---

## 3 — PREDICT: early-warning upgrades

| Signal | What to build | Numbers | Effort |
|---|---|---|---|
| **Per-domain reply rate** (the leading indicator — placement drops show in replies 48h before opens/bounces) | Daily job: reply rate per sending DOMAIN (not campaign) from Smartlead stats → alert when >30% below its own 7-day rolling average; hard alert <1% after 200 sends | catchable/reversible within 48h if acted on | ~1 day; data already in analytics endpoints |
| Google Postmaster Tools | ⚠️ v2 API no longer exposes reputation — only compliance status + spam rate. Worth pulling **spam rate** (keep <0.1%, hard fail 0.3%) for Gmail-heavy clients | needs domain verification per domain — do top clients only | ~1 day, low priority |
| Microsoft SNDS | Skip — IP-centric, needs dedicated IPs ≥100 msg/day; we're on shared pools | — | n/a |
| Seed monitoring (EmailGuard $49/mo, GlockApps $59/mo) | Skip for now — SmartDelivery native covers placement testing; GlockApps methodology is newsletter-oriented | revisit if SmartDelivery credits become the bottleneck | n/a |
| SpamAssassin factors | Parse SmartDelivery report (score + firing rules like HTML_MESSAGE, LINK_REDIRECT) into health workbook "what to do" | <5.0 required, <3.0 target | ~1 day (deferred from last round) |

---

## 4 — Rotation: rest-based cycling (upgrade to current replace-only model)

Industry standard we don't implement: domains aren't binary healthy/dead — they fatigue and **recover with rest**.

- **Cycle:** ~45-60 days active sending → **4-6 weeks REST** (warmup-only, 5-10/day — our IDLE profile already does this) → return to bench. Rest recovers 10-15 placement points.
- **Placement decay curve** (plan retirement BEFORE burn): weeks 1-2 ≈ 95% → month 2 ≈ 90% → month 3 ≈ 85% (warning) → month 4 ≈ 78% (rotate NOW) → month 6 ≈ 70% (burned).
- **Cost:** resting = $0 + 4-6 weeks idle; replacing = ~$12 domain + inbox fees + 4-6 weeks warmup. Rest is strictly cheaper when the domain isn't blacklisted/burned.
- **Build:** add `RESTING` state to the fleet model (tag-driven). Rotation executor gains: domain active >60 days OR placement <85% trending ↓ → recommend rest swap (bench domain in, tired domain to RESTING). Portfolio mix target: **active + 25% reserve + 20% resting + warmup pipeline ≈ monthly churn**.
- Effort: ~2 days on top of existing rotation executor. Needs domain age tracking (§1) first.

---

## 5 — Remaining quality gaps (small builds)

| # | Gap | Spec | Effort |
|---|---|---|---|
| 1 | **Send-volume cap enforcement** | Sweep: `message_per_day > 35` → set 30 (POST /email-accounts). Dry-run flag like the others. Smartlead's own data: 20-49/day band = 88% placement | ½ day |
| 2 | **Inboxes-per-domain audit** | Sync check: >3 inboxes on one domain → workbook flag | 1 hour |
| 3 | **Tracking-domain audit** | Per campaign: custom tracking domain present? on a clean (non-listed) domain? → workbook flag (ties into §2) | ½ day |
| 4 | **Campaign settings sweep** | Same endpoint as bounce protect: `enable_ai_esp_matching=true` (+16% deliverability), open-tracking off on cold campaigns | ½ day (extend bounce_protect_executor) |
| 5 | **Retirement automation** | Grade D for 14 consecutive days OR blacklisted+burned → auto-tag retired, warmup off, remove from campaigns (recommend-first, then auto) | 1 day |
| 6 | **Warmup executor perf** | Stale-campaign check fetches EVERY lead of every campaign (~1h run). Use analytics-by-date sent_count only + cache newest-lead date daily in Mongo | ½ day |
| 7 | **List hygiene gate** | Pre-upload verification coverage check (≥95% verified) — hook into lead-upload flow; SURBL root cause | 1 day |
| 8 | **Domain age registry** | First-seen tracking in Mongo + WHOIS backfill (needed by §1 capacity + §4 rotation) | ½ day |

---

## 6 — Build sequence (priority order)

```
ROUND 1 (this week)   — capacity planner + domain age registry (§1)  ← the asked-for feature
                      — tracking-domain audit + body-link audit (§2)  ← SURBL impact triage
ROUND 2 (next week)   — per-domain reply-rate early warning (§3)
                      — volume cap sweep + ESP/tracking settings sweep (§5.1, §5.4)
                      — warmup executor perf fix (§5.6)
ROUND 3 (week 3+)     — rest-based rotation + RESTING state (§4)
                      — retirement automation (§5.5)
                      — SpamAssassin parsing (§3)
ROUND 4 (when proven) — Zapmail provisioning phases A→B→C (§1)
                      — Postmaster spam-rate pull for top clients (§3)
ONGOING               — enablement sequence from OPERATOR_PLAYBOOK §3 (bounce → warmup → retest → rotation)
```

**When all four rounds land, the loop closes:** system watches every inbox daily → fixes what's mechanical → rests what's tired → retires what's dead → tells you exactly how many domains to buy 4-6 weeks before you run out → (phase C) buys and warms them itself. Human keeps: copy, replies, client decisions, delisting judgment.
