# Deliverability Master Plan — Consolidated (2026-07-29)

**Written:** 2026-07-29 · **Status:** consolidated roadmap. Merges every research pass to date with the measured fleet state.
**Supersedes as the single entry point:** read this first, then the source docs it points to.
**Source docs merged:**
- [`DELIVERABILITY_WORKFLOW_PLAN.md`](./DELIVERABILITY_WORKFLOW_PLAN.md) (2026-07-28) — the workflow design + Smartlead reallocation findings
- [`PLACEMENT_FINDINGS_2026-07-27.md`](./PLACEMENT_FINDINGS_2026-07-27.md) — the 4-domain placement test that overturned the retirement list
- [`DELIVERABILITY_RESEARCH_ADDENDUM_2026-07-29.md`](./DELIVERABILITY_RESEARCH_ADDENDUM_2026-07-29.md) — external research on the contested/open points
- `~/Documents/Last30Days/cold-email-deliverability-inbox-placement-*-raw-v3*.md` — two /last30days community-research passes (Reddit, HN, GitHub, TikTok, Instagram, YouTube)

---

## 0. The three-sentence thesis

Our sending-domain **reputation is fine** (aligned SPF/DKIM/DMARC + p=reject, fleet bounce 1.23%, 0/121 on Spamhaus DBL). We are killed by two things no per-domain fix touches: **infrastructure fingerprinting** (68/97 domains on one shared IP `52.15.49.97` + brand-permutation naming + 99/121 on SURBL) and **Microsoft's structural collapse** (M365-hosted domains score 0/8 everywhere with clean auth). So the durable workflow is: **diversify infrastructure** so we stop looking like one bulk operation, **add a leading-indicator trend loop** (spam-complaint rate, per domain, over time) so we see death before it lands, and **keep the human reallocation step tight** because Smartlead mailbox reallocation is UI-only.

---

## 1. Measured fleet state (2026-07-28)

| Client | Inboxes | In campaigns | Bench (idle) | Domains |
|---|---|---|---|---|
| Belardi Wong | 66 | 34 | 32 | 22 |
| DARLEAN | 108 | 69 | 39 | 36 |
| MYTHIC | 42 | 12 | 30 | 15 |
| PRECISE_LEADS | 342 | 53 | 289 | 101 |
| **Total** | **558** | **168** | **390** | **174** |

- Backup pool already exists: **390 idle inboxes (70% of fleet)**. Gap is not supply — it's that we don't know which spares are healthy and have no swap mechanism.
- **68/97 domains share IP `52.15.49.97`** (provider parking infra). Clean on blacklists, but a fingerprint.
- Placement truth: Google-hosted domains 8/8 inbox at Google; M365-hosted 0/16 anywhere.

---

## 2. What is already LIVE (built 2026-07-27/28)

- Per-inbox bounce rate, scored, alarmed at 3% (fleet 1.23%; one inbox caught at 4.3%)
- Bounce auto-pause on all 21 active campaigns (none had it before)
- Working Spamhaus DBL check (was silently reporting clean; 0/121 on first real scan)
- Health score reweighted onto signals that actually vary (warmup 25→10)
- API-key watchdog (keys rotated 3× in one day, jobs went blind silently)
- Reply-rate decay + DBL alerts routed to Slack
- EmailGuard placement testing integrated end-to-end, dry-run by default

**Modules present** (`smartlead_sync/`): blacklist_monitor, bounce_protect_executor, capacity_planner, check_dns, eg_test_executor (EmailGuard), key_health_monitor, nc_test_executor, reply_monitor, retest_executor, rotation_executor, warmup_executor, run.py (orchestrator).

---

## 3. What the research settled (stop debating these)

| Question | Verdict | Evidence |
|---|---|---|
| Is M365 collapse our misconfig? | **No — structural, industry-wide.** De-weight M365 domains. | Outlook placement ~49%→~27% YoY; hard `550 5.7.515` rejection now fires before Safe-Senders. Our 0/8-clean-auth matches. |
| Does warmup measure health? | **No — weak signal, possibly detectable.** Keep it on as hygiene, don't trust its reputation number. | 100% warmup reputation coexisted with 0/4 Microsoft placement on the same mailbox. |
| Root cause of our placement failures? | **Infrastructure fingerprinting**, not reputation. | Shared IP + permutation naming + SURBL. Sources prescribe varying DKIM selectors / SPF include order / DNS providers to defeat it. |
| Sequence length? | **3-4 steps. 4 is the ceiling.** | Complaint risk triples beyond step 4; 58% of replies on email 1. Our BW campaigns at 4 are at the edge, not over. |
| Mid-sequence rotation on Smartlead? | **Not possible without stranding leads.** Reallocation is UI-only. | Smartlead support confirmed: sender sticks to a lead for the whole sequence; no API for reallocation. |
| ESP matching? | **Directional only — do not treat the numbers as measured.** | Cited as same-provider 94-96% vs 85-88% cross, but our own data cuts against it: our M365-hosted domains failed *at Microsoft*, which is same-provider. Built into Smartlead from $39/mo, so cheap to try, but not evidence-backed for our fleet. |
| Links in email 1? | **One link max, none until a positive reply, never shorteners.** | Loud community rule (r/Coldemailing) + web corroboration (Mailpool). |
| Biggest lever overall? | **List quality + targeting, above infrastructure.** | Repeated across r/agency and r/EmailOutreach; matches our own bounce data. |

---

## 4. The gap that reframes the build: we have DETECTION, not a TREND LOOP

Everything live today is **point-in-time**: a placement test is a snapshot (and it catches a domain *after* it's dead); bounce and DBL are current. The community's actual early-warning system is one signal we do not capture:

**Spam-complaint rate over time, per domain — Google Postmaster Tools (Gmail) + SNDS / postmaster.live.com (Microsoft).**

- Google Postmaster Tools: hard **0.3%**, Google-recommended **<0.1%**, and we flag at **0.08%** (our own safety
  margin, not a Google figure). Measured against *inboxed* not *sent*. Lags 24-48h. Monitor **weekly by domain**.
  Domain-scoped, so it works for us — needs one DNS TXT verification per domain.
- ~~Microsoft SNDS~~ — **NOT AVAILABLE TO US. Verified 2026-07-29.** SNDS is IP-scoped, and we own none of our
  sending IPs: every domain's SPF is `include:_spf.google.com` or `include:spf.protection.outlook.com`, i.e. we
  send through Google's and Microsoft's shared outbound pools. You can only register IPs you control. Same reason
  JMRP (Microsoft's complaint feedback loop) is closed to us.
- **Consequence: Microsoft stays a blind spot.** There is no complaint-rate or reputation feed available to a
  shared-IP sender at Microsoft. Our only Microsoft signal is placement testing, which is point-in-time and
  lagging. This is an argument for de-weighting M365 rather than a gap we can close with tooling.
- The community runs a **daily 15-min loop**: bounce (>2% flag), spam rate (>0.08% flag), Postmaster reputation,
  per-inbox reply-rate trend. All of that is achievable for Gmail recipients; none of it for Microsoft.

Detection tells us *what is dead*. A complaint-rate + placement **time-series with trend alerts** tells us *what is about to die*. That is the "get notified before we land on spam" the standup asked for.

**Numeric tripwires (adopt as alert thresholds):**
- Spam rate: `<0.1%` OK · `0.1-0.3%` warn · `≥0.3%` critical (Gmail throttles)
- Bounce: `<1%` OK · `2-5%` warn · `>5%` stop the sequence (we currently pause at 3% — keep)
- Blacklist: any Spamhaus / Barracuda / SORBS listing = critical

---

## 5. Consolidated build roadmap (everything, in order)

Priorities 0-6 below fold the workflow-plan §5 order together with the trend-loop gap and the new community tactics. Nothing here contradicts the existing plan; it extends it.

| # | Build | Type | Why here | Depends on |
|---|---|---|---|---|
| **0** | **Reallocation header test** — 2-step campaign → controlled seed → Reallocate Mailboxes after email 1 → inspect email 2 raw `In-Reply-To`/`References` | Test, 1 hr | The hinge of the whole swap workflow, still unresolved. Decides if replacement is cheap or costs prospect experience. | none |
| **1** | **Per-domain placement scheduler** on the tiered cadence (active 14d / failed 7d / new-on-entry / bench 45d) | Build | Detection is the workflow's input | EmailGuard paid plan |
| **1b** | **Trend loop: Google Postmaster complaint-rate ingestion** as a per-domain time-series, with the §4 tripwire alerts to Slack. **Gmail only — SNDS is unavailable to shared-IP senders (see §4)** | Build | The "notify before spam" capability; the leading indicator placement tests lag | Postmaster TXT verification per domain (Zapmail owns DNS) |
| **2** | **Two-strike retirement rule** + Slack alert naming dead domain and its bench replacement | Build | Turns detection into a decision; single tests are too noisy (our own July-9 sweep proved it) | #1 |
| **3** | **Bench health tracking** — test the 390 spares before they're needed | Build | A never-tested spare is a guess, not a spare | #1 |
| **4** | **Slack swap instruction** — dead mailbox + bench replacement + campaign + inline Reallocate-Mailboxes steps (see §5b of workflow plan for exact shape) | Build | Reallocation is UI-only; make the human step instant + unambiguous (<60s) | #2, #3, #0 |
| **5** | **Guardrails: open-tracking guard + link-policy guard + ESP-match routing** | Build | Cheap, rule already agreed. Extend the open-tracking flag to also catch: >1 link or any link in email 1, shortened URLs. Route M365-heavy prospects from healthy Google inboxes only. | none / Smartlead ESP-match |
| **6** | **Cohort-aware campaign builder** (rotate at cohort boundary, throttle volume continuously; 3-4 step sequences) | Build | The real rotation mechanism; largest build | #1-4 proven |
| **7** | **Next domain batch only: break the fingerprint** — spread registrars/DNS providers, vary DKIM selectors + SPF include order, kill permutation naming, 2-domain non-Google provider test | Provisioning policy | Structural fix for the confirmed root cause. Apply to NEW provisioning; do not touch live domains. | none |

### Daily / weekly operating rhythm (wrap around the builds)
- **Daily (automated, alert-only):** bounce scan, spam-rate check (once #1b lands), reply-rate trend, key-health watchdog. Fleet already does most of this; add spam-rate.
- **Weekly (automated):** per-domain placement test on active domains; MXToolbox/DBL blacklist sweep (Spamhaus, Barracuda, SORBS).
- **Per new campaign:** placement test before first send; 3-4 step sequence; no link in email 1.
- **Monthly:** bench sweep progress; review M365-domain de-weighting; fingerprint audit on new domains.

---

## 6. Immediate next actions (this week)

1. **Run build #0** (reallocation header test) — 1 hour, unblocks #4.
2. **Verify domains in Google Postmaster Tools** — prerequisite for #1b. One DNS TXT per domain, and Zapmail owns
   our DNS, so this is a request to them, not a self-serve task. (SNDS registration is NOT possible — see §4.)
3. **Ship build #5 open-tracking + link-policy guard** — cheapest win, rules already agreed.
4. **Decide EmailGuard paid plan** — gates #1 and the whole detection tier.

---

## 7. Source-confidence discipline (carried from the addendum)

- **Trusted (our own measurement):** sender stickiness, 321 in-flight leads, 390 bench inboxes, per-inbox bounce, all 4 placement results, 68/97 shared IP, warmup-vs-placement divergence.
- **Trusted (official docs):** Smartlead reallocation behaviour; Google 0.3%/0.1% thresholds + measurement method; Microsoft `550 5.7.515`.
- **Directional (repeated, unverified for our fleet):** 2-3 mailboxes/domain, volume ceilings, domain-age windows, ESP-match lift %, provider-diversity ROI.
- **Settle by our own test, not by any source:** reallocation header behaviour (#0); whether a distinct-IP domain changes placement.

---

## 8. What we still cannot see (coverage gaps)

- **X/Twitter** is offline in the /last30days research setup (Windows needs a Firefox x.com login or an API key) — cold-email practitioners are active there, so a future pass with X would add practitioner voices.
- **Reddit's deepest deliverability threads** are intermittently reachable; treat single-thread findings as directional.
- Community research corpus and thresholds captured in the two saved raw files under `~/Documents/Last30Days/`.
