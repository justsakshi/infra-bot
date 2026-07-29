# Deliverability Research Addendum — 2026-07-29

**Written:** 2026-07-29 · **Status:** research + roadmap refinement, no build
**Companion to:** [`DELIVERABILITY_WORKFLOW_PLAN.md`](./DELIVERABILITY_WORKFLOW_PLAN.md) (2026-07-28) · [`PLACEMENT_FINDINGS_2026-07-27.md`](./PLACEMENT_FINDINGS_2026-07-27.md)
**Purpose:** external research targeted at the points the workflow plan marked "directional only" and "genuinely contested," plus the one capability the standup asked for that the current system does not have — a trend/feedback loop ("learn from the mistakes and see the trends").

---

## 0. What this addendum does and does not do

It does **not** rewrite the workflow plan. That plan is sound and its §5 build order stands. This addendum:
- Confirms (with external corroboration) the contested calls so we can stop debating them.
- Corrects two numbers.
- Names the one gap the research exposes: detection is point-in-time; there is no longitudinal trend loop.
- Slots the fixes into the existing build order.

The workflow plan §4b already flagged that Reddit and real practitioner threads were unreachable during its research, and that much 2026 guidance is vendor SEO. That caveat holds — the sources below are weighted accordingly (official-provider + cross-source-consensus trusted; single-vendor claims directional).

---

## 1. Contested calls — now corroborated (stop debating)

| Our call (workflow plan) | External finding | Verdict |
|---|---|---|
| De-weight M365 domains; no config fix | Outlook placement ~49%→~27% YoY confirmed across sources. Hard SMTP rejection (`550 5.7.515`) now fires *before* Safe-Senders lists. Our 0/8-clean-auth result is the industry norm. | **Confirmed.** M365 collapse is structural, not our misconfiguration. |
| Warmup reputation is a weak health signal (reweighted 25→10) | Google/Microsoft actively detect warmup networks; cheap recycled-pool warmup is discounted or penalised. "100% reputation + 0/4 placement" is the textbook symptom. | **Confirmed.** Reweight was right. Warmup-off test cohort is defensible. |
| Shared IP + permutation naming = bulk fingerprint | Sources explicitly prescribe varying DKIM selector names, SPF include order, and DNS providers across domains to defeat bulk fingerprinting — exactly our `heybelardiwong/getbelardiwong/trybelardiwong` + one-IP (`52.15.49.97`) situation. | **Confirmed as the root cause.** Reputation is fine; infrastructure *identifiability* is the kill. |
| Provider diversity worth testing (Aerosend / alt) | Mixing Google + M365 so one provider's policy event can't zero the operation is the recommended structural fix. | **Confirmed directional.** 2-domain non-Google test justified. |

**Sources (this section):** litemail.ai/blog/outlook-cold-email-inbox-placement-2026, egerionreviews.com/outlook-black-hole-2026, mailivery.io/blog/does-email-warmup-work, maildoso.ai ranking, infraforge.ai/blog/google-workspace-alternatives.

---

## 2. Two number corrections

1. **Sequence length: 3–4 steps, not 2–3.** Instantly's 2026 benchmark (billions of emails) puts the optimum at 4–7, but complaint/unsubscribe risk **triples beyond step 4** — so 4 is the practical ceiling, and "shorter is always better" overshoots. 58% of replies on email 1 holds; 42% come from follow-ups, so cutting to 2 leaves replies on the table. **Recommendation:** SMB 3 steps, cap at 4. Our BW campaigns at 4 are at the edge, not over it. (Source: unifygtm.com cold-email-2026.)

2. **Domain age confirmed in-band:** Google 2–4 weeks, Microsoft 90+ days. Our provision numbers need no change.

---

## 3. The gap: detection without a trend loop

The standup asked to *"learn from the mistakes and see the trends."* Everything we have today is **point-in-time**:
- Placement test = a snapshot (and it catches a domain *after* it is already dead).
- Bounce rate = current.
- DBL / SURBL = current.

The industry's actual early-warning system is one signal we do not capture: **spam-complaint rate over time, per domain.**

### Google Postmaster Tools — the leading indicator
- Thresholds: **hard 0.3%**, **recommended <0.1%**.
- Measured against **inboxed** volume, not sent.
- Data **lags 24–48h**; monitor **weekly by domain** (not by campaign).
- This *precedes* a placement drop. Placement tests are lagging; complaint-rate trend is leading — it catches a domain while it is dying, not after.

### Microsoft SNDS — for the M365 side
- Free IP reputation as green / yellow / red. Directly relevant since our M365 domains fail there; a red flag corroborates the placement result without spending a test credit.

**Why this is the answer to "see the trends":** detection tells us *what is dead*. A complaint-rate + placement **time-series with trend alerts** tells us *what is about to die*. That longitudinal layer is the feedback loop the current system is missing.

**Sources:** mailflowauthority.com/gmail-complaint-rate-threshold, prospeo.io/s/google-postmaster-tools.

---

## 4. Roadmap — insertions into the existing §5 build order

The workflow-plan §5 order (placement scheduler → two-strike retirement → bench health → Slack swap alert → open-tracking guard → cohort builder) stands. Insert:

| Priority | Action | Why now |
|---|---|---|
| **0 — this week** | Run the 1-hour reallocation header test (2-step campaign → controlled seed → Reallocate Mailboxes after email 1 → inspect Email 2 raw `In-Reply-To` / `References`). | Workflow plan §0 names this as the hinge and it is still unresolved. One hour decides whether replacement is cheap or costs prospect experience. |
| **1** | Build §5 #1 (per-domain placement scheduler) **and, in parallel, Google Postmaster Tools complaint-rate ingestion** as a per-domain time-series. | Detection + the leading indicator together = the trend loop from §3. |
| **2** | §5 #2 two-strike retirement + §5 #5 open-tracking guard. | Turns detection into decisions; open-tracking guard is nearly free and enforces an already-agreed rule. |
| **3** | Next domain batch only: break the fingerprint — spread registrars/DNS providers, vary DKIM selectors + SPF include order, kill permutation naming, run a 2-domain non-Google provider test. | Structural fix for the root cause confirmed in §1. Do not touch live domains; apply on new provisioning. |
| **4** | §5 #3 bench health + §5 #4 Slack swap alert. | Makes the human reallocation step rare, fast, unambiguous. Depends on the header test (#0) resolving. |
| **5** | §5 #6 cohort-aware campaign builder. | Largest build; needs 1–4 proven first. Unchanged from workflow plan. |

---

## 5. One-line thesis

Our sending-domain **reputation is fine** — good auth, low bounce (fleet 1.23%), clean DBL. We are being killed by **infrastructure fingerprinting** (shared IP + permutation naming) and **Microsoft's structural collapse**, neither of which is a per-domain fix. So the durable workflow is three moves: **diversify infrastructure** so we stop looking like one bulk operation, **add a complaint-rate trend loop** to see death coming, and **keep the human reallocation step tight**. Everything in §4 serves one of those three.

---

## 6. Source weighting (per §4b discipline)

- **Trusted (official / cross-source consensus):** Google 0.3%/0.1% complaint thresholds and measurement method; Microsoft `550 5.7.515` hard-rejection behaviour; Outlook YoY placement decline; warmup-network detectability; DNS-variation anti-fingerprinting prescription.
- **Directional (repeated, unverified for our fleet):** exact sequence-length optima, domain-age windows, provider-diversity ROI, SNDS predictive value for shared-IP domains.
- **To settle by our own test, not by any source:** the reallocation header behaviour (§4 #0), and whether a distinct-IP domain changes placement (workflow plan open question #2).
