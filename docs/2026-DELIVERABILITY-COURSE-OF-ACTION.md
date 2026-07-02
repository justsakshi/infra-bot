# 2026 Deliverability — What's New + Course of Action

**Research date:** 2026-07-03. Sources: Smartlead, Mailgun, Microsoft, Google, topo.io, Instantly, Woodpecker 2025 data, Validity 2025 benchmark.

This doc: (1) what changed in 2024-2026 vs our baseline, (2) what the **system** should auto-handle, (3) what the **campaign manager** does for each case.

---

## Part 1 — The 7 Things That Changed (2024→2026)

| # | Change | Year | We handle it? |
|---|---|---|---|
| 1 | **Spam complaint rate < 0.3% (target < 0.1%)** — Google now *permanently rejects* over this | 2024, escalated Nov 2025 | ❌ new metric to add |
| 2 | **Google Workspace cold-email crackdown** — whole tenants suspended | late 2025 | ⚠️ policy/manager |
| 3 | **Per-inbox volume dropped to 20-50/day** (15-25 conservative); 100+/day = 4.3× bounce | 2025 | ⚠️ lower our ramp |
| 4 | **Keep maintenance warmup ON for ACTIVE inboxes** (5-10/day trickle) — off = erosion in 6-8 wks | 2025 | ⚠️ change our rule |
| 5 | **One-click unsubscribe header (RFC 8058)** required for bulk | June 2024 | ✅ Smartlead native (verify) |
| 6 | **DKIM 2048-bit + rotate selectors 2×/yr** | 2025 std | ⚠️ DNS audit |
| 7 | **Continuous list verification** (catch-all scoring, daily re-verify) | 2025 | ⚠️ manager process |

Plus context: **DMARC enforcement (p=quarantine/reject) now effectively mandatory at 5k+/day** (Microsoft rejects outright since May 2025); **fully-authed senders 2.7× more likely to inbox**; **global avg inbox placement is ~84%** (so our 85% target = market average, not exceptional).

---

## Part 2 — What the SYSTEM Should Do (build items)

### A. Lower warmup volume defaults (quick config)
- Change `WARMUP_PER_DAY` 40 → **30**, keep ramp +5. Matches 2026 safe band (20-50/day).
- **Status:** one-line config change.

### B. Maintenance-warmup for active inboxes (refine the auto-warmup rule)
- Current: warmup ON (idle) / OFF (sending). **2026 says never fully off.**
- New 3-state: idle → **full warmup** (30/day); actively sending → **trickle warmup** (5-10/day, don't fully disable); stale campaign → full warmup (rescue).
- **Status:** refine `warmup_planner.py` — change "disable" to "trickle" for active senders.

### C. Spam signal (API-verified 2026-07-03)
- Smartlead does **NOT** expose true provider spam-complaint rate (that lives in Google Postmaster Tools — manager checks there).
- BUT it DOES expose: `warmup_details.total_spam_count` (per inbox), `block_count` + `unsubscribed_count` + `bounce_count` (per campaign analytics). These are usable proxies.
- **Action:** add `total_spam_count > 0` and rising `block_count` as red flags in the health score. True complaint-rate stays manager-tracked (Google Postmaster Tools).

### D. Volume-cap watch (flag over-sending inboxes)
- Flag any inbox sending **>50/day** in the health workbook (2026 risk band).
- **Status:** buildable — we already have `sent_today`/`max_per_day`.

### E. DNS auth checker (SPF/DKIM/DMARC per domain)
- Add a `dig`-based check: SPF present + not `+all`, DKIM 2048-bit, DMARC at enforcement. Flag gaps.
- **Status:** new feature (moderate) — runs per domain in the sync.

---

## Part 3 — Manager's Per-Case Action Guide

**For every situation the workbook/system can surface, here's exactly what the manager does.**

### Deliverability / placement
| Case (workbook signal) | What the manager does |
|---|---|
| **Failed placement test** (P0, red) | Pause inbox. Check SPF/DKIM/DMARC. Simplify copy (cut links/images). Verify list. Retest after fix. |
| **Warmup spam count > 0 / block_count rising** | Inbox landing in spam even in warmup — pause, audit copy + auth, retest. (System can flag this.) |
| **Spam complaint rate > 0.3%** (Google Postmaster Tools — manager checks, not in Smartlead API) | STOP the campaign. The list or copy is generating complaints — audit both. Never resume until root-caused. |
| **Placement 70-85%** | Inspect spam-filter details; tune copy; recheck auth. |
| **Stale placement test (>14d)** | Auto-tester handles it, or run a manual test. |

### Volume / sending
| Case | Manager action |
|---|---|
| **Inbox sending >50/day** | Lower its daily cap to 30-40. High volume = 4.3× bounce + reputation scrutiny. |
| **Scaling need** | Add more warmed inboxes (rotation), don't raise per-inbox volume. 500/day = 10-15 inboxes @ 30-50. |
| **On primary Google Workspace** | Move cold email to secondary domains only. Primary GWS tenants get suspended. |

### Warmup / lifecycle
| Case | Manager action |
|---|---|
| **New inbox** | System auto-enables warmup (30/day ramp 5). Wait 14-21 days before live use. |
| **Warmup blocked** | Young (<30d) → retire. Established → re-warm at 15/day. Investigate blocked reason. |
| **Low warmup rep (<90%)** | Keep OUT of campaigns; let warmup recover; retest. |
| **Active inbox** | Keep a maintenance warmup trickle ON (5-10/day) — do NOT fully disable. |

### Campaign health
| Case | Manager action |
|---|---|
| **Stale campaign** (ACTIVE but dead 14d+) | Feed it new leads, OR pause/complete it, OR reassign its inboxes. Don't leave inboxes trapped. |
| **Reply rate <1% after 200 sends** | Compare copy vs a healthy sibling; rotate the inbox out if reputation confirmed bad. |
| **Bounce 3-5%** | List problem (hard bounces) → clean list. Reputation (soft) → cut volume 50% for 2 weeks. |
| **Bounce >5%** | STOP the campaign immediately. |
| **Reply drop >30% week-over-week** | Run full deliverability audit. |

### List / copy hygiene
| Case | Manager action |
|---|---|
| **Before any campaign upload** | Verify the list (catch-all scoring, not just syntax). Remove invalid + toxic addresses. |
| **Gmail → Promotions tab** | Cut to 0-1 links in email 1, remove images/heavy HTML, text-only signature. |
| **Same copy to 200+ recipients** | Use spintax (vary sentence structure, not just words) + AI-personalized first lines. |
| **>5 follow-ups in a sequence** | Trim to 3-4. Beyond 5 = 2.8× spam complaints. |

### Authentication (one-time per domain, then 2×/year)
| Case | Manager action |
|---|---|
| **New domain** | SPF (`include:... ~all`, never `+all`), DKIM 2048-bit at `default._domainkey`, DMARC `p=none` first 2 weeks. |
| **Domain 30d+ old, clean** | Tighten DMARC to `p=quarantine` then `p=reject`. |
| **DKIM 1024-bit (old)** | Upgrade to 2048-bit. Rotate selectors twice a year. |
| **Sending 5k+/day to a provider** | DMARC MUST be at enforcement or Microsoft/Google reject outright. |

---

## Part 4 — Priority Order (what to do first)

1. **Now (config):** lower warmup volume 40→30 (Part 2A).
2. **Now (refine):** maintenance-warmup trickle for active inboxes (Part 2B) — fixes a rule that 2026 says is wrong.
3. **Check + maybe build:** spam-complaint-rate signal (Part 2C) — pending API.
4. **Build:** volume-cap flag (Part 2D) — cheap, high value.
5. **Build later:** DNS auth checker (Part 2E) — catches the DKIM-2048/DMARC gaps automatically.
6. **Manager, ongoing:** the Part 3 per-case guide is the daily/weekly playbook.

The system handles the *watching + safe automation*. The manager handles *DNS, copy, list, and scale-vs-retire decisions* — now with 2026-current thresholds.
