# Spec — Complaint-Rate Trend Monitor (build #1b)

**Written:** 2026-07-29 · **Status:** spec, nothing built
**Implements:** master-plan build #1b — "the trend loop" ([`DELIVERABILITY_MASTER_PLAN_CONSOLIDATED_2026-07-29.md`](./DELIVERABILITY_MASTER_PLAN_CONSOLIDATED_2026-07-29.md) §4)
**One-line goal:** capture per-domain **spam-complaint rate over time** so we get notified *before* a domain lands in spam — the leading indicator that placement tests lag.

---

## 0. Why this is a new module, not a change to an existing one

Everything live today is point-in-time. `blacklist_monitor.py` answers "listed right now?"; `eg_test_executor.py` answers "where did a seed land right now?". Neither answers "is the complaint trend rising toward the cliff?". Google Postmaster + Microsoft SNDS are the only feeds that give a **daily time-series of recipient-side complaint signal**, which is the exact thing the standup asked to be notified on. It follows the `blacklist_monitor.py` shape exactly: standalone cron entrypoint, `discover_accounts()`, async, per-domain+date Mongo upsert, `notify.post_digest` on breach, control/self-test discipline so we never silently report "clean".

New file: `smartlead_sync/postmaster_monitor.py`.

---

## 1. The two feeds are NOT symmetric — design around it

| | Google Postmaster Tools | Microsoft SNDS |
|---|---|---|
| Keyed by | **domain** | **sending IP** |
| Auth | OAuth2 / service account + domain verified in Postmaster | per-IP "automated data access" key (URL query param) |
| Transport | REST JSON (`gmailpostmastertools.googleapis.com`) | CSV over HTTPS GET (`postmaster.live.com/snds/data.aspx?key=…`) |
| Signal we want | `spamRate` (0.0-1.0), `domainReputation`, `ipReputation`, spf/dkim/dmarc success % | complaint-rate band, spam-trap hits, filter result, RCPT volume |
| Data lag | 1-2 days | ~1 day |
| Populates only when | domain sends a meaningful volume to Gmail that day | IP sends to Outlook/Hotmail that day |

**The IP-vs-domain asymmetry is the catch, and it interacts with our worst finding.** SNDS reports per IP, and **68/97 of our domains share IP `52.15.49.97`** (per [`PLACEMENT_FINDINGS_2026-07-27.md`](./PLACEMENT_FINDINGS_2026-07-27.md)). So SNDS gives one blended Microsoft signal for that whole shared-IP group, not per-domain resolution. Treat SNDS as a **fleet-group early warning for the Microsoft side**, and Postmaster as the **per-domain** signal for the Gmail side. Do not pretend SNDS is per-domain.

---

## 2. Data model (Mongo, trend-friendly — mirrors `blacklist_checks`)

Two collections in `HEALTH_HISTORY_DB`, both upsert on the natural key + date so a daily cron builds a clean time-series (same pattern as `blacklist_monitor._save`).

**`postmaster_domain_daily`** — unique index `(domain, date)`:
```
{ domain, date: "YYYY-MM-DD", clients: [..],
  spam_rate: 0.0007,            # float 0..1, from Postmaster spamRate
  domain_reputation: "HIGH",    # HIGH|MEDIUM|LOW|BAD
  ip_reputation: "HIGH",
  spf_success: 0.99, dkim_success: 0.99, dmarc_success: 0.99,
  status: "ok" | "no-data" | "error",   # no-data = verified but no traffic that day
  source: "postmaster" }
```

**`snds_ip_daily`** — unique index `(ip, date)`:
```
{ ip: "52.15.49.97", date, domains: [..], clients: [..],
  rcpt_commands: 1234, complaint_band: "<0.1%" | "0.1-1%" | ...,
  trap_hits: 0, filter_result: "GREEN" | "YELLOW" | "RED",
  status: "ok" | "no-data" | "error", source: "snds" }
```

Trend is then a range query on these; no separate trend table. A 7-day slope per domain is computed at read time in the alert step (see §4).

---

## 3. Module shape (match `blacklist_monitor.py` line-for-line where it applies)

```
smartlead_sync/postmaster_monitor.py
  - Windows utf-8 reconfigure header (copy from blacklist_monitor.py:24-30)
  - _collect_domains()            # reuse the exact impl from blacklist_monitor (discover_accounts → domains→clients)
  - _collect_sending_ips()        # resolve each domain's A record (DoH, like check_dns) → {ip: {domains, clients}}
  - Google leg:
      _postmaster_client()        # service-account creds via google-auth; scope gmailpostmastertools.readonly
      _fetch_domain(domain)       # GET trafficStats, take yesterday's row; None on 404-not-verified
      _CONTROL: assert the API returns SOMETHING for ≥1 known-verified domain,
                else mark the whole Google leg INCONCLUSIVE (blacklist_monitor control discipline)
  - Microsoft leg:
      _fetch_snds(key)            # GET data.aspx?key=…, parse CSV rows (one per IP per day)
      key from env SNDS_ACCESS_KEY; if unset → leg 'skipped', not 'clean'
  - _save_domains(), _save_ips()  # bulk upsert, copy blacklist_monitor._save Mongo guard
  - main(): fetch both legs, persist, then the alert step (§4)
```

Reuse, don't reinvent: `discover_accounts`, `SmartleadClient`, `is_excluded_inbox`, `get_domain_from_email`, `notify.post_digest`, the Mongo guard (`MONGO_URI` unset → print, don't crash), and the DoH helper style from `check_dns.py`.

---

## 4. Alert logic — the whole point (leading-indicator tripwires)

After persisting today's rows, compute two things per domain and alert on breach. Thresholds from master-plan §4 (community-confirmed):

**Absolute tripwires (alert immediately):**
- `spam_rate ≥ 0.003` (0.3%, Gmail throttle line) → **CRITICAL**
- `0.001 ≤ spam_rate < 0.003` (0.1-0.3%) → **WARN**
- `domain_reputation` in {LOW, BAD} → **CRITICAL**
- SNDS `filter_result == RED` or `complaint_band ≥ 0.1%` → **CRITICAL** (fleet-group, names the shared IP + its domains)

**Trend tripwire (the leading indicator — this is what "notify before spam" means):**
- Pull the last 7 daily `spam_rate` values for the domain. If the 7-day trend is **monotonically rising AND today ≥ 2× the 7-day-ago value AND today ≥ 0.0005**, alert **RISING** even if still under the absolute WARN line. A domain climbing 0.02%→0.04%→0.08% is dying; catch it three days before it trips 0.1%.
- Require ≥4 non-`no-data` points in the window before trusting a slope (low-volume domains produce sparse data).

**Alert delivery:** one consolidated `notify.post_digest` per run, grouped CRITICAL → RISING → WARN, each line naming domain, client(s), today's rate, and the 7-day mini-trend (`0.02→0.04→0.08%`). Mirror the Spamhaus-DBL-only restraint in `blacklist_monitor.py`: do not page on every WARN daily — WARN goes to the digest, only CRITICAL and RISING page. Suppress a repeat alert for the same domain+band within 24h (dedupe on last alert state in Mongo) so a stuck-WARN domain doesn't spam Slack.

**Feed the health score.** Emit the per-domain verdict into the existing health pipeline (`smartlead.health`) as a new weighted signal, so complaint trend shows up in the Inbox Health workbook alongside bounce. Weight it high — it is a real recipient signal, unlike warmup (which was de-weighted 25→10). Exact weight: start at 20, tune against real data.

---

## 5. Cadence & orchestration

- **Standalone daily cron**, like `blacklist_monitor.py` (which is weekly). Postmaster/SNDS data lags 24-48h, so daily at a fixed morning time is right; more often is wasted calls.
- Do NOT fold into `run.py` (that's the Sheets-sync orchestrator on its own schedule). Keep monitors independent so one failing feed can't take down the sheet sync — the same isolation `blacklist_monitor` already has.
- Add to whatever cron/scheduler drives the other `smartlead_sync` monitors.

---

## 6. Setup prerequisites (must happen before the module returns data — user actions, no code)

These gate the whole build; the code is useless until they're done. Track as a checklist:

1. **Google Postmaster:** verify each active sending domain in Google Postmaster Tools (DNS TXT, one per domain — 174 domains is a lot; script the TXT record generation, but verification is per-domain). Data only appears for domains sending enough volume to Gmail.
2. **Postmaster API auth:** create/point a service account with the `gmailpostmastertools.readonly` scope; you already have `service_account.json` + `googleapis` for Sheets — check whether the same SA can be granted Postmaster access or if a new one is cleaner.
3. **Microsoft SNDS:** register the sending IP(s) at postmaster.live.com (primarily `52.15.49.97`), request the automated-data-access key, put it in `.env` as `SNDS_ACCESS_KEY`. Because of the shared IP, one registration covers most of the fleet's Microsoft signal.

**Reality check to set expectations:** Postmaster shows data only for domains with meaningful Gmail volume, and many of our 174 domains send little per day, so early coverage will be partial — that's expected, not a bug. The module must render `no-data` honestly (not as "healthy"), exactly like the blacklist control-domain discipline.

---

## 7. Config additions (`smartlead/config.py`)

```
POSTMASTER_DOMAIN_COLLECTION = "postmaster_domain_daily"
SNDS_IP_COLLECTION = "snds_ip_daily"
SPAM_RATE_WARN = 0.001      # 0.1%
SPAM_RATE_CRIT = 0.003      # 0.3%
COMPLAINT_TREND_WINDOW_DAYS = 7
COMPLAINT_TREND_MIN_POINTS = 4
```
Env: `SNDS_ACCESS_KEY`, plus reuse existing Google SA config. `MONGO_URI` already used.

---

## 8. Test plan

- **Unit:** CSV parse for SNDS (real sample rows — bands, RED/YELLOW/GREEN, trap counts); Postmaster JSON parse (spamRate float, reputation enum); trend detector (feed synthetic 7-day series: flat, rising-2x, sparse-with-gaps → assert RISING fires only on the real climb).
- **Control/self-test:** Google leg asserts a known-verified domain returns non-null; on total-null, whole leg → INCONCLUSIVE digest line, never silent-clean. SNDS leg with no key → `skipped`, not `clean`.
- **Dedupe:** same domain WARN twice in 24h → one Slack line.
- **Dry-run flag:** `--dry-run` prints the digest instead of posting (match the EmailGuard executor's dry-run-by-default posture).
- Follow the repo rule: versioned nothing here, but log metrics (row counts, per-leg status) like the other monitors' print lines.

---

## 9. Build order within this module

1. Data model + Mongo upsert + `_collect_domains`/`_collect_sending_ips` (cheapest, reuses blacklist_monitor).
2. Google Postmaster leg + control self-test.
3. Persist + basic absolute-threshold digest (usable end-to-end here, even before SNDS).
4. SNDS leg (blocked on the access key; ship #1-3 first).
5. Trend detector + RISING alert + dedupe.
6. Health-score integration.

Ship 1-3 as the first PR — it's the per-domain Gmail trend loop and delivers the core "notify before spam" value on its own. SNDS and trend are follow-ups.
