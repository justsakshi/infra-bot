"""Centralised configuration for the Smartlead dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# ── Smartlead API ────────────────────────────────────────────────────────────
BASE_URL: str = "https://server.smartlead.ai/api/v1"
API_CHUNK_SIZE: int = 5           # concurrent detail-fetches per batch (lowered to ease 429s)
API_TIMEOUT: float = 60.0         # httpx timeout (seconds)
API_CHUNK_DELAY: float = 2.0      # pause between batches to respect rate-limits
API_MAX_RETRIES: int = 4          # retry count on 429 / 5xx / network errors
API_RETRY_BASE_DELAY: float = 2.0 # exponential backoff base (seconds)
API_RETRY_MAX_DELAY: float = 30.0 # cap for Retry-After / backoff (seconds)

# Campaign statuses considered "active" for filtering
ACTIVE_STATUSES: set[str] = {"ACTIVE", "START", "PAUSED", "PAUSE"}

# ── Google Sheets ────────────────────────────────────────────────────────────
DEFAULT_SHEET_ID: str = os.getenv("SHEET_ID", "197vytufJI-r2ruOrkmox4tm_r1FNoE_rihuUE254zhU")
SERVICE_ACCOUNT_FILE: str = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

# Deliverability / Belardiwong test sheet
TEST_SHEET_ID: str = os.getenv("TEST_SHEET_ID", "1CgxN8hKgqL2rouCOkQKRAyIzessMVGbt81RSc0Itgyg")
TEST_TAB_NAME: str = os.getenv("TEST_TAB_NAME", "Belardiwong")

# ── Availability Rules ──────────────────────────────────────────────────────
MIN_WARMUP_REP_PCT: float = 90.0  # Rule 2: minimum warmup reputation %
MAX_INBOX_LIMIT: int = 35         # Max safe sending limit per inbox per day
TEST_STALE_DAYS: int = 14         # deliverability test older than this -> "stale"

# ── Warmup ramp classification ───────────────────────────────────────────────
WARMUP_RAMP_DAYS: int = 21        # warmup younger than this is still "warming"

# ── Master "All Inboxes" tab ─────────────────────────────────────────────────
MASTER_SHEET_ID: str = os.getenv("MASTER_SHEET_ID", DEFAULT_SHEET_ID)
MASTER_TAB_NAME: str = os.getenv("MASTER_TAB_NAME", "All Inboxes")
DELIVERABILITY_QUEUE_TAB_NAME: str = os.getenv("DELIVERABILITY_QUEUE_TAB_NAME", "Deliverability Queue")

# Account name -> deliverability test tab name(s). Maps are merged; "fail" wins.
# (Moved out of run.py so adding a client is a config edit, not a code edit.)
_ACCOUNT_DELIVERABILITY_TABS_RAW: dict[str, list[str]] = {
    "Belardi Wong": ["Belardiwong"],
    "PRECISE_LEADS": ["Melior", "Precise Leads", "Bettrdata"],  # Avench(old), OSC+StaffAI(paused Q3) dropped
    "DARLEAN": ["Darlean new"],
    # BettrData became its own Smartlead account (previously a PRECISE_LEADS
    # sub-client). Without an entry here it fell through to the default tab,
    # which meant reading another client's deliverability results.
    "BETTRDATA": ["Bettrdata"],
}


class _CaseInsensitiveTabs(dict):
    """Account-name -> deliverability tabs, tolerant of key casing.

    Account names come from env-var suffixes, and Windows upper-cases env-var
    names while Linux preserves them. So SMARTLEAD_API_KEY_Darlean produces
    "DARLEAN" locally and "Darlean" on Render — a plain dict lookup then finds
    the tabs in dev and silently returns the default in production, which reads
    as "this client has no deliverability data" rather than as an error.
    """

    def get(self, key, default=None):  # type: ignore[override]
        if key in self:
            return self[key]
        want = str(key).strip().lower()
        for k, v in self.items():
            if k.strip().lower() == want:
                return v
        return default


ACCOUNT_DELIVERABILITY_TABS = _CaseInsensitiveTabs(_ACCOUNT_DELIVERABILITY_TABS_RAW)

# Old/inactive clients — exclude their inboxes from ALL tracking (health score,
# workbook, placement tests, warmup). Matched against an inbox's tags AND domain
# (case-insensitive substring). Don't waste API calls / credits on dead clients.
EXCLUDED_CLIENTS: set[str] = {"avench", "monarch", "capsule", "gofloater",
                              # paused for Q3 2026 per Sri (2026-07-07) - re-add by removing these:
                              "osc", "staffai"}

# ── HeyReach ─────────────────────────────────────────────────────────────────
HEYREACH_BASE_URL: str = "https://api.heyreach.io/api/public"
# Expandi is served from liaufa.com — that is correct, not a misconfiguration.
# The `/open-api/v2` suffix matters: `/api/v1` alone 401s on every endpoint,
# which looks like an auth failure but is a wrong path.
EXPANDI_BASE_URL: str = "https://api.liaufa.com/api/v1/open-api/v2"

# ── Campaign Metrics dashboard ───────────────────────────────────────────────
CAMPAIGN_METRICS_TAB_NAME: str = os.getenv("CAMPAIGN_METRICS_TAB_NAME", "Campaign Metrics")
CAMPAIGN_METRICS_SHEET_ID: str = os.getenv("CAMPAIGN_METRICS_SHEET_ID", DEFAULT_SHEET_ID)


def _parse_client_sheets(raw: str) -> dict[str, str]:
    """Parse "CLIENT:sheet_id,CLIENT2:sheet_id2" into {CLIENT: sheet_id}.

    A malformed entry is skipped with a warning rather than raising: one typo
    should cost that client their standalone sheet, not stop the whole metrics
    run for everyone else.
    """
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        client, sep, sheet_id = chunk.partition(":")
        client, sheet_id = client.strip().upper(), sheet_id.strip()
        if not sep or not client or not sheet_id:
            print(f"[config] CAMPAIGN_METRICS_CLIENT_SHEETS: ignoring malformed "
                  f"entry {chunk!r} (want CLIENT:sheet_id)")
            continue
        out[client] = sheet_id
    return out


# Per-client standalone spreadsheets, so a client's numbers can be shared
# without exposing another client's campaigns — a tab in a shared file cannot
# be shared on its own, the whole file goes with it.
#
# Format: "BETTRDATA:1AbC...,DARLEAN:1XyZ..."
# A client absent here still appears on the shared tab and its per-client tab;
# it just has no standalone file. The service account must be an Editor on each
# spreadsheet or the write fails with a permission error naming that sheet.
CAMPAIGN_METRICS_CLIENT_SHEETS: dict[str, str] = _parse_client_sheets(
    os.getenv("CAMPAIGN_METRICS_CLIENT_SHEETS", ""))
# Smartlead accounts (by discovered name) to include in the metrics tab.
# Names come from discover_accounts(), which upper-cases the env-var suffix:
# SMARTLEAD_API_KEY_BettrData -> "BETTRDATA".
# This one list gates all three platforms — Smartlead accounts, HeyReach
# workspaces and Expandi workspaces are each filtered by name against it. A
# client missing here is discovered and then dropped with no error and no rows,
# which is indistinguishable from "that client has no campaigns", so the
# default carries every client the tab actually reports on.
#
# BETTRDATA pulls in both its Expandi (LinkedIn) and Smartlead (email)
# campaigns, since the name matches on both platforms.
CAMPAIGN_METRICS_CLIENTS: set[str] = {
    c.strip().upper()
    for c in os.getenv("CAMPAIGN_METRICS_CLIENTS", "DARLEAN,BETTRDATA").split(",")
    if c.strip()
}
# Smartlead lead-category ids treated as positive/neutral (from /leads/fetch-categories):
# 1=Interested, 2=Meeting Request, 5=Information Request. Excluded: 3=Not Interested,
# 4=Do Not Contact, 6=Out Of Office (auto), 7=Wrong Person, 9=Sender Originated Bounce.
SMARTLEAD_POSITIVE_CATEGORY_IDS: set[int] = {1, 2, 5}

# ── Inbox Health workbook ────────────────────────────────────────────────────
INBOX_HEALTH_TAB_NAME: str = os.getenv("INBOX_HEALTH_TAB_NAME", "Inbox Health")
HEALTH_NOTIFY_CHANNEL: str = os.getenv("HEALTH_NOTIFY_CHANNEL", os.getenv("REMINDER_CHANNEL", ""))
HEALTH_HISTORY_DB: str = os.getenv("HEALTH_HISTORY_DB", "infrabot")
HEALTH_HISTORY_COLLECTION: str = os.getenv("HEALTH_HISTORY_COLLECTION", "inbox_health_history")
# Daily snapshots of Expandi's all-time campaign counters. The API has no date
# filter (every spelling is accepted and silently ignored), so month-to-date is
# only obtainable by differencing today's snapshot against the first one on or
# before the month start.
EXPANDI_SNAPSHOT_COLLECTION: str = os.getenv("EXPANDI_SNAPSHOT_COLLECTION", "expandi_campaign_snapshots")
# Per-lead invite/accept timestamps from the messengers endpoint. Cached
# because that endpoint returns ~4.6 rows/second whatever the page size, and
# the accounts hold ~11k leads — a full sweep is ~98 minutes, far too slow to
# repeat daily. The cached fields never change once set, so later runs only
# fetch what is new.
EXPANDI_LEADS_COLLECTION: str = os.getenv("EXPANDI_LEADS_COLLECTION", "expandi_lead_activity")
# Pages of messengers to pull per campaign per run. The first runs backfill
# history; once caught up a day's new activity is far below this. Raise it to
# backfill faster, at the cost of a longer sync.
EXPANDI_LEAD_PAGES_PER_RUN: int = int(os.getenv("EXPANDI_LEAD_PAGES_PER_RUN", "40"))
# Rows per page. 100 is ~10x fewer requests than the default 10 for the same
# throughput, which means far fewer chances to hit a transient 500.
EXPANDI_LEAD_PAGE_SIZE: int = int(os.getenv("EXPANDI_LEAD_PAGE_SIZE", "100"))

# Health score weights (sum = 100) and thresholds. Tune after seeing real data.
# Reweighted 2026-07-27 after measuring what these components actually
# discriminate:
#   - bounce was 20 points awarded UNCONDITIONALLY because nothing populated
#     bounce_rate. The data existed all along at
#     /campaigns/{id}/mailbox-statistics (sent_count + sender_bounce_count per
#     mailbox); it is now aggregated per inbox across every campaign it sends
#     on, so bounce is a REAL signal again and carries 20 points. Live BW data
#     on wiring it up: individual inboxes at 6.7% and 10% bounce, far past the
#     3% auto-pause threshold.
#   - warmup reputation proved near-useless as a deliverability proxy:
#     sam@heybelardiwong.com showed 125 warmup sends, 125 inboxed, 100%
#     reputation, while real seed mail from that same mailbox was spam-foldered
#     4/4 by Microsoft. Warmup measures a friendly network's acceptance, not the
#     open internet's. Cut 25 -> 15.
#   - placement testing is the only ground truth we have, and EmailGuard makes
#     it cheap enough to run continuously. Raised 40 -> 60.
# Weights must sum to 100.
HEALTH_WEIGHTS: dict[str, int] = {"placement": 45, "warmup": 10, "bounce": 25, "connection": 20}
# Per-inbox bounce rate at/above this raises a P0. Matches Smartlead's own 3%
# campaign auto-pause threshold, so the inbox-level alarm and the campaign-level
# safety net fire on the same number.
HEALTH_BOUNCE_P0_PCT: float = float(os.getenv("HEALTH_BOUNCE_P0_PCT", "3.0"))
HEALTH_TEST_STALE_DAYS: int = 14   # test older than this starts decaying placement credit
HEALTH_TEST_DEAD_DAYS: int = 28    # test older than this treated as untested (neutral)
HEALTH_WARMUP_FULL: float = 99.0   # rep >= this -> full warmup credit
HEALTH_WARMUP_ZERO: float = 90.0   # rep <  this -> zero warmup credit
HEALTH_TREND_DROP: int = 8         # score drop >= this over 7d -> "declining" early warning

# R3 TOGGLE — warmup spam-count red flag. OFF = ignore (legacy). ON = an inbox
# with warmup spam landings (total_spam_count > threshold) is flagged as a P0
# spam-risk in the workbook (2026: landing in spam even during warmup = bad).
HEALTH_SPAM_FLAG_ENABLED: bool = os.getenv("HEALTH_SPAM_FLAG_ENABLED", "false").lower() == "true"
HEALTH_SPAM_COUNT_THRESHOLD: int = int(os.getenv("HEALTH_SPAM_COUNT_THRESHOLD", "3"))

# Volume-cap watch (2026): sending above this per day = reputation risk
# (Woodpecker 2025: 100+/day = 4.3x bounce; safe band 20-50). Read-only flag
# in the workbook — tells the manager to lower the campaign cap.
VOLUME_SAFE_MAX: int = int(os.getenv("VOLUME_SAFE_MAX", "50"))

# ── Auto placement-test (health phase 3) ─────────────────────────────────────
SMARTDELIVERY_BASE_URL: str = "https://smartdelivery.smartlead.ai/api/v1"
# DRY-RUN by default: executor selects + logs targets but does NOT create tests.
RETEST_ENABLED: bool = os.getenv("RETEST_ENABLED", "false").lower() == "true"
RETEST_PER_CLIENT_DAILY_CAP: int = int(os.getenv("RETEST_PER_CLIENT_DAILY_CAP", "2"))
RETEST_INBOX_THRESHOLD: float = float(os.getenv("RETEST_INBOX_THRESHOLD", "80"))
RETEST_MIN_TIME_MINUTES: int = 5   # SmartDelivery business rule: >= 5
PLACEMENT_TESTS_COLLECTION: str = os.getenv("PLACEMENT_TESTS_COLLECTION", "placement_tests")
PLACEMENT_RESULTS_COLLECTION: str = os.getenv("PLACEMENT_RESULTS_COLLECTION", "placement_results")

# ── Non-connected (Anjali-style) placement tests ─────────────────────────────
# Semi-automated flow: a human creates a NON-CONNECTED SmartDelivery test in the
# PRECISE_LEADS UI (the only step the public API cannot do — verified 2026-07-10:
# `test_with_sl_account` is rejected on the create endpoint, and connected mode
# requires the sender imported into the same account, which is vetoed). The
# human names the test EXACTLY the inbox email and pastes the seed list +
# Track-ID into the "NC Tests" sheet tab. The executor does the rest: sends the
# test email from the inbox via a one-off campaign in the inbox's OWN Smartlead
# account, polls the PL test, writes results to Mongo + the API Tests tab.
# DRY-RUN by default: logs the full plan, mutates nothing.
NC_TEST_ENABLED: bool = os.getenv("NC_TEST_ENABLED", "false").lower() == "true"
NC_TAB_NAME: str = os.getenv("NC_TAB_NAME", "NC Tests")
# Which account's SmartDelivery holds the non-connected tests (the credit pool).
NC_TEST_ACCOUNT: str = os.getenv("NC_TEST_ACCOUNT", "PRECISE_LEADS")
NC_CAMPAIGN_PREFIX: str = "NC Placement Test"
# Minutes between seed sends inside the one-off campaign (Smartlead minimum 3).
NC_MIN_GAP_MINUTES: int = int(os.getenv("NC_MIN_GAP_MINUTES", "3"))
# Extra slots added on top of the seed count when temporarily raising the
# inbox's daily limit for the send day (restored when the test completes).
NC_LIMIT_BUFFER: int = int(os.getenv("NC_LIMIT_BUFFER", "5"))
# Suggestion phase: which clients get worst-first "NEEDS TEST" rows written
# into the NC Tests tab (advisory — runs even in dry-run, like the capacity
# planner). Default = the credit-poor accounts: BW confirmed exhausted.
# MYTHIC removed 2026-09-01 (no longer a client). Darlean + PL keep the
# fully-automatic connected tester.
NC_SUGGEST_CLIENTS: list[str] = [
    c.strip() for c in os.getenv("NC_SUGGEST_CLIENTS", "Belardi Wong").split(",")
    if c.strip()
]
NC_SUGGEST_CAP: int = int(os.getenv("NC_SUGGEST_CAP", "5"))

# --- Campaign Metrics: campaigns the manual sheet deliberately leaves out ---
# The team's sheet is curated: it lists the verticals currently being reported
# on, not every campaign that exists. No status or lead-count rule separates
# these from the rest — a paused campaign with 3,662 leads is included in one
# vertical and excluded in another — so the distinction has to be stated, and
# revisited whenever the reported verticals change.
#
# Matched as a case-insensitive substring against the campaign name.
#   "marketing agenc" / "marketing agency" - vertical not on the report
#   "test copy"                            - copy tests, not real sends
#   "darlean-hvac"                         - 13-lead scratch campaign
#
# The remaining four are superseded runs: each vertical was relaunched on new
# inboxes and the manual sheet reports only the current run. Excluding them is
# what closes the last gap against that sheet (26,458 vs 26,255). They are
# matched on their full names because the replacement campaigns share a prefix
# — "non profits ai features" alone would also match the live
# "Non Profits AI Features (New inboxes)" and remove the wrong row.
# "bds/bddm" covers the four inherited Expandi campaigns (Outreach and
# Outreach 2, each Main Flow and Happy Path). They predate this team's work —
# the oldest launched 2025-08-27 — and carry 1,122 of Expandi's 2,210 leads, so
# leaving them in makes the client's own activity unreadable. Confirmed with
# the account owner 2026-08-07. Everything else on Expandi is kept: Agencies
# twain, all four Data Provider campaigns (including BD Select), and the four
# August campaigns.
CAMPAIGN_METRICS_EXCLUDE: list[str] = [
    s.strip().lower()
    for s in os.getenv(
        "CAMPAIGN_METRICS_EXCLUDE",
        "marketing agenc,test copy,darlean-hvac,"
        "non profits ai features campaign,"
        "events - ai features campaign,"
        "architecture & interior design  - new,"
        "construction - new,"
        "bds/bddm",
    ).split(",")
    if s.strip()
]
# Health-row building is expensive — run the suggestion phase only on the run
# whose UTC hour matches this (9:15 IST = 03:45 UTC -> default 3), once a day.
NC_SUGGEST_HOUR_UTC: int = int(os.getenv("NC_SUGGEST_HOUR_UTC", "3"))

# ── Auto inbox rotation (swap broken senders for healthy bench inboxes) ─────
# DRY-RUN by default: logs every intended add/reassign/remove, mutates nothing.
ROTATION_ENABLED: bool = os.getenv("ROTATION_ENABLED", "false").lower() == "true"
ROTATION_PER_CLIENT_DAILY_CAP: int = int(os.getenv("ROTATION_PER_CLIENT_DAILY_CAP", "2"))
# bench eligibility: reuse HEALTH_WARMUP_ZERO (rep >= 90) + fresh inbox test
ROTATION_LOG_COLLECTION: str = os.getenv("ROTATION_LOG_COLLECTION", "rotation_log")

# Bench-inbox fallback: a placement test must send through a campaign's
# sequence, so idle/bench inboxes (no campaign) get routed through the
# account's standing deliverability-test campaign — every account has one
# (confirmed 2026-07-09: "Deliverability test Campaign" in Belardi Wong,
# DARLEAN, MYTHIC, PRECISE_LEADS). Matched case-insensitively by name.
RETEST_TEST_CAMPAIGN_KEYWORD: str = os.getenv("RETEST_TEST_CAMPAIGN_KEYWORD", "deliverability test")

# TOGGLE — disable warmup on the sender inboxes before a placement test, then
# create the test with is_warmup=false, so the test measures REAL send-path
# deliverability (faster + true inbox placement) instead of warmup-network
# placement. Warmup is restored (re-enabled) after the test is created.
# OFF by default: legacy behavior (test with is_warmup=true, warmup untouched).
RETEST_DISABLE_WARMUP: bool = os.getenv("RETEST_DISABLE_WARMUP", "false").lower() == "true"

# ── Auto warmup (health phase 4) ─────────────────────────────────────────────
# DRY-RUN by default: planner logs would-enable/would-disable but changes nothing.
WARMUP_AUTO_ENABLED: bool = os.getenv("WARMUP_AUTO_ENABLED", "false").lower() == "true"

# POLICY (Avi): warmup ON permanently, never paused. When ON (default), the
# planner ONLY enables warmup on inboxes that are off — it never disables or
# trickles down. Underperformance is handled by lowering campaign volume, not
# warmup. Set false to fall back to the on/off/trickle behavior.
WARMUP_ALWAYS_ON: bool = os.getenv("WARMUP_ALWAYS_ON", "true").lower() == "true"

# R1 TOGGLE — conservative 2026 volume. OFF = legacy 40/day; ON = 30/day (2026
# safe band 20-50/day; 100+/day = 4.3x bounce per Woodpecker 2025).
WARMUP_CONSERVATIVE_VOLUME: bool = os.getenv("WARMUP_CONSERVATIVE_VOLUME", "false").lower() == "true"
WARMUP_PER_DAY: int = int(os.getenv("WARMUP_PER_DAY", "30" if WARMUP_CONSERVATIVE_VOLUME else "40"))
# CONFLICT (verified 2026-07-08): Smartlead's own sources disagree on this
# ramp number — api.smartlead.ai's parameter table + example payload use 2-3,
# but their blog says 5-10/day. We use 5 (inside the blog's band, above the
# API doc's default) — a judgment call, not a single settled vendor number.
WARMUP_DAILY_RAMPUP: int = int(os.getenv("WARMUP_DAILY_RAMPUP", "5"))
# CONFLICT (verified 2026-07-08): Smartlead's helpcenter (art. 52) + blog say
# 20-30% reply rate, "above 30% inadvisable" — but api.smartlead.ai's own
# parameter table recommends 30-40% and its example payload uses 30. We use
# the more conservative helpcenter band (20-30%), not the API doc's number.
WARMUP_REPLY_RATE: int = int(os.getenv("WARMUP_REPLY_RATE", "25"))

# ── State-based warmup profiles (2026-07-07 plan, docs/DELIVERABILITY_MASTER_PLAN.md §1) ──
# Warmup stays ON in every state; only the volume/ramp/reply-rate profile changes.
# NOTE: the 4-state framework itself (NEW/ACTIVE/IDLE/RECOVERING) is OUR OWN
# design, not a vendor concept — verified 2026-07-08, neither Smartlead nor
# Instantly documents a state-machine model. It's built to satisfy vendor
# guardrails, not copied from a vendor doc. Per-state sourcing:
#   NEW        -> vendor-confirmed: ramp to a <=40/day ceiling (api.smartlead.ai,
#                 verbatim "do not set above 40 for new accounts"); ramp-up
#                 feature is vendor-confirmed as "for fresh domains only,
#                 unnecessary once established" (helpcenter art. 164).
#   ACTIVE     -> vendor range is 15-25/day while campaigns run (Smartlead
#                 warmup-timeline doc + helpcenter art. 52, two overlapping but
#                 not identical ranges); we picked 20 as the midpoint. auto-adjust
#                 ON is vendor-confirmed to trim warmup ~7-10/day automatically
#                 (helpcenter art. 63, verbatim).
#   IDLE       -> NOT FOUND in any vendor source (verified 2026-07-08) — no
#                 vendor publishes a bench/insurance-inbox number. 20/day is
#                 our own call (low end of the general 20-40 band).
#   RECOVERING -> NOT FOUND in Smartlead docs. A possible Instantly blog figure
#                 (10-15/day for damaged reputation) surfaced but could only be
#                 verified via search-summary, not a raw fetch — treat as
#                 unconfirmed. 15/day is our own call, not a cited vendor number.
WARMUP_NEW_PER_DAY: int = int(os.getenv("WARMUP_NEW_PER_DAY", str(WARMUP_PER_DAY)))       # young inbox, full ramp
WARMUP_ACTIVE_PER_DAY: int = int(os.getenv("WARMUP_ACTIVE_PER_DAY", "20"))                # in live campaign
WARMUP_IDLE_PER_DAY: int = int(os.getenv("WARMUP_IDLE_PER_DAY", "20"))                    # insurance/bench (our own call — no vendor number)
# LONG_IDLE sub-state (2026-07-08, no volume change — see reasoning below):
# an inbox that hasn't sent a real campaign email in a long time has had no
# fresh human-engagement signal for a while. We do NOT raise its volume
# (no evidence 20/day is insufficient — see WARMUP_IDLE_PER_DAY note above);
# instead nudge reply rate up slightly (cheap, same lever as RECOVERING) and
# flag it for a placement retest before it's ever promoted back to ACTIVE.
WARMUP_LONG_IDLE_DAYS: int = int(os.getenv("WARMUP_LONG_IDLE_DAYS", "30"))
WARMUP_LONG_IDLE_REPLY_RATE: int = int(os.getenv("WARMUP_LONG_IDLE_REPLY_RATE", "28"))
WARMUP_RECOVERY_PER_DAY: int = int(os.getenv("WARMUP_RECOVERY_PER_DAY", "15"))            # rep < 90 / post-incident (our own call — no confirmed vendor number)
# Boost = raise the REPLY RATE, not volume (replies are the reputation signal;
# volume spikes without engagement read as spam) — principle is vendor-
# confirmed (Smartlead), the specific 30% figure is our own choice at the top
# of their 20-30% conservative band. Never above 30.
WARMUP_BOOST_REPLY_RATE: int = int(os.getenv("WARMUP_BOOST_REPLY_RATE", "30"))
# Enable Smartlead's smart-adjusting algorithm on campaign-attached inboxes
# (auto-reduces warmup 7-10/day while campaigns run; restores after).
WARMUP_AUTO_ADJUST_ACTIVE: bool = os.getenv("WARMUP_AUTO_ADJUST_ACTIVE", "true").lower() == "true"
# Retune tolerance: skip a volume change when |current - target| <= this
# (auto-adjust legitimately moves the number 7-10 below target; don't fight it).
WARMUP_RETUNE_TOLERANCE: int = int(os.getenv("WARMUP_RETUNE_TOLERANCE", "10"))

# R2 TOGGLE — maintenance-warmup trickle. OFF = fully disable warmup on active
# senders (legacy). ON = keep a low trickle instead (2026: never fully off, or
# deliverability erodes in 6-8 weeks). Trickle volume/day when ON:
WARMUP_MAINTENANCE_TRICKLE: bool = os.getenv("WARMUP_MAINTENANCE_TRICKLE", "false").lower() == "true"
WARMUP_TRICKLE_PER_DAY: int = int(os.getenv("WARMUP_TRICKLE_PER_DAY", "8"))
# an inbox counts as "actively sending" (warmup should be OFF) at/above this today
WARMUP_ACTIVE_SENT_MIN: int = int(os.getenv("WARMUP_ACTIVE_SENT_MIN", "1"))


# ── Provider-aware campaign-capacity caps (2026-07-09) ───────────────────────
# The raised max_email_per_day bucket (headroom for warmup) must NOT translate
# into more COLD sends. Providers tolerate very different cold volumes —
# Outlook inboxes were deliberately kept at 10/day cold and must stay there
# even though their total bucket is now bigger (10 cold + 20 warmup). These
# caps bound the sheet's "Avail. Capacity" column (precise-automator's volume
# budget) per provider, so no selector can assign more cold volume than the
# provider safely handles, regardless of the raw limit.
CAMPAIGN_CAP_OUTLOOK: int = int(os.getenv("CAMPAIGN_CAP_OUTLOOK", "10"))
CAMPAIGN_CAP_GMAIL: int = int(os.getenv("CAMPAIGN_CAP_GMAIL", "25"))
CAMPAIGN_CAP_DEFAULT: int = int(os.getenv("CAMPAIGN_CAP_DEFAULT", "15"))  # SMTP/other

# ── REMOVED 2026-07-10: warmup "headroom" fix ────────────────────────────────
# The premise was that max_email_per_day is ONE shared bucket for warmup +
# campaign sends (Smartlead's API reference does say "including warmup and
# campaign emails"). That is WRONG — or at least no longer true. Proven live:
# the account object carries a separate warmup allowance at
# warmup_details.max_email_per_day, and inboxes whose campaign cap was 10 were
# sending 41-43 warmup emails/day, which one shared bucket makes impossible.
# The job therefore only raised the COLD ceiling for no benefit; its 15 live
# changes were rolled back from the timestamped snapshot and the executor,
# cron entry, and capacity-side warmup reservation were all removed.
# Campaign volume is governed by CAMPAIGN_CAP_* below; warmup volume by the
# WARMUP_*_PER_DAY profiles.

# ── Bounce auto-protection sweep (plan §5.4) ────────────────────────────────
# DRY-RUN by default: logs which ACTIVE campaigns would get the Smartlead
# "High Bounce Rate Auto Protection" threshold set, changes nothing.
# Field: bounce_autopause_threshold (string %) on POST /campaigns/{id}/settings.
BOUNCE_PROTECT_ENABLED: bool = os.getenv("BOUNCE_PROTECT_ENABLED", "false").lower() == "true"
BOUNCE_PROTECT_THRESHOLD: str = os.getenv("BOUNCE_PROTECT_THRESHOLD", "3")  # % — pause at 3
BOUNCE_PROTECT_PER_RUN_CAP: int = int(os.getenv("BOUNCE_PROTECT_PER_RUN_CAP", "10"))

# ── Blacklist monitor (plan §5.6) ────────────────────────────────────────────
# Domain-reputation DNSBLs we treat as SERIOUS (act: pause + delist + escalate).
# UCEProtect-style pay-to-delist lists are deliberately excluded (noise).
BLACKLIST_ZONES: dict[str, str] = {
    "dbl.spamhaus.org": "Spamhaus DBL",
    "multi.surbl.org": "SURBL",
    "multi.uribl.com": "URIBL",
}
BLACKLIST_COLLECTION: str = os.getenv("BLACKLIST_COLLECTION", "blacklist_checks")
# Smartlead's campaign GET does not return bounce_autopause_threshold, so the
# only way to know protection is already on is to remember setting it.
BOUNCE_PROTECT_COLLECTION: str = os.getenv("BOUNCE_PROTECT_COLLECTION", "bounce_protect_applied")

# ── API-key health watchdog (read-only, no enable flag) ─────────────────────
KEY_HEALTH_COLLECTION: str = os.getenv("KEY_HEALTH_COLLECTION", "key_health_checks")

# ── EmailGuard placement testing (credit-free alternative to SmartDelivery) ──
# Placement truth is 40% of every inbox's health score, but Smartlead bills
# tests per account, so most inboxes carried stale or missing results.
# EmailGuard bills against a workspace-level quota and exposes the whole loop
# over an API, so testing runs unattended for every client from one pool.
# DRY-RUN by default: selects and logs targets, creates nothing.
EG_TEST_ENABLED: bool = os.getenv("EG_TEST_ENABLED", "false").lower() == "true"
EG_PER_RUN_CAP: int = int(os.getenv("EG_PER_RUN_CAP", "2"))
# Never spend the workspace down to zero — leave credits for a human to run an
# urgent ad-hoc test during an incident.
EG_MIN_QUOTA_RESERVE: int = int(os.getenv("EG_MIN_QUOTA_RESERVE", "1"))
# Smartlead rejects a campaign schedule with min_time_btw_emails < 3.
EG_SEND_GAP_MINUTES: int = int(os.getenv("EG_SEND_GAP_MINUTES", "3"))

# ── Capacity planner (read-only Monday advisory) ─────────────────────────────
CAPACITY_TAB_NAME: str = os.getenv("CAPACITY_TAB_NAME", "Capacity")
CAPACITY_PER_INBOX_CAP: int = int(os.getenv("CAPACITY_PER_INBOX_CAP", "30"))   # sends/day counted per healthy inbox
CAPACITY_HEADROOM: float = float(os.getenv("CAPACITY_HEADROOM", "1.2"))        # demand buffer (20%)
CAPACITY_BENCH_RATIO: float = float(os.getenv("CAPACITY_BENCH_RATIO", "0.25")) # bench = 25% of active fleet
CAPACITY_BENCH_MIN: int = int(os.getenv("CAPACITY_BENCH_MIN", "5"))
CAPACITY_LEAD_TIME_DAYS: int = int(os.getenv("CAPACITY_LEAD_TIME_DAYS", "35")) # purchase+warmup ≈ 5 weeks
DOMAIN_REGISTRY_COLLECTION: str = os.getenv("DOMAIN_REGISTRY_COLLECTION", "domain_registry")

# ── Per-domain reply-rate early warning ──────────────────────────────────────
REPLY_STATS_COLLECTION: str = os.getenv("REPLY_STATS_COLLECTION", "domain_reply_stats")
REPLY_ALERT_DROP_RATIO: float = float(os.getenv("REPLY_ALERT_DROP_RATIO", "0.7"))  # alert below 70% of own baseline
# Retuned 2026-07-08 (final-review finding): after the cumulative->daily-delta
# fix, this floor is compared against a true 1-DAY send count, not a summed
# multi-day total like before. At the fleet's per-inbox cap (20-30/day) x
# 2 inboxes/domain, a single-inbox domain sending normally lands ~20-30/day —
# the old 50 floor would have silently exempted many real domains from ever
# getting a drop alert. Lowered to 20 so a domain with just one active sender
# is still covered; still high enough to filter near-zero noise.
REPLY_ALERT_MIN_SENT: int = int(os.getenv("REPLY_ALERT_MIN_SENT", "20"))           # drop-alert send floor (1-day delta)
REPLY_ONE_PERCENT_MIN_SENT: int = int(os.getenv("REPLY_ONE_PERCENT_MIN_SENT", "200"))


@dataclass
class AccountConfig:
    """Represents a single Smartlead account discovered from environment."""

    name: str
    api_key: str
    sheet_id: str = field(default_factory=lambda: DEFAULT_SHEET_ID)
