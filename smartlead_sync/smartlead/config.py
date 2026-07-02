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
ACCOUNT_DELIVERABILITY_TABS: dict[str, list[str]] = {
    "Belardi Wong": ["Belardiwong"],
    "PRECISE_LEADS": ["Melior", "Precise Leads", "OSC", "StaffAI", "Bettrdata"],  # Avench dropped (old)
    "DARLEAN": ["Darlean new"],
    "MYTHIC": ["Mythic "],  # Note: trailing space in actual tab name
}

# Old/inactive clients — exclude their inboxes from ALL tracking (health score,
# workbook, placement tests, warmup). Matched against an inbox's tags AND domain
# (case-insensitive substring). Don't waste API calls / credits on dead clients.
EXCLUDED_CLIENTS: set[str] = {"avench", "monarch", "capsule", "gofloater"}

# ── HeyReach ─────────────────────────────────────────────────────────────────
HEYREACH_BASE_URL: str = "https://api.heyreach.io/api/public"

# ── Campaign Metrics dashboard ───────────────────────────────────────────────
CAMPAIGN_METRICS_TAB_NAME: str = os.getenv("CAMPAIGN_METRICS_TAB_NAME", "Campaign Metrics")
CAMPAIGN_METRICS_SHEET_ID: str = os.getenv("CAMPAIGN_METRICS_SHEET_ID", DEFAULT_SHEET_ID)
# Smartlead accounts (by discovered name) to include in the metrics tab
CAMPAIGN_METRICS_CLIENTS: set[str] = {"DARLEAN"}
# Smartlead lead-category ids treated as positive/neutral (from /leads/fetch-categories):
# 1=Interested, 2=Meeting Request, 5=Information Request. Excluded: 3=Not Interested,
# 4=Do Not Contact, 6=Out Of Office (auto), 7=Wrong Person, 9=Sender Originated Bounce.
SMARTLEAD_POSITIVE_CATEGORY_IDS: set[int] = {1, 2, 5}

# ── Inbox Health workbook ────────────────────────────────────────────────────
INBOX_HEALTH_TAB_NAME: str = os.getenv("INBOX_HEALTH_TAB_NAME", "Inbox Health")
HEALTH_NOTIFY_CHANNEL: str = os.getenv("HEALTH_NOTIFY_CHANNEL", os.getenv("REMINDER_CHANNEL", ""))
HEALTH_HISTORY_DB: str = os.getenv("HEALTH_HISTORY_DB", "infrabot")
HEALTH_HISTORY_COLLECTION: str = os.getenv("HEALTH_HISTORY_COLLECTION", "inbox_health_history")

# Health score weights (sum = 100) and thresholds. Tune after seeing real data.
HEALTH_WEIGHTS: dict[str, int] = {"placement": 40, "warmup": 25, "bounce": 20, "connection": 15}
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

# ── Auto placement-test (health phase 3) ─────────────────────────────────────
SMARTDELIVERY_BASE_URL: str = "https://smartdelivery.smartlead.ai/api/v1"
# DRY-RUN by default: executor selects + logs targets but does NOT create tests.
RETEST_ENABLED: bool = os.getenv("RETEST_ENABLED", "false").lower() == "true"
RETEST_PER_CLIENT_DAILY_CAP: int = int(os.getenv("RETEST_PER_CLIENT_DAILY_CAP", "2"))
RETEST_INBOX_THRESHOLD: float = float(os.getenv("RETEST_INBOX_THRESHOLD", "80"))
RETEST_MIN_TIME_MINUTES: int = 5   # SmartDelivery business rule: >= 5
PLACEMENT_TESTS_COLLECTION: str = os.getenv("PLACEMENT_TESTS_COLLECTION", "placement_tests")
PLACEMENT_RESULTS_COLLECTION: str = os.getenv("PLACEMENT_RESULTS_COLLECTION", "placement_results")

# TOGGLE — disable warmup on the sender inboxes before a placement test, then
# create the test with is_warmup=false, so the test measures REAL send-path
# deliverability (faster + true inbox placement) instead of warmup-network
# placement. Warmup is restored (re-enabled) after the test is created.
# OFF by default: legacy behavior (test with is_warmup=true, warmup untouched).
RETEST_DISABLE_WARMUP: bool = os.getenv("RETEST_DISABLE_WARMUP", "false").lower() == "true"

# ── Auto warmup (health phase 4) ─────────────────────────────────────────────
# DRY-RUN by default: planner logs would-enable/would-disable but changes nothing.
WARMUP_AUTO_ENABLED: bool = os.getenv("WARMUP_AUTO_ENABLED", "false").lower() == "true"

# R1 TOGGLE — conservative 2026 volume. OFF = legacy 40/day; ON = 30/day (2026
# safe band 20-50/day; 100+/day = 4.3x bounce per Woodpecker 2025).
WARMUP_CONSERVATIVE_VOLUME: bool = os.getenv("WARMUP_CONSERVATIVE_VOLUME", "false").lower() == "true"
WARMUP_PER_DAY: int = int(os.getenv("WARMUP_PER_DAY", "30" if WARMUP_CONSERVATIVE_VOLUME else "40"))
WARMUP_DAILY_RAMPUP: int = int(os.getenv("WARMUP_DAILY_RAMPUP", "5"))
WARMUP_REPLY_RATE: int = int(os.getenv("WARMUP_REPLY_RATE", "20"))

# R2 TOGGLE — maintenance-warmup trickle. OFF = fully disable warmup on active
# senders (legacy). ON = keep a low trickle instead (2026: never fully off, or
# deliverability erodes in 6-8 weeks). Trickle volume/day when ON:
WARMUP_MAINTENANCE_TRICKLE: bool = os.getenv("WARMUP_MAINTENANCE_TRICKLE", "false").lower() == "true"
WARMUP_TRICKLE_PER_DAY: int = int(os.getenv("WARMUP_TRICKLE_PER_DAY", "8"))
# an inbox counts as "actively sending" (warmup should be OFF) at/above this today
WARMUP_ACTIVE_SENT_MIN: int = int(os.getenv("WARMUP_ACTIVE_SENT_MIN", "1"))


@dataclass
class AccountConfig:
    """Represents a single Smartlead account discovered from environment."""

    name: str
    api_key: str
    sheet_id: str = field(default_factory=lambda: DEFAULT_SHEET_ID)
