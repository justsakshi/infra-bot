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

# Account name -> deliverability test tab name(s). Maps are merged; "fail" wins.
# (Moved out of run.py so adding a client is a config edit, not a code edit.)
ACCOUNT_DELIVERABILITY_TABS: dict[str, list[str]] = {
    "Belardi Wong": ["Belardiwong"],
    "PRECISE_LEADS": ["Melior", "Precise Leads", "Avench", "OSC", "StaffAI", "Bettrdata"],
    "DARLEAN": ["Darlean new"],
    "MYTHIC": [],  # separate workspace; no deliverability tab yet -> all inboxes "untested"
}


@dataclass
class AccountConfig:
    """Represents a single Smartlead account discovered from environment."""

    name: str
    api_key: str
    sheet_id: str = field(default_factory=lambda: DEFAULT_SHEET_ID)
