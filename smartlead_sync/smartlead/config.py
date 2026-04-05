"""Centralised configuration for the Smartlead dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# ── Smartlead API ────────────────────────────────────────────────────────────
BASE_URL: str = "https://server.smartlead.ai/api/v1"
API_CHUNK_SIZE: int = 10          # concurrent detail-fetches per batch
API_TIMEOUT: float = 60.0         # httpx timeout (seconds)
API_CHUNK_DELAY: float = 2.0      # pause between batches to respect rate-limits
API_MAX_RETRIES: int = 3          # retry count on 429 / 5xx
API_RETRY_BASE_DELAY: float = 2.0 # exponential backoff base (seconds)

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


@dataclass
class AccountConfig:
    """Represents a single Smartlead account discovered from environment."""

    name: str
    api_key: str
    sheet_id: str = field(default_factory=lambda: DEFAULT_SHEET_ID)
