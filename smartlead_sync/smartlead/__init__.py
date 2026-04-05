"""Smartlead Dashboard - modular package for fetching campaign data and syncing to Google Sheets."""

from smartlead.api import SmartleadClient
from smartlead.sheets import SheetsWriter, DeliverabilityReader
from smartlead.processing import process_inbox_availability, detect_provider, get_domain_from_email
from smartlead.accounts import discover_accounts
from smartlead.mock import get_mock_data

__all__ = [
    "SmartleadClient",
    "SheetsWriter",
    "DeliverabilityReader",
    "process_inbox_availability",
    "detect_provider",
    "get_domain_from_email",
    "discover_accounts",
    "get_mock_data",
]
