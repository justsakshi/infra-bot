"""Discover Smartlead accounts from environment variables.

Convention:
    SMARTLEAD_API_KEY           -> "Main" account (default)
    SMARTLEAD_API_KEY_<NAME>    -> additional accounts
    SMARTLEAD_SHEET_ID_<NAME>   -> per-account sheet override (falls back to SHEET_ID)
"""

from __future__ import annotations

import os

from smartlead.config import AccountConfig, DEFAULT_SHEET_ID


def discover_accounts() -> list[AccountConfig]:
    """Return a list of :class:`AccountConfig` found in the environment."""
    accounts: list[AccountConfig] = []

    # 1. Default / Main account
    default_key = os.getenv("SMARTLEAD_API_KEY")
    default_sheet = os.getenv("SHEET_ID", DEFAULT_SHEET_ID)

    if default_key:
        accounts.append(AccountConfig(name="Belardi Wong", api_key=default_key, sheet_id=default_sheet))

    # 2. Additional named accounts (SMARTLEAD_API_KEY_*)
    skip = {"SMARTLEAD_API_KEY", "SMARTLEAD_API_KEY_CLIENT_NAME"}
    for key, value in sorted(os.environ.items()):
        if key.startswith("SMARTLEAD_API_KEY_") and key not in skip:
            name = key[len("SMARTLEAD_API_KEY_"):]
            sheet = os.getenv(f"SMARTLEAD_SHEET_ID_{name}", default_sheet)
            accounts.append(AccountConfig(name=name, api_key=value, sheet_id=sheet))

    return accounts
