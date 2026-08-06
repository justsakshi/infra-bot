"""Discover Expandi workspaces from EXPANDEE_API_KEY[_<NAME>] env vars.

Mirrors heyreach_accounts, with one difference: Expandi needs a key AND a
secret, so a workspace only exists when both are present. A key with no secret
is a half-configured workspace — it is skipped with a warning rather than
failing later inside an HTTP call, where the cause would be much less obvious.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

_KEY_PREFIX = "EXPANDEE_API_KEY"
_SECRET_PREFIX = "EXPANDEE_SECRET"


@dataclass
class ExpandiWorkspace:
    name: str
    api_key: str
    api_secret: str


def discover_expandi_workspaces() -> list[ExpandiWorkspace]:
    """Return every workspace with both a key and a secret.

    Accepts the unsuffixed pair (EXPANDEE_API_KEY / EXPANDEE_SECRET), which is
    named BETTRDATA since that is the account it belongs to, and any suffixed
    pair (EXPANDEE_API_KEY_FOO / EXPANDEE_SECRET_FOO) named after the suffix.
    """
    out: list[ExpandiWorkspace] = []
    for key_var, key_val in sorted(os.environ.items()):
        if not key_var.startswith(_KEY_PREFIX) or not key_val:
            continue
        suffix = key_var[len(_KEY_PREFIX):].lstrip("_")
        secret_var = f"{_SECRET_PREFIX}_{suffix}" if suffix else _SECRET_PREFIX
        secret = os.getenv(secret_var, "").strip()
        if not secret:
            print(f"[Expandi] {key_var} has no matching {secret_var} — skipping")
            continue
        out.append(ExpandiWorkspace(
            name=(suffix or "BETTRDATA").upper(),
            api_key=key_val.strip(),
            api_secret=secret,
        ))
    return out
