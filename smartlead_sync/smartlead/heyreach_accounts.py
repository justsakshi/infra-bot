"""Discover HeyReach workspaces from HEYREACH_API_KEY_<NAME> env vars."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

_PREFIX = "HEYREACH_API_KEY_"


@dataclass
class HeyReachWorkspace:
    name: str
    api_key: str


def discover_heyreach_workspaces() -> list[HeyReachWorkspace]:
    out: list[HeyReachWorkspace] = []
    for key, value in sorted(os.environ.items()):
        if key.startswith(_PREFIX) and value:
            out.append(HeyReachWorkspace(name=key[len(_PREFIX):], api_key=value.strip()))
    return out
