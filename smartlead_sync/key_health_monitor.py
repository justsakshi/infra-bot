#!/usr/bin/env python3
"""Smartlead API-key health check — the watchdog for silent blindness.

A rotated/revoked key does not break loudly: every job catches the 401, logs a
line nobody reads, and the client simply stops appearing in the sheets and
monitors. On 2026-07-10 one account's key was rotated three times in a day and
the fleet ran blind on it for hours; another account had been 401 on the server
for over a day before anyone noticed.

This job answers one question per account — "can we still see this client?" —
and makes the answer impossible to miss: non-zero exit code, a Slack alert when
a channel is configured, and a Mongo record so the failure has a timestamp.

Read-only against Smartlead. Safe to run anywhere, anytime; no enable flag.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import httpx

from smartlead.accounts import discover_accounts
from smartlead.config import BASE_URL, HEALTH_HISTORY_DB, KEY_HEALTH_COLLECTION

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None


async def _probe(account) -> dict:
    """One cheap authenticated read. Classifies the outcome precisely — a dead
    key (401) and an unreachable API (network/5xx) need different responses and
    must never be conflated."""
    started = datetime.now(timezone.utc)
    url = f"{BASE_URL}/email-accounts/"
    params = {"api_key": account.api_key, "offset": 0, "limit": 1}
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            resp = await c.get(url, params=params)
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        return {"client": account.name, "state": "UNREACHABLE",
                "detail": f"{type(exc).__name__}: {exc}"[:150]}

    if resp.status_code == 200:
        payload = resp.json()
        n = len(payload) if isinstance(payload, list) else 0
        return {"client": account.name, "state": "OK", "detail": f"{n} account(s) visible",
                "latency_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000)}
    if resp.status_code in (401, 403):
        return {"client": account.name, "state": "DEAD_KEY",
                "detail": f"HTTP {resp.status_code}: {resp.text[:120]}",
                "key_tail": account.api_key[-8:]}
    return {"client": account.name, "state": "API_ERROR",
            "detail": f"HTTP {resp.status_code}: {resp.text[:120]}"}


def _save(results: list[dict], checked_at: str) -> None:
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][KEY_HEALTH_COLLECTION]
        for r in results:
            col.update_one({"client": r["client"], "checked_at": checked_at},
                           {"$set": {**r, "checked_at": checked_at}}, upsert=True)
    except Exception as exc:  # noqa: BLE001
        print(f"  [KeyHealth] Mongo write failed (non-fatal): {exc}")


def _alert(dead: list[dict], unreachable: list[dict]) -> None:
    """Slack alert — only fires when a channel is configured, and only when
    something is actually broken (never a daily all-clear ping)."""
    from smartlead.config import HEALTH_NOTIFY_CHANNEL
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token or not HEALTH_NOTIFY_CHANNEL:
        return
    lines = ["*🔑 Smartlead API key alert*", ""]
    for r in dead:
        lines.append(f"   • *{r['client']}* — key rejected (`…{r.get('key_tail', '')}`). "
                     "Every job is blind for this client until a new key is set "
                     "in `.env` AND on Render.")
    for r in unreachable:
        lines.append(f"   • *{r['client']}* — Smartlead unreachable: {r['detail']}")
    from smartlead.notify import post_digest
    post_digest("\n".join(lines))


async def main() -> None:
    accounts = discover_accounts()
    if not accounts:
        print("[KeyHealth] No SMARTLEAD_API_KEY* found — nothing to check.")
        sys.exit(1)

    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[KeyHealth] probing {len(accounts)} account key(s) at {checked_at}")
    results = await asyncio.gather(*(_probe(a) for a in accounts))

    for r in sorted(results, key=lambda r: r["state"] != "OK", reverse=True):
        icon = {"OK": "✅", "DEAD_KEY": "🚨", "UNREACHABLE": "⚠", "API_ERROR": "⚠"}[r["state"]]
        print(f"  {icon} {r['client']:16} {r['state']:12} {r['detail']}")

    _save(results, checked_at)
    dead = [r for r in results if r["state"] == "DEAD_KEY"]
    unreachable = [r for r in results if r["state"] in ("UNREACHABLE", "API_ERROR")]

    if dead:
        names = ", ".join(r["client"] for r in dead)
        print(f"\n[KeyHealth] 🚨 DEAD KEY(S): {names}")
        print("  Every job (sync, warmup, retest, blacklist, capacity) is BLIND for "
              "these clients until fixed.")
        print("  Fix: Smartlead → Settings → API → copy key → update .env AND Render "
              "env vars (SMARTLEAD_API_KEY for Belardi Wong, SMARTLEAD_API_KEY_<NAME> "
              "for the rest), then re-run the daily sync.")
    if unreachable:
        print(f"\n[KeyHealth] ⚠ {len(unreachable)} account(s) could not be reached "
              "(network/API issue, not necessarily a key problem).")
    if not dead and not unreachable:
        print(f"\n[KeyHealth] ✅ all {len(results)} key(s) valid.")

    _alert(dead, unreachable)
    # Non-zero exit so a cron/CI wrapper can escalate without parsing logs.
    sys.exit(2 if dead else (1 if unreachable else 0))


if __name__ == "__main__":
    asyncio.run(main())
