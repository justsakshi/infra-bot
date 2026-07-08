#!/usr/bin/env python3
"""Blacklist monitor (plan §5.6). Weekly cron; read-only (no Smartlead writes).

Checks every sending domain across all clients against the SERIOUS
domain-reputation blacklists (Spamhaus DBL, SURBL, URIBL — configured in
BLACKLIST_ZONES). Pay-to-delist noise lists (UCEProtect etc.) are deliberately
not checked. Uses Google DNS-over-HTTPS, same as check_dns.py.

A domain is "listed" when {domain}.{zone} resolves to an A record. Results are
printed (Slack-digest friendly) and upserted to Mongo (blacklist_checks) so the
health workbook / trend queries can pick them up later.

Usage:
    python3 blacklist_monitor.py             # all client domains
    python3 blacklist_monitor.py example.com # ad-hoc single domain
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import httpx

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.client_filter import is_excluded_inbox
from smartlead.config import BLACKLIST_ZONES, BLACKLIST_COLLECTION, HEALTH_HISTORY_DB
from smartlead.processing import get_domain_from_email

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None

_DOH_URL = "https://dns.google/resolve"
_CONCURRENCY = 10

# DNSBL sentinel answers that mean "your resolver is blocked / invalid query",
# NOT a listing (public resolvers like Google DoH trip these intermittently).
_SENTINEL_IPS = {"127.0.0.1", "127.0.0.255", "127.255.255.254", "127.255.255.255"}

# SURBL multi bitmask (last octet) -> which SURBL sub-list fired
_SURBL_BITS = {8: "PH-phishing", 16: "MW-malware", 64: "ABUSE", 128: "CR-cracked"}


def _decode(zone: str, ip: str) -> str:
    """Human label for a DNSBL answer IP (e.g. SURBL 127.0.0.64 -> ABUSE)."""
    try:
        last = int(ip.rsplit(".", 1)[1])
    except (ValueError, IndexError):
        return ip
    if "surbl" in zone:
        flags = [name for bit, name in _SURBL_BITS.items() if last & bit]
        return "+".join(flags) if flags else ip
    return ip


async def _listed_ips(client: httpx.AsyncClient, domain: str, zone: str) -> list[str] | None:
    """Listing answer IPs (sentinels filtered), [] = clean, None = lookup error."""
    try:
        resp = await client.get(_DOH_URL, params={"name": f"{domain}.{zone}", "type": "A"})
        resp.raise_for_status()
        data = resp.json()
        if data.get("Status") == 3:  # NXDOMAIN = not listed
            return []
        ips = [a.get("data", "") for a in data.get("Answer", []) or [] if a.get("type") == 1]
        return [ip for ip in ips if ip and ip not in _SENTINEL_IPS]
    except Exception as exc:  # noqa: BLE001
        print(f"  [Blacklist] lookup error {domain} @ {zone}: {exc}", file=sys.stderr)
        return None


async def _check_domain(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                        domain: str) -> dict:
    async with sem:
        hits: list[str] = []
        errors: list[str] = []
        for zone, label in BLACKLIST_ZONES.items():
            ips = await _listed_ips(client, domain, zone)
            if ips:
                codes = ",".join(_decode(zone, ip) for ip in ips)
                hits.append(f"{label} ({codes})")
            elif ips is None:
                errors.append(label)
        return {"domain": domain, "listed_on": hits, "lookup_errors": errors}


async def _collect_domains() -> dict[str, set[str]]:
    """{domain: {client, ...}} across all Smartlead accounts (excluded clients dropped)."""
    domains: dict[str, set[str]] = {}
    for acc in discover_accounts():
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                accounts = await c.list_email_accounts()
        except Exception as exc:  # noqa: BLE001
            print(f"  [Blacklist] {acc.name}: account list failed: {exc}")
            continue
        for a in accounts:
            email = str(a.get("from_email", "")).strip()
            if not email or is_excluded_inbox(a):
                continue
            dom = get_domain_from_email(email)
            if dom:
                domains.setdefault(dom, set()).add(acc.name)
    return domains


def _save(results: list[dict], today: str) -> None:
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [Blacklist] Mongo unavailable - results not persisted.")
        return
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][BLACKLIST_COLLECTION]
        col.create_index([("domain", 1), ("date", 1)], unique=True)
        ops = [
            UpdateOne({"domain": r["domain"], "date": today}, {"$set": r}, upsert=True)
            for r in results
        ]
        col.bulk_write(ops, ordered=False)
    except Exception as exc:  # noqa: BLE001
        print(f"  [Blacklist] Mongo write failed: {exc}")


async def main() -> None:
    today = date.today().strftime("%Y-%m-%d")
    if len(sys.argv) > 1:
        domain_clients = {sys.argv[1].strip().lower(): {"adhoc"}}
    else:
        domain_clients = await _collect_domains()
    print(f"[Blacklist] checking {len(domain_clients)} domain(s) against "
          f"{', '.join(BLACKLIST_ZONES.values())}")

    sem = asyncio.Semaphore(_CONCURRENCY)
    async with httpx.AsyncClient(timeout=10.0) as client:
        results = await asyncio.gather(
            *(_check_domain(client, sem, d) for d in sorted(domain_clients)))

    records = []
    listed = []
    for r in results:
        r["date"] = today
        r["clients"] = sorted(domain_clients.get(r["domain"], set()))
        records.append(r)
        if r["listed_on"]:
            listed.append(r)

    if listed:
        print(f"[Blacklist] 🚨 {len(listed)} domain(s) LISTED on serious blacklists:")
        for r in listed:
            print(f"    {r['domain']:35} {', '.join(r['listed_on']):30} "
                  f"clients: {', '.join(r['clients'])}")
        print("  ACTION: pause campaigns on these domains, submit delisting request, "
              "escalate to Zapmail if DNS-related. Domain <30d old -> replace instead.")
    else:
        print("[Blacklist] ✅ all domains clean.")
    errs = sum(1 for r in results if r["lookup_errors"])
    if errs:
        print(f"[Blacklist] ⚠ {errs} domain(s) had lookup errors (unknown status).")

    _save(records, today)


if __name__ == "__main__":
    asyncio.run(main())
