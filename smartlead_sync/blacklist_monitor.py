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


# ── direct authoritative-NS queries (fallback when public resolvers are blocked) ──
# Spamhaus DBL returns NXDOMAIN and URIBL returns its 127.0.0.1 "refused" code
# to public resolvers (Google DoH included) — found 2026-07-10 via control
# domains: our DBL/URIBL coverage had silently been zero. Querying each zone's
# own authoritative nameserver directly bypasses the block. Raw UDP (no deps).

def _udp_query_a(name: str, server_ip: str, timeout: float = 5.0) -> list[str] | None:
    """Minimal DNS A query over UDP. Returns answer IPs, [] for NXDOMAIN/none,
    None on network error."""
    import random
    import socket
    import struct
    tid = random.randint(0, 65535)
    header = struct.pack(">HHHHHH", tid, 0x0100, 1, 0, 0, 0)
    qname = b"".join(bytes([len(p)]) + p.encode() for p in name.split(".") if p) + b"\x00"
    pkt = header + qname + struct.pack(">HH", 1, 1)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(timeout)
        s.sendto(pkt, (server_ip, 53))
        data, _ = s.recvfrom(4096)
        s.close()
    except OSError:
        return None
    if len(data) < 12 or data[:2] != pkt[:2]:
        return None
    if (data[3] & 0x0F) == 3:  # NXDOMAIN
        return []
    ancount = struct.unpack(">H", data[6:8])[0]
    ips: list[str] = []
    i = 12
    while i < len(data) and data[i] != 0:  # skip question name
        i += data[i] + 1
    i += 5
    for _ in range(ancount):
        while i < len(data):
            b = data[i]
            if b == 0:
                i += 1
                break
            if b & 0xC0:
                i += 2
                break
            i += b + 1
        if i + 10 > len(data):
            break
        rtype, _, _, rdlen = struct.unpack(">HHIH", data[i:i + 10])
        i += 10
        if rtype == 1 and rdlen == 4:
            ips.append(".".join(str(x) for x in data[i:i + 4]))
        i += rdlen
    return ips


async def _zone_ns_ip(client: httpx.AsyncClient, zone: str) -> str | None:
    """IP of one authoritative nameserver for a DNSBL zone (NS records are not
    blocked on public resolvers — only the listing A-lookups are)."""
    import socket
    try:
        resp = await client.get(_DOH_URL, params={"name": zone, "type": "NS"})
        ns_names = [a.get("data", "").rstrip(".") for a in resp.json().get("Answer", []) or []
                    if a.get("type") == 2]
        for ns in ns_names:
            try:
                return await asyncio.to_thread(socket.gethostbyname, ns)
            except OSError:
                continue
    except Exception:  # noqa: BLE001
        pass
    return None


async def _listed_ips_auth(ns_ip: str, domain: str, zone: str) -> list[str] | None:
    ips = await asyncio.to_thread(_udp_query_a, f"{domain}.{zone}", ns_ip)
    if ips is None:
        return None
    return [ip for ip in ips if ip and ip not in _SENTINEL_IPS]


# Control domains each list PUBLISHES as permanently listed. If a zone's lookup
# path can't see its own control, that path is broken/blocked — report the zone
# as INCONCLUSIVE instead of silently calling every domain clean.
_CONTROLS = {
    "dbl.spamhaus.org": "dbltest.com",
    "multi.surbl.org": "test.surbl.org",
    "multi.uribl.com": "test.uribl.com",
}

# A zone's DoH path must pass EVERY control probe to be trusted for the run;
# anything less means intermittent blocking and we use the authoritative NS.
_CONTROL_PROBES: int = 3


async def _pick_strategies(client: httpx.AsyncClient) -> dict[str, dict]:
    """Per zone: {'mode': 'doh'|'auth', 'ns_ip': str|None} for the first lookup
    path whose control domain fires; mode 'broken' if neither works."""
    out: dict[str, dict] = {}
    for zone in BLACKLIST_ZONES:
        control = _CONTROLS.get(zone)
        if control:
            # DoH answers for these zones are INTERMITTENT (measured 2026-07-10:
            # dbltest.com resolved on 4 of 6 consecutive attempts, NXDOMAIN on
            # the rest). One failed probe must not silently disable a zone, and
            # one lucky probe must not certify a flaky path — retry before
            # falling back, and prefer the stable authoritative path when DoH
            # proves unreliable.
            doh_hits = 0
            for _ in range(_CONTROL_PROBES):
                if await _listed_ips(client, control, zone):
                    doh_hits += 1
            if doh_hits == _CONTROL_PROBES:
                out[zone] = {"mode": "doh", "ns_ip": None}
                continue
            if doh_hits:
                print(f"  [Blacklist] {zone}: DoH flaky ({doh_hits}/{_CONTROL_PROBES} "
                      "control probes) - preferring authoritative NS")
            ns_ip = await _zone_ns_ip(client, zone)
            if ns_ip and await _listed_ips_auth(ns_ip, control, zone):
                out[zone] = {"mode": "auth", "ns_ip": ns_ip}
                print(f"  [Blacklist] {zone}: public resolver blocked -> using "
                      f"authoritative NS {ns_ip} directly")
                continue
            out[zone] = {"mode": "broken", "ns_ip": None}
            print(f"  [Blacklist] ⚠ {zone}: control domain '{control}' not detected "
                  "on ANY lookup path — zone results are INCONCLUSIVE this run")
        else:
            out[zone] = {"mode": "doh", "ns_ip": None}
    return out


async def _check_domain(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                        domain: str, strategies: dict[str, dict]) -> dict:
    async with sem:
        hits: list[str] = []
        errors: list[str] = []
        inconclusive: list[str] = []
        for zone, label in BLACKLIST_ZONES.items():
            strat = strategies.get(zone, {"mode": "doh", "ns_ip": None})
            if strat["mode"] == "broken":
                inconclusive.append(label)
                continue
            if strat["mode"] == "auth":
                ips = await _listed_ips_auth(strat["ns_ip"], domain, zone)
            else:
                ips = await _listed_ips(client, domain, zone)
            if ips:
                codes = ",".join(_decode(zone, ip) for ip in ips)
                hits.append(f"{label} ({codes})")
            elif ips is None:
                errors.append(label)
        return {"domain": domain, "listed_on": hits, "lookup_errors": errors,
                "inconclusive_zones": inconclusive}


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
        strategies = await _pick_strategies(client)
        results = await asyncio.gather(
            *(_check_domain(client, sem, d, strategies) for d in sorted(domain_clients)))

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
