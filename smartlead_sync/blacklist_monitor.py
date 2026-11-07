#!/usr/bin/env python3
"""Blocklist monitor (plan §5.6). Weekly cron, Mondays 09:00 IST; read-only (no Smartlead writes).

Checks every sending domain of every client against the domain blocklists that
matter (Spamhaus DBL, SURBL, URIBL: BLACKLIST_ZONES). Pay-to-delist lists
(UCEProtect etc.) are deliberately not checked. Lookups go straight to each
list's own name server, which must show the list's published test domain as
listed, every run. When that server can't be used, Google DNS-over-HTTPS is
asked instead, but it misses some listings, so there only a listing counts: a
domain it shows as not listed is UNKNOWN on that list, never clean.

Every weekly run posts ONE summary to the infra channel (BLACKLIST_CHANNEL
overrides it), clean or not (audit 7 Nov 2026, BLOCKLIST-1/3/4/8/9/10/12):

    Blocklists 9 Nov: checked 40 of 40 domains · Spamhaus DBL 2 (1 new) · URIBL 0 · SURBL new 1 · unknown 0

The thread names each listed domain's client (Melior vs Precise Leads from the
inbox's client_id, not the Smartlead account), its inboxes, whether it sends in
live campaigns, when it was first seen listed, and new vs ongoing, live senders
first. SURBL shows only when new since the last check (SURBL listings tend to
be long-running, and repeating them every week would be noise). A list that could not be read, or answered
with an error code, is UNKNOWN, never clean. A Smartlead account that cannot be
read still has its domains checked, from its last saved list, and is named as
skipped. The run exits 1 when coverage is partial (an account skipped, a domain
unknown) or the save or post failed, so the cron log shows a failure.

Mongo blacklist_checks keeps one row per domain per day: {domain, date,
listed_on, lookup_errors, inconclusive_zones, clients (Smartlead account names,
as before), client_names, inboxes, unknown, run, source, read_from (the accounts
whose Smartlead inbox list showed the domain that day)}.

Usage:
    python3 blacklist_monitor.py                    # weekly run: save + post
    python3 blacklist_monitor.py --dry-run          # weekly checks, print only
    python3 blacklist_monitor.py a.com b.com        # by hand: print only
    python3 blacklist_monitor.py a.com --save --post
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
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
from smartlead.notify import day, infra_channel, redact
from smartlead.processing import get_domain_from_email
from smartlead.report_sheet import client_label
from smartlead.v2_results import is_test_campaign

try:
    from pymongo import MongoClient, UpdateOne
except ImportError:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

_DOH_URL = "https://dns.google/resolve"
_CONCURRENCY = 10
_RETRY_PAUSE_S = 1.0        # one retry per failed lookup, after this pause

# DNSBL answers that mean "query refused / invalid", NEVER a listing: Spamhaus
# 127.255.255.252 (typo in the list name), .254 (query via a public resolver),
# .255 (too many queries) and 127.0.1.255 (IP query to DBL); URIBL and SURBL
# answer 127.0.0.1 to a refused resolver. Any of these makes the lookup an
# ERROR, so the domain is UNKNOWN on that list, not clean (BLOCKLIST-12).
_SENTINEL_IPS = {"127.0.0.1", "127.0.0.255", "127.0.1.255",
                 "127.255.255.252", "127.255.255.254", "127.255.255.255"}


def _is_error_answer(ip: str) -> bool:
    return ip in _SENTINEL_IPS or ip.startswith("127.255.255.")

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
    """Listing answer IPs, [] = clean, None = lookup error (an error-code answer included)."""
    try:
        resp = await client.get(_DOH_URL, params={"name": f"{domain}.{zone}", "type": "A"})
        resp.raise_for_status()
        data = resp.json()
        status = data.get("Status")
        if status == 3:  # NXDOMAIN = not listed
            return []
        if status != 0:  # SERVFAIL, REFUSED, ...: no answer from the list, so no verdict
            print(f"  [Blacklist] {domain} @ {zone}: DNS status {status} - unknown", file=sys.stderr)
            return None
        ips = [a.get("data", "") for a in data.get("Answer", []) or [] if a.get("type") == 1]
        ips = [ip for ip in ips if ip]
        if any(_is_error_answer(ip) for ip in ips):
            print(f"  [Blacklist] {domain} @ {zone}: refused ({', '.join(ips)}) - unknown", file=sys.stderr)
            return None
        return ips
    except Exception as exc:  # noqa: BLE001
        print(f"  [Blacklist] lookup error {domain} @ {zone}: {exc}", file=sys.stderr)
        return None


# ── direct authoritative-NS queries (fallback when public resolvers are blocked) ──
# Spamhaus DBL returns NXDOMAIN and URIBL returns its 127.0.0.1 "refused" code
# to public resolvers (Google DoH included) — found 2026-07-10 via control
# domains: our DBL/URIBL coverage had silently been zero. Querying each zone's
# own authoritative nameserver directly bypasses the block. Raw UDP (no deps).

def _udp_query_a(name: str, server_ip: str, timeout: float = 5.0) -> list[str] | None:
    """Minimal DNS A query over UDP. Returns answer IPs, [] for NXDOMAIN or no A
    record, None on a network error, an error rcode (SERVFAIL, REFUSED, ...) or a
    truncated reply: a failed answer is never clean."""
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
    rcode = data[3] & 0x0F
    if rcode == 3:  # NXDOMAIN
        return []
    if rcode != 0 or data[2] & 0x02:  # error rcode, or truncated (TC bit): no verdict
        return None
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
    if ips is None or any(_is_error_answer(ip) for ip in ips):
        return None
    return [ip for ip in ips if ip]


# Control domains each list PUBLISHES as permanently listed. If a zone's lookup
# path can't see its own control, that path is broken/blocked — report the zone
# as INCONCLUSIVE instead of silently calling every domain clean.
_CONTROLS = {
    "dbl.spamhaus.org": "dbltest.com",
    "multi.surbl.org": "test.surbl.org",
    "multi.uribl.com": "test.uribl.com",
}

# Google DoH answers these lists only some of the time (7 Nov 2026: Spamhaus's
# own test listing on 6 of 8 tries, the rest NXDOMAIN, and its cache repeats a
# miss), so for a list with a control domain a DoH "not listed" is never trusted:
# mode 'hits_only' counts what DoH shows as listed and makes every other domain
# UNKNOWN on that list. Up to this many probes decide whether DoH sees it at all.
_CONTROL_PROBES: int = 3


async def _pick_strategies(client: httpx.AsyncClient) -> dict[str, dict]:
    """Per zone: {'mode', 'ns_ip'}. 'auth': the list's own name server (its control
    fired there). 'hits_only': Google DoH, whose listings count but whose
    not-listed answers are unknown. 'broken': the control was seen on no path.
    'doh': a list with no control domain (none today)."""
    out: dict[str, dict] = {}
    for zone in BLACKLIST_ZONES:
        control = _CONTROLS.get(zone)
        if control:
            # The list's own name server FIRST. Public resolvers answer these
            # lists intermittently (2026-07-10: dbltest.com on 4 of 6 DoH tries;
            # 2026-11-07: 6 of 8, the rest NXDOMAIN), so three lucky probes
            # used to certify DoH and a quarter of real listings then read as
            # clean (a by-hand check of dbltest.com came back clean that day).
            # The authoritative answer was the same on every try.
            ns_ip = await _zone_ns_ip(client, zone)
            if ns_ip and await _listed_ips_auth(ns_ip, control, zone):
                out[zone] = {"mode": "auth", "ns_ip": ns_ip}
                print(f"  [Blacklist] {zone}: using its authoritative NS {ns_ip}")
                continue
            seen = False
            for _ in range(_CONTROL_PROBES):
                if await _listed_ips(client, control, zone):
                    seen = True
                    break
            if seen:
                out[zone] = {"mode": "hits_only", "ns_ip": None}
                print(f"  [Blacklist] ⚠ {zone}: authoritative NS not usable -> Google DoH, listings only "
                      "(a not-listed answer there is unknown, not clean)")
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
            ips = None
            for attempt in range(2):                 # one retry: a timeout must not make a domain unknown
                if strat["mode"] == "auth":
                    ips = await _listed_ips_auth(strat["ns_ip"], domain, zone)
                else:
                    ips = await _listed_ips(client, domain, zone)
                if ips is not None:
                    break
                if attempt == 0:
                    await asyncio.sleep(_RETRY_PAUSE_S)
            if ips:
                codes = ",".join(_decode(zone, ip) for ip in ips)
                hits.append(f"{label} ({codes})")
            elif strat["mode"] == "hits_only":
                inconclusive.append(label)          # DoH misses some listings: not seen is not clean
            elif ips is None:
                errors.append(label)
        return {"domain": domain, "listed_on": hits, "lookup_errors": errors,
                "inconclusive_zones": inconclusive}


# ── which domains to check ────────────────────────────────────────────────────

DBL, SURBL, URIBL = "Spamhaus DBL", "SURBL", "URIBL"       # labels in BLACKLIST_ZONES / listed_on
SERIOUS = (DBL, URIBL)      # judge the sending domain; SURBL judges links in the body (chronic, info only)


def _entry() -> dict:
    """One domain in the run: Smartlead accounts, client names, {inbox: account}; stale = from a saved list."""
    return {"accounts": set(), "clients": set(), "emails": {}, "stale": False}


async def _list_inboxes(acc) -> list[dict]:
    async with SmartleadClient(acc.api_key, acc.name) as c:
        return await c.list_email_accounts()


async def _collect_domains(accounts: list) -> tuple[dict[str, dict], dict[str, int], list[dict]]:
    """Every sending domain on the accounts that could be read (excluded-client inboxes dropped).

    Returns (inventory, per_account, failed): inventory {domain: _entry()}, per_account
    {account: inboxes Smartlead returned} for each account read, failed [{account, error}]
    for each that was not.
    The client is client_label on the inbox's client_id, so a Melior inbox in the Precise
    Leads account reads Melior (BLOCKLIST-8)."""
    inventory: dict[str, dict] = {}
    per_account: dict[str, int] = {}
    failed: list[dict] = []
    for acc in accounts:
        try:
            inboxes = await _list_inboxes(acc)
        except Exception as exc:  # noqa: BLE001 - one account must not hide the others
            msg = redact(str(exc))[:150] or type(exc).__name__
            print(f"  [Blacklist] ⚠ {acc.name}: could not read its inboxes ({msg})")
            failed.append({"account": acc.name, "error": msg})
            continue
        for a in inboxes or []:
            email = str(a.get("from_email", "")).strip().lower()
            if not email or is_excluded_inbox(a):
                continue
            dom = get_domain_from_email(email)
            if not dom:
                continue
            e = inventory.setdefault(dom, _entry())
            e["accounts"].add(acc.name)
            e["clients"].add(client_label(acc.name, a.get("client_id")))
            e["emails"][email] = acc.name
        per_account[acc.name] = len(inboxes or [])
    return inventory, per_account, failed


def _fill_from_last_lists(inventory: dict, per_account: dict, failed: list[dict], store) -> list[dict]:
    """Add each unreadable account's domains from its last saved check, so they are still
    checked (BLOCKLIST-3: an unreadable account used to be skipped silently). An account
    that answered with no inboxes, when its last check had some, counts as unreadable too.
    Returns the skipped accounts: [{account, error, list_date, domains, mongo_ok}];
    mongo_ok False = Mongo could not be read, so there was no list to fall back on."""
    todo = list(failed) + [{"account": n, "error": "Smartlead returned no inboxes", "empty": True}
                           for n, count in per_account.items() if not count]
    skipped = []
    for f in todo:
        when, rows, mongo_ok = None, [], store is not None
        if mongo_ok:
            try:
                when, rows = store.last_list(f["account"])
            except Exception as exc:  # noqa: BLE001 - say so, keep checking the rest
                print(f"  [Blacklist] ⚠ Mongo read failed for {f['account']}'s last list ({redact(str(exc))[:150]})")
                mongo_ok = False
        if f.get("empty") and mongo_ok and not rows:
            continue                       # no inboxes then, none now: nothing was missed
        for r in rows:
            e = inventory.get(r["domain"])
            if e is None:
                e = inventory[r["domain"]] = _entry()
                e["stale"] = True
            e["accounts"].add(f["account"])
            e["clients"].update(r.get("client_names") or [])
        skipped.append({"account": f["account"], "error": f["error"], "list_date": when, "domains": len(rows),
                        "mongo_ok": mongo_ok})
    return skipped


def _adhoc_domains(raw: list[str]) -> list[str]:
    out: list[str] = []
    for x in raw:
        d = str(x).strip().lower().split("//")[-1].split("/")[0].split("@")[-1].strip(".")
        if "." in d and d not in out:
            out.append(d)
    return out


# ── Mongo history (blacklist_checks) ─────────────────────────────────────────

def _is_adhoc(row: dict) -> bool:
    return row.get("run") == "adhoc" or list(row.get("clients") or []) == ["adhoc"]


def _read_live(row: dict, account: str) -> bool:
    """Did this account's own Smartlead inbox list show the row's domain that day? A row
    checked only from an earlier list (source 'last list') did not, so it never becomes
    the next 'last list'. Rows saved before read_from existed count by their clients."""
    if "read_from" in row:
        return account in (row.get("read_from") or [])
    return row.get("source") != "last list"


_RESULT_FIELDS = ("domain", "date", "listed_on", "lookup_errors", "inconclusive_zones", "unknown")


class Store:
    """blacklist_checks: one row per domain per day (unique on domain + date)."""

    def __init__(self, col) -> None:
        self.col = col

    def history(self, domains) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for r in self.col.find({"domain": {"$in": sorted(domains)}}, {"_id": 0}):
            out.setdefault(r["domain"], []).append(r)
        return out

    def last_list(self, account: str) -> tuple[str | None, list[dict]]:
        """(date, rows) of the newest weekly check that read this Smartlead account's own
        inbox list. Weeks it was skipped don't count, so the date stays that of the last
        real read and a second skipped week doesn't hide domains added in between."""
        rows = [r for r in self.col.find({"clients": account},
                                         {"_id": 0, "domain": 1, "date": 1, "clients": 1, "client_names": 1,
                                          "run": 1, "source": 1, "read_from": 1})
                if not _is_adhoc(r) and r.get("date") and _read_live(r, account)]
        if not rows:
            return None, []
        when = max(r["date"] for r in rows)
        return when, sorted((r for r in rows if r["date"] == when), key=lambda r: r["domain"])

    def save(self, records: list[dict], adhoc: bool = False) -> bool:
        """Upsert today's rows. A check by hand never overwrites a weekly row's clients."""
        try:
            self.col.create_index([("domain", 1), ("date", 1)], unique=True)
            ops = []
            for r in records:
                key = {"domain": r["domain"], "date": r["date"]}
                if adhoc:
                    fields = {k: r[k] for k in _RESULT_FIELDS if k in r}
                    ops.append(UpdateOne(key, {"$set": fields, "$setOnInsert": {
                        "clients": ["adhoc"], "client_names": [], "inboxes": None, "run": "adhoc",
                        "source": "by hand"}}, upsert=True))
                else:
                    ops.append(UpdateOne(key, {"$set": r}, upsert=True))
            if ops:
                self.col.bulk_write(ops, ordered=False)
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"  [Blacklist] ⚠ Mongo write failed: {redact(str(exc))[:200]}")
            return False


def _open_store() -> Store | None:
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [Blacklist] ⚠ Mongo unavailable: no history, nothing saved.")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return Store(client[HEALTH_HISTORY_DB][BLACKLIST_COLLECTION])
    except Exception as exc:  # noqa: BLE001
        print(f"  [Blacklist] ⚠ Mongo unavailable ({redact(str(exc))[:150]}): no history, nothing saved.")
        return None


def _on(row: dict, label: str) -> bool:
    return any(str(s).startswith(label) for s in row.get("listed_on") or [])


def _unknown_on(row: dict, label: str) -> bool:
    return label in (row.get("lookup_errors") or []) or label in (row.get("inconclusive_zones") or [])


def listing_age(rows: list[dict], label: str, today: str) -> dict:
    """For a domain on `label` today: {new, since, clean_on}. Pure.

    new: the last weekly check before today that could read `label` did not show it
    (or there is none). clean_on: that check's date. since: the first day of the unbroken
    run of checks (weekly or by hand) ending today that showed it, i.e. first seen."""
    earlier = sorted((r for r in rows if str(r.get("date", "")) < today), key=lambda r: r["date"], reverse=True)
    new, clean_on = True, None
    for r in earlier:
        if _is_adhoc(r) or _unknown_on(r, label):
            continue
        new = not _on(r, label)
        clean_on = r["date"] if new else None
        break
    since = today
    for r in earlier:
        if _on(r, label):
            since = r["date"]
        elif not _unknown_on(r, label):
            break
    return {"new": new, "since": since, "clean_on": clean_on}


# ── what the run found ───────────────────────────────────────────────────────

_RANK = {"sending": 0, "idle": 1, "unknown": 2, "none": 3, None: 4}


def analyse(results: list[dict], inventory: dict, history: dict | None, today: str, *,
            surbl_all: bool = False, modes: dict[str, str] | None = None) -> dict:
    """Pure. Counts for the headline and one hit per listed domain worth showing:
    every DBL/URIBL listing, and SURBL only when new since the last check (all SURBL
    when surbl_all, for a check by hand). history None = Mongo unreadable.
    modes = {list label: lookup mode} from _check_all, to say why a list is unknown."""
    hits, surbl, unknown = [], [], []
    counts = {"total": len(results), "dbl": 0, "dbl_new": 0, "uribl": 0, "surbl": 0, "surbl_new": 0}
    for r in results:
        e = inventory.get(r["domain"]) or _entry()
        if r.get("lookup_errors") or r.get("inconclusive_zones"):
            unknown.append(r)
        ages = {}
        for label in (DBL, URIBL, SURBL):
            if _on(r, label):
                ages[label] = listing_age(history.get(r["domain"], []), label, today) if history is not None else None
        base = {"domain": r["domain"], "clients": sorted(e["clients"]), "accounts": sorted(e["accounts"]),
                "inboxes": None if e["stale"] or not e["emails"] else len(e["emails"]),
                "status": "unknown" if e["stale"] else None, "campaigns": 0, "sending": 0}
        if DBL in ages:
            counts["dbl"] += 1
            counts["dbl_new"] += bool(ages[DBL] and ages[DBL]["new"])
        if URIBL in ages:
            counts["uribl"] += 1
        if SURBL in ages:
            counts["surbl"] += 1
            counts["surbl_new"] += bool(ages[SURBL] and ages[SURBL]["new"])
        serious = [l for l in SERIOUS if l in ages]
        if serious:
            hits.append({**base, "lists": [s for s in r["listed_on"] if any(s.startswith(l) for l in serious)],
                         "age": _combine([ages[l] for l in serious])})
        if SURBL in ages and (surbl_all or (ages[SURBL] and ages[SURBL]["new"])):
            surbl.append({**base, "lists": [s for s in r["listed_on"] if s.startswith(SURBL)], "age": ages[SURBL]})
    counts["unknown"] = len(unknown)
    counts["checked"] = counts["total"] - counts["unknown"]
    return {"counts": counts, "hits": hits, "surbl": surbl, "unknown": unknown, "history_ok": history is not None,
            "modes": dict(modes or {})}


def _combine(ages: list[dict | None]) -> dict | None:
    """One age for a domain on DBL and URIBL: new if either is new."""
    if not ages or any(a is None for a in ages):
        return None
    new = [a for a in ages if a["new"]]
    if new:
        return {"new": True, "since": min(a["since"] for a in new), "clean_on": new[0]["clean_on"]}
    return {"new": False, "since": min(a["since"] for a in ages), "clean_on": None}


def _status(entry: dict | None, att: dict[str, dict | None]) -> dict:
    """Does the domain send? att = {account: attachments() result, or None when unreadable}."""
    if entry is None or not entry["emails"]:
        return {"status": "unknown" if entry and entry["stale"] else None, "campaigns": 0, "sending": 0}
    camps: set[str] = set()
    sending: set[str] = set()
    unread = False
    for email, account in entry["emails"].items():
        a = att.get(account)
        if a is None:
            unread = True
            continue
        at = a.get(email) or {}
        camps |= {c for c in at.get("campaigns") or [] if not is_test_campaign(c)}
        sending |= {c for c in at.get("sending") or [] if not is_test_campaign(c)}
    status = "sending" if sending else "idle" if camps else "unknown" if unread else "none"
    return {"status": status, "campaigns": len(camps), "sending": len(sending)}


def _attachments(api_key: str) -> dict:
    """{inbox: {campaigns, sending}}: live campaigns (ACTIVE, leads left) and those that sent in 7 days."""
    from placement_report import attachments
    return attachments(api_key, sending_days=7)


async def _fill_status(found: dict, inventory: dict, accounts: list) -> None:
    """Sending status for each shown domain; reads only the accounts those domains are on."""
    shown = found["hits"] + found["surbl"]
    need = {acct for h in shown for acct in (inventory.get(h["domain"]) or _entry())["emails"].values()}
    keys = {a.name: a.api_key for a in accounts}
    att: dict[str, dict | None] = {}
    for name in sorted(need):
        try:
            att[name] = await asyncio.to_thread(_attachments, keys[name])
        except Exception as exc:  # noqa: BLE001 - say "sending unknown", keep the rest
            print(f"  [Blacklist] ⚠ {name}: could not read campaigns ({redact(str(exc))[:150]})")
            att[name] = None
    for h in shown:
        h.update(_status(inventory.get(h["domain"]), att))


# ── the Slack summary ────────────────────────────────────────────────────────

_IP_CODES = re.compile(r"\s*\((?:\d{1,3}\.){3}\d{1,3}(?:,(?:\d{1,3}\.){3}\d{1,3})*\)")


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 'es' if word.endswith('x') else 's'}"


def _who(h: dict) -> str:
    return ", ".join(h["clients"]) or ", ".join(f"{a} account" for a in h["accounts"])


def hit_line(h: dict, today: str = "") -> str:
    parts = [f"• *{h['domain']}*"]
    if _who(h):
        parts.append(_who(h))
    parts.append(", ".join(_IP_CODES.sub("", s) for s in h["lists"]))
    if h.get("inboxes"):
        parts.append(_plural(h["inboxes"], "inbox"))
    st = h.get("status")
    if st == "sending":
        parts.append(f"sending in {_plural(h['sending'], 'live campaign')}")
    elif st == "idle":
        parts.append(f"in {_plural(h['campaigns'], 'live campaign')}, nothing sent in 7 days")
    elif st == "none":
        parts.append("in no live campaign")
    elif st == "unknown":
        parts.append("sending unknown")
    age = h.get("age")
    if age and age["new"]:
        txt = f"new (clean on {day(age['clean_on'])})" if age["clean_on"] else "new (first check)"
        if age["clean_on"] and age["since"] != today:
            txt += f", first seen {day(age['since'])}"
        parts.append(txt)
    elif age:
        parts.append(f"ongoing, first seen {day(age['since'])}")
    return " · ".join(parts)


def _order(hs: list[dict]) -> list[dict]:
    return sorted(hs, key=lambda h: (_RANK.get(h.get("status"), 4), not (h.get("age") or {}).get("new"), h["domain"]))


ADVICE = ("What to do: decide with the client lead whether each listed domain keeps sending. Listings expire "
          "once the sending that caused them stops, and Spamhaus removal asks for confirmed opt-in, so don't "
          "file a removal request without a change. Check the client's other domains and the links in the copy. "
          "Domains under 30 days old: replace instead.")


def summary(found: dict, today: str, *, skipped: list[dict] = (), accounts: int = 0,
            adhoc: bool = False) -> tuple[list[str], list[str]]:
    """(headline_lines, thread_parts). The headline is one count line, plus who was skipped and
    which listed domains still send; the domains, unknowns and advice go in the thread. Pure."""
    c = found["counts"]
    hits, surbl = _order(found["hits"]), _order(found["surbl"])
    hist = found["history_ok"]
    dbl_new = f"{c['dbl_new']} new" if hist else "new unknown"
    surbl_part = (f"SURBL {c['surbl']}" + (f" ({c['surbl_new']} new)" if hist else "")) if adhoc else \
        (f"SURBL new {c['surbl_new']}" if hist else "SURBL new unknown")
    live = [h for h in hits if h.get("status") == "sending"]
    alarm = any(h.get("status") == "sending" or (h.get("age") or {}).get("new") for h in hits)
    worry = hits or surbl or c["unknown"] or skipped or not hist or not c["total"]
    emoji = "🚨" if alarm else "⚠️" if worry else "✅"
    when = day(today) + (" (by hand)" if adhoc else "")
    if c["total"]:
        head = [f"{emoji} Blocklists {when}: checked {c['checked']} of {_plural(c['total'], 'domain')} · "
                f"Spamhaus DBL {c['dbl']} ({dbl_new}) · URIBL {c['uribl']} · {surbl_part} · unknown {c['unknown']}"]
    else:
        head = [f"{emoji} Blocklists {when}: no domains to check"]
    if skipped:
        bits = [f"skipped {s['account']}: " + (f"its {_plural(s['domains'], 'domain')} came from the "
                                               f"{day(s['list_date'])} check" if s["domains"]
                                               else _not_checked(s))
                for s in skipped]
        head.append(f"Read {accounts - len(skipped)} of {_plural(accounts, 'Smartlead account')} · " + "; ".join(bits))
    if live:
        names = [f"{h['domain']} ({_who(h)})" if _who(h) else h["domain"] for h in live[:3]]
        more = f" and {len(live) - 3} more" if len(live) > 3 else ""
        head.append(f"Listed and still sending: {', '.join(names)}{more}")

    thread: list[str] = []
    if hits:
        thread.append(f"On Spamhaus DBL or URIBL ({len(hits)})" + (":\n" if adhoc else ", live senders first:\n")
                      + "\n".join(hit_line(h, today) for h in hits))
        thread.append(ADVICE)
    if surbl:
        title = (f"On SURBL ({len(surbl)})" if adhoc else f"New on SURBL since the last check ({len(surbl)})")
        thread.append(f"{title}. SURBL judges the links in an email, so check the links in the copy:\n"
                      + "\n".join(hit_line(h, today) for h in surbl))
    if found["unknown"]:
        zones = sorted({z for r in found["unknown"] for z in r.get("inconclusive_zones") or []})
        lines = [f"• {z}: " + _ZONE_WHY.get(found.get("modes", {}).get(z), _ZONE_WHY["broken"]) for z in zones]
        errs = [r for r in found["unknown"] if r.get("lookup_errors")]
        lines += [f"• {r['domain']}: {', '.join(r['lookup_errors'])} lookup failed" for r in errs[:30]]
        if len(errs) > 30:
            lines.append(f"… and {len(errs) - 30} more")
        thread.append(f"Unknown ({c['unknown']}): a list could not be read fully for these, so they don't count as clean:\n"
                      + "\n".join(lines))
    if skipped:
        thread.append("Could not read from Smartlead:\n" + "\n".join(_skip_line(s) for s in skipped)
                      + "\n\nRe-run once the account reads again.")
    if not hist:
        thread.append("Mongo history could not be read, so new vs ongoing is unknown"
                      + ("." if adhoc else " and new SURBL listings can't be picked out."))
    return head, thread


_ZONE_WHY = {
    "broken": "its test domain was not seen on any lookup path, so every domain is unknown on it",
    "hits_only": ("its own name server could not be used, and Google DNS misses some of its listings, "
                  "so a domain not seen listed there is unknown on it, not clean"),
}


def _not_checked(s: dict) -> str:
    """Why a skipped account's domains were not checked at all."""
    why = "no saved list" if s.get("mongo_ok", True) else "Mongo unreadable"
    return f"{why}, so its domains were not checked"


def _skip_line(s: dict) -> str:
    n = s["domains"]
    if not n:
        why = _not_checked(s)
        return f"• {s['account']}: {s['error']}. {why[0].upper()}{why[1:]}."
    return (f"• {s['account']}: {s['error']}. Its {_plural(n, 'domain')} from the {day(s['list_date'])} check "
            f"{'was' if n == 1 else 'were'} checked; inbox counts, sending and any domain added since are missing.")


def _post(head: list[str], thread: list[str]) -> bool:
    from smartlead.notify import post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    if not token:
        print("[Blacklist] ⚠ SLACK_BOT_TOKEN missing: the summary was NOT posted.")
        return False
    if not post_with_thread(token, infra_channel("BLACKLIST_CHANNEL"), head, thread):
        print("[Blacklist] ⚠ Slack post failed: the summary was NOT posted.")
        return False
    return True


# ── run ──────────────────────────────────────────────────────────────────────

async def _check_all(domains: list[str]) -> tuple[list[dict], dict[str, str]]:
    """(one result per domain, {list label: lookup mode})."""
    sem = asyncio.Semaphore(_CONCURRENCY)
    async with httpx.AsyncClient(timeout=10.0) as client:
        strategies = await _pick_strategies(client)
        results = list(await asyncio.gather(*(_check_domain(client, sem, d, strategies) for d in domains)))
    return results, {BLACKLIST_ZONES[z]: st["mode"] for z, st in strategies.items() if z in BLACKLIST_ZONES}


def _args(argv: list[str] | None):
    ap = argparse.ArgumentParser(description="Blocklist check (read-only for Smartlead)")
    ap.add_argument("domains", nargs="*", help="check these domains by hand (prints only unless --save / --post)")
    ap.add_argument("--save", action="store_true", help="by hand: save the results to Mongo")
    ap.add_argument("--post", action="store_true", help="by hand: post the summary to the infra channel")
    ap.add_argument("--dry-run", action="store_true", help="check and print only: save nothing, post nothing")
    return ap.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _args(argv)
    today = date.today().isoformat()
    adhoc = bool(args.domains)
    save = not args.dry_run and (args.save if adhoc else True)
    post = not args.dry_run and (args.post if adhoc else True)
    store = _open_store()

    accounts: list = []
    skipped: list[dict] = []
    if adhoc:
        inventory = {d: _entry() for d in _adhoc_domains(args.domains)}
    else:
        accounts = discover_accounts()
        inventory, per_account, failed = await _collect_domains(accounts)
        skipped = _fill_from_last_lists(inventory, per_account, failed, store)
    print(f"[Blacklist] checking {len(inventory)} domain(s) against {', '.join(BLACKLIST_ZONES.values())}"
          + (f" ({len(accounts) - len(skipped)} of {len(accounts)} Smartlead accounts read)" if not adhoc else ""))

    results, modes = await _check_all(sorted(inventory)) if inventory else ([], {})
    history = None
    if store is not None:
        try:
            history = store.history(inventory) if inventory else {}
        except Exception as exc:  # noqa: BLE001
            print(f"  [Blacklist] ⚠ Mongo history read failed ({redact(str(exc))[:150]})")
    found = analyse(results, inventory, history, today, surbl_all=adhoc, modes=modes)
    if not adhoc:
        await _fill_status(found, inventory, accounts)

    head, thread = summary(found, today, skipped=skipped, accounts=len(accounts), adhoc=adhoc)
    print("\n".join(head))
    for part in thread:
        print("\n" + part)

    ok = True
    if save:
        if store is None:
            ok = False
        else:
            records = []
            for r in results:
                e = inventory.get(r["domain"]) or _entry()
                records.append({**r, "date": today, "clients": sorted(e["accounts"]),
                                "client_names": sorted(e["clients"]),
                                "inboxes": None if e["stale"] or not e["emails"] else len(e["emails"]),
                                "unknown": bool(r.get("lookup_errors") or r.get("inconclusive_zones")),
                                "read_from": sorted(set(e["emails"].values())),
                                "run": "adhoc" if adhoc else "weekly",
                                "source": "last list" if e["stale"] else ("by hand" if adhoc else "smartlead")})
            ok = store.save(records, adhoc=adhoc) and ok
    if post:
        ok = _post(head, thread) and ok
    partial = bool(skipped) or found["counts"]["unknown"] > 0 or not results
    if partial:
        print("[Blacklist] ⚠ partial coverage: see the summary above. Exit 1.")
    return 1 if (partial or not ok) else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
