#!/usr/bin/env python3
"""Daily infrastructure audit across the current clients' Smartlead inboxes.

READ-ONLY: reads Smartlead, DNS (DoH) and each domain's website; changes
nothing. Checks (rules in smartlead/infra_audit.py):
  MX present · DMARC beyond p=none · name-server footprint · website/redirect
  to the client's real site · never sending from the main domain · mailboxes
  and sends per domain · per-mailbox caps · inboxes under 21 days in live
  campaigns · warmup on and close to cold volume · signature + real sender
  name · provider mix · ESP matching · SMTP sending IPs on blacklists.

    python infra_audit.py                 # print
    python infra_audit.py --json          # one JSON line (Slack)
    python infra_audit.py --post          # post to INFRA_AUDIT_CHANNEL (else HEALTH_NOTIFY_CHANNEL)
    python infra_audit.py --client Bettrdata
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

import httpx

from smartlead import infra_audit as ia
from smartlead.accounts import discover_accounts
from smartlead.report_sheet import client_label
from smartlead.zapmail_clients import CURRENT_CLIENTS

SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}
DOH = "https://dns.google/resolve"
IP_ZONES_DOH = ("b.barracudacentral.org", "bl.spamcop.net")


def _get(path, key, **params):
    r = httpx.get(SL + path, params={"api_key": key, **params}, headers=UA, timeout=120)
    r.raise_for_status()
    return r.json()


def collect_smartlead(only: str | None):
    """Accounts (connected, current clients), active attachments, active campaigns."""
    from placement_report import active_attachments
    accounts, client_of_email, active, campaigns, client_of_campaign = [], {}, {}, [], {}
    for acc in discover_accounts():
        if acc.name.upper() == "DARLEAN":          # past client
            continue
        off = 0
        rows = []
        while True:
            page = _get("/email-accounts/", acc.api_key, offset=off, limit=100)
            rows += page
            if len(page) < 100:
                break
            off += 100
        for a in rows:
            client = client_label(acc.name, a.get("client_id"))
            if client not in CURRENT_CLIENTS or (only and client != only):
                continue
            if not (a.get("is_smtp_success") and a.get("is_imap_success")):
                continue
            accounts.append(a)
            client_of_email[a["from_email"].lower()] = client
        active.update(active_attachments(acc.api_key))
        for c in _get("/campaigns", acc.api_key):
            if str(c.get("status", "")).upper() == "ACTIVE":
                client = client_label(acc.name, c.get("client_id"))
                if client in CURRENT_CLIENTS and (not only or client == only):
                    campaigns.append(c)
                    client_of_campaign[c["id"]] = client
    active = {e: v for e, v in active.items() if e in client_of_email}
    return accounts, client_of_email, active, campaigns, client_of_campaign


async def _doh(client, sem, name, rtype):
    async with sem:
        try:
            r = await client.get(DOH, params={"name": name, "type": rtype})
            data = r.json()
        except Exception:  # noqa: BLE001
            return None
    if data.get("Status") not in (0, 3):      # 3 = NXDOMAIN (answered: nothing there)
        return None
    want = {"MX": 15, "TXT": 16, "NS": 2, "A": 1}[rtype]
    return [a.get("data", "").strip('"') for a in data.get("Answer", []) or [] if a.get("type") == want]


async def _site(client, sem, domain):
    async with sem:
        try:
            r = await client.get(f"http://{domain}", follow_redirects=True, timeout=12)
            return {"final_url": str(r.url), "status": r.status_code, "page": r.text[:30000]}
        except Exception as exc:  # noqa: BLE001
            return {"error": type(exc).__name__}


async def collect_dns_and_sites(domains: list[str]):
    sem = asyncio.Semaphore(20)
    async with httpx.AsyncClient(timeout=10, headers=UA) as c:
        mx, dmarc, ns, sites = await asyncio.gather(
            asyncio.gather(*(_doh(c, sem, d, "MX") for d in domains)),
            asyncio.gather(*(_doh(c, sem, f"_dmarc.{d}", "TXT") for d in domains)),
            asyncio.gather(*(_doh(c, sem, d, "NS") for d in domains)),
            asyncio.gather(*(_site(c, sem, d) for d in domains)))
    return dict(zip(domains, mx)), dict(zip(domains, dmarc)), dict(zip(domains, ns)), dict(zip(domains, sites))


async def collect_smtp_ips(hosts: list[str]) -> dict:
    """{host: {ip: [zones listed on]}} — Spamhaus ZEN via its own name servers
    (public resolvers are refused), Barracuda + SpamCop via DoH."""
    out: dict = {}
    if not hosts:
        return out
    from blacklist_monitor import _listed_ips_auth, _zone_ns_ip
    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=10, headers=UA) as c:
        zen_ns = await _zone_ns_ip(c, "zen.spamhaus.org")
        for host in hosts:
            ips = await _doh(c, sem, host, "A") or []
            out[host] = {}
            for ip in ips[:4]:
                rev = ".".join(reversed(ip.split(".")))
                zones = []
                if zen_ns:
                    hit = await _listed_ips_auth(zen_ns, rev, "zen.spamhaus.org")
                    if hit:
                        zones.append("Spamhaus ZEN")
                for z in IP_ZONES_DOH:
                    hit = await _doh(c, sem, f"{rev}.{z}", "A")
                    if hit and any(h.startswith("127.") for h in hit):
                        zones.append(z)
                out[host][ip] = zones
    return out


def main_domains() -> dict[str, str]:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domain_clients.json")
    with open(path, encoding="utf-8") as fh:
        clients = json.load(fh).get("clients") or {}
    return {k: (v.get("main_domain") or "").lower() for k, v in clients.items()}


def run(only: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    accounts, client_of_email, active, campaigns, client_of_campaign = collect_smartlead(only)
    client_of_domain = {e.split("@")[1]: c for e, c in client_of_email.items()}
    domains = sorted(client_of_domain)
    mx, dmarc, ns, sites = asyncio.run(collect_dns_and_sites(domains))
    smtp_hosts = sorted({str(a.get("smtp_host") or "").lower() for a in accounts
                         if str(a.get("type") or "").upper() == "SMTP" and a.get("smtp_host")})
    client_of_host = {}
    for a in accounts:
        if a.get("smtp_host"):
            client_of_host.setdefault(str(a["smtp_host"]).lower(), client_of_email[a["from_email"].lower()])
    ip_listings = asyncio.run(collect_smtp_ips(smtp_hosts))
    mains = main_domains()

    findings = []
    for d in domains:
        c = client_of_domain[d]
        findings += ia.check_main_domain(d, c, mains.get(c, ""))
        findings += ia.check_dns(d, c, mx[d], dmarc[d])
        findings += ia.check_redirect(d, c, mains.get(c, ""), sites[d])
    findings += ia.check_ns_clusters(ns, client_of_domain)
    findings += ia.check_mailboxes(accounts, client_of_email, active, now)
    findings += ia.check_provider_mix(accounts, client_of_email)
    findings += ia.check_esp_matching(campaigns, client_of_campaign)
    findings += ia.check_smtp_ips(ip_listings, client_of_host)
    findings.sort(key=lambda f: (ia.SEV_ORDER[f["severity"]], f["check"], f["client"], f["subject"]))
    checked = {"domains": len(domains), "inboxes": len(accounts), "in_campaigns": len(active),
               "active_campaigns": len(campaigns), "smtp_hosts": len(smtp_hosts)}
    return {"checked": checked, "findings": findings, "text": ia.summarize(findings, checked),
            "unknown": {"mx": sum(1 for v in mx.values() if v is None),
                        "dmarc": sum(1 for v in dmarc.values() if v is None)}}


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily infra audit (read-only)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--client", choices=list(CURRENT_CLIENTS))
    args = ap.parse_args()
    try:
        res = run(args.client)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}) if args.json else f"ERROR: {exc}")
        return 0 if args.json else 1
    if args.post:
        from smartlead.notify import _post
        token = os.getenv("SLACK_BOT_TOKEN", "")
        channel = os.getenv("INFRA_AUDIT_CHANNEL") or os.getenv("HEALTH_NOTIFY_CHANNEL", "")
        res["posted"] = bool(token and channel and _post(token, channel, res["text"][:39000]))
        if not res["posted"]:
            print("  [Audit] INFRA_AUDIT_CHANNEL / SLACK_BOT_TOKEN missing or post failed — printed only")
    if args.json:
        print(json.dumps(res, default=str))
    else:
        print(res["text"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
