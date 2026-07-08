"""Automated DNS authentication checker for domain deliverability.

Queries Google's DNS-over-HTTPS (DoH) API to audit SPF, DKIM, and DMARC.
Caches domain results in-memory to prevent duplicate lookups.

DKIM is checked at MULTIPLE selectors (Google Workspace publishes at
'google._domainkey', Microsoft 365 at 'selector1/selector2._domainkey',
generic/SMTP setups at 'default._domainkey') — a domain passes if a valid
DKIM record exists at ANY of them. Checking only 'default' (the old
behavior) falsely flagged nearly every Gmail/Outlook domain as
"DKIM missing" (verified 2026-07-08: e.g. belardiwongconnect.com has valid
DKIM at google._domainkey but nothing at default._domainkey).

Lookup ERRORS (DoH timeout, network) are distinguished from "record absent":
an errored lookup reports ok=True with an 'unknown' message and is NOT
cached, so transient DoH flakiness can't fabricate a false P0 or persist.
"""
from __future__ import annotations

import asyncio
import sys
import httpx

# In-memory cache to prevent duplicate queries during a single sync run
# Format: {domain: {spf_ok, spf_msg, dkim_ok, dkim_msg, dmarc_ok, dmarc_msg}}
_DNS_CACHE: dict[str, dict] = {}

# DKIM selectors to try, in order of how common they are in this fleet.
DKIM_SELECTORS: tuple[str, ...] = ("google", "selector1", "selector2", "default")


async def resolve_txt(name: str) -> list[str] | None:
    """Resolve TXT records via Google DoH.

    Returns a list of record strings ([] = domain answered, no records =
    genuinely absent) or None on lookup ERROR (timeout/network) — callers
    must treat None as "unknown", never as "missing"."""
    url = "https://dns.google/resolve"
    params = {"name": name, "type": "TXT"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            records = []
            for answer in data.get("Answer", []):
                # Type 16 is TXT
                if answer.get("type") == 16:
                    raw_data = answer.get("data", "")
                    # Clean up quotes and joined segments (DNS splits long values)
                    clean_data = raw_data.strip('"').replace('" "', '')
                    records.append(clean_data)
            return records
    except Exception as e:
        print(f"[DNS] Error querying {name}: {e}", file=sys.stderr)
        return None


def audit_spf(records: list[str]) -> tuple[bool, str]:
    spf_records = [r for r in records if r.startswith("v=spf1")]
    if not spf_records:
        return False, "Missing SPF record"
    if len(spf_records) > 1:
        return False, f"Multiple SPF records found: {spf_records}"

    spf = spf_records[0]
    if "+all" in spf:
        return False, f"SPF has unsafe '+all': {spf}"
    if spf.endswith("~all") or spf.endswith("-all") or "~all " in spf or "-all " in spf:
        return True, spf

    return False, f"SPF policy not optimal: {spf}"


def _is_dkim(record: str) -> bool:
    return "v=DKIM1" in record or "k=rsa" in record


def audit_dkim_multi(by_selector: dict[str, list[str] | None]) -> tuple[bool, str]:
    """Pass if ANY selector holds a DKIM record. by_selector maps
    selector -> TXT records ([] = absent, None = lookup error)."""
    errored: list[str] = []
    for sel, records in by_selector.items():
        if records is None:
            errored.append(sel)
            continue
        hits = [r for r in records if _is_dkim(r)]
        if hits:
            return True, f"DKIM found at selector '{sel}': {hits[0][:60]}..."
    if errored:
        # couldn't check everything -> unknown, never "missing"
        return True, f"DKIM lookup error at selector(s) {','.join(errored)} - status unknown"
    checked = "/".join(by_selector.keys())
    return False, f"No DKIM record at any known selector ({checked})"


def audit_dmarc(records: list[str]) -> tuple[bool, str]:
    dmarc_records = [r for r in records if r.startswith("v=DMARC1")]
    if not dmarc_records:
        return False, "Missing DMARC record"
    if len(dmarc_records) > 1:
        return False, f"Multiple DMARC records found: {dmarc_records}"

    dmarc = dmarc_records[0]
    if "p=reject" in dmarc or "p=quarantine" in dmarc:
        return True, dmarc
    if "p=none" in dmarc:
        # None policy is OK for new domains, but flagged as a warning in health.py
        return True, dmarc

    return False, f"DMARC policy not recognized or invalid: {dmarc}"


async def audit_domain_dns(domain: str, selector: str | None = None) -> dict:
    """Full DNS deliverability audit for a domain. Uses caching.

    `selector` narrows DKIM to one selector (ad-hoc/CLI use); default None
    checks all DKIM_SELECTORS and passes on any hit."""
    dom_key = f"{domain}:{selector or 'multi'}".lower()
    if dom_key in _DNS_CACHE:
        return _DNS_CACHE[dom_key]

    # Test/mock guard: exact 'mock*' domains and .local only (a real client
    # domain merely CONTAINING 'mock' must not skip the audit).
    if (not domain or "." not in domain or domain.endswith(".local")
            or domain.split(".", 1)[0].startswith("mock")):
        res = {
            "spf_ok": True, "spf_msg": "v=spf1 include:_spf.google.com ~all",
            "dkim_ok": True, "dkim_msg": "v=DKIM1; k=rsa; p=MIIB...",
            "dmarc_ok": True, "dmarc_msg": "v=DMARC1; p=quarantine"
        }
        _DNS_CACHE[dom_key] = res
        return res

    selectors = (selector,) if selector else DKIM_SELECTORS
    dmarc_domain = f"_dmarc.{domain}"

    lookups = await asyncio.gather(
        resolve_txt(domain),
        resolve_txt(dmarc_domain),
        *(resolve_txt(f"{sel}._domainkey.{domain}") for sel in selectors),
    )
    spf_records, dmarc_records = lookups[0], lookups[1]
    dkim_by_selector = dict(zip(selectors, lookups[2:]))

    had_error = False

    if spf_records is None:
        spf_ok, spf_msg = True, "SPF lookup error - status unknown"
        had_error = True
    else:
        spf_ok, spf_msg = audit_spf(spf_records)

    dkim_ok, dkim_msg = audit_dkim_multi(dkim_by_selector)
    if "status unknown" in dkim_msg:
        had_error = True

    if dmarc_records is None:
        dmarc_ok, dmarc_msg = True, "DMARC lookup error - status unknown"
        had_error = True
    else:
        dmarc_ok, dmarc_msg = audit_dmarc(dmarc_records)

    res = {
        "spf_ok": spf_ok,
        "spf_msg": spf_msg,
        "dkim_ok": dkim_ok,
        "dkim_msg": dkim_msg,
        "dmarc_ok": dmarc_ok,
        "dmarc_msg": dmarc_msg,
    }

    # never cache an errored audit — retry fresh next call/run
    if not had_error:
        _DNS_CACHE[dom_key] = res
    return res


def clear_dns_cache() -> None:
    """Clear the in-memory DNS cache."""
    _DNS_CACHE.clear()
