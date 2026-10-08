"""Daily infrastructure audit: the setup checks nothing else in the bot covered.

Built 2026-10-08 against a full cold-email setup checklist (domains, DNS,
name servers, redirects, mailbox caps, warmup, sender identity, provider
mix). The DNS-auth, blacklist-domain, bounce, placement and reply-drop
checks already run elsewhere; this adds the rest. Pure functions here (unit
tested); ``infra_audit.py`` collects the data and posts.

Every finding: {"check", "severity" P0/P1/P2, "client", "subject", "detail", "fix"}.
  P0 = act today (sending at real risk)   P1 = fix this week   P2 = tidy up
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime

# Mailboxes per domain (Google Workspace / SMTP). Outlook tenants hold 25-50
# by design and are capped per mailbox instead.
MAX_MAILBOXES_PER_DOMAIN = {"GMAIL": 3, "SMTP": 4}     # SMTP: ScaledMail sells 4 per domain
MAX_SENDS_PER_DOMAIN = 100                             # per day, Google + SMTP domains
MAX_PER_MAILBOX = {"GMAIL": 25, "OUTLOOK": 10, "SMTP": 15}   # config.CAMPAIGN_CAP_*
MIN_WARMUP_DAYS = 21
# Domains of one client on one name-server set. 2026-10-08: Precise Leads had
# 13 and Melior 10 on single Cloudflare accounts (ScaledMail domains) — the
# exact footprint blacklists started hitting in 2026.
NS_CLUSTER_MAX = 8
PROVIDER_SKEW = 0.9            # one provider above 90% of a client's inboxes
WARMUP_TO_COLD_MIN = 0.5       # warmup max/day at least half the cold max/day


def finding(check, severity, client, subject, detail, fix):
    return {"check": check, "severity": severity, "client": client or "?",
            "subject": subject, "detail": detail, "fix": fix}


def _days_since(iso: str | None, now: datetime) -> float | None:
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (now - t).total_seconds() / 86400


# ── DNS ──────────────────────────────────────────────────────────────────────

def check_dns(domain: str, client: str, mx: list[str] | None, dmarc: list[str] | None) -> list[dict]:
    """MX present (replies + bounces need it) and DMARC beyond p=none.
    ``None`` = lookup error: unknown, never reported as missing."""
    out = []
    if mx is not None and not mx:
        out.append(finding("dns_mx", "P0", client, domain, "no MX record",
                           "Add the provider's MX records (Google / Outlook / SMTP host)."))
    if dmarc is not None:
        recs = [r for r in dmarc if r.lower().startswith("v=dmarc1")]
        if recs and "p=none" in recs[0].replace(" ", "").lower():
            out.append(finding("dns_dmarc_policy", "P2", client, domain, "DMARC policy is p=none",
                               "Move to p=quarantine once the domain has sent cleanly for 2-3 weeks."))
    return out


def check_ns_clusters(ns_by_domain: dict[str, list[str]], client_of: dict[str, str]) -> list[dict]:
    """Many of one client's domains on the same name servers is a footprint
    blacklists look for (e.g. one Cloudflare account for everything)."""
    groups: dict[tuple, list[str]] = defaultdict(list)
    for dom, ns in ns_by_domain.items():
        if ns:
            groups[(client_of.get(dom, "?"), tuple(sorted(n.lower().rstrip(".") for n in ns)))].append(dom)
    out = []
    for (client, ns), doms in groups.items():
        if len(doms) > NS_CLUSTER_MAX:
            where = (" (one Cloudflare account)" if any("cloudflare" in n for n in ns)
                     else " (Zapmail's shared DNS — Zapmail requires it)" if any("cloudns" in n for n in ns) else "")
            out.append(finding("ns_cluster", "P1" if "Cloudflare" in where else "P2", client,
                               f"{len(doms)} domains on {', '.join(ns[:2])}",
                               f"{len(doms)} of this client's sending domains share one name-server set{where}",
                               "Spread new domains across registrars / name-server sets (e.g. ScaledMail, "
                               "Dynadot) instead of adding more to this set."))
    return out


# ── website / redirect ───────────────────────────────────────────────────────

def _host(url: str) -> str:
    u = str(url or "").lower().split("://")[-1]
    return u.split("/")[0].split(":")[0].removeprefix("www.")


def check_redirect(domain: str, client: str, main_domain: str, result: dict) -> list[dict]:
    """``result`` = {final_url, status, error}. A cold domain should send a
    visitor to the client's real site (redirect) or show it (masking)."""
    main = _host(main_domain)
    if result.get("error"):
        return [finding("website", "P1", client, domain, f"no website ({result['error'][:60]})",
                        f"Redirect {domain} to {main or 'the client site'} (or set up masking).")]
    final = _host(result.get("final_url", ""))
    status = int(result.get("status") or 0)
    if final == main or (main and final.endswith("." + main)):
        return []
    if final == domain and 200 <= status < 300:
        # Its own page: fine when it is the client's site shown through
        # masking; a parked / generic page tells a curious prospect nothing.
        brand = main.split(".")[0]
        page = str(result.get("page") or "").lower()
        if brand and page and brand not in page.replace(" ", ""):
            return [finding("website", "P2", client, domain, "own page that never mentions the client (parked?)",
                            f"Redirect {domain} to {main}, or mask the client's site on it.")]
        return []
    if final == domain:
        return [finding("website", "P1", client, domain, f"site answers {status}",
                        f"Redirect {domain} to {main or 'the client site'}.")]
    return [finding("website", "P1", client, domain, f"redirects to {final}, not {main}",
                    f"Point the redirect at {main}.")]


def check_main_domain(domain: str, client: str, main_domain: str) -> list[dict]:
    """Never send cold email from the client's real ('sacred') domain."""
    if main_domain and domain == _host(main_domain):
        return [finding("main_domain", "P0", client, domain, "cold mail is sent from the client's MAIN domain",
                        "Remove these inboxes from cold campaigns now; use lookalike domains.")]
    return []


# ── mailboxes ────────────────────────────────────────────────────────────────

def check_mailboxes(accounts: list[dict], client_of_email: dict[str, str],
                    active: dict[str, list[str]], now: datetime) -> list[dict]:
    """Per-domain caps, per-mailbox caps, warmup age/volume, signature."""
    out = []
    by_domain: dict[str, list[dict]] = defaultdict(list)
    for a in accounts:
        by_domain[a["from_email"].lower().split("@")[1]].append(a)
    for dom, boxes in by_domain.items():
        kind = str(boxes[0].get("type") or "").upper()
        client = client_of_email.get(boxes[0]["from_email"].lower(), "?")
        cap = MAX_MAILBOXES_PER_DOMAIN.get(kind)
        if cap and len(boxes) > cap:
            out.append(finding("mailboxes_per_domain", "P1", client, dom,
                               f"{len(boxes)} {kind.title()} mailboxes on one domain (max {cap})",
                               "Move the extra mailboxes to another domain or retire them."))
        if kind in ("GMAIL", "SMTP"):
            total = sum(int(b.get("message_per_day") or 0) for b in boxes)
            if total > MAX_SENDS_PER_DOMAIN:
                out.append(finding("sends_per_domain", "P1", client, dom,
                                   f"{total} cold emails/day allowed on one domain (max {MAX_SENDS_PER_DOMAIN})",
                                   "Lower the per-inbox daily limits in Smartlead."))
    for a in accounts:
        email = a["from_email"].lower()
        client = client_of_email.get(email, "?")
        kind = str(a.get("type") or "").upper()
        in_use = email in active
        mpd = int(a.get("message_per_day") or 0)
        cap = MAX_PER_MAILBOX.get(kind, MAX_PER_MAILBOX["SMTP"])
        if mpd > cap:
            out.append(finding("sends_per_mailbox", "P2", client, email, f"daily limit {mpd} (cap {cap} for {kind.title()})",
                               f"Set the daily limit to {cap} or less in Smartlead."))
        w = a.get("warmup_details") or {}
        age = _days_since(w.get("warmup_created_at") or a.get("created_at"), now)
        if in_use and age is not None and age < MIN_WARMUP_DAYS:
            out.append(finding("young_inbox_in_campaign", "P0", client, email,
                               f"warmed {age:.0f} days, already in {', '.join(active[email][:2])}",
                               f"Take it out of the campaign until day {MIN_WARMUP_DAYS}."))
        if in_use and str(w.get("status") or "").upper() != "ACTIVE":
            out.append(finding("warmup_off", "P0", client, email, "warmup is OFF on an inbox that is sending",
                               "Turn warmup back on (team rule: warmup always on)."))
        wmax = int(w.get("max_email_per_day") or w.get("warmup_max_count") or 0)
        if in_use and mpd and wmax and wmax < mpd * WARMUP_TO_COLD_MIN:
            out.append(finding("warmup_below_cold", "P2", client, email,
                               f"warmup {wmax}/day vs cold {mpd}/day",
                               "Raise warmup so it is close to the cold volume."))
        if in_use and not str(a.get("signature") or "").strip():
            out.append(finding("signature_missing", "P2", client, email, "no signature",
                               "Apply the client's signature (Zapmail domain look-up → Name & signature)."))
        if not str(a.get("from_name") or "").strip() or "@" in str(a.get("from_name") or ""):
            out.append(finding("sender_name", "P2", client, email, "no real sender name",
                               "Set a real first + last name as the sender."))
    return out


def check_provider_mix(accounts: list[dict], client_of_email: dict[str, str]) -> list[dict]:
    """Don't depend on one provider: a ban or filter change takes everything."""
    per: dict[str, Counter] = defaultdict(Counter)
    for a in accounts:
        per[client_of_email.get(a["from_email"].lower(), "?")][str(a.get("type") or "?").upper()] += 1
    out = []
    for client, c in per.items():
        total = sum(c.values())
        kind, n = c.most_common(1)[0]
        if total >= 10 and n / total > PROVIDER_SKEW:
            out.append(finding("provider_mix", "P2", client, f"{n}/{total} inboxes are {kind.title()}",
                               "one provider carries almost all sending",
                               "Add some Outlook / SMTP (or Google) inboxes so one provider's change can't stop everything."))
    return out


def check_esp_matching(campaigns: list[dict], client_of_campaign: dict) -> list[dict]:
    """Active campaigns should match Google→Google, Outlook→Outlook."""
    out = []
    for c in campaigns:
        if c.get("enable_ai_esp_matching") is False:
            out.append(finding("esp_matching", "P2", client_of_campaign.get(c["id"], "?"), c.get("name", c["id"]),
                               "ESP matching is off", "Turn on ESP matching in the campaign settings."))
    return out


def check_smtp_ips(listings: dict[str, dict[str, list[str]]], client_of_host: dict[str, str]) -> list[dict]:
    """``{host: {ip: [zones listed on]}}`` for SMTP sending hosts."""
    out = []
    for host, ips in listings.items():
        for ip, zones in ips.items():
            if zones:
                out.append(finding("ip_blacklist", "P0", client_of_host.get(host, "?"), f"{host} ({ip})",
                                   "sending IP listed on " + ", ".join(zones),
                                   "Ask the SMTP provider to move these inboxes to a clean IP; pause them meanwhile."))
    return out


# ── report ───────────────────────────────────────────────────────────────────

NAMES = {
    "dns_mx": "No MX record", "dns_dmarc_policy": "DMARC p=none", "ns_cluster": "Name-server footprint",
    "website": "Website / redirect", "main_domain": "Sending from the main domain",
    "mailboxes_per_domain": "Too many mailboxes on a domain", "sends_per_domain": "Domain over 100/day",
    "sends_per_mailbox": "Mailbox over its cap", "young_inbox_in_campaign": "Inbox under 21 days in a campaign",
    "warmup_off": "Warmup off while sending", "warmup_below_cold": "Warmup below cold volume",
    "signature_missing": "No signature", "sender_name": "No real sender name",
    "provider_mix": "One provider only", "esp_matching": "ESP matching off", "ip_blacklist": "Sending IP blacklisted",
}
SEV_ORDER = {"P0": 0, "P1": 1, "P2": 2}


def summarize(findings: list[dict], checked: dict) -> str:
    """Slack text: counts per check, worst first, a few examples each."""
    sev = Counter(f["severity"] for f in findings)
    head = (f"*🔎 Infra audit* — {checked.get('domains', 0)} domains, {checked.get('inboxes', 0)} inboxes · "
            f"*{sev.get('P0', 0)} P0* · {sev.get('P1', 0)} P1 · {sev.get('P2', 0)} P2")
    if not findings:
        return head + "\nAll checks passed."
    groups: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        groups[f["check"]].append(f)
    lines = [head]
    for check, items in sorted(groups.items(), key=lambda kv: (min(SEV_ORDER[f["severity"]] for f in kv[1]), kv[0])):
        s = min((f["severity"] for f in items), key=SEV_ORDER.get)
        by_client = Counter(f["client"] for f in items)
        lines.append(f"\n*{s} · {NAMES.get(check, check)}* ({len(items)}) — "
                     + ", ".join(f"{c} {n}" for c, n in by_client.most_common()))
        for f in items[:4]:
            lines.append(f"   • `{f['subject']}` — {f['detail']}")
        if len(items) > 4:
            lines.append(f"   • …and {len(items) - 4} more")
        lines.append(f"   _Fix: {items[0]['fix']}_")
    lines.append("\n_Not checkable by the bot: Google Postmaster / Microsoft SNDS reputation (needs account "
                 "access), spam traps, profile photos. Blacklists, bounces, placement and reply drops run in their own jobs._")
    return "\n".join(lines)
