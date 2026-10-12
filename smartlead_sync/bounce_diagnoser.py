#!/usr/bin/env python3
"""Bounce diagnoser — answers "why are bounces high on this campaign?".

Read-only. Never writes to Smartlead, DNS, or Mongo.

Complements bounce_protect_executor.py: that one PREVENTS damage (auto-pause at
3%); this one EXPLAINS a spike after the fact, so the fix targets the real cause
instead of the list.

Built 2026-08-25 from a client bounce post-mortem. That spike looked like list
decay and was actually 2-month-old sending domains plus one inbox group running
3x the daily cap of its siblings. Every check below is one that mattered in that diagnosis.

The four questions, in order:
  1. What IS the rate?           bounced / unique_sent - the denominator people skip
  2. Who does Smartlead blame?   lead_category "Sender Originated Bounce" = our side
  3. One inbox or all of them?   attribution via per-lead message history
  4. Is the infra sane?          warmup %, daily caps, SPF/DKIM/DMARC, domain age

Usage:
    python3 bounce_diagnoser.py <campaign_id>              # default account
    python3 bounce_diagnoser.py <campaign_id> --account BettrData
    python3 bounce_diagnoser.py --scan                     # every ACTIVE campaign over threshold
"""
from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter
from datetime import datetime, timezone

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import httpx

# `smartlead/__init__.py` imports the Sheets stack (gspread, polars). This tool writes
# nothing to Sheets, so stub that submodule before importing — a read-only diagnosis
# should not be gated on reporting dependencies being installed.
import types as _types

if "smartlead.sheets" not in sys.modules:
    _stub = _types.ModuleType("smartlead.sheets")
    _stub.SheetsWriter = None       # type: ignore[attr-defined]
    _stub.DeliverabilityReader = None  # type: ignore[attr-defined]
    sys.modules["smartlead.sheets"] = _stub

from smartlead.accounts import discover_accounts  # noqa: E402
from smartlead.api import SmartleadClient  # noqa: E402

DANGER_RATE = 3.0          # % — the industry line where inbox reputation starts to suffer
YOUNG_DOMAIN_DAYS = 90     # domains younger than this get throttled by corporate gateways
LOW_WARMUP = 90            # % — below this, the inbox is not trusted yet
ATTRIBUTION_SAMPLE = 15    # bounced messages to trace back to a sending inbox


# ── DNS (DoH, same approach as check_dns.py / blacklist_monitor.py) ──────────

async def _doh(name: str, rtype: str) -> list[str]:
    url = "https://dns.google/resolve"
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(url, params={"name": name, "type": rtype})
            r.raise_for_status()
            return [a.get("data", "").strip('"') for a in r.json().get("Answer", [])]
    except Exception:
        return []


async def domain_facts(domain: str) -> dict:
    """SPF / DMARC / provider / registration age for one sending domain."""
    txt, dmarc, mx = await asyncio.gather(
        _doh(domain, "TXT"), _doh("_dmarc." + domain, "TXT"), _doh(domain, "MX"))
    spf = next((t for t in txt if t.lower().startswith("v=spf1")), "")
    dm = next((t for t in dmarc if "v=DMARC1" in t), "")
    mxs = " ".join(mx).lower()
    provider = ("Google" if "google" in mxs
                else "Microsoft" if ("outlook" in mxs or "microsoft" in mxs) else "?")

    age_days, registered = None, ""
    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            r = await c.get(f"https://rdap.verisign.com/com/v1/domain/{domain}")
            if r.status_code == 200:
                ev = next((e for e in r.json().get("events", [])
                           if e.get("eventAction") == "registration"), None)
                if ev:
                    registered = ev["eventDate"][:10]
                    age_days = (datetime.now(timezone.utc)
                                - datetime.fromisoformat(ev["eventDate"].replace("Z", "+00:00"))).days
    except Exception:
        pass

    return {"domain": domain, "spf": spf, "dmarc": dm, "provider": provider,
            "registered": registered, "age_days": age_days,
            "spf_softfail": "~all" in spf, "spf_missing": not spf, "dmarc_missing": not dm}


# ── diagnosis ────────────────────────────────────────────────────────────────

async def diagnose(client: SmartleadClient, campaign_id: str) -> dict:
    analytics = await client.get_campaign_analytics(campaign_id)
    unique_sent = int(analytics.get("unique_sent_count") or 0)
    bounced = int(analytics.get("bounce_count") or 0)
    rate = (100.0 * bounced / unique_sent) if unique_sent else 0.0

    print(f"\n=== {analytics.get('name')} ({campaign_id}) — {analytics.get('status')} ===")
    print(f"unique sent {unique_sent} | bounced {bounced} | replies {analytics.get('reply_count')}")
    verdict = "*** ABOVE THE 3% DANGER LINE ***" if rate > DANGER_RATE else "(under 3%, healthy)"
    print(f"BOUNCE RATE: {rate:.1f}%  {verdict}")

    # 2. Smartlead's own attribution
    stats = await client._get(f"/campaigns/{campaign_id}/statistics",
                              {"limit": 500, "email_status": "bounced"})
    rows = stats.get("data", []) if isinstance(stats, dict) else []
    cats = Counter(r.get("lead_category") or "uncategorized" for r in rows)
    recips = Counter(str(r.get("lead_email", "?")).split("@")[-1] for r in rows)
    dates = Counter(str(r.get("sent_time") or "")[:10] for r in rows if r.get("sent_time"))

    sender_originated = sum(v for k, v in cats.items() if "sender originated" in k.lower())

    print("\n--- Smartlead bounce categories ---")
    for k, v in cats.most_common():
        flag = "  <-- OUR SENDING SIDE, not the recipient" if "sender originated" in k.lower() else ""
        print(f"  {v:3d}  {k}{flag}")

    print("\n--- recipient domains (3+ = gateway refusing our sender, not dead mailboxes) ---")
    for k, v in recips.most_common(12):
        print(f"  {v:3d}  {k}" + ("  <-- whole-company cluster" if v >= 3 else ""))

    print("\n--- bounces by send date (spikes = volume ramp outrunning domain trust) ---")
    for k, v in sorted(dates.items()):
        print(f"  {v:3d}  {k}")

    # 3. Which sending inbox?
    print(f"\n--- sending-inbox attribution (sampling {ATTRIBUTION_SAMPLE}) ---")
    froms: Counter = Counter()
    for r in rows[:ATTRIBUTION_SAMPLE]:
        try:
            found = await client._get("/leads/", {"email": r["lead_email"]})
            lead = found[0] if isinstance(found, list) and found else found
            lead_id = (lead or {}).get("id")
            if not lead_id:
                continue
            hist = await client._get(f"/campaigns/{campaign_id}/leads/{lead_id}/message-history")
            for m in (hist.get("history") or []):
                src = m.get("from") or m.get("from_email")
                if src:
                    froms[src] += 1
                    break
        except Exception:
            continue
    for k, v in froms.most_common():
        print(f"  {v:3d}  {k}")
    if len(froms) > 1:
        print("  READ: spread across inboxes -> systemic (domain age / volume), not one broken inbox.")
    elif froms:
        print("  READ: concentrated in one inbox -> isolate that inbox first.")

    # 4. Infrastructure
    accounts = await client.get_campaign_email_accounts(campaign_id)
    print("\n--- sending inboxes ---")
    print(f"{'inbox':<40}{'daily':<7}{'warmup%':<9}status")
    degraded, caps, domains = [], Counter(), set()
    for a in sorted(accounts, key=lambda x: str(x.get("from_email", ""))):
        w = a.get("warmup_details") or {}
        rep_raw = str(w.get("warmup_reputation") or "")
        rep = int("".join(ch for ch in rep_raw if ch.isdigit()) or 0)
        cap = a.get("message_per_day")
        email = str(a.get("from_email", "?"))
        low = 0 < rep < LOW_WARMUP
        if low:
            degraded.append((email, rep, cap))
        caps[cap] += 1
        domains.add(email.split("@")[-1])
        print(f"{email:<40}{str(cap):<7}{rep_raw:<9}{w.get('status', '')}" + ("  <-- DEGRADED" if low else ""))

    if len(caps) > 1:
        print(f"\n  !! MIXED DAILY CAPS across inboxes: {dict(caps)} — the high-cap group is the one to check first.")

    print("\n--- sending domains: auth + age ---")
    facts = await asyncio.gather(*(domain_facts(d) for d in sorted(domains)))
    young = []
    for f in facts:
        print(f"\n  {f['domain']}  [{f['provider']}]  registered {f['registered'] or '?'}"
              + (f"  ({f['age_days']}d old)" if f['age_days'] is not None else ""))
        print(f"    SPF   {f['spf'][:90] or 'NONE'}"
              + ("   <-- ~all is a SOFTFAIL; -all is stricter" if f["spf_softfail"] else ""))
        print(f"    DMARC {f['dmarc'][:70] or 'NONE'}")
        if f["age_days"] is not None and f["age_days"] < YOUNG_DOMAIN_DAYS:
            young.append(f["domain"])
            print(f"    *** {f['age_days']} DAYS OLD — young domains get throttled/rejected by corporate gateways ***")

    # verdict
    print("\n=== VERDICT ===")
    clustered = [d for d, n in recips.items() if n >= 3]
    if rate <= DANGER_RATE:
        print("  Bounce rate is within tolerance. No action needed.")
    elif sender_originated or clustered or young:
        print("  SENDING-SIDE problem, not list quality. Evidence:")
        if sender_originated:
            print(f"    - {sender_originated}/{len(rows)} categorised 'Sender Originated Bounce' by Smartlead")
        if clustered:
            print(f"    - whole-company clusters at {', '.join(clustered[:5])} (verified mailboxes, refused at the gateway)")
        if young:
            print(f"    - young sending domains: {', '.join(young)}")
        if degraded:
            print(f"    - degraded warmup: {', '.join(f'{e} ({r}%, {c}/day)' for e, r, c in degraded)}")
        print("  FIX: lower daily caps on the weakest group, let warmup recover past 90%,")
        print("       and hold total volume down until the domains age past ~90 days.")
    else:
        print("  Looks like genuine list decay — verify addresses pre-send (BounceBan, 1cr each)")
        print("  and confirm with an independent verifier before re-mailing this list.")
    print("  NOTE: if a verifier calls the bounced addresses deliverable, the list was never the problem.\n")

    return {"campaign_id": campaign_id, "rate": rate, "bounced": bounced,
            "unique_sent": unique_sent, "sender_originated": sender_originated,
            "young_domains": young, "degraded": degraded}


async def scan(client: SmartleadClient) -> None:
    """Every ACTIVE campaign over the danger line, worst first."""
    campaigns = await client.list_campaigns()
    hits = []
    for c in campaigns:
        if str(c.get("status", "")).upper() not in {"ACTIVE", "PAUSED"}:
            continue
        try:
            a = await client.get_campaign_analytics(str(c["id"]))
        except Exception:
            continue
        sent = int(a.get("unique_sent_count") or 0)
        b = int(a.get("bounce_count") or 0)
        if sent >= 50 and b and (100.0 * b / sent) > DANGER_RATE:
            hits.append((100.0 * b / sent, str(c["id"]), c.get("name")))
    hits.sort(reverse=True)
    print(f"\n{len(hits)} campaign(s) over {DANGER_RATE}%:")
    for rate, cid, name in hits:
        print(f"  {rate:5.1f}%  {cid}  {name}")
    print("\nRun `python3 bounce_diagnoser.py <campaign_id>` on any of them for the full read.")


async def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    want_scan = "--scan" in sys.argv
    account_name = None
    if "--account" in sys.argv:
        account_name = sys.argv[sys.argv.index("--account") + 1]

    accounts = discover_accounts()
    if not accounts:
        sys.exit("No Smartlead accounts found in the environment (SMARTLEAD_API_KEY*).")
    cfg = next((a for a in accounts if a.name.lower() == (account_name or "").lower()),
               accounts[0])
    print(f"[account: {cfg.name}]")

    async with SmartleadClient(cfg.api_key, cfg.name) as client:
        if want_scan:
            await scan(client)
        elif args:
            await diagnose(client, args[0])
        else:
            sys.exit("usage: bounce_diagnoser.py <campaign_id> [--account NAME] | --scan")


if __name__ == "__main__":
    asyncio.run(main())
