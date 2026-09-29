#!/usr/bin/env python3
"""Live self-test of every FREE Zapmail feature, on every account + provider.

READ endpoints only. Nothing here buys, renews, connects, creates, exports,
tags or changes anything; the spend switch is forced off for the run. The one
thing it consumes is Zapmail's domain-search budget: 1 bulk-availability
request, 1 single-name request, and 1 AI Domain Finder run (of the 10 per
30 minutes).

    python zapmail_selftest.py                # table on stdout
    python zapmail_selftest.py --json         # machine-readable
    python zapmail_selftest.py --md docs/ZAPMAIL_LIVE_TEST.md

Use it after a Zapmail change, a new API key, or when a support answer lands,
to see in one run what works, what errors, and what each call returns.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

os.environ.pop("ZAPMAIL_ALLOW_SPEND", None)  # this tool never spends

from smartlead.zapmail import PROVIDERS, ZapmailClient, ZapmailError  # noqa: E402
from smartlead.zapmail_accounts import discover_zapmail_accounts  # noqa: E402

# Keys whose values are credentials — never printed.
_SECRET_KEYS = {"password", "appPassword", "secret", "adminDetails", "msSubAdminDetails",
                "recoveryEmail", "apiKey", "token"}


def _summarize(resp) -> str:
    """A one-line description of a response's shape and counts. No secrets."""
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    if data is None:
        return "empty"
    if isinstance(data, list):
        first = data[0] if data else None
        keys = sorted(k for k in first.keys() if k not in _SECRET_KEYS)[:8] \
            if isinstance(first, dict) else []
        return f"list[{len(data)}]" + (f" keys={keys}" if keys else "")
    if not isinstance(data, dict):
        return str(data)[:80]
    parts = []
    for k, v in data.items():
        if k in _SECRET_KEYS:
            continue
        if isinstance(v, list):
            parts.append(f"{k}[{len(v)}]")
        elif isinstance(v, (int, float, bool)) or v is None:
            parts.append(f"{k}={v}")
        elif isinstance(v, str) and len(v) <= 24:
            parts.append(f"{k}={v}")
    return ", ".join(parts[:10]) or f"keys={sorted(data.keys())[:8]}"


async def _check(results, feature, account, provider, coro_factory, note=""):
    t = time.monotonic()
    row = {"feature": feature, "account": account, "provider": provider or "-",
           "note": note}
    try:
        resp = await coro_factory()
        row.update(ok=True, summary=_summarize(resp))
    except ZapmailError as exc:
        row.update(ok=False, summary=str(exc)[:160])
    except Exception as exc:  # noqa: BLE001 — record, keep going
        row.update(ok=False, summary=f"{type(exc).__name__}: {str(exc)[:140]}")
    row["ms"] = int((time.monotonic() - t) * 1000)
    results.append(row)
    return row


async def run() -> list[dict]:
    results: list[dict] = []
    accounts = discover_zapmail_accounts()
    first_domain_id: dict[tuple, str] = {}
    renewal_ids: dict[tuple, list[str]] = {}
    placement_order: dict[str, str] = {}
    shield_sub: dict[str, str] = {}

    for acc in accounts:
        # ── account-level (provider-independent) ─────────────────────────
        async with ZapmailClient(api_key=acc.api_key, workspace_key=acc.workspace_key) as z:
            a = acc.name
            await _check(results, "User + plan", a, None, z.get_user)
            await _check(results, "Wallet balance", a, None, z.get_wallet_balance)
            await _check(results, "Workspaces", a, None,
                         lambda: z._request("GET", "/v2/workspaces"))
            await _check(results, "Subscriptions", a, None,
                         lambda: z._request("GET", "/v2/subscriptions"))
            await _check(results, "Domain tags", a, None, z.list_tags)
            await _check(results, "Export accounts (Smartlead)", a, None,
                         lambda: z.list_third_party_accounts("SMARTLEAD"))
            await _check(results, "Export workspaces (Smartlead)", a, None,
                         lambda: z.fetch_export_workspaces("SMARTLEAD"),
                         note="docs say SMARTLEAD unsupported here")
            await _check(results, "Placement subscriptions", a, None, z.placement_subscriptions)
            await _check(results, "Placement credits", a, None, z.placement_credits)
            await _check(results, "Placement overall report", a, None,
                         z.placement_overall_report)
            row = await _check(results, "Placement orders", a, None,
                               lambda: z.placement_orders(page=1, limit=5))
            try:
                orders = (await z.placement_orders(page=1, limit=1) or {}).get("data") or {}
                lst = orders if isinstance(orders, list) else (
                    orders.get("orders") or orders.get("data") or [])
                if lst and isinstance(lst[0], dict):
                    oid = lst[0].get("cartOrderId") or lst[0].get("id")
                    if oid:
                        placement_order[a] = str(oid)
            except ZapmailError:
                pass
            if a in placement_order:
                await _check(results, "Placement report (latest order)", a, None,
                             lambda: z.placement_report(placement_order[a]))
            await _check(results, "DNS Shield slots", a, None, z.dns_shield_available_slots)
            row = await _check(results, "DNS Shield subscriptions", a, None,
                               z.dns_shield_subscriptions)
            await _check(results, "Prewarmed: available count", a, None, z.prewarmed_count)
            await _check(results, "Prewarmed: domains for sale", a, None,
                         lambda: z.prewarmed_domains(page=1, limit=5))
            await _check(results, "Prewarmed: our subscriptions", a, None,
                         z.prewarmed_subscriptions)
            await _check(results, "Aged domains marketplace", a, None,
                         lambda: z.aged_domains(page=1, limit=5))

        # ── provider-scoped ─────────────────────────────────────────────
        for p in PROVIDERS:
            async with ZapmailClient(api_key=acc.api_key, workspace_key=acc.workspace_key,
                                     service_provider=p) as z:
                a = acc.name
                await _check(results, "Domains list", a, p,
                             lambda: z.list_domains(page=1, limit=5))
                await _check(results, "Domains list (filter ACTIVE)", a, p,
                             lambda: z.list_domains(status=["ACTIVE"]))
                await _check(results, "Assignable domains", a, p,
                             lambda: z.list_assignable_domains(page=1, limit=5))
                await _check(results, "Mailboxes list", a, p,
                             lambda: z.list_mailboxes(page=1, limit=5))
                await _check(results, "Connection requests", a, p,
                             lambda: z.list_connection_requests(page=1, limit=5),
                             note="docs: needs x-workspace-key")
                row = await _check(results, "Renewal soon (<=2 months)", a, p,
                                   lambda: z.list_renewal_soon(page=1, limit=200))
                try:
                    soon = ((await z.list_renewal_soon(page=1, limit=200)) or {}
                            ).get("data") or {}
                    renewal_ids[(a, p)] = [str(d["id"]) for d in soon.get("domains") or []
                                           if d.get("id")][:5]
                    doms = ((await z.list_domains(page=1, limit=1)) or {}
                            ).get("data") or {}
                    if doms.get("domains"):
                        first_domain_id[(a, p)] = str(doms["domains"][0]["id"])
                except ZapmailError:
                    pass
                if renewal_ids.get((a, p)):
                    await _check(results, "Renewal price", a, p,
                                 lambda: z.get_renewal_price(domain_ids=renewal_ids[(a, p)]))
                if first_domain_id.get((a, p)):
                    await _check(results, "DNS records (one domain)", a, p,
                                 lambda: z.get_dns_records(first_domain_id[(a, p)]))
                    await _check(results, "Domain health score (one domain)", a, p,
                                 lambda: z.get_domain_health(first_domain_id[(a, p)]))
                await _check(results, "Placement-eligible mailboxes", a, p,
                             lambda: z.placement_eligible_mailboxes(page=1, limit=5))
                await _check(results, "DNS Shield eligible domains", a, p,
                             lambda: z.dns_shield_eligible_domains(page=1, limit=5))

    # ── global (spends the domain-search budget: 3 requests) ─────────────
    if accounts:
        acc = accounts[0]
        async with ZapmailClient(api_key=acc.api_key) as z:
            await _check(results, "Availability, bulk (3 names)", "any", None,
                         lambda: z.check_availability_bulk(
                             ["google.com", "qzvxwmpldkrt7742.com", "streamintake.com"]),
                         note="1 of 10 searches / 30 min")
            await _check(results, "Availability, single name", "any", None,
                         lambda: z._request("POST", "/v2/domains/available",
                                            json={"domainName": "ledgerpipeline",
                                                  "tlds": ["com"], "years": 1}),
                         note="1 of 10 searches / 30 min")
        async with ZapmailClient(api_key=acc.api_key, service_provider="GOOGLE") as z:
            await _check(results, "AI Domain Finder", "any", "GOOGLE",
                         lambda: z.ai_domain_finder(["data", "ingest", "pipeline"],
                                                    ["com"], 10),
                         note="async ~15-20s; cached 5 min")
    return results


def to_markdown(results: list[dict]) -> str:
    ok = sum(1 for r in results if r["ok"])
    lines = [
        f"# Zapmail live self-test — {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
        "",
        f"{ok}/{len(results)} checks OK. Read-only; spend switch forced off.",
        "",
        "| Feature | Account | Provider | OK | ms | Result |",
        "|---|---|---|---|---|---|",
    ]
    for r in results:
        res = r["summary"].replace("|", "/")
        if r.get("note"):
            res += f" _({r['note']})_"
        lines.append(f"| {r['feature']} | {r['account']} | {r['provider']} | "
                     f"{'✅' if r['ok'] else '❌'} | {r['ms']} | {res} |")
    return "\n".join(lines) + "\n"


async def main() -> int:
    ap = argparse.ArgumentParser(description="Live self-test of free Zapmail features")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--md", help="Also write a markdown report to this path")
    args = ap.parse_args()
    results = await run()
    if args.json:
        print(json.dumps(results))
    else:
        for r in results:
            print(f"  {'OK ' if r['ok'] else 'ERR'} {r['feature']:34} {r['account']:14} "
                  f"{r['provider']:9} {r['ms']:>6}ms  {r['summary'][:110]}")
        print(f"\n  {sum(r['ok'] for r in results)}/{len(results)} OK")
    if args.md:
        with open(args.md, "w", encoding="utf-8") as fh:
            fh.write(to_markdown(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
