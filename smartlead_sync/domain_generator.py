#!/usr/bin/env python3
"""Cold-email domain generator. Read-only; buys nothing.

Generates candidate sending domains for a client, screens them against the
2026 naming rules (no brand permutations, no phishing shapes, .com only),
checks availability + price on Zapmail, checks DNSBL history so we never
register a domain someone else already burned, and prints a staggered
purchase plan across registrars.

The plan is the output. Purchasing stays manual by design: a script that both
picks and buys domains turns a naming mistake into a billing mistake.

Usage:
    python3 domain_generator.py --client "Better Data" \
        --main-domain betterdata.com \
        --value data,ingest,clarity,coherence \
        --problem signal,coverage,accuracy \
        --industry pipeline,revenue,warehouse \
        --need 10

    python3 domain_generator.py --client X --main-domain x.com \
        --value a,b --need 5 --no-network     # naming rules only, no API calls
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead.domain_availability import (
    DEFAULT_PRICE_CEILING_USD, RATE_LIMIT_CALLS, RATE_LIMIT_WINDOW_S,
    cache_status, enrich, zapmail_key, zones_checked,
)
from smartlead.domain_estate import owned_domain_list
from smartlead.domain_naming import (
    Candidate, ClientVocabulary, generate_with_rejects, owned_stems_from,
    purchase_schedule,
)

# Spread purchases across accounts you actually hold. Registrar diversity is
# half the point; the other half is the day gap in purchase_schedule().
#
# NOTE: buying every domain inside Zapmail is operationally simplest but puts
# them all behind one registrar, which is exactly the correlation the spread
# rule exists to break. Zapmail-only is listed first because it is what the
# current key supports; override with --registrars once separate registrar
# accounts exist.
DEFAULT_REGISTRARS = ("Zapmail",)


def _split(raw: str | None) -> list[str]:
    return [t.strip() for t in (raw or "").split(",") if t.strip()]


def _print_candidates(cands: list[Candidate], show_rejects: bool) -> None:
    passing = [c for c in cands if c.ok]
    rejected = [c for c in cands if not c.ok]

    print(f"\n{'=' * 72}")
    print(f"  Candidates: {len(passing)} passing / {len(cands)} generated")
    print(f"{'=' * 72}")
    if passing:
        print(f"  {'DOMAIN':30} {'AVAIL':7} {'PRICE':>9}  {'SIM':>5}  BUILT FROM")
        for c in passing:
            avail = {True: "yes", False: "TAKEN", None: "?"}[c.available]
            price = f"${c.price_usd:.2f}" if c.price_usd is not None else "-"
            print(f"  {c.domain:30} {avail:7} {price:>9}  {c.similarity_to_main:>5.2f}  "
                  f"{'+'.join(c.source_tokens)}")
    else:
        print("  (none passed — widen the vocabulary or relax the price ceiling)")

    if show_rejects and rejected:
        print(f"\n  Rejected ({len(rejected)}):")
        for c in rejected[:40]:
            print(f"    {c.domain:30} {'; '.join(c.rejections)}")
        if len(rejected) > 40:
            print(f"    ... and {len(rejected) - 40} more")


def _print_plan(purchasable: list[Candidate], registrars: list[str],
                per_batch: int, day_gap: int) -> None:
    print(f"\n{'=' * 72}")
    print("  PURCHASE PLAN")
    print(f"{'=' * 72}")
    if not purchasable:
        print("  Nothing purchasable. Re-run with more vocabulary tokens.")
        return

    batches = purchase_schedule(
        [c.domain for c in purchasable], registrars,
        per_batch=per_batch, day_gap=day_gap,
    )
    total = sum(c.price_usd or 0.0 for c in purchasable)
    for b in batches:
        when = "today" if b.day_offset == 0 else f"day +{b.day_offset}"
        print(f"  {when:>8}  {b.registrar:12} {', '.join(b.domains)}")
    print(f"\n  {len(purchasable)} domains across {len({b.registrar for b in batches})} "
          f"registrars over {max(b.day_offset for b in batches)} days. "
          f"Est. ${total:.2f}/yr.")
    print("  Availability/price are INFERRED from Zapmail's suggestion response "
          "(it does not answer for the exact name). Confirm in the Zapmail UI "
          "before buying.")
    bought_elsewhere = {b.registrar for b in batches} - {"Zapmail"}
    if bought_elsewhere:
        print(f"\n  Bought outside Zapmail ({', '.join(sorted(bought_elsewhere))}): "
              "point NS at Zapmail and wait 15-20 min for DNS before connecting.")
    print("  Then: connect on Zapmail, create 2 inboxes/domain, and WARM 2-3 WEEKS "
          "before the first cold send.")


def _emit_json(cands: list[Candidate], purchasable: list[Candidate],
               vocab: ClientVocabulary, registrars: list[str],
               per_batch: int, day_gap: int, checked: bool) -> None:
    """Machine-readable result for the Slack bot.

    Printed to stdout as a single line so the Node side can parse the last
    stdout line without worrying about interleaved progress logging (which
    goes to stderr).
    """
    batches = (purchase_schedule([c.domain for c in purchasable], registrars,
                                 per_batch=per_batch, day_gap=day_gap)
               if purchasable else [])
    payload = {
        "client": vocab.name,
        "main_domain": vocab.main_domain,
        "vocabulary": vocab.token_bank(),
        "brand_fragments": vocab.brand_fragment_tokens(),
        "availability_checked": checked,
        "generated": len(cands),
        "passing": [
            {
                "domain": c.domain,
                "available": c.available,
                "price": c.price_usd,
                "similarity": c.similarity_to_main,
                "built_from": list(c.source_tokens),
                "blacklisted_on": list(c.blacklisted_on),
            }
            for c in cands if c.ok
        ],
        "rejected": [
            {"domain": c.domain, "reasons": list(c.rejections)}
            for c in cands if not c.ok
        ],
        "purchasable": [
            {"domain": c.domain, "price": c.price_usd} for c in purchasable
        ],
        "plan": [
            {"day_offset": b.day_offset, "registrar": b.registrar,
             "domains": list(b.domains)}
            for b in batches
        ],
        "estimated_annual_usd": round(
            sum(c.price_usd or 0.0 for c in purchasable), 2),
    }
    print(json.dumps(payload))


async def main() -> int:
    ap = argparse.ArgumentParser(description="Generate + screen cold email domains")
    ap.add_argument("--client", required=True, help="Client name (labels only)")
    ap.add_argument("--main-domain", required=True,
                    help="Client's real domain — used ONLY to reject lookalikes")
    ap.add_argument("--value", help="Comma-separated value/product nouns")
    ap.add_argument("--problem", help="Comma-separated problem-statement nouns")
    ap.add_argument("--industry", help="Comma-separated industry nouns")
    ap.add_argument("--need", type=int, default=10, help="How many domains to buy")
    ap.add_argument("--price-ceiling", type=float, default=DEFAULT_PRICE_CEILING_USD)
    ap.add_argument("--registrars", default=",".join(DEFAULT_REGISTRARS))
    ap.add_argument("--per-batch", type=int, default=3,
                    help="Domains bought per registrar per day")
    ap.add_argument("--day-gap", type=int, default=2, help="Days between batches")
    ap.add_argument("--no-network", action="store_true",
                    help="Naming rules only — no Zapmail, no DNSBL lookups")
    ap.add_argument("--max-calls", type=int, default=RATE_LIMIT_CALLS,
                    help=f"Zapmail searches to spend this run (limit: "
                         f"{RATE_LIMIT_CALLS} per {RATE_LIMIT_WINDOW_S // 60} min)")
    ap.add_argument("--skip-blacklist", action="store_true")
    ap.add_argument("--show-rejects", action="store_true")
    ap.add_argument("--exclude", default="",
                    help="Comma-separated domains we already own that are not "
                         "yet in Smartlead (merged with the auto-fetched list)")
    ap.add_argument("--no-estate", action="store_true",
                    help="Skip the Smartlead owned-domain lookup (offline use)")
    ap.add_argument("--json", action="store_true",
                    help="Emit one JSON line on stdout (progress goes to stderr)")
    args = ap.parse_args()

    # In JSON mode stdout must carry ONLY the payload, so every human-facing
    # line is routed to stderr instead of being suppressed — the bot still
    # gets the logs, just not on the parsed channel.
    log = (lambda *a, **k: print(*a, file=sys.stderr, **k)) if args.json else print

    vocab = ClientVocabulary(
        name=args.client,
        main_domain=args.main_domain,
        value_nouns=_split(args.value),
        problem_nouns=_split(args.problem),
        industry_nouns=_split(args.industry),
    )
    if len(vocab.token_bank()) < 3:
        print("ERROR: supply at least 3 tokens across --value/--problem/--industry.",
              file=sys.stderr)
        return 2

    log(f"[Domains] {args.client} — main domain {args.main_domain} "
          f"(candidates must NOT resemble it)")
    log(f"[Domains] vocabulary: {', '.join(vocab.token_bank())}")

    fragments = vocab.brand_fragment_tokens()
    if fragments:
        log(f"[Domains] ⚠ these tokens are pieces of '{vocab.main_stem()}' and will "
            f"build nothing: {', '.join(fragments)}")
        log("[Domains]   Add vocabulary that describes the PROBLEM or the "
            "OUTCOME instead of the brand name.")

    # Domains we already own must never be suggested again, for any client.
    owned: list[str] = []
    estate_ok = True
    if args.no_estate:
        owned = _split(args.exclude)
        estate_ok = False
    else:
        owned, estate_ok = await owned_domain_list(_split(args.exclude))
    if owned:
        log(f"[Domains] excluding {len(owned)} domains already in the estate"
            + ("" if estate_ok else " (Smartlead lookup incomplete — see warnings)"))
    owned_stems = owned_stems_from(owned)

    # Generate a surplus: availability kills most real-word .com names.
    cands = generate_with_rejects(vocab, limit=max(args.need * 6, 40),
                                  owned_stems=owned_stems)

    if not args.no_network:
        cached, fresh = cache_status([c.domain for c in cands if c.ok])
        log(f"[Domains] {len(cached)} cached, {len(fresh)} need a Zapmail search "
            f"(budget {args.max_calls} per {RATE_LIMIT_WINDOW_S // 60} min)")
        log(f"[Domains] blacklist history via {zones_checked()} (free, DNS only)")
        cands = await enrich(cands, price_ceiling=args.price_ceiling,
                             skip_blacklist=args.skip_blacklist,
                             max_calls=args.max_calls)

    purchasable = ([] if args.no_network
                   else [c for c in cands if c.purchasable][:args.need])

    # JSON mode short-circuits every human-facing print: stdout must carry the
    # payload and nothing else.
    if args.json:
        _emit_json(cands, purchasable, vocab, _split(args.registrars),
                   args.per_batch, args.day_gap, checked=not args.no_network)
        return 0

    _print_candidates(cands, args.show_rejects)

    if args.no_network:
        print("\n  --no-network: availability unchecked, no purchase plan produced.")
        return 0

    if len(purchasable) < args.need:
        # Distinguish "we checked and they're taken" from "we never checked".
        unknown = sum(1 for c in cands if c.ok and c.available is None)
        if unknown and not zapmail_key():
            print(f"\n  ⚠ Availability was never checked — ZAPMAIL_API_KEY is not set, "
                  f"so all {unknown} passing names are unverified.")
            print("    Set ZAPMAIL_API_KEY (Zapmail > Settings > API; API access "
                  "requires the Pro plan) and re-run to get a real purchase plan.")
        elif unknown:
            print(f"\n  ⚠ {unknown} names still unchecked — the "
                  f"{RATE_LIMIT_CALLS}-search/{RATE_LIMIT_WINDOW_S // 60}min rate "
                  "limit was reached. Re-run later; cached results carry over.")
        else:
            print(f"\n  ⚠ Only {len(purchasable)} of {args.need} requested domains are "
                  "purchasable. Add vocabulary tokens and re-run.")
    _print_plan(purchasable, _split(args.registrars), args.per_batch, args.day_gap)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
