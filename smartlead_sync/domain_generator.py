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
    DEFAULT_PRICE_CEILING_USD, DEFAULT_RUN_CALLS, RATE_LIMIT_CALLS,
    RATE_LIMIT_WINDOW_S, cache_status, enrich, zapmail_key, zones_checked,
)
from smartlead.domain_estate import owned_domain_list
from smartlead.domain_naming import (
    Candidate, ClientVocabulary, diversify, generate_with_rejects,
    owned_stems_from, purchase_schedule,
)
from smartlead.zapmail_accounts import api_key_for_client, account_name_for_client

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


async def _ai_suggestions(vocab, owned_stems, client=None) -> list[dict]:
    """AI Domain Finder results, screened + availability-checked. Read-only.

    Late-imported so the generator's normal path stays cheap, and kept separate
    from the token-compound list: a model-sourced name obeys the same naming
    rules (no brand permutation, no phishing shape), but reports a distinct
    ``ai_suggestions`` block rather than being mixed into the shortlist.
    """
    from smartlead.domain_ai import ai_suggest
    from smartlead.domain_availability import check_availability_bulk
    from smartlead.domain_naming import screen
    from smartlead.zapmail_accounts import api_key_for_client

    # The AI finder works best on a few specific words; the whole token bank
    # (often 20+ scraped words) dilutes it.
    rows = await ai_suggest(vocab.token_bank()[:6], client=client)
    if not rows:
        return []

    passing: list[dict] = []
    for r in rows:
        sld, _, tld = r["domain"].partition(".")
        if not sld or not tld:
            continue
        c = screen(sld, f".{tld}", vocab, source_tokens=("ai",),
                   owned_stems=owned_stems)
        if c.ok:
            passing.append(r)
    if not passing:
        return []

    # The finder already answers availability + price; only re-check rows it
    # left unknown, so the 10-search budget isn't spent twice.
    unknown = [r["domain"] for r in passing if r["available"] is None]
    if unknown:
        avail = await check_availability_bulk(unknown, api_key=api_key_for_client(client))
        for r in passing:
            if r["domain"] in avail:
                r["available"], r["price"] = avail[r["domain"]]
    return [{"domain": r["domain"], "available": r["available"], "price": r["price"]}
            for r in passing]


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
               per_batch: int, day_gap: int, checked: bool,
               estate_counts: dict[str, int] | None = None,
               estate_ok: bool = True,
               ai_suggestions: list[dict] | None = None) -> None:
    """Machine-readable result for the Slack bot.

    Printed to stdout as a single line so the Node side can parse the last
    stdout line without worrying about interleaved progress logging (which
    goes to stderr).
    """
    batches = (purchase_schedule([c.domain for c in purchasable], registrars,
                                 per_batch=per_batch, day_gap=day_gap)
               if purchasable else [])
    estate_counts = estate_counts or {}
    payload = {
        "client": vocab.name,
        "main_domain": vocab.main_domain,
        "vocabulary": vocab.token_bank(),
        "brand_fragments": vocab.brand_fragment_tokens(),
        "availability_checked": checked,
        "estate": estate_counts,
        "estate_complete": estate_ok,
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
        "ai_suggestions": ai_suggestions or [],
        "estimated_annual_usd": round(
            sum(c.price_usd or 0.0 for c in purchasable), 2),
    }
    print(json.dumps(payload))


def _run_estate_subcommand(argv: list[str]) -> int:
    """`--register` / `--list-owned`: manage the owned-domain list.

    Handled before the main parser because they take no client or vocabulary
    arguments — they are estate bookkeeping, not generation.
    """
    from smartlead.domain_estate import (
        read_asset_tracker, read_registered_domains, read_seed_file,
        register_domains,
    )
    as_json = "--json" in argv

    if "--list-owned" in argv:
        tracker, tracker_ok = read_asset_tracker()
        seed = read_seed_file()
        recorded = read_registered_domains()
        total = len(set(tracker) | set(seed) | set(recorded))
        if as_json:
            print(json.dumps({"asset_tracker": tracker,
                              "asset_tracker_ok": tracker_ok,
                              "seed_file": seed, "registered": recorded,
                              "total": total}))
        else:
            print(f"Asset tracker ({len(tracker)})"
                  + ("" if tracker_ok else "  [READ FAILED]"))
            print(f"Seed file ({len(seed)}): {', '.join(seed) or '(none)'}")
            print(f"Added ad hoc ({len(recorded)}): "
                  f"{', '.join(recorded) or '(none)'}")
            print(f"Total unique: {total}")
        return 0

    i = argv.index("--register")
    if i + 1 >= len(argv):
        print("ERROR: --register needs a comma-separated domain list",
              file=sys.stderr)
        return 2

    added_by = ""
    if "--added-by" in argv:
        j = argv.index("--added-by")
        if j + 1 < len(argv):
            added_by = argv[j + 1]

    domains = [d for d in argv[i + 1].split(",") if d.strip()]
    try:
        saved, bad = register_domains(domains, added_by=added_by)
    except RuntimeError as exc:
        if as_json:
            print(json.dumps({"error": str(exc)}))
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if as_json:
        print(json.dumps({"saved": saved, "rejected": bad}))
    else:
        if saved:
            print(f"Recorded {len(saved)} domain(s): {', '.join(saved)}")
        if bad:
            print(f"Skipped {len(bad)} unusable entr(ies): {', '.join(bad)}")
    return 0



async def _run_buy_subcommand(argv: list[str]) -> int:
    """`--buy a.com,b.com [--client X] [--json]` — STAGE ONLY, never buys.

    This is the entry point the Slack `/domains buy` command spawns, so it has
    no execute path at all: it stages a staggered, ledgered plan (availability
    + price + batch ids) via :func:`domain_batch.stage_batches`. Buying happens
    only through ``zapmail_buy.py --execute <batch_id> --approve`` with the
    ``ZAPMAIL_ALLOW_SPEND`` kill-switch on. ``--approve`` here is refused
    rather than ignored, so nobody mistakes a stage for a buy.
    """
    from smartlead.domain_batch import stage_batches

    as_json = "--json" in argv

    def _fail(msg: str, code: int = 2) -> int:
        # stderr always (the Slack side surfaces its tail on a non-zero exit);
        # JSON on stdout too for machine callers.
        print(f"ERROR: {msg}", file=sys.stderr)
        if as_json:
            print(json.dumps({"error": msg}))
        return code

    if "--approve" in argv:
        return _fail("--buy only stages. To buy, use: zapmail_buy.py --execute "
                     "<batch_id> --approve (with ZAPMAIL_ALLOW_SPEND=true).")

    i = argv.index("--buy")
    if i + 1 >= len(argv) or argv[i + 1].startswith("-"):
        return _fail("--buy needs a comma-separated domain list")
    domains = [d.strip().lower() for d in argv[i + 1].split(",") if d.strip()]
    if not domains:
        return _fail("--buy needs at least one domain")

    client = ""
    if "--client" in argv:
        j = argv.index("--client")
        if j + 1 >= len(argv) or argv[j + 1].startswith("-"):
            return _fail("--client needs a client name")
        client = argv[j + 1]

    plan = await stage_batches(domains, client=client)
    if as_json:
        print(json.dumps(plan, default=str))
        return 0
    print("\n  Purchase plan (stage only — nothing bought):")
    for b in plan["batches"]:
        print(f"    {b['earliest_date']:12} ${b['estimated_usd']:>7.2f}  "
              f"{', '.join(b['domains'])}   [{b['batch_id']}]")
    for label, key in (("taken", "unavailable"), ("unverified", "unknown"),
                       ("over price ceiling", "over_ceiling"),
                       ("already in ledger", "already_planned")):
        if plan[key]:
            print(f"  ⚠ {label}: {', '.join(plan[key])}")
    print(f"\n  est ${plan['total_usd']}/yr · bills: "
          f"{plan['spend_account'] or 'NO ACCOUNT (buying refused)'}")
    print("  To buy: zapmail_buy.py --execute <batch_id> --approve "
          "(ZAPMAIL_ALLOW_SPEND=true)")
    return 0


async def main() -> int:
    # Estate bookkeeping short-circuits the generator's own argument contract.
    if "--register" in sys.argv or "--list-owned" in sys.argv:
        return _run_estate_subcommand(sys.argv[1:])
    # Purchase is a separate concern from generation. --buy only STAGES a
    # ledgered plan; buying lives in zapmail_buy.py --execute.
    if "--buy" in sys.argv:
        return await _run_buy_subcommand(sys.argv[1:])

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
    ap.add_argument("--max-calls", type=int, default=DEFAULT_RUN_CALLS,
                    help=f"Zapmail searches to spend this run (limit: "
                         f"{RATE_LIMIT_CALLS} per {RATE_LIMIT_WINDOW_S // 60} min)")
    ap.add_argument("--skip-blacklist", action="store_true")
    ap.add_argument("--auto-vocab", action="store_true",
                    help="Scrape the main domain's site to seed the vocabulary")
    ap.add_argument("--ai", action="store_true",
                    help="Also pull Zapmail AI Domain Finder suggestions")
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

    if args.auto_vocab:
        from smartlead.domain_ai import source_vocabulary
        scraped = await source_vocabulary(args.main_domain)
        if scraped:
            vocab.value_nouns.extend(scraped)
            log(f"[Domains] auto-vocab from {args.main_domain}: "
                f"{', '.join(scraped[:12])}"
                + ("…" if len(scraped) > 12 else ""))
        else:
            log(f"[Domains] auto-vocab: could not read {args.main_domain}")

    if len(vocab.token_bank()) < 3:
        print("ERROR: supply at least 3 tokens across --value/--problem/--industry "
              "(or --auto-vocab to scrape them from the site).",
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
    estate_counts: dict[str, int] = {}
    if args.no_estate:
        owned = _split(args.exclude)
        estate_ok = False
    else:
        owned, estate_ok, estate_counts = await owned_domain_list(_split(args.exclude))
    if owned:
        detail = ", ".join(f"{k}={v}" for k, v in estate_counts.items() if v)
        log(f"[Domains] excluding {len(owned)} domains already owned"
            + (f" ({detail})" if detail else "")
            + ("" if estate_ok else " — WARNING: a source failed, dedupe is incomplete"))
    elif not args.no_estate:
        log("[Domains] ⚠ no owned domains found — dedupe is not protecting this run")
    owned_stems = owned_stems_from(owned)

    # Generate a surplus: availability kills most real-word .com names.
    cands = generate_with_rejects(vocab, limit=max(args.need * 6, 40),
                                  owned_stems=owned_stems)

    if not args.no_network:
        cached, fresh = cache_status([c.domain for c in cands if c.ok])
        log(f"[Domains] {len(cached)} cached, {len(fresh)} need a Zapmail search "
            f"(budget {args.max_calls} per {RATE_LIMIT_WINDOW_S // 60} min)")
        log(f"[Domains] blacklist history via {zones_checked()} (free, DNS only)")
        zkey = api_key_for_client(args.client)
        zname = account_name_for_client(args.client) or "default"
        log(f"[Domains] Zapmail account for {args.client}: {zname}")
        cands = await enrich(cands, price_ceiling=args.price_ceiling,
                             skip_blacklist=args.skip_blacklist,
                             max_calls=args.max_calls,
                             api_key=zkey)

    # Diversify AFTER availability: most generated names are taken, so a
    # shortlist chosen before the availability check gets re-expanded into
    # near-duplicates by whatever happens to survive. Applying it here is what
    # stops eight names all hanging off one word (Bettrdata, 2026-08-21).
    purchasable = ([] if args.no_network
                   else [c for c in diversify(
                             [c for c in cands if c.purchasable], args.need)
                         if c.purchasable][:args.need])

    ai_suggestions: list[dict] = []
    if args.ai and not args.no_network:
        try:
            ai_suggestions = await _ai_suggestions(vocab, owned_stems, client=args.client)
        except Exception as exc:  # noqa: BLE001
            log(f"[Domains] AI finder failed: {exc}")

    # JSON mode short-circuits every human-facing print: stdout must carry the
    # payload and nothing else.
    if args.json:
        _emit_json(cands, purchasable, vocab, _split(args.registrars),
                   args.per_batch, args.day_gap, checked=not args.no_network,
                   estate_counts=estate_counts, estate_ok=estate_ok,
                   ai_suggestions=ai_suggestions)
        return 0

    _print_candidates(cands, args.show_rejects)

    if ai_suggestions:
        print(f"\n  AI suggestions (Zapmail AI Domain Finder, {len(ai_suggestions)}):")
        for s in ai_suggestions:
            avail = {True: "yes", False: "TAKEN", None: "?"}[s["available"]]
            price = f"${s['price']:.2f}" if s["price"] else "-"
            print(f"    {s['domain']:30} {avail:7} {price:>9}")

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
