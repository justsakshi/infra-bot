"""One-click domain suggestions for a client: Zapmail AI Domain Finder + ours.

Backs Slack's "Suggest domains for <client>" button. Reads the client's
profile (``domain_clients.json``: real site + the business's words by role)
and merges two sources into one short list of names that are available to
buy right now:

  1. **Zapmail AI Domain Finder** — seeded with the profile's plain-language
     ``ai_seeds`` (three prompts in parallel, ~20s).
  2. **Our generator** (``domain_quality.build_names``) — "<describer> +
     <noun>" names from the business's own words (``postalworks``,
     ``clearrecords``), checked with DNS + Zapmail bulk availability +
     blacklist history.

Every name, from either source, must pass the SAFETY rules (``domain_naming``:
no brand lookalike, no phishing shape), the QUALITY rules
(``domain_quality``: familiar words only, about the business, ends in a
noun), must not already be owned or staged, and must be available,
non-premium, at or under the price ceiling, and clean on the blacklists.
Survivors are ranked by quality score. READ-ONLY: nothing is bought or
reserved here; buying goes through the staged batch plan.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from smartlead.domain_ai import ai_suggest
from smartlead.domain_availability import (
    DEFAULT_PRICE_CEILING_USD, check_blacklists, enrich,
)
from smartlead.domain_estate import owned_domain_list
from smartlead.domain_naming import (
    ClientVocabulary, diversify, owned_stems_from, screen,
)
from smartlead.domain_quality import BusinessWords, build_names, judge
from smartlead.zapmail_accounts import api_key_for_client

PROFILES_PATH = Path(os.getenv(
    "DOMAIN_CLIENTS_FILE",
    Path(__file__).resolve().parent.parent / "domain_clients.json"))

# Generator availability checks per click (each covers up to 20 names).
# Zapmail's bucket is 100 per 30 min per key (support, 2026-09-29), so 10 per
# click leaves room for several clicks; the AI finder doesn't use this bucket.
GENERATOR_MAX_CALLS = 10
# Best-scoring generated names handed to the availability check. DNS (free)
# drops the registered ones first; the rest share the 10-call budget above.
GENERATOR_MAX_NAMES = 600


def load_profiles() -> dict[str, dict]:
    """``{client_key: {label, main_domain, keywords}}`` from the profiles file."""
    with open(PROFILES_PATH, encoding="utf-8") as fh:
        return json.load(fh).get("clients") or {}


def brand_of(prof: dict) -> str:
    """What names must never look like: ``brand_stem`` when given, else the site.

    The stem wins because a site can carry a prefix the brand does not:
    Melior's site is getmelior.com, and screening against "getmelior" would
    let "melioragency" through.
    """
    return (prof.get("brand_stem") or prof.get("main_domain") or "").strip().lower()


def site_of(prof: dict) -> str:
    """The client's website for display (falls back to the brand word)."""
    return (prof.get("main_domain") or prof.get("brand_stem") or "").strip().lower()


def profile_for(client: str) -> tuple[str, dict]:
    """``(client_key, profile)``; raises ValueError when missing or incomplete."""
    want = client.strip().lower().replace("_", " ")
    for key, prof in load_profiles().items():
        if key.lower() == want or str(prof.get("label", "")).lower() == want:
            if not brand_of(prof) or len(prof.get("keywords") or []) < 3:
                raise ValueError(
                    f"{prof.get('label') or key} needs a main_domain (or brand_stem) and at "
                    f"least 3 keywords in {PROFILES_PATH.name} before it can get suggestions.")
            return key, prof
    raise ValueError(f"no domain profile for {client!r} in {PROFILES_PATH.name}.")


def ai_seeds(prof: dict) -> list[list[str]]:
    """The AI finder's prompts: the profile's ``ai_seeds``, else keyword windows."""
    seeds = [[w.strip().lower() for w in s if w.strip()] for s in prof.get("ai_seeds") or []]
    seeds = [s for s in seeds if s][:3]
    return seeds or keyword_windows(prof.get("keywords") or [])


def keyword_windows(keywords: list[str], size: int = 3, max_windows: int = 2) -> list[list[str]]:
    """Few-word seeds for the AI finder, which does best on 3 precise words."""
    kws = [k.strip().lower() for k in keywords if k.strip()]
    windows = [kws[i:i + size] for i in range(0, len(kws), size)]
    return [w for w in windows if w][:max_windows]


def pick_ai_rows(rows: list[dict], vocab: ClientVocabulary, *, owned: set[str],
                 owned_stems: frozenset[str], price_ceiling: float) -> list[dict]:
    """Keep AI rows that are buyable and pass our naming rules. Pure."""
    out: list[dict] = []
    seen: set[str] = set()
    for r in rows:
        d = r["domain"]
        if d in seen or d in owned:
            continue
        seen.add(d)
        if r.get("available") is not True or r.get("premium"):
            continue
        if r.get("price") is None or r["price"] > price_ceiling:
            continue
        sld, _, tld = d.partition(".")
        if not screen(sld, f".{tld}", vocab, source_tokens=("ai",),
                      owned_stems=owned_stems).ok:
            continue
        out.append({"domain": d, "price": r["price"],
                    "renew_price": r.get("renew_price"), "source": "ai"})
    return out


def merge(ai: list[dict], gen: list[dict], count: int) -> list[dict]:
    """AI names first (they read best), then ours; unique, capped at count.

    Up to a third of the slots are kept for our generator when it has names,
    so the list always shows both sources; unused slots fall back to AI.
    """
    gen_slots = min(len(gen), count // 3)
    head = ai[:count - gen_slots]
    out: list[dict] = []
    seen: set[str] = set()
    for row in [*head, *gen, *ai[len(head):]]:
        if row["domain"] in seen:
            continue
        seen.add(row["domain"])
        out.append(row)
        if len(out) >= count:
            break
    return out


def _held_in_plans(errors: list[str]) -> set[str]:
    """Domains in a planned / in-flight / bought batch of the purchase ledger."""
    try:
        from smartlead.domain_batch import BatchStore
        store = BatchStore()
        if not store.available:
            errors.append("purchase ledger unreachable: names already staged may reappear")
            return set()
        return store.held_domains()
    except Exception as exc:  # noqa: BLE001 - suggestions still work without it
        errors.append(f"purchase ledger: {str(exc)[:150]}")
        return set()


async def suggest(
    client: str,
    *,
    count: int = 10,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    use_generator: bool = True,
) -> dict:
    """Up to ``count`` buyable names for ``client``. Read-only."""
    key, prof = profile_for(client)
    keywords = [k.strip().lower() for k in prof["keywords"] if k.strip()]
    words = BusinessWords.from_profile(prof)
    # Every business word goes into the safety vocabulary too, so a word that
    # is a fragment of the client's brand is caught whichever role it plays.
    vocab = ClientVocabulary(name=prof.get("label") or key,
                             main_domain=brand_of(prof),
                             value_nouns=sorted(words.on_topic | set(keywords)))
    errors: list[str] = []

    owned_list, estate_ok, estate_counts = await owned_domain_list()
    owned = set(owned_list)
    owned_stems = owned_stems_from(owned_list)
    # Names already staged in a purchase plan are spoken for: suggesting them
    # again invites a second plan for the same domain.
    held = _held_in_plans(errors)
    owned |= held

    # 1. Zapmail AI, one run per seed, in parallel.
    runs = await asyncio.gather(
        *(ai_suggest(s, desired_count=12, client=key) for s in ai_seeds(prof)),
        return_exceptions=True)
    ai_rows: list[dict] = []
    for r in runs:
        if isinstance(r, Exception):
            errors.append(f"AI finder: {str(r)[:150]}")
        else:
            ai_rows.extend(r)
    ai = pick_ai_rows(ai_rows, vocab, owned=owned, owned_stems=owned_stems,
                      price_ceiling=price_ceiling)
    # Safe is not the same as good: drop names that do not read as a company
    # in this business, and say which and why so the filter can be checked.
    dropped: list[dict] = []
    kept: list[dict] = []
    for r in ai:
        v = judge(r["domain"].split(".")[0], words)
        if v.ok:
            kept.append({**r, "score": v.score, "words": list(v.words)})
        else:
            dropped.append({"domain": r["domain"], "reason": v.reason})
    ai = sorted(kept, key=lambda r: -r["score"])
    if ai:
        listed = await check_blacklists([r["domain"] for r in ai])
        ai = [r for r in ai if not listed.get(r["domain"])]

    # 2. Our generator: business words by role, best-scoring names checked first.
    gen: list[dict] = []
    if use_generator:
        built = build_names(words)
        scores = {sld: v for sld, _, v in built}
        cands = [screen(sld, ".com", vocab, source_tokens=parts, owned_stems=owned_stems)
                 for sld, parts, _ in built]
        cands = [c for c in cands if c.ok and c.domain not in owned][:GENERATOR_MAX_NAMES]
        cands = await enrich(cands, price_ceiling=price_ceiling,
                             max_calls=GENERATOR_MAX_CALLS,
                             api_key=api_key_for_client(key))
        best = [c for c in diversify([c for c in cands if c.purchasable], count)
                if c.purchasable][:count]
        gen = [{"domain": c.domain, "price": c.price_usd, "renew_price": None,
                "source": "generator", "built_from": list(c.source_tokens),
                "score": scores[c.sld].score, "words": list(scores[c.sld].words)}
               for c in best]
        gen.sort(key=lambda r: -r["score"])

    return {
        "client": key,
        "label": prof.get("label") or key,
        "main_domain": site_of(prof),
        "keywords": keywords,
        "suggestions": merge(ai, gen, count),
        "ai_found": len(ai_rows),
        "ai_usable": len(ai),
        "ai_dropped": dropped,
        "generator_usable": len(gen),
        "estate_ok": estate_ok,
        "estate_counts": estate_counts,
        "held_in_plans": len(held),
        "price_ceiling": price_ceiling,
        "errors": errors,
    }
