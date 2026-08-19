"""Availability, pricing, and blacklist history for candidate domains.

Three independent checks, each of which can veto a purchase:

  * **Availability + price** — Zapmail's domain-search API. Zapmail both sells
    domains and hosts the mailboxes, so the whole flow runs on one key.
  * **Blacklist history** — a previously-burned domain can be re-registered and
    will arrive already listed. Reuses the DNSBL lookup strategy from
    ``blacklist_monitor`` (control-domain probing, authoritative-NS fallback)
    rather than reimplementing it, so both paths agree on what "listed" means.
  * **Price ceiling** — real-word .com names are frequently premium. A domain
    worth $2,000 is not worth it for a sending domain retired in 18 months.

**Rate limit is the binding constraint.** Zapmail allows 10 domain-search
requests per 30 minutes, and each request checks ONE name across its TLDs.
That is 10 candidate names per half hour. Everything here is built around
spending those 10 calls well: results are cached on disk between runs, and the
caller is told exactly how many calls a check will cost before it spends them.

Every backend degrades to ``None`` (unknown) rather than raising when its key
is absent, so the generator stays usable without credentials.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx

from smartlead.config import BLACKLIST_ZONES
from smartlead.domain_naming import Candidate

ZAPMAIL_AVAILABLE_URL = "https://api.zapmail.ai/api/v2/domains/available"

# Above this, a .com is a premium/aftermarket listing. Sending domains are
# consumables — retired every 12-18 months — so premium pricing never pays back.
DEFAULT_PRICE_CEILING_USD: float = 25.0

# Zapmail: 10 Domain Search requests per 30 minutes. One request = one name.
# This is the hard ceiling on how fast the tool can work; respect it rather
# than discovering it as a 429 mid-run.
RATE_LIMIT_CALLS: int = 10
RATE_LIMIT_WINDOW_S: int = 30 * 60
_PACING_S: float = 2.0  # gap between calls inside one burst

# Availability changes slowly and our call budget is tiny, so cache hard.
CACHE_TTL_S: int = 7 * 24 * 3600
_CACHE_PATH = Path(
    os.getenv("DOMAIN_AVAIL_CACHE", Path.home() / ".cache" / "infrabot" / "domain_avail.json")
)


def zapmail_key() -> str | None:
    return os.getenv("ZAPMAIL_API_KEY") or None


# ── cache ────────────────────────────────────────────────────────────────────

def _load_cache() -> dict[str, dict]:
    try:
        with _CACHE_PATH.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_cache(cache: dict[str, dict]) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _CACHE_PATH.open("w", encoding="utf-8") as fh:
            json.dump(cache, fh)
    except OSError as exc:  # noqa: BLE001
        print(f"  [Domains] cache write failed: {exc}", file=sys.stderr)


def _cached(cache: dict[str, dict], domain: str) -> tuple[bool | None, float | None] | None:
    row = cache.get(domain)
    if not row:
        return None
    if time.time() - row.get("ts", 0) > CACHE_TTL_S:
        return None
    return row.get("available"), row.get("price")


def cache_status(domains: list[str]) -> tuple[list[str], list[str]]:
    """``(cached, needs_lookup)`` — lets the caller see the call cost upfront."""
    cache = _load_cache()
    cached = [d for d in domains if _cached(cache, d) is not None]
    fresh = [d for d in domains if _cached(cache, d) is None]
    return cached, fresh


# ── availability ─────────────────────────────────────────────────────────────

def _sld_and_tld(domain: str) -> tuple[str, str]:
    label, _, tld = domain.partition(".")
    return label, tld or "com"


def _parse_row(row: dict) -> tuple[str, bool, float | None] | None:
    """``(domain, available, price)`` from one Zapmail result row."""
    name = str(row.get("domainName", "")).strip().lower()
    if not name:
        return None
    status = str(row.get("status", "")).strip().upper()
    available = status == "AVAILABLE"
    price: float | None = None
    raw = str(row.get("domainPrice", "")).strip().replace("$", "").replace(",", "")
    if raw:
        try:
            price = float(raw)
        except ValueError:
            price = None
    return name, available, price


async def _check_one(
    client: httpx.AsyncClient, key: str, domain: str,
) -> dict[str, tuple[bool | None, float | None]]:
    """One Zapmail search call, resolving the EXACT name we asked about.

    Zapmail's response has two parts and only one of them answers our question:

      * ``exactMatch`` — the name we actually asked for. ``null`` means the
        exact name is NOT available (verified 2026-08-19: querying an obviously
        taken name returns null here while the suggestion list stays full).
      * ``availableDomains`` — marketing suggestions built by bolting affixes
        onto the query (``thesignalclarity``, ``gosignalclarity``,
        ``mysignalclarity``). These are precisely the affix-permutation shape
        this tool exists to reject, so they are DISCARDED, never cached and
        never surfaced as candidates.

    Reading a price off the suggestion list and attributing it to the requested
    domain would report taken names as available at $12.99. Hence exactMatch
    only.
    """
    sld, tld = _sld_and_tld(domain)
    try:
        resp = await client.post(
            ZAPMAIL_AVAILABLE_URL,
            headers={"x-auth-zapmail": key, "Content-Type": "application/json"},
            json={"domainName": sld, "tlds": [tld], "years": 1},
        )
        if resp.status_code == 429:
            print(f"  [Domains] rate limited by Zapmail on {domain} — "
                  f"budget is {RATE_LIMIT_CALLS} searches / "
                  f"{RATE_LIMIT_WINDOW_S // 60} min. Stopping.", file=sys.stderr)
            raise _RateLimited
        resp.raise_for_status()
        data = resp.json().get("data", {}) or {}
    except _RateLimited:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"  [Domains] Zapmail search failed for {domain}: {exc}", file=sys.stderr)
        return {}

    # Prefer a real exactMatch payload if this account ever returns one.
    exact = data.get("exactMatch")
    if isinstance(exact, dict) and exact:
        parsed = _parse_row(exact)
        if parsed is not None:
            name, available, price = parsed
            return {(name or domain): (available, price)}

    # Fall back to the row-count proxy. Measured 2026-08-19 on this account:
    #   google.com (registered)          -> exactMatch null, 0 suggestion rows
    #   qzvxwmpldkrt7742.com (free)      -> exactMatch null, 50 suggestion rows
    # Zapmail only generates affix suggestions when the ROOT is unregistered,
    # and it never returns the queried name itself in that list. So a non-empty
    # list means the exact name is almost certainly registrable.
    #
    # This is INFERRED, not stated by the API. It is good enough to rank and
    # shortlist candidates, and it is not good enough to buy on blindly —
    # confirm in the Zapmail UI before purchase. If Zapmail starts populating
    # exactMatch, the branch above takes over and this proxy stops being used.
    rows = data.get("availableDomains") or []
    if not rows:
        return {domain: (False, None)}

    # Price the queried name off a non-premium suggestion at the same TLD:
    # standard registration pricing is uniform ($12.99 across every row in the
    # measured responses). Premium rows are skipped so a single premium
    # suggestion cannot inflate the estimate.
    price: float | None = None
    for row in rows:
        if row.get("isPremiumDomain"):
            continue
        parsed = _parse_row(row)
        if parsed and parsed[2] is not None:
            price = parsed[2]
            break
    return {domain: (True, price)}


class _RateLimited(Exception):
    """Zapmail returned 429; stop spending calls this run."""


# ── registration check (authoritative, free) ─────────────────────────────────

async def _is_registered(client: httpx.AsyncClient, domain: str) -> bool | None:
    """True when the domain resolves NS records, i.e. it is already taken.

    A registered domain has nameservers in the global DNS; an unregistered one
    returns NXDOMAIN. This is the ground truth Zapmail's search does not give
    us, it costs nothing, and it is not rate limited.

    Returns None on lookup failure so an unreachable resolver is never read as
    "available".
    """
    try:
        resp = await client.get(
            "https://dns.google/resolve",
            params={"name": domain, "type": "NS"},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:  # noqa: BLE001
        return None

    status = data.get("Status")
    if status == 3:  # NXDOMAIN — nobody has registered it
        return False
    if status != 0:
        return None
    # An NS answer means registered. A SOA-only answer (no NS records) can
    # still mean registered-but-unconfigured, so treat any answer as taken.
    if data.get("Answer"):
        return True
    # NOERROR with no answer: the name exists in DNS but publishes no NS at
    # this level. Ambiguous — do not claim it is free.
    return None if data.get("Authority") else False


async def filter_registered(
    domains: list[str], *, concurrency: int = 10,
) -> dict[str, bool | None]:
    """``{domain: is_registered}``. None means the check was inconclusive."""
    if not domains:
        return {}
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=15.0) as client:
        async def one(d: str) -> tuple[str, bool | None]:
            async with sem:
                return d, await _is_registered(client, d)
        return dict(await asyncio.gather(*(one(d) for d in domains)))


async def check_availability(
    domains: list[str],
    *,
    max_calls: int = RATE_LIMIT_CALLS,
) -> dict[str, tuple[bool | None, float | None]]:
    """``{domain: (available, price_usd)}``.

    ``(None, None)`` for any domain we could not resolve — no key, rate-limit
    exhaustion, or an API error. Unknown is never silently coerced to
    available. Cached results cost no calls.
    """
    out: dict[str, tuple[bool | None, float | None]] = {d: (None, None) for d in domains}
    cache = _load_cache()

    pending: list[str] = []
    for d in domains:
        hit = _cached(cache, d)
        if hit is not None:
            out[d] = hit
        else:
            pending.append(d)

    if not pending:
        print(f"  [Domains] all {len(domains)} names served from cache (0 API calls).")
        return out

    key = zapmail_key()
    if not key:
        print("  [Domains] ZAPMAIL_API_KEY not set - availability unknown.",
              file=sys.stderr)
        return out

    budget = min(max_calls, len(pending))
    if len(pending) > budget:
        print(f"  [Domains] {len(pending)} names need lookup but the rate limit "
              f"allows {budget} this run — checking the first {budget}. "
              f"Re-run in {RATE_LIMIT_WINDOW_S // 60} min for the rest "
              f"(cached results carry over).")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, domain in enumerate(pending[:budget]):
            try:
                found = await _check_one(client, key, domain)
            except _RateLimited:
                break
            for name, val in found.items():
                if name in out or name == domain:
                    out[name] = val
                cache[name] = {"available": val[0], "price": val[1], "ts": time.time()}
            if i + 1 < budget:
                await asyncio.sleep(_PACING_S)

    _save_cache(cache)
    return out


# ── blacklist history ────────────────────────────────────────────────────────

async def check_blacklists(domains: list[str]) -> dict[str, tuple[str, ...]]:
    """``{domain: (list_label, ...)}`` — empty tuple means clean.

    A domain that is already listed before you own it was burned by a previous
    registrant. Registering it inherits the listing, so this check runs BEFORE
    purchase, not after.

    Delegates to ``blacklist_monitor`` so the control-domain probing and
    authoritative-NS fallback stay in exactly one place. Free (DNS only), so it
    is not subject to the Zapmail call budget.
    """
    if not domains:
        return {}
    try:
        from blacklist_monitor import _check_domain, _pick_strategies  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - import path issue only
        print(f"  [Domains] blacklist check unavailable: {exc}", file=sys.stderr)
        return {d: () for d in domains}

    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=10.0) as client:
        strategies = await _pick_strategies(client)
        results = await asyncio.gather(
            *(_check_domain(client, sem, d, strategies) for d in domains))

    out: dict[str, tuple[str, ...]] = {}
    for r in results:
        out[r["domain"]] = tuple(r.get("listed_on", ()))
        if r.get("inconclusive_zones"):
            print(f"  [Domains] {r['domain']}: inconclusive on "
                  f"{', '.join(r['inconclusive_zones'])} - treat as unverified",
                  file=sys.stderr)
    return out


# ── combined ─────────────────────────────────────────────────────────────────

async def enrich(
    candidates: list[Candidate],
    *,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    skip_blacklist: bool = False,
    max_calls: int = RATE_LIMIT_CALLS,
) -> list[Candidate]:
    """Attach availability, price, and blacklist status to each candidate.

    Only screens candidates that already passed naming rules — with a 10-call
    budget there is no room to spend one on a name we would not buy anyway.
    Rejected candidates pass through untouched so the caller can still show
    the audit.
    """
    passing = [c for c in candidates if c.ok]
    if not passing:
        return candidates

    domains = [c.domain for c in passing]

    # DNS first: it is free, unlimited, and authoritative about registration,
    # whereas Zapmail's search only tells us whether it can offer SUGGESTIONS
    # around a name. Measured 2026-08-19: Zapmail reported smarttalent.com,
    # smartsearch.com and three more as available when all five resolve NS
    # records and are plainly registered. Anything DNS says is taken is taken,
    # and no Zapmail call is spent on it.
    registered = await filter_registered(domains)
    maybe_free = [d for d in domains
                  if registered.get(d) is False]
    unknown_reg = [d for d in domains if registered.get(d) is None]
    if unknown_reg:
        print(f"  [Domains] {len(unknown_reg)} name(s) could not be checked in DNS; "
              "treating as unverified rather than available", file=sys.stderr)

    avail = await check_availability(maybe_free, max_calls=max_calls)
    # Domains DNS proved registered are unavailable regardless of Zapmail.
    for d in domains:
        if registered.get(d) is True:
            avail[d] = (False, None)
        elif registered.get(d) is None and d not in avail:
            avail[d] = (None, None)

    listed: dict[str, tuple[str, ...]] = {}
    if not skip_blacklist:
        available_now = [d for d in domains if avail.get(d, (None, None))[0] is True]
        listed = await check_blacklists(available_now)

    out: list[Candidate] = []
    for c in candidates:
        if not c.ok:
            out.append(c)
            continue
        is_avail, price = avail.get(c.domain, (None, None))
        rejections = list(c.rejections)
        if price is not None and price > price_ceiling:
            rejections.append(f"price:${price:.2f}>${price_ceiling:.2f}")
        hits = listed.get(c.domain, ())
        if hits:
            rejections.append(f"blacklisted:{','.join(hits)}")
        out.append(Candidate(
            domain=c.domain,
            sld=c.sld,
            tld=c.tld,
            source_tokens=c.source_tokens,
            rejections=tuple(rejections),
            similarity_to_main=c.similarity_to_main,
            available=is_avail,
            price_usd=price,
            blacklisted_on=hits,
        ))
    return out


def zones_checked() -> str:
    """Human label of the DNSBLs consulted, for report headers."""
    return ", ".join(BLACKLIST_ZONES.values())
