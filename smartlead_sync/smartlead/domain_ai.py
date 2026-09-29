"""AI-sourced domain candidates and vocabulary for the domain suggester.

Two read-only sources that make suggestions land closer to the client's real
business rather than a generic two-word compound:

  * :func:`ai_suggest` — Zapmail's own AI Domain Finder
    (``POST /v2/domains/ai-finder``). Returns generated, availability-aware
    domains from a keyword seed. Costs one domain-search request (within the
    existing 10/30min budget), no money.

  * :func:`source_vocabulary` — reads the client's own homepage and pulls the
    words their business actually uses, so the generator builds names that read
    like the company. Plain web fetch, no API key involved.

Neither ever spends wallet/credits. The AI finder's results may include TLDs
Zapmail cannot register (``.io``, ``.co``, ...), so every domain returned here
is filtered to :data:`zapmail.REGISTRABLE_TLDS`. Names still pass through the
normal :func:`domain_naming.screen` rules (no brand permutation, no phishing
shape) before being offered.
"""

from __future__ import annotations

import re

import httpx

from smartlead.zapmail import REGISTRABLE_TLDS, ZapmailClient

# ── vocabulary scraping helpers ──────────────────────────────────────────────

# Words that carry no useful name signal. Kept tight on purpose: the generator
# already rejects brand permutations and spam substrings, so the goal here is
# only to avoid seeding a thousand names from "the", "and", "with".
_STOPWORDS: frozenset[str] = frozenset({
    "the", "and", "for", "with", "that", "this", "from", "your", "you", "our",
    "are", "was", "were", "have", "has", "had", "not", "but", "all", "any",
    "can", "will", "would", "should", "about", "into", "than", "they", "their",
    "them", "its", "our", "out", "who", "what", "when", "where", "which",
    "how", "why", "more", "most", "some", "such", "only", "own", "same",
    "then", "than", "too", "very", "just", "get", "got", "let", "see", "been",
    "being", "does", "did", "doing", "https", "http", "www", "com", "inc",
    "llc", "ltd", "contact", "policy", "privacy", "terms", "cookie", "rights",
    "reserved", "copyright", "subscribe", "newsletter",
    # Site chrome: menus, skip links, buttons (bettrdata.io's scrape yielded
    # "home", "skip", "content" -> homeingest.com, seekingskip.com; 2026-09-28).
    "home", "skip", "content", "main", "menu", "navigation", "login", "logout",
    "sign", "signup", "search", "close", "open", "toggle", "click", "page",
    "read", "learn", "book", "demo", "request", "started", "start", "welcome",
    "blog", "careers", "about", "team", "company", "services", "solutions",
    # Filler verbs / marketing adjectives that make meaningless compounds.
    "make", "makes", "making", "help", "helps", "helping", "provide",
    "provides", "providing", "seeking", "seek", "driven", "cutting", "edge",
    "smarter", "smart", "better", "best", "leading", "every", "today", "free",
    "success", "successful", "informed", "decisions", "business", "businesses",
    "world", "people", "need", "needs", "want", "like", "also", "well", "work",
    "works", "using", "used", "ensure", "across", "through", "within", "over",
})

_WORD_RE = re.compile(r"[a-z]{4,16}")


def _extract_words(text: str) -> list[str]:
    """Alphabetic words (4-16 chars), stopwords dropped, order preserved."""
    seen: list[str] = []
    for w in _WORD_RE.findall(text.lower()):
        if w in _STOPWORDS or w in seen:
            continue
        seen.append(w)
    return seen


async def source_vocabulary(
    domain: str,
    *,
    max_words: int = 24,
    timeout: float = 10.0,
) -> list[str]:
    """Seed words scraped from a client's own site. Read-only, best-effort.

    Pulls the ``<title>``, ``<meta name="description">``, and ``<meta
    name="keywords">`` text, plus one pass of body/h1 words, and returns the
    most on-brand candidates (title/meta first, body as overflow). Returns an
    empty list when the site is unreachable — the caller falls back to
    hand-supplied vocabulary rather than failing.
    """
    domain = domain.strip().lower().split("/")[0]
    if not domain or "." not in domain:
        return []

    page_title = ""
    meta_words: list[str] = []
    body_words: list[str] = []

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for scheme in ("https", "http"):
            try:
                resp = await client.get(f"{scheme}://{domain}")
                resp.raise_for_status()
                html = resp.text
            except Exception:  # noqa: BLE001
                continue
            break
        else:
            return []

    # Title — highest-signal text on the page.
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        page_title = re.sub(r"\s+", " ", m.group(1)).strip()
    title_words = _extract_words(page_title)

    # Meta description / keywords — the company's chosen vocabulary.
    for pattern in (
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\'](.*?)["\']',
    ):
        m = re.search(pattern, html, re.I | re.S)
        if m:
            meta_words.extend(_extract_words(m.group(1)))

    # Body/h1 overflow, only used if title+meta don't fill the budget.
    text = re.sub(r"<script.*?</script>|<style.*?</style>|<[^>]+>", " ", html,
                  flags=re.I | re.S)
    body_words = _extract_words(text)

    ordered: list[str] = []
    for w in (*title_words, *meta_words, *body_words):
        if w not in ordered:
            ordered.append(w)
        if len(ordered) >= max_words:
            break
    return ordered


# ── AI Domain Finder ─────────────────────────────────────────────────────────

def parse_ai_domains(result: dict) -> list[dict]:
    """Normalise AI-finder rows to ``{domain, available, price, renew_price,
    premium}``; registrable single-label TLDs only, de-duplicated.

    Verified live 2026-09-28: rows are objects, not strings —
    ``{"domainName": "streamintake.com", "status": "AVAILABLE",
    "isPremiumDomain": false, "domainPrice": "12.99", "renewPrice": "20.99"}``
    (the first version expected strings and silently returned nothing).
    Plain strings are still accepted in case the shape varies.
    """
    from smartlead.domain_availability import parse_price as _money  # NaN-safe

    out: list[dict] = []
    seen: set[str] = set()
    for raw in (result.get("domains") or []):
        row = raw if isinstance(raw, dict) else {"domainName": raw}
        name = str(row.get("domainName") or "").strip().lower()
        if not name or name.count(".") != 1 or name in seen:
            continue
        if name.rsplit(".", 1)[-1] not in REGISTRABLE_TLDS:
            continue
        seen.add(name)
        status = str(row.get("status") or "").strip().upper()
        out.append({
            "domain": name,
            "available": (status == "AVAILABLE") if status else None,
            "price": _money(row.get("domainPrice")),
            "renew_price": _money(row.get("renewPrice")),
            "premium": bool(row.get("isPremiumDomain")),
        })
    return out


async def ai_suggest(
    keywords: list[str],
    *,
    tlds: list[str] | None = None,
    desired_count: int = 12,
    max_polls: int = 12,
    client: str | None = None,
) -> list[dict]:
    """AI-suggested, availability-checked domains from Zapmail. Read-only, free.

    ``keywords`` seed the model — a FEW specific words work best (3-6);
    ``tlds`` are clamped to registrable TLDs. The first call starts generation
    and later calls poll (~15-20s live), up to ``max_polls x 5s``. Returns
    rows from :func:`parse_ai_domains` (may be empty). Zapmail returns fewer
    than ``desired_count`` (asked 10, got 6 on 2026-09-28).

    ``client`` picks which Zapmail key to ask with; any works (account-agnostic).
    """
    keywords = [k.strip().lower() for k in keywords if k.strip()]
    if not keywords:
        return []
    tlds = [t.strip().lower() for t in (tlds or ["com"])
            if t.strip().lower() in REGISTRABLE_TLDS] or ["com"]

    from smartlead.zapmail_accounts import api_key_for_client
    api_key = api_key_for_client(client)

    async with ZapmailClient(api_key=api_key) as z:
        result = await z.ai_domain_finder(
            keywords, tlds, desired_count, max_polls=max_polls)
    return parse_ai_domains(result)