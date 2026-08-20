"""Cold-email domain name generation and screening.

Generates candidate sending domains for a client, then screens each candidate
against the rules that actually decide whether a domain survives 2026-era
filtering:

  1. Similarity to the client's main domain. Spamhaus/SURBL began flagging
     domain BATCHES that look like permutations of one root (``meetpatrick``,
     ``getpatrick``, ``patrickhq``). Candidates that read as a permutation of
     the main brand are rejected outright, not merely down-ranked.
  2. Phishing shape. Digit-for-letter substitution (``br4nd``), hyphens, and
     near-miss misspellings of the main brand read as typosquatting to both
     filters and recipients.
  3. TLD. ``.com`` only by default — cheap TLDs (.xyz/.click/.top/.info) carry
     a spam prior that no warmup undoes.
  4. Awkward substrings. Screens unintended words formed at the seam of two
     joined tokens (the classic ``the`` + ``rapist``).

Naming strategy follows the "industry-adjacent, visually distinct" model: the
domain should make sense to a recipient who reads it in a From: header, while
sharing no visible stem with the client's main domain. Instead of permuting the
brand, we combine the client's VOCABULARY (problem statements, value props,
product nouns) into names that stand on their own.

The registrar-spread and purchase-timing rules that pair with this module are
plan-level concerns, exposed via :func:`purchase_schedule`.
"""

from __future__ import annotations

import difflib
import itertools
import re
from dataclasses import dataclass, field

# ── Naming policy ────────────────────────────────────────────────────────────

# Only .com. Recipients and filters both pattern-match on TLD, and the cheap
# TLDs are where the spam prior lives.
ALLOWED_TLDS: tuple[str, ...] = (".com",)

# Brand-permutation affixes. A candidate built from the client's own brand plus
# any of these is exactly the batch shape the DNSBLs started flagging.
BANNED_BRAND_AFFIXES: frozenset[str] = frozenset({
    "get", "go", "try", "use", "my", "the", "hey", "join", "meet", "with",
    "run", "one", "team", "app", "hq", "co", "io", "ai", "now", "pro", "lab",
    "hub", "inc", "group", "mail", "email", "send", "outreach", "reply",
})

# Substrings that read as bulk-mail or spam infrastructure to a filter.
BANNED_SUBSTRINGS: frozenset[str] = frozenset({
    "spam", "bulk", "blast", "mailer", "mailing", "newsletter", "promo",
    "offer", "deal", "discount", "free", "cheap", "mega", "ultra", "grp",
    "click", "cash", "winner", "guaranteed",
})

# Unintended words formed when two clean tokens are joined. Not exhaustive by
# design — it catches the seam-collision class, and review catches the rest.
AWKWARD_SUBSTRINGS: frozenset[str] = frozenset({
    "rapist", "analc", "cumc", "shit", "fuck", "cunt", "dick", "penis",
    "sexc", "nigg", "hell", "damn", "kill", "died", "dead", "slut", "anus",
})

MAX_SLD_LENGTH: int = 20  # shorter reads as a real brand; hard ceiling
MIN_SLD_LENGTH: int = 6

# Two domains we own that differ only by a suffix are the same batch
# fingerprint as a brand permutation — the cluster just points at us instead of
# at the client's brand. A new candidate this similar to a domain already in
# the estate is rejected for the same reason.
SIBLING_REJECT: float = 0.80

# A candidate this similar to the main domain is a permutation, not a
# distinct brand. Tuned so "getbetterdata" vs "betterdata" (0.83) rejects
# while "dataingest" vs "betterdata" (0.42) passes.
SIMILARITY_REJECT: float = 0.62


@dataclass(frozen=True)
class Candidate:
    """One generated domain plus every reason it did or did not survive."""

    domain: str
    sld: str
    tld: str
    source_tokens: tuple[str, ...]
    rejections: tuple[str, ...] = ()
    similarity_to_main: float = 0.0
    # Filled by the availability backend, not the generator.
    available: bool | None = None
    price_usd: float | None = None
    blacklisted_on: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """Passed every naming rule. Says nothing about availability."""
        return not self.rejections

    @property
    def purchasable(self) -> bool:
        """Passed naming rules, is available, and carries no blacklist history."""
        return self.ok and self.available is True and not self.blacklisted_on


@dataclass
class ClientVocabulary:
    """The word bank a client's outreach domains are built from.

    ``main_domain`` is never used to BUILD names — only to reject candidates
    that resemble it. That inversion is the whole point of the module.
    """

    name: str
    main_domain: str
    # What the client's product/service does: "data", "ingest", "clarity".
    value_nouns: list[str] = field(default_factory=list)
    # The problem the client solves: "signal", "coverage", "accuracy".
    problem_nouns: list[str] = field(default_factory=list)
    # Industry words a recipient would recognise: "pipeline", "revenue".
    industry_nouns: list[str] = field(default_factory=list)

    def main_stem(self) -> str:
        """Bare second-level label of the main domain, lowercased."""
        return _sld_of(self.main_domain)

    def token_bank(self) -> list[str]:
        """Deduplicated, order-preserving pool of buildable tokens."""
        seen: set[str] = set()
        out: list[str] = []
        for tok in (*self.value_nouns, *self.problem_nouns, *self.industry_nouns):
            t = re.sub(r"[^a-z]", "", tok.lower())
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def brand_fragment_tokens(self) -> list[str]:
        """Vocabulary tokens that are pieces of the main domain's stem.

        These are legal to supply — a client's vocabulary naturally overlaps
        their own brand — but every name built from one is rejected as a
        permutation, so the caller should surface them rather than let the
        operator wonder why half their vocabulary produced nothing.

        Generic nouns inside the stem are NOT reported: 'data' in 'bettrdata'
        stays usable, so flagging it would send the operator hunting for a
        replacement they do not need.
        """
        return sorted(_stem_fragments(self.main_stem(), tuple(self.token_bank())))


def _sld_of(domain: str) -> str:
    """'try-better-data.com' -> 'trybetterdata' (letters only, lowercased)."""
    label = domain.strip().lower().split("/")[-1].split(".")[0]
    return re.sub(r"[^a-z0-9]", "", label)


def _similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


# Generic English nouns that carry no brand identity even when they appear in
# a client's own domain. 'bettrdata' contains 'data', but 'data' identifies
# nobody — thousands of companies use it, so a recipient cannot read it as
# "same sender". The distinctive half ('bettr') is what must never reappear.
#
# Only add a word here if seeing it ALONE would not bring one company to mind.
GENERIC_BRAND_WORDS: frozenset[str] = frozenset({
    "data", "mail", "email", "lead", "leads", "send", "sales", "market",
    "marketing", "media", "group", "tech", "digital", "cloud", "soft",
    "systems", "system", "solutions", "labs", "works", "global", "world",
    "first", "prime", "core", "next", "smart", "point", "link", "connect",
    "health", "care", "home", "auto", "food", "travel", "money", "pay",
    "shop", "store", "trade", "build", "design", "studio", "agency",
    "partners", "capital", "ventures", "consulting", "services", "service",
})


def _distinctive_stem_parts(main_stem: str) -> set[str]:
    """What is left of the stem after removing generic nouns.

    'bettrdata' -> {'bettr'}; 'preciseleads' -> {'precise'}; 'melior' ->
    {'melior'} (nothing generic to strip). These are the pieces that identify
    the client, so no candidate may contain one.

    Remainders shorter than 4 characters are dropped: they collide with
    ordinary words too easily to be a reliable brand signal.
    """
    if not main_stem:
        return set()
    parts = {main_stem}
    for generic in GENERIC_BRAND_WORDS:
        if generic in main_stem and generic != main_stem:
            for piece in main_stem.split(generic):
                if len(piece) >= 4:
                    parts.add(piece)
    return {p for p in parts if len(p) >= 4}



def _stem_fragments(main_stem: str, tokens: tuple[str, ...]) -> set[str]:
    """Vocabulary tokens that are DISTINCTIVE pieces of the main stem.

    'preciseleads' + {precise, leads, signal} yields {precise} only: 'precise'
    identifies the brand, whereas 'leads' is a generic industry noun that
    thousands of senders use. Reusing a distinctive fragment rebuilds a
    visible piece of the brand — the pattern recipients and list operators
    both read as "same sender, new domain". Reusing a generic noun does not.

    This is why 'bettrdata' can still build names from 'data' but never from
    'bettr'.
    """
    if not main_stem:
        return set()
    return {
        t for t in tokens
        if len(t) >= 4 and t in main_stem and t not in GENERIC_BRAND_WORDS
    }


def _is_brand_permutation(sld: str, main_stem: str,
                          source_tokens: tuple[str, ...] = (),
                          vocab_tokens: tuple[str, ...] = ()) -> bool:
    """True when sld is main_stem wearing a hat.

    Four shapes, in order of how obvious they are to a filter:
      1. the stem itself, or the stem embedded in a longer name;
      2. the stem with a banned affix bolted on either end;
      3. a name built from vocabulary tokens that are fragments of the stem
         ('precise' + 'signal' for preciseleads.in) — the subtle case, and the
         one a pure string-similarity check gets wrong in both directions;
      4. a near-miss misspelling of the stem (typosquat shape).
    """
    if not main_stem:
        return False
    if sld == main_stem:
        return True
    if main_stem in sld or sld in main_stem:
        return True
    # The distinctive remainder of the stem, once generic nouns are stripped:
    # 'bettrdata' minus 'data' leaves 'bettr'. A candidate containing that
    # remainder is a brand permutation even when the operator never supplied
    # it as a vocabulary word ('bettringest'), so check the string itself
    # rather than trusting the token list.
    for remainder in _distinctive_stem_parts(main_stem):
        if remainder in sld:
            return True
    for affix in BANNED_BRAND_AFFIXES:
        if sld == f"{affix}{main_stem}" or sld == f"{main_stem}{affix}":
            return True
    # Any component that is a fragment of the brand stem re-exposes the brand.
    if source_tokens:
        fragments = _stem_fragments(main_stem, tuple(vocab_tokens) or source_tokens)
        if any(t in fragments for t in source_tokens):
            return True
    # Near-miss misspelling (br4nd / brnad) reads as typosquatting. Only
    # meaningful for names of comparable length — a long compound that merely
    # shares letters with the stem is not a typosquat.
    if len(sld) >= 4 and abs(len(sld) - len(main_stem)) <= 3 \
            and _similarity(sld, main_stem) >= SIMILARITY_REJECT:
        return True
    return False


def _has_phishing_shape(sld: str) -> bool:
    """Digit-for-letter substitution or leading/trailing digits."""
    if re.search(r"[a-z][0-9][a-z]", sld):  # br4nd, d4ta
        return True
    if re.search(r"^[0-9]|[0-9]$", sld):
        return True
    return False


def screen(sld: str, tld: str, vocab: ClientVocabulary,
           source_tokens: tuple[str, ...] = (),
           owned_stems: frozenset[str] = frozenset()) -> Candidate:
    """Apply every naming rule to one candidate and record why it failed.

    ``owned_stems`` are the second-level labels of domains already in the
    estate (any client). A candidate that duplicates one, or that is a
    near-sibling of one, is rejected: re-suggesting a domain we own wastes a
    purchase, and buying its lookalike rebuilds the batch fingerprint.
    """
    sld = sld.lower()
    rejections: list[str] = []
    main_stem = vocab.main_stem()
    sim = _similarity(sld, main_stem) if main_stem else 0.0

    if sld in owned_stems:
        rejections.append("already-owned")
    else:
        for owned in owned_stems:
            # Only compare against names of similar length; a short stem is
            # naturally similar to many longer words without being a sibling.
            if abs(len(sld) - len(owned)) <= 3 and \
                    _similarity(sld, owned) >= SIBLING_REJECT:
                rejections.append(f"too-close-to-owned:{owned}")
                break

    if tld not in ALLOWED_TLDS:
        rejections.append(f"tld:{tld} not in {','.join(ALLOWED_TLDS)}")
    if len(sld) > MAX_SLD_LENGTH:
        rejections.append(f"length:{len(sld)}>{MAX_SLD_LENGTH}")
    if len(sld) < MIN_SLD_LENGTH:
        rejections.append(f"length:{len(sld)}<{MIN_SLD_LENGTH}")
    if "-" in sld:
        rejections.append("hyphen")
    if _is_brand_permutation(sld, main_stem, source_tokens,
                             tuple(vocab.token_bank())):
        rejections.append(f"brand-permutation of '{main_stem}'")
    if _has_phishing_shape(sld):
        rejections.append("phishing-shape (digit substitution)")
    for bad in BANNED_SUBSTRINGS:
        if bad in sld:
            rejections.append(f"banned-substring:{bad}")
            break
    for bad in AWKWARD_SUBSTRINGS:
        if bad in sld:
            rejections.append(f"awkward-substring:{bad}")
            break

    return Candidate(
        domain=f"{sld}{tld}",
        sld=sld,
        tld=tld,
        source_tokens=source_tokens,
        rejections=tuple(rejections),
        similarity_to_main=round(sim, 3),
    )


def owned_stems_from(domains: list[str]) -> frozenset[str]:
    """Second-level labels of domains already in the estate.

    Accepts full domains ('ingestsignal.com') or bare labels; both normalise
    to the same stem so callers can pass Smartlead output and hand-typed lists
    interchangeably.
    """
    return frozenset(s for s in (_sld_of(d) for d in domains) if s)


def generate(vocab: ClientVocabulary, limit: int = 60,
             owned_stems: frozenset[str] = frozenset()) -> list[Candidate]:
    """Build candidate domains from the client's vocabulary, best first.

    Two-token compounds only. One token is rarely available in .com; three
    tokens push past the length ceiling and stop reading as a brand. Returns
    ONLY candidates that pass screening — rejects are available via
    :func:`generate_with_rejects` when you want to show the reasoning.
    """
    return [c for c in generate_with_rejects(vocab, limit=limit,
                                             owned_stems=owned_stems) if c.ok][:limit]


def generate_with_rejects(vocab: ClientVocabulary, limit: int = 60,
                          owned_stems: frozenset[str] = frozenset()) -> list[Candidate]:
    """Same as :func:`generate` but keeps rejected candidates for auditing."""
    tokens = vocab.token_bank()
    seen: set[str] = set()
    out: list[Candidate] = []

    for a, b in itertools.permutations(tokens, 2):
        sld = f"{a}{b}"
        if sld in seen:
            continue
        seen.add(sld)
        out.append(screen(sld, ".com", vocab, source_tokens=(a, b),
                          owned_stems=owned_stems))
        if len(out) >= limit * 4:  # generate a surplus; screening thins it
            break

    # Shortest passing names first — they read most like real brands.
    out.sort(key=lambda c: (not c.ok, len(c.sld)))
    return out


# ── Purchase planning ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PurchaseBatch:
    """One registrar/day slice of a domain purchase plan."""

    day_offset: int
    registrar: str
    domains: tuple[str, ...]


def purchase_schedule(
    domains: list[str],
    registrars: list[str],
    per_batch: int = 3,
    day_gap: int = 2,
) -> list[PurchaseBatch]:
    """Split a domain buy across registrars and days.

    Buying N similar domains at one registrar on one day is the batch
    fingerprint that list operators cluster on. Spreading the same N across
    several registrars and several days removes the correlation without
    changing what you end up owning.

    ``day_offset`` is relative (0 = today), so the caller decides the calendar.
    """
    if not registrars:
        raise ValueError("at least one registrar is required")
    if per_batch < 1:
        raise ValueError("per_batch must be >= 1")

    batches: list[PurchaseBatch] = []
    for i in range(0, len(domains), per_batch):
        chunk = tuple(domains[i:i + per_batch])
        idx = i // per_batch
        batches.append(PurchaseBatch(
            day_offset=idx * day_gap,
            registrar=registrars[idx % len(registrars)],
            domains=chunk,
        ))
    return batches
