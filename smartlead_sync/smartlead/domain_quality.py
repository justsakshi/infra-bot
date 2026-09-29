"""Does a domain name sound like a real company in the client's business?

The naming rules in ``domain_naming`` decide what is SAFE (no brand
permutation, no phishing shape, no spam words). They say nothing about
whether a name is any GOOD. Live runs on 2026-09-29 showed the gap:

  * Zapmail's AI finder drifts into metaphors: ``recordflume``,
    ``streamsluice``, ``batchriver`` - odd words, nothing a recipient links to
    BettrData's business.
  * Our generator paired keywords blindly: ``ingestdedupe`` (verb + verb),
    ``catalogpostal`` (adjective last), ``catalogretail``.

This module is the missing judgement, applied to BOTH sources:

  1. The name must split cleanly into familiar words (the client's own words
     plus a curated list of plain brand words). An unfamiliar chunk such as
     ``flume`` or ``sluice`` rejects it.
  2. At least one word must be about the client's business.
  3. It must read like a company: at most three words, ending in a noun
     ("clear" + "records", "postal" + "works"), never ending in an adjective
     or a verb.

Surviving names get a score (specific > generic, two words > three, shorter
> longer) so the best ones are offered first. Pure and deterministic: no
network, no model, same answer every run.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

# Plain, positive first words that make a noun read like a brand
# ("clearrecords", "steadyintake"). Deliberately ordinary: a recipient should
# not have to decode them. Never a spam word, never a brand affix.
# Each must describe the business noun it precedes: colours and trees read
# as random next to one ("cedarpipeline", "oakmail"; probe 2026-09-29).
TONE_WORDS: frozenset[str] = frozenset({
    "clear", "true", "bright", "steady", "solid", "sure", "fresh", "sharp",
    "simple", "swift", "exact", "direct", "open", "bold", "ready", "modern",
    "trusted", "proven", "honest", "prime", "first", "sound", "whole",
})

# Short business suffixes (same set the generator uses for tier-2 names).
SUFFIXES: frozenset[str] = frozenset({
    "hq", "works", "group", "labs", "co", "team", "desk", "base", "point",
    "field", "stack", "grid", "core", "path", "scope",
})

# Nouns that finish a company name well whatever the business
# ("postalworks", "catalogcraft", "recordsbridge").
GENERIC_HEADS: frozenset[str] = SUFFIXES | frozenset({
    "craft", "house", "studio", "bridge", "lane", "line", "loop", "mark",
    "source", "signal", "sight", "view", "lens", "gate",
    "forge", "circle", "office", "room", "table", "index", "atlas", "compass",
    "beacon", "anchor", "harbor", "port", "nest", "hub", "lab", "partners",
    "collective", "bureau", "network", "exchange", "center", "flow", "shift",
    "spark", "ledger", "ridge",
})

# Generic heads good enough to build new names from (the long tail above is
# only there so Zapmail's AI names containing them are understood).
BUILD_HEADS: tuple[str, ...] = (
    "works", "craft", "house", "desk", "bridge", "line", "mark", "source",
    "signal", "view", "gate", "circle", "index", "ledger", "studio", "path",
)

# Suffixes worth appending when two-word names are exhausted.
BUILD_SUFFIXES: tuple[str, ...] = ("hq", "co", "works", "labs", "group", "desk")

MAX_WORDS = 3
IDEAL_LENGTH = 12


@dataclass(frozen=True)
class BusinessWords:
    """What a client's names may be built from, by role.

    ``lead`` words go first (describe: "postal", "clean", "outbound").
    ``heads`` go last (things: "records", "catalog", "meetings").
    ``related`` are on-topic words that may sit anywhere ("stream", "print").
    ``keywords`` is the old flat list; kept so older profiles still work.
    """

    lead: frozenset[str] = frozenset()
    heads: frozenset[str] = frozenset()
    related: frozenset[str] = frozenset()
    keywords: frozenset[str] = frozenset()

    @classmethod
    def from_profile(cls, prof: dict) -> "BusinessWords":
        def words(key: str) -> frozenset[str]:
            return frozenset(w.strip().lower() for w in prof.get(key) or [] if w.strip())
        kw = words("keywords")
        lead, heads = words("lead_words"), words("head_nouns")
        if not lead and not heads:
            # Old profile: no roles given, so any keyword may lead or finish.
            lead = heads = kw
        return cls(lead=lead, heads=heads, related=words("related"), keywords=kw)

    @property
    def on_topic(self) -> frozenset[str]:
        """Words that tie a name to this business (tone words never do)."""
        return (self.lead | self.heads | self.related | self.keywords) - TONE_WORDS

    @property
    def vocabulary(self) -> frozenset[str]:
        return self.lead | self.heads | self.related | self.keywords | TONE_WORDS | GENERIC_HEADS


@dataclass(frozen=True)
class Verdict:
    ok: bool
    score: float = 0.0
    words: tuple[str, ...] = ()
    reason: str = ""


def segment(sld: str, vocabulary: frozenset[str]) -> tuple[str, ...] | None:
    """Split ``sld`` into the fewest known words, or None if it cannot be."""
    return _segment(sld, vocabulary)


@lru_cache(maxsize=4096)
def _segment(sld: str, vocabulary: frozenset[str]) -> tuple[str, ...] | None:
    n = len(sld)
    best: list[tuple[str, ...] | None] = [None] * (n + 1)
    best[0] = ()
    for i in range(n):
        if best[i] is None:
            continue
        for j in range(i + 2, n + 1):  # words of 2+ letters
            w = sld[i:j]
            if w in vocabulary:
                cand = best[i] + (w,)
                if best[j] is None or len(cand) < len(best[j]):
                    best[j] = cand
    return best[n]


def _overlapping(words: tuple[str, ...]) -> bool:
    """Same word twice, or one inside another: data+datasets, mail+mailbox."""
    for i, a in enumerate(words):
        for b in words[i + 1:]:
            if a == b or a in b or b in a or a[:4] == b[:4]:
                return True
    return False


# A word starting with a common ending, glued to a word ending in a
# consonant, reads as that ending: clean + ingest = "cleaningest"
# ("cleaning est"), print + edge = "printedge" ("printed ge").
_SEAM_ENDINGS = ("ing", "ed", "er", "est")


def _misreads(words: tuple[str, ...]) -> bool:
    for a, b in zip(words, words[1:]):
        if a[-1] not in "aeiouy" and b.startswith(_SEAM_ENDINGS):
            return True
    return False


def judge(sld: str, bw: BusinessWords) -> Verdict:
    """Accept or reject one name, with a score for ranking the survivors."""
    sld = sld.lower()
    words = segment(sld, bw.vocabulary)
    if words is None:
        return Verdict(False, reason="contains an unfamiliar word")
    if len(words) < 2:
        return Verdict(False, words=words, reason="a single word")
    if len(words) > MAX_WORDS:
        return Verdict(False, words=words, reason="too many words")
    if _overlapping(words):
        return Verdict(False, words=words, reason="repeats a word")
    if _misreads(words):
        return Verdict(False, words=words, reason="reads as a different word at the join")

    on_topic = [w for w in words if w in bw.on_topic]
    if not on_topic:
        return Verdict(False, words=words, reason="nothing about the business")

    core = list(words)
    suffixed = len(core) == 3 and core[-1] in SUFFIXES
    if suffixed:
        core = core[:-1]
    head = core[-1]
    finishers = bw.heads | bw.related | GENERIC_HEADS
    if head not in finishers:
        return Verdict(False, words=words, reason=f"ends on '{head}', not a noun")
    first = core[0]
    starters = bw.lead | bw.heads | bw.related | TONE_WORDS
    if first not in starters:
        return Verdict(False, words=words, reason=f"starts with '{first}'")
    if len(core) == 3 and core[1] not in (bw.heads | bw.related | bw.lead):
        return Verdict(False, words=words, reason="three words that do not read as one name")

    score = 10.0
    specific = [w for w in on_topic if w in bw.heads or w in bw.related]
    score += 3 * min(len(set(specific)), 2)
    score += 2 if any(w in bw.lead for w in on_topic) else 0
    score += 1 if head in bw.heads else 0
    score += 2 if len(words) == 2 else -2
    score -= 0.4 * max(0, len(sld) - IDEAL_LENGTH)
    return Verdict(True, score=round(score, 2), words=words)


def build_names(bw: BusinessWords, *, with_suffixes: bool = True) -> list[tuple[str, tuple[str, ...], Verdict]]:
    """Candidate names built by role, judged and sorted best first.

    Shapes, all ending in a noun:
      lead + head        postal + catalog, outbound + meetings
      tone + head        clear + records, steady + intake
      lead|head + noun   postal + works, records + bridge
      any above + suffix clearrecords + hq (only as a fallback tier)
    """
    leads = sorted(bw.lead - TONE_WORDS) or sorted(bw.keywords)
    heads = sorted(bw.heads) or sorted(bw.keywords)
    seen: set[str] = set()
    out: list[tuple[str, tuple[str, ...], Verdict]] = []

    def add(*parts: str) -> None:
        sld = "".join(parts)
        if sld in seen or _overlapping(parts) or _misreads(parts):
            return
        seen.add(sld)
        v = judge(sld, bw)
        if v.ok:
            out.append((sld, parts, v))

    for a in leads:
        for h in heads:
            add(a, h)
    for t in sorted(TONE_WORDS):
        for h in heads:
            add(t, h)
    for a in sorted(set(leads) | set(heads)):
        for g in BUILD_HEADS:
            add(a, g)
    if with_suffixes:
        two = [p for _, p, _ in out]
        for parts in two:
            for s in BUILD_SUFFIXES:
                if s not in parts:
                    add(*parts, s)
    out.sort(key=lambda r: (-r[2].score, len(r[0]), r[0]))
    return out
