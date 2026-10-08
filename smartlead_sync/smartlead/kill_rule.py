"""When should an inbox be killed? The team's rule (agreed 2026-10-08).

WARNING ONLY. This module never retires anything. It says SHOULD KILL / WATCH /
GOOD / NO DATA and why; a person decides and acts.

Sources: our own research (docs/DELIVERABILITY_RESEARCH_AND_WORKFLOW.md —
"two consecutive failures = retire, one test is too noisy"), the team's
thresholds (Anjali 2026-10-06: under 80% inbox = spam; domain with half or
more mailboxes in spam = spam), the health playbook (re-test after 7 days),
and a binomial check: one ~20-seed test wrongly flags a truly-85% inbox
~17% of the time; two strikes or ~40 pooled seeds brings that near 3%.

Rule:
  * A test counts only with >= MIN_SEEDS classified seeds for that inbox.
  * Each test is graded on its WORSE receiving provider (Google / Outlook):
    >= 80% good, under 80% a strike.
  * SHOULD KILL when any of:
      (a) the two newest counting tests are both strikes, >= 7 days apart;
      (b) the newest counting test is under 50% on >= 20 seeds;
      (c) its domain's newest tests, pooled across its mailboxes, have
          >= 40 seeds under 80% AND half or more of those mailboxes struck.
  * WATCH: newest counting test is a strike (one strike, not yet proof).
  * GOOD: newest counting test >= 80% and not older than FRESH_DAYS.
  * NO DATA: no counting test in FRESH_DAYS.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

GOOD_AT = 80.0
DEAD_BELOW = 50.0
MIN_SEEDS = 10
DEAD_MIN_SEEDS = 20
DOMAIN_MIN_SEEDS = 40
MIN_DAYS_BETWEEN = 7
FRESH_DAYS = 21

KILL, WATCH, GOOD, NO_DATA = "SHOULD KILL", "WATCH", "GOOD", "NO DATA"


def grade(test: dict) -> float | None:
    """Worse-provider inbox % of one test, or None when it does not count.
    ``test`` = {date, seeds, spam, by: {provider: [seeds, spam]}}."""
    if int(test.get("seeds") or 0) < MIN_SEEDS:
        return None
    pcts = []
    for prov, (n, spam) in (test.get("by") or {}).items():
        if prov in ("Google", "Outlook") and n >= 3:          # a provider with 1-2 seeds says little
            pcts.append(100.0 * (n - spam) / n)
    overall = 100.0 * (test["seeds"] - test["spam"]) / test["seeds"]
    return min(pcts + [overall])


def counting_tests(tests: list[dict]) -> list[tuple[dict, float]]:
    """Newest first, only tests that count, with their grade."""
    out = []
    for t in sorted(tests or [], key=lambda t: t["date"], reverse=True):
        g = grade(t)
        if g is not None:
            out.append((t, g))
    return out


def _days(a: str, b: str) -> int:
    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def judge(email: str, tests: list[dict], domain_verdict: tuple[bool, str] | None, today: date) -> dict:
    """Verdict for one inbox. ``domain_verdict`` = (domain is dead by rule c, why)."""
    ct = counting_tests(tests)
    fresh = [(t, g) for t, g in ct if (today - date.fromisoformat(t["date"])).days <= FRESH_DAYS]
    if domain_verdict and domain_verdict[0]:
        return {"email": email, "verdict": KILL, "rule": "c", "why": domain_verdict[1]}
    if not ct:
        return {"email": email, "verdict": NO_DATA, "rule": "", "why": f"no test with {MIN_SEEDS}+ seeds"}
    (t1, g1) = ct[0]
    if g1 < DEAD_BELOW and t1["seeds"] >= DEAD_MIN_SEEDS and fresh:
        return {"email": email, "verdict": KILL, "rule": "b",
                "why": f"{g1:.0f}% inbox on {t1['seeds']} seeds ({t1['date']}) — clearly dead"}
    if len(ct) >= 2:
        (t2, g2) = ct[1]
        if g1 < GOOD_AT and g2 < GOOD_AT and _days(t1["date"], t2["date"]) >= MIN_DAYS_BETWEEN and fresh:
            return {"email": email, "verdict": KILL, "rule": "a",
                    "why": f"two strikes: {g2:.0f}% ({t2['date']}) then {g1:.0f}% ({t1['date']})"}
    if not fresh:
        return {"email": email, "verdict": NO_DATA, "rule": "",
                "why": f"last counting test {t1['date']} ({g1:.0f}%) is older than {FRESH_DAYS} days"}
    if g1 < GOOD_AT:
        return {"email": email, "verdict": WATCH, "rule": "",
                "why": f"one strike: {g1:.0f}% on {t1['seeds']} seeds ({t1['date']}) — retest before killing"}
    return {"email": email, "verdict": GOOD, "rule": "", "why": f"{g1:.0f}% inbox ({t1['date']})"}


def domain_verdicts(tests_by_email: dict[str, list[dict]], today: date) -> dict[str, tuple[bool, str]]:
    """Rule (c) per domain: pool each mailbox's newest fresh counting test."""
    per: dict[str, list[tuple[dict, float]]] = defaultdict(list)
    for email, tests in tests_by_email.items():
        ct = [(t, g) for t, g in counting_tests(tests)
              if (today - date.fromisoformat(t["date"])).days <= FRESH_DAYS]
        if ct:
            per[email.split("@")[1]].append(ct[0])
    out = {}
    for dom, rows in per.items():
        seeds = sum(t["seeds"] for t, _ in rows)
        inbox = sum(t["seeds"] - t["spam"] for t, _ in rows)
        struck = sum(1 for _, g in rows if g < GOOD_AT)
        pooled = 100.0 * inbox / seeds if seeds else 100.0
        dead = seeds >= DOMAIN_MIN_SEEDS and pooled < GOOD_AT and struck * 2 >= len(rows)
        out[dom] = (dead, f"domain {dom}: {pooled:.0f}% inbox pooled over {seeds} seeds, "
                          f"{struck}/{len(rows)} mailboxes struck")
    return out


def judge_all(tests_by_email: dict[str, list[dict]], today: date) -> dict[str, dict]:
    doms = domain_verdicts(tests_by_email, today)
    return {e: judge(e, t, doms.get(e.split("@")[1]), today) for e, t in tests_by_email.items()}
