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
  * Each test is graded on the inbox's OWN receiving provider (test ``kind``: the sender's Smartlead type;
    a Google inbox on its Google seeds, an Outlook inbox on its Outlook seeds; every campaign is ESP-matched,
    smartlead/own_provider.py, 17 Nov 2026): >= 80% good, under 80% a strike. The other provider is only a
    signal named in the reason. An SMTP inbox, or one whose type is not known, is graded on its WORSE
    receiving provider, as before. A typed inbox with fewer than 3 seeds at its own provider: the test does
    not count.
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

from smartlead.own_provider import own_provider, signal

GOOD_AT = 80.0
DEAD_BELOW = 50.0
MIN_SEEDS = 10
DEAD_MIN_SEEDS = 20
DOMAIN_MIN_SEEDS = 40
MIN_DAYS_BETWEEN = 7
FRESH_DAYS = 21

KILL, WATCH, GOOD, NO_DATA = "SHOULD KILL", "WATCH", "GOOD", "NO DATA"


_RECEIVER = {"google": "Google", "microsoft": "Outlook"}      # own_provider key -> the receiver label in ``by``
MIN_PROVIDER_SEEDS = 3                                       # a provider with 1-2 seeds says little


def _own(test: dict) -> str | None:
    return _RECEIVER.get(own_provider(test.get("kind")) or "")


def own_counts(test: dict) -> tuple[int, int]:
    """(seeds, spam) the inbox is judged on: its own receiving provider's, or the whole test's when it has none."""
    own = _own(test)
    if own:
        n, spam = ((test.get("by") or {}).get(own) or [0, 0])[:2]
        return int(n), int(spam)
    return int(test.get("seeds") or 0), int(test.get("spam") or 0)


def other_signal(test: dict) -> str:
    """'; also 40% inbox at Google (not its provider: a signal, not a verdict)' when the other receiving provider
    was under GOOD_AT for a typed inbox, else ''."""
    own = _own(test)
    if not own:
        return ""
    bits = []
    for prov, (n, spam) in sorted((test.get("by") or {}).items()):
        if prov in ("Google", "Outlook") and prov != own and n >= MIN_PROVIDER_SEEDS and 100.0 * (n - spam) / n < GOOD_AT:
            bits.append(signal(prov, f"{100.0 * (n - spam) / n:.0f}% inbox"))
    return "; also " + ", ".join(bits) if bits else ""


def grade(test: dict) -> float | None:
    """Inbox % of one test at the inbox's own receiving provider (worse provider for SMTP / unknown type), or None
    when it does not count. ``test`` = {date, seeds, spam, by: {provider: [seeds, spam]}, kind}."""
    if int(test.get("seeds") or 0) < MIN_SEEDS:
        return None
    if _own(test):
        n, spam = own_counts(test)
        return 100.0 * (n - spam) / n if n >= MIN_PROVIDER_SEEDS else None
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
        return {"email": email, "verdict": KILL, "rule": "c", "why": domain_verdict[1],
                "pct": domain_verdict[2] if len(domain_verdict) > 2 else None}
    if not ct:
        return {"email": email, "verdict": NO_DATA, "rule": "", "why": f"no test with {MIN_SEEDS}+ seeds"}
    (t1, g1) = ct[0]
    out = _judge_latest(email, ct, fresh, t1, g1)
    out["pct"] = g1
    return out


def _judge_latest(email: str, ct: list, fresh: list, t1: dict, g1: float) -> dict:
    if g1 < DEAD_BELOW and t1["seeds"] >= DEAD_MIN_SEEDS and fresh:
        return {"email": email, "verdict": KILL, "rule": "b",
                "why": f"only {g1:.0f}% of {_emails(t1)} reached the inbox ({t1['date']})"}
    if len(ct) >= 2:
        (t2, g2) = ct[1]
        if g1 < GOOD_AT and g2 < GOOD_AT and _days(t1["date"], t2["date"]) >= MIN_DAYS_BETWEEN and fresh:
            return {"email": email, "verdict": KILL, "rule": "a",
                    "why": f"failed 2 tests: {g2:.0f}% ({t2['date']}), then {g1:.0f}% ({t1['date']})"}
    if not fresh:
        return {"email": email, "verdict": NO_DATA, "rule": "",
                "why": f"last counting test {t1['date']} ({g1:.0f}%) is older than {FRESH_DAYS} days"}
    if g1 < GOOD_AT:
        return {"email": email, "verdict": WATCH, "rule": "",
                "why": f"failed 1 test: {g1:.0f}% of {_emails(t1)} reached the inbox ({t1['date']})"}
    return {"email": email, "verdict": GOOD, "rule": "", "why": f"{g1:.0f}% inbox ({t1['date']}){other_signal(t1)}"}


def _emails(t: dict) -> str:
    """'20 test emails', or 'its 10 Google test emails' when it is judged on its own provider."""
    own = _own(t)
    return f"its {own_counts(t)[0]} {own} test emails" if own else f"{t['seeds']} test emails"


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
        seeds = sum(own_counts(t)[0] for t, _ in rows)          # each inbox at its own provider
        inbox = sum(own_counts(t)[0] - own_counts(t)[1] for t, _ in rows)
        struck = sum(1 for _, g in rows if g < GOOD_AT)
        pooled = 100.0 * inbox / seeds if seeds else 100.0
        dead = seeds >= DOMAIN_MIN_SEEDS and pooled < GOOD_AT and struck * 2 >= len(rows)
        at = " at their own provider" if any(_own(t) for t, _ in rows) else ""
        out[dom] = (dead, f"whole domain: {pooled:.0f}% of {seeds} test emails{at} reached the inbox "
                          f"(needs {GOOD_AT:.0f}%), {struck} of {len(rows)} inboxes failed", pooled)
    return out


def judge_all(tests_by_email: dict[str, list[dict]], today: date) -> dict[str, dict]:
    doms = domain_verdicts(tests_by_email, today)
    return {e: judge(e, t, doms.get(e.split("@")[1]), today) for e, t in tests_by_email.items()}
