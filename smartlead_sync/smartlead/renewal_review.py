"""Before every bill: which inboxes are worth paying for again?

Inboxes renew with their subscription (Zapmail) or order (ScaledMail) — the
card is charged and every inbox in it renews. On 2026-10-08 the team found
out at billing time that BettrData was paying for inboxes that had sat in
spam for weeks. This review runs before each bill and says, per inbox,
KEEP / RETIRE / CHECK, from what we actually know:

  RETIRE  latest placement test under 80% inbox (team rule), or its domain is
          a "spam" domain (half or more of its tested mailboxes under 80%),
          or the inbox is not in any Smartlead account (paid for, unused)
  CHECK   no placement test in the last 21 days, Smartlead connection broken,
          or warmup reputation under 70%
  KEEP    tested in the inbox recently and healthy

Pure functions (unit-tested); ``renewal_review.py`` collects and posts.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

GOOD_AT = 80.0          # mailbox inbox % (team rule, 2026-10-06)
FRESH_DAYS = 21         # a placement result older than this is not evidence
MIN_REPUTATION = 70.0   # Smartlead warmup reputation


def _rep(v) -> float | None:
    try:
        return float(str(v).strip().rstrip("%"))
    except (TypeError, ValueError):
        return None


def judge_inbox(email: str, facts: dict | None, domain_spam: bool, today: date,
                complete: bool = True) -> tuple[str, str]:
    """``(verdict, reason)`` for one inbox. ``facts`` from Smartlead +
    SmartDelivery: {in_smartlead, connected, reputation, pct, tested_on, campaigns}."""
    f = facts or {}
    if not f.get("in_smartlead"):
        # Only proof when every Smartlead account was read: a 429 on 2026-10-08
        # made all 39 Precise Leads inboxes look "unused".
        if not complete:
            return "CHECK", "Smartlead could not be read just now — no verdict"
        return "RETIRE", "not in any Smartlead account (paid for, never used)"
    pct, tested = f.get("pct"), f.get("tested_on")
    age = (today - date.fromisoformat(tested)).days if tested else None
    fresh = age is not None and age <= FRESH_DAYS
    if fresh and pct is not None and pct < GOOD_AT:
        return "RETIRE", f"{pct:.0f}% inbox in the {tested} test"
    if domain_spam:
        return "RETIRE", "its domain is in spam (half or more of its mailboxes under 80%)"
    if not f.get("connected", True):
        return "CHECK", "Smartlead connection broken"
    rep = _rep(f.get("reputation"))
    if rep is not None and rep < MIN_REPUTATION:
        return "CHECK", f"warmup reputation {rep:.0f}%"
    if not fresh:
        return "CHECK", "no placement test in 21 days" + (f" (last {tested}: {pct:.0f}%)" if tested and pct is not None else "")
    return "KEEP", f"{pct:.0f}% inbox ({tested})"


def spam_domains(facts: dict[str, dict], today: date) -> set[str]:
    """Domains where half or more of the freshly tested mailboxes are under 80%."""
    per: dict[str, list[bool]] = defaultdict(list)
    for email, f in facts.items():
        t = f.get("tested_on")
        if f.get("pct") is None or not t or (today - date.fromisoformat(t)).days > FRESH_DAYS:
            continue
        per[email.split("@")[1]].append(f["pct"] < GOOD_AT)
    return {d for d, bad in per.items() if bad and sum(bad) * 2 >= len(bad)}


def review_bill(bill: dict, facts: dict[str, dict], today: date, complete: bool = True) -> dict:
    """``bill`` = {provider, label, bills_on, price, inboxes:[emails], client(s), ...}."""
    spam = spam_domains(facts, today)
    rows = []
    for email in bill.get("inboxes") or []:
        v, why = judge_inbox(email, facts.get(email), email.split("@")[1] in spam, today, complete)
        rows.append({"email": email, "verdict": v, "why": why,
                     "campaigns": (facts.get(email) or {}).get("campaigns") or []})
    n = len(bill.get("inboxes") or []) or int(bill.get("mailboxes") or 0) or 1
    per_inbox = float(bill.get("price") or 0) / n if bill.get("per_inbox") is None else float(bill["per_inbox"])
    retire = [r for r in rows if r["verdict"] == "RETIRE"]
    return {**bill, "rows": rows,
            "counts": {k: sum(1 for r in rows if r["verdict"] == k) for k in ("KEEP", "RETIRE", "CHECK")},
            "retire_saves_monthly": round(per_inbox * len(retire), 2)}


def format_review(reviews: list[dict], days: int) -> str:
    if not reviews:
        return f"*🧾 Renewal review* — nothing bills in the next {days} days."
    total_save = sum(r["retire_saves_monthly"] for r in reviews)
    lines = [f"*🧾 Renewal review — bills in the next {days} days* · retiring the flagged inboxes saves "
             f"*${total_save:,.2f}/month*"]
    for r in reviews:
        c = r["counts"]
        lines.append(f"\n*{r['bills_on']} · {r['provider']} · {r['label']}* — ${float(r.get('price') or 0):,.2f} · "
                     f"{len(r['rows'])} inboxes: {c['KEEP']} keep · *{c['RETIRE']} retire* · {c['CHECK']} check"
                     + (f" · {', '.join(r.get('clients') or [])}" if r.get("clients") else ""))
        if r.get("note"):
            lines.append(f"   _{r['note']}_")
        for row in [x for x in r["rows"] if x["verdict"] == "RETIRE"][:15]:
            live = " :warning: in " + ", ".join(row["campaigns"][:2]) if row["campaigns"] else ""
            lines.append(f"   :x: `{row['email']}` — {row['why']}{live}")
        for row in [x for x in r["rows"] if x["verdict"] == "CHECK"][:8]:
            lines.append(f"   :grey_question: `{row['email']}` — {row['why']}")
    lines.append("\n_Zapmail: *Retire* removes the inboxes at this bill (domain kept, can be undone before). "
                 "ScaledMail bills per order: retiring one domain means *Replace domain* or cancelling the order._")
    return "\n".join(lines)
