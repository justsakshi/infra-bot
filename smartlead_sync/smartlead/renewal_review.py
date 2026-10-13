"""Before every bill: which inboxes are worth paying for again?

Inboxes renew with their subscription (Zapmail) or order (ScaledMail) — the
card is charged and every inbox in it renews. On 2026-10-08 the team found
out at billing time that BettrData was paying for inboxes that had sat in
spam for weeks. This review runs before each bill and says, per inbox,
KEEP / RETIRE / CHECK, from what we actually know:

  RETIRE  "should kill" by the team's kill rule (smartlead/kill_rule.py: two
          strikes 7+ days apart, under 50% on 20+ seeds, or a dead domain
          pooled over 40+ seeds), or not in any Smartlead account
  CHECK   one strike (retest before paying again), no counting test in 21
          days, Smartlead connection broken, or warmup reputation under 70%
  KEEP    tested in the inbox recently and healthy

Pure functions (unit-tested); ``renewal_review.py`` collects and posts.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from smartlead import kill_rule as kr

MIN_REPUTATION = 70.0   # Smartlead warmup reputation


def _rep(v) -> float | None:
    try:
        return float(str(v).strip().rstrip("%"))
    except (TypeError, ValueError):
        return None


def judge_inbox(email: str, facts: dict | None, kill: dict | None, today: date,
                complete: bool = True) -> tuple[str, str]:
    """``(verdict, reason)`` for one inbox. ``facts`` from Smartlead +
    SmartDelivery: {in_smartlead, connected, reputation, tests, campaigns};
    ``kill`` is its kill_rule verdict (the team's two-strike rule)."""
    f = facts or {}
    if not f.get("in_smartlead"):
        # Only proof when every Smartlead account was read: a 429 on 2026-10-08
        # made all 39 Precise Leads inboxes look "unused".
        if not complete:
            return "CHECK", "Smartlead could not be read just now — no verdict"
        return "RETIRE", "not in any Smartlead account (paid for, never used)"
    k = kill or {"verdict": kr.NO_DATA, "why": "no placement test"}
    if k["verdict"] == kr.KILL:
        return "RETIRE", k["why"]
    if not f.get("connected", True):
        return "CHECK", "Smartlead connection broken"
    rep = _rep(f.get("reputation"))
    if rep is not None and rep < MIN_REPUTATION:
        return "CHECK", f"warmup reputation {rep:.0f}%"
    if k["verdict"] == kr.WATCH:
        return "CHECK", k["why"]
    if k["verdict"] == kr.NO_DATA:
        return "CHECK", k["why"]
    return "KEEP", k["why"]


def review_bill(bill: dict, facts: dict[str, dict], today: date, complete: bool = True) -> dict:
    """``bill`` = {provider, label, bills_on, price, inboxes:[emails], client(s), ...}."""
    # The kill rule pools a domain across ALL its known mailboxes, not just this bill's.
    kills = kr.judge_all({e: (f or {}).get("tests") or [] for e, f in facts.items()}, today)
    rows = []
    for email in bill.get("inboxes") or []:
        v, why = judge_inbox(email, facts.get(email), kills.get(email), today, complete)
        rows.append({"email": email, "verdict": v, "why": why,
                     "campaigns": (facts.get(email) or {}).get("campaigns") or []})
    n = len(bill.get("inboxes") or []) or int(bill.get("mailboxes") or 0) or 1
    per_inbox = float(bill.get("price") or 0) / n if bill.get("per_inbox") is None else float(bill["per_inbox"])
    retire = [r for r in rows if r["verdict"] == "RETIRE"]
    out = {**bill, "rows": rows,
            "counts": {k: sum(1 for r in rows if r["verdict"] == k) for k in ("KEEP", "RETIRE", "CHECK")},
            "retire_saves_monthly": round(per_inbox * len(retire), 2)}
    out["support_message"] = support_message(out)
    return out


EXPLAIN = ("_Zapmail: *Retire* removes the inboxes at this bill (domain kept, can be undone before). "
           "ScaledMail bills per order: retiring one domain means *Replace domain* or cancelling the order._")


def _money(v) -> str:
    return f"${float(v or 0):,.2f}"


def format_review(reviews: list[dict], days: int) -> tuple[list[str], list[str]]:
    """``(headline_lines, thread_parts)`` for notify.post_with_thread: the
    totals in the channel, one reply per bill (every inbox), then each
    copy-ready support message, then the how-to-retire note once."""
    if not reviews:
        return [f"✅ Nothing bills in the next {days} days"], []
    total = sum(float(r.get("price") or 0) for r in reviews)
    save = sum(r["retire_saves_monthly"] for r in reviews)
    n = len(reviews)
    c = {k: sum(r["counts"][k] for r in reviews) for k in ("KEEP", "RETIRE", "CHECK")}
    lead = f"Bills in the next {days} days: {_money(total)} across {n} bill{'s' if n != 1 else ''}"
    if c["RETIRE"]:
        head = [f"⚠️ {lead} · retiring the flagged inboxes saves {_money(save)}/mo"]
    elif c["CHECK"]:
        head = [f"⚠️ {lead}, nothing to retire, {c['CHECK']} inbox{'es' if c['CHECK'] != 1 else ''} to check"]
    else:
        head = [f"✅ {lead}, nothing to retire"]
    by_prov: dict[str, list[float]] = {}
    for r in reviews:
        by_prov.setdefault(r["provider"], []).append(float(r.get("price") or 0))
    head.append(" · ".join(f"{p} {_money(sum(v))} ({len(v)} bill{'s' if len(v) != 1 else ''})"
                           for p, v in by_prov.items()))
    head.append(f"{c['KEEP']} keep · {c['RETIRE']} retire · {c['CHECK']} check")

    thread: list[str] = []
    for r in reviews:
        k = r["counts"]
        lines = [f"*{r['bills_on']} · {r['provider']} · {r['label']}* — {_money(r.get('price'))}"
                 + (f" · {', '.join(r.get('clients') or [])}" if r.get("clients") else ""),
                 f"{len(r['rows'])} inboxes: {k['KEEP']} keep · *{k['RETIRE']} retire* · {k['CHECK']} check"
                 + (f" · retiring saves {_money(r['retire_saves_monthly'])}/mo" if k["RETIRE"] else "")]
        if r.get("note"):
            lines.append(f"_{r['note']}_")
        for row in [x for x in r["rows"] if x["verdict"] == "RETIRE"]:
            live = " :warning: in " + ", ".join(row["campaigns"][:2]) if row["campaigns"] else ""
            lines.append(f":x: `{row['email']}` — {row['why']}{live}")
        for row in [x for x in r["rows"] if x["verdict"] == "CHECK"]:
            lines.append(f":grey_question: `{row['email']}` — {row['why']}")
        keep = [x["email"] for x in r["rows"] if x["verdict"] == "KEEP"]
        if keep:
            lines.append(f":white_check_mark: keep ({len(keep)}): " + ", ".join(keep))
        thread.append("\n".join(lines))
    for r in reviews:
        if r.get("support_message"):
            thread.append(f"_Message for {r['provider']} support · {r['bills_on']} · {r['label']} (copy & send):_\n"
                          "```" + r["support_message"] + "```")
    thread.append(EXPLAIN)
    return head, thread


def support_message(review: dict) -> str:
    """A ready-to-send note to the provider's support for the RETIRE inboxes on
    this bill: what is not delivering (with the test evidence), what to keep,
    and the ask - replace them / take them off the order and do not charge for
    them this cycle. Empty when nothing is flagged."""
    bad = [r for r in review.get("rows") or [] if r["verdict"] == "RETIRE"]
    if not bad:
        return ""
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for r in bad:
        by_dom[r["email"].split("@")[1]].append(r)
    keep = sorted({r["email"].split("@")[1] for r in review.get("rows") or [] if r["verdict"] == "KEEP"} - set(by_dom))
    ref = review.get("order_id") or review.get("subscription_id") or ""
    lines = [f"Hi {review['provider']} team,", "",
             f"Our {review['label']} ({', '.join(review.get('clients') or []) or 'our account'}"
             + (f", ref {ref}" if ref else "") + f") renews on {review['bills_on']}.",
             f"{len(bad)} of its inboxes are not delivering — our inbox-placement tests put them in spam:", ""]
    for dom in sorted(by_dom):
        whys = sorted({r["why"] for r in by_dom[dom]})
        lines.append(f"• {dom} ({len(by_dom[dom])} inboxes) — {'; '.join(whys)}")
    lines += ["", "Could you please replace these inboxes (fresh domains) or remove them from the order, "
              "and not charge us for them for this billing cycle?"]
    if keep:
        lines.append(f"Please keep the rest as they are ({', '.join(keep)}).")
    lines += ["", "Thanks!"]
    return "\n".join(lines)
