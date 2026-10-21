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
    setting_up = {e.lower() for e in bill.get("setting_up") or []}   # the provider is still creating these
    for email in bill.get("inboxes") or []:
        if email.lower() in setting_up:
            continue
        v, why = judge_inbox(email, facts.get(email), kills.get(email), today, complete)
        unused = v == "RETIRE" and not (facts.get(email) or {}).get("in_smartlead")
        rows.append({"email": email, "verdict": v, "why": why, "kind": "unused" if unused else "kill",
                     "campaigns": (facts.get(email) or {}).get("campaigns") or []})
    n = len(bill.get("inboxes") or []) or int(bill.get("mailboxes") or 0) or 1
    per_inbox = float(bill.get("price") or 0) / n if bill.get("per_inbox") is None else float(bill["per_inbox"])
    retire = [r for r in rows if r["verdict"] == "RETIRE"]
    out = {**bill, "rows": rows, "setting_up_count": len(setting_up),
            "counts": {k: sum(1 for r in rows if r["verdict"] == k) for k in ("KEEP", "RETIRE", "CHECK")},
            "retire_saves_monthly": round(per_inbox * len(retire), 2),
            "kill_saves_monthly": round(per_inbox * sum(1 for r in retire if r["kind"] == "kill"), 2),
            "unused_saves_monthly": round(per_inbox * sum(1 for r in retire if r["kind"] == "unused"), 2)}
    out["support_message"] = support_message(out)
    return out


EXPLAIN = ("_Zapmail: *Retire* removes the inboxes at this bill (domain kept, can be undone before). "
           "ScaledMail bills per order: retiring one domain means *Replace domain* or cancelling the order._")


def _money(v) -> str:
    return f"${float(v or 0):,.2f}"


def _day(iso: str) -> str:
    d = date.fromisoformat(iso)
    return f"{d.day} {d:%b}"


def _bill_name(r: dict) -> str:
    who = f" ({', '.join(r.get('clients') or [])})" if r.get("clients") else ""
    return f"Bill on {_day(r['bills_on'])} · {r['provider']} · {r['label']} · {_money(r.get('price'))}{who}"


NO_RECENT_TEST = ("no test with", "last counting test", "no placement test")


def _doms(rows: list[dict]) -> str:
    by: dict[str, int] = defaultdict(int)
    for x in rows:
        by[x["email"].split("@")[1]] += 1
    return ", ".join(f"{d} ({k} inbox{'es' if k != 1 else ''})" for d, k in sorted(by.items()))


def format_review(reviews: list[dict], days: int) -> tuple[list[str], list[str]]:
    """``(headline, parts)`` for ONE message: kill-list inboxes to drop before
    each bill (by domain), paid-for inboxes not in Smartlead, what to check
    (no-recent-test collapsed to one line per bill), the clean bills in one
    line each, then copy-ready support messages and how to retire. Inboxes
    the provider is still setting up are left out."""
    if not reviews:
        return [f"✅ Nothing bills in the next {days} days"], []
    total = sum(float(r.get("price") or 0) for r in reviews)
    n = len(reviews)
    kill = sum(1 for r in reviews for x in r["rows"] if x["verdict"] == "RETIRE" and x.get("kind") != "unused")
    unused = sum(1 for r in reviews for x in r["rows"] if x["verdict"] == "RETIRE" and x.get("kind") == "unused")
    checks = sum(r["counts"]["CHECK"] for r in reviews)
    save = sum(r["retire_saves_monthly"] for r in reviews)
    span = f"Bills in the next {days} days ({_money(total)}, {n} bill{'s' if n != 1 else ''})"
    ib = lambda k: f"inbox{'es' if k != 1 else ''}"        # noqa: E731
    if kill or unused:
        bits = ([f"drop {kill} kill-list {ib(kill)}"] if kill else []) \
            + ([f"{unused} paid-for {ib(unused)} not in Smartlead"] if unused else [])
        tail = (f"before they renew, saves {_money(save)}/mo" if not unused
                else f"saves up to {_money(save)}/mo")
        head = [f"⚠️ {span}: {', '.join(bits)}{' ' if not unused else ', '}{tail}"]
    elif checks:
        head = [f"⚠️ {span}: nothing on the kill list, {checks} {ib(checks)} to check first"]
    else:
        head = [f"✅ {span}: nothing on the kill list"]

    parts: list[str] = []
    drop, gone, check, clean = [], [], [], []
    for r in reviews:
        k_rows = [x for x in r["rows"] if x["verdict"] == "RETIRE" and x.get("kind") != "unused"]
        u_rows = [x for x in r["rows"] if x["verdict"] == "RETIRE" and x.get("kind") == "unused"]
        c_rows = [x for x in r["rows"] if x["verdict"] == "CHECK"]
        note = f" · {r['note']}" if r.get("note") else ""
        su = r.get("setting_up_count") or 0
        if su:
            note += f" · {su} {ib(su)} still being set up, left alone"
        if k_rows:
            line = f"• {_bill_name(r)}: drop {_doms(k_rows)}, saves {_money(r.get('kill_saves_monthly', 0))}/mo"
            live = [x for x in k_rows if x["campaigns"]]
            if live:
                line += " · ⚠️ " + "; ".join(f"{x['email']} is still in {', '.join(x['campaigns'][:2])}" for x in live)
            drop.append(line + note)
        if u_rows:
            gone.append(f"• {_bill_name(r)}: {_doms(u_rows)} paid for but not in any Smartlead account, saves "
                        f"{_money(r.get('unused_saves_monthly', 0))}/mo. Ask the provider to remove them, or add "
                        "them to Smartlead." + ("" if k_rows else note))
        stale = [x for x in c_rows if x["why"].startswith(NO_RECENT_TEST)]
        if stale:
            who = f" ({', '.join(r.get('clients') or [])})" if r.get("clients") else ""
            check.append(f"• Bill on {_day(r['bills_on'])}{who}: {len(stale)} {ib(len(stale))} "
                         f"{'has' if len(stale) == 1 else 'have'} no recent placement test")
        check += [f"• `{x['email']}` (bill {_day(r['bills_on'])}): {x['why']}" for x in c_rows if x not in stale]
        if not k_rows and not u_rows and not c_rows:
            clean.append(f"• {_bill_name(r)}{note}")
    for title, lines in (("*Drop before the bill*", drop), ("*Paying for inboxes not in Smartlead*", gone),
                         ("*Check before paying*", check), ("*Nothing to drop*", clean)):
        if lines:
            parts.append("\n".join([title] + lines))
    for r in reviews:
        if r.get("support_message"):
            parts.append(f"_Message for {r['provider']} support · {_day(r['bills_on'])} · {r['label']} (copy & send):_\n"
                         "```" + r["support_message"] + "```")
    parts.append(EXPLAIN + " _Retire them in `/domains` → Before the bill._")
    return head, parts


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
             + (f", ref {ref}" if ref else "") + f") renews on {_day(review['bills_on'])} {review['bills_on'][:4]}.",
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
