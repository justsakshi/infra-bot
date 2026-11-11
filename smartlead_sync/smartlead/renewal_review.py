"""Before every bill: which inboxes are worth paying for again?

Inboxes renew with their subscription (Zapmail) or order (ScaledMail) — the
card is charged and every inbox in it renews. On 2026-10-08 the team found
out at billing time that BettrData was paying for inboxes that had sat in
spam for weeks. This review runs before each bill and says, per inbox,
KEEP / DROP / CHECK / UNTESTED, from what we actually know (rule: Manveen's
decision 4, 7 Nov 2026; until then every inbox came out CHECK because only
classic tests were read):

  DROP      (code "RETIRE") only for a Smart Delivery v2 FAIL on two weekly
            nights with no pass since, the newer of them in the last 8 days,
            or a person's DO NOT USE tag in Smartlead. A DROP flag can lead a
            person to cancel, so one bad test, or old tests, never make one.
  CHECK     one v2 FAIL (or a WATCH, below 4 of 4) with no pass since, two
            weekly FAILs older than 8 days (it may have been fixed or rested
            since), a pull from campaigns less than 30 days old, still
            carrying the v2 job's own DO NOT USE tag, Smartlead connection
            broken, warmup reputation under 70%, a source that could not be
            read, or not in Smartlead (new and not connected yet, or
            abandoned: a person decides).
  UNTESTED  no v2 result in the last 8 days: never KEEP without a test.
  KEEP      only a v2 PASS in the last 8 days and none of the above.

Classic (v1) seed tests are an early warning only: they add a WATCH note to
the row, never a verdict, so a failed classic read only loses the notes. The
v2 job's auto tag ("DO NOT USE - failed v2
(auto)") marks a pull after ONE failed test, so it counts as a pull (CHECK),
not as a person's DO NOT USE.

A Spamhaus DBL listing of the domain is a note on the row, never a verdict
(Manveen, 11 Nov 2026). On 10 Nov a listed Outlook domain put 96 of 96 Google
and 95 of 96 Microsoft seeds in the inbox on v2, and 25 of that week's 46
CHECK rows were CHECK for the listing alone. The v2 result decides; the
listing is shown so a person can weigh it.

Pure functions (unit-tested); ``renewal_review.py`` collects and posts.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from smartlead import kill_rule as kr
from smartlead.v2_results import FAIL, JUDGED, PASS, WATCH, is_weekly_night

MIN_REPUTATION = 70.0   # Smartlead warmup reputation
NOT_IN_SMARTLEAD = "not in Smartlead: connect it if it's new, or drop it if nobody will use it"
V2_FRESH_DAYS = 8       # a v2 result older than this does not count as tested
PULL_CHECK_DAYS = 30    # a pull younger than this is still CHECK
DNU = "do not use"      # gtm presend refuses any tag starting with this
AUTO_DNU_TAG = "DO NOT USE - failed v2 (auto)"     # v2_weekly.TAG: the v2 job's own pull tag
UNTESTED = "UNTESTED"
VERDICTS = ("KEEP", "CHECK", "RETIRE", "UNTESTED")
NO_V2 = f"no v2 result in the last {V2_FRESH_DAYS} days"
V2_DROP = "failed v2 on the weekly tests of "     # + "26 Oct and 2 Nov"


def _rep(v) -> float | None:
    try:
        return float(str(v).strip().rstrip("%"))
    except (TypeError, ValueError):
        return None


def _iso(v) -> date | None:
    try:
        return date.fromisoformat(str(v or "")[:10])
    except ValueError:
        return None


def _day(iso: str) -> str:
    d = date.fromisoformat(str(iso)[:10])
    return f"{d.day} {d:%b}"


def is_dbl(listed_on) -> bool:
    """A blacklist_checks zone list (``listed_on``, ``lookup_errors``, ``inconclusive_zones``)
    that includes Spamhaus DBL."""
    if not isinstance(listed_on, (list, tuple)):
        return False
    return any("spamhaus" in str(s).lower() and "dbl" in str(s).lower() for s in listed_on)


def _dbl_unread(row: dict) -> bool:
    """This blacklist_checks row could not see Spamhaus DBL: the lookup failed, the zone's
    control probe failed (blacklist_monitor puts the label in lookup_errors / inconclusive_zones),
    or listed_on is not a list. A listing it did see still counts."""
    if is_dbl(row.get("listed_on")):
        return False
    return (not isinstance(row.get("listed_on"), list) or is_dbl(row.get("lookup_errors"))
            or is_dbl(row.get("inconclusive_zones")))


def dbl_status(rows: list[dict]) -> dict:
    """One domain's blacklist_checks rows -> ``{date, listed_on, unknown_on}``.

    ``date`` / ``listed_on``: the newest row that could read Spamhaus DBL (None / [] when no row
    could). ``unknown_on``: the newest row's date when that row could not read DBL, else None.
    An unread check is never clean (audit BLOCKLIST-4): judge_inbox then falls back to the newest
    readable check, and with none, or a listing there, the inbox is CHECK."""
    rows = sorted((r for r in rows or [] if isinstance(r, dict)), key=lambda r: str(r.get("date") or ""))
    if not rows:
        return {}
    seen = [r for r in rows if not _dbl_unread(r)]
    return {"date": str(seen[-1].get("date") or "") if seen else None,
            "listed_on": [str(x) for x in seen[-1].get("listed_on") or []] if seen else [],
            "unknown_on": str(rows[-1].get("date") or "") if _dbl_unread(rows[-1]) else None}


def dbl_note(dbl: dict | None) -> str:
    """The row's note when the newest check that could read Spamhaus DBL listed the domain
    ('' otherwise). A note only: it never changes the verdict."""
    if not dbl or not is_dbl(dbl.get("listed_on")):
        return ""
    return dbl_reason(dbl)


def dbl_reason(dbl: dict) -> str:
    """What the domain's blocklist state says in words ('' when the newest readable check is clean)."""
    listed, unknown = is_dbl(dbl.get("listed_on")), dbl.get("unknown_on")
    seen = f"the check of {_day(dbl['date'])}" if _iso(dbl.get("date")) else "an earlier check"
    if unknown is not None:
        when = f"the check of {_day(unknown)}" if _iso(unknown) else "the newest check"
        if listed:
            return f"Spamhaus DBL could not be read in {when}; {seen} listed the domain"
        if not dbl.get("date"):
            return f"Spamhaus DBL could not be read in {when}, and no earlier check read it"
        return ""          # the newest check that could read DBL found the domain clean
    if listed:
        return (f"domain on Spamhaus DBL (check of {_day(dbl['date'])})" if _iso(dbl.get("date"))
                else "domain on Spamhaus DBL")
    return ""


def v2_summary(docs: list[dict], today: date) -> dict:
    """What the v2_verdicts rows of one inbox say. Rows that are not PASS / WATCH / FAIL
    (INCOMPLETE, NOT_SENT, UNKNOWN) or have no readable night are left out.

    ``fails``: FAIL nights after the newest PASS; ``weekly_fails``: the two nights when the two
    newest weekly-night results after the newest PASS are both FAIL, else []; ``latest_recent``:
    the newest result from the last V2_FRESH_DAYS days (or None); ``latest``: the newest result
    of any age (or None); ``pulled_on``: the newest pull."""
    judged = []
    for d in docs or []:
        night = _iso(d.get("night"))
        verdict = str(d.get("verdict") or "").upper()
        if night and verdict in JUDGED:
            weekly = d.get("weekly")
            if not isinstance(weekly, bool):
                weekly = is_weekly_night(night.isoformat())
            judged.append({"night": night, "verdict": verdict, "weekly": weekly})
    judged.sort(key=lambda x: x["night"])
    passes = [i for i, x in enumerate(judged) if x["verdict"] == PASS]
    after = judged[passes[-1] + 1:] if passes else judged
    weekly = [x for x in after if x["weekly"]][-2:]
    recent = [x for x in judged if (today - x["night"]).days <= V2_FRESH_DAYS]
    pulls = [p for p in (_iso(d.get("pulled_on")) for d in docs or []) if p]
    return {"fails": [x["night"] for x in after if x["verdict"] == FAIL],
            "weekly_fails": [x["night"] for x in weekly] if len(weekly) == 2
            and all(x["verdict"] == FAIL for x in weekly) else [],
            "latest_recent": recent[-1] if recent else None,
            "latest": judged[-1] if judged else None,
            "pulled_on": max(pulls) if pulls else None}


def judge_inbox(email: str, facts: dict | None, kill: dict | None, today: date,
                complete: bool = True, v2: list[dict] | None = None,
                dbl: dict | None = None) -> tuple[str, str]:
    """``(verdict, reason)`` for one inbox.

    ``facts``: Smartlead {in_smartlead, connected, reputation, tags, ...}. ``v2``: this inbox's
    v2_verdicts rows ([] = none; None = the collection could not be read). ``dbl`` (its domain's
    dbl_status) and ``kill`` (the classic kill rule) are not used here: a Spamhaus listing and
    classic tests only add a note to the row (review_bill)."""
    f = facts or {}
    if not f.get("in_smartlead"):
        # Only proof when every Smartlead account was read: a 429 on 2026-10-08
        # made all 39 Precise Leads inboxes look "unused".
        if not complete:
            return "CHECK", "Smartlead could not be read just now — no verdict"
        return "CHECK", NOT_IN_SMARTLEAD
    tags = [str(t).strip() for t in f.get("tags") or [] if str(t).strip()]
    own = [t for t in tags if t.lower().startswith(DNU) and t.lower() != AUTO_DNU_TAG.lower()]
    if own:
        return "RETIRE", f'tagged "{own[0]}" in Smartlead'
    s = v2_summary(v2, today) if v2 is not None else None
    stale = ""                  # two weekly fails, the newer too old to cancel on
    none_since = False
    if s and s["weekly_fails"]:
        a, b = s["weekly_fails"]
        pair = f"{V2_DROP}{_day(a.isoformat())} and {_day(b.isoformat())}"
        if (today - b).days <= V2_FRESH_DAYS:
            return "RETIRE", pair
        # Old fails never cancel an inbox: it may have been fixed or rested since (review R1).
        none_since = s["latest"]["night"] == b
        stale = pair + (", no v2 result since" if none_since else ", no pass since")
    why: list[str] = []
    pulled = s and s["pulled_on"] and (today - s["pulled_on"]).days < PULL_CHECK_DAYS
    if stale:
        why.append(stale)
    elif s and s["fails"]:
        why.append(f"failed v2 on {_day(s['fails'][-1].isoformat())}, no pass since")
    elif s and s["latest_recent"] and s["latest_recent"]["verdict"] == WATCH:
        why.append(f"v2 below 4 of 4 on {_day(s['latest_recent']['night'].isoformat())}")
    if pulled:
        why.append(f"taken out of campaigns on {_day(s['pulled_on'].isoformat())} after v2, "
                   f"under {PULL_CHECK_DAYS} days ago")
    if not pulled and any(t.lower() == AUTO_DNU_TAG.lower() for t in tags):
        why.append("still tagged DO NOT USE by the v2 job, waiting for two passes")
    if not f.get("connected", True):
        why.append("Smartlead connection broken")
    rep = _rep(f.get("reputation"))
    if rep is not None and rep < MIN_REPUTATION:
        why.append(f"warmup reputation {rep:.0f}%")
    if why:
        if s is not None and not s["latest_recent"] and not none_since:
            why.append(NO_V2)
        return "CHECK", "; ".join(why)
    if s is None:
        return "CHECK", "v2 results could not be read, so not kept"
    if not s["latest_recent"]:
        return UNTESTED, NO_V2
    return "KEEP", f"passed v2 on {_day(s['latest_recent']['night'].isoformat())}"


def price_of(bill: dict) -> float | None:
    """The bill's price, or None when it is not known (missing, unreadable or zero)."""
    try:
        p = float(bill.get("price"))
    except (TypeError, ValueError):
        return None
    return p if p > 0 else None


def review_bill(bill: dict, facts: dict[str, dict], today: date, complete: bool = True,
                v2: dict[str, list[dict]] | None = None, dbl: dict[str, dict] | None = None) -> dict:
    """``bill`` = {provider, label, bills_on, price, inboxes:[emails], client(s), ...}.
    ``v2`` {email: v2_verdicts rows}; None means it could not be read, so nothing on the bill is
    KEEP. ``dbl`` {domain: dbl_status}; a listing adds the row's ``listed`` note and nothing else
    (None = not read: no notes)."""
    # The kill rule pools a domain across ALL its known mailboxes, not just this bill's.
    kills = kr.judge_all({e: (f or {}).get("tests") or [] for e, f in facts.items()}, today)
    rows = []
    setting_up = {e.lower() for e in bill.get("setting_up") or []}   # the provider is still creating these
    for email in _as_in_smartlead(bill.get("inboxes") or [], facts):
        if email.lower() in setting_up:
            continue
        docs = None if v2 is None else v2.get(email.lower()) or []
        bl = None if dbl is None else dbl.get(email.lower().split("@")[-1]) or {}
        v, why = judge_inbox(email, facts.get(email), kills.get(email), today, complete, docs, bl)
        unused = complete and not (facts.get(email) or {}).get("in_smartlead")
        latest = v2_summary(docs, today)["latest_recent"] if docs else None
        row = {"email": email, "verdict": v, "why": why, "kind": "unused" if unused else "kill",
               "campaigns": (facts.get(email) or {}).get("campaigns") or [],
               "passed_v2": bool(latest and latest["verdict"] == PASS)}
        k = kills.get(email) or {}
        if k.get("verdict") in (kr.KILL, kr.WATCH):          # early warning only, never the verdict
            row["watch"] = f"classic test: {k['why']}"
        if dbl_note(bl):                                      # a note, never the verdict
            row["listed"] = dbl_note(bl)
        rows.append(row)
    n = len(bill.get("inboxes") or []) or int(bill.get("mailboxes") or 0) or 1
    per_inbox = float(bill.get("price") or 0) / n if bill.get("per_inbox") is None else float(bill["per_inbox"])
    retire = [r for r in rows if r["verdict"] == "RETIRE"]
    passing = sum(1 for r in rows if r["passed_v2"])
    price = price_of(bill)
    out = {**bill, "rows": rows, "setting_up_count": len(setting_up),
           "reviewed": bool(bill.get("inboxes")), "v2_read": v2 is not None,
           "counts": {k: sum(1 for r in rows if r["verdict"] == k) for k in VERDICTS},
           "passing": passing,
           "cost_per_passing": round(price / passing, 2) if price and passing else None,
           "retire_saves_monthly": round(per_inbox * len(retire), 2),
           "kill_saves_monthly": round(per_inbox * sum(1 for r in retire if r["kind"] == "kill"), 2),
           "unused_saves_monthly": round(per_inbox * sum(1 for r in rows if r["kind"] == "unused"), 2)}
    out["support_message"] = support_message(out)
    return out


def _as_in_smartlead(inboxes: list[str], facts: dict[str, dict]) -> list[str]:
    """The bill's inboxes, with a renamed domain swapped for what Smartlead holds.

    ScaledMail kept listing one old address x25 on a client domain after the mailboxes
    were re-created under new names; Smartlead had the 25 new ones, so every
    listed address looked unused (3 Nov 2026). When none of a domain's listed
    addresses is in Smartlead but other inboxes on that domain are, judge those."""
    in_sl: dict[str, list[str]] = defaultdict(list)
    for e, f in facts.items():
        if (f or {}).get("in_smartlead"):
            in_sl[e.lower().split("@")[-1]].append(e)
    by_dom: dict[str, list[str]] = defaultdict(list)
    for e in inboxes:
        by_dom[e.lower().split("@")[-1]].append(e)
    out: list[str] = []
    for dom, listed in by_dom.items():
        if in_sl.get(dom) and not any(e in facts and (facts[e] or {}).get("in_smartlead") for e in listed):
            out += sorted(in_sl[dom])
        else:
            out += listed
    return out


EXPLAIN = ("_Zapmail: *Retire* removes the inboxes at this bill (domain kept, can be undone before). "
           "ScaledMail: cancel single domains from its billing page, not the whole order._")


def _money(v) -> str:
    return f"${float(v or 0):,.2f}"


def _bill_name(r: dict) -> str:
    who = f" ({', '.join(r.get('clients') or [])})" if r.get("clients") else ""
    p = price_of(r)
    return (f"Bill on {_day(r['bills_on'])} · {r['provider']} · {r['label']} · "
            f"{_money(p) if p is not None else 'price not known'}{who}")


def _ib(k: int) -> str:
    return f"inbox{'es' if k != 1 else ''}"


def _doms(rows: list[dict]) -> str:
    by: dict[str, int] = defaultdict(int)
    for x in rows:
        by[x["email"].split("@")[1]] += 1
    return ", ".join(f"{d} ({k} {_ib(k)})" for d, k in sorted(by.items()))


def _doms_why(rows: list[dict]) -> str:
    """'x.com (3 inboxes: reason); y.com (1 inbox: reason)'."""
    by: dict[str, list[dict]] = defaultdict(list)
    for x in rows:
        by[x["email"].split("@")[1]].append(x)
    return "; ".join(f"{d} ({len(xs)} {_ib(len(xs))}: {'; '.join(sorted({x['why'] for x in xs}))})"
                     for d, xs in sorted(by.items()))


def _grouped(r: dict, rows: list[dict], key: str) -> list[str]:
    """One line per reason within a bill: a single inbox by address, several by domain."""
    who = f" ({', '.join(r.get('clients') or [])})" if r.get("clients") else ""
    groups: dict[str, list[dict]] = defaultdict(list)
    for x in rows:
        groups[x[key]].append(x)
    out = []
    for why, xs in groups.items():
        if len(xs) == 1:
            out.append(f"• `{xs[0]['email']}` (bill {_day(r['bills_on'])}): {why}")
        else:
            out.append(f"• Bill on {_day(r['bills_on'])}{who}: {_doms(xs)}: {why}")
    return out


def _bill_line(r: dict) -> str:
    """One bill: price, the four counts, cost per passing inbox."""
    if not r.get("reviewed", True):
        return f"• {_bill_name(r)}: not reviewed (no inbox list)"
    c = r.get("counts") or {}
    line = (f"• {_bill_name(r)}: {c.get('KEEP', 0)} keep, {c.get('CHECK', 0)} check, "
            f"{c.get('RETIRE', 0)} drop, {c.get(UNTESTED, 0)} untested")
    su = r.get("setting_up_count") or 0
    if su:
        line += f", {su} still being set up"
    passing = r.get("passing") or 0
    if not r.get("v2_read", True):
        line += " · v2 results not read"
    elif passing and r.get("cost_per_passing") is not None:
        line += (f" · {_money(r['cost_per_passing'])} per passing inbox "
                 f"({passing} passed v2 in the last {V2_FRESH_DAYS} days)")
    elif passing:
        line += f" · {passing} passed v2 in the last {V2_FRESH_DAYS} days"
    elif r.get("rows"):
        line += f" · none passed v2 in the last {V2_FRESH_DAYS} days"
    return line


def format_review(reviews: list[dict], days: int) -> tuple[list[str], list[str]]:
    """``(headline, thread parts)``. Headline: the bills' total and how many inboxes to
    drop / check / leave untested / keep (they add up to the inboxes judged). Thread: one
    line per bill (price, counts, cost per passing inbox), then what to drop (by domain,
    with the reason), paid-for inboxes not in Smartlead, what to check, what is untested,
    bills not reviewed, classic early warnings, copy-ready support messages and how to
    retire, with Spamhaus listings as a note. Inboxes the provider is still setting up are left out."""
    if not reviews:
        return [f"✅ Nothing bills in the next {days} days"], []
    rows = [x for r in reviews for x in r["rows"]]
    count = {k: sum(1 for x in rows if x["verdict"] == k) for k in VERDICTS}
    unused = sum(1 for x in rows if x.get("kind") == "unused")
    unreviewed = [r for r in reviews if not r.get("reviewed", True)]
    prices = [price_of(r) for r in reviews]
    total = sum(p for p in prices if p is not None)
    unknown = sum(1 for p in prices if p is None)
    n = len(reviews)
    save = sum(r["retire_saves_monthly"] for r in reviews)
    money = _money(total) + (f" + {unknown} price{'s' if unknown != 1 else ''} not known" if unknown else "")
    bad = count["RETIRE"] or count["CHECK"] or count[UNTESTED] or unreviewed
    head = [f"{'⚠️' if bad else '✅'} Bills in the next {days} days: {n} bill{'s' if n != 1 else ''}, {money}"]
    if rows:
        bits = [(f"{count['RETIRE']} to drop before they renew" + (f" (saves {_money(save)}/mo)" if save else ""))
                if count["RETIRE"] else "nothing to drop"]
        if count["CHECK"]:
            bits.append(f"{count['CHECK']} to check" + (f" ({unused} of them paid for but not in Smartlead)"
                                                        if unused else ""))
        if count[UNTESTED]:
            bits.append(f"{count[UNTESTED]} untested")
        bits.append(f"{count['KEEP']} to keep")
        head.append(f"Of {len(rows)} {_ib(len(rows))}: " + ", ".join(bits))
    if unreviewed:
        k = len(unreviewed)
        cost = sum(price_of(r) or 0 for r in unreviewed)
        head.append(f"{k} bill{'s' if k != 1 else ''} ({_money(cost)}) not reviewed: no inbox list")

    parts: list[str] = ["\n".join(["*Each bill*"] + [_bill_line(r) for r in reviews])]
    drop, gone, check, untested, notrev, watch, listed = [], [], [], [], [], [], []
    for r in reviews:
        k_rows = [x for x in r["rows"] if x["verdict"] == "RETIRE"]
        u_rows = [x for x in r["rows"] if x.get("kind") == "unused"]
        c_rows = [x for x in r["rows"] if x["verdict"] == "CHECK" and x.get("kind") != "unused"]
        t_rows = [x for x in r["rows"] if x["verdict"] == UNTESTED]
        note = f" · {r['note']}" if r.get("note") else ""
        su = r.get("setting_up_count") or 0
        if su:
            note += f" · {su} {_ib(su)} still being set up, left alone"
        if k_rows:
            line = f"• {_bill_name(r)}: drop {_doms_why(k_rows)}"
            if r.get("kill_saves_monthly"):
                line += f", saves {_money(r['kill_saves_monthly'])}/mo"
            live = [x for x in k_rows if x["campaigns"]]
            if live:
                line += " · ⚠️ " + "; ".join(f"{x['email']} is still in {', '.join(x['campaigns'][:2])}" for x in live)
            drop.append(line + note)
        if u_rows:
            gone.append(f"• {_bill_name(r)}: {_doms(u_rows)} paid for but not in Smartlead "
                        f"({_money(r.get('unused_saves_monthly', 0))}/mo). Connect them if they're new; if nobody "
                        "will use them, ask the provider to remove them." + ("" if k_rows else note))
        check += _grouped(r, c_rows, "why")
        if t_rows:
            who = f" ({', '.join(r.get('clients') or [])})" if r.get("clients") else ""
            untested.append(f"• Bill on {_day(r['bills_on'])}{who}: {_doms(t_rows)}")
        if not r.get("reviewed", True):
            owner = "" if r.get("clients") else " · no client named"
            notrev.append(f"• {_bill_name(r)}: no inbox list, so no inbox was judged{note}{owner}")
        watch += _grouped(r, [x for x in r["rows"] if x.get("watch") and x["verdict"] != "RETIRE"], "watch")
        listed += _grouped(r, [x for x in r["rows"] if x.get("listed")], "listed")
    for title, lines in (("*Drop before the bill*", drop), ("*Paying for inboxes not in Smartlead*", gone),
                         ("*Check before paying*", check),
                         (f"*Untested: no v2 result in the last {V2_FRESH_DAYS} days, so not kept*", untested),
                         ("*Not reviewed*", notrev),
                         ("*Classic tests (early warning only, never a verdict)*", watch),
                         ("*On Spamhaus DBL (a note only: the v2 result decides keep, check or drop)*", listed)):
        if lines:
            parts.append("\n".join([title] + lines))
    for r in reviews:
        if r.get("support_message"):
            parts.append(f"_Message for {r['provider']} support · {_day(r['bills_on'])} · {r['label']} (copy & send):_\n"
                         "```" + r["support_message"] + "```")
    parts.append(EXPLAIN + " _Retire them in `/domains` → Before the bill._")
    return head, parts


CLASSIC_SOURCE = "classic tests"       # SmartDelivery v1 reports: notes only
CAMPAIGNS_SOURCE = "campaign lists"    # which campaigns an inbox is in: notes only
BLOCKLIST_SOURCE = "blocklist checks"  # Spamhaus DBL listings: notes only (11 Nov 2026)
NOTE_ONLY = (CLASSIC_SOURCE, CAMPAIGNS_SOURCE, BLOCKLIST_SOURCE)
READ_EFFECT = (("Zapmail", "a bill may be missing"), ("ScaledMail", "a bill may be missing"),
               ("Smartlead", "some inboxes have no verdict"), ("v2 results", "no inbox is marked keep"),
               (BLOCKLIST_SOURCE, "Spamhaus listing notes are missing"),
               (CLASSIC_SOURCE, "classic early warnings are missing"),
               (CAMPAIGNS_SOURCE, "an inbox to drop may still be in a campaign without a warning"))


def decision_errors(errors: list[str]) -> list[str]:
    """The read errors that can change a verdict: all but the note-only sources (classic tests,
    campaign lists, blocklist checks), which never decide KEEP / CHECK / DROP / UNTESTED."""
    return [e for e in errors or [] if not str(e).startswith(NOTE_ONLY)]


def read_warning(errors: list[str], days: int, reviewed: bool) -> str:
    """One true line on what an unread source means for this post. ``errors`` start with the
    source's name (renewal_review.py adds it)."""
    names = [name for name, _ in READ_EFFECT if any(e.startswith(name) for e in errors)] or ["a source"]
    effects = list(dict.fromkeys(eff for name, eff in READ_EFFECT if name in names))
    joined = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    if not reviewed:
        return f"⚠️ Renewal review: could not read {joined}, so a bill in the next {days} days may be missing"
    return f":warning: Could not read {joined}, so {' and '.join(effects) or 'some verdicts are CHECK'} (details in the thread)"


def support_message(review: dict) -> str:
    """A ready-to-send note to the provider's support for the inboxes we drop: what is not
    working (v2 tests failed, or we marked them do not use), what to keep, and the ask -
    replace them / take them off the order and do not charge for them this cycle. Empty when
    nothing is dropped. Inboxes not in Smartlead get no note: the bot cannot tell a new inbox
    from an abandoned one (3 Nov 2026)."""
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
             "", f"{len(bad)} of its inboxes {'is' if len(bad) == 1 else 'are'} not working for us:", ""]
    for dom in sorted(by_dom):
        n = len(by_dom[dom])
        whys = []
        for r in by_dom[dom]:
            if r["why"].startswith(V2_DROP):
                whys.append("failed our inbox-placement tests of " + r["why"][len(V2_DROP):])
            else:
                whys.append("we no longer use them")
        lines.append(f"• {dom} ({n} {'inbox' if n == 1 else 'inboxes'}) — {'; '.join(sorted(set(whys)))}")
    lines += ["", "Could you please replace these inboxes (fresh domains) or remove them from the order, "
              "and not charge us for them for this billing cycle?"]
    if keep:
        lines.append(f"Please keep the rest as they are ({', '.join(keep)}).")
    lines += ["", "Thanks!"]
    return "\n".join(lines)
