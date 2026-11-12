"""Rules review: do the pipeline's rules still match what happens to real mail? Pure: no network, no database.

The weekly job adapts what it does with each inbox (out on a fail, back after two passes). Its RULES are fixed
numbers, and a fixed rule can be wrong for months without anyone seeing it (classic tests and warm-up said
"healthy" while every Google inbox sat in spam; then v2 failed Google inboxes that real people were answering).
This review holds each rule up against an outcome it did not use, and says: holds, in doubt, or not enough data.

It never changes a rule. A rule "in doubt" comes with a proposal, and a proposal needs a person's yes.

Rules reviewed:
  * the test decides (an inbox is judged on its placement test at its own provider), checked against the daily
    reply check of the same domain: a group where the test fails domains that get replies at the normal rate, or
    passes domains that get far fewer, is in doubt;
  * back after two passes, checked against what the returned inboxes did next: failing again soon after coming
    back puts it in doubt.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from smartlead import reply_stats as rs

JUDGED = ("PASS", "WATCH", "FAIL")
TEST_DAYS = 21          # an inbox's newest judged test within this many days is its test result
REPLY_DAYS = 3          # a reply-check day older than this is not used
MIN_DOMAINS = 4         # a group with fewer domains that have both a test result and a reply view is not judged
MIN_RETURNS = 5         # the return rule is not judged on fewer returned inboxes than this
RETURN_GAP = 6          # two passes this many days apart bring an inbox back (v2_weekly's rule)
HOLDS, DOUBT, THIN = "holds", "in doubt", "not enough data yet"
_TYPE = {"google": "Google", "gmail": "Google", "googleworkspace": "Google", "outlook": "Outlook",
         "microsoft": "Outlook", "microsoft365": "Outlook", "smtp": "SMTP"}
UNKNOWN_TYPE = "unknown type"


def _key(text) -> str:
    return "".join(ch for ch in str(text or "").lower() if ch.isalnum())


def inbox_types(assets: list[dict]) -> dict[str, str]:
    """{email: "Google" | "Outlook" | "SMTP"} from the tracker's INBOX rows (``name``, ``workspace``)."""
    out = {}
    for a in assets or []:
        typ = _TYPE.get(_key(a.get("workspace")))
        name = str(a.get("name") or "").strip().lower()
        if str(a.get("type") or "").upper() == "INBOX" and typ and "@" in name:
            out[name] = typ
    return out


def _judged(rows: list[dict]) -> dict[str, list[tuple[str, str, str]]]:
    """{email: [(night, verdict, client)] oldest first}, judged results only."""
    by: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for r in rows or []:
        email, night, v = str(r.get("email") or "").strip().lower(), str(r.get("night") or "")[:10], str(r.get("verdict") or "").upper()
        if "@" in email and night and v in JUDGED:
            by[email].append((night, v, str(r.get("client") or "")))
    return {e: sorted(rs) for e, rs in by.items()}


def domain_tests(v2_rows: list[dict], types: dict[str, str], today: date, days: int = TEST_DAYS) -> dict[tuple, dict]:
    """{(client, type, domain): {"pass", "watch", "fail", "night"}} counting each inbox's newest judged result
    within ``days``. The stored verdict is the own-provider one (v2_weekly publishes it)."""
    floor = (today - timedelta(days=days)).isoformat()
    out: dict[tuple, dict] = defaultdict(lambda: {"pass": 0, "watch": 0, "fail": 0, "night": ""})
    for email, rs in _judged(v2_rows).items():
        night, v, client = rs[-1]
        if not floor <= night <= today.isoformat():
            continue
        c = out[(client, types.get(email, UNKNOWN_TYPE), email.split("@")[-1])]
        c[v.lower()] += 1
        c["night"] = max(c["night"], night)
    return dict(out)


def domain_test(c: dict) -> str:
    """FAIL when half or more of the domain's judged inboxes fail (the job's own domain rule), PASS when all pass,
    MIXED otherwise."""
    n = c["pass"] + c["watch"] + c["fail"]
    return "FAIL" if n and c["fail"] * 2 >= n else "PASS" if n and c["pass"] == n else "MIXED"


def reply_views(reply_docs: list[dict], today: date, days: int = REPLY_DAYS) -> dict[str, dict]:
    """{domain: {"view": "OK" | "ALERT", "sent", "replies", "date"}} from each domain's newest stored reply-check
    day within ``days``. "OK" only for a domain that is answered like its peers (reply_stats.answered); OK on
    thin numbers, watch, too few sends, no reference and no sends are no view."""
    floor, newest = (today - timedelta(days=days)).isoformat(), {}
    for d in reply_docs or []:
        dom, day = str(d.get("domain") or "").lower(), str(d.get("date") or "")[:10]
        if dom and floor <= day <= today.isoformat() and day >= newest.get(dom, {}).get("date", ""):
            view = "OK" if rs.answered(d) else "ALERT" if str(d.get("verdict") or "") == rs.ALERT else ""
            newest[dom] = {"view": view, "sent": int(d.get("window_sent") or 0),
                           "replies": int(d.get("window_replies") or 0), "date": day}
    return {dom: v for dom, v in newest.items() if v["view"] in ("OK", "ALERT")}


def test_vs_replies(tests: dict[tuple, dict], views: dict[str, dict]) -> list[dict]:
    """One row per tested domain: what the test says and what real replies say (None = no view)."""
    rows = []
    for (client, typ, dom), c in sorted(tests.items()):
        v = views.get(dom) or {}
        rows.append({"client": client, "type": typ, "domain": dom, "test": domain_test(c), "night": c["night"],
                     "inboxes": c["pass"] + c["watch"] + c["fail"], "replies_view": v.get("view"),
                     "sent": v.get("sent"), "replies": v.get("replies")})
    return rows


def judge_test_rule(rows: list[dict]) -> list[dict]:
    """The rule "the test decides", per client and inbox type. -> [{group, state, evidence, proposal, counts}]."""
    by: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by[(r["client"], r["type"])].append(r)
    out = []
    for (client, typ), rs in sorted(by.items()):
        both = [r for r in rs if r["test"] in ("PASS", "FAIL") and r["replies_view"]]
        fail = [r for r in both if r["test"] == "FAIL"]
        ok = [r for r in both if r["test"] == "PASS"]
        fail_answered = sum(1 for r in fail if r["replies_view"] == "OK")
        pass_silent = sum(1 for r in ok if r["replies_view"] == "ALERT")
        counts = {"domains": len(rs), "with_both": len(both), "fail": len(fail), "fail_answered": fail_answered,
                  "pass": len(ok), "pass_silent": pass_silent}
        group = f"{client} {typ}".strip()
        state, evidence, proposal = HOLDS, "", ""
        if len(both) < MIN_DOMAINS:
            state = THIN
            evidence = (f"{len(both)} of {len(rs)} tested {_n(len(rs), 'domain', 'domains')} "
                        f"{'has' if len(both) == 1 else 'have'} both a clear test result and enough sends for the "
                        f"reply check; {MIN_DOMAINS} are needed")
        elif fail and fail_answered * 2 >= len(fail):
            state = DOUBT
            evidence = (f"{fail_answered} of {len(fail)} {_n(len(fail), 'domain', 'domains')} that fail the test get "
                        "replies at the same rate as the client's other domains")
            proposal = (f"For {group} inboxes, rest an inbox only when the test and the reply check agree, the way "
                        "the bill review already decides a drop")
        elif ok and pass_silent * 2 >= len(ok):
            state = DOUBT
            evidence = (f"{pass_silent} of {len(ok)} {_n(len(ok), 'domain', 'domains')} that pass the test get far "
                        "fewer replies than the client's other domains")
            proposal = (f"For {group}, run the copy check and look at the list before trusting the pass: the test "
                        "may be too easy for this group, or the email is the problem")
        else:
            evidence = (f"test and replies agree on {len(both) - fail_answered - pass_silent} of {len(both)} "
                        f"{_n(len(both), 'domain', 'domains')}")
        out.append({"rule": "the test decides", "group": group, "state": state, "evidence": evidence,
                    "proposal": proposal, "counts": counts})
    return out


def returns(v2_rows: list[dict], gap: int = RETURN_GAP) -> dict:
    """Inboxes that came back (a fail, then two passes ``gap``+ days apart with no fail between) and how many of
    them failed again afterwards. -> {"came_back", "failed_again"}."""
    came_back = failed_again = 0
    for rs in _judged(v2_rows).values():
        seen_fail, first_pass, back = False, None, False
        for night, v, _ in rs:
            day = date.fromisoformat(night)
            if back:
                if v == "FAIL":
                    failed_again += 1
                    break
                continue
            if v == "FAIL":
                seen_fail, first_pass = True, None
            elif v == "PASS" and seen_fail:
                first_pass = first_pass or day
                if (day - first_pass).days >= gap:
                    back, came_back = True, came_back + 1
            elif v == "WATCH":
                first_pass = None
    return {"came_back": came_back, "failed_again": failed_again}


def judge_return_rule(r: dict) -> dict:
    """The rule "back after two passes". In doubt when a third or more of the returned inboxes failed again."""
    n, again = r["came_back"], r["failed_again"]
    state, evidence, proposal = HOLDS, f"{again} of {n} returned {_n(n, 'inbox', 'inboxes')} failed again", ""
    if n < MIN_RETURNS:
        state = THIN
        evidence = f"{n} {_n(n, 'inbox has', 'inboxes have')} come back so far; {MIN_RETURNS} are needed"
    elif again * 3 >= n:
        state = DOUBT
        proposal = "Ask for three passes before an inbox comes back"
    return {"rule": "back after two passes", "group": "all clients", "state": state, "evidence": evidence,
            "proposal": proposal, "counts": dict(r)}


def _n(k: int, one: str, many: str) -> str:
    return one if k == 1 else many


def review(v2_rows: list[dict], reply_docs: list[dict], assets: list[dict], today: date) -> dict:
    """Everything the review found. -> {"today", "rules": [...], "rows": [...]}; ``rows`` is the evidence table."""
    rows = test_vs_replies(domain_tests(v2_rows, inbox_types(assets), today), reply_views(reply_docs, today))
    return {"today": today.isoformat(), "rows": rows,
            "rules": judge_test_rule(rows) + [judge_return_rule(returns(v2_rows))]}


def format_review(res: dict) -> tuple[list[str], list[str]]:
    """(headline, thread) in plain words. Proposals are named as needing a person's yes."""
    rules = res["rules"]
    doubt = [r for r in rules if r["state"] == DOUBT]
    thin = [r for r in rules if r["state"] == THIN]
    holds = [r for r in rules if r["state"] == HOLDS]
    day = date.fromisoformat(res["today"])
    head = [f"Rules review, {day.day} {day.strftime('%b')}: {len(doubt)} in doubt, {len(holds)} holding, "
            f"{len(thin)} without enough data yet"]
    if doubt:
        head.append("In doubt: " + "; ".join(f"{r['rule']} ({r['group']})" for r in doubt))
    thread = []
    for title, group in (("In doubt", doubt), ("Holding", holds), ("Not enough data yet", thin)):
        if not group:
            continue
        lines = [f"*{title}*"]
        for r in group:
            lines.append(f"• {r['rule']}, {r['group']}: {r['evidence']}.")
            if r["proposal"]:
                lines.append(f"   Proposal, needs Manveen's yes: {r['proposal']}.")
        thread.append("\n".join(lines))
    thread.append("How this is judged: each rule is held up against an outcome it did not use. The test is compared "
                  "with real replies per sending domain, and the return rule with what returned inboxes did next. "
                  "Nothing changes by itself; a rule changes only on a person's yes.")
    return head, thread
