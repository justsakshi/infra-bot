#!/usr/bin/env python3
"""Weekly kill warnings: which inboxes SHOULD be killed, and which to watch.

WARNING ONLY — nothing is retired, paused or changed. The team's kill rule
(smartlead/kill_rule.py) over every current-client inbox's placement tests:

  SHOULD KILL  two strikes 7+ days apart, under 50% on 20+ seeds, or a dead
               domain (40+ pooled seeds under 80%, half its mailboxes struck)
  WATCH        one strike — retest before deciding
  NO DATA      no test with 10+ seeds in 21 days

    python kill_report.py            # print
    python kill_report.py --post     # to KILL_REPORT_CHANNEL (else PLACEMENT_REPORT_CHANNEL, else C0AGVSUNEFP)
    python kill_report.py --json

Runs Tuesday 13:30 IST, after the Mon/Tue placement reports.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import kill_rule as kr
from smartlead.zapmail_clients import CURRENT_CLIENTS

DEFAULT_CHANNEL = "C0AGVSUNEFP"


def _platform_of() -> dict[str, str]:
    """inbox -> provider as the tracker records it (Zapmail / Scaledmail / ...)."""
    try:
        from smartlead.zapmail_asset_sync import read_tracker
        return {k: str(v.get("provider") or "") for k, v in read_tracker().items() if "@" in k}
    except Exception:  # noqa: BLE001
        return {}


def build(facts: dict[str, dict], today: date, platform: dict[str, str]) -> dict:
    facts = {e: f for e, f in facts.items() if f.get("client") in CURRENT_CLIENTS}
    verdicts = kr.judge_all({e: f.get("tests") or [] for e, f in facts.items()}, today)
    by_client: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for e, v in verdicts.items():
        f = facts[e]
        by_client[f["client"]][v["verdict"]].append({
            **v, "platform": platform.get(e, "") or "?", "campaigns": f.get("campaigns") or [],
            "sending": f.get("sending") or [],
            "connected": f.get("connected", True)})
    return {"by_client": {c: dict(d) for c, d in by_client.items()},
            "counts": {k: sum(len(d.get(k, [])) for d in by_client.values())
                       for k in (kr.KILL, kr.WATCH, kr.GOOD, kr.NO_DATA)}}


RULE = "Rule: an inbox passes a test when 80%+ of its test emails reach the inbox."
DECIDE = ("2 fails a week apart, or one under 50%, means stop using it; a whole domain when its inboxes "
          "together stay under 80% over 40+ test emails.")
_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def _day(d: date) -> str:
    return f"{d.day} {d:%b}"


def _short(why: str) -> str:
    """The rule's reason with '5 Oct' dates."""
    return _ISO.sub(lambda m: _day(date(int(m[1]), int(m[2]), int(m[3]))), why)


def _n(k: int, one: str, many: str) -> str:
    return f"{k} {one if k == 1 else many}"


def _idle(r: dict) -> list[str]:
    """Switched-on campaigns this inbox is in that sent nothing in 7 days."""
    return [c for c in r.get("campaigns") or [] if c not in (r.get("sending") or [])]


def _pct(rows: list[dict]) -> str:
    vals = sorted(round(r["pct"]) for r in rows if r.get("pct") is not None)
    if not vals:
        return "?"
    return f"{vals[0]}%" if vals[0] == vals[-1] else f"{vals[0]}-{vals[-1]}%"


def bill_dates(bills: list[dict]) -> dict[str, dict]:
    """domain -> its next bill {bills_on, provider, label, price} (the earliest
    bill listing one of its inboxes). Pre-warmed plans list no inboxes, so
    their domains get no bill. Pure."""
    out: dict[str, dict] = {}
    for b in bills:
        for email in b.get("inboxes") or []:
            dom = email.lower().split("@")[-1]
            if dom not in out or b["bills_on"] < out[dom]["bills_on"]:
                out[dom] = {k: b.get(k) for k in ("bills_on", "provider", "label", "price")}
    return out


def _bill(dom: str, bills: dict | None) -> str:
    if bills is None:
        return ""
    b = bills.get(dom)
    return f" · bills {_day(date.fromisoformat(b['bills_on']))}" if b else " · no bill found"


def format_report(res: dict, errors: list[str], today: date,
                  bills: dict | None = None) -> tuple[list[str], list[str]]:
    """(headline_lines, parts): one message, action first. ``bills`` =
    bill_dates(...) or None when bills were not read."""
    c = res["counts"]
    by_client = res["by_client"]
    rank = {cl: i for i, cl in enumerate(CURRENT_CLIENTS)}
    kills = [(cl, r) for cl, d in by_client.items() for r in d.get(kr.KILL, [])]
    watch = sorted(((cl, r) for cl, d in by_client.items() for r in d.get(kr.WATCH, [])), key=lambda x: x[1]["email"])
    sending = sorted(((cl, r) for cl, r in kills if r.get("sending")), key=lambda x: x[1]["email"])
    idle = sorted(((cl, r) for cl, r in kills if not r.get("sending") and _idle(r)), key=lambda x: x[1]["email"])
    doms: dict[tuple, list[dict]] = defaultdict(list)
    for cl, r in kills:
        doms[(cl, r["email"].split("@")[1])].append(r)
    when = _day(today)

    if not kills:
        head = [f"✅ Kill warnings, {when}: no domain is landing in spam"]
    else:
        n = len(doms)
        head = [f"{'🚨' if sending else '⚠️'} Kill warnings, {when}: {_n(n, 'domain is', 'domains are')} "
                f"landing in spam, stop using {'it' if n == 1 else 'them'}"]
        if sending:
            k = len(sending)
            head.append(f"{k} of their inboxes {'is' if k == 1 else 'are'} still sending: "
                        f"take {'it' if k == 1 else 'them'} out of campaigns today.")
        else:
            head.append("None of them is sending right now. Don't add them to campaigns; "
                        "retire or replace them at their next bill.")
    if errors:
        head.append(f"Could not read {'; '.join(errors)}; inboxes there are missing from this list")

    parts: list[str] = []
    look = []
    for cl, r in sending:
        look.append(f"• `{r['email']}` is sending in {', '.join(r['sending'][:2])} and is on the kill list "
                    f"({_short(r['why'])}). Take it out of campaigns today.")
    for cl, r in idle:
        camps = _idle(r)
        one = len(camps) == 1
        look.append(f"• `{r['email']}` is in {_n(len(camps), 'campaign', 'campaigns')} that "
                    f"{'is' if one else 'are'} switched on but not sending ({', '.join(camps[:2])}). "
                    f"Pause {'it' if one else 'them'} so {'it' if one else 'they'} can't restart.")
    for cl, r in watch:
        if r.get("sending"):
            look.append(f"• `{r['email']}` is sending in {', '.join(r['sending'][:2])} and failed its last test "
                        f"({_pct([r])} reached the inbox). Retest it this week; take it out if it fails again.")
    if look:
        parts.append("\n".join(["*Needs a look today*"] + look))

    if doms:
        lines = ["*Stop using these domains*"]
        per_client = defaultdict(int)
        for (cl, _), rows in doms.items():
            per_client[cl] += len(rows)
        order = sorted(doms.items(), key=lambda kv: (-per_client[kv[0][0]], rank.get(kv[0][0], 99), kv[0][0],
                                                     not all(r["rule"] == "c" for r in kv[1]), -len(kv[1]), kv[0][1]))
        for (cl, dom), rows in order:
            who = (f"({rows[0]['email'].split('@')[0]}@)" if len(rows) == 1 and rows[0]["rule"] != "c"
                   else f"({_n(len(rows), 'inbox', 'inboxes')})")
            lines.append(f"• {cl} · {dom} {who} · {_pct(rows)} reached the inbox{_bill(dom, bills)}")
        parts.append("\n".join(lines))

    retest = [(cl, r) for cl, r in watch if not r.get("sending")]
    if retest:
        parts.append("\n".join(["*Retest*"] + [f"• `{r['email']}` ({cl}) · failed 1 test ({_pct([r])})"
                                               for cl, r in retest]))
    parts.append(f"_{RULE} {DECIDE} Also checked: {c[kr.GOOD]} passed, {c[kr.NO_DATA]} with no recent test. "
                 "Warning only: the bot changes nothing._")
    return head, parts


def render_text(head: list[str], parts: list[str]) -> str:
    """The whole message, as posted (one message, no thread)."""
    return "\n".join(head) + "".join(f"\n\n{p}" for p in parts)


def _bills(errors: list[str]) -> dict | None:
    """Every Zapmail and ScaledMail bill in the next 35 days (all monthly bills),
    as domain -> next bill. None when neither could be read."""
    try:
        from renewal_review import scaledmail_bills, zapmail_bills
        n = len(errors)
        bills = zapmail_bills(35, errors) + scaledmail_bills(35, errors)
        return bill_dates(bills) if bills or len(errors) == n else None
    except Exception as exc:  # noqa: BLE001 - the warnings still post, without bill dates
        errors.append(f"bills ({type(exc).__name__})")
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly kill warnings (read-only)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--post", action="store_true")
    args = ap.parse_args()
    from renewal_review import smartlead_facts
    errors: list[str] = []
    today = date.today()
    try:
        facts = smartlead_facts(errors)
        res = build(facts, today, _platform_of())
        head, thread = format_report(res, errors, today, _bills(errors))
    except Exception as exc:  # noqa: BLE001 - say so in Slack too
        res = {"error": f"{type(exc).__name__}: {exc}"}
        head = [f"🚨 Kill warnings, {_day(today)}: the check failed ({type(exc).__name__}: {exc}). "
                "No inboxes were judged; rerun it or check the bot's log."]
        thread = []
    text = render_text(head, thread)
    if args.post:
        from smartlead.notify import post_with_thread
        token = os.getenv("SLACK_BOT_TOKEN", "")
        channel = os.getenv("KILL_REPORT_CHANNEL") or os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL
        res["posted"] = bool(token and post_with_thread(token, channel, [render_text(head, thread)], []))
    print(json.dumps({**res, "headline": head, "thread": thread, "text": text}, default=str)
          if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
