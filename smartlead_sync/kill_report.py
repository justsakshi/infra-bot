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


RULE = "How we decide: a test passes when 80%+ of its emails reach the inbox."
DECIDE = ("Take an inbox out after 2 failed tests a week apart, or one test under 50% (20+ test emails). "
          "Take a whole domain out when its inboxes together reach the inbox under 80% of the time "
          "over 40+ test emails.")
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


def _inbox(r: dict, why: str = "") -> str:
    plat = f" ({r['platform']})" if r.get("platform") not in ("", "?", None) else ""
    where = ""
    if r.get("sending"):
        where = f" · 🚨 still sending in: {', '.join(r['sending'][:2])}"
    elif _idle(r):
        where = f" · in switched-on campaigns that sent nothing in 7 days: {', '.join(_idle(r)[:2])}"
    return f"• `{r['email']}`{plat}{' — ' + why if why else ''}{where}"


def _client_part(client: str, kill: list[dict]) -> str:
    """One thread reply: a client's inboxes to take out, grouped by domain;
    a whole domain's reason is said once on its header line."""
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for r in kill:
        by_dom[r["email"].split("@")[1]].append(r)
    lines = [f"*{client}: {_n(len(kill), 'inbox', 'inboxes')} to take out*"]
    # whole domains first (biggest first), then inbox-level reasons
    order = sorted(by_dom.items(), key=lambda kv: (not any(r["rule"] == "c" for r in kv[1]), -len(kv[1]), kv[0]))
    for dom, rows in order:
        rows = sorted(rows, key=lambda r: r["email"])
        if all(r["rule"] == "c" for r in rows):
            lines.append(f"*{dom}* — {_short(rows[0]['why'])}")
            lines += [_inbox(r) for r in rows]
        else:
            lines.append(f"*{dom}*")
            lines += [_inbox(r, _short(r["why"])) for r in rows]
    return "\n".join(lines)


def format_report(res: dict, errors: list[str], today: date) -> tuple[list[str], list[str]]:
    """(headline_lines, thread_parts): the headline goes in the channel, the
    lists and how we decide go in its thread (spec 2026-10-13)."""
    c = res["counts"]
    by_client = res["by_client"]
    kills = [r for d in by_client.values() for r in d.get(kr.KILL, [])]
    watch = [(cl, r) for cl, d in by_client.items() for r in d.get(kr.WATCH, [])]
    sending = sorted(r["email"] for r in kills if r.get("sending"))
    idle = [r for r in kills if not r.get("sending") and _idle(r)]
    domains = {r["email"].split("@")[1] for r in kills}
    when = _day(today)
    retest = f" · {len(watch)} to retest" if watch else ""

    if not kills:
        head = [f"✅ Kill warnings, {when}: nothing to take out" + (f", {len(watch)} to retest" if watch else "")]
    else:
        icon = "🚨" if sending else "⚠️"
        one = len(kills) == 1
        head = [f"{icon} Kill warnings, {when}: {_n(len(kills), 'inbox', 'inboxes')} "
                f"{'is' if one else 'are'} landing in spam, take {'it' if one else 'them'} out of campaigns "
                f"({_n(len(domains), 'domain', 'domains')})"]
        if sending:
            more = f" +{len(sending) - 3} more" if len(sending) > 3 else ""
            head.append(f"{len(sending)} still sending: {', '.join(f'`{e}`' for e in sending[:3])}{more}, start there")
        elif idle:
            head.append(f"None is sending now; {len(idle)} {'sits' if len(idle) == 1 else 'sit'} in switched-on "
                        "campaigns that sent nothing in 7 days")
        per = {cl: len(by_client.get(cl, {}).get(kr.KILL, [])) for cl in CURRENT_CLIENTS}
        per.update({cl: len(d.get(kr.KILL, [])) for cl, d in by_client.items()})
        rank = {cl: i for i, cl in enumerate(CURRENT_CLIENTS)}
        head.append(" · ".join(f"{cl} {k}" for cl, k in
                               sorted(per.items(), key=lambda kv: (-kv[1], rank.get(kv[0], 99), kv[0]))) + retest)
    if errors:
        head.append(f"Could not read {'; '.join(errors)}; inboxes there are missing from this list")

    thread: list[str] = []
    for client, d in sorted(by_client.items(), key=lambda kv: (-len(kv[1].get(kr.KILL, [])), kv[0])):
        if d.get(kr.KILL):
            thread.append(_client_part(client, d[kr.KILL]))
    if watch:
        thread.append("\n".join([f"*To retest ({len(watch)}): one failed test is not proof yet*"]
                                + [_inbox(r, f"{cl}: {_short(r['why'])}")
                                   for cl, r in sorted(watch, key=lambda x: (x[0], x[1]["email"]))]))
    thread.append("\n".join([
        f"_{RULE} {DECIDE}_",
        "What to do: take them out of campaigns now, then retire or replace them at their next bill (`/domains review`).",
        f"Also checked: {c[kr.GOOD]} passed, {c[kr.NO_DATA]} with no recent test. Warning only: the bot changes nothing."]))
    return head, thread


def render_text(head: list[str], thread: list[str]) -> str:
    """Both parts as one text (print mode and the /domains kill screen)."""
    return "\n".join(head) + "".join(f"\n\n— thread —\n{p}" if i == 0 else f"\n\n{p}"
                                     for i, p in enumerate(thread))


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
        head, thread = format_report(res, errors, today)
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
        res["posted"] = bool(token and post_with_thread(token, channel, head, thread))
    print(json.dumps({**res, "headline": head, "thread": thread, "text": text}, default=str)
          if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
