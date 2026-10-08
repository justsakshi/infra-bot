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
            "connected": f.get("connected", True)})
    return {"by_client": {c: dict(d) for c, d in by_client.items()},
            "counts": {k: sum(len(d.get(k, [])) for d in by_client.values())
                       for k in (kr.KILL, kr.WATCH, kr.GOOD, kr.NO_DATA)}}


def format_report(res: dict, errors: list[str], today: date) -> str:
    c = res["counts"]
    lines = [f"*:warning: Kill warnings — {today.isoformat()}* (warning only; nothing is changed)",
             f"*{c[kr.KILL]} should kill* · {c[kr.WATCH]} watch (one strike) · {c[kr.GOOD]} good · "
             f"{c[kr.NO_DATA]} no recent test"]
    for client, d in sorted(res["by_client"].items()):
        kill, watch = d.get(kr.KILL, []), d.get(kr.WATCH, [])
        if not kill and not watch:
            continue
        lines.append(f"\n*{client}*")
        for r in sorted(kill, key=lambda r: r["email"])[:40]:
            live = f" :rotating_light: still in {', '.join(r['campaigns'][:2])}" if r["campaigns"] else ""
            lines.append(f":x: `{r['email']}` ({r['platform']}) — {r['why']}{live}")
        if len(kill) > 40:
            lines.append(f"…and {len(kill) - 40} more")
        if watch:
            lines.append(f":eyes: Watch ({len(watch)}): " + ", ".join(f"`{r['email']}`" for r in sorted(watch, key=lambda r: r['email'])[:25])
                         + ("…" if len(watch) > 25 else ""))
    lines.append("\n_Should kill = the team's rule: two strikes 7+ days apart, under 50% on 20+ seeds, or a dead "
                 "domain (40+ pooled seeds under 80%). Take them out of campaigns, then retire / replace at the next bill "
                 "(`/domains review`)._")
    if errors:
        lines.append(":warning: could not read: " + "; ".join(errors) + " — inboxes in those accounts are missing from this list.")
    return "\n".join(lines)


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
        text = format_report(res, errors, today)
    except Exception as exc:  # noqa: BLE001 - say so in Slack too
        res, text = {"error": f"{type(exc).__name__}: {exc}"}, f":x: Kill warnings failed: {type(exc).__name__}: {exc}"
    if args.post:
        from smartlead.notify import _post
        token = os.getenv("SLACK_BOT_TOKEN", "")
        channel = os.getenv("KILL_REPORT_CHANNEL") or os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL
        res["posted"] = bool(token and _post(token, channel, text[:39000]))
    print(json.dumps({**res, "text": text}, default=str) if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
