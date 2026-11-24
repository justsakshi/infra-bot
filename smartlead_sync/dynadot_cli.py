#!/usr/bin/env python3
"""Dynadot domains from the command line (runbook: docs/DYNADOT.md).

READ (free):
    python dynadot_cli.py balance
    python dynadot_cli.py check a.co b.co            # availability + first-year price
    python dynadot_cli.py check --names names.txt    # same, for a whole names file
    python dynadot_cli.py ledger                     # what the buy runs did

SPEND (takes money from the Dynadot balance):
    DYNADOT_ALLOW_SPEND=true python dynadot_cli.py buy --names names.txt --count 100 --approve --notify

Add --sandbox to any command to use Dynadot's pre-funded test account
(DYNADOT_SANDBOX_KEY; no real money, so buy needs only --approve there).

The ledger and the names file live outside the repo (default
~/.infrabot/dynadot/): the repo is public and never holds domain names.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
IST = timezone(timedelta(hours=5, minutes=30))
OWNER = "U09TQ9D7YNM"   # Manveen's Slack id (same as smartlead/digest.py OWNER)

if sys.stdout is not None:   # pythonw under Task Scheduler has no console
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _log_to(path: Path | None):
    def log(line: str) -> None:
        stamp = f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST  {line}"
        if sys.stdout is not None:
            print(stamp, flush=True)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(stamp + "\n")
    return log


def _dm(text: str, log) -> bool:
    import requests
    from smartlead.notify import redact
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        log("SLACK_BOT_TOKEN missing: no DM sent")
        return False
    try:
        r = requests.post("https://slack.com/api/chat.postMessage", timeout=15,
                          headers={"Authorization": f"Bearer {token}"},
                          json={"channel": OWNER, "text": redact(text), "unfurl_links": False})
        ok = bool(r.json().get("ok"))
    except Exception as exc:   # a failed DM must never hide the run's result
        log(f"DM failed: {type(exc).__name__}")
        return False
    log("DM sent" if ok else "DM refused by Slack")
    return ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Dynadot domains (read free, buy armed)")
    ap.add_argument("--sandbox", action="store_true", help="use the pre-funded test account")
    ap.add_argument("--env-file", default=str(HERE.parent / ".env"))
    ap.add_argument("--dir", default=None, help="ledger/log folder (default ~/.infrabot/dynadot)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("balance")
    c = sub.add_parser("check")
    c.add_argument("domains", nargs="*")
    c.add_argument("--names")
    sub.add_parser("ledger")
    b = sub.add_parser("buy")
    b.add_argument("--names", required=True)
    b.add_argument("--count", type=int, required=True)
    b.add_argument("--approve", action="store_true")
    b.add_argument("--price-ceiling", type=float, default=None)
    b.add_argument("--total-ceiling", type=float, default=None)
    b.add_argument("--wait-minutes", type=int, default=0,
                   help="if the price is above the ceiling (sale not live yet), keep checking this long")
    b.add_argument("--poll-seconds", type=int, default=120)
    b.add_argument("--notify", action="store_true", help="DM Manveen the result on Slack")
    b.add_argument("--allow-spend", action="store_true",
                   help="same as DYNADOT_ALLOW_SPEND=true for this run (for Task Scheduler)")
    b.add_argument("--require-sandbox-pass", action="store_true",
                   help="refuse a live buy unless a sandbox buy run has bought at least one domain")
    args = ap.parse_args(argv)
    if args.cmd == "buy" and args.allow_spend:
        os.environ["DYNADOT_ALLOW_SPEND"] = "true"

    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
    except Exception:
        pass
    if args.sandbox:
        os.environ["DYNADOT_SANDBOX"] = "true"
    if args.dir:
        os.environ["DYNADOT_DIR"] = args.dir
    sys.path.insert(0, str(HERE))
    from smartlead import dynadot_orders as orders
    from smartlead.dynadot import DynadotClient, DynadotError

    base = Path(os.getenv("DYNADOT_DIR") or orders.DEFAULT_DIR)
    ledger_path = base / ("ledger-sandbox.json" if args.sandbox else "ledger.json")
    log = _log_to(base / "buy.log" if args.cmd == "buy" else None)

    if args.cmd == "ledger":
        print(json.dumps(orders.Ledger(ledger_path).data, indent=1))
        return 0
    if args.cmd == "buy" and args.require_sandbox_pass and not args.sandbox:
        runs = orders.Ledger(base / "ledger-sandbox.json").data["runs"]
        if not any(r.get("bought", 0) >= 1 for r in runs):
            msg = "Dynadot buy did not run: no sandbox test has bought a domain yet (docs/DYNADOT.md step 3)."
            log(msg)
            if args.notify:
                _dm(msg, log)
            return 2

    try:
        return _run(args, DynadotClient(sandbox=args.sandbox), orders, ledger_path, log)
    except DynadotError as exc:          # no key, Dynadot unreachable: say so, never crash silently
        log(f"NOT RUN: {exc}")
        if args.cmd == "buy" and args.notify:
            _dm(f"Dynadot buy did not run: {exc}", log)
        return 2


def _run(args, client, orders, ledger_path, log) -> int:
    from smartlead.dynadot import DynadotError
    with client as dd:
        if args.cmd == "balance":
            print(json.dumps({"balance": dd.balance(), "sandbox": dd.sandbox}, indent=1))
            return 0
        if args.cmd == "check":
            names = list(args.domains) + (orders.read_names(args.names) if args.names else [])
            rows = []
            for d in names:
                try:
                    rows.append(dd.search(d.strip().lower()))
                except DynadotError as exc:
                    rows.append({"domain": d, "error": str(exc)})
            avail = [r for r in rows if r.get("available") and not r.get("premium")]
            prices = sorted({r["price_usd"] for r in avail if r.get("price_usd") is not None})
            print(json.dumps({"results": rows, "checked": len(rows), "available": len(avail),
                              "prices_usd": prices, "sandbox": dd.sandbox}, indent=1))
            return 0
        # buy
        names = orders.read_names(args.names)
        ledger = orders.Ledger(ledger_path)
        wait_until = (orders.now_utc() + timedelta(minutes=args.wait_minutes)) if args.wait_minutes else None
        try:
            s = orders.buy(dd, names, target=args.count, ledger=ledger, approve=args.approve,
                           price_ceiling=args.price_ceiling or orders.PRICE_CEILING,
                           total_ceiling=args.total_ceiling or orders.TOTAL_CEILING,
                           wait_until=wait_until, poll_seconds=args.poll_seconds, log=log)
        except DynadotError as exc:
            log(f"NOT RUN: {exc}")
            if args.notify:
                _dm(f"Dynadot buy did not run: {exc}", log)
            return 2
        if args.notify:
            _dm(orders.summary_text(s, str(ledger_path)), log)
        return 0 if s["bought"] >= args.count else 1


def _crash(argv) -> None:
    """Under pythonw a crash is invisible: write it to the log and DM it."""
    base = Path(os.getenv("DYNADOT_DIR") or Path.home() / ".infrabot" / "dynadot")
    log = _log_to(base / "buy.log")
    log("CRASH: " + traceback.format_exc()[-1500:])
    if "--notify" in (argv or sys.argv):
        _dm("Dynadot buy CRASHED. Traceback in buy.log; the ledger has every domain bought before it.", log)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except BaseException:
        _crash(None)
        raise
