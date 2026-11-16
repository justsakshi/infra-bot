#!/usr/bin/env python3
"""Write the Campaign Metrics tabs and today's Campaign History rows.

The daily campaign-stats job: index.js runs it at 09:00 IST, and the 10:30
stats post (campaign_stats_post.py) reads the history it writes. It used to
be a quick re-check of the metrics block at the end of run.py; that block now
lives only here, so the numbers come from one place and run.py no longer
spends ~24 minutes repeating it.

    python metrics_only.py              write the tabs and today's history
    python metrics_only.py --dry-run    print the rows, write nothing

Daily digest (16 Nov 2026, smartlead/digest.py): with the digest on, a failed run is one "broken" ask (which
sources were not read, where the log is, and the env var names when a key is missing) instead of a post; a clean
run clears it. If the digest can't be written, the old post goes out.
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from datetime import datetime, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead import campaign_history as ch
from smartlead import campaign_metrics as cm
from smartlead.config import (BETTRDATA_TRACKER_SHEET_ID, CAMPAIGN_HISTORY_TAB_NAME,
                              CAMPAIGN_METRICS_SHEET_ID)
from smartlead import client_tracker, waalaxy_log
from smartlead.metrics_collect import FAILED, STATUSES, collect_metric_rows
from smartlead.sheets import SheetsWriter


async def main() -> int:
    ap = argparse.ArgumentParser(description="Write Campaign Metrics + Campaign History")
    ap.add_argument("--month", default="auto")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the rows instead of writing to the sheet")
    args = ap.parse_args()

    today = datetime.now(timezone.utc)
    start_dt, end_dt, month_name = cm.get_reporting_range(args.month, today)
    print(f"[Metrics] range {start_dt:%Y-%m-%d} -> {end_dt:%Y-%m-%d} ({month_name})")

    rows = await collect_metric_rows(today, start_dt, end_dt)
    if not rows:
        print("[Metrics] no campaigns qualified — nothing written.")
        return 1

    out = cm.rows_with_totals(rows)
    print(f"\n[Metrics] {len(rows)} campaign(s) -> {len(out)} row(s)\n")
    print(f"  {'client':14} {'campaign':40} {'plat':10} {'total':>6} {'sent':>6} "
          f"{'life_sent':>9} {'invites':>7} {'replies':>7}")
    for r in out:
        print(f"  {str(r.get('client', '')):14} {str(r.get('campaign', ''))[:40]:40} "
              f"{str(r.get('platform', '')):10} {str(r.get('total_leads', '')):>6} "
              f"{str(r.get('msg_sent', '')):>6} {str(r.get('life_sent', '')):>9} "
              f"{str(r.get('life_invites', '')):>7} {str(r.get('life_replies', '')):>7}")

    day = ch.snapshot_day()
    # PL LinkedIn: replies from the n8n Waalaxy log, history only (Waalaxy's
    # API has no stats, so there is nothing for the Campaign Metrics tabs).
    try:
        try:
            accepts = waalaxy_log.read_log(waalaxy_log.ACCEPTS_TAB)
        except Exception as exc:  # noqa: BLE001 - replies still count without it
            accepts = None
            print(f"[!] Waalaxy accepts log unreadable: {type(exc).__name__}")
        li_pl = waalaxy_log.history_rows(waalaxy_log.read_log(), accepts)
        print(f"[Metrics] Waalaxy log: {len(li_pl)} PL LinkedIn campaign(s)")
    except Exception as exc:  # noqa: BLE001
        li_pl = []
        print(f"[!] Waalaxy log unreadable, PL LinkedIn left out: {type(exc).__name__}")
    history = ch.history_rows(rows + li_pl, day)
    if args.dry_run:
        print(f"\n[Metrics] --dry-run: sheet not written ({len(history)} history rows "
              f"for {day} not written).")
        return 0

    writer = SheetsWriter(CAMPAIGN_METRICS_SHEET_ID)
    # An unreadable account still writes what was read, but the run is not
    # clean: the alert names it, and the 10:30 post says the client is missing.
    failed = bool(FAILED)
    for source in FAILED:
        print(f"[!] not read this run: {source}")
    # History first: it is the record the 10:30 post depends on, and a missed
    # day cannot be rebuilt later the way a stale tab can be rewritten.
    try:
        n = ch.write_history(writer, history, day, CAMPAIGN_HISTORY_TAB_NAME)
        print(f"[Metrics] {CAMPAIGN_HISTORY_TAB_NAME}: {n} row(s) for {day}")
    except Exception as exc:  # noqa: BLE001
        print(f"[!] {CAMPAIGN_HISTORY_TAB_NAME} write failed: {exc}")
        failed = True
    try:
        writer.write_campaign_metrics(out, month_name=month_name)
        # `rows`, not `out`: the per-client writer adds its own subtotal.
        writer.write_campaign_metrics_per_client(rows, month_name=month_name)
        # No-ops unless CAMPAIGN_METRICS_CLIENT_SHEETS names a spreadsheet.
        SheetsWriter.write_campaign_metrics_client_sheets(rows, month_name=month_name)
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Campaign Metrics write failed: {exc}")
        failed = True
    print(f"\n[Metrics] written to sheet {CAMPAIGN_METRICS_SHEET_ID}")
    if BETTRDATA_TRACKER_SHEET_ID:
        try:
            changed, added = client_tracker.update_dashboard(
                BETTRDATA_TRACKER_SHEET_ID,
                [r for r in rows if r.get("client") == "BettrData"], day,
                {(name, ch): st for (client, ch, name), st in STATUSES.items()
                 if client == "BettrData"})
            print(f"[Metrics] BettrData tracker: {changed} cell(s) updated, {added} row(s) added")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] BettrData tracker update failed: {exc}")
            failed = True
    return 2 if failed else 0


def _alert(text: str) -> None:
    """Failures go to the infra alerts channel; index.js only logs exit codes."""
    import os
    from smartlead.config import HEALTH_NOTIFY_CHANNEL
    from smartlead.notify import _post
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if token and HEALTH_NOTIFY_CHANNEL:
        _post(token, HEALTH_NOTIFY_CHANNEL, text)


SOURCE = "campaign-metrics"          # its name in the daily digest
_MISSING_KEYS = re.compile(r"\(no ([A-Z0-9_ +]+?) on this server\)")


def failure_text(code: int, failed: list[str]) -> str:
    """The failure post, as it has always read."""
    return ("⚠️ Campaign stats run failed\n\nThe 9:00 job that fills Campaign History, "
            "Campaign Metrics and BettrData's tracker did not finish cleanly "
            f"(exit {code}). Its log is on Render: search for [campaign-metrics]."
            + ("\n\nNot read: " + "; ".join(failed) if failed else ""))


def digest_ask(code: int, failed: list[str]):
    """The failure as one "broken" digest ask: which sources were not read, and where to look. When the cause
    is a key missing on the server, the step names its env vars. Pure."""
    from smartlead import digest as dg
    # A missing key is said in the step; any other reason (an error type) stays with its source.
    sources = list(dict.fromkeys(f.split(" (", 1)[0] if _MISSING_KEYS.search(f) else f for f in failed))
    keys = list(dict.fromkeys(n.strip() for f in failed for m in _MISSING_KEYS.finditer(f)
                              for n in m.group(1).split("+") if n.strip()))
    text = ("The 9:00 campaign stats job did not finish cleanly"
            + (f"; not read: {'; '.join(sources)}." if sources else f" (exit {code})."))
    step = (f"Set {' and '.join(keys)} on Render (Environment); the log is on Render, search for [campaign-metrics]."
            if keys else "Read why in the log on Render (search for [campaign-metrics]); the job runs again tomorrow "
                         "at 9:00 IST.")
    return dg.Ask(text, step, key=f"{SOURCE}:failed", kind="broken")


def hand_over(code: int, failed: list[str], dry_run: bool) -> None:
    """After a run: the digest gets the failure (or an empty list on success, which clears it); without the
    digest, a failure is posted as before."""
    if dry_run:
        return
    from smartlead import digest as dg
    from smartlead.notify import redact
    detail = [redact(failure_text(code, failed))] if code else []
    if dg.on() and dg.report(SOURCE, [digest_ask(code, failed)] if code else [], [], detail):
        return
    if code:
        _alert(failure_text(code, failed))


if __name__ == "__main__":
    try:
        code = asyncio.run(main())
    except Exception as exc:  # noqa: BLE001
        code = 1
        print(f"[!] metrics_only crashed: {exc}")
    hand_over(code, list(FAILED), "--dry-run" in sys.argv)
    sys.exit(code)
