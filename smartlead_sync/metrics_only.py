#!/usr/bin/env python3
"""Write the Campaign Metrics tabs and today's Campaign History rows.

The daily campaign-stats job: index.js runs it at 09:00 IST, and the 10:30
stats post (campaign_stats_post.py) reads the history it writes. It used to
be a quick re-check of the metrics block at the end of run.py; that block now
lives only here, so the numbers come from one place and run.py no longer
spends ~24 minutes repeating it.

    python metrics_only.py              write the tabs and today's history
    python metrics_only.py --dry-run    print the rows, write nothing
"""
from __future__ import annotations

import argparse
import asyncio
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
from smartlead.config import CAMPAIGN_HISTORY_TAB_NAME, CAMPAIGN_METRICS_SHEET_ID
from smartlead.metrics_collect import collect_metric_rows
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
    history = ch.history_rows(rows, day)
    if args.dry_run:
        print(f"\n[Metrics] --dry-run: sheet not written ({len(history)} history rows "
              f"for {day} not written).")
        return 0

    writer = SheetsWriter(CAMPAIGN_METRICS_SHEET_ID)
    failed = False
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
    return 2 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
