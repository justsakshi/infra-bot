#!/usr/bin/env python3
"""Fill the team's test grid ("Deliverability Test results", one tab per client) from Smart Delivery v2.

The grid was filled by hand. On 10 Nov 2026 the Melior tab was current, Precise Leads stopped at 5 Oct and
Bettrdata at 28 Sep, so those two clients had no fresh "Test Status" and the tool that picks inboxes saw them
as stale. v2 results reach the bot as CSVs (v2_weekly.py on the PC) and are kept in Mongo ``v2_verdicts``;
this copies each test night into the grid, per sending domain:

    one new column pair per night, headed "9 Nov 2026 v2", with "G suite" and "Outlook" beneath it;
    a domain's cell is "Inbox" when 80%+ of its v2 seed emails at that provider reached the inbox, else "spam".

It keeps the grid writer's two rules (smartlead/placement_grid.py): a cell that already holds a value is never
overwritten, and a domain is matched exactly on column A. A domain that is not in the tab is reported, never
added: the tab's rows are also the list of domains the team tracks.

    python v2_grid.py            # dry run: what would be written
    python v2_grid.py --apply    # write
The daily sheet sync (run.py) calls ``sync`` before it reads the grid; V2_GRID_SYNC=off makes that a dry run.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead import placement_grid as pg
from smartlead.report_sheet import grid_tab

LOOKBACK_DAYS = 21
THRESHOLD = 80.0
_MON = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
SUFFIX = " v2"          # smartlead/sheets._parse_test_date reads the date through this suffix


def label_for(night: str) -> str:
    """'2026-11-09' -> '9 Nov 2026 v2': a column the team can tell from a hand-typed one."""
    d = date.fromisoformat(night[:10])
    return f"{d.day} {_MON[d.month - 1]} {d.year}{SUFFIX}"


def night_results(rows) -> dict[tuple[str, str], dict[str, dict]]:
    """{(grid tab, night): {domain: {"G Suite": {"inbox_pct"}, "Office365": {"inbox_pct"}}}} from v2_verdicts rows.

    Pooled over the domain's inboxes that got a verdict that night. A provider with no reported seed is left
    out (its cell stays empty); INCOMPLETE rows are not judged. Pure."""
    pool: dict = defaultdict(lambda: defaultdict(lambda: {"G Suite": [0, 0], "Office365": [0, 0]}))
    for d in rows or []:
        if str(d.get("verdict") or "").upper() not in ("PASS", "WATCH", "FAIL"):
            continue
        tab = grid_tab(str(d.get("client") or ""))
        night, dom = str(d.get("night") or "")[:10], str(d.get("domain") or "").strip().lower()
        if not tab or not night or not dom:
            continue
        for key, field in (("G Suite", "google"), ("Office365", "microsoft")):
            x = d.get(field) or {}
            pool[(tab, night)][dom][key][0] += int(x.get("inbox") or 0)
            pool[(tab, night)][dom][key][1] += int(x.get("reported") or 0)
    out: dict = {}
    for key, doms in pool.items():
        out[key] = {dom: {p: {"inbox_pct": 100.0 * i / n} for p, (i, n) in by.items() if n}
                    for dom, by in doms.items()}
        out[key] = {dom: by for dom, by in out[key].items() if by}
    return out


def plan_tab(rows: list[list[str]], results: dict[str, dict], label: str) -> dict:
    """Every write for one tab and one night, planned on one read of the tab. Pure.

    Returns {"writes": [(row, col, value)], "new_columns": (gs, ol) | None, "skipped": {domain: reason}}."""
    writes, new_cols, skipped = [], None, {}
    for dom in sorted(results):
        plan = pg.plan_writes(rows, dom, results[dom], label, THRESHOLD)
        if plan.skipped_reason:
            skipped[dom] = plan.skipped_reason
        if plan.writes:
            writes.extend(plan.writes)
            new_cols = new_cols or plan.new_column_indices
    return {"writes": sorted(writes), "new_columns": new_cols, "skipped": skipped}


def _write(ws, rows: list[list[str]], plan: dict, label: str) -> None:
    cells = []
    if plan["new_columns"]:
        gs, ol = plan["new_columns"]
        head = pg.plan_header_row(rows)
        cells += [{"range": pg._a1(head, gs), "values": [[label]]},
                  {"range": pg._a1(head + 1, gs), "values": [["G suite"]]},
                  {"range": pg._a1(head + 1, ol), "values": [["Outlook"]]}]
        need = ol + 1
        if need > ws.col_count:
            ws.add_cols(need - ws.col_count)
    cells += [{"range": pg._a1(r, c), "values": [[v]]} for r, c, v in plan["writes"]]
    ws.batch_update(cells)


def _verdict_rows(since: str) -> list[dict]:
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    col = MongoClient(uri, serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]["v2_verdicts"]
    return list(col.find({"night": {"$gte": since}}, {"_id": 0, "client": 1, "domain": 1, "night": 1,
                                                       "verdict": 1, "google": 1, "microsoft": 1}))


def sync(apply: bool = False, today: date | None = None, verdict_rows=None, open_tab=None) -> list[dict]:
    """Copy the last LOOKBACK_DAYS of v2 nights into the grid. Never raises: the grid is the team's report,
    Mongo is the record. Returns one summary per (tab, night)."""
    today = today or date.today()
    out: list[dict] = []
    try:
        rows = verdict_rows if verdict_rows is not None else _verdict_rows((today - timedelta(days=LOOKBACK_DAYS)).isoformat())
        nights = night_results(rows)
    except Exception as exc:  # noqa: BLE001
        print(f"  [v2 grid] v2 verdicts not read (grid left as it is): {str(exc)[:150]}")
        return [{"error": str(exc)[:150]}]
    if open_tab is None:
        def open_tab(tab):
            from smartlead.config import TEST_SHEET_ID
            from smartlead.sheets import _authorize
            return _authorize().open_by_key(TEST_SHEET_ID).worksheet(tab)
    for (tab, night) in sorted(nights):
        label = label_for(night)
        item = {"tab": tab, "night": night, "label": label, "domains": len(nights[(tab, night)]),
                "written": 0, "skipped": {}, "applied": False, "error": ""}
        try:
            ws = open_tab(tab)
            grid = ws.get_all_values()
            plan = plan_tab(grid, nights[(tab, night)], label)
            item["written"], item["skipped"] = len(plan["writes"]), plan["skipped"]
            if apply and plan["writes"]:
                _write(ws, grid, plan, label)
                item["applied"] = True
        except Exception as exc:  # noqa: BLE001 - one tab must not stop the others
            item["error"] = str(exc)[:150]
        out.append(item)
        missing = sorted(d for d, why in item["skipped"].items() if why == "domain not in sheet")
        print(f"  [v2 grid] {tab} · {label}: {item['written']} cell(s) {'written' if item['applied'] else 'to write'}"
              f" for {item['domains']} domain(s)"
              + (f"; not in the tab, add a row to track them: {', '.join(missing)}" if missing else "")
              + (f"; error: {item['error']}" if item["error"] else ""))
    if not out:
        print("  [v2 grid] no v2 night in the last 21 days")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Copy Smart Delivery v2 results into the team's test grid")
    ap.add_argument("--apply", action="store_true", help="write (default: say what would be written)")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
        load_dotenv()
    except Exception:
        pass
    res = sync(apply=args.apply)
    return 1 if any(r.get("error") for r in res) else 0


if __name__ == "__main__":
    raise SystemExit(main())
