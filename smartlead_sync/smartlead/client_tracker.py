"""Keep BettrData's own tracker sheet (Campaign Dashboard tab) current.

BettrData's "Outbound Tracker V2" has one row per campaign: what it is (titles,
CTA, plan, notes: written by people) and how it is doing (status, leads, sent,
opens, connections, replies, positives: until 29 Oct 2026 copied in by hand,
and weeks out of date). metrics_only.py calls this every morning with the
collected rows (Manveen, 29 Oct: "can we update this as well").

Only the number columns are written, and never:
  - a cell holding a formula (Connect % / Reply %, the totals row),
  - a "Blocked" status (a person's call, not a platform state),
  - an Expandi lead count (the API has no uploaded-list total; see
    expandi_metric_row), so the hand-entered figure stays,
  - LinkedIn Positive: nobody tags leads in Expandi, so its "interested"
    count is 0 while the team's count (from the Expandi Responses tab) is real,
  - a lower count: Sent, Connected, Replies and Positive only go up. A lower
    API figure means it misses something the team counted (a reply Smartlead
    never categorised), not that a reply un-happened.
A campaign with no row yet is added at the bottom with the sheet's own
Connect % / Reply % formulas and "New row" in Last Change, the way the team
marks new rows. The person-written columns stay blank for Precise to fill.
"""
from __future__ import annotations

from datetime import date

from gspread.utils import rowcol_to_a1

DASHBOARD_TAB = "Campaign Dashboard"
# Column positions (1-based) on the dashboard, by header.
_COL = {"campaign": 1, "channel": 2, "status": 4, "launched": 5, "leads": 9,
        "sent": 10, "open": 11, "connected": 12, "replies": 13, "positive": 14,
        "connect_pct": 15, "reply_pct": 16, "last_change": 17, "change_date": 18}
_ONLY_UP = {"sent", "connected", "replies", "positive"}
_STATUS = {"ACTIVE": "Active", "IN_PROGRESS": "Active", "PAUSED": "Paused",
           "COMPLETED": "Completed", "STOPPED": "Stopped", "DRAFTED": "Draft",
           "ARCHIVED": "Stopped"}


def _norm(name: str) -> str:
    return " ".join(str(name or "").lower().split())


def _us_date(iso: str) -> str:
    """'2026-10-27' -> '10/27/2026', the sheet's date style."""
    try:
        d = date.fromisoformat(str(iso)[:10])
    except ValueError:
        return ""
    return f"{d.month}/{d.day}/{d.year}"


def _numbers(r: dict) -> dict:
    """Dashboard values for one collected row; None = leave the cell alone."""
    email = r.get("channel") == "Email"
    leads = r.get("total_leads")
    out = {
        "status": _STATUS.get(str(r.get("status", "")).upper()),
        "leads": leads if isinstance(leads, int) else None,
        "sent": r.get("unique_sent") if email else r.get("life_invites"),
        "open": None,
        "connected": None if email else r.get("life_accepted"),
        "replies": r.get("life_replies"),
        "positive": r.get("life_positive") if email else None,
    }
    # Open % only where opens are tracked (a no-tracking campaign reports 0).
    if email and r.get("unique_opens") and r.get("unique_sent"):
        out["open"] = f"{100 * r['unique_opens'] / r['unique_sent']:.2f}%"
    return out


def plan(values: list[list[str]], formulas: list[list[str]], rows: list[dict],
         today: str) -> tuple[list[dict], list[list]]:
    """(cell updates, rows to append) for the dashboard.

    `values` / `formulas` are the tab read as displayed and as formulas.
    `rows` are collector rows for this client (Total rows are skipped).
    """
    def cell(grid, r, c):
        return grid[r - 1][c - 1] if r - 1 < len(grid) and c - 1 < len(grid[r - 1]) else ""

    where = {}
    for i, row in enumerate(values[1:], start=2):
        ch = cell(values, i, _COL["channel"])
        if ch in ("Email", "LinkedIn"):
            where.setdefault((_norm(cell(values, i, _COL["campaign"])), ch), i)

    updates, new_rows = [], []
    next_row = len(values) + 1
    for r in rows:
        if not r.get("channel") or not r.get("campaign"):
            continue
        nums = _numbers(r)
        at = where.get((_norm(r["campaign"]), r["channel"]))
        if at is None:
            line = [""] * _COL["change_date"]
            line[_COL["campaign"] - 1] = r["campaign"]
            line[_COL["channel"] - 1] = r["channel"]
            line[_COL["launched"] - 1] = _us_date(r.get("launch_date", ""))
            for k, v in nums.items():
                if v is not None:
                    line[_COL[k] - 1] = v
            n = next_row + len(new_rows)
            line[_COL["connect_pct"] - 1] = f'=IFERROR(L{n}/J{n},"")'
            line[_COL["reply_pct"] - 1] = f'=IFERROR(M{n}/J{n},"")'
            line[_COL["last_change"] - 1] = "New row"
            line[_COL["change_date"] - 1] = _us_date(today)
            new_rows.append(line)
            continue
        for k, v in nums.items():
            if v is None or str(cell(formulas, at, _COL[k])).startswith("="):
                continue
            current = cell(values, at, _COL[k])
            if k == "status" and current == "Blocked":
                continue
            if k in _ONLY_UP:
                try:
                    if float(str(current).replace(",", "")) >= float(v):
                        continue
                except ValueError:
                    pass
            if str(current).replace(",", "") != str(v):
                updates.append({"range": rowcol_to_a1(at, _COL[k]), "values": [[v]]})
        # Rows added by hand or by gtm launch can lack the rate formulas.
        for k, col in (("connect_pct", "L"), ("reply_pct", "M")):
            if not str(cell(formulas, at, _COL[k])).strip():
                updates.append({"range": rowcol_to_a1(at, _COL[k]),
                                "values": [[f'=IFERROR({col}{at}/J{at},"")']]})
    return updates, new_rows


def update_dashboard(sheet_id: str, rows: list[dict], today: str) -> tuple[int, int]:
    """Apply plan() to the live tab. Returns (cells changed, rows added)."""
    from smartlead.sheets import SheetsWriter, _retry_write
    ws = SheetsWriter(sheet_id)._sh.worksheet(DASHBOARD_TAB)
    width = rowcol_to_a1(1, _COL["change_date"]).rstrip("1")
    values = ws.get_all_values()
    formulas = ws.get(f"A1:{width}{max(len(values), 1)}", value_render_option="FORMULA")
    updates, new_rows = plan(values, formulas, rows, today)
    if updates:
        _retry_write(ws.batch_update, updates, value_input_option="USER_ENTERED")
    if new_rows:
        first = len(values) + 1
        _retry_write(ws.update, values=new_rows,
                     range_name=f"A{first}", value_input_option="USER_ENTERED")
    # Rows added below the hand-formatted block show rates as 0.0076; give the
    # rate columns the sheet's own 15.9% style.
    if updates or new_rows:
        _retry_write(ws.format, f"O2:P{len(values) + len(new_rows)}",
                     {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    return len(updates), len(new_rows)
