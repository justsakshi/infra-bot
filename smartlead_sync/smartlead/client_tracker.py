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
A campaign with no row yet is inserted just above the totals row (so the
totals count it, and it takes the campaign rows' styling), with the sheet's own
Connect % / Reply % formulas and "New row" in Last Change, the way the team
marks new rows. The person-written columns stay blank for Precise to fill.
"""
from __future__ import annotations

import re
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


def _channel(text: str) -> str:
    """'Email' / 'LinkedIn' whatever the casing ("Linkedin"), else ''."""
    return {"email": "Email", "linkedin": "LinkedIn"}.get(str(text or "").strip().lower(), "")


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
        # "-" on email rows, as the team writes it: the row's Connect %
        # formula then shows blank instead of 0.0%.
        "connected": "-" if email else r.get("life_accepted"),
        "replies": r.get("life_replies"),
        "positive": r.get("life_positive") if email else None,
    }
    # Open % only where opens are tracked (a no-tracking campaign reports 0).
    if email and r.get("unique_opens") and r.get("unique_sent"):
        out["open"] = f"{100 * r['unique_opens'] / r['unique_sent']:.2f}%"
    return out


def totals_row(formulas: list[list[str]]) -> int | None:
    """The row whose Leads cell is =SUM(...): campaign rows sit above it."""
    for i, row in enumerate(formulas, start=1):
        if len(row) >= _COL["leads"] and str(row[_COL["leads"] - 1]).upper().startswith("=SUM("):
            return i
    return None


def extend_totals(formula: str, old_end: int, added: int) -> str:
    """'=SUM(I2:I68)' -> '=SUM(I2:I78)' when 10 rows went in above row 69.
    Only ranges ending on the last campaign row move; others are left as written."""
    return re.sub(r"([A-Z]+)(\d+):([A-Z]+)(\d+)",
                  lambda m: (f"{m[1]}{m[2]}:{m[3]}{int(m[4]) + added}"
                             if int(m[4]) == old_end else m[0]), formula)


def plan(values: list[list[str]], formulas: list[list[str]], rows: list[dict],
         today: str, statuses: dict | None = None) -> tuple[list[dict], list[list]]:
    """(cell updates, rows to insert above the totals row) for the dashboard.

    `values` / `formulas` are the tab read as displayed and as formulas.
    `rows` are collector rows for this client (Total rows are skipped).
    `statuses` maps (campaign name, channel) to the platform status of every
    campaign, including ones the collector left out for being paused and
    quiet; without it such a row would say "Active" forever.
    """
    def cell(grid, r, c):
        return grid[r - 1][c - 1] if r - 1 < len(grid) and c - 1 < len(grid[r - 1]) else ""

    where = {}
    for i, row in enumerate(values[1:], start=2):
        ch = _channel(cell(values, i, _COL["channel"]))
        if ch:
            where.setdefault((_norm(cell(values, i, _COL["campaign"])), ch), i)

    updates, new_rows = [], []
    next_row = totals_row(formulas) or len(values) + 1
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
    reported = {(_norm(r["campaign"]), r["channel"]) for r in rows
                if r.get("channel") and r.get("campaign")}
    for (name, ch), status in (statuses or {}).items():
        key = (_norm(name), ch)
        at, new = where.get(key), _STATUS.get(str(status).upper())
        if at is None or key in reported or not new:
            continue
        current = cell(values, at, _COL["status"])
        if current not in ("Blocked", new):
            updates.append({"range": rowcol_to_a1(at, _COL["status"]), "values": [[new]]})
    return updates, new_rows


def update_dashboard(sheet_id: str, rows: list[dict], today: str,
                     statuses: dict | None = None) -> tuple[int, int]:
    """Apply plan() to the live tab. Returns (cells changed, rows added)."""
    from smartlead.sheets import SheetsWriter, _retry_write
    ws = SheetsWriter(sheet_id)._sh.worksheet(DASHBOARD_TAB)
    width = rowcol_to_a1(1, _COL["change_date"]).rstrip("1")
    lift_rows_above_totals(ws)
    values = ws.get_all_values()
    formulas = ws.get(f"A1:{width}{max(len(values), 1)}", value_render_option="FORMULA")
    updates, new_rows = plan(values, formulas, rows, today, statuses)
    if updates:
        _retry_write(ws.batch_update, updates, value_input_option="USER_ENTERED")
    if new_rows:
        insert_rows(ws, formulas, new_rows)
    return len(updates), len(new_rows)


def insert_rows(ws, formulas: list[list[str]], new_rows: list[list]) -> None:
    """Insert above the totals row, styled like the row above, totals extended."""
    from smartlead.sheets import _retry_write
    tot = totals_row(formulas)
    if tot is None:      # no totals row: plain append
        _retry_write(ws.append_rows, new_rows, value_input_option="USER_ENTERED")
        return
    _retry_write(ws.insert_rows, new_rows, row=tot, value_input_option="USER_ENTERED",
                 inherit_from_before=True)
    compact(ws, tot, tot + len(new_rows) - 1)
    _extend_totals_row(ws, tot + len(new_rows), tot - 1, len(new_rows))


def stray_rows(values: list[list[str]], formulas: list[list[str]]) -> list[int]:
    """Campaign rows below the totals row (gtm launch appends there, under the
    notes, so the totals miss them and they take the notes' grey italics)."""
    tot = totals_row(formulas)
    if tot is None:
        return []
    return [i for i in range(tot + 1, len(values) + 1)
            if len(values[i - 1]) >= _COL["channel"]
            and values[i - 1][_COL["channel"] - 1] in ("Email", "LinkedIn")]


def lift_rows_above_totals(ws) -> int:
    """Move stray campaign rows above the totals row, style them like the
    campaign row above, and extend the totals over them. Returns rows moved."""
    from smartlead.sheets import _retry_write
    sh, gid, moved = ws.spreadsheet, ws.id, 0
    first_tot = None
    while True:
        values = ws.get_all_values()
        formulas = ws.get(f"A1:U{max(len(values), 1)}", value_render_option="FORMULA")
        stray = stray_rows(values, formulas)
        tot = totals_row(formulas)
        if first_tot is None:
            first_tot = tot
        if not stray:
            break
        i = stray[0]
        _retry_write(sh.batch_update, {"requests": [{"moveDimension": {
            "source": {"sheetId": gid, "dimension": "ROWS", "startIndex": i - 1, "endIndex": i},
            "destinationIndex": tot - 1}}]})
        moved += 1
    if moved:
        last = first_tot - 1          # campaign rows used to end here
        _retry_write(sh.batch_update, {"requests": [{"copyPaste": {
            "source": {"sheetId": gid, "startRowIndex": last - 1, "endRowIndex": last,
                       "startColumnIndex": 0, "endColumnIndex": 21},
            "destination": {"sheetId": gid, "startRowIndex": last, "endRowIndex": last + moved,
                            "startColumnIndex": 0, "endColumnIndex": 21},
            "pasteType": "PASTE_FORMAT"}}]})
        _extend_totals_row(ws, first_tot + moved, last, moved)
        compact(ws, last + 1, last + moved)
    return moved


def compact(ws, first: int, last: int) -> None:
    """One line per row for rows first..last: text clipped, height 21 px.
    Copied campaign styling wraps text, and the long Target Titles cell then
    grows a row to ~10 lines (Manveen, 29 Oct: "can you crop this")."""
    from smartlead.sheets import _retry_write
    gid = ws.id
    _retry_write(ws.spreadsheet.batch_update, {"requests": [
        {"repeatCell": {"range": {"sheetId": gid, "startRowIndex": first - 1, "endRowIndex": last,
                                  "startColumnIndex": 0, "endColumnIndex": 21},
                        "cell": {"userEnteredFormat": {"wrapStrategy": "CLIP"}},
                        "fields": "userEnteredFormat.wrapStrategy"}},
        {"updateDimensionProperties": {"range": {"sheetId": gid, "dimension": "ROWS",
                                                 "startIndex": first - 1, "endIndex": last},
                                       "properties": {"pixelSize": 21}, "fields": "pixelSize"}}]})


def _extend_totals_row(ws, row: int, old_end: int, added: int) -> None:
    """Re-read the totals row where it now is and stretch its SUM ranges.

    Read after the insert or move, not before: Sheets has already shifted the
    row's references to itself (=N79/M79 became =N81/M81); writing back the
    pre-insert formulas would point them at a campaign row.
    """
    from smartlead.sheets import _retry_write
    current = ws.get(f"A{row}:U{row}", value_render_option="FORMULA")
    current = current[0] if current else []
    fixed = [extend_totals(c, old_end, added) if str(c).startswith("=") else c for c in current]
    if fixed != current:
        _retry_write(ws.update, values=[fixed], range_name=f"A{row}",
                     value_input_option="USER_ENTERED")
