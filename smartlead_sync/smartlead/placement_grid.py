"""Write placement results into the per-client deliverability grid.

The grid is the sheet the team actually works from, and the one
DeliverabilityReader parses. Its shape is positional: a date row, a row of
paired "G suite"/"Outlook" labels beneath it, then one row per domain.

Planning is separated from writing so the decisions that can corrupt the sheet
— which row, which column, whether a cell may be touched at all — are pure
functions that tests can pin down. Only `write_result` talks to Sheets.

Two rules the planner never breaks:

1. A non-empty cell is never overwritten. The team hand-corrects this sheet,
   and a re-run that clobbers a correction is worse than a missing result.
2. A domain is matched exactly. `bettrdatas.com` is a substring of
   `thebettrdatas.com`; a prefix match would silently write a verdict onto the
   wrong domain's row.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

_MONTHS = ("January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December")

_GSUITE = "g suite"
_OUTLOOK = "outlook"

# Which report provider feeds which column. The grid's pair is the *seed*
# provider, not the sending ESP — a Google domain legitimately has values in
# both columns.
_PROVIDER_COLUMN = {"G Suite": _GSUITE, "Office365": _OUTLOOK}


@dataclass
class GridHeader:
    date_row: int
    label_row: int
    pairs: dict[str, tuple[int, int]]  # date label -> (gsuite col, outlook col)
    width: int


@dataclass
class WritePlan:
    writes: list[tuple[int, int, str]] = field(default_factory=list)  # (row, col, value)
    new_column_label: str | None = None
    new_column_indices: tuple[int, int] | None = None
    skipped_reason: str = ""


def column_label_for(iso_date: str) -> str:
    """'2026-09-11' -> '11 September', matching the labels already in the sheet."""
    d = datetime.fromisoformat(iso_date[:10]).date()
    return f"{d.day} {_MONTHS[d.month - 1]}"


def parse_grid_header(rows: list[list[str]]) -> GridHeader | None:
    """Locate the date row, the G suite/Outlook label row, and the column pairs.

    Returns None when the tab does not have the expected shape. A caller that
    gets None must refuse to write rather than guess at column positions.
    """
    for ri, row in enumerate(rows[:10]):
        labels = [c.strip().lower() for c in row]
        if _GSUITE not in labels and _OUTLOOK not in labels:
            continue
        date_row = rows[ri - 1] if ri > 0 else []
        pairs: dict[str, tuple[int, int]] = {}
        # Walk the label row pairing each "G suite" with the "Outlook" that
        # follows it; the date sits above the pair's first column.
        pending_gsuite: int | None = None
        for ci, label in enumerate(labels):
            if label == _GSUITE:
                pending_gsuite = ci
            elif label == _OUTLOOK and pending_gsuite is not None:
                key = date_row[pending_gsuite].strip() if pending_gsuite < len(date_row) else ""
                if key:
                    pairs[key] = (pending_gsuite, ci)
                pending_gsuite = None
        return GridHeader(date_row=ri - 1, label_row=ri, pairs=pairs,
                          width=max(len(r) for r in rows))
    return None


def find_domain_row(rows: list[list[str]], domain: str) -> int | None:
    """Row index for a domain, matched exactly on column A (case-insensitive)."""
    target = domain.strip().lower()
    for ri, row in enumerate(rows):
        if row and row[0].strip().lower() == target:
            return ri
    return None


def _cell(rows: list[list[str]], ri: int, ci: int) -> str:
    row = rows[ri]
    return row[ci].strip() if ci < len(row) else ""


def plan_writes(rows: list[list[str]], domain: str, by_provider: dict,
                column_label: str, threshold: float) -> WritePlan:
    """Decide what to write for one domain, without touching the sheet.

    `by_provider` is `get_report()`'s breakdown. Each provider is judged on its
    own inbox percentage — never a blend, because a domain at 100% Google and
    0% Microsoft is half-dead, not average.
    """
    header = parse_grid_header(rows)
    if header is None:
        return WritePlan(skipped_reason="grid header not found")

    ri = find_domain_row(rows, domain)
    if ri is None:
        return WritePlan(skipped_reason="domain not in sheet")

    plan = WritePlan()
    cols = header.pairs.get(column_label)
    if cols is None:
        # Unseen date: one new pair at the end. Appending is the only mutation
        # to the grid's shape, and it happens at most once per run per tab.
        gs_col, ol_col = header.width, header.width + 1
        plan.new_column_label = column_label
        plan.new_column_indices = (gs_col, ol_col)
        existing = {}
    else:
        gs_col, ol_col = cols
        existing = {_GSUITE: _cell(rows, ri, gs_col), _OUTLOOK: _cell(rows, ri, ol_col)}

    col_for = {_GSUITE: gs_col, _OUTLOOK: ol_col}
    filled = 0
    for provider, stats in sorted(by_provider.items()):
        which = _PROVIDER_COLUMN.get(provider)
        if which is None:
            continue
        if existing.get(which):
            filled += 1
            continue
        verdict = "Inbox" if float(stats.get("inbox_pct", 0)) >= threshold else "spam"
        plan.writes.append((ri, col_for[which], verdict))

    if not plan.writes and filled:
        plan.skipped_reason = "cells already filled"
        # Nothing to write means nothing to create.
        plan.new_column_label = None
        plan.new_column_indices = None
    plan.writes.sort()
    return plan


def write_result(client_tab: str, domain: str, by_provider: dict,
                 when: str | None = None, threshold: float = 80.0,
                 dry_run: bool = False) -> WritePlan:
    """Apply a domain's result to its client tab. Never raises.

    A sheet failure must not strand a test or leave warmup off, so problems are
    logged and reported through the returned plan rather than propagated. Mongo
    is the durable record; this sheet is the team's report.
    """
    from smartlead.config import TEST_SHEET_ID

    label = column_label_for(when or date.today().isoformat())
    try:
        from smartlead.sheets import _authorize
        gc = _authorize()
        ws = gc.open_by_key(TEST_SHEET_ID).worksheet(client_tab)
        rows = ws.get_all_values()
    except Exception as exc:  # noqa: BLE001
        print(f"  [Grid] read {client_tab} failed (non-fatal): {exc}")
        return WritePlan(skipped_reason=f"sheet read failed: {exc}")

    plan = plan_writes(rows, domain, by_provider, label, threshold)
    if dry_run or not plan.writes:
        return plan

    try:
        if plan.new_column_indices:
            gs_col, ol_col = plan.new_column_indices
            ws.update_cell(plan_header_row(rows) + 1, gs_col + 1, label)
            ws.update_cell(plan_header_row(rows) + 2, gs_col + 1, "G suite")
            ws.update_cell(plan_header_row(rows) + 2, ol_col + 1, "Outlook")
        ws.batch_update([
            {"range": _a1(r, c), "values": [[v]]} for r, c, v in plan.writes
        ])
    except Exception as exc:  # noqa: BLE001
        print(f"  [Grid] write {client_tab}/{domain} failed (non-fatal): {exc}")
        return WritePlan(skipped_reason=f"sheet write failed: {exc}")
    return plan


def plan_header_row(rows: list[list[str]]) -> int:
    header = parse_grid_header(rows)
    return header.date_row if header else 0


def _a1(row: int, col: int) -> str:
    letters = ""
    c = col
    while True:
        letters = chr(ord("A") + c % 26) + letters
        c = c // 26 - 1
        if c < 0:
            break
    return f"{letters}{row + 1}"
