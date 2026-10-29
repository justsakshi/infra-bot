"""Campaign History: every campaign's running totals, one row per day.

Campaign Metrics is rewritten each morning, so it only says where things stand
today. History keeps each morning's running totals (emails sent, invites,
accepted, replies, positives), so the progress between any two days is a
subtraction: the daily stats post reads it, and anyone can filter the tab by
campaign to watch it move (Manveen, 29 Oct 2026: "track progress of all my
campaigns periodically").

Rows are written by metrics_only.py at 09:00 IST and dated in IST. Re-running
on the same day replaces that day's rows instead of adding a second set.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

HEADER = ["Date", "Client", "Channel", "Platform", "Campaign", "Status", "Launched",
          "Leads", "Emails sent", "Bounced", "Invites sent", "Accepted", "Replies",
          "Positive/neutral", "Not interested", "Wrong person", "Campaign ID"]
FIELDS = ["date", "client", "channel", "platform", "campaign", "status", "launch_date",
          "leads", "sent", "bounced", "invites", "accepted", "replies",
          "positive", "not_interested", "wrong_person", "campaign_id"]
COUNTERS = ("sent", "bounced", "invites", "accepted", "replies", "positive",
            "not_interested", "wrong_person")
# Statuses that mean "running now" on each platform; anything else is listed
# in the post only when its numbers moved.
_RUNNING = {"ACTIVE", "IN_PROGRESS", "INPROGRESS", "RUNNING", "STARTED"}


def snapshot_day(now: datetime | None = None) -> str:
    """Today's date in IST, as YYYY-MM-DD."""
    return (now or datetime.now(IST)).astimezone(IST).date().isoformat()


def _cell(v):
    return "-" if v is None or v == "" else v


def history_rows(metric_rows: list[dict], day: str) -> list[list]:
    """Sheet rows for one day from collector rows (Total rows are skipped)."""
    out = []
    for r in metric_rows:
        if not r.get("client") or not r.get("channel"):
            continue
        out.append([
            day, r["client"], r["channel"], r.get("platform", ""), r.get("campaign", ""),
            r.get("status", ""), r.get("launch_date", ""), _cell(r.get("total_leads")),
            _cell(r.get("life_sent")), _cell(r.get("life_bounced")),
            _cell(r.get("life_invites")), _cell(r.get("life_accepted")),
            _cell(r.get("life_replies")), _cell(r.get("life_positive")),
            _cell(r.get("life_not_interested")), _cell(r.get("life_wrong_person")),
            str(r.get("campaign_id", "")),
        ])
    out.sort(key=lambda row: (row[1], row[2], row[4].lower()))
    return out


def _num(v) -> int | None:
    """A sheet cell as a count; '-' or blank (no such figure) is None."""
    s = str(v if v is not None else "").replace(",", "").strip()
    try:
        return int(float(s))
    except ValueError:
        return None


def parse_history(values: list[list[str]]) -> list[dict]:
    """Sheet values (header first) -> row dicts with counters as int / None."""
    if not values:
        return []
    pos = {label: i for i, label in enumerate(values[0])}
    out = []
    for raw in values[1:]:
        row = {}
        for field, label in zip(FIELDS, HEADER):
            i = pos.get(label)
            row[field] = raw[i] if i is not None and i < len(raw) else ""
        if not row["date"]:
            continue
        for f in COUNTERS:
            row[f] = _num(row[f])
        out.append(row)
    return out


def _row_ranges(row_numbers: list[int]) -> list[tuple[int, int]]:
    """[5, 6, 7, 10] -> [(5, 7), (10, 10)]: contiguous runs, for delete_rows."""
    runs: list[tuple[int, int]] = []
    for n in sorted(row_numbers):
        if runs and n == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], n)
        else:
            runs.append((n, n))
    return runs


def write_history(writer, rows: list[list], day: str, tab: str) -> int:
    """Replace `day`'s rows on the history tab with `rows`. Returns rows written.

    `writer` is a SheetsWriter (for its spreadsheet handle, tab lookup and
    write ledger).
    """
    from smartlead.sheets import _record_write, _retry_write
    try:
        ws = writer._get_or_create_shared_tab(tab, rows=1000, cols=len(HEADER))
        col_a = ws.col_values(1)
        if not col_a:
            _retry_write(ws.update, values=[HEADER], range_name="A1")
            ws.freeze(rows=1)
            col_a = [HEADER[0]]
        elif ws.row_values(1) != HEADER:
            # Columns changed (29 Oct: reply kinds added). Rows already on the
            # tab keep their old layout; only the first day predates this.
            _retry_write(ws.update, values=[HEADER], range_name="A1")
        same_day = [i + 1 for i, v in enumerate(col_a) if v == day]
        for start, end in reversed(_row_ranges(same_day)):
            _retry_write(ws.delete_rows, start, end)
        if rows:
            _retry_write(ws.append_rows, rows, value_input_option="RAW")
    except Exception as exc:
        _record_write(tab, error=exc)
        raise
    _record_write(tab, rows=len(rows))
    return len(rows)


# ── progress between two days ────────────────────────────────────────────────

def baseline_day(days: list[str], today: str) -> str | None:
    """The snapshot the post compares today against.

    The last post day: Friday for a Monday post (so it covers the weekend),
    yesterday otherwise. If that snapshot is missing, the latest one before it;
    failing that, the earliest one after it. None when today is the only day.
    """
    prior = sorted(d for d in set(days) if d < today)
    if not prior:
        return None
    t = date.fromisoformat(today)
    want = (t - timedelta(days=3 if t.weekday() == 0 else 1)).isoformat()
    at_or_before = [d for d in prior if d <= want]
    return at_or_before[-1] if at_or_before else prior[0]


def _key(r: dict) -> tuple[str, str]:
    return (r.get("platform", ""), r.get("campaign_id") or r.get("campaign", ""))


def campaign_changes(history: list[dict], today: str, base: str | None) -> list[dict]:
    """Today's campaigns with their totals and the change since `base`.

    `change` is None when the change cannot be known: the campaign is missing
    from the base day and was launched before it (it was filtered out then, so
    its whole history would wrongly count as new). A campaign launched on or
    after the base day starts from zero. A counter that went down (a lead
    re-categorised, a lead deleted) shows as 0, not a negative.
    """
    cur = [r for r in history if r["date"] == today]
    before = {_key(r): r for r in history if base and r["date"] == base}
    out = []
    for r in cur:
        totals = {f: r.get(f) for f in COUNTERS}
        b = before.get(_key(r))
        if b is None and base and (r.get("launch_date") or "") >= base:
            b = {f: 0 for f in COUNTERS}
        change = None
        if b is not None:
            change = {f: (max(0, totals[f] - (b.get(f) or 0))
                          if totals[f] is not None else None) for f in COUNTERS}
        out.append({**{k: r.get(k, "") for k in ("client", "channel", "platform",
                                                 "campaign", "status")},
                    "running": str(r.get("status", "")).upper() in _RUNNING,
                    "totals": totals, "change": change})
    return out
