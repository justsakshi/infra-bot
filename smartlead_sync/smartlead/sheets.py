"""Google Sheets integration - read deliverability data and write dashboard tabs."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import gspread
import polars as pl
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from smartlead.config import (
    DELIVERABILITY_QUEUE_TAB_NAME,
    MASTER_TAB_NAME,
    SERVICE_ACCOUNT_FILE,
    TEST_SHEET_ID,
    TEST_TAB_NAME,
)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _parse_test_date(cell: str) -> str:
    """Parse a test-date header cell to 'YYYY-MM-DD', else ''.

    Handles formats like '15 Apr 2026', '07 May', '11 Jun', '2026-06-22',
    '22/06/2026', etc. Returns '' for non-date cells.
    """
    s = str(cell).strip()
    if not s:
        return ""
    formats = (
        "%d %b %Y", "%d %B %Y", "%d %b", "%d %B",
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year == 1900:  # year-less format -> assume current year
                dt = dt.replace(year=datetime.now().year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _age_days(date_str: str) -> int | None:
    """Return whole days since a YYYY-MM-DD date, else None."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(str(date_str)[:10])
    except ValueError:
        return None
    return (datetime.now() - dt).days


def _domain_from_email(email: str) -> str:
    if "@" not in str(email):
        return ""
    return str(email).split("@", 1)[1].strip().lower()


def _parse_number(value: object) -> float:
    try:
        return float(str(value).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _dedupe_inbox_rows(rows: list[dict]) -> list[dict]:
    """Return one row per (client, email), with active campaign count attached."""
    groups: dict[tuple, dict] = {}
    for r in rows:
        key = (r.get("client", ""), str(r.get("email", "")).strip().lower())
        g = groups.get(key)
        if g is None:
            g = {"row": r, "campaigns": 0}
            groups[key] = g
        campaign = str(r.get("campaign_name", ""))
        if campaign and not campaign.startswith("N/A"):
            g["campaigns"] += 1

    deduped: list[dict] = []
    for g in groups.values():
        row = dict(g["row"])
        row["campaigns"] = g["campaigns"]
        deduped.append(row)
    return deduped


def _queue_item(row: dict) -> dict | None:
    """Translate one master inbox row into a queue item when action is needed."""
    email = str(row.get("email", "")).strip()
    if not email:
        return None

    reasons = {
        r.strip()
        for r in str(row.get("busy_reason", "")).split(",")
        if r.strip()
    }
    status = str(row.get("test_sheet_status", "")).strip().lower()
    test_age = _age_days(str(row.get("test_date", "")))
    campaigns = int(_parse_number(row.get("campaigns", 0)))
    max_per_day = _parse_number(row.get("max_per_day", 0))
    true_load = _parse_number(row.get("true_load", 0))
    sent_today = _parse_number(row.get("sent_today", 0))
    live_load = max(true_load, sent_today)

    priority = ""
    reason = ""
    action = ""
    owner_skill = ""

    if "failed_test" in reasons or status in {"fail", "spam"}:
        priority = "P0"
        reason = "failed placement test"
        action = "Pause inbox/domain, run incident response, fix auth/copy/list, then retest."
        owner_skill = "deliverability-incident-response"
    elif "warmup_blocked" in reasons:
        priority = "P0"
        reason = "warmup blocked"
        action = "Investigate block reason; pause or retire inbox if it does not recover."
        owner_skill = "smartlead-inbox-manager"
    elif "disconnected" in reasons:
        priority = "P1"
        reason = "SMTP/IMAP disconnected"
        action = "Reconnect the inbox before any campaign assignment."
        owner_skill = "smartlead-inbox-manager"
    elif "low_rep" in reasons:
        priority = "P1"
        reason = "warmup reputation below threshold"
        action = "Keep out of campaigns; continue/adjust warmup and retest after reputation recovers."
        owner_skill = "smartlead-inbox-manager"
    elif "untested" in reasons or status in {"", "unknown"}:
        priority = "P1"
        reason = "no placement test on record"
        action = "Run initial GSuite + Outlook placement test before assigning to campaigns."
        owner_skill = "email-deliverability-audit"
    elif "stale_test" in reasons or status == "stale":
        priority = "P1"
        reason = "placement test is stale"
        action = "Run fresh placement test; keep unavailable until test result is inbox."
        owner_skill = "email-deliverability-audit"
    elif campaigns > 0 and live_load >= 25 and (test_age is None or test_age >= 7):
        priority = "P2"
        reason = "active high-volume inbox needs routine retest"
        action = "Retest before increasing volume; hold current cap until fresh inbox result."
        owner_skill = "deliverability-test-public"
    elif campaigns > 0 and max_per_day >= 35 and (test_age is None or test_age >= 7):
        priority = "P2"
        reason = "near max daily cap needs scale check"
        action = "Run placement check before using remaining capacity."
        owner_skill = "deliverability-test-public"
    else:
        return None

    return {
        "priority": priority,
        "client": row.get("client", ""),
        "email": email,
        "domain": _domain_from_email(email),
        "provider": row.get("provider", ""),
        "account_id": row.get("account_id", ""),
        "queue_reason": reason,
        "recommended_action": action,
        "owner_skill": owner_skill,
        "availability": row.get("availability", ""),
        "busy_reason": row.get("busy_reason", ""),
        "campaigns": campaigns,
        "max_per_day": row.get("max_per_day", ""),
        "sent_today": row.get("sent_today", ""),
        "true_load": row.get("true_load", ""),
        "available_capacity": row.get("available_capacity", ""),
        "warmup_state": row.get("warmup_state", ""),
        "warmup_rep_pct": row.get("warmup_rep_pct", ""),
        "test_sheet_status": row.get("test_sheet_status", ""),
        "test_date": row.get("test_date", ""),
    }


def build_deliverability_queue(rows: list[dict]) -> list[dict]:
    """Build the cross-account action queue from raw inbox rows."""
    queue = [
        item
        for row in _dedupe_inbox_rows(rows)
        for item in [_queue_item(row)]
        if item is not None
    ]
    priority_order = {"P0": 0, "P1": 1, "P2": 2}
    return sorted(
        queue,
        key=lambda r: (
            priority_order.get(str(r.get("priority")), 9),
            str(r.get("client", "")),
            str(r.get("domain", "")),
            str(r.get("email", "")),
        ),
    )



def _authorize() -> gspread.Client:
    import json
    sa_json = os.environ.get("SMARTLEAD_SERVICE_ACCOUNT_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
    else:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=_SCOPES)
    return gspread.authorize(creds)


# ── Colors ───────────────────────────────────────────────────────────────────

_COLORS = {
    "header_bg":    {"red": 0.16, "green": 0.16, "blue": 0.22},   # dark charcoal
    "header_fg":    {"red": 1.0,  "green": 1.0,  "blue": 1.0},    # white text
    "alt_row":      {"red": 0.95, "green": 0.96, "blue": 0.98},   # light blue-gray
    "white":        {"red": 1.0,  "green": 1.0,  "blue": 1.0},
    "green_bg":     {"red": 0.85, "green": 0.95, "blue": 0.85},   # FREE / ACTIVE
    "red_bg":       {"red": 0.97, "green": 0.85, "blue": 0.85},   # BUSY / fail
    "yellow_bg":    {"red": 1.0,  "green": 0.96, "blue": 0.80},   # PAUSED / warning
    "gray_bg":      {"red": 0.92, "green": 0.92, "blue": 0.92},   # DRAFTED / N/A
    "green_text":   {"red": 0.13, "green": 0.55, "blue": 0.13},
    "red_text":     {"red": 0.80, "green": 0.12, "blue": 0.12},
    "orange_text":  {"red": 0.80, "green": 0.50, "blue": 0.0},
    "gray_text":    {"red": 0.50, "green": 0.50, "blue": 0.50},
}

# Human-readable header labels per tab
_HEADER_LABELS = {
    "Campaign Summary": {
        "campaign_id": "Campaign ID",
        "name": "Campaign Name",
        "status": "Status",
        "total_leads": "Total Leads",
        "unique_sent": "Unique Sent",
        "reach_pct": "Reach %",
        "sent": "Sent",
        "opened": "Opened",
        "replied": "Replied",
        "bounced": "Bounced",
        "not_started": "Not Started",
        "in_progress": "In Progress",
        "paused": "Paused",
        "completed": "Completed",
        "stopped": "Stopped",
    },
    "Inboxes": {
        "email": "Email",
        "name": "Name",
        "provider": "Provider",
        "campaign_name": "Campaign",
        "campaign_status": "Camp. Status",
        "daily_limit": "Daily Limit",
        "leads_remaining": "Leads Remaining",
        "total_inboxes": "Inbox Count",
        "individual_load": "Indiv. Leads",
        "true_load": "True Load",
        "available_capacity": "Avail. Capacity",
        "warmup_rep_pct": "Warmup Rep %",
        "test_sheet_status": "Test Status",
        "test_date": "Test Date",
        "availability": "Availability",
        "busy_reason": "Busy Reason",
        "account_id": "Account ID",
    },
    "Warmup Reputation": {
        "email": "Email",
        "name": "Name",
        "provider": "Provider",
        "warmup_enabled": "Warmup On",
        "daily_limit": "Daily Limit",
        "warmup_limit": "Warmup Limit",
        "warmup_sent": "Warmup Sent",
        "landed_inbox": "Landed Inbox",
        "landed_spam": "Landed Spam",
        "warmup_reputation": "Reputation %",
    },
    "All Inboxes": {
        "client": "Client",
        "email": "Email",
        "name": "Name",
        "provider": "Provider",
        "account_id": "Account ID",
        "availability": "Availability",
        "busy_reason": "Busy Reason",
        "campaigns": "# Campaigns",
        "connection_ok": "Connection OK",
        "max_per_day": "Max/Day",
        "sent_today": "Sent Today",
        "capacity_left": "Capacity Left",
        "true_load": "True Load",
        "available_capacity": "Avail. Capacity",
        "warmup_state": "Warmup State",
        "warmup_rep_pct": "Warmup Rep %",
        "warmup_max_count": "Warmup Max",
        "last_active_date": "Last Active",
        "test_sheet_status": "Test Status",
        "test_date": "Test Date",
    },
    "Deliverability Queue": {
        "priority": "Priority",
        "client": "Client",
        "email": "Email",
        "domain": "Domain",
        "provider": "Provider",
        "account_id": "Account ID",
        "queue_reason": "Queue Reason",
        "recommended_action": "Recommended Action",
        "owner_skill": "Owner Skill",
        "availability": "Availability",
        "busy_reason": "Busy Reason",
        "campaigns": "# Campaigns",
        "max_per_day": "Max/Day",
        "sent_today": "Sent Today",
        "true_load": "True Load",
        "available_capacity": "Avail. Capacity",
        "warmup_state": "Warmup State",
        "warmup_rep_pct": "Warmup Rep %",
        "test_sheet_status": "Test Status",
        "test_date": "Test Date",
    },
    "Campaign Metrics": {
        "campaign": "Campaign name", "platform": "Platform", "status": "Campaign Status",
        "total_leads": "Total leads", "leads_added_month": "Leads added this month",
        "leads_added_yesterday": "Leads added yesterday", "leads_in_progress": "Leads in progress",
        "connections_sent": "Connections sent", "connections_accepted": "Connections accepted",
        "msg_sent": "Msg Sent", "positive_responses_yesterday": "Positive Responses Yesterday",
        "total_responses_month": "Total Responses this month",
        "positive_neutral_month": "Positive/ Neutral responses this month",
    },
    "Inbox Health": {
        "priority": "Priority", "client": "Client", "email": "Email", "domain": "Domain",
        "provider": "Provider", "score": "Health Score", "grade": "Grade", "trend": "Trend (7d)",
        "status": "Status", "top_problem": "Top Problem", "what_to_do": "What To Do",
        "owner": "Owner", "how_long": "How Long", "manager": "Manager", "drivers": "Score Drivers",
        "warmup_rep_pct": "Warmup Rep %", "test_sheet_status": "Test Status", "test_date": "Test Date",
        "campaigns": "# Campaigns", "owner_skill": "Owner Skill",
    },
}

# Column widths (pixels) per tab - only specify overrides, rest get a default
_COL_WIDTHS = {
    "Campaign Summary": {"name": 260, "status": 110, "reach_pct": 90},
    "Inboxes":          {"email": 250, "campaign_name": 230, "daily_limit": 110, "availability": 110, "true_load": 90, "available_capacity": 120},
    "Warmup Reputation": {"email": 250, "warmup_reputation": 110},
    "All Inboxes": {"client": 140, "email": 250, "availability": 110, "busy_reason": 200, "campaigns": 95, "warmup_state": 120, "last_active_date": 110},
    "Deliverability Queue": {"priority": 80, "client": 140, "email": 250, "queue_reason": 220, "recommended_action": 420, "owner_skill": 190, "busy_reason": 200},
    "Campaign Metrics": {"campaign": 320, "platform": 100, "status": 130},
    "Inbox Health": {"priority": 80, "client": 130, "email": 250, "score": 90, "grade": 70,
                     "trend": 100, "status": 130, "top_problem": 220, "what_to_do": 420,
                     "owner": 90, "manager": 130, "drivers": 320},
}
_DEFAULT_COL_WIDTH = 120


# ── Deliverability Reader (Belardiwong tab) ──────────────────────────────────

class DeliverabilityReader:
    """Reads domain -> inbox/fail status from the test-results sheet."""

    def __init__(
        self,
        sheet_id: str = TEST_SHEET_ID,
        tab_name: str = TEST_TAB_NAME,
    ) -> None:
        self.sheet_id = sheet_id
        self.tab_name = tab_name

    async def fetch(self) -> dict[str, dict]:
        """Return ``{domain: {"status": str, "date": "YYYY-MM-DD"|""}}`` mapping.

        ``date`` is the most recent test-column date that fed the result, used
        downstream to flag stale tests. Runs synchronous gspread under the hood.
        """
        print(f"  [Sheets] Reading deliverability from '{self.tab_name}' tab ...")
        try:
            gc = _authorize()
            sh = gc.open_by_key(self.sheet_id)
            ws = sh.worksheet(self.tab_name)
            rows = ws.get_all_values()
        except Exception as exc:
            print(f"  [!] Could not read '{self.tab_name}': {type(exc).__name__}: {exc}")
            return {}

        # Find the row that contains "G suite" / "Outlook" column labels (search up to row 10)
        type_row_idx = -1
        gsuite_cols: list[int] = []
        outlook_cols: list[int] = []
        for ri, row in enumerate(rows[:10]):  # Search only first 10 rows
            labels = [c.strip().lower() for c in row]
            if "g suite" in labels or "outlook" in labels:
                type_row_idx = ri
                for ci, label in enumerate(labels):
                    if label == "g suite":
                        gsuite_cols.append(ci)
                    elif label == "outlook":
                        outlook_cols.append(ci)
                break

        if type_row_idx == -1:
            print(f"  [!] Could not find G suite/Outlook header row in '{self.tab_name}'")
            return {}

        # The row directly above the type-label row holds the test dates per column
        date_row = rows[type_row_idx - 1] if type_row_idx > 0 else []
        col_dates: dict[int, str] = {}
        for ci in gsuite_cols + outlook_cols:
            if ci < len(date_row):
                parsed = _parse_test_date(date_row[ci])
                if parsed:
                    col_dates[ci] = parsed

        # Data rows start after the type-label row
        data_start = type_row_idx + 1

        mapping: dict[str, dict] = {}
        for row in rows[data_start:]:
            if not row or not row[0]:
                continue
            domain = row[0].strip().lower()
            if not domain or "." not in domain:
                continue

            # Skip rows that are just ESP provider info (no actual test results)
            has_test_result = any(
                (ci < len(row) and row[ci].strip().lower() in ["inbox", "fail"])
                for ci in gsuite_cols + outlook_cols
            )
            if not has_test_result:
                continue

            # Find latest non-empty GSuite and Outlook results (with their columns)
            gs_val, gs_col = self._latest_result(row, gsuite_cols)
            ol_val, ol_col = self._latest_result(row, outlook_cols)

            # Both must be inbox for "inbox"; if either is spam/fail → "fail"
            status = self._merge_provider_status(gs_val, ol_val)

            # Date = most recent test column that contributed a value
            dates = [col_dates[c] for c in (gs_col, ol_col) if c is not None and c in col_dates]
            test_date = max(dates) if dates else ""
            mapping[domain] = {"status": status, "date": test_date}

        print(f"  [Sheets] Loaded {len(mapping)} domain statuses.")
        return mapping

    @staticmethod
    def _latest_result(row: list[str], cols: list[int]) -> tuple[str, int | None]:
        """Return ``(value, col_index)`` of the latest non-empty value (right to left)."""
        for ci in reversed(cols):
            if ci < len(row):
                val = row[ci].strip().lower()
                if val and val not in ("-", ""):
                    return val, ci
        return "", None

    @staticmethod
    def _merge_provider_status(gsuite: str, outlook: str) -> str:
        """Combine GSuite and Outlook results: both must be inbox for 'inbox'."""
        fail_values = ("spam", "low reputation", "disqualified")

        gs_fail = gsuite in fail_values
        ol_fail = outlook in fail_values
        gs_inbox = "inbox" in gsuite
        ol_inbox = "inbox" in outlook

        # If either provider fails → fail
        if gs_fail or ol_fail:
            return "fail"
        # Both inbox → inbox
        if gs_inbox and ol_inbox:
            return "inbox"
        # Only one provider has data — use it
        if gs_inbox and not outlook:
            return "inbox"
        if ol_inbox and not gsuite:
            return "inbox"
        # Fallback
        return gsuite or outlook or "Unknown"


# ── Dashboard Writer ─────────────────────────────────────────────────────────

class SheetsWriter:
    """Writes Campaign Summary / Inboxes / Warmup Reputation tabs to a Google Sheet."""

    def __init__(self, sheet_id: str, account_name: str | None = None) -> None:
        self.sheet_id = sheet_id
        self.prefix = f"{account_name} - " if account_name else ""
        self._gc = _authorize()
        try:
            self._sh = self._gc.open_by_key(sheet_id)
        except Exception as exc:
            raise RuntimeError(f"Cannot open sheet {sheet_id}: {exc}") from exc

    # ── public API ───────────────────────────────────────────────────────

    def write_campaign_summary(self, data: list[dict]) -> None:
        self._write_tab("Campaign Summary", data)

    def write_inboxes(self, data: list[dict]) -> None:
        self._write_tab("Inboxes", data)

    def write_warmup(self, data: list[dict]) -> None:
        if data:
            self._write_tab("Warmup Reputation", data)

    def write_glossary(self) -> None:
        self._write_glossary_tab()

    def write_sync_timestamp(self, sync_ts: str | None = None) -> None:
        ts = sync_ts or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._write_sync_timestamp(ts)

    def write_all(
        self,
        campaign_summary: list[dict],
        inbox_data: list[dict],
        warmup_data: list[dict],
    ) -> None:
        url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}"
        print(f"  [Sheets] Syncing to {url} ...")
        self.write_campaign_summary(campaign_summary)
        self.write_inboxes(inbox_data)
        self.write_warmup(warmup_data)
        print(f"  [Sheets] Done - summary={len(campaign_summary)}, inboxes={len(inbox_data)}, warmup={len(warmup_data)}")

    MASTER_COLUMNS = [
        "client", "email", "name", "provider", "account_id",
        "availability", "busy_reason", "campaigns",
        "connection_ok", "max_per_day", "sent_today",
        "capacity_left", "true_load", "available_capacity",
        "warmup_state", "warmup_rep_pct", "warmup_max_count",
        "last_active_date", "test_sheet_status", "test_date",
    ]

    DELIVERABILITY_QUEUE_COLUMNS = [
        "priority", "client", "email", "domain", "provider", "account_id",
        "queue_reason", "recommended_action", "owner_skill",
        "availability", "busy_reason", "campaigns",
        "max_per_day", "sent_today", "true_load", "available_capacity",
        "warmup_state", "warmup_rep_pct", "test_sheet_status", "test_date",
    ]

    def write_master_inboxes(self, rows: list[dict]) -> None:
        """Write the flat cross-client 'All Inboxes' tab.

        Dedupes to ONE row per (client, email) — an inbox attached to several
        campaigns appears once — and records how many active campaigns it's in
        (``campaigns``). This is the clean selection source the downstream tool
        reads: each inbox listed once with its availability + usable volume.
        """
        from smartlead.config import MASTER_TAB_NAME
        if not rows:
            print("  [Sheets] No inbox rows for master tab — skipping.")
            return

        deduped = _dedupe_inbox_rows(rows)

        ordered = sorted(
            deduped, key=lambda r: (r.get("client", ""), r.get("availability", ""), r.get("email", "")),
        )
        projected = [{c: r.get(c, "") for c in self.MASTER_COLUMNS} for r in ordered]
        self._write_tab(MASTER_TAB_NAME, projected)
        print(f"  [Sheets] master dedup: {len(rows)} campaign-rows -> {len(deduped)} unique inboxes")

    def write_deliverability_queue(self, rows: list[dict]) -> None:
        """Write the cross-account work queue for placement tests and incidents."""
        if not rows:
            print("  [Sheets] No inbox rows for deliverability queue - skipping.")
            return
        queue = build_deliverability_queue(rows)
        projected = [{c: r.get(c, "") for c in self.DELIVERABILITY_QUEUE_COLUMNS} for r in queue]
        self._write_tab(DELIVERABILITY_QUEUE_TAB_NAME, projected)
        print(f"  [Sheets] deliverability queue written: {len(queue)} action item(s)")

    INBOX_HEALTH_COLUMNS = [
        "priority", "client", "email", "domain", "provider", "score", "grade", "trend",
        "status", "top_problem", "what_to_do", "owner", "how_long", "manager", "drivers",
        "warmup_rep_pct", "test_sheet_status", "test_date", "campaigns", "owner_skill",
    ]

    def write_inbox_health(self, rows: list[dict]) -> None:
        """Write the 'Inbox Health' workbook tab."""
        from smartlead.config import INBOX_HEALTH_TAB_NAME
        if not rows:
            print("  [Sheets] No inbox health rows - skipping.")
            return
        projected = [{c: r.get(c, "") for c in self.INBOX_HEALTH_COLUMNS} for r in rows]
        self._write_tab(INBOX_HEALTH_TAB_NAME, projected)
        print(f"  [Sheets] Inbox Health tab written: {len(rows)} inboxes")

    def write_campaign_metrics(self, rows: list[dict], month_name: str | None = None) -> None:
        """Write the 'Campaign Metrics' tab (Smartlead + HeyReach campaigns)."""
        from smartlead.config import CAMPAIGN_METRICS_TAB_NAME
        from smartlead.campaign_metrics import COLUMNS
        if not rows:
            print("  [Sheets] No campaign metrics rows - skipping.")
            return
        projected = [{c: r.get(c, "") for c in COLUMNS} for r in rows]
        custom_headers = {}
        if month_name:
            custom_headers["leads_added_month"] = f"Leads added in {month_name}"
        self._write_tab(CAMPAIGN_METRICS_TAB_NAME, projected, custom_headers=custom_headers)
        print(f"  [Sheets] Campaign Metrics tab written: {len(rows)} rows")

    # ── internals ────────────────────────────────────────────────────────

    def _get_or_create_tab(self, title: str, rows: int = 500, cols: int = 20) -> Any:
        full_title = f"{self.prefix}{title}"
        try:
            return self._sh.worksheet(full_title)
        except gspread.exceptions.WorksheetNotFound:
            return self._sh.add_worksheet(title=full_title, rows=str(rows), cols=str(cols))

    def _get_or_create_shared_tab(self, title: str, rows: int = 500, cols: int = 20) -> Any:
        """Create/get a tab without the account prefix (shared across accounts)."""
        try:
            return self._sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            return self._sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))

    def _write_tab(self, tab_key: str, data: list[dict], custom_headers: dict[str, str] | None = None) -> None:
        ws = self._get_or_create_tab(tab_key)
        ws.clear()
        if not data:
            return

        df = pl.DataFrame(data)
        columns = df.columns
        num_cols = len(columns)
        num_rows = len(data) + 1  # +1 for header

        # Build header row with pretty labels
        labels = dict(_HEADER_LABELS.get(tab_key, {}))
        if custom_headers:
            labels.update(custom_headers)
        header = [labels.get(c, c.replace("_", " ").title()) for c in columns]
        body = [[_serialize(v) for v in row.values()] for row in data]
        ws.update(values=[header] + body, range_name="A1")

        # ── Batch format requests ────────────────────────────────────────
        sheet_id = ws.id
        requests: list[dict] = []

        # 1. Freeze header row
        requests.append(_freeze_rows(sheet_id, 1))

        # 2. Header style: dark bg, white bold text, centered
        requests.append(_format_range(
            sheet_id, 0, 1, 0, num_cols,
            bg=_COLORS["header_bg"],
            fg=_COLORS["header_fg"],
            bold=True,
            h_align="CENTER",
            wrap="WRAP",
        ))

        # 3. Alternating row colors (data rows)
        for r in range(1, num_rows):
            bg = _COLORS["alt_row"] if r % 2 == 0 else _COLORS["white"]
            requests.append(_format_range(
                sheet_id, r, r + 1, 0, num_cols, bg=bg,
            ))

        # 4. Column widths
        widths = _COL_WIDTHS.get(tab_key, {})
        for ci, col_name in enumerate(columns):
            px = widths.get(col_name, _DEFAULT_COL_WIDTH)
            requests.append(_col_width(sheet_id, ci, px))

        # 5. Conditional cell coloring based on tab type
        requests.extend(self._status_colors(sheet_id, tab_key, columns, data))

        # 6. Borders around entire data range
        requests.append(_outer_border(sheet_id, 0, num_rows, 0, num_cols))

        # Execute all formatting in one batch
        self._sh.batch_update({"requests": requests})

    def _write_glossary_tab(self) -> None:
        """Write a 'Column Glossary' tab explaining every column across all tabs."""
        ws = self._get_or_create_shared_tab("Column Glossary", rows=60, cols=4)
        ws.clear()

        rows: list[list[str]] = [
            ["Tab", "Column", "Description", "Values / Rules"],
            # ── Campaign Summary ─────────────────────────────
            ["Campaign Summary", "Campaign ID", "Unique Smartlead campaign identifier", ""],
            ["", "Campaign Name", "Name of the email campaign", ""],
            ["", "Status", "Current campaign state", "ACTIVE, PAUSED, COMPLETED, STOPPED, DRAFTED, ARCHIVED"],
            ["", "Total Leads", "Total number of leads loaded into the campaign", ""],
            ["", "Unique Sent", "Number of unique leads who received at least one email", ""],
            ["", "Reach %", "Percentage of total leads reached (Unique Sent / Total Leads)", ""],
            ["", "Sent", "Total emails sent (including follow-ups)", ""],
            ["", "Opened", "Number of emails opened", ""],
            ["", "Replied", "Number of replies received", ""],
            ["", "Bounced", "Number of emails that bounced", ""],
            ["", "Not Started", "Leads that haven't been emailed yet", ""],
            ["", "In Progress", "Leads currently in an active email sequence", ""],
            ["", "Paused", "Leads whose sequence is paused", ""],
            ["", "Completed", "Leads who finished the full email sequence", ""],
            ["", "Stopped", "Leads whose sequence was manually stopped", ""],
            # ── Inboxes ──────────────────────────────────────
            ["Inboxes", "Email", "Sending email address (inbox)", ""],
            ["", "Name", "Display name on the inbox", ""],
            ["", "Provider", "Email provider", "Gmail, Outlook, Other"],
            ["", "Campaign", "Campaign this inbox is assigned to", "'N/A (No active campaign)' if not in any active campaign"],
            ["", "Camp. Status", "Status of the assigned campaign", "ACTIVE, PAUSED, N/A"],
            ["", "Daily Limit", "Emails sent today vs. daily cap", "Format: sent / limit"],
            ["", "Leads Remaining", "Not Started + In Progress leads in the campaign", ""],
            ["", "Inbox Count", "Number of inboxes assigned to the same campaign", ""],
            ["", "Indiv. Leads", "Leads per inbox (Leads Remaining / Inbox Count)", ""],
            ["", "True Load", "Aggregated daily sending load across all campaigns (campaign daily limit / inbox count, summed)", "Higher = more utilized"],
            ["", "Avail. Capacity", "Remaining daily send capacity (inbox daily limit - True Load)", "0 if overloaded"],
            ["", "Warmup Rep %", "Warmup reputation percentage from Smartlead", ">=90% is healthy (green), <90% is at risk (red)"],
            ["", "Test Status", "Deliverability test result (latest GSuite + Outlook)", "inbox = both land inbox, fail = either lands spam"],
            ["", "Availability", "Whether inbox is available for new campaigns", "FREE = capacity > 0 AND rep >= 90% AND test = inbox; otherwise BUSY"],
            ["", "Account ID", "Smartlead email account ID", ""],
            # ── Warmup Reputation ────────────────────────────
            ["Warmup Reputation", "Email", "Email address", ""],
            ["", "Name", "Display name", ""],
            ["", "Provider", "Email provider", "Gmail, Outlook, Other"],
            ["", "Warmup On", "Whether warmup is currently active", "Yes / No"],
            ["", "Daily Limit", "Emails sent today vs. daily cap", "Format: sent / limit"],
            ["", "Warmup Limit", "Max warmup emails per day", ""],
            ["", "Warmup Sent", "Total warmup emails sent (all time)", ""],
            ["", "Landed Inbox", "Warmup emails that landed in inbox", ""],
            ["", "Landed Spam", "Warmup emails that landed in spam", ""],
            ["", "Reputation %", "Warmup reputation (inbox / total sent)", ">=90% is healthy, <90% needs attention"],
            # ── All Inboxes (master) ─────────────────────────
            ["All Inboxes", "Client", "Which client the inbox belongs to", ""],
            ["", "Email / Name / Provider", "Sending address, display name, provider", "Gmail, Outlook, Other"],
            ["", "Availability", "Can this inbox take a new campaign?", "FREE = capacity + rep>=90% + fresh 'inbox' test + connected; else BUSY"],
            ["", "Busy Reason", "Why the inbox is not FREE", "disconnected, warmup_blocked, no_capacity, low_rep, stale_test, failed_test, untested"],
            ["", "# Campaigns", "How many active campaigns use this inbox", ""],
            ["", "Warmup State", "Warmup lifecycle stage", "off / warming / ramped / blocked"],
            ["", "Last Active", "Last date the inbox actually sent", "YYYY-MM-DD"],
            ["", "Test Status / Test Date", "Latest placement test result + when", "inbox / fail / stale (>14 days old) / untested"],
            # ── Inbox Health (the workbook) ──────────────────
            ["Inbox Health", "Priority", "Urgency of the row", "P0 = today, P1 = this week, P2 = routine, blank = healthy"],
            ["", "Health Score", "Daily 0-100 health score", "placement 40 + warmup 25 + bounce 20 + connection 15"],
            ["", "Grade", "Score band, color-coded", "A 90+ (green), B 70-89, C 50-69 (yellow), D <50 (red)"],
            ["", "Trend (7d)", "Score change vs 7 days ago", "↑ improving, ↓ declining (act before it fails), — no history yet"],
            ["", "Status", "One-word state", "healthy / needs_action / broken"],
            ["", "Top Problem", "The #1 thing wrong with this inbox", ""],
            ["", "What To Do", "The exact fix instruction - follow it as written", ""],
            ["", "Owner", "Who handles it", "\U0001F916 Auto = robot fixes it (skip), \U0001F464 You = human action needed"],
            ["", "How Long", "Expected fix time", ""],
            ["", "Manager", "Person responsible for this client's inboxes", "Balasankar / Anjali / Varsha"],
            ["", "Score Drivers", "Points breakdown behind the score", "test x/40 - warmup x/25 - bounce x/20 - conn x/15"],
            ["", "Owner Skill", "Playbook used to fix this problem type", ""],
            # ── Campaign Metrics ─────────────────────────────
            ["Campaign Metrics", "Campaign / Platform / Status", "Campaign name, Smartlead (email) or Heyreach (LinkedIn), state", ""],
            ["", "Total leads", "Leads loaded in the campaign", ""],
            ["", "Leads added this month / yesterday", "New leads added in the reporting window", ""],
            ["", "Leads in progress", "Leads currently mid-sequence", ""],
            ["", "Connections sent / accepted", "LinkedIn connection requests (HeyReach only)", "'-' for Smartlead"],
            ["", "Msg Sent", "Emails / LinkedIn messages sent", "COMPLETED campaigns show month totals from analytics"],
            ["", "Total Responses this month", "All replies in the reporting month", "API counts include auto-replies"],
            ["", "Positive/Neutral responses", "Interested + Meeting Request + Info Request", "auto-categorized; humans can refine"],
            # ── API Tests (deliverability sheet) ─────────────
            ["API Tests (deliverability sheet)", "Date / Client / Domain", "When the robot ran a placement test + for whom", ""],
            ["", "Result", "Where the test emails landed", "inbox (>=80% inboxed) or fail"],
            ["", "Inbox % / Spam %", "Exact placement percentages", ""],
            ["", "Test ID / Emails Tested", "Smartlead test id + how many senders were seeded", ""],
        ]

        # ensure the grid is big enough for the grown glossary
        try:
            ws.resize(rows=len(rows) + 5)
        except Exception:  # noqa: BLE001 - resize is best-effort
            pass
        ws.update(values=rows, range_name="A1")

        # Format
        sheet_id = ws.id
        num_rows = len(rows)
        requests: list[dict] = [
            _freeze_rows(sheet_id, 1),
            _format_range(sheet_id, 0, 1, 0, 4,
                          bg=_COLORS["header_bg"], fg=_COLORS["header_fg"],
                          bold=True, h_align="CENTER", wrap="WRAP"),
            _col_width(sheet_id, 0, 160),
            _col_width(sheet_id, 1, 150),
            _col_width(sheet_id, 2, 420),
            _col_width(sheet_id, 3, 320),
            _outer_border(sheet_id, 0, num_rows, 0, 4),
        ]

        # Alternating row colors
        for r in range(1, num_rows):
            bg = _COLORS["alt_row"] if r % 2 == 0 else _COLORS["white"]
            requests.append(_format_range(sheet_id, r, r + 1, 0, 4, bg=bg))

        # Bold the tab name cells (first column, non-empty)
        for r, row in enumerate(rows[1:], start=1):
            if row[0]:
                requests.append(_format_range(sheet_id, r, r + 1, 0, 1, bold=True))

        self._sh.batch_update({"requests": requests})

    def _write_sync_timestamp(self, sync_ts: str) -> None:
        """Write a 'Last Sync' tab with the sync timestamp."""
        ws = self._get_or_create_shared_tab("Last Sync", rows=5, cols=2)
        ws.clear()
        ws.update(values=[
            ["Last Synced", sync_ts],
        ], range_name="A1")

        sheet_id = ws.id
        requests = [
            _format_range(sheet_id, 0, 1, 0, 1, bold=True, h_align="LEFT"),
            _col_width(sheet_id, 0, 120),
            _col_width(sheet_id, 1, 200),
        ]
        self._sh.batch_update({"requests": requests})

    def _status_colors(
        self, sheet_id: int, tab_key: str, columns: list[str], data: list[dict],
    ) -> list[dict]:
        """Return format requests that color-code status/availability cells."""
        from smartlead.config import CAMPAIGN_METRICS_TAB_NAME, INBOX_HEALTH_TAB_NAME
        reqs: list[dict] = []

        if tab_key == INBOX_HEALTH_TAB_NAME and "grade" in columns:
            col_idx = columns.index("grade")
            palette = {"A": (_COLORS["green_bg"], _COLORS["green_text"]),
                       "B": (_COLORS["green_bg"], _COLORS["green_text"]),
                       "C": (_COLORS["yellow_bg"], _COLORS["orange_text"]),
                       "D": (_COLORS["red_bg"], _COLORS["red_text"])}
            for ri, row in enumerate(data, start=1):
                bg, fg = palette.get(str(row.get("grade", "")).upper(), (None, None))
                if bg:
                    reqs.append(_format_range(sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                                              bg=bg, fg=fg, bold=True, h_align="CENTER"))

        if tab_key == CAMPAIGN_METRICS_TAB_NAME and "status" in columns:
            col_idx = columns.index("status")
            for ri, row in enumerate(data, start=1):
                bg, fg = _status_style(str(row.get("status", "")).upper())
                if bg:
                    reqs.append(_format_range(sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                                              bg=bg, fg=fg, bold=True, h_align="CENTER"))

        if tab_key == "Campaign Summary" and "status" in columns:
            col_idx = columns.index("status")
            for ri, row in enumerate(data, start=1):
                status = str(row.get("status", "")).upper()
                bg, fg = _status_style(status)
                if bg:
                    reqs.append(_format_range(
                        sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                        bg=bg, fg=fg, bold=True, h_align="CENTER",
                    ))

        if tab_key in ("Inboxes", MASTER_TAB_NAME):
            # Availability column
            if "availability" in columns:
                col_idx = columns.index("availability")
                for ri, row in enumerate(data, start=1):
                    val = str(row.get("availability", "")).upper()
                    if val == "FREE":
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=_COLORS["green_bg"], fg=_COLORS["green_text"], bold=True, h_align="CENTER",
                        ))
                    elif val == "BUSY":
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=_COLORS["red_bg"], fg=_COLORS["red_text"], bold=True, h_align="CENTER",
                        ))

            # Campaign status column
            if "campaign_status" in columns:
                col_idx = columns.index("campaign_status")
                for ri, row in enumerate(data, start=1):
                    status = str(row.get("campaign_status", "")).upper()
                    bg, fg = _status_style(status)
                    if bg:
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=bg, fg=fg, bold=True, h_align="CENTER",
                        ))

            # Test sheet status column
            if "test_sheet_status" in columns:
                col_idx = columns.index("test_sheet_status")
                for ri, row in enumerate(data, start=1):
                    val = str(row.get("test_sheet_status", "")).lower()
                    if val == "inbox":
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=_COLORS["green_bg"], fg=_COLORS["green_text"], h_align="CENTER",
                        ))
                    elif val in ("fail", "spam"):
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=_COLORS["red_bg"], fg=_COLORS["red_text"], h_align="CENTER",
                        ))
                    elif val == "stale":
                        reqs.append(_format_range(
                            sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                            bg=_COLORS["yellow_bg"], fg=_COLORS["orange_text"], h_align="CENTER",
                        ))

        if tab_key in ("Inboxes", MASTER_TAB_NAME) and "warmup_rep_pct" in columns:
            col_idx = columns.index("warmup_rep_pct")
            for ri, row in enumerate(data, start=1):
                rep_str = str(row.get("warmup_rep_pct", ""))
                try:
                    rep_val = float(rep_str.replace("%", "").strip()) if "%" in rep_str else -1
                except ValueError:
                    rep_val = -1
                if rep_val >= 90:
                    reqs.append(_format_range(
                        sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                        fg=_COLORS["green_text"], bold=True, h_align="CENTER",
                    ))
                elif 0 <= rep_val < 90:
                    reqs.append(_format_range(
                        sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                        bg=_COLORS["red_bg"], fg=_COLORS["red_text"], bold=True, h_align="CENTER",
                    ))

        if tab_key == "Warmup Reputation" and "warmup_reputation" in columns:
            col_idx = columns.index("warmup_reputation")
            for ri, row in enumerate(data, start=1):
                rep_str = str(row.get("warmup_reputation", ""))
                try:
                    rep_val = float(rep_str.replace("%", "").strip()) if "%" in rep_str else -1
                except ValueError:
                    rep_val = -1
                if rep_val >= 90:
                    reqs.append(_format_range(
                        sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                        fg=_COLORS["green_text"], bold=True, h_align="CENTER",
                    ))
                elif 0 <= rep_val < 90:
                    reqs.append(_format_range(
                        sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                        bg=_COLORS["red_bg"], fg=_COLORS["red_text"], bold=True, h_align="CENTER",
                    ))

        return reqs


# ── Formatting helpers (Google Sheets API v4 request builders) ───────────────

def _serialize(value: Any) -> Any:
    """Convert booleans and other non-JSON types for Sheets."""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return value


def _status_style(status: str) -> tuple[dict | None, dict | None]:
    """Return (bg, fg) color dicts for a campaign status string."""
    s = status.upper().strip()
    if not s:
        return None, None
    if s in ("ACTIVE",) or s.startswith("START") or s.startswith("IN_PROGRESS") or s.startswith("IN PROGRESS"):
        return _COLORS["green_bg"], _COLORS["green_text"]
    if s.startswith("COMPLETE"):
        return _COLORS["gray_bg"], _COLORS["gray_text"]
    if s in ("PAUSED", "PAUSE") or s.startswith("PAUSE"):
        return _COLORS["yellow_bg"], _COLORS["orange_text"]
    if s in ("STOPPED", "STOP") or s.startswith("STOP"):
        return _COLORS["red_bg"], _COLORS["red_text"]
    if s.startswith("DRAFT"):
        return _COLORS["gray_bg"], _COLORS["gray_text"]
    return None, None


def _freeze_rows(sheet_id: int, count: int) -> dict:
    return {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": count},
            },
            "fields": "gridProperties.frozenRowCount",
        }
    }


def _col_width(sheet_id: int, col_index: int, px: int) -> dict:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": col_index,
                "endIndex": col_index + 1,
            },
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }


def _format_range(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
    *,
    bg: dict | None = None,
    fg: dict | None = None,
    bold: bool = False,
    h_align: str | None = None,
    wrap: str | None = None,
) -> dict:
    cell_format: dict[str, Any] = {}
    fields: list[str] = []

    if bg:
        cell_format["backgroundColor"] = bg
        fields.append("userEnteredFormat.backgroundColor")
    if fg or bold:
        text_fmt: dict[str, Any] = {}
        if fg:
            text_fmt["foregroundColor"] = fg
            fields.append("userEnteredFormat.textFormat.foregroundColor")
        if bold:
            text_fmt["bold"] = True
            fields.append("userEnteredFormat.textFormat.bold")
        cell_format["textFormat"] = text_fmt
    if h_align:
        cell_format["horizontalAlignment"] = h_align
        fields.append("userEnteredFormat.horizontalAlignment")
    if wrap:
        cell_format["wrapStrategy"] = wrap
        fields.append("userEnteredFormat.wrapStrategy")

    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": cell_format},
            "fields": ",".join(fields),
        }
    }


def _outer_border(
    sheet_id: int,
    start_row: int,
    end_row: int,
    start_col: int,
    end_col: int,
) -> dict:
    border_style = {
        "style": "SOLID",
        "color": {"red": 0.80, "green": 0.80, "blue": 0.80},
    }
    return {
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "top": border_style,
            "bottom": border_style,
            "left": border_style,
            "right": border_style,
            "innerHorizontal": border_style,
            "innerVertical": border_style,
        }
    }
