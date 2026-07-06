"""Human-visible log of API placement-test results.

Appends one row per tested domain to an "API Tests" tab in the deliverability
sheet (the same sheet the team already uses for manual test results), so API
results are never invisible to the humans who work off that sheet.
"""
from __future__ import annotations

from datetime import date

from smartlead.config import TEST_SHEET_ID

_TAB = "API Tests"
_HEADER = ["Date", "Client", "Domain", "Result", "Inbox %", "Spam %", "Test ID", "Emails Tested"]


def append_api_result(client: str, test_id: int, emails: list[str],
                      inbox_pct: float, spam_pct: float, status: str) -> bool:
    """Append per-domain rows for a completed API test. Never raises."""
    try:
        from smartlead.sheets import _authorize
        gc = _authorize()
        sh = gc.open_by_key(TEST_SHEET_ID)
        try:
            ws = sh.worksheet(_TAB)
        except Exception:  # noqa: BLE001 - tab missing -> create with header
            ws = sh.add_worksheet(title=_TAB, rows=2000, cols=len(_HEADER))
            ws.append_row(_HEADER)
        today = date.today().strftime("%Y-%m-%d")
        domains: dict[str, int] = {}
        for e in emails:
            if "@" in e:
                d = e.split("@", 1)[1].lower()
                domains[d] = domains.get(d, 0) + 1
        rows = [[today, client, d, status, round(inbox_pct, 1), round(spam_pct, 1),
                 str(test_id), n] for d, n in sorted(domains.items())]
        if rows:
            ws.append_rows(rows)
        return True
    except Exception as exc:  # noqa: BLE001 - sheet failure must never break Pass A
        print(f"  [Retest] API-result sheet write failed (non-fatal): {exc}")
        return False
