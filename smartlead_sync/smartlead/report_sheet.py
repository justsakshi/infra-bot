"""Write a placement report into the "Deliverability Test results" sheet.

Two outputs:

1. The per-client grid tabs (Melior, Precise Leads, Bettrdata ...): one cell per
   domain per receiving provider (G suite / Outlook) under the test date.
   Existing cells are never overwritten (the team hand-corrects this grid) and a
   domain is matched exactly: both rules live in placement_grid.plan_writes.
2. An "Inbox Status" tab, rewritten each run: one row per inbox, and a `Usable`
   column (YES / EXISTING / NO) that Campaign Desk reads to choose inboxes.

Both are dry-run unless `apply=True`. Sheet problems are returned, never raised.
"""
from __future__ import annotations

from smartlead.placement_grid import column_label_for, plan_writes, write_result
from smartlead.placement_report import INBOX_HEADER, inbox_rows

STATUS_TAB = "Inbox Status"
# Smartlead client label -> the grid tab that holds it.
TAB_BY_CLIENT = {"melior": "Melior", "bettrdata": "Bettrdata", "precise leads": "Precise Leads",
                 "osc": "OSC", "belardi wong": "Belardiwong", "belardiwong": "Belardiwong"}
_CLIENT_BY_ID = {12256: "Melior", 456214: "Bettrdata", 145916: "OSC"}


def client_label(account_name: str, client_id) -> str:
    """Which client an inbox belongs to. Only the Precise Leads agency account
    holds several (Melior and OSC inside it); elsewhere the account is the client."""
    if account_name.strip().lower().replace("_", " ") == "precise leads":
        return _CLIENT_BY_ID.get(client_id, "Precise Leads")
    return account_name.title().replace("_", " ")


def grid_tab(client: str) -> str | None:
    return TAB_BY_CLIENT.get(client.strip().lower().replace("_", " "))


def grid_writes(report: dict, client_by_email: dict[str, str], tested_on: str,
                apply: bool) -> list[dict]:
    """Fill each domain's G suite / Outlook cells. Returns what was (or would be) done."""
    out = []
    for dom in report["domains"].values():
        client = client_by_email.get(dom["emails"][0], "")
        tab = grid_tab(client)
        if not tab or not dom["cells"]:
            out.append({"domain": dom["domain"], "tab": tab, "note": "no grid tab or no seed result"})
            continue
        # plan_writes judges inbox_pct against a threshold; the team's majority rule
        # has already decided each cell, so pass it as 100 (Inbox) or 0 (spam).
        by_provider = {"G Suite": {"inbox_pct": 100.0 if dom["cells"].get("Google") == "Inbox" else 0.0},
                       "Office365": {"inbox_pct": 100.0 if dom["cells"].get("Outlook") == "Inbox" else 0.0}}
        if "Google" not in dom["cells"]:
            del by_provider["G Suite"]
        if "Outlook" not in dom["cells"]:
            del by_provider["Office365"]
        plan = write_result(tab, dom["domain"], by_provider, when=tested_on, dry_run=not apply)
        out.append({"domain": dom["domain"], "tab": tab, "cells": dom["cells"],
                    "writes": len(plan.writes), "note": plan.skipped_reason})
    return out


def write_status_tab(report: dict, client_by_email: dict[str, str], tested_on: str,
                     apply: bool) -> dict:
    rows = inbox_rows(report, client_by_email, tested_on)
    if not apply:
        return {"rows": len(rows), "written": False}
    try:
        from smartlead.config import TEST_SHEET_ID
        from smartlead.sheets import _authorize
        sh = _authorize().open_by_key(TEST_SHEET_ID)
        try:
            ws = sh.worksheet(STATUS_TAB)
        except Exception:  # noqa: BLE001
            ws = sh.add_worksheet(title=STATUS_TAB, rows=max(1000, len(rows) + 50), cols=len(INBOX_HEADER))
        existing = ws.get_all_values()
        # Keep other clients' rows: this run only replaces the clients it tested.
        tested_clients = {r[2] for r in rows}
        kept = [r for r in existing[1:] if len(r) > 2 and r[2] not in tested_clients and r[0]]
        merged = [INBOX_HEADER] + kept + rows
        ws.clear()
        ws.update(values=merged, range_name="A1")
        ws.freeze(rows=1)
        return {"rows": len(rows), "kept_other_clients": len(kept), "written": True}
    except Exception as exc:  # noqa: BLE001
        print(f"  [ReportSheet] Inbox Status write failed (non-fatal): {exc}")
        return {"rows": len(rows), "written": False, "error": str(exc)}
