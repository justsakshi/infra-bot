#!/usr/bin/env python3
"""Non-connected (Anjali-style) placement-test executor. Dry-run by default.

The one step the public API cannot do (verified live 2026-07-10) is CREATING a
non-connected SmartDelivery test — that stays human: in the PRECISE_LEADS UI,
Start Spam Test -> Manual -> non-connected accounts, name the test EXACTLY the
inbox email, then paste the seed list + Track-ID into the "NC Tests" sheet tab.

This executor automates everything else:

  Phase SEND (rows with empty/PENDING status):
    - match the sheet row to its PL SmartDelivery test by test name
    - if the test is already COMPLETED (human sent manually) -> ingest result
    - else build a one-off campaign in the inbox's OWN Smartlead account
      (sequence body carries the Track-ID, seeds are the leads, inbox attached,
      daily limit temporarily raised with the old value snapshotted in the row)
      and START it — Smartlead sends via the inbox's OAuth connection, no
      credentials or imports needed.

  Phase POLL (rows with SENT status):
    - poll the PL test; on completion write the result to Mongo + the
      "API Tests" tab, restore the inbox's daily limit, pause the campaign,
      mark the row DONE.

The sheet row is the state machine: (blank|PENDING) -> SENT -> DONE / ERROR.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import httpx

from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import (
    NC_TEST_ENABLED, NC_TAB_NAME, NC_TEST_ACCOUNT, NC_CAMPAIGN_PREFIX,
    NC_MIN_GAP_MINUTES, NC_LIMIT_BUFFER, TEST_SHEET_ID,
    SMARTDELIVERY_BASE_URL, RETEST_INBOX_THRESHOLD,
    NC_SUGGEST_CLIENTS, NC_SUGGEST_CAP, NC_SUGGEST_HOUR_UTC,
)
from smartlead.retest_targets import select_targets
from smartlead.smart_delivery import SmartDeliveryClient, SmartDeliveryError
from smartlead.placement_store import PlacementStore
from smartlead.placement_sheet import append_api_result

_HEADER = ["Inbox Email", "Track ID", "Seed Emails", "Status", "Test ID",
           "Campaign ID", "Prev Limit", "Result", "Inbox %", "Spam %",
           "Notes", "Updated"]
_COL = {name: i for i, name in enumerate(_HEADER)}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


def _parse_seeds(cell: str) -> list[str]:
    out: list[str] = []
    for part in cell.replace(";", ",").replace("\n", ",").split(","):
        e = part.strip().lower()
        if e and "@" in e and e not in out:
            out.append(e)
    return out


# ── sheet state machine ──────────────────────────────────────────────────────

def _open_tab():
    from smartlead.sheets import _authorize
    gc = _authorize()
    sh = gc.open_by_key(TEST_SHEET_ID)
    try:
        return sh.worksheet(NC_TAB_NAME)
    except Exception:  # noqa: BLE001 - tab missing -> create with header
        ws = sh.add_worksheet(title=NC_TAB_NAME, rows=500, cols=len(_HEADER))
        ws.append_row(_HEADER)
        ws.append_row(["<inbox email — must equal the PL test name>",
                       "<Track-ID from the PL UI>",
                       "<seed list from the PL UI, comma/newline separated>",
                       "", "", "", "", "", "", "",
                       "Fill the first 3 columns; leave the rest to the robot.", _now()])
        print(f"  [NC] created '{NC_TAB_NAME}' tab with header + example row")
        return ws


def _load_rows(ws) -> list[dict]:
    values = ws.get_all_values()
    rows: list[dict] = []
    for ri, raw in enumerate(values[1:], start=2):  # 1-based sheet rows, skip header
        get = lambda c: raw[_COL[c]].strip() if _COL[c] < len(raw) else ""
        email = get("Inbox Email").lower()
        if not email or "@" not in email or email.startswith("<"):
            continue
        rows.append({
            "row": ri, "email": email, "track_id": get("Track ID"),
            "seeds": _parse_seeds(get("Seed Emails")),
            "status": get("Status").upper(), "test_id": get("Test ID"),
            "campaign_id": get("Campaign ID"), "prev_limit": get("Prev Limit"),
        })
    return rows


def _update_row(ws, row: int, fields: dict) -> None:
    """Write named cells on one row in a single batch."""
    fields = dict(fields)
    fields["Updated"] = _now()
    cells = [{"range": f"{chr(ord('A') + _COL[k])}{row}", "values": [[str(v)]]}
             for k, v in fields.items()]
    ws.batch_update(cells)


# ── PL SmartDelivery test lookup ─────────────────────────────────────────────

async def _pl_tests(pl_key: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=30) as c:
        resp = await c.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report?api_key={pl_key}",
                            headers={"Content-Type": "application/json"},
                            json={"limit": 300})
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []


def _match_test(tests: list[dict], email: str) -> dict | None:
    """Match by exact test name == inbox email, falling back to the inbox's
    domain (Anjali's existing convention names tests after the domain)."""
    for key in (email, _domain(email)):
        hits = [t for t in tests if str(t.get("test_name", "")).strip().lower() == key]
        if hits:
            return max(hits, key=lambda t: str(t.get("created_at", "")))
    return None


# ── inbox lookup across all Smartlead accounts ───────────────────────────────

async def _find_inbox(accounts, email: str):
    """Return (account, email_account_dict) for an inbox email, else (None, None)."""
    for acc in accounts:
        async with SmartleadClient(acc.api_key, acc.name) as c:
            try:
                for a in await c.list_email_accounts():
                    if str(a.get("from_email", "")).strip().lower() == email:
                        return acc, a
            except Exception as exc:  # noqa: BLE001
                print(f"  [NC] account scan {acc.name} failed: {exc}")
    return None, None


# ── phases ───────────────────────────────────────────────────────────────────

async def _ingest_completed(pl_key: str, store: PlacementStore, ws, r: dict, test: dict) -> None:
    """Test finished (whether we sent or the human did) -> record everywhere."""
    tid = int(test.get("spam_test_id") or r["test_id"])
    async with SmartDeliveryClient(pl_key) as sd:
        rep = await sd.get_report(tid)
    status = "inbox" if rep["inbox_pct"] >= RETEST_INBOX_THRESHOLD else "fail"
    today = date.today().strftime("%Y-%m-%d")
    store.save_result(r["email"], _domain(r["email"]), status, today, "nc")
    append_api_result(r.get("client", "NC"), tid, [r["email"]],
                      rep["inbox_pct"], rep["spam_pct"], status)
    _update_row(ws, r["row"], {"Status": "DONE", "Test ID": tid, "Result": status,
                               "Inbox %": round(rep["inbox_pct"], 1),
                               "Spam %": round(rep["spam_pct"], 1)})
    print(f"  [NC] {r['email']} -> {status} ({rep['inbox_pct']:.0f}% inbox / "
          f"{rep['spam_pct']:.0f}% spam) [test {tid}]")


async def _send_via_campaign(accounts, ws, r: dict, test_id: int) -> None:
    """Build + start the one-off sender campaign in the inbox's own account."""
    acc, inbox = await _find_inbox(accounts, r["email"])
    if not acc:
        _update_row(ws, r["row"], {"Notes": "inbox not found in any Smartlead account"})
        print(f"  [NC] {r['email']}: not found in any account — skipped")
        return
    r["client"] = acc.name
    seeds = r["seeds"]
    prev_limit = int(inbox.get("message_per_day") or 0)
    target_limit = max(prev_limit, len(seeds) + NC_LIMIT_BUFFER)
    name = f"{NC_CAMPAIGN_PREFIX} - {r['email']} - {date.today().isoformat()}"
    body_html = (
        "<p>Hi,</p><p>Sharing a short note ahead of this week's update — "
        "let me know if the timing works on your side and I'll send over the "
        "details.</p><p>Best,<br>{{sender_name}}</p>"
        f"<p>{r['track_id']}</p>"
    )
    async with SmartleadClient(acc.api_key, acc.name) as c:
        camp = await c.create_campaign(name)
        cid = str(camp.get("id"))
        await c.save_campaign_sequences(cid, "Quick note before this week's update", body_html)
        await c.update_campaign_schedule(cid, {
            "timezone": "Asia/Kolkata",
            "days_of_the_week": [0, 1, 2, 3, 4, 5, 6],
            "start_hour": "00:00", "end_hour": "23:59",
            "min_time_btw_emails": NC_MIN_GAP_MINUTES,
            "max_new_leads_per_day": len(seeds) + NC_LIMIT_BUFFER,
        })
        await c.add_campaign_leads(cid, seeds)
        await c.add_campaign_email_accounts(cid, [int(inbox["id"])])
        if target_limit > prev_limit:
            await c.update_email_account(str(inbox["id"]), {"max_email_per_day": target_limit})
            print(f"  [NC] {r['email']}: daily limit {prev_limit} -> {target_limit} "
                  "for the send day (restored on completion)")
        await c.set_campaign_status(cid, "START")
    _update_row(ws, r["row"], {"Status": "SENT", "Test ID": test_id, "Campaign ID": cid,
                               "Prev Limit": prev_limit,
                               "Notes": f"sending {len(seeds)} seeds via {acc.name}"})
    print(f"  [NC] {r['email']}: campaign {cid} started in {acc.name} "
          f"({len(seeds)} seeds, gap {NC_MIN_GAP_MINUTES}m)")


async def _finish_sent(accounts, pl_key: str, store: PlacementStore, ws, r: dict) -> None:
    """Poll a SENT row; on completion ingest + restore limit + pause campaign."""
    if not r["test_id"]:
        _update_row(ws, r["row"], {"Notes": "SENT but no Test ID — fix manually"})
        return
    tid = int(r["test_id"])
    async with SmartDeliveryClient(pl_key) as sd:
        try:
            poll = await sd.poll_test(tid)
        except SmartDeliveryError as exc:
            print(f"  [NC] poll {tid} failed: {exc}")
            return
    if not poll["done"]:
        print(f"  [NC] {r['email']}: test {tid} still running")
        return
    acc, inbox = await _find_inbox(accounts, r["email"])
    if acc:
        r["client"] = acc.name
    await _ingest_completed(pl_key, store, ws, r, {"spam_test_id": tid})
    # cleanup: restore the inbox's original daily limit, pause the campaign
    if acc and inbox:
        async with SmartleadClient(acc.api_key, acc.name) as c:
            try:
                if r["prev_limit"]:
                    await c.update_email_account(str(inbox["id"]),
                                                 {"max_email_per_day": int(r["prev_limit"])})
                    print(f"  [NC] {r['email']}: daily limit restored to {r['prev_limit']}")
                if r["campaign_id"]:
                    await c.set_campaign_status(r["campaign_id"], "PAUSED")
                    print(f"  [NC] campaign {r['campaign_id']} paused")
            except Exception as exc:  # noqa: BLE001
                _update_row(ws, r["row"],
                            {"Notes": f"DONE but cleanup failed: {exc} — restore limit "
                                      f"to {r['prev_limit']} and pause campaign manually"})
                print(f"  [NC] cleanup failed for {r['email']}: {exc}")


async def _suggest_targets(accounts, ws, rows: list[dict]) -> None:
    """Advisory phase: write worst-first 'NEEDS TEST' rows for the credit-poor
    clients so the human opens the tab and sees exactly which inboxes to create
    non-connected tests for. Runs once a day (NC_SUGGEST_HOUR_UTC) because
    health-row building is expensive; runs even in dry-run — it's read-only
    with respect to Smartlead, same policy as the capacity planner."""
    if datetime.utcnow().hour != NC_SUGGEST_HOUR_UTC:
        return
    # never suggest an email already anywhere in the tab (any status)
    already = {r["email"] for r in rows}
    from retest_executor import _health_rows_for
    suggestions: list[list[str]] = []
    for acc in accounts:
        # Case-insensitive: account names derive from env-var suffixes, and
        # Windows upper-cases those while Linux does not (see config.py).
        if acc.name.upper() not in {c.upper() for c in NC_SUGGEST_CLIENTS}:
            continue
        try:
            health, _ = await _health_rows_for(acc)
        except Exception as exc:  # noqa: BLE001
            print(f"  [NC] suggest: health rows for {acc.name} failed: {exc}")
            continue
        for t in select_targets(health, NC_SUGGEST_CAP, already):
            already.add(t["email"])
            suggestions.append([
                t["email"], "", "", "NEEDS TEST", "", "", "", "", "", "",
                f"{t['client']}: {t['reason']} — create a non-connected test in "
                "the PL UI named exactly this email, paste Track-ID + seeds here",
                _now(),
            ])
    if suggestions:
        ws.append_rows(suggestions)
        print(f"[NC] suggested {len(suggestions)} inbox(es) to test "
              f"(clients: {', '.join(NC_SUGGEST_CLIENTS)})")
    else:
        print("[NC] suggest: nothing new to test")


async def main() -> None:
    accounts = discover_accounts()
    pl = next((a for a in accounts if a.name == NC_TEST_ACCOUNT), None)
    if not pl:
        print(f"[NC] test account '{NC_TEST_ACCOUNT}' not configured — aborting.")
        return
    store = PlacementStore()

    ws = _open_tab()
    rows = _load_rows(ws)
    pending = [r for r in rows if r["status"] in ("", "PENDING", "NEEDS TEST")]
    sent = [r for r in rows if r["status"] == "SENT"]
    print(f"[NC] {len(rows)} row(s): {len(pending)} pending, {len(sent)} sent, "
          f"{len(rows) - len(pending) - len(sent)} done/other"
          f"{' (DRY-RUN)' if not NC_TEST_ENABLED else ''}")
    if not pending and not sent:
        await _suggest_targets(accounts, ws, rows)
        return

    tests = await _pl_tests(pl.api_key)

    for r in pending:
        test = _match_test(tests, r["email"])
        if not test:
            if r["status"] == "NEEDS TEST":
                # suggestion the human hasn't acted on yet — keep its
                # instruction note intact, just log
                print(f"  [NC] {r['email']}: suggested, awaiting PL test")
                continue
            _update_row(ws, r["row"],
                        {"Status": "PENDING",
                         "Notes": f"no PL test named '{r['email']}' — create the "
                                  "non-connected test in the PL UI with this exact name"})
            print(f"  [NC] {r['email']}: no matching PL test yet")
            continue
        tid = int(test["spam_test_id"])
        if str(test.get("status", "")).upper() == "COMPLETED":
            # human already sent manually — pure ingestion, no campaign needed
            if not NC_TEST_ENABLED:
                print(f"  [NC] DRY-RUN would ingest completed test {tid} for {r['email']}")
                continue
            acc, _ = await _find_inbox(accounts, r["email"])
            r["client"] = acc.name if acc else "NC"
            await _ingest_completed(pl.api_key, store, ws, r, test)
            continue
        if not r["track_id"] or not r["seeds"]:
            if r["status"] == "NEEDS TEST":
                print(f"  [NC] {r['email']}: PL test exists, awaiting Track-ID + seeds")
                continue
            _update_row(ws, r["row"], {"Status": "PENDING",
                                       "Notes": "missing Track ID or Seed Emails"})
            print(f"  [NC] {r['email']}: row incomplete (needs Track ID + seeds)")
            continue
        if not NC_TEST_ENABLED:
            print(f"  [NC] DRY-RUN would send: {r['email']} -> test {tid}, "
                  f"{len(r['seeds'])} seeds, track id present")
            continue
        try:
            await _send_via_campaign(accounts, ws, r, tid)
        except Exception as exc:  # noqa: BLE001
            _update_row(ws, r["row"], {"Notes": f"send failed: {exc}"})
            print(f"  [NC] send failed for {r['email']}: {exc}")

    for r in sent:
        if not NC_TEST_ENABLED:
            print(f"  [NC] DRY-RUN would poll: {r['email']} test {r['test_id']}")
            continue
        try:
            await _finish_sent(accounts, pl.api_key, store, ws, r)
        except Exception as exc:  # noqa: BLE001
            print(f"  [NC] finish failed for {r['email']}: {exc}")

    await _suggest_targets(accounts, ws, rows)
    print("[NC] run complete.")


if __name__ == "__main__":
    asyncio.run(main())
