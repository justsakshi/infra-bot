#!/usr/bin/env python3
"""Gate self-test: do all the places that decide "this inbox may send" agree? READ-ONLY for Smartlead.

Several tools stop a bad inbox, and each one trusts the "All Inboxes" tab:
infra-bot writes the tab, Campaign Desk refuses an inbox whose Busy Reason carries evidence (re-sync, launch,
twin), gtm's write guard refuses the same reasons (upload, resume, start), and the v2 job takes a failed inbox
out of running campaigns. On 11 Nov 2026 a comparison by hand showed the tab had never carried `do_not_use`
for any of the 63 tagged inboxes (the tags did not reach the row). Nothing would have said so on its own.

This job reads the four sources again, each from its origin, and compares them:

  tags          Smartlead's list call (GET /email-accounts), the only call that returns tags
  v2 verdicts   Mongo v2_verdicts (written by the v2 job from the downloaded result files)
  the list      the published "All Inboxes" tab, as Campaign Desk and gtm read it
  campaigns     which inboxes are attached to each ACTIVE campaign right now

Checks (smartlead/gate_selftest.checks):
  1. every inbox tagged DO NOT USE is BUSY on the list with reason do_not_use
  2. every inbox whose newest v2 verdict is FAIL is BUSY on the list with reason failed_v2
  3. once a v2 pass is required: every FREE inbox has a v2 pass in the last 10 days
  4. no tagged or failed inbox is attached to a sending campaign (report-only clients are listed, not failed)
  5. the v2 job on the PC ran in the last 26 hours and is in act mode
  6. every connected inbox of a current client has a row in the asset tracker (Mongo assets), so the
     inventory that bills and expiry reminders are read from is complete

    python gate_selftest.py                  # print the result
    python gate_selftest.py --save --post    # daily on Render: record it; post when a check disagrees,
                                             # and always on Monday (the weekly "all agree" line)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from smartlead import gate_selftest as gs  # noqa: E402
from smartlead import notify, usable  # noqa: E402
from smartlead.accounts import PAST_CLIENT_ACCOUNTS, discover_accounts  # noqa: E402
from smartlead.api import SmartleadClient  # noqa: E402
from smartlead.config import MASTER_TAB_NAME  # noqa: E402
from smartlead.report_sheet import client_label  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
COLLECTION = "gate_selftest"


async def smartlead_side(errors: list[str]) -> tuple[list[dict], list[dict]]:
    """([{email, tags, account, connected, client}], [{client, campaign, emails}]) for every current account."""
    from smartlead.sheets import _is_test_campaign
    inboxes, campaigns = [], []
    for acc in discover_accounts():
        if acc.name.strip().lower().replace("_", " ") in PAST_CLIENT_ACCOUNTS:
            continue
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                for a in await c.list_email_accounts():
                    inboxes.append({"email": str(a.get("from_email") or "").strip().lower(),
                                    "tags": usable.tag_names(a), "account": acc.name,
                                    "connected": bool(a.get("is_smtp_success")) and bool(a.get("is_imap_success")),
                                    "client": client_label(acc.name, a.get("client_id"))})
                for camp in await c.list_campaigns() or []:
                    if str(camp.get("status") or "").upper() != "ACTIVE" or _is_test_campaign(camp.get("name") or ""):
                        continue
                    attached = await c.get_campaign_email_accounts(str(camp["id"])) or []
                    campaigns.append({"client": client_label(acc.name, camp.get("client_id")),
                                      "campaign": str(camp.get("name") or camp["id"]),
                                      "emails": sorted({str(a.get("from_email") or "").strip().lower() for a in attached})})
        except Exception as exc:  # noqa: BLE001 - say so; a half read must not pass as "all agree"
            errors.append(f"Smartlead {acc.name}: {str(exc)[:120]}")
    return inboxes, campaigns


def list_rows(errors: list[str]) -> list[dict]:
    """The published tab's rows with the labels other tools read (Email, Availability, Busy Reason)."""
    rows: list[dict] = []
    try:
        from smartlead.sheets import _authorize
        gc = _authorize()
        for sheet_id in sorted({a.sheet_id for a in discover_accounts() if a.sheet_id}):
            values = gc.open_by_key(sheet_id).worksheet(MASTER_TAB_NAME).get_all_values()
            if len(values) > 1:
                rows += [dict(zip(values[0], r)) for r in values[1:]]
    except Exception as exc:  # noqa: BLE001
        errors.append(f"the list ({MASTER_TAB_NAME}): {str(exc)[:120]}")
    return rows


def mongo_side(errors: list[str]) -> tuple[dict, dict | None, object]:
    data = usable.load_gate_data()
    if not data.get("checked"):
        errors.append(f"v2 verdicts: {data.get('error') or 'not read'}")
    heartbeat, db = None, None
    try:
        db = usable._db()
        heartbeat = db["v2_heartbeat"].find_one({"_id": "pc"})
    except Exception as exc:  # noqa: BLE001
        errors.append(f"v2 heartbeat: {str(exc)[:120]}")
    return data.get("v2") or {}, heartbeat, db


def tracker_names(errors: list[str]) -> set[str]:
    """Every name (domain or inbox) in the asset tracker."""
    from smartlead.zapmail_asset_sync import read_tracker
    names = set(read_tracker())
    if not names:
        errors.append("the tracker (Mongo assets): not read")
    return names


def main() -> int:
    ap = argparse.ArgumentParser(description="Gate self-test (read-only for Smartlead)")
    ap.add_argument("--post", action="store_true", help="post when a check disagrees, and always on Monday")
    ap.add_argument("--always-post", action="store_true", help="post whatever the result")
    ap.add_argument("--save", action="store_true", help="record the result in Mongo gate_selftest")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    now = datetime.now(IST)
    errors: list[str] = []
    inboxes, campaigns = asyncio.run(smartlead_side(errors))
    rows = list_rows(errors)
    v2, heartbeat, db = mongo_side(errors)
    tracker = tracker_names(errors)
    results = gs.checks(inboxes, campaigns, rows, v2, heartbeat, now, usable.require_v2_pass(now.date()), errors,
                        tracker=tracker)
    head, thread = gs.format_post(results, now.date(), errors)
    bad = [r for r in results if not r["ok"]]
    out = {"date": now.date().isoformat(), "ok": not bad and not errors, "errors": errors,
           "checks": [{k: r[k] for k in ("name", "ok", "checked", "bad")} for r in results], "posted": False}
    if args.save and db is not None:
        try:
            db[COLLECTION].replace_one({"_id": out["date"]}, {"_id": out["date"], **{k: v for k, v in out.items() if k != "posted"},
                                                                "ran": now.astimezone(timezone.utc).isoformat(timespec="seconds")},
                                       upsert=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[gate-selftest] result not saved: {str(exc)[:120]}", file=sys.stderr)
    if args.always_post or (args.post and (bad or errors or now.weekday() == 0)):
        token = os.getenv("SLACK_BOT_TOKEN", "").strip()
        if token:
            out["posted"] = bool(notify.post_with_thread(token, notify.infra_channel("GATE_SELFTEST_CHANNEL"), head, thread))
        else:
            print("[gate-selftest] SLACK_BOT_TOKEN not set: nothing posted", file=sys.stderr)
    print(json.dumps(out) if args.json else "\n".join(head) + ("\n\n" + "\n\n".join(thread) if thread else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
