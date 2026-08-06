"""Build Expandi metric rows for the Campaign Metrics tab.

Lives here rather than inline in the entrypoints because run.py and
metrics_only.py both need it, and the Smartlead block being duplicated across
those two files is exactly how one of them drifted and started reporting
different numbers.
"""
from __future__ import annotations

from datetime import datetime

from smartlead import campaign_metrics as cm
from smartlead.config import CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_EXCLUDE
from smartlead.expandi import ExpandiClient
from smartlead.expandi_accounts import discover_expandi_workspaces
from smartlead.expandi_store import ExpandiStore


async def build_expandi_rows(today: datetime, start_dt: datetime,
                             log: str = "[Metrics]") -> list[dict]:
    """Snapshot today's counters, then build one row per campaign.

    The snapshot is written before the rows are built so that today's numbers
    are recorded even if row-building later fails — a missing day leaves a
    permanent hole in every future month-to-date calculation, and unlike a
    failed sheet write it cannot be fixed by re-running tomorrow.
    """
    rows: list[dict] = []
    workspaces = discover_expandi_workspaces()
    if not workspaces:
        return rows

    store = ExpandiStore()
    if not store.available:
        print(f"{log} Expandi: no Mongo — month-to-date columns will show '?'")

    for ws in workspaces:
        if CAMPAIGN_METRICS_CLIENTS and ws.name.upper() not in CAMPAIGN_METRICS_CLIENTS:
            continue
        try:
            async with ExpandiClient(ws.api_key, ws.api_secret, ws.name) as exc_client:
                accounts = await exc_client.list_li_accounts()
                campaigns: list[dict] = []
                for acc in accounts:
                    acc_id = acc.get("id")
                    if acc_id is None:
                        continue
                    campaigns.extend(await exc_client.list_campaigns(acc_id))

                print(f"{log} Expandi {ws.name}: {len(accounts)} account(s), "
                      f"{len(campaigns)} campaign(s)")

                saved = store.save_snapshot(ws.name, campaigns, today.date())
                if saved:
                    print(f"{log} Expandi {ws.name}: snapshotted {saved} campaign(s)")

                for camp in campaigns:
                    if camp.get("archived"):
                        continue
                    if cm.is_excluded_campaign_name(camp.get("name", ""),
                                                    CAMPAIGN_METRICS_EXCLUDE):
                        continue
                    stats = camp.get("stats") or {}
                    # An empty shell — no contacts ever added. Same rule the
                    # Smartlead side applies, so the tab stays consistent.
                    if not int(stats.get("people_in_campaign") or 0):
                        continue
                    cid = camp.get("id")
                    rows.append(cm.expandi_metric_row(
                        camp,
                        store.baseline(ws.name, cid, start_dt.date()),
                        store.previous_day(ws.name, cid, today.date()),
                        client=ws.name,
                    ))
        except Exception as exc:  # noqa: BLE001
            print(f"[!] Expandi workspace {ws.name} failed: {exc}")
    return rows
