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


# Counters that are summed when the same campaign runs on several LinkedIn
# accounts. Deliberately not the whole stats dict: step_count is a property of
# the sequence, not a total, and summing it would report a 12-step campaign as
# 24-step.
_SUMMED = (
    "people_in_campaign", "in_queue", "initiated", "connected",
    "contacted_people", "replied_msg", "replied_excl_msg",
    "interested_people", "finished", "stopped",
)


def _merge_by_name(campaigns: list[dict]) -> list[dict]:
    """Combine campaign instances that share a name across LinkedIn accounts.

    Expandi runs one campaign from several sender profiles, and the API returns
    each as its own instance under its own account. "BD Select : Data Providers
    (Tier B)" comes back twice — 60 connections from one profile, 66 from the
    other. Listed separately they read as two half-sized campaigns; the team's
    manual sheet reports the one campaign at 126, which is what the client
    recognises.

    Verified against that sheet: four campaigns match exactly once merged, and
    the three that do not are higher only in `initiated` (activity since the
    sheet was made) with accepted counts still matching exactly.

    The first instance seen supplies the identity fields; `id` is kept from it
    so the snapshot store has a stable key across runs. Sorting by id makes that
    choice deterministic rather than dependent on account iteration order.
    """
    merged: dict[str, dict] = {}
    for camp in sorted(campaigns, key=lambda c: (str(c.get("name", "")).strip(),
                                                 c.get("id") or 0)):
        name = str(camp.get("name", "")).strip()
        if not name:
            continue
        existing = merged.get(name)
        if existing is None:
            copy = dict(camp)
            copy["stats"] = dict(camp.get("stats") or {})
            copy["_instance_ids"] = [camp.get("id")]
            merged[name] = copy
            continue
        stats = existing["stats"]
        incoming = camp.get("stats") or {}
        for field in _SUMMED:
            stats[field] = int(stats.get(field) or 0) + int(incoming.get(field) or 0)
        # Active on any profile means the campaign is still running.
        existing["active"] = bool(existing.get("active")) or bool(camp.get("active"))
        # Archived only when every instance is.
        existing["archived"] = bool(existing.get("archived")) and bool(camp.get("archived"))
        existing["_instance_ids"].append(camp.get("id"))
    return list(merged.values())


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
                raw: list[dict] = []
                for acc in accounts:
                    acc_id = acc.get("id")
                    if acc_id is None:
                        continue
                    raw.extend(await exc_client.list_campaigns(acc_id))

                campaigns = _merge_by_name(raw)
                print(f"{log} Expandi {ws.name}: {len(accounts)} account(s), "
                      f"{len(raw)} campaign instance(s) -> {len(campaigns)} campaign(s)")

                saved = store.save_snapshot(ws.name, campaigns, today.date())
                if saved:
                    print(f"{log} Expandi {ws.name}: snapshotted {saved} campaign(s)")
                # Drop any of today's rows for names no longer reported — see
                # purge_stale. Without this a renamed or newly-merged campaign
                # leaves a half-sized row that a later baseline lookup can find.
                dropped = store.purge_stale(
                    ws.name, [str(c.get("name", "")).strip() for c in campaigns],
                    today.date())
                if dropped:
                    print(f"{log} Expandi {ws.name}: purged {dropped} stale snapshot(s)")

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
                    name = str(camp.get("name", "")).strip()
                    rows.append(cm.expandi_metric_row(
                        camp,
                        store.baseline(ws.name, name, start_dt.date()),
                        store.previous_day(ws.name, name, today.date()),
                        client=ws.name,
                    ))
        except Exception as exc:  # noqa: BLE001
            print(f"[!] Expandi workspace {ws.name} failed: {exc}")
    return rows
