"""Build Expandi metric rows for the Campaign Metrics tab.

Lives here rather than inline in the entrypoints because run.py and
metrics_only.py both need it, and the Smartlead block being duplicated across
those two files is exactly how one of them drifted and started reporting
different numbers.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from smartlead import campaign_metrics as cm
from smartlead.config import (CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_EXCLUDE,
                              EXPANDI_LEAD_PAGES_PER_RUN, EXPANDI_LEAD_PAGE_SIZE)
from smartlead.expandi import ExpandiClient
from smartlead.expandi_accounts import discover_expandi_workspaces
from smartlead.expandi_leads import ExpandiLeadStore
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
            # (li_account_id, campaign_instance_id) pairs — the messengers
            # endpoint is keyed on both, so the owning account has to survive
            # the merge or the per-lead sweep cannot address the instance.
            copy["_instance_refs"] = [(camp.get("li_account"), camp.get("id"))]
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
        existing["_instance_refs"].append((camp.get("li_account"), camp.get("id")))
    return list(merged.values())


async def _sweep_lead_activity(client, leads, workspace: str,
                               campaigns: list[dict], log: str) -> None:
    """Top up the per-lead activity cache for each campaign.

    Bounded work per run. The messengers endpoint returns ~4.6 rows/second
    whatever the page size, so a full sweep of ~11k leads takes about 98
    minutes — unacceptable daily. Instead each campaign fetches at most
    EXPANDI_LEAD_PAGES_PER_RUN pages and stops as soon as a page contains
    nothing new, which after the initial backfill is the first page.

    Cached rows are immutable in practice (a lead invited on 28 July keeps that
    timestamp), so stopping early cannot miss an update to an existing row — it
    can only defer rows not yet seen, which the next run picks up.
    """
    if not leads.available:
        return
    known = leads.known_ids(workspace)
    swept = 0
    for camp in campaigns:
        name = str(camp.get("name", "")).strip()
        # Each merged campaign may span several LinkedIn accounts; messengers
        # are per account+instance, so every pairing has to be asked.
        for acc_id, inst_id in camp.get("_instance_refs", []):
            def stop_when(rows, _known=known):
                # A page whose ids are all cached means the tail is reached.
                return bool(rows) and all(r.get("id") in _known for r in rows)

            rows = await client.list_messengers(
                acc_id, inst_id,
                page_size=EXPANDI_LEAD_PAGE_SIZE,
                max_pages=EXPANDI_LEAD_PAGES_PER_RUN,
                stop_when=stop_when)
            if rows:
                leads.save(workspace, name, rows)
                known.update(r.get("id") for r in rows if r.get("id") is not None)
                swept += len(rows)
    # Always log, including the zero case. In steady state the sweep fetches
    # nothing because stop_when halts on the first fully-cached page, and a
    # silent run is indistinguishable from one that never executed — which is
    # exactly the ambiguity that makes a daily job hard to trust.
    print(f"{log} Expandi {workspace}: lead sweep fetched {swept} row(s), "
          f"cache holds {len(known)}")


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
    leads = ExpandiLeadStore()
    if not store.available:
        print(f"{log} Expandi: no Mongo — activity columns fall back to all-time")

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

                reportable = []
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
                    reportable.append(camp)

                await _sweep_lead_activity(exc_client, leads, ws.name,
                                           reportable, log)

                pending = []
                for camp in reportable:
                    name = str(camp.get("name", "")).strip()
                    yesterday = (today - timedelta(days=1)).date()
                    lc = leads.counts(ws.name, name, start_dt.date(),
                                      today.date(), yesterday)
                    contacted = int((camp.get("stats") or {}).get("initiated") or 0)
                    if contacted > 0 and int(lc.get("cached", 0)) < contacted:
                        pending.append(f"{name} ({lc.get('cached', 0)}/{contacted})")
                    rows.append(cm.expandi_metric_row(
                        camp,
                        store.baseline(ws.name, name, start_dt.date()),
                        store.previous_day(ws.name, name, today.date()),
                        client=ws.name,
                        lead_counts=lc,
                    ))
                if pending:
                    # These rows are on the less accurate snapshot path. Naming
                    # them means an incomplete backfill is visible in the log
                    # rather than quietly reported as a finished figure.
                    print(f"{log} Expandi {ws.name}: {len(pending)} campaign(s) "
                          f"still backfilling, using snapshot deltas: "
                          f"{'; '.join(pending[:5])}"
                          f"{' …' if len(pending) > 5 else ''}")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] Expandi workspace {ws.name} failed: {exc}")
    return rows
