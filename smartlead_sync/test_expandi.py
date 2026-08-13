"""Expandi metric rows, workspace discovery, and snapshot differencing."""
import os
import sys

from smartlead.campaign_metrics import expandi_metric_row, total_row, COLUMNS
from smartlead.expandi_accounts import discover_expandi_workspaces


def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


# Live shape, taken from a real response (Bettrdata- Retail).
CAMP = {
    "id": 813723, "name": "Bettrdata- Retail", "active": True,
    "stats": {
        "stopped": 0, "finished": 0, "in_queue": 11, "connected": 4,
        "initiated": 29, "step_count": 12, "replied_msg": 2,
        "contacted_people": 29, "latest_action_id": 32835657,
        "replied_excl_msg": 1, "interested_people": 3,
        "people_in_campaign": 45, "replied_first_action": 0,
        "not_interested_people": 0, "replied_other_actions": 0,
    },
}

# --- with a baseline: month-to-date is a difference ---
BASE = {"people_in_campaign": 40, "initiated": 20, "connected": 1,
        "contacted_people": 20, "replied_msg": 1, "replied_excl_msg": 0,
        "interested_people": 1}
PREV = {"people_in_campaign": 44, "initiated": 27, "connected": 3,
        "contacted_people": 27, "replied_msg": 2, "replied_excl_msg": 1,
        "interested_people": 2}

r = expandi_metric_row(CAMP, BASE, PREV, client="BETTRDATA")
ok(r["platform"] == "Expandi", "platform")
ok(r["status"] == "IN_PROGRESS", "active True -> IN_PROGRESS")
# total_leads is blank for Expandi: people_in_campaign counts the batch
# assigned to the sending instances, not the leads uploaded to the Shared
# Campaign, and only the latter is comparable with the Smartlead rows.
ok(r["total_leads"] == "-", f"total_leads blank for Expandi (got {r['total_leads']})")
ok(r["leads_not_started"] == "-", "not-started blank: it was derived from the same batch figure")
ok(r["leads_in_progress"] == 11, "in_queue -> leads_in_progress")
ok(r["connections_sent"] == 9, f"initiated 29-20==9 (got {r['connections_sent']})")
ok(r["connections_accepted"] == 3, f"connected 4-1==3 (got {r['connections_accepted']})")
ok(r["msg_sent"] == 9, f"contacted 29-20==9 (got {r['msg_sent']})")
# Responses are running totals, not month deltas — verified against the team's
# sheet, where every response figure matched the all-time count exactly.
ok(r["total_responses_month"] == 3, f"replies 2+1==3 all-time (got {r['total_responses_month']})")
ok(r["positive_neutral_month"] == 3, f"interested==3 all-time (got {r['positive_neutral_month']})")
# not-started is people minus initiated; in_queue is the send queue, not this.

# Leads added now come from each lead's own `created` day, not from
# differencing people_in_campaign against a snapshot. Without swept per-lead
# data the value is "-" rather than a number derived from a standing total.
ok(r["leads_added_yesterday"] == "-",
   f"no swept lead data -> '-' for yesterday's intake (got {r['leads_added_yesterday']})")
ok(r["positive_responses_yesterday"] == 1, f"interested 3-2==1 (got {r['positive_responses_yesterday']})")

# --- no baseline: month columns must NOT fall back to all-time ---
nb = expandi_metric_row(CAMP, None, None, client="BETTRDATA")
# With neither per-lead data nor a snapshot, windowed columns are "-".
# An earlier version substituted the all-time counter on the reasoning that a
# true number over a wider window beats a placeholder. In the sheet that read
# as a month figure and overstated every campaign — most visibly on leads
# added, where a campaign's whole history appeared as both this month's intake
# and yesterday's. A lifetime total in a month column is a wrong answer the
# reader cannot detect.
ok(nb["connections_sent"] == "-", f"no baseline -> '-' (got {nb['connections_sent']})")
ok(nb["connections_accepted"] == "-", f"no baseline -> '-' (got {nb['connections_accepted']})")
ok(nb["msg_sent"] == "-", f"no baseline -> '-' (got {nb['msg_sent']})")
ok(nb["leads_added_yesterday"] == "-", "no per-lead data -> '-' for yesterday")
ok(nb["leads_added_month"] == "-", "no per-lead data -> '-' for the month")
# Standing totals are unaffected — they answer a question that needs no window.
ok(nb["total_leads"] == "-", "total_leads blank regardless of baseline")
ok(nb["total_responses_month"] == 3, "responses are running totals, still real")
ok(nb["leads_not_started"] == "-", "not-started blank regardless of baseline")
# Running totals need no baseline, so these are real from the very first run —
# only the windowed columns depend on snapshot history.
ok(nb["total_responses_month"] == 3, f"responses real without a baseline (got {nb['total_responses_month']})")
ok(nb["positive_neutral_month"] == 3, "positive/neutral real without a baseline")


# A '?' must not be summed into the Total row as a zero or crash it.
tot = total_row([r, nb], client="BETTRDATA")
# The '-' row contributes nothing, so the total reflects only campaigns whose
# window is actually known. Previously it added an all-time counter here and
# reported 38 for a month in which 9 connections were sent.
ok(tot["connections_sent"] == 9,
   f"total counts only known windows, skipping '-' (got {tot['connections_sent']})")
ok(tot["total_leads"] == 0, "Total row skips the blank Expandi lead counts")

# A non-numeric cell must still be skipped rather than crashing the total —
# Smartlead rows carry "-" in the LinkedIn-only columns.
dashed = {**r, "connections_sent": "-"}
ok(total_row([dashed, nb], client="X")["connections_sent"] == 0,
   "a Total over only '-' rows is 0, not a crash")
# And a mix still sums just the known values.
ok(total_row([dashed, r], client="X")["connections_sent"] == 9,
   "mixed '-' and numeric rows sum only the numeric ones")

# --- counters must never go backwards ---
# Expandi can restate a counter downward (contacts removed from a campaign).
# A negative "sent this month" is worse than a zero: it silently subtracts from
# the Total row and makes the whole column untrustworthy.
shrunk = expandi_metric_row(
    CAMP, {**BASE, "initiated": 999}, PREV, client="BETTRDATA")
ok(shrunk["connections_sent"] == 0,
   f"counter going backwards clamps to 0 (got {shrunk['connections_sent']})")

# --- paused campaign ---
paused = expandi_metric_row({**CAMP, "active": False}, BASE, PREV)
ok(paused["status"] == "PAUSED", "active False -> PAUSED")

# --- row shape matches the sheet ---
ok(set(r.keys()) == set(COLUMNS), "row keys match COLUMNS exactly")

# --- workspace discovery needs BOTH key and secret ---
saved = {k: v for k, v in os.environ.items() if k.startswith("EXPANDEE_")}
for k in saved:
    os.environ.pop(k, None)
try:
    os.environ["EXPANDEE_API_KEY"] = "k1"
    os.environ["EXPANDEE_SECRET"] = "s1"
    ws = discover_expandi_workspaces()
    ok(len(ws) == 1 and ws[0].name == "BETTRDATA",
       f"unsuffixed pair -> BETTRDATA (got {[w.name for w in ws]})")

    os.environ["EXPANDEE_API_KEY_ACME"] = "k2"  # no matching secret
    ws = discover_expandi_workspaces()
    ok([w.name for w in ws] == ["BETTRDATA"],
       f"key without secret is skipped (got {[w.name for w in ws]})")

    os.environ["EXPANDEE_SECRET_ACME"] = "s2"
    ws = discover_expandi_workspaces()
    ok(sorted(w.name for w in ws) == ["ACME", "BETTRDATA"],
       f"suffixed pair discovered (got {sorted(w.name for w in ws)})")
finally:
    for k in list(os.environ):
        if k.startswith("EXPANDEE_"):
            os.environ.pop(k, None)
    os.environ.update(saved)


# --- merging campaign instances across LinkedIn accounts ---
# Expandi runs one campaign from several sender profiles and returns each as its
# own instance. Real numbers: "BD Select" came back as 60 and 66 connections,
# and the team's sheet reports the single campaign at 126.
from smartlead.expandi_rows import _merge_by_name  # noqa: E402

INSTANCES = [
    {"id": 812428, "name": "BD Select : Data Providers (Tier B)", "active": True,
     "stats": {"initiated": 60, "connected": 7, "people_in_campaign": 60,
               "replied_msg": 0, "step_count": 12}},
    {"id": 812427, "name": "BD Select : Data Providers (Tier B)", "active": False,
     "stats": {"initiated": 66, "connected": 2, "people_in_campaign": 66,
               "replied_msg": 1, "step_count": 12}},
    {"id": 813723, "name": "Bettrdata- Retail", "active": True,
     "stats": {"initiated": 29, "connected": 4, "people_in_campaign": 45,
               "replied_msg": 0, "step_count": 12}},
]
m = _merge_by_name(INSTANCES)
ok(len(m) == 2, f"3 instances -> 2 campaigns (got {len(m)})")
bd = next(c for c in m if c["name"].startswith("BD Select"))
ok(bd["stats"]["initiated"] == 126, f"60+66==126, matching the manual sheet (got {bd['stats']['initiated']})")
ok(bd["stats"]["connected"] == 9, f"7+2==9 (got {bd['stats']['connected']})")
ok(bd["stats"]["replied_msg"] == 1, "replies summed across instances")
ok(bd["stats"]["step_count"] == 12,
   f"step_count is NOT summed - a 12-step campaign stays 12 (got {bd['stats']['step_count']})")
ok(bd["active"] is True, "active on any profile means the campaign is running")
ok(len(bd["_instance_ids"]) == 2, "both instance ids retained")
ok(bd["id"] == 812427, f"lowest id wins, so the snapshot key is stable (got {bd['id']})")

# Merging must not disturb a campaign that runs on only one profile.
retail = next(c for c in m if c["name"] == "Bettrdata- Retail")
ok(retail["stats"]["initiated"] == 29, "single-instance campaign is unchanged")

# Merging must not mutate the caller's data — build_expandi_rows snapshots the
# merged list, and a mutated input would corrupt the stored counters.
ok(INSTANCES[0]["stats"]["initiated"] == 60, "input instances are not mutated")


# --- the snapshot store keys on name, not id ---
# Merging picks one instance's id to survive, and which one can change between
# runs. An id-keyed snapshot then writes a second row for the same campaign,
# and a later baseline lookup can find the stale half-sized one — reporting a
# delta of +66 connections that never happened. Caught in production: the store
# held id=812428/initiated=60 beside id=812427/initiated=126 for one campaign.
import inspect  # noqa: E402
from smartlead import expandi_store as _es  # noqa: E402

_save = inspect.getsource(_es.ExpandiStore.save_snapshot)
ok('"campaign_name": doc["campaign_name"]' in _save,
   "save_snapshot upserts on campaign_name")
ok('"campaign_id"' not in _save.split("UpdateOne")[1].split("{\"$set\"")[0],
   "save_snapshot filter does not key on campaign_id")

_init = inspect.getsource(_es.ExpandiStore.__init__)
ok('("campaign_name", 1)' in _init, "unique index is on campaign_name")
ok('("campaign_id", 1)' not in _init, "unique index is not on campaign_id")

for fn in ("baseline", "previous_day"):
    src = inspect.getsource(getattr(_es.ExpandiStore, fn))
    ok("campaign_name" in src, f"{fn} looks up by campaign_name")
    ok("campaign_id" not in src, f"{fn} does not look up by campaign_id")

ok(hasattr(_es.ExpandiStore, "purge_stale"),
   "store can purge snapshots for names no longer reported")

print("\nALL PASSED")
