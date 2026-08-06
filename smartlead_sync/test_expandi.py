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
ok(r["total_leads"] == 45, f"total_leads is all-time (got {r['total_leads']})")
ok(r["leads_in_progress"] == 11, "in_queue -> leads_in_progress")
ok(r["connections_sent"] == 9, f"initiated 29-20==9 (got {r['connections_sent']})")
ok(r["connections_accepted"] == 3, f"connected 4-1==3 (got {r['connections_accepted']})")
ok(r["msg_sent"] == 9, f"contacted 29-20==9 (got {r['msg_sent']})")
ok(r["total_responses_month"] == 2, f"replies (2+1)-(1+0)==2 (got {r['total_responses_month']})")
ok(r["positive_neutral_month"] == 2, f"interested 3-1==2 (got {r['positive_neutral_month']})")
ok(r["leads_added_yesterday"] == 1, f"45-44==1 vs prev day (got {r['leads_added_yesterday']})")
ok(r["positive_responses_yesterday"] == 1, f"interested 3-2==1 (got {r['positive_responses_yesterday']})")
ok(r["leads_not_started"] == "-", "no not-started concept in Expandi")

# --- no baseline: month columns must NOT fall back to all-time ---
nb = expandi_metric_row(CAMP, None, None, client="BETTRDATA")
ok(nb["connections_sent"] == "?", f"no baseline -> '?' not 29 (got {nb['connections_sent']})")
ok(nb["msg_sent"] == "?", f"no baseline -> '?' (got {nb['msg_sent']})")
ok(nb["total_responses_month"] == "?", f"no baseline -> '?' (got {nb['total_responses_month']})")
ok(nb["positive_neutral_month"] == "?", "no baseline -> '?'")
ok(nb["leads_added_yesterday"] == "?", "no prev day -> '?'")
ok(nb["total_leads"] == 45, "total_leads still real without a baseline")

# A '?' must not be summed into the Total row as a zero or crash it.
tot = total_row([r, nb], client="BETTRDATA")
ok(tot["connections_sent"] == 9, f"total skips '?' rows (got {tot['connections_sent']})")
ok(tot["total_leads"] == 90, f"total_leads 45+45 (got {tot['total_leads']})")

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

print("\nALL PASSED")
