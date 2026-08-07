"""Per-lead Expandi activity: day bucketing, completeness gating, fallback."""
from smartlead.campaign_metrics import expandi_metric_row
from smartlead.expandi_leads import _day
from smartlead.expandi_rows import _merge_by_name


def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


# --- timestamp -> UTC day ---
# Expandi returns local-offset stamps with no colon in the offset ('+0200'),
# which several parsers reject outright.
ok(_day("2026-07-10T21:05:01+0200") == "2026-07-10", "parses Expandi's offset format")
ok(_day("2026-08-06T23:30:00+0000") == "2026-08-06", "UTC stamp keeps its day")
# 00:30 at +0200 is 22:30 the previous day in UTC. Bucketing by the local date
# would file this under the 7th while every other platform files it under the
# 6th, so the same action would appear on different days by platform.
ok(_day("2026-08-07T00:30:00+0200") == "2026-08-06", "converts to UTC before taking the day")
ok(_day(None) is None, "None stays None")
ok(_day("") is None, "empty string is not a day")
ok(_day("not-a-date") is None, "garbage does not raise")

# --- row uses per-lead counts when the cache is complete ---
CAMP = {"name": "BD Select", "active": True, "stats": {
    "people_in_campaign": 126, "in_queue": 0, "initiated": 126, "connected": 9,
    "contacted_people": 126, "replied_msg": 1, "replied_excl_msg": 0,
    "interested_people": 1}}
BASE = {"initiated": 100, "connected": 5, "contacted_people": 100,
        "people_in_campaign": 120}
FULL = {"cached": 126, "invited_month": 41, "connected_month": 4,
        "invited_yesterday": 12, "connected_yesterday": 1}

r = expandi_metric_row(CAMP, BASE, None, client="BETTRDATA", lead_counts=FULL)
ok(r["connections_sent"] == 41,
   f"per-lead count wins over the snapshot delta of 26 (got {r['connections_sent']})")
ok(r["connections_accepted"] == 4,
   f"accepted from per-lead data (got {r['connections_accepted']})")

# --- a partially swept campaign must NOT be trusted ---
# 40 of 126 leads cached would report 12 invites as the month's total, which is
# wrong and reads exactly like a genuinely quiet month.
PARTIAL = {"cached": 40, "invited_month": 12, "connected_month": 1,
           "invited_yesterday": 3, "connected_yesterday": 0}
p = expandi_metric_row(CAMP, BASE, None, client="BETTRDATA", lead_counts=PARTIAL)
ok(p["connections_sent"] == 26,
   f"partial cache falls back to the snapshot delta 126-100 (got {p['connections_sent']})")

# --- no cache at all falls back cleanly ---
n = expandi_metric_row(CAMP, BASE, None, client="BETTRDATA", lead_counts={})
ok(n["connections_sent"] == 26, "empty lead_counts uses the snapshot delta")
n2 = expandi_metric_row(CAMP, None, None, client="BETTRDATA", lead_counts=None)
ok(n2["connections_sent"] == 126, "no baseline and no cache -> all-time, never 0")

# A campaign that has sent nothing must not be treated as "complete" just
# because both numbers are zero — cached >= contacted > 0 guards that.
ZERO = {"name": "New", "active": True, "stats": {
    "people_in_campaign": 50, "initiated": 0, "connected": 0,
    "contacted_people": 0, "in_queue": 50}}
z = expandi_metric_row(ZERO, None, None, client="X",
                       lead_counts={"cached": 0, "invited_month": 0,
                                    "connected_month": 0})
ok(z["connections_sent"] == 0, "a campaign that has sent nothing reports 0")
ok(z["leads_not_started"] == 50, "all 50 leads are not started")

# --- merge keeps (account, instance) pairs for the sweep ---
INST = [
    {"id": 812428, "name": "BD Select", "li_account": 182607, "active": True,
     "stats": {"initiated": 60, "connected": 7, "people_in_campaign": 60}},
    {"id": 812427, "name": "BD Select", "li_account": 154923, "active": False,
     "stats": {"initiated": 66, "connected": 2, "people_in_campaign": 66}},
]
m = _merge_by_name(INST)[0]
ok(sorted(m["_instance_refs"]) == [(154923, 812427), (182607, 812428)],
   f"both (account, instance) pairs survive the merge (got {m['_instance_refs']})")
ok(m["stats"]["initiated"] == 126, "stats still sum across instances")

print("\nALL PASSED")
