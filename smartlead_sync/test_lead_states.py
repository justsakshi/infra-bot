"""Lead-state accounting for the Campaign Metrics tab.

`campaign_lead_stats` reports six mutually-exclusive states that sum to total:
inprogress, notStarted, completed, blocked, paused, stopped. (`interested` is a
category that overlaps the others and is NOT part of the sum.)

The tab reported only the first two, so every row silently lost the rest. On
2026-09-15, "Marketplace | Manager Beta Feedback" read 14 in-progress and 0
not-started against a total of 100 - hiding 83 completed leads and making a
nearly-finished campaign look barely started. Verified against the live API:
all six states summed to total on every campaign that had any leads.
"""
from smartlead.campaign_metrics import smartlead_summary_from_analytics

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def analytics(**stats):
    base = {"total": 0, "inprogress": 0, "notStarted": 0, "completed": 0,
            "blocked": 0, "paused": 0, "stopped": 0, "interested": 0}
    base.update(stats)
    return {"id": 1, "name": "c", "status": "ACTIVE", "sent_count": "0",
            "campaign_lead_stats": base}


# Live shape, campaign 3934047 on 2026-09-15.
s = smartlead_summary_from_analytics(analytics(
    total=739, inprogress=574, notStarted=38, completed=124, blocked=3, interested=1))
ok(s["total_leads"] == 739, "total passed through")
ok(s["in_progress"] == 574, "in-progress read")
ok(s["not_started"] == 38, "not-started read")
ok(s["completed"] == 124, f"completed read, got {s.get('completed')}")
ok(s["blocked"] == 3, f"blocked read, got {s.get('blocked')}")

# The invariant: the six exclusive states account for every lead.
accounted = (s["in_progress"] + s["not_started"] + s["completed"]
             + s["blocked"] + s["paused"] + s["stopped"])
ok(accounted == s["total_leads"],
   f"six states account for all {s['total_leads']} leads, got {accounted}")
ok(s["unaccounted"] == 0, f"nothing unaccounted, got {s['unaccounted']}")

# The case that made the tab misleading: most leads finished.
s = smartlead_summary_from_analytics(analytics(
    total=100, inprogress=14, notStarted=0, completed=83, blocked=3))
ok(s["completed"] == 83, "a nearly-finished campaign reports its completed leads")
ok(s["in_progress"] + s["not_started"] + s["completed"] + s["blocked"] == 100,
   "states still account for the full campaign")
ok(s["unaccounted"] == 0, "no gap")

# A campaign with no leads must not divide or report phantom states.
s = smartlead_summary_from_analytics(analytics(total=0))
ok(s["total_leads"] == 0 and s["completed"] == 0, "empty campaign is all zeros")
ok(s["unaccounted"] == 0, "empty campaign has no gap")

# `interested` overlaps the other states, so counting it would double-count.
s = smartlead_summary_from_analytics(analytics(
    total=10, inprogress=6, notStarted=4, interested=3))
ok(s["unaccounted"] == 0,
   "interested is a category, not a state - it must not create a gap")

# Missing stats block entirely (older payloads) must not crash.
s = smartlead_summary_from_analytics({"id": 2, "name": "x", "status": "ACTIVE"})
ok(s["total_leads"] == 0 and s["in_progress"] == 0, "absent lead stats -> zeros")

# If Smartlead ever adds a state we do not know about, the gap must be visible
# rather than silently swallowed.
s = smartlead_summary_from_analytics(analytics(total=100, inprogress=40, notStarted=10))
ok(s["unaccounted"] == 50,
   f"unexplained leads are surfaced, not hidden, got {s['unaccounted']}")

print("\nALL PASSED")
