"""Campaign Summary rows account for every lead.

`campaign_lead_stats` has six exclusive states that sum to total. The row
omitted `blocked`, so on 2026-09-16 "Variant 1 : Consumer DM+Home Services"
showed states summing to 121 against 124 total - the 3 blocked leads. Same
class of bug as the Campaign Metrics tab had the day before.
"""
from smartlead.processing import _summary_row, _basic_campaign_row

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

STATES = ("not_started", "in_progress", "paused", "completed", "stopped", "blocked")

# Live shape from campaign 3867136 on 2026-09-16.
an = {"sent_count": "359", "unique_sent_count": "124", "open_count": "200",
      "reply_count": "1", "bounce_count": "5",
      "campaign_lead_stats": {"total": 124, "notStarted": 0, "inprogress": 3,
                              "paused": 0, "completed": 117, "stopped": 0,
                              "blocked": 4, "interested": 0}}
r = _summary_row("3867136", "Variant 1", "PAUSED", an)
ok(r["blocked"] == 4, f"blocked is reported, got {r.get('blocked')}")
ok(sum(int(r[k]) for k in STATES) == r["total_leads"],
   f"six states sum to total ({r['total_leads']}), got {sum(int(r[k]) for k in STATES)}")
ok(r["sent"] == 359 and r["replied"] == 1 and r["bounced"] == 5, "counters pass through")
ok(r["reach_pct"] == "100.0%", f"reach = unique_sent/total, got {r['reach_pct']}")

# opened falls back to unique_open_count when open_count is 0 (no-open-tracking campaigns)
an2 = dict(an, open_count="0", unique_open_count="17")
ok(_summary_row("x", "n", "ACTIVE", an2)["opened"] == 17, "opened falls back to unique opens")

# No leads: no division, 0% reach, every state 0.
empty = {"campaign_lead_stats": {"total": 0}}
r = _summary_row("x", "n", "DRAFTED", empty)
ok(r["reach_pct"] == "0%" and r["total_leads"] == 0, "empty campaign is safe")
ok(all(r[k] == 0 for k in STATES), "all states zero when absent")

# Missing stats block entirely.
r = _summary_row("x", "n", "ACTIVE", {})
ok(r["total_leads"] == 0 and r["blocked"] == 0, "absent lead stats -> zeros, no crash")

# The basic (no-analytics) row carries the same keys so the tab's columns line up.
b = _basic_campaign_row({"id": "1", "name": "n", "status": "DRAFTED"})
ok(set(b) == set(_summary_row("1", "n", "DRAFTED", an)),
   "basic row and full row have identical columns")
ok(b["blocked"] == "-", "basic row shows '-' for blocked, not a fake 0")

print("\nALL PASSED")
