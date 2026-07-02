from datetime import date
from smartlead.campaign_freshness import is_campaign_stale

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TODAY = date(2026, 7, 3)

# Fresh: lead added recently
ok(is_campaign_stale(newest_lead_date="2026-06-29", sent_last_14d=0, today=TODAY) is False,
   "recent lead -> fresh")
# Fresh: sending recently even if no new leads
ok(is_campaign_stale(newest_lead_date="2026-05-01", sent_last_14d=151, today=TODAY) is False,
   "recent sends -> fresh")
# Stale: no lead in 14d AND no sends in 14d
ok(is_campaign_stale(newest_lead_date="2026-06-01", sent_last_14d=0, today=TODAY) is True,
   "old lead + no sends -> STALE")
# Stale: no lead data at all + no sends
ok(is_campaign_stale(newest_lead_date="", sent_last_14d=0, today=TODAY) is True,
   "no leads + no sends -> STALE")
# Edge: exactly 14 days is the boundary (>=14 stale if no sends)
ok(is_campaign_stale(newest_lead_date="2026-06-19", sent_last_14d=0, today=TODAY) is True,
   "14d-old lead + no sends -> STALE")
ok(is_campaign_stale(newest_lead_date="2026-06-20", sent_last_14d=0, today=TODAY) is False,
   "13d-old lead -> fresh")
print("\nALL PASSED")
