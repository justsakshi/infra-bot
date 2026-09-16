"""An inbox's load is bounded by its own daily limit.

true_load summed each campaign's `max_leads_per_day / inbox_count` across every
campaign an inbox belonged to, as if the campaigns stacked. They do not:
`message_per_day` is the mailbox's hard ceiling, and campaigns compete for
those sends. On 2026-09-16 that produced true_load 210.8 against a 15/day cap
on jennifer.kramer@bettrdatas.com, and ALL 57 BettrData inboxes read over
their limit.

The consequence was not cosmetic. available_capacity is max(0, limit - load),
so it was 0 everywhere, every inbox got busy_reason "no_capacity", and
precise-automator - which selects inboxes off this column - had nothing to
assign fleet-wide.

Demand is still worth knowing (it says the fleet is oversubscribed), so it is
kept as its own figure rather than folded into load.
"""
from smartlead.processing import resolve_inbox_load

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


# --- the real failure: six campaigns claiming a 15/day mailbox ---
r = resolve_inbox_load(demand=210.8, inbox_limit=15)
ok(r["true_load"] == 15.0, f"load is capped at the inbox limit, got {r['true_load']}")
ok(r["demand"] == 210.8, "raw demand is preserved, not silently discarded")
ok(r["capacity"] == 0.0, "a genuinely saturated inbox has no capacity left")
ok(r["oversubscribed"], "and is flagged as oversubscribed")

# --- the normal case: demand below the limit ---
r = resolve_inbox_load(demand=8.3, inbox_limit=15)
ok(r["true_load"] == 8.3, "load below the limit passes through unchanged")
ok(r["capacity"] == 6.7, f"capacity is limit - load, got {r['capacity']}")
ok(not r["oversubscribed"], "not oversubscribed")

# --- exactly at the limit ---
r = resolve_inbox_load(demand=15.0, inbox_limit=15)
ok(r["true_load"] == 15.0 and r["capacity"] == 0.0, "exactly full: no capacity, no flag")
ok(not r["oversubscribed"], "meeting the limit exactly is not oversubscription")

# --- an idle inbox ---
r = resolve_inbox_load(demand=0.0, inbox_limit=15)
ok(r["true_load"] == 0.0 and r["capacity"] == 15.0, "idle inbox offers its full limit")

# --- a missing/zero limit must not hand out infinite capacity ---
r = resolve_inbox_load(demand=5.0, inbox_limit=0)
ok(r["capacity"] == 0.0, f"no known limit -> no capacity claimed, got {r['capacity']}")
ok(r["true_load"] == 0.0, "and no load asserted either")

# --- rounding stays at one decimal, matching the sheet ---
r = resolve_inbox_load(demand=8.33333, inbox_limit=15)
ok(r["true_load"] == 8.3, f"load rounded to 1dp, got {r['true_load']}")
ok(r["capacity"] == 6.7, f"capacity rounded to 1dp, got {r['capacity']}")

# --- capacity can never go negative, whatever the inputs ---
for demand, limit in ((1000, 15), (15.1, 15), (0.1, 0)):
    ok(resolve_inbox_load(demand=demand, inbox_limit=limit)["capacity"] >= 0,
       f"capacity never negative for demand={demand} limit={limit}")

# ── campaign_share: which campaigns count toward an inbox's load ──────────
from smartlead.processing import campaign_share

share, counts = campaign_share("ACTIVE", 100, 7, 50)
ok(share == 14.3, f"100/day over 7 inboxes = 14.3 each, got {share}")
ok(counts, "an ACTIVE campaign with leads left counts as load")

share, counts = campaign_share("IN_PROGRESS", 60, 4, 10)
ok(share == 15.0 and counts, "IN_PROGRESS counts too")

# The dominant error: paused campaigns were summed as if still sending.
share, counts = campaign_share("PAUSED", 100, 7, 500)
ok(share == 14.3, "a paused campaign's share is still computed for its own row")
ok(not counts, "but a PAUSED campaign is not a claim on the mailbox today")
for st in ("COMPLETED", "ARCHIVED", "STOPPED", "DRAFTED"):
    ok(not campaign_share(st, 100, 7, 500)[1], f"{st} does not count")

# No leads left means nothing to send, whatever the status.
share, counts = campaign_share("ACTIVE", 100, 7, 0)
ok(share == 0.0 and not counts, "ACTIVE with no leads remaining is zero load")

# No inboxes attached: nothing to divide by, and nothing sending.
share, counts = campaign_share("ACTIVE", 100, 0, 50)
ok(share == 0.0 and not counts, "no inboxes -> zero share, no load")

# Case-insensitive status, missing limit.
ok(campaign_share("active", 30, 3, 5) == (10.0, True), "lowercase status")
ok(campaign_share("ACTIVE", None, 3, 5) == (0.0, True), "missing limit -> 0 share, still counts")

# Live reproduction: jennifer.kramer@bettrdatas.com, 6 campaigns, 2 active.
# Old figure summed all six; the honest demand is only the two that send.
all_six = [("ACTIVE", 100, 7), ("ACTIVE", 140, 9), ("PAUSED", 90, 6),
           ("PAUSED", 100, 6), ("PAUSED", 200, 3), ("PAUSED", 350, 4)]
demand = sum(s for s, c in (campaign_share(st, lim, n, 10) for st, lim, n in all_six) if c)
naive = sum(campaign_share(st, lim, n, 10)[0] for st, lim, n in all_six)
ok(round(demand, 1) == 29.9, f"active-only demand ~30/day, got {demand:.1f}")
ok(naive > 150, f"the old all-status sum was >150 for the same inbox, got {naive:.1f}")

print("\nALL PASSED")
