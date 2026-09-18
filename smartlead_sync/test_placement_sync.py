"""Placement test results are memory, and a thin test is not a verdict.

The week of 2026-09-14 was lost to reading half-finished tests. Test 535281 said
88% inbox at 49% coverage and 57% at 84% - same senders, same domains, same day.
Unclassified seeds are dropped from the denominator and Google reports slower
than Microsoft, so an early read is biased toward whichever provider answered
first, not merely noisy.

So `scored` gates every verdict, and domain memory is built ONLY from scored
tests. A 25%-coverage read on 2026-09-18 put every weak domain at 100% inbox;
writing that as a verdict would have told the team a 28% domain was clean.
"""
from smartlead.placement_sync import summarise, verdict_for
from smartlead.config import PLACEMENT_MIN_COVERAGE


def ok(c, m):
    print(f"  {'PASS' if c else 'FAIL'}: {m}")
    assert c, m


def report(**kw):
    d = {"inbox_pct": 57.0, "spam_pct": 43.0, "classified": 836, "dispatched": 1000,
         "coverage": 0.836, "by_provider": {}, "worst_provider_inbox_pct": 34.3}
    d.update(kw)
    return d


def senders(*rows):
    return {e: {"inbox": i, "spam": c - i, "classified": c,
                "inbox_pct": 100.0 * i / c if c else 0.0,
                "spf_fail": 0, "dkim_fail": 0, "dmarc_fail": 0}
            for e, i, c in rows}


# --- a verdict needs BOTH a finished test and enough coverage ---
thin = summarise(535281, report(coverage=0.49, inbox_pct=88.1),
                 senders(("a@x.com", 7, 8)), done=True)
ok(thin["scored"] is False,
   f"49% coverage is not a verdict even when finished (gate {PLACEMENT_MIN_COVERAGE})")
ok(thin["inbox_pct"] == 88.1,
   "the thin percentage is still recorded - we keep the reading, we just do not trust it")

# 535799 read 47% coverage / 97.3% inbox mid-flight and settled at 90% / 98.6%.
# An unfinished test keeps classifying, so its numbers are provisional whatever
# the coverage says.
running = summarise(535799, report(coverage=0.90, inbox_pct=98.6),
                    senders(("a@x.com", 18, 18)), done=False)
ok(running["scored"] is False,
   "an ACTIVE test is never a verdict, even at 90% coverage")
ok(running["done"] is False, "and it is recorded as unfinished")

full = summarise(535537, report(), senders(("a@x.com", 17, 20)), done=True)
ok(full["scored"] is True, "finished + 84% coverage clears the gate")
ok(full["done"] is True, "and is recorded as finished")

# --- per-domain rollup: mailboxes on one domain sum, they do not average ---
row = summarise(535537, report(), done=True, senders=senders(
    ("aaron@ingestresolve.com", 8, 20),
    ("aarond@ingestresolve.com", 4, 20),
    ("aaron.dix@ingestresolve.com", 5, 20),
    ("aaron@onebettrdata.com", 20, 20),
))
dom = row["domains"]
ok(dom["ingestresolve.com"]["classified"] == 60, "three mailboxes roll into one domain")
ok(dom["ingestresolve.com"]["inbox_pct"] == 28.3,
   f"17/60 = 28.3% (got {dom['ingestresolve.com']['inbox_pct']})")
ok(dom["onebettrdata.com"]["inbox_pct"] == 100.0, "clean domain stays 100%")
ok(row["sender_count"] == 4, "every classified sender is kept")

# --- a sender with no classified seeds is absent, not zero ---
partial = summarise(1, report(), done=True, senders={
    "live@x.com": {"inbox": 5, "spam": 0, "classified": 5, "inbox_pct": 100.0},
    "pending@x.com": {"inbox": 0, "spam": 0, "classified": 0, "inbox_pct": 0.0},
})
ok("pending@x.com" not in partial["senders"],
   "an unclassified sender is not recorded as 0% - that is a false spam verdict")
ok(partial["domains"]["x.com"]["classified"] == 5,
   "and it does not dilute its domain's denominator")

# --- verdict tiers, drawn on the 2026-09-17 audit ---
ok(verdict_for(100.0) == "GOOD", "100% -> GOOD")
ok(verdict_for(90.0) == "GOOD", "90% is the GOOD floor")
ok(verdict_for(79.0) == "OK", "79% (gohivecloud) -> OK")
ok(verdict_for(60.0) == "OK", "60% is the OK floor")
ok(verdict_for(47.0) == "WEAK", "47% -> WEAK")
ok(verdict_for(28.3) == "BAD", "28% (ingestresolve) -> BAD")

print("\nALL PASSED")
