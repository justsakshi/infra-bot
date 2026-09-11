"""Rotation and due-date logic for the weekly placement run.

Pure functions over plain dicts, so the rules that decide credit spend can be
pinned down without Mongo.
"""
from smartlead.placement_schedule import (
    next_inbox, is_due, advance, select_batch,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

INBOXES = ["a@d.com", "b@d.com", "c@d.com"]

# --- rotation ---
ok(next_inbox(INBOXES, 0) == "a@d.com", "index 0 -> first inbox")
ok(next_inbox(INBOXES, 1) == "b@d.com", "index 1 -> second")
ok(next_inbox(INBOXES, 3) == "a@d.com", "wraps around")
# An inbox removed from Smartlead must not break rotation.
ok(next_inbox(["a@d.com"], 7) == "a@d.com", "index beyond list length is clamped")
ok(next_inbox([], 0) is None, "no inboxes -> None")

# Sorting is what makes rotation deterministic; Smartlead's order is not stable.
ok(next_inbox(["c@d.com", "a@d.com", "b@d.com"], 0) == "a@d.com",
   "inbox list is sorted before indexing")

# --- due dates ---
ok(is_due({}, "2026-09-11", 7), "never tested -> due")
ok(is_due({"last_tested_date": "2026-09-01"}, "2026-09-11", 7), "10 days ago -> due")
ok(is_due({"last_tested_date": "2026-09-04"}, "2026-09-11", 7), "exactly 7 days -> due")
ok(not is_due({"last_tested_date": "2026-09-05"}, "2026-09-11", 7), "6 days -> not due")
ok(not is_due({"last_tested_date": "2026-09-11"}, "2026-09-11", 7), "same day -> not due")
# A malformed date must not be read as "tested recently" — that would silently
# park a domain forever.
ok(is_due({"last_tested_date": "garbage"}, "2026-09-11", 7), "unparseable date -> due")

# --- advancing on result ---
rec = {"rotation_index": 0, "consecutive_fails": 0}
after = advance(rec, "a@d.com", "2026-09-11", "inbox", {"G Suite": {"inbox_pct": 100.0}})
ok(after["rotation_index"] == 1, "index advances on a recorded result")
ok(after["last_tested_email"] == "a@d.com", "records which inbox was used")
ok(after["last_tested_date"] == "2026-09-11", "records the date")
ok(after["consecutive_fails"] == 0, "a pass resets the fail counter")

failed = advance({"rotation_index": 1, "consecutive_fails": 1},
                 "b@d.com", "2026-09-11", "fail", {})
ok(failed["rotation_index"] == 2, "index advances on a fail too - next week tries another inbox")
ok(failed["consecutive_fails"] == 2, "consecutive fails accumulate")

# --- batch selection ---
DOMAINS = {
    "never.com":   {"inboxes": ["x@never.com", "y@never.com"]},
    "fresh.com":   {"inboxes": ["x@fresh.com"], "last_tested_date": "2026-09-10"},
    "stale.com":   {"inboxes": ["x@stale.com"], "last_tested_date": "2026-08-01"},
    "failing.com": {"inboxes": ["x@failing.com"], "last_tested_date": "2026-08-20",
                    "consecutive_fails": 3},
}

batch = select_batch(DOMAINS, today="2026-09-11", interval_days=7, pending=set())
picked = [b["domain"] for b in batch]
ok("fresh.com" not in picked, "a domain tested 1 day ago is not re-tested")
ok(set(picked) == {"failing.com", "stale.com", "never.com"}, f"due domains selected, got {picked}")
# Worst-first: repeated failures are the most urgent thing to re-check.
ok(picked[0] == "failing.com", f"highest consecutive_fails goes first, got {picked}")
# Never-tested outranks long-ago-tested: no data at all is a bigger blind spot
# than stale data.
ok(picked[1] == "never.com", f"then never-tested, got {picked}")
ok(picked[2] == "stale.com", f"then oldest tested, got {picked}")

# An in-flight test must not be duplicated.
batch = select_batch(DOMAINS, today="2026-09-11", interval_days=7,
                     pending={"x@stale.com"})
ok("stale.com" not in [b["domain"] for b in batch], "domain with a pending test is skipped")

# A capped batch keeps the worst, not an arbitrary prefix.
batch = select_batch(DOMAINS, today="2026-09-11", interval_days=7, pending=set(), cap=1)
ok([b["domain"] for b in batch] == ["failing.com"], "cap keeps the most urgent")

# Each selection names the inbox to use, so the caller never re-derives it.
batch = select_batch({"never.com": DOMAINS["never.com"]}, today="2026-09-11",
                     interval_days=7, pending=set())
ok(batch[0]["email"] == "x@never.com", f"selection carries the inbox, got {batch[0]}")

print("\nALL PASSED")
