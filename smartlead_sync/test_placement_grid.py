"""Grid-writer tests. The writer edits a sheet the team works from, so the
cases here are the ones that would corrupt it: wrong row, duplicate column,
overwritten manual correction.
"""
from smartlead.placement_grid import (
    parse_grid_header, find_domain_row, plan_writes, column_label_for,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

# A miniature of the real Bettrdata tab: date row, label row, then domains.
GRID = [
    ["", "", "", "", "19 August", "", "26 August", ""],
    ["Domains ", "ESP", "Purchased On", "", "G suite", "Outlook", "G suite", "Outlook"],
    ["heybettrdata.com", "Google", "09 Jun", "old", "Inbox", "spam", "Inbox", "spam"],
    ["bettrdatas.com", "Outlook", "29 Jul", "new", "Inbox", "Inbox", "", ""],
    ["thebettrdatas.com", "Google", "29 Jul", "new", "", "", "", ""],
]

# --- header parsing ---
h = parse_grid_header(GRID)
ok(h.date_row == 0, "date row found at 0")
ok(h.label_row == 1, "label row found at 1")
ok(h.pairs == {"19 August": (4, 5), "26 August": (6, 7)},
   f"column pairs mapped by date label, got {h.pairs}")

# A tab whose header is missing must be refused, not guessed at.
missing = parse_grid_header([["a", "b"], ["c", "d"]])
ok(missing is None, "no G suite/Outlook labels -> None (refuse to write)")

# --- row lookup: exact match only ---
ok(find_domain_row(GRID, "heybettrdata.com") == 2, "exact domain -> row 2")
ok(find_domain_row(GRID, "HeyBettrData.com") == 2, "case-insensitive match")
ok(find_domain_row(GRID, "bettrdatas.com") == 3, "bettrdatas.com -> row 3")
# The prefix trap: bettrdatas.com is a substring of thebettrdatas.com.
ok(find_domain_row(GRID, "thebettrdatas.com") == 4,
   "thebettrdatas.com -> row 4, NOT the bettrdatas.com row")
ok(find_domain_row(GRID, "nosuch.com") is None, "unknown domain -> None")

# --- column label ---
ok(column_label_for("2026-09-11") == "11 September", "date -> '11 September'")
ok(column_label_for("2026-08-01") == "1 August", "no zero padding on day")

# --- write planning: blank cells only ---
by_provider = {"G Suite": {"inbox_pct": 100.0}, "Office365": {"inbox_pct": 0.0}}

# Existing column, both cells already filled -> nothing written.
plan = plan_writes(GRID, "heybettrdata.com", by_provider, "26 August", threshold=80)
ok(plan.writes == [], "filled cells are never overwritten")
ok(plan.skipped_reason == "cells already filled", f"reason recorded, got {plan.skipped_reason}")

# Existing column, blank cells -> writes into that pair, no new column.
plan = plan_writes(GRID, "bettrdatas.com", by_provider, "26 August", threshold=80)
ok(plan.new_column_label is None, "existing date -> no new column created")
ok(plan.writes == [(3, 6, "Inbox"), (3, 7, "spam")],
   f"G Suite 100 -> Inbox, Office365 0 -> spam, got {plan.writes}")

# New date -> appends one pair, and only one.
plan = plan_writes(GRID, "thebettrdatas.com", by_provider, "11 September", threshold=80)
ok(plan.new_column_label == "11 September", "unseen date -> new column pair")
ok(plan.new_column_indices == (8, 9), f"appended at 8,9, got {plan.new_column_indices}")
ok(plan.writes == [(4, 8, "Inbox"), (4, 9, "spam")], f"writes into new pair, got {plan.writes}")

# Threshold is per provider, not blended: 90/70 must not average to a pass.
mixed = {"G Suite": {"inbox_pct": 90.0}, "Office365": {"inbox_pct": 70.0}}
plan = plan_writes(GRID, "bettrdatas.com", mixed, "26 August", threshold=80)
ok(plan.writes == [(3, 6, "Inbox"), (3, 7, "spam")],
   "70% Office365 is judged on its own -> spam")

# Unknown domain -> refuse, never write to a guessed row.
plan = plan_writes(GRID, "notinsheet.com", by_provider, "26 August", threshold=80)
ok(plan.writes == [], "unknown domain -> no writes")
ok(plan.skipped_reason == "domain not in sheet", f"got {plan.skipped_reason}")

# A provider missing from the report leaves its cell alone rather than
# writing a fabricated verdict.
partial = {"G Suite": {"inbox_pct": 100.0}}
plan = plan_writes(GRID, "bettrdatas.com", partial, "26 August", threshold=80)
ok(plan.writes == [(3, 6, "Inbox")], f"only the reported provider is written, got {plan.writes}")

print("\nALL PASSED")
