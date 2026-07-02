from smartlead.notify import build_digest

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

rows = [
    {"priority": "P0", "client": "DARLEAN", "email": "a@x.com", "top_problem": "Failed placement test",
     "owner": "👤 You", "manager": "Dmitrii", "_mgr_slack": "@dmitrii"},
    {"priority": "P1", "client": "Melior", "email": "b@y.com", "top_problem": "Stale test",
     "owner": "🤖 Auto", "manager": "Sam", "_mgr_slack": ""},
    {"priority": "", "client": "DARLEAN", "email": "ok@x.com", "top_problem": "", "owner": "", "manager": "Dmitrii"},
]
msg = build_digest(rows, "https://sheet")
ok("DARLEAN" in msg and "Melior" in msg, "clients present")
ok("a@x.com" in msg, "P0 inbox listed")
ok("ok@x.com" not in msg, "healthy inbox excluded from digest")
ok("@dmitrii" in msg, "manager mentioned")
ok("https://sheet" in msg, "workbook link present")
ok(msg.index("DARLEAN") < msg.index("Melior") or "P0" in msg, "P0 client surfaced")

# all-healthy path
msg2 = build_digest([{"priority": "", "client": "X", "email": "z@x.com"}], "https://s")
ok("all inboxes healthy" in msg2, "healthy-day message")
print("\nALL PASSED")
