from smartlead.manager_map import resolve_manager, MANAGER_MAP

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

ok("DARLEAN" in MANAGER_MAP, "DARLEAN mapped")
# multi-client-under-one-account: Melior/Precise Leads/Better Data distinct
ok("Melior" in MANAGER_MAP, "Melior present")
ok("Bettrdata" in MANAGER_MAP, "Better Data (Bettrdata) present")
ok(resolve_manager("Melior") is not resolve_manager("Precise Leads"), "distinct entries")
ok(resolve_manager("NoSuchClient") == {"name": "Unassigned", "slack": ""}, "unknown -> Unassigned")
print("\nALL PASSED")
