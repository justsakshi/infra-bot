import os
from smartlead.heyreach_accounts import discover_heyreach_workspaces

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

os.environ["HEYREACH_API_KEY_DARLEAN"] = "key_d"
os.environ["HEYREACH_API_KEY_MELIOR"] = "key_m"
ws = {w.name: w.api_key for w in discover_heyreach_workspaces()}
ok(ws.get("DARLEAN") == "key_d", f"DARLEAN discovered ({ws})")
ok(ws.get("MELIOR") == "key_m", "MELIOR discovered")
print("\nALL PASSED")
