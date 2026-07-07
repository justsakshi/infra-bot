from smartlead.client_filter import is_excluded_inbox

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

# excluded by domain
ok(is_excluded_inbox({"email": "sam@getavench.com", "tags": []}), "avench domain -> excluded")
ok(is_excluded_inbox({"email": "x@runmonarch.com", "tags": []}), "monarch domain -> excluded")
ok(is_excluded_inbox({"email": "y@capsulevideo.com", "tags": []}), "capsule domain -> excluded")
ok(is_excluded_inbox({"email": "z@gofloaters.com", "tags": []}), "gofloaters domain -> excluded")
# excluded by tag
ok(is_excluded_inbox({"email": "a@x.com", "tags": [{"tag_name": "monarch"}]}), "monarch tag -> excluded")
ok(is_excluded_inbox({"email": "a@x.com", "tags": [{"tag_name": "capsule"}]}), "capsule tag -> excluded")
# current clients NOT excluded
ok(not is_excluded_inbox({"email": "d@darleansuite.com", "tags": []}), "darlean -> kept")
ok(not is_excluded_inbox({"email": "m@mailmelior.com", "tags": [{"tag_name": "Melior"}]}), "melior -> kept")
ok(not is_excluded_inbox({"email": "b@gobettrdata.com", "tags": []}), "bettrdata -> kept")
ok(not is_excluded_inbox({"email": "w@wemythic.com", "tags": []}), "mythic -> kept")
# also accepts the health-row shape (client field)
ok(is_excluded_inbox({"email": "a@x.com", "client": "Avench"}), "client field -> excluded")
ok(not is_excluded_inbox({"email": "a@x.com", "client": "DARLEAN"}), "client field DARLEAN -> kept")

# paused clients (Q3 2026): OSC + StaffAI excluded
ok(is_excluded_inbox({"email": "mark@growwithosc.com", "tags": []}), "osc domain -> excluded")
ok(is_excluded_inbox({"email": "m@beginosc.com", "tags": []}), "beginosc domain -> excluded")
ok(is_excluded_inbox({"email": "x@usestaffai.com", "tags": []}), "staffai domain -> excluded")
ok(is_excluded_inbox({"email": "a@x.com", "tags": [{"tag_name": "StaffAI"}]}), "StaffAI tag -> excluded")
# local part must NOT trigger short keywords
ok(not is_excluded_inbox({"email": "oscar@darleansuite.com", "tags": []}),
   "local part 'oscar' does NOT match 'osc' (domain-only matching)")
print("\nALL PASSED")
