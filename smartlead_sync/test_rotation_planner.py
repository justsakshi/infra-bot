from smartlead.rotation_planner import select_swaps

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

def row(email, name, client="BW", camp="C1", camp_status="ACTIVE", test="inbox",
        rep="95%", conn=True, cap=10, avail="FREE", busy="", acct="1", wstate="warming"):
    return {"email": email, "name": name, "client": client, "campaign_name": camp,
            "campaign_status": camp_status, "test_sheet_status": test,
            "warmup_rep_pct": rep, "connection_ok": conn, "capacity_left": cap,
            "availability": avail, "busy_reason": busy, "account_id": acct,
            "warmup_state": wstate}

rows = [
    # victim: failed test, in ACTIVE campaign
    row("sam@bad.com", "Sam Lonsdale", test="fail", busy="failed_test", acct="10"),
    # bench: persona match (Sam), healthy, FREE, idle
    row("sam@good.com", "Sam Lonsdale", camp="", camp_status="", rep="97%", acct="20"),
    # bench: different persona, higher rep
    row("amy@good.com", "Amy Cole", camp="", camp_status="", rep="99%", acct="21"),
]
s = select_swaps(rows, per_client_cap=2)
ok(len(s) == 1, f"one swap (got {len(s)})")
ok(s[0]["victim_email"] == "sam@bad.com", "victim identified")
ok(s[0]["replacement_email"] == "sam@good.com", "persona match preferred over higher rep")
ok(s[0]["inflight_policy"] == "reassign", "persona match -> reassign in-flight")

# no persona match -> pause policy
rows2 = [row("sam@bad.com", "Sam L", test="fail", busy="failed_test"),
         row("amy@good.com", "Amy Cole", camp="", camp_status="", acct="21")]
s2 = select_swaps(rows2, 2)
ok(s2[0]["replacement_email"] == "amy@good.com" and s2[0]["inflight_policy"] == "pause",
   "no persona match -> swap still happens, in-flight paused")

# no bench at all -> alert
s3 = select_swaps([row("sam@bad.com", "Sam", test="fail", busy="failed_test")], 2)
ok(s3[0]["inflight_policy"] == "alert" and s3[0]["replacement_email"] is None,
   "no bench -> alert, no swap")

# same-client hard rule: bench from another client never used
rows4 = [row("sam@bad.com", "Sam", client="BW", test="fail", busy="failed_test"),
         row("sam@other.com", "Sam", client="DARLEAN", camp="", camp_status="")]
ok(select_swaps(rows4, 2)[0]["replacement_email"] is None, "cross-client bench rejected")

# cap respected + bench used once
rows5 = [row("a@bad.com", "Sam", test="fail", busy="failed_test", camp="C1"),
         row("b@bad.com", "Sam", test="fail", busy="failed_test", camp="C2"),
         row("c@bad.com", "Sam", test="fail", busy="failed_test", camp="C3"),
         row("x@good.com", "Sam", camp="", camp_status="", acct="30"),
         row("y@good.com", "Sam", camp="", camp_status="", acct="31")]
s5 = [x for x in select_swaps(rows5, per_client_cap=2) if x["replacement_email"]]
ok(len(s5) == 2, f"per-client cap 2 (got {len(s5)})")
ok(s5[0]["replacement_email"] != s5[1]["replacement_email"], "bench inbox used once per run")

# already-swapped dedupe
s6 = select_swaps(rows, 2, already_swapped={("sam@bad.com", "C1")})
ok(s6 == [], "already-swapped pair skipped")

# broken bench never selected (failed inbox can't be a replacement)
rows7 = [row("sam@bad.com", "Sam", test="fail", busy="failed_test"),
         row("z@alsobad.com", "Sam", test="fail", camp="", camp_status="")]
ok(select_swaps(rows7, 2)[0]["replacement_email"] is None, "failed inbox never used as bench")

print("\nALL PASSED")
