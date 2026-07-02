from smartlead.warmup_planner import plan_warmup_changes

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

def row(email, warmup_state, availability="FREE", busy_reason="", sent_today=0,
        campaign_status="", account_id="1", client="DARLEAN"):
    return {"email": email, "warmup_state": warmup_state, "availability": availability,
            "busy_reason": busy_reason, "sent_today": sent_today,
            "campaign_status": campaign_status, "account_id": account_id, "client": client}

# Actively sending in a live campaign -> warmup should be OFF
r1 = row("a@x.com", "warming", availability="BUSY", sent_today=12, campaign_status="ACTIVE")
# Idle inbox with warmup off -> should be ON
r2 = row("b@x.com", "off", availability="FREE", sent_today=0)
# Idle inbox already warming -> no change
r3 = row("c@x.com", "ramped", availability="FREE", sent_today=0)
# In a PAUSED campaign, not sending -> should be ON
r4 = row("d@x.com", "off", availability="BUSY", busy_reason="no_capacity", sent_today=0, campaign_status="PAUSED")
# Blocked warmup -> never auto-touch (human)
r5 = row("e@x.com", "blocked", sent_today=0)

changes = plan_warmup_changes([r1, r2, r3, r4, r5])
by_email = {c["email"]: c for c in changes}

ok(by_email["a@x.com"]["action"] == "disable", "active sender -> disable warmup")
ok(by_email["b@x.com"]["action"] == "enable", "idle off -> enable warmup")
ok("c@x.com" not in by_email, "already warming idle -> no change")
ok(by_email["d@x.com"]["action"] == "enable", "paused campaign idle -> enable")
ok("e@x.com" not in by_email, "blocked -> not auto-touched")

# already-off active sender needs no disable
r6 = row("f@x.com", "off", availability="BUSY", sent_today=30, campaign_status="ACTIVE")
c6 = {c["email"]: c for c in plan_warmup_changes([r6])}
ok("f@x.com" not in c6, "active sender already off -> no change")

ok(plan_warmup_changes([]) == [], "empty -> no changes")
print("\nALL PASSED")
