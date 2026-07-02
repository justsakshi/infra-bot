from smartlead.warmup_planner import plan_warmup_changes

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

def row(email, warmup_state, campaign_status="", campaign_is_stale=False,
        account_id="1", client="DARLEAN"):
    return {"email": email, "warmup_state": warmup_state,
            "campaign_status": campaign_status, "campaign_is_stale": campaign_is_stale,
            "account_id": account_id, "client": client}

# In a LIVE ACTIVE campaign (not stale), warmup on -> disable
r1 = row("a@x.com", "warming", campaign_status="ACTIVE", campaign_is_stale=False)
# Idle inbox with warmup off -> enable
r2 = row("b@x.com", "off", campaign_status="")
# Idle inbox already warming -> no change
r3 = row("c@x.com", "ramped", campaign_status="")
# PAUSED campaign, warmup off -> enable
r4 = row("d@x.com", "off", campaign_status="PAUSED")
# Blocked warmup -> never auto-touch
r5 = row("e@x.com", "blocked")
# SAFETY: ACTIVE campaign, 0 sends today but NOT stale, warmup off -> must NOT enable
r6 = row("f@x.com", "off", campaign_status="ACTIVE", campaign_is_stale=False)
# STALE ACTIVE campaign (dead 14d+), warmup off -> ENABLE (rescue)
r7 = row("g@x.com", "off", campaign_status="ACTIVE", campaign_is_stale=True)

changes = plan_warmup_changes([r1, r2, r3, r4, r5, r6, r7])
by_email = {c["email"]: c for c in changes}

ok(by_email["a@x.com"]["action"] == "disable", "live active sender -> disable warmup")
ok(by_email["b@x.com"]["action"] == "enable", "idle off -> enable warmup")
ok("c@x.com" not in by_email, "already warming idle -> no change")
ok(by_email["d@x.com"]["action"] == "enable", "paused campaign idle -> enable")
ok("e@x.com" not in by_email, "blocked -> not auto-touched")
ok("f@x.com" not in by_email, "SAFETY: active non-stale campaign, 0 sends -> NOT enabled")
ok(by_email["g@x.com"]["action"] == "enable", "STALE active campaign -> enable (rescue)")
ok("STALE" in by_email["g@x.com"]["reason"], "stale reason flagged")

ok(plan_warmup_changes([]) == [], "empty -> no changes")

# --- R2 trickle toggle ---
import smartlead.warmup_planner as wp
wp.WARMUP_MAINTENANCE_TRICKLE = True  # simulate toggle ON
tr = {c["email"]: c for c in plan_warmup_changes([r1])}  # r1 = live active sender, warmup on
ok(tr["a@x.com"]["action"] == "trickle", "trickle ON: live sender -> trickle (not disable)")
wp.WARMUP_MAINTENANCE_TRICKLE = False  # restore
tr2 = {c["email"]: c for c in plan_warmup_changes([r1])}
ok(tr2["a@x.com"]["action"] == "disable", "trickle OFF: live sender -> disable (legacy)")

print("\nALL PASSED")
