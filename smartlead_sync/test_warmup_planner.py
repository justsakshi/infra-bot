import smartlead.warmup_planner as wp
from smartlead.warmup_planner import plan_warmup_changes

# The r1-r7 assertions below test the LEGACY on/off/trickle logic, so force
# always-on OFF here (it's ON by default per Avi's policy).
wp.WARMUP_ALWAYS_ON = False

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

# --- WARMUP_ALWAYS_ON policy (Avi): only enable/boost, never disable/trickle ---
wp.WARMUP_ALWAYS_ON = True
ao = {c["email"]: c for c in plan_warmup_changes([r1, r2])}  # r1=live sender ON, r2=idle OFF
ok("a@x.com" not in ao, "always-on: live sender ON -> no change (never disabled)")
ok(ao["b@x.com"]["action"] == "enable", "always-on: idle OFF -> enable")
# a live sender that is OFF gets ENABLED (never leave warmup off)
r_off = row("h@x.com", "off", campaign_status="ACTIVE", campaign_is_stale=False)
ok({c["email"]: c for c in plan_warmup_changes([r_off])}["h@x.com"]["action"] == "enable",
   "always-on: live sender OFF -> enable (warmup never paused)")
# low-rep inbox already ON -> BOOST (increase warmup, never cut)
r_low = row("i@x.com", "warming"); r_low["warmup_rep_pct"] = "85%"
ok({c["email"]: c for c in plan_warmup_changes([r_low])}["i@x.com"]["action"] == "boost",
   "always-on: low rep (85%) -> boost warmup")
# healthy rep ON -> no change
r_hi = row("j@x.com", "warming"); r_hi["warmup_rep_pct"] = "98%"
ok("j@x.com" not in {c["email"]: c for c in plan_warmup_changes([r_hi])},
   "always-on: healthy rep ON -> no change")
wp.WARMUP_ALWAYS_ON = False  # restore for legacy tests below

# --- R2 trickle toggle (legacy mode, always-on off) ---
wp.WARMUP_MAINTENANCE_TRICKLE = True
tr = {c["email"]: c for c in plan_warmup_changes([r1])}
ok(tr["a@x.com"]["action"] == "trickle", "trickle ON: live sender -> trickle (not disable)")
wp.WARMUP_MAINTENANCE_TRICKLE = False
tr2 = {c["email"]: c for c in plan_warmup_changes([r1])}
ok(tr2["a@x.com"]["action"] == "disable", "trickle OFF: live sender -> disable (legacy)")

print("\nALL PASSED")
