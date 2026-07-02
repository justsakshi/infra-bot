"""Unit tests for health scoring, action resolution, trend."""
from datetime import date
from smartlead.health import compute_health_score, resolve_action, compute_trend

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TODAY = date(2026, 7, 2)

def snap(**kw):
    base = dict(email="a@x.com", client="DARLEAN", provider="Gmail",
                warmup_rep_pct="100%", warmup_state="active", connection_ok=True,
                test_sheet_status="inbox", test_date="2026-07-01", busy_reason="",
                campaign_name="C1", max_per_day=30, sent_today=5, true_load=10)
    base.update(kw); return base

# --- scoring ---
s = compute_health_score(snap(), TODAY)
ok(s["score"] == 100, f"all-healthy == 100 (got {s['score']})")
ok(s["grade"] == "A", "grade A")
ok(s["drivers"]["placement"] == 40, "full placement credit")

f = compute_health_score(snap(test_sheet_status="fail", busy_reason="failed_test"), TODAY)
ok(f["drivers"]["placement"] == 0, "failed test -> 0 placement")
ok(f["score"] == 60, f"fail test drops 40 -> 60 (got {f['score']})")

w = compute_health_score(snap(warmup_rep_pct="90%"), TODAY)
ok(w["drivers"]["warmup"] == 0, "rep 90% -> 0 warmup credit")

wm = compute_health_score(snap(warmup_rep_pct="99%"), TODAY)
ok(wm["drivers"]["warmup"] == 25, "rep 99% -> full warmup credit")

d = compute_health_score(snap(connection_ok=False, busy_reason="disconnected"), TODAY)
ok(d["drivers"]["connection"] == 0, "disconnected -> 0 connection")

# stale test decays, dead test -> neutral half
st = compute_health_score(snap(test_sheet_status="inbox", test_date="2026-06-10"), TODAY)  # 22d old
ok(0 < st["drivers"]["placement"] < 40, f"stale test decays placement (got {st['drivers']['placement']})")
dead = compute_health_score(snap(test_sheet_status="", test_date=""), TODAY)
ok(dead["drivers"]["placement"] == 20, f"untested -> neutral 20/40 (got {dead['drivers']['placement']})")

# no bounce data -> full (don't penalize; bounce is campaign-level)
ok(compute_health_score(snap(), TODAY)["drivers"]["bounce"] == 20, "no bounce data -> full 20 (not penalized)")

# --- action resolution ---
a = resolve_action(snap(test_sheet_status="fail", busy_reason="failed_test"), 60)
ok(a["priority"] == "P0", "failed -> P0")
ok(a["owner"] == "human", "failed -> human owner")
ok("SPF" in a["what_to_do"] or "retest" in a["what_to_do"].lower(), "failed action mentions fix")

a2 = resolve_action(snap(test_sheet_status="stale", busy_reason="stale_test"), 80)
ok(a2["priority"] == "P1" and a2["owner"] == "auto", "stale -> P1 auto")

a3 = resolve_action(snap(), 100)
ok(a3["priority"] == "" and a3["status"] == "healthy", "healthy -> no priority")

# --- trend ---
t = compute_trend(70, 85)
ok(t["delta_7d"] == -15 and t["arrow"] == "↓" and t["declining"] is True, "declining trend")
ok(compute_trend(90, None)["arrow"] == "—", "no prior -> flat/unknown")
ok(compute_trend(90, 88)["arrow"] == "↑", "improving -> up arrow")

print("\nALL PASSED")
