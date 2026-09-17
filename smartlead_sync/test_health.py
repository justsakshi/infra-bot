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
# A genuinely all-healthy snapshot needs a PROVEN-clean bounce rate. The bare
# snap() has no bounce history, which scores neutral (12/25) rather than full:
# "never sent" is not the same as "sends cleanly".
s = compute_health_score(snap(bounce_rate=0.2), TODAY)
ok(s["score"] == 100, f"all-healthy == 100 (got {s['score']})")
ok(s["grade"] == "A", "grade A")
ok(s["drivers"]["placement"] == 45, "full placement credit")

f = compute_health_score(snap(test_sheet_status="fail", busy_reason="failed_test"), TODAY)
ok(f["drivers"]["placement"] == 0, "failed test -> 0 placement")
# A failed placement test is now capped at 25 outright: the other components
# summed to 55 (grade C) for a mailbox measured landing 0/15 in spam.
ok(f["score"] == 25, f"failed test caps score at 25 (got {f['score']})")
ok(f["grade"] == "D", f"failed test grades D (got {f['grade']})")

w = compute_health_score(snap(warmup_rep_pct="90%"), TODAY)
ok(w["drivers"]["warmup"] == 0, "rep 90% -> 0 warmup credit")

wm = compute_health_score(snap(warmup_rep_pct="99%"), TODAY)
ok(wm["drivers"]["warmup"] == 10, "rep 99% -> full warmup credit")

d = compute_health_score(snap(connection_ok=False, busy_reason="disconnected"), TODAY)
ok(d["drivers"]["connection"] == 0, "disconnected -> 0 connection")

# stale test decays, dead test -> neutral half
st = compute_health_score(snap(test_sheet_status="inbox", test_date="2026-06-10"), TODAY)  # 22d old
ok(0 < st["drivers"]["placement"] < 45, f"stale test decays placement (got {st['drivers']['placement']})")
dead = compute_health_score(snap(test_sheet_status="", test_date=""), TODAY)
ok(dead["drivers"]["placement"] == 22, f"untested -> neutral 22/45 (got {dead['drivers']['placement']})")

# no bounce data -> full (don't penalize; bounce is campaign-level)
ok(compute_health_score(snap(), TODAY)["drivers"]["bounce"] == 12, "no bounce data -> neutral 12/25 (unproven is not clean)")

# --- action resolution ---
a = resolve_action(snap(test_sheet_status="fail", busy_reason="failed_test"), 60)
ok(a["priority"] == "P0", "failed -> P0")
ok(a["owner"] == "human", "failed -> human owner")
ok("SPF" in a["what_to_do"] or "retest" in a["what_to_do"].lower(), "failed action mentions fix")

a2 = resolve_action(snap(test_sheet_status="stale", busy_reason="stale_test"), 80)
ok(a2["priority"] == "P1" and a2["owner"] == "auto", "stale -> P1 auto")

a3 = resolve_action(snap(), 100)
ok(a3["priority"] == "" and a3["status"] == "healthy", "healthy -> no priority")

# --- R3 spam-flag toggle ---
import smartlead.health as _h
sp = snap(warmup_spam_count=5)  # over threshold 3
_h.HEALTH_SPAM_FLAG_ENABLED = False
ok(resolve_action(sp, 100)["priority"] == "", "spam flag OFF -> ignored")
_h.HEALTH_SPAM_FLAG_ENABLED = True
r = resolve_action(sp, 100)
ok(r["priority"] == "P0" and "spam" in r["top_problem"].lower(), "spam flag ON -> P0 spam")
ok(resolve_action(snap(warmup_spam_count=1), 100)["priority"] == "", "under threshold -> no flag")
_h.HEALTH_SPAM_FLAG_ENABLED = False  # restore

# --- 2026 volume-cap watch ---
v = resolve_action(snap(sent_today=60), 100)
ok(v["priority"] == "P2" and "volume" in v["top_problem"].lower(), "sent 60/day -> P2 volume flag")
ok(resolve_action(snap(sent_today=40), 100)["priority"] == "", "sent 40/day -> no volume flag")

# --- trend ---
t = compute_trend(70, 85)
ok(t["delta_7d"] == -15 and t["arrow"] == "↓" and t["declining"] is True, "declining trend")
ok(compute_trend(90, None)["arrow"] == "—", "no prior -> flat/unknown")
ok(compute_trend(90, 88)["arrow"] == "↑", "improving -> up arrow")

# --- build_health_rows integration (no Mongo, no manager file needed) ---
class _NoStore:
    def prior_score(self, *a, **k): return None
from smartlead.health import build_health_rows, health_records_for_store
inbox = [snap(email="a@d1.com", client="DARLEAN"),
         snap(email="a@d1.com", client="DARLEAN", campaign_name="C2"),  # dup -> merged
         snap(email="b@d2.com", client="DARLEAN", test_sheet_status="fail", busy_reason="failed_test")]
rows = build_health_rows(inbox, TODAY, _NoStore(), lambda c: {"name": "Dmitrii", "slack": "@d"})
ok(len(rows) == 2, f"deduped to 2 inboxes (got {len(rows)})")
ok(rows[0]["priority"] == "P0", "P0 sorts first")
ok(rows[0]["manager"] == "Dmitrii", "manager attached")
recs = health_records_for_store(rows, TODAY)
ok(recs[0]["date"] == "2026-07-02" and "score" in recs[0], "history record shape")

# --- DNS authentication checks ---
# 1. Scoring penalties
# connection is 20 points; penalties are SPF 8, DKIM 8, DMARC 5.
s_dns_spf = compute_health_score(snap(dns_spf_ok=False, bounce_rate=0.2), TODAY)
ok(s_dns_spf["drivers"]["connection"] == 12, f"failed SPF drops connection 20 -> 12 (got {s_dns_spf['drivers']['connection']})")
ok(s_dns_spf["score"] == 92, f"failed SPF drops overall score to 92 (got {s_dns_spf['score']})")

s_dns_dkim = compute_health_score(snap(dns_dkim_ok=False), TODAY)
ok(s_dns_dkim["drivers"]["connection"] == 12, "failed DKIM drops connection to 12")

s_dns_dmarc = compute_health_score(snap(dns_dmarc_ok=False), TODAY)
ok(s_dns_dmarc["drivers"]["connection"] == 15, f"failed DMARC drops connection to 15 (got {s_dns_dmarc['drivers']['connection']})")

s_all_dns_fail = compute_health_score(snap(dns_spf_ok=False, dns_dkim_ok=False, dns_dmarc_ok=False), TODAY)
ok(s_all_dns_fail["drivers"]["connection"] == 0, f"all DNS failures drop connection to 0 (20 - 8 - 8 - 5 < 0) (got {s_all_dns_fail['drivers']['connection']})")

# 2. Action resolution priorities
a_spf = resolve_action(snap(dns_spf_ok=False, dns_spf_msg="Unsafe SPF"), 95)
ok(a_spf["priority"] == "P0" and "SPF" in a_spf["top_problem"] and "Unsafe SPF" in a_spf["what_to_do"], "failed SPF is P0")

a_dkim = resolve_action(snap(dns_dkim_ok=False, dns_dkim_msg="Missing DKIM"), 95)
ok(a_dkim["priority"] == "P0" and "DKIM" in a_dkim["top_problem"], "failed DKIM is P0")

a_dmarc = resolve_action(snap(dns_dmarc_ok=False, dns_dmarc_msg="Missing DMARC"), 97)
ok(a_dmarc["priority"] == "P1" and "DMARC" in a_dmarc["top_problem"], "failed DMARC is P1")

print("\nALL PASSED (with build_health_rows and DNS tests)")
