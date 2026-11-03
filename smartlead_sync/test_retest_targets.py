from smartlead.retest_targets import select_targets

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

def row(client, email, problem, test_date="", camp="C1"):
    return {"client": client, "email": email, "top_problem": problem,
            "owner": "🤖 Auto", "test_date": test_date, "campaign_name": camp}

rows = [
    row("DARLEAN", "a@d.com", "No placement test on record"),                 # untested
    row("DARLEAN", "b@d.com", "Placement test is stale", "2026-06-01"),       # stale old
    row("DARLEAN", "c@d.com", "Placement test is stale", "2026-06-20"),       # stale newer
    row("DARLEAN", "d@d.com", "Failed placement test"),                       # P0 human -> NOT auto
    row("MELIOR",  "e@m.com", "No placement test on record"),                 # other client
]
t = select_targets(rows, per_client_cap=2, pending_emails=set())
darlean = [x for x in t if x["client"] == "DARLEAN"]
ok(len(darlean) == 2, f"per-client cap 2 (got {len(darlean)})")
ok(darlean[0]["email"] == "a@d.com", "untested first")
ok(darlean[1]["email"] == "b@d.com", "then oldest stale")
ok(any(x["client"] == "MELIOR" for x in t), "other client included")
ok(all(x["email"] != "d@d.com" for x in t), "failed(human) excluded")

t2 = select_targets(rows, per_client_cap=2, pending_emails={"a@d.com"})
ok(all(x["email"] != "a@d.com" for x in t2), "pending excluded")
ok([x for x in t2 if x["client"] == "DARLEAN"][0]["email"] == "b@d.com", "skips pending, next worst")

ok(select_targets([], 2, set()) == [], "empty -> no targets")

# warm-up spam trigger (3 Nov 2026): tested first, same cap, same 7-day floor
from smartlead.health import WARMUP_SPAM_PROBLEM
from datetime import date, timedelta
recent = (date.today() - timedelta(days=3)).isoformat()
rows3 = [
    row("PL", "u@p.com", "No placement test on record"),
    row("PL", "w@p.com", WARMUP_SPAM_PROBLEM, "2026-01-01"),
    row("PL", "fresh@p.com", WARMUP_SPAM_PROBLEM, recent),
]
t3 = select_targets(rows3, per_client_cap=1, pending_emails=set())
ok([x["email"] for x in t3] == ["w@p.com"], f"warm-up spam goes before untested (got {[x['email'] for x in t3]})")
ok(t3[0]["reason"] == WARMUP_SPAM_PROBLEM, "reason carried")
t4 = select_targets(rows3, per_client_cap=5, pending_emails=set())
ok("fresh@p.com" not in [x["email"] for x in t4], "tested 3 days ago -> 7-day floor holds")

print("\nALL PASSED")
