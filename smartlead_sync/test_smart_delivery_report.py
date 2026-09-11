"""Report parsing, specifically the difference between "landed in spam" and
"no seed was classified at all".

On 2026-09-11 a batch of 15 concurrent tests was flipped to COMPLETED by
Smartlead while the seed classifications were still arriving. Reports came back
with an empty or partial `result` array against overallTotalCount=15. The old
parser scored those as 0% inbox, which reads as a total placement failure and
would have written `spam` into the team's grid for six healthy domains.
"""
from smartlead.smart_delivery import summarize_report

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

FULL = {"overallTotalCount": 15, "status": "COMPLETED", "result": [
    {"provider_name": "Office365", "inbox_count": 7, "spam_count": 0, "adjusted_total_email_count": 7},
    {"provider_name": "G Suite", "inbox_count": 8, "spam_count": 0, "adjusted_total_email_count": 8},
]}
r = summarize_report(FULL)
ok(r["classified"] == 15, "counts classified seeds")
ok(r["dispatched"] == 15, "reads overallTotalCount")
ok(r["has_data"], "a full panel has data")
ok(r["coverage"] == 1.0, "full coverage")
ok(r["worst_provider_inbox_pct"] == 100.0, "worst provider computed")

EMPTY = {"overallTotalCount": 15, "status": "COMPLETED", "result": []}
r = summarize_report(EMPTY)
ok(not r["has_data"], "empty result -> no data, NOT a 0% failure")
ok(r["classified"] == 0, "zero classified")
ok(r["coverage"] == 0.0, "zero coverage")

PARTIAL = {"overallTotalCount": 15, "status": "COMPLETED", "result": [
    {"provider_name": "Office365", "inbox_count": 3, "spam_count": 0, "adjusted_total_email_count": 3},
]}
r = summarize_report(PARTIAL)
ok(r["has_data"], "partial data is still data")
ok(r["classified"] == 3, "3 of 15 classified")
ok(abs(r["coverage"] - 0.2) < 1e-9, "coverage is 3/15")
ok(r["worst_provider_inbox_pct"] == 100.0, "the seeds that landed all inboxed")

# A real failure: seeds classified, and they went to spam.
REAL_FAIL = {"overallTotalCount": 15, "status": "COMPLETED", "result": [
    {"provider_name": "Office365", "inbox_count": 0, "spam_count": 7, "adjusted_total_email_count": 7},
    {"provider_name": "G Suite", "inbox_count": 8, "spam_count": 0, "adjusted_total_email_count": 8},
]}
r = summarize_report(REAL_FAIL)
ok(r["has_data"], "a real failure has data")
ok(r["worst_provider_inbox_pct"] == 0.0, "Office365 0% is the worst provider")
ok(r["by_provider"]["G Suite"]["inbox_pct"] == 100.0, "G Suite unaffected")

# Missing overallTotalCount must not make coverage look complete.
r = summarize_report({"result": [
    {"provider_name": "Office365", "inbox_count": 2, "spam_count": 0, "adjusted_total_email_count": 2}]})
ok(r["dispatched"] == 2, "absent overallTotalCount falls back to classified")
ok(r["has_data"], "still has data")

print("\nALL PASSED")
