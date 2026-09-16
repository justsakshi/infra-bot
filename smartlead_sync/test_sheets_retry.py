"""Sheet writes retry on quota errors instead of silently losing a tab.

The daily run.py sync writes ~15 tabs in a burst before the Campaign Metrics
tabs, each tab being three API calls. Google's Sheets write quota is per
minute, so the last writers in the burst are the ones that get a 429 - and on
2026-09-16 the per-client "Campaign Metrics — BETTRDATA" tab was left
holding the previous day's numbers while the shared tab, written seconds
earlier, was current.

Worse, `_write_tab` clears the tab before writing it. A 429 on the update
leaves the tab EMPTY, which reads as "this client had no campaigns" - a
different and more misleading failure than stale data.
"""
from smartlead import sheets
from gspread.exceptions import APIError

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


class _Resp:
    """Minimal stand-in for requests.Response, shaped as gspread reads it."""
    def __init__(self, code, status="RESOURCE_EXHAUSTED", message="Quota exceeded"):
        self._body = {"error": {"code": code, "status": status, "message": message}}
        self.text = message
        self.status_code = code
    def json(self):
        return self._body


def quota_error():
    return APIError(_Resp(429))


def not_found_error():
    return APIError(_Resp(404, status="NOT_FOUND", message="Requested entity was not found"))


# Patch the sleeper so tests run instantly and we can see the backoff schedule.
slept = []
sheets._sleep = lambda s: slept.append(s)


# --- retries a 429 and eventually succeeds ---
calls = {"n": 0}
def flaky():
    calls["n"] += 1
    if calls["n"] < 3:
        raise quota_error()
    return "written"

slept.clear()
ok(sheets._retry_write(flaky) == "written", "succeeds after two quota errors")
ok(calls["n"] == 3, f"called three times, got {calls['n']}")
ok(len(slept) == 2, f"slept between attempts, got {slept}")
ok(slept[0] < slept[1], f"backoff grows, got {slept}")
# Google's write quota is per minute, so the schedule has to reach past 60s
# in total or the retry just hits the same closed window again.
ok(sum(sheets._RETRY_SCHEDULE) >= 60,
   f"total backoff reaches past the quota window, got {sum(sheets._RETRY_SCHEDULE)}s")

# --- gives up after the schedule is exhausted, re-raising the real error ---
def always_429():
    raise quota_error()

slept.clear()
raised = None
try:
    sheets._retry_write(always_429)
except APIError as exc:
    raised = exc
ok(raised is not None and raised.code == 429, "exhausted retries re-raise the 429")
ok(len(slept) == len(sheets._RETRY_SCHEDULE),
   f"slept once per scheduled retry, got {len(slept)}")

# --- a non-quota API error is NOT retried: that would just repeat a real fault ---
calls["n"] = 0
def missing():
    calls["n"] += 1
    raise not_found_error()

slept.clear()
raised = None
try:
    sheets._retry_write(missing)
except APIError as exc:
    raised = exc
ok(raised is not None and raised.code == 404, "404 propagates")
ok(calls["n"] == 1, f"404 is not retried, got {calls['n']} calls")
ok(slept == [], "no sleeping on a non-retryable error")

# --- arguments pass through ---
ok(sheets._retry_write(lambda a, b=0: a + b, 2, b=3) == 5, "args and kwargs forwarded")

# --- 503 (backend unavailable) is treated like 429: transient, worth a retry ---
calls["n"] = 0
def flaky_503():
    calls["n"] += 1
    if calls["n"] == 1:
        raise APIError(_Resp(503, status="UNAVAILABLE", message="The service is currently unavailable"))
    return "ok"
slept.clear()
ok(sheets._retry_write(flaky_503) == "ok", "503 is retried once and succeeds")

print("\nALL PASSED")
