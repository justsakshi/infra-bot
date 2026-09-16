"""Every tab write reports its own outcome on the Last Sync tab.

The old Last Sync tab held one timestamp, written at the end of the run
whether or not anything before it had succeeded. "BETTRDATA - Campaign
Summary" went weeks without a successful write while that timestamp advanced
every morning. These tests pin the behaviour that replaces it: a success
records the row count, a failure records the error and still propagates.
"""
from smartlead import sheets
from smartlead.sheets import SheetsWriter

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def writer(prefix=""):
    # Bypass __init__: no Google auth in a unit test.
    w = object.__new__(SheetsWriter)
    w.prefix = prefix
    w.sheet_id = "test"
    return w


# --- successful write is recorded with its row count and prefixed tab name ---
sheets._SYNC_LEDGER.clear()
w = writer("BETTRDATA - ")
w._write_tab_unguarded = lambda key, data, headers=None: None
w._write_tab("Campaign Summary", [{"a": 1}, {"a": 2}, {"a": 3}])
ok(len(sheets._SYNC_LEDGER) == 1, "one ledger entry per write")
e = sheets._SYNC_LEDGER[0]
ok(e["tab"] == "BETTRDATA - Campaign Summary", f"records the full prefixed tab name, got {e['tab']!r}")
ok(e["status"] == "ok" and e["rows"] == 3, f"success carries row count, got {e}")
ok(e["error"] == "", "no error text on success")

# --- a failed write is recorded AND still raises, so callers' handling runs ---
sheets._SYNC_LEDGER.clear()
w = writer("BETTRDATA - ")
def boom(key, data, headers=None):
    raise RuntimeError("APIError: [429]: Quota exceeded for quota metric 'Write requests'")
w._write_tab_unguarded = boom
raised = False
try:
    w._write_tab("Inboxes", [{"a": 1}])
except RuntimeError:
    raised = True
ok(raised, "the failure still propagates to the caller")
e = sheets._SYNC_LEDGER[0]
ok(e["status"] == "FAILED", "failure recorded")
ok("429" in e["error"] and "RuntimeError" in e["error"],
   f"error text names the exception and its message, got {e['error']!r}")
ok(e["rows"] == "", "no row count on a failed write")

# --- a very long error is truncated so it cannot blow out the sheet cell ---
sheets._SYNC_LEDGER.clear()
sheets._record_write("X", error=ValueError("y" * 5000))
ok(len(sheets._SYNC_LEDGER[0]["error"]) <= 300, "error text capped at 300 chars")

# --- ledger rows are shaped for the sheet: tab, at, rows, status, error ---
sheets._SYNC_LEDGER.clear()
sheets._record_write("All Inboxes", rows=523)
sheets._record_write("Capacity", error=KeyError("client"))
rows = sheets.sync_ledger_rows()
ok(len(rows) == 2, "one sheet row per entry")
ok(rows[0][0] == "All Inboxes" and rows[0][2] == "523" and rows[0][3] == "ok",
   f"success row shape, got {rows[0]}")
ok(rows[1][0] == "Capacity" and rows[1][3] == "FAILED" and "KeyError" in rows[1][4],
   f"failure row shape, got {rows[1]}")
ok(all(len(r) == 5 for r in rows), "every row has five cells")

# --- shared (unprefixed) tabs record their plain name ---
sheets._SYNC_LEDGER.clear()
w = writer("")
w._write_tab_unguarded = lambda key, data, headers=None: None
w._write_tab("Campaign Metrics", [])
ok(sheets._SYNC_LEDGER[0]["tab"] == "Campaign Metrics", "no prefix -> plain tab name")
ok(sheets._SYNC_LEDGER[0]["rows"] == 0, "an empty-but-successful write records 0 rows")

# --- the run verdict splits the ledger into written and failed ---
sheets._SYNC_LEDGER.clear()
sheets._record_write("A - Campaign Summary", rows=10)
sheets._record_write("A - Inboxes", error=RuntimeError("429"))
sheets._record_write("Campaign Metrics", rows=31)
sheets._record_write("B - (account sync)", error=RuntimeError("401 bad key"))
ok_tabs, failed = sheets.sync_ledger_summary()
ok([e["tab"] for e in ok_tabs] == ["A - Campaign Summary", "Campaign Metrics"],
   f"ok list, got {[e['tab'] for e in ok_tabs]}")
ok([e["tab"] for e in failed] == ["A - Inboxes", "B - (account sync)"],
   f"failed list keeps order, got {[e['tab'] for e in failed]}")
sheets._SYNC_LEDGER.clear()
ok(sheets.sync_ledger_summary() == ([], []), "empty ledger -> nothing ok, nothing failed")

# --- write_all attempts every tab even after one fails, then re-raises ---
sheets._SYNC_LEDGER.clear()
w = writer("A - ")
attempted = []
def _tab(key):
    def _f(data, headers=None):
        attempted.append(key)
        if key == "Campaign Summary":
            raise RuntimeError("429 on the first tab")
    return _f
w._write_tab_unguarded = lambda key, data, headers=None: _tab(key)(data, headers)
raised = False
try:
    w.write_all([{"x": 1}], [{"x": 1}], [{"x": 1}])
except RuntimeError:
    raised = True
ok(raised, "write_all still raises after attempting everything")
ok(attempted == ["Campaign Summary", "Inboxes", "Warmup Reputation"],
   f"all three tabs attempted despite the first failing, got {attempted}")
statuses = {e["tab"]: e["status"] for e in sheets._SYNC_LEDGER}
ok(statuses == {"A - Campaign Summary": "FAILED", "A - Inboxes": "ok",
                "A - Warmup Reputation": "ok"},
   f"ledger records the one failure and the two successes, got {statuses}")

sheets._SYNC_LEDGER.clear()
print("\nALL PASSED")
