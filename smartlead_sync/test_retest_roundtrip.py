"""INTEGRATION: full result round-trip WITHOUT spending a credit.

Simulates a completed SmartDelivery test through the REAL Pass A code path:
fake SmartDelivery client -> real Mongo (placement_tests/placement_results)
-> real deliverability-sheet 'API Tests' append -> placement_merge overlay
-> health scoring flips the inbox. Cleans up everything it writes.

Run: python3 test_retest_roundtrip.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from smartlead.placement_store import PlacementStore
from smartlead.placement_merge import apply_api_results
from smartlead.health import compute_health_score, resolve_action
from retest_executor import _poll_pending

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TEST_ID = 99999901                      # far outside real SmartDelivery id space
TEST_CLIENT = "__ROUNDTRIP_TEST__"
TEST_EMAILS = ["probe@roundtrip-check-domain.com"]
TEST_DOMAIN = "roundtrip-check-domain.com"


class FakeSD:
    """Stands in for SmartDeliveryClient: test is DONE, 91% inbox."""
    def __init__(self, api_key): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    async def poll_test(self, tid):
        return {"status": "COMPLETED", "done": True, "end_date": "2026-07-07T10:00:00Z"}
    async def get_report(self, tid):
        return {"inbox_pct": 91.0, "spam_pct": 9.0}


def cleanup(store: PlacementStore):
    if store._tests is not None:
        store._tests.delete_many({"client": TEST_CLIENT})
    if store._results is not None:
        store._results.delete_many({"email": TEST_EMAILS[0]})
    # remove sheet probe rows
    try:
        from smartlead.sheets import _authorize
        from smartlead.config import TEST_SHEET_ID
        ws = _authorize().open_by_key(TEST_SHEET_ID).worksheet("API Tests")
        vals = ws.get_all_values()
        for i in range(len(vals), 1, -1):
            if len(vals[i - 1]) > 2 and vals[i - 1][1] == TEST_CLIENT:
                ws.delete_rows(i)
    except Exception as exc:  # noqa: BLE001
        print(f"  (sheet cleanup note: {exc})")


async def main():
    store = PlacementStore()
    ok(store.available, "Mongo available")
    cleanup(store)  # fresh start

    # 1. record a fake ACTIVE test (as Pass B would)
    store.record_created(TEST_ID, TEST_CLIENT, 123, TEST_EMAILS)
    ok(any(t["test_id"] == TEST_ID for t in store.pending_tests()), "test recorded ACTIVE")

    # 2. run the REAL Pass A with the fake SmartDelivery client
    import retest_executor
    real = retest_executor.SmartDeliveryClient
    retest_executor.SmartDeliveryClient = FakeSD
    try:
        completed = await _poll_pending(store, {TEST_CLIENT: "fake-key"})
    finally:
        retest_executor.SmartDeliveryClient = real
    ok(completed >= 1, f"Pass A completed the test (got {completed})")

    # 3. result written to placement_results with source=api
    row = store._results.find_one({"email": TEST_EMAILS[0], "source": "api"})
    ok(row is not None and row["status"] == "inbox", f"result saved: {row and row['status']} (91% -> inbox)")

    # 4. sheet append happened (API Tests tab has our row)
    from smartlead.sheets import _authorize
    from smartlead.config import TEST_SHEET_ID
    vals = _authorize().open_by_key(TEST_SHEET_ID).worksheet("API Tests").get_all_values()
    ok(any(len(v) > 2 and v[1] == TEST_CLIENT and v[2] == TEST_DOMAIN for v in vals),
       "API Tests sheet row appended")

    # 5. merge overlays the API result onto the deliverability map (newer wins)
    manual_map = {TEST_DOMAIN: {"status": "fail", "date": "2026-06-01"}}  # old manual fail
    merged = apply_api_results(manual_map)
    ok(merged[TEST_DOMAIN]["status"] == "inbox", "newer API 'inbox' overrides old manual 'fail'")

    # 6. health scoring flips: the inbox is now test=inbox+fresh -> full placement pts
    snap = {"email": TEST_EMAILS[0], "client": TEST_CLIENT, "warmup_rep_pct": "100%",
            "connection_ok": True, "test_sheet_status": merged[TEST_DOMAIN]["status"],
            "test_date": merged[TEST_DOMAIN]["date"], "busy_reason": "", "max_per_day": 20}
    hs = compute_health_score(snap, date.today())
    act = resolve_action(snap, hs["score"])
    ok(hs["drivers"]["placement"] == 40, f"placement 40/40 after result (got {hs['drivers']['placement']})")
    ok(act["status"] == "healthy", f"action resolves healthy (got {act['status']})")

    cleanup(store)
    print("\nALL PASSED - full round-trip verified without spending a credit")

asyncio.run(main())
