"""Volume review: approved limits in the ledger, drift against them, Apply (spec 2026-10-15). No network."""
import asyncio
from datetime import date

from smartlead import inbox_intake as ii
from smartlead import volume_rule as vr
from test_inbox_intake import FakeCol, FakeSL, acct, run

TODAY = date(2026, 11, 3)
DONE = {"tags": [(9, "Bot-Setup"), (153335, "ScaledMail-Google")]}


def setup_box(i, email, limit=15):
    a = acct(i, email, **DONE)
    a.update({"message_per_day": limit, "minTimeToWaitInMins": 20})
    a["warmup_details"].update({"max_email_per_day": 25, "reply_rate": 30})
    return a


def test_an_approved_raise_is_not_drift_but_a_hand_change_is():
    store = ii.IntakeStore(FakeCol())
    store.approve_limit("a1@d1.com", 17, by="U1", action=vr.RAISE, on=TODAY)
    store.approve_limit("a2@d1.com", 17, by="U1", action=vr.RAISE, on=TODAY)
    out, _ = run(FakeSL([setup_box(1, "a1@d1.com", 17), setup_box(2, "a2@d1.com", 18)]), store=store, plans=())
    assert out["problems"] == ["Changed since setup on 1 inbox (a2@d1.com): daily limit 18, want 17"]
    assert store.approved_limits() == {"a1@d1.com": (17, "2026-11-03"), "a2@d1.com": (17, "2026-11-03")}


ROWS = [{"client": "Precise Leads", "account": "PRECISE_LEADS", "id": 1, "email": "a1@d1.com", "kind": "google",
         "action": vr.RAISE, "from": 15, "to": 17, "why": ""},
        {"client": "Precise Leads", "account": "PRECISE_LEADS", "id": 2, "email": "a2@d1.com", "kind": "google",
         "action": vr.HOLD, "from": 15, "to": 15, "why": "x"},
        {"client": "Melior", "account": "PRECISE_LEADS", "id": 3, "email": "a3@d2.com", "kind": "google",
         "action": vr.STEP_BACK, "from": 17, "to": 15, "why": "y"}]


def apply(sl, store, client="Precise Leads", today=TODAY):
    return asyncio.run(vr.apply_review({"PRECISE_LEADS": sl}, store, "2026-11-03", client, by="U1", today=today))


def fresh_store():
    store = ii.IntakeStore(FakeCol())
    store.save_review("2026-11-03", ROWS)
    return store


def test_apply_writes_only_that_clients_changes_reads_back_and_records():
    sl, store = FakeSL([setup_box(1, "a1@d1.com"), setup_box(2, "a2@d1.com"), setup_box(3, "a3@d2.com", 17)]), fresh_store()
    r = apply(sl, store)
    assert sl.writes == [(1, {"max_email_per_day": 17})]
    assert r["changed"] == ["a1@d1.com 15 -> 17"] and not r["failed"] and not r["skipped"]
    assert store.approved_limits()["a1@d1.com"] == (17, "2026-11-03")
    assert "a3@d2.com" not in store.approved_limits()


def test_a_second_apply_and_a_stale_review_are_refused():
    sl, store = FakeSL([setup_box(1, "a1@d1.com")]), fresh_store()
    apply(sl, store)
    assert "already applied" in apply(sl, store)["error"]
    assert "older than 7 days" in apply(sl, fresh_store(), today=date(2026, 11, 11))["error"]
    assert "no volume review" in asyncio.run(vr.apply_review({}, ii.IntakeStore(FakeCol()), "2026-11-03",
                                                           "Melior", by="U1", today=TODAY))["error"].lower()


def test_a_limit_moved_since_the_review_is_skipped_not_written():
    sl, store = FakeSL([setup_box(1, "a1@d1.com", 12)]), fresh_store()
    r = apply(sl, store)
    assert not sl.writes and r["skipped"] == ["a1@d1.com: now 12, not 15"]


def test_a_failed_write_or_readback_is_reported_and_not_recorded():
    sl, store = FakeSL([setup_box(1, "a1@d1.com")], ignore=("limit",)), fresh_store()
    r = apply(sl, store)
    assert r["failed"] == ["a1@d1.com: Smartlead shows 15 after the write"]
    assert "a1@d1.com" not in store.approved_limits()
    sl, store = FakeSL([setup_box(1, "a1@d1.com")], fail=(1,)), fresh_store()
    r = apply(sl, store)
    assert r["failed"][0].startswith("a1@d1.com: ") and "SECRET" not in r["failed"][0]
