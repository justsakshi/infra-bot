"""Volume review: approved limits, the pilot list, drift against approved limits, and writes. No network."""
import asyncio
from datetime import date

from smartlead import inbox_intake as ii
from smartlead import volume_rule as vr
from test_inbox_intake import FakeCol, FakeSL, acct, run

TODAY = date(2026, 10, 21)
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
    assert store.approved_limits() == {"a1@d1.com": (17, "2026-10-21"), "a2@d1.com": (17, "2026-10-21")}


def test_pilot_list_is_stored_and_merged():
    store = ii.IntakeStore(FakeCol())
    assert store.pilot_domains() == set()
    store.set_pilot({"askmelior.com", "MeliorBuild.com"}, by="U1")
    assert store.pilot_domains() == {"askmelior.com", "meliorbuild.com"}


ROW = {"domain": "d1.com", "client": "Melior", "account": "PRECISE_LEADS", "kind": "google", "action": vr.RAISE,
       "from": [15, 15], "to": [17, 17], "why": "", "pilot": True,
       "inboxes": [{"id": 1, "email": "a1@d1.com"}, {"id": 2, "email": "a2@d1.com"}]}


def apply(sl, store=None):
    store = store or ii.IntakeStore(FakeCol())
    return asyncio.run(vr.apply_rows({"PRECISE_LEADS": sl}, store, [ROW], by="pilot", today=TODAY)), store


def test_apply_writes_each_inbox_reads_back_and_records():
    sl = FakeSL([setup_box(1, "a1@d1.com"), setup_box(2, "a2@d1.com"), setup_box(3, "other@d2.com")])
    r, store = apply(sl)
    assert sl.writes == [(1, {"max_email_per_day": 17}), (2, {"max_email_per_day": 17})]
    assert r["d1.com"]["changed"] == ["a1@d1.com 15 -> 17", "a2@d1.com 15 -> 17"]
    assert store.approved_limits()["a1@d1.com"] == (17, "2026-10-21")


def test_a_limit_moved_since_the_review_is_skipped_not_written():
    sl = FakeSL([setup_box(1, "a1@d1.com", 12), setup_box(2, "a2@d1.com")])
    r, _ = apply(sl)
    assert sl.writes == [(2, {"max_email_per_day": 17})] and r["d1.com"]["skipped"] == ["a1@d1.com: now 12, not 15"]


def test_a_failed_write_or_readback_is_reported_and_not_recorded():
    r, store = apply(FakeSL([setup_box(1, "a1@d1.com"), setup_box(2, "a2@d1.com")], ignore=("limit",)))
    assert r["d1.com"]["failed"][0] == "a1@d1.com: Smartlead shows 15 after the write"
    assert "a1@d1.com" not in store.approved_limits()
    r, _ = apply(FakeSL([setup_box(1, "a1@d1.com"), setup_box(2, "a2@d1.com")], fail=(1,)))
    assert r["d1.com"]["failed"][0].startswith("a1@d1.com: ") and "SECRET" not in r["d1.com"]["failed"][0]


def test_comparison_list_is_stored():
    store = ii.IntakeStore(FakeCol())
    store.set_control({"SolidClients.com"}, by="U1")
    assert store.control_domains() == {"solidclients.com"}
