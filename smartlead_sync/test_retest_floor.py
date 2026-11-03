"""The auto-retest 7-day floor reads every recent test from Mongo (no network).
python -m pytest test_retest_floor.py -q"""
import asyncio
from types import SimpleNamespace

from smartlead.placement_store import PlacementStore
from smartlead.retest_targets import select_targets


class FakeCol:
    def __init__(self, docs):
        self.docs = docs

    def find(self, q, proj=None):
        def match(d):
            for k, cond in q.items():
                v = d.get(k)
                if isinstance(cond, dict):
                    if "$gte" in cond and not (v is not None and v >= cond["$gte"]):
                        return False
                    if "$ne" in cond and v == cond["$ne"]:
                        return False
                elif v != cond:
                    return False
            return True
        return [d for d in self.docs if match(d)]


def _store(tests, results):
    s = PlacementStore.__new__(PlacementStore)
    s._tests, s._results, s._snapshots = FakeCol(tests), FakeCol(results), None
    return s


def _row(email):
    return {"client": "PL", "email": email, "top_problem": "No placement test on record",
            "test_date": "", "campaign_name": ""}


def test_floor_counts_every_source_but_abandoned():
    s = _store(
        tests=[{"created": "2026-11-02", "status": "DONE", "emails": ["Bot@x.com"]},
               {"created": "2026-11-02", "status": "ABANDONED", "emails": ["lost@x.com"]},
               {"created": "2026-10-20", "status": "DONE", "emails": ["old@x.com"]}],
        results=[{"email": "weekly@x.com", "date": "2026-11-03", "source": "weekly_report"},
                 {"email": "stale@x.com", "date": "2026-10-01", "source": "api"}])
    assert s.emails_tested_since("2026-10-27") == {"bot@x.com", "weekly@x.com"}


def test_floor_unreadable_is_none_not_empty():
    s = PlacementStore.__new__(PlacementStore)
    s._tests = s._results = s._snapshots = None
    assert s.emails_tested_since("2026-10-27") is None


def test_inbox_the_bot_tested_yesterday_is_not_picked_again():
    # 3 Nov 2026: health rows read only the sheet grid, so once the bot's own
    # test finished the inbox still looked untested and was picked again the
    # next day, every day.
    s = _store([{"created": "2026-11-02", "status": "DONE", "emails": ["bot@x.com"]}], [])
    picked = select_targets([_row("Bot@x.com"), _row("next@x.com")], 2,
                            s.emails_tested_since("2026-10-27"))
    assert [t["email"] for t in picked] == ["next@x.com"]


def test_executor_creates_nothing_when_history_is_unreadable(monkeypatch, capsys):
    import retest_executor as rx

    store = SimpleNamespace(available=True, pending_emails=set,
                            emails_tested_since=lambda since: None)
    reached = []

    async def no_poll(store, keys):
        return 0

    async def rows(acc):
        reached.append(acc)
        return [_row("a@x.com")], {}

    monkeypatch.setattr(rx, "PlacementStore", lambda: store)
    monkeypatch.setattr(rx, "discover_accounts", lambda: [SimpleNamespace(name="PL", api_key="k")])
    monkeypatch.setattr(rx, "_poll_pending", no_poll)
    monkeypatch.setattr(rx, "_health_rows_for", rows)
    asyncio.run(rx.main())
    assert reached == [] and "not creating tests today" in capsys.readouterr().out


def _t(tid, created, target, status="DONE", emails=None, **kw):
    return {"test_id": tid, "created": created, "client": "PL", "target": target, "status": status,
            "emails": emails or [target], "result": "inbox", "inbox_pct": 95, "reason": "r", **kw}


def test_watch_passes_a_clean_week():
    from datetime import date
    import retest_watch as w
    res = w.check([_t(1, "2026-11-04", "a@x.com"), _t(2, "2026-11-04", "b@x.com"),
                   _t(3, "2026-11-11", "a@x.com")], date(2026, 11, 11), cap=2)
    assert res["problems"] == [] and res["warnings"] == [] and len(res["results"]) == 3


def test_watch_catches_cap_floor_warmup_and_stuck_tests():
    from datetime import date
    import retest_watch as w
    res = w.check([_t(1, "2026-11-04", "a@x.com"), _t(2, "2026-11-04", "b@x.com"),
                   _t(3, "2026-11-04", "c@x.com"), _t(4, "2026-11-05", "a@x.com"),
                   _t(5, "2026-11-05", "d@x.com", warmup_off_ids=["9"]),
                   _t(6, "2026-11-04", "e@x.com", status="ACTIVE"),
                   _t(7, "2026-11-05", "f@x.com", emails=[f"s{i}@x.com" for i in range(40)])],
                  date(2026, 11, 6), cap=2)
    text = " ".join(res["problems"])
    assert "PL got 4 tests on 2026-11-04" in text and "a@x.com was tested on 2026-11-04 and again" in text
    assert "still have warm-up off" in text
    warn = " ".join(res["warnings"])
    assert "running since 2026-11-04" in warn and "tested 40 inboxes" in warn


class _FakeSL:
    """Smartlead client with one campaign of 100 senders."""
    def __init__(self, *a):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass

    async def list_campaigns(self):
        return [{"id": 4085678, "name": "Melior - Deliverability Test Campaign"}]

    async def _get(self, path):
        return [{"id": 77}]

    async def get_campaign_email_accounts(self, cid):
        return [{"id": 1000 + i, "from_email": f"s{i}@peer.com"} for i in range(99)] + \
               [{"id": 5, "from_email": "M_Ryan@peer.com"}]


def test_one_flagged_inbox_tests_one_inbox_not_its_whole_campaign(monkeypatch):
    # 3 Nov 2026 dry run: two picks would each have tested all 100 senders of
    # Melior's test campaign.
    import retest_executor as rx
    monkeypatch.setattr(rx, "SmartleadClient", _FakeSL)
    acc = SimpleNamespace(name="PRECISE_LEADS", api_key="k")
    args = asyncio.run(rx._campaign_launch_args(acc, "Melior - Deliverability Test Campaign", "m_ryan@peer.com"))
    assert args == (4085678, 77, ["M_Ryan@peer.com"], ["5"])
    # not attached any more -> None, so the caller uses the standing test campaign
    assert asyncio.run(rx._campaign_launch_args(acc, "Melior - Deliverability Test Campaign", "gone@peer.com")) is None
