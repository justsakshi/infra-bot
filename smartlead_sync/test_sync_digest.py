"""run.py's sheet-sync verdict and the daily digest (16 Nov 2026): a tab not written is one "broken" ask, a clean
sync clears it, and the old post goes out when the digest can't be written or is off. No network."""
import pytest

import run
from smartlead import digest as dg
from smartlead import notify

OK = [{"tab": "Melior"}, {"tab": "Bettrdata"}]
BAD = [{"tab": "Precise Leads", "error": "APIError 503"}]


@pytest.fixture
def go(monkeypatch):
    def call(ok_tabs, failed_tabs, report_ok=True):
        posts, reports = [], []
        monkeypatch.setattr(notify, "post_digest", lambda text, full=None: posts.append(text) or True)
        monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None:
                            reports.append((src, asks, done, detail)) or report_ok)
        run.report_sync(ok_tabs, failed_tabs)
        return posts, reports
    return call


def test_with_the_digest_on_a_tab_not_written_is_a_broken_ask_and_nothing_is_posted(go, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    posts, reports = go(OK, BAD)
    assert posts == []
    (src, (a,), done, detail), = reports
    assert src == "smartlead-sync" and done == [] and a.kind == "broken" and a.key == "smartlead-sync:tabs-not-written"
    assert a.text == "The daily sheet sync could not write 1 of 3 tabs (Precise Leads), so it shows old numbers."
    assert "Last Sync tab" in a.step and "*Sheet sync: 1 of 3 tabs not written*" in detail[0]


def test_a_clean_sync_clears_the_ask(go, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    assert go(OK, []) == ([], [("smartlead-sync", [], [], [])])


def test_when_the_digest_cannot_be_written_or_is_off_the_old_post_goes_out(go, monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "on")
    posts, reports = go(OK, BAD, report_ok=False)
    assert reports and len(posts) == 1 and posts[0].startswith("⚠️ *Sheet sync: 1 of 3 tabs not written*")
    monkeypatch.setenv("DIGEST_MODE", "off")
    posts, reports = go(OK, BAD)
    assert reports == [] and len(posts) == 1
    assert go(OK, []) == ([], [])


def test_many_failed_tabs_are_named_up_to_three():
    bad = [{"tab": f"T{i}", "error": "x"} for i in range(5)]
    (a,) = run.sync_asks([], bad)
    assert "(T0, T1, T2 and 2 more), so they show old numbers." in a.text
