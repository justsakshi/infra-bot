"""'# Campaigns' on the All Inboxes tab counts ACTIVE campaigns only.

Campaign Desk splits an inbox's daily capacity by this number, so counting
paused/completed campaigns silently throttled sending.

    .venv\\Scripts\\python.exe -m pytest test_inbox_campaign_count.py -q
"""
from smartlead.sheets import _dedupe_inbox_rows


def _row(campaign, status, email="a@x.com", client="PL"):
    return {"client": client, "email": email, "campaign_name": campaign,
            "campaign_status": status}


def test_counts_only_live_campaigns():
    rows = [
        _row("Q3 retail", "ACTIVE"),
        _row("Q2 agencies", "COMPLETED"),
        _row("Q1 test", "PAUSED"),      # paused resumes onto the inbox: counts
        _row("Draft", "DRAFTED"),
        _row("Old", "STOPPED"),
        _row("Q3 law firms", "ACTIVE"),
    ]
    [out] = _dedupe_inbox_rows(rows)
    assert out["campaigns"] == 3
    assert out["campaign_status"] == "ACTIVE"   # most-live row still kept


def test_inbox_only_on_finished_campaigns_counts_zero():
    [out] = _dedupe_inbox_rows([_row("Old", "COMPLETED"), _row("Older", "STOPPED")])
    assert out["campaigns"] == 0


def test_orphan_rows_never_count():
    [out] = _dedupe_inbox_rows([_row("N/A (no campaign)", "ACTIVE"), _row("", "")])
    assert out["campaigns"] == 0


def test_status_case_insensitive_and_per_inbox():
    rows = [_row("A", "active", email="a@x.com"), _row("B", "ACTIVE", email="b@x.com"),
            _row("C", "ACTIVE", email="b@x.com")]
    counts = {r["email"]: r["campaigns"] for r in _dedupe_inbox_rows(rows)}
    assert counts == {"a@x.com": 1, "b@x.com": 2}
