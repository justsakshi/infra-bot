"""Which switched-on campaigns an inbox is in, and which of them actually sent
in the last 7 days (live 2026-10-13: two ACTIVE Marketplace campaigns with 181
leads mid-sequence had sent nothing since 1 Oct). No network."""
from datetime import date

import placement_report as pr

CAMPAIGNS = [{"id": 1, "name": "Ingest | Main", "status": "ACTIVE"},
             {"id": 2, "name": "Marketplace | New Agencies", "status": "ACTIVE"},
             {"id": 3, "name": "Old", "status": "PAUSED"}]
LEADS = {"status": "ACTIVE", "campaign_lead_stats": {"notStarted": 0, "inprogress": 5}}


def _fake(monkeypatch, sent, fail=()):
    calls = []

    def get(url, key, **params):
        calls.append((url, params))
        if url.endswith("/campaigns"):
            return CAMPAIGNS
        cid = int(url.split("/campaigns/")[1].split("/")[0])
        if url.endswith("/analytics"):
            return LEADS
        if url.endswith("/analytics-by-date"):
            if cid in fail:
                raise RuntimeError("502")
            return {"sent_count": str(sent.get(cid, 0))}
        if url.endswith("/email-accounts"):
            return [{"from_email": "Dan@x.com"}, {"from_email": "eve@x.com"}] if cid == 1 else [{"from_email": "eve@x.com"}]
        raise AssertionError(url)
    monkeypatch.setattr(pr, "_get", get)
    return calls


def test_sending_lists_only_campaigns_that_sent_in_the_window(monkeypatch):
    calls = _fake(monkeypatch, {1: 40, 2: 0})
    out = pr.attachments("k", sending_days=7, today=date(2026, 10, 13))
    assert out["dan@x.com"] == {"campaigns": ["Ingest | Main"], "sending": ["Ingest | Main"]}
    assert out["eve@x.com"] == {"campaigns": ["Ingest | Main", "Marketplace | New Agencies"], "sending": ["Ingest | Main"]}
    window = [p for u, p in calls if u.endswith("/analytics-by-date")]
    assert window[0] == {"start_date": "2026-10-06", "end_date": "2026-10-13"}


def test_unreadable_window_counts_as_sending(monkeypatch):
    _fake(monkeypatch, {1: 0}, fail=(2,))
    out = pr.attachments("k", sending_days=7, today=date(2026, 10, 13))
    assert out["eve@x.com"]["sending"] == ["Marketplace | New Agencies"]   # never hide a possibly live campaign


def test_active_attachments_keeps_its_old_shape(monkeypatch):
    _fake(monkeypatch, {})
    assert pr.active_attachments("k") == {"dan@x.com": ["Ingest | Main"], "eve@x.com": ["Ingest | Main", "Marketplace | New Agencies"]}
