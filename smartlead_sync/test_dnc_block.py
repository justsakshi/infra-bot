"""Do Not Contact -> global block list (dnc_block.py). No network."""
from datetime import datetime, timezone

from dnc_block import missing, opted_out, wanted_campaigns


def _row(cat=None, email="a@x.com", unsub=False):
    return {"lead_category_id": cat, "status": "BLOCKED", "lead": {"email": email, "is_unsubscribed": unsub}}


def test_do_not_contact_and_unsubscribe_link_count():
    # 24 Nov 2026: "Please remove me from your mailing list" / "Unsubscribe", tagged Do Not Contact (4).
    assert opted_out(_row(4, "Bruce@M2.com")) == "bruce@m2.com"
    assert opted_out(_row(None, unsub=True)) == "a@x.com"


def test_other_categories_and_bad_rows_do_not():
    for cat in (None, 1, 2, 3, 5, 6, 7, 8, 9):     # not interested (3) is not an opt-out
        assert opted_out(_row(cat)) is None
    assert opted_out({"lead_category_id": 4, "lead": {"email": ""}}) is None
    assert opted_out({"lead_category_id": 4}) is None


def test_only_addresses_not_already_blocked_by_address_or_domain():
    found = {"a@x.com", "b@y.com", "c@z.com"}
    assert missing(found, {"a@x.com", "y.com"}) == ["c@z.com"]


def test_daily_reads_live_and_recent_campaigns_backfill_reads_all_but_drafts():
    now = datetime(2026, 11, 24, tzinfo=timezone.utc)
    camps = [{"id": 1, "status": "ACTIVE", "created_at": "2025-01-01T00:00:00Z"},
             {"id": 2, "status": "COMPLETED", "created_at": "2026-11-01T00:00:00Z"},
             {"id": 3, "status": "COMPLETED", "created_at": "2025-06-01T00:00:00Z"},
             {"id": 4, "status": "DRAFTED", "created_at": "2026-11-20T00:00:00Z"},
             {"id": 5, "status": "PAUSED", "created_at": None}]
    assert [c["id"] for c in wanted_campaigns(camps, False, now)] == [1, 2, 5]
    assert [c["id"] for c in wanted_campaigns(camps, True, now)] == [1, 2, 3, 5]


def test_record_keeps_why_where_and_who_for_later_analysis():
    from dnc_block import record
    row = {"lead_category_id": 4, "status": "BLOCKED", "created_at": "2026-11-01T00:00:00Z",
           "lead": {"email": "Bruce@M2.com", "first_name": "Bruce", "last_name": "F", "company_name": "M2"}}
    r = record(row, "BETTRDATA", {"id": 4443938, "name": "Roundtable"})
    assert r == {"email": "bruce@m2.com", "domain": "m2.com", "account": "BETTRDATA", "reason": "do_not_contact",
                 "campaign_id": "4443938", "campaign": "Roundtable", "lead_status": "BLOCKED",
                 "lead_added": "2026-11-01T00:00:00Z", "company": "M2", "first_name": "Bruce", "last_name": "F"}
    assert record(_row(None, unsub=True), "PL", {"id": 1})["reason"] == "unsubscribe_link"
    assert record(_row(3), "PL", {"id": 1}) is None


def test_report_counts_without_addresses():
    from dnc_block import summary
    docs = [{"_id": "a@x.com", "sightings": [{"account": "BETTRDATA", "campaign": "R", "reason": "do_not_contact"}]},
            {"_id": "b@y.com", "sightings": [{"account": "BETTRDATA", "campaign": "R", "reason": "unsubscribe_link"}]}]
    text = "\n".join(summary(docs))
    assert "2 opted-out address(es)" in text and "BETTRDATA 2" in text and "2  BETTRDATA · R" in text
    assert "@" not in text
