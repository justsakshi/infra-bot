"""warmup_reply_rate_pct must flow from warmup_details into inbox rows,
and the planner must stop re-emitting LONG_IDLE nudges once it matches."""
from smartlead.processing import process_inbox_availability


def _mk_row(email="a@x.com"):
    return {
        "email": email, "connection_ok": True, "message_per_day": 30,
        "test_sheet_status": "inbox", "dns_spf_ok": True,
        "dns_dkim_ok": True, "dns_dmarc_ok": True,
    }


def test_reply_rate_flows_into_row():
    row = _mk_row()
    rep_map = {"a@x.com": {"rep": "99%", "warmup_state": "on",
                           "warmup_max_count": 20, "last_active_date": "",
                           "warmup_spam_count": 0, "warmup_reply_rate_pct": 28}}
    process_inbox_availability([row], rep_map, {})
    assert row["warmup_reply_rate_pct"] == 28


def test_missing_reply_rate_is_none():
    row = _mk_row()
    rep_map = {"a@x.com": {"rep": "99%", "warmup_state": "on",
                           "warmup_max_count": 20, "last_active_date": "",
                           "warmup_spam_count": 0}}
    process_inbox_availability([row], rep_map, {})
    assert row["warmup_reply_rate_pct"] is None
