"""Mock data for ``--mock`` testing mode."""

from __future__ import annotations

MOCK_CAMPAIGNS = [
    {"id": 101, "name": "Outbound Outreach Q1", "status": "START"},
    {"id": 102, "name": "Follow-up Batch A", "status": "PAUSE"},
    {"id": 103, "name": "Cold Leads B", "status": "STOP"},
]

MOCK_EMAIL_ACCOUNTS = [
    {"id": 201, "from_email": "sales1@company.com", "from_name": "Sales One", "campaign_id": 101, "type": "GMAIL"},
    {"id": 202, "from_email": "sales2@company.com", "from_name": "Sales Two", "campaign_id": 101, "type": "OUTLOOK"},
    {"id": 203, "from_email": "hello@marketing.co", "from_name": "Marketing Team", "campaign_id": 102, "type": "GMAIL"},
    {"id": 204, "from_email": "support@org.net", "from_name": "Support", "campaign_id": None, "type": "OTHER"},
]

MOCK_STATS: dict[int, dict] = {
    101: {"sent_count": 500, "open_count": 250, "reply_count": 50, "bounce_count": 10, "total": 1000, "notStarted": 400, "inProgress": 600},
    102: {"sent_count": 200, "open_count": 100, "reply_count": 20, "bounce_count": 5, "total": 500, "notStarted": 300, "inProgress": 200},
    103: {"sent_count": 0, "open_count": 0, "reply_count": 0, "bounce_count": 0, "total": 100, "notStarted": 100, "inProgress": 0},
}


def get_mock_data() -> tuple[list[dict], list[dict], list[dict]]:
    """Return ``(inbox_data, campaign_summary, warmup_data)`` from fake fixtures."""
    print("[mock] Generating mock data ...")

    campaign_map = {c["id"]: c for c in MOCK_CAMPAIGNS}

    campaign_summary = []
    for c_id, c in campaign_map.items():
        stats = MOCK_STATS.get(c_id, {})
        campaign_summary.append({
            "campaign_id": c_id,
            "name": c["name"],
            "status": c["status"],
            "total_leads": stats.get("total", 0),
            "unique_sent": 0,
            "reach_pct": "0%",
            "sent": stats.get("sent_count", 0),
            "opened": stats.get("open_count", 0),
            "replied": stats.get("reply_count", 0),
            "bounced": stats.get("bounce_count", 0),
            "not_started": stats.get("notStarted", 0),
            "in_progress": stats.get("inProgress", 0),
            "paused": 0,
            "completed": 0,
            "stopped": 0,
        })

    inbox_data = []
    for acc in MOCK_EMAIL_ACCOUNTS:
        c_id = acc.get("campaign_id")
        campaign = campaign_map.get(c_id, {})
        inbox_data.append({
            "email": acc["from_email"],
            "name": acc["from_name"],
            "provider": "Gmail" if "GMAIL" in acc.get("type", "") else "Other",
            "campaign_name": campaign.get("name", "N/A (Orphaned)"),
            "campaign_status": campaign.get("status", "N/A"),
            "daily_limit": "0 / 50",
            "leads_remaining": 0,
            "total_inboxes": 0,
            "individual_load": 0,
            "warmup_rep_pct": "95.0%",
            "test_sheet_status": "inbox",
            "availability": "FREE",
            "account_id": acc["id"],
        })

    return inbox_data, campaign_summary, []
