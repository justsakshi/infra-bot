from datetime import datetime, timezone
from smartlead.campaign_metrics import (
    smartlead_metric_row, heyreach_metric_row, total_row, COLUMNS,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TODAY = datetime(2026, 6, 15, tzinfo=timezone.utc)  # yesterday = 2026-06-14; month = June

# --- Smartlead ---
summary = {"name": "SL Camp", "status": "ACTIVE", "total_leads": 200,
           "in_progress": 40, "sent": 100, "replied": 7}
leads = [
    {"created_at": "2026-06-10T00:00:00Z", "lead_category_id": 1},   # this month, positive
    {"created_at": "2026-06-14T00:00:00Z", "lead_category_id": 3},   # yesterday, not positive
    {"created_at": "2026-05-20T00:00:00Z", "lead_category_id": 1},   # last month
]
slr = smartlead_metric_row(summary, leads, month_replies=5, yest_replies=1,
                           today=TODAY, positive_ids={1, 2, 5})
ok(slr["platform"] == "Smartlead", "platform")
ok(slr["total_leads"] == 200, "total_leads")
ok(slr["leads_added_month"] == 2, f"leads added this month==2 (got {slr['leads_added_month']})")
ok(slr["leads_added_yesterday"] == 1, f"leads added yesterday==1 (got {slr['leads_added_yesterday']})")
ok(slr["connections_sent"] == "-", "connections '-' for smartlead")
ok(slr["total_responses_month"] == 5, "total responses month")
ok(slr["positive_neutral_month"] == 2, f"positive/neutral (cat 1 x2)==2 (got {slr['positive_neutral_month']})")

# --- HeyReach ---
camp = {"name": "HR Camp", "status": "IN_PROGRESS",
        "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}}
overall_all = {"overallStats": {"connectionsSent": 25, "connectionsAccepted": 4,
               "messagesSent": 12, "totalMessageReplies": 9, "autoTaggedInterested": 3}}
overall_month = {"overallStats": {"totalMessageReplies": 4, "autoTaggedInterested": 2},
                 "byDayStats": {"2026-06-14T00:00:00Z": {"autoTaggedInterested": 1, "totalMessageReplies": 1}}}
hr_leads = [{"creationTime": "2026-06-14T09:00:00Z"}, {"creationTime": "2026-06-02T09:00:00Z"},
            {"creationTime": "2026-05-30T09:00:00Z"}]
hrr = heyreach_metric_row(camp, overall_all, overall_month, hr_leads, today=TODAY)
ok(hrr["platform"] == "Heyreach", "platform hr")
ok(hrr["total_leads"] == 99, "hr total_leads")
ok(hrr["leads_in_progress"] == 35, "hr in progress")
ok(hrr["connections_sent"] == 25, "hr connections sent")
ok(hrr["msg_sent"] == 12, "hr msg sent")
ok(hrr["leads_added_yesterday"] == 1, f"hr leads yest==1 (got {hrr['leads_added_yesterday']})")
ok(hrr["leads_added_month"] == 2, f"hr leads month==2 (got {hrr['leads_added_month']})")
ok(hrr["total_responses_month"] == 4, "hr responses month")
ok(hrr["positive_responses_yesterday"] == 1, "hr positive yesterday")
ok(hrr["positive_neutral_month"] == 2, "hr positive month")

# --- Total footer ---
tr = total_row([slr, hrr])
ok(tr["campaign"] == "Total", "total label")
ok(tr["total_leads"] == 299, f"total leads sum==299 (got {tr['total_leads']})")
ok(tr["msg_sent"] == 112, f"msg sent sum (100+12)==112 (got {tr['msg_sent']})")
ok(set(COLUMNS) >= {"campaign", "platform", "status", "total_leads"}, "COLUMNS defined")
print("\nALL PASSED")
