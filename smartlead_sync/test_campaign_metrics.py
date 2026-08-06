from datetime import datetime, timezone
from smartlead.campaign_metrics import (
    smartlead_metric_row, smartlead_summary_from_analytics,
    heyreach_metric_row, total_row, COLUMNS,
    should_include_smartlead_campaign,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TODAY = datetime(2026, 6, 15, tzinfo=timezone.utc)  # yesterday = 2026-06-14; month = June

# Regression: Campaign Metrics must be buildable directly from Smartlead's
# lightweight campaign analytics, without waiting for the full inbox sync.
analytics = {
    "id": 3716545,
    "name": "Field Services Consolidated Roundtable - V3",
    "status": "ACTIVE",
    "sent_count": "3122",
    "campaign_lead_stats": {"total": 2987, "inprogress": 1920, "notStarted": 819},
}
summary_from_analytics = smartlead_summary_from_analytics(analytics)
ok(summary_from_analytics == {
    "campaign_id": 3716545,
    "name": "Field Services Consolidated Roundtable - V3",
    "status": "ACTIVE",
    "total_leads": 2987,
    "in_progress": 1920,
    "not_started": 819,
    "sent": 3122,
}, "Smartlead analytics maps to the campaign summary consumed by metrics")

# --- Smartlead ---
summary = {"name": "SL Camp", "status": "ACTIVE", "total_leads": 200,
           "in_progress": 40, "sent": 100, "replied": 7}
leads = [
    {"created_at": "2026-06-10T00:00:00Z", "lead_category_id": 1},   # this month, positive
    {"created_at": "2026-06-14T00:00:00Z", "lead_category_id": 3},   # yesterday, not positive
    {"created_at": "2026-05-20T00:00:00Z", "lead_category_id": 1},   # last month
]
# month_sent is what Msg Sent reports — the campaign's all-time `sent` (100)
# is deliberately NOT used, so the sheet matches the team's month-to-date sheet.
slr = smartlead_metric_row(summary, leads, month_replies=5, yest_replies=1,
                           today=TODAY, positive_ids={1, 2, 5}, month_sent=100)
ok(slr["platform"] == "Smartlead", "platform")
ok(slr["total_leads"] == 200, "total_leads")
ok(slr["leads_added_month"] == 2, f"leads added this month==2 (got {slr['leads_added_month']})")
ok(slr["leads_added_yesterday"] == 1, f"leads added yesterday==1 (got {slr['leads_added_yesterday']})")
ok(slr["leads_not_started"] == 0, "Smartlead leads-not-started defaults to zero")
ok(slr["connections_sent"] == "-", "connections '-' for smartlead")
ok(slr["total_responses_month"] == 5, "total responses month")
ok(slr["positive_neutral_month"] == 2, f"positive/neutral (cat 1 x2)==2 (got {slr['positive_neutral_month']})")

# Msg Sent is month-to-date, never the all-time figure. Verified against the
# team's sheet (Legal Firms 374 month vs 1068 all-time). Here `sent` is 100 and
# month_sent is 7, so 7 is correct and 100 would be the old bug.
_mtd = smartlead_metric_row(summary, leads, month_replies=0, yest_replies=0,
                            today=TODAY, positive_ids={1, 2, 5}, month_sent=7)
ok(_mtd["msg_sent"] == 7, f"msg_sent is month-to-date, not all-time (got {_mtd['msg_sent']})")

# A paused campaign that sent nothing this month still holds real leads, so it
# must appear. Only drafts and empty shells are dropped.
ok(should_include_smartlead_campaign(
    {"status": "PAUSED", "total_leads": 6905}, 0, 0), "paused campaign with leads is included")
ok(not should_include_smartlead_campaign(
    {"status": "PAUSED", "total_leads": 0}, 0, 0), "paused empty shell is dropped")
ok(not should_include_smartlead_campaign(
    {"status": "DRAFTED", "total_leads": 500}, 0, 0), "draft is dropped even with leads")
ok(should_include_smartlead_campaign(
    {"status": "COMPLETED", "total_leads": 119}, 0, 0), "completed campaign with leads is included")

# --- HeyReach ---
camp = {"name": "HR Camp", "status": "IN_PROGRESS",
        "progressStats": {"totalUsers": 99, "totalUsersInProgress": 35}}
overall_all = {"overallStats": {"connectionsSent": 25, "connectionsAccepted": 4,
               "messagesSent": 12, "totalMessageReplies": 9, "autoTaggedInterested": 3}}
# Deliberately different from the all-time block above: the row must read the
# month window, so these are the values it has to pick up.
overall_month = {"overallStats": {"connectionsSent": 20, "connectionsAccepted": 3,
                 "messagesSent": 8, "totalMessageReplies": 4, "autoTaggedInterested": 2},
                 "byDayStats": {"2026-06-14T00:00:00Z": {"autoTaggedInterested": 1, "totalMessageReplies": 1}}}
hr_leads = [{"creationTime": "2026-06-14T09:00:00Z"}, {"creationTime": "2026-06-02T09:00:00Z"},
            {"creationTime": "2026-05-30T09:00:00Z"}]
hrr = heyreach_metric_row(camp, overall_all, overall_month, hr_leads, today=TODAY)
ok(hrr["platform"] == "Heyreach", "platform hr")
ok(hrr["total_leads"] == 99, "hr total_leads")
ok(hrr["leads_in_progress"] == 35, "hr in progress")
ok(hrr["leads_not_started"] == "-", "HeyReach leads-not-started is not available")
# Month-to-date, not all-time — the all-time block offers 25/4/12 and these
# must not be picked up. Verified against the team's sheet: Consulting Firms
# shows 127 connections sent, which is the month figure (all-time is 180).
ok(hrr["connections_sent"] == 20, f"hr connections sent is month-to-date (got {hrr['connections_sent']})")
ok(hrr["connections_accepted"] == 3, f"hr connections accepted is month-to-date (got {hrr['connections_accepted']})")
ok(hrr["msg_sent"] == 8, f"hr msg sent is month-to-date (got {hrr['msg_sent']})")
ok(hrr["leads_added_yesterday"] == 1, f"hr leads yest==1 (got {hrr['leads_added_yesterday']})")
ok(hrr["leads_added_month"] == 2, f"hr leads month==2 (got {hrr['leads_added_month']})")
ok(hrr["total_responses_month"] == 4, "hr responses month")
ok(hrr["positive_responses_yesterday"] == 1, "hr positive yesterday")
ok(hrr["positive_neutral_month"] == 2, "hr positive month")

# --- Total footer ---
tr = total_row([slr, hrr])
ok(tr["campaign"] == "Total", "total label")
ok(tr["total_leads"] == 299, f"total leads sum==299 (got {tr['total_leads']})")
ok(tr["msg_sent"] == 108, f"msg sent sum (100 SL + 8 HR)==108 (got {tr['msg_sent']})")
ok(set(COLUMNS) >= {"campaign", "platform", "status", "total_leads", "leads_not_started"}, "COLUMNS defined")

# --- Reporting Range ---
from smartlead.campaign_metrics import get_reporting_range

# 1. Test Auto detection on day <= 5
t1 = datetime(2026, 7, 3, 12, 0, 0, tzinfo=timezone.utc)
start, end, name = get_reporting_range("auto", t1)
ok(name == "June", f"auto-month on July 3rd should be June (got {name})")
ok(start == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc), "June start")
ok(end == datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc), "June end")

# 2. Test Auto detection on day > 5
t2 = datetime(2026, 7, 10, 12, 0, 0, tzinfo=timezone.utc)
start, end, name = get_reporting_range("auto", t2)
ok(name == "July", f"auto-month on July 10th should be July (got {name})")
ok(start == datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc), "July start")
ok(end == t2, "July end capped at today")

# 3. Test explicit month name
start, end, name = get_reporting_range("june", t2)
ok(name == "June", "explicit June name")
ok(start == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc), "June start")
ok(end == datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc), "June end")

# 4. Test explicit previous
start, end, name = get_reporting_range("previous", t2)
ok(name == "June", "explicit previous")
ok(start == datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc), "June start")

print("\nALL PASSED")
