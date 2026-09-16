"""Which Smartlead campaigns belong on the metrics tab.

The tab is an operating view: what is running now, and what ran recently
enough to still matter. A campaign completed weeks ago is history - it pads
the tab, buries the active work, and drags the Total row.

HeyReach already had this rule; Smartlead did not, so the BettrData tab
carried 32 paused and 3 completed campaigns against 7 active ones.

Active work is never dropped, whatever its numbers.
"""
from smartlead.campaign_metrics import should_include_smartlead_campaign

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def s(status, total=100, **kw):
    d = {"status": status, "total_leads": total}
    d.update(kw)
    return d


# --- active work is always kept, regardless of activity ---
ok(should_include_smartlead_campaign(s("ACTIVE"), 0, 0),
   "ACTIVE with no sends is kept - it is live inventory")
ok(should_include_smartlead_campaign(s("IN_PROGRESS"), 0, 0), "IN_PROGRESS kept")

# --- drafts are never real ---
ok(not should_include_smartlead_campaign(s("DRAFTED"), 5, 5), "DRAFTED dropped")
ok(not should_include_smartlead_campaign(s("DRAFT"), 0, 0), "DRAFT dropped")

# --- paused/completed: kept only while they sent something in the last week ---
# The window is the week, not the month: a campaign that finished a week ago
# still matters, one that finished in early September does not - and a month
# window would keep the latter until October.
ok(should_include_smartlead_campaign(s("PAUSED"), week_sent=12, month_sent=40),
   "PAUSED that sent this week is kept - it may resume")
ok(not should_include_smartlead_campaign(s("PAUSED"), week_sent=0, month_sent=400),
   "PAUSED that sent earlier this month but not this week is history")
ok(should_include_smartlead_campaign(s("COMPLETED"), week_sent=12, month_sent=12),
   "COMPLETED this week is kept")
ok(not should_include_smartlead_campaign(s("COMPLETED"), week_sent=0, month_sent=0),
   "COMPLETED weeks ago is dropped")
ok(not should_include_smartlead_campaign(s("STOPPED"), week_sent=0, month_sent=0),
   "STOPPED with no recent sends is dropped")

# month_sent never rescues a finished campaign - that is the point of using a
# week window. Callers all compute week_sent (metrics_only.py and run.py both
# query the week explicitly), so there is no path where it is silently absent.
ok(not should_include_smartlead_campaign(s("PAUSED"), week_sent=0, month_sent=40),
   "month_sent alone does not rescue a campaign that was silent this week")

# ARCHIVED is a finished state like COMPLETED/STOPPED. It was missing from the
# list, so on 2026-09-16 six archived Melior campaigns with 0 leads and 0 sends
# landed on the client's tab.
ok(not should_include_smartlead_campaign(s("ARCHIVED", total=0), week_sent=0, month_sent=0),
   "ARCHIVED with nothing sent is dropped")
ok(not should_include_smartlead_campaign(s("ARCHIVED", total=500), week_sent=0, month_sent=0),
   "ARCHIVED holding leads but silent this week is still history")
ok(should_include_smartlead_campaign(s("ARCHIVED"), week_sent=30, month_sent=30),
   "ARCHIVED that somehow sent this week is kept, like any finished campaign")

# --- empty shells ---
ok(not should_include_smartlead_campaign(s("PAUSED", total=0), 0, 0),
   "a paused campaign holding no leads is an abandoned shell")
ok(should_include_smartlead_campaign(s("ACTIVE", total=0), 0, 0),
   "an ACTIVE campaign with no leads yet is still kept - it was just created")

# --- unknown/absent status must not silently drop real work ---
ok(should_include_smartlead_campaign(s(""), 0, 0),
   "a campaign with no status is kept rather than guessed away")

print("\nALL PASSED")
