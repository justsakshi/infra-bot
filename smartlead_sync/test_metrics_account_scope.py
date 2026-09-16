"""Which accounts a metrics run must open, given the wanted client list.

The filter used to run only at account level: an account whose NAME was not in
CAMPAIGN_METRICS_CLIENTS was never opened. That breaks for a client living
inside an agency account - asking for "MELIOR" would open nothing, because the
account is called PRECISE_LEADS and Melior is a client_id inside it.

So accounts are selected by whether they can CONTAIN a wanted client, and the
per-campaign rows are filtered afterwards by resolved client name.
"""
from smartlead.campaign_metrics import account_in_scope, row_client_wanted

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


WANT = {"BELARDI WONG", "MELIOR", "BETTRDATA"}

# --- direct accounts: matched on their own name ---
ok(account_in_scope("BETTRDATA", WANT), "BETTRDATA is wanted directly")
ok(account_in_scope("Belardi Wong", WANT), "Belardi Wong matches case-insensitively")
ok(not account_in_scope("DARLEAN", WANT), "DARLEAN is not wanted - not opened")

# --- the agency account is opened because it CONTAINS a wanted client ---
ok(account_in_scope("PRECISE_LEADS", WANT),
   "PRECISE_LEADS is opened because Melior lives inside it")
ok(account_in_scope("Precise Leads", WANT), "agency name spelling variants")

# ...but not when none of its sub-clients are wanted.
ok(not account_in_scope("PRECISE_LEADS", {"DARLEAN"}),
   "agency is skipped when none of its clients are wanted")
ok(account_in_scope("PRECISE_LEADS", {"OSC"}), "OSC also lives inside PL")

# --- row-level filter decides what actually lands on the tab ---
ok(row_client_wanted("Melior", WANT), "Melior rows are kept")
ok(row_client_wanted("BETTRDATA", WANT), "BETTRDATA rows are kept")
ok(not row_client_wanted("Precise Leads", WANT),
   "the agency's own campaigns are dropped - PL itself was not asked for")
ok(not row_client_wanted("OSC", WANT), "OSC rows are dropped")
ok(not row_client_wanted("DARLEAN", WANT), "DARLEAN rows are dropped")

# Case and spacing from different platforms must not change the decision.
ok(row_client_wanted("melior", WANT), "lowercase row client")
ok(row_client_wanted("Belardi Wong", WANT), "two-word client name")
ok(row_client_wanted("BELARDI_WONG", WANT), "underscore spelling")

# An empty want-set means no filtering was configured: keep everything rather
# than silently emptying the tab.
ok(row_client_wanted("anyone", set()), "empty client list keeps every row")
ok(account_in_scope("ANY_ACCOUNT", set()), "empty client list opens every account")

print("\nALL PASSED")
