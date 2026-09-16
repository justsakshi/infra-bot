"""Campaign Metrics attributes PRECISE_LEADS campaigns to their real client.

PRECISE_LEADS is an agency account: Melior, Bettrdata and OSC live inside it,
separated only by each campaign's Smartlead `client_id`. Grouping metrics rows
by account name would file 169 Melior campaigns under "PRECISE_LEADS" and put
another client's numbers on Melior's tab.

Campaign NAME is not a usable signal. Measured on the live account 2026-09-16:
169 non-draft campaigns carry client_id 12256, only 100 have "melior" in the
name, and 70 of the id-matched ones do not - they are auto-created
reply-category campaigns named "Interested" / "Information Request".
"""
from smartlead.campaign_metrics import metrics_client_for

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


# --- the agency account splits by client_id ---
ok(metrics_client_for("PRECISE_LEADS", 12256) == "Melior", "12256 -> Melior")
ok(metrics_client_for("PRECISE_LEADS", 456214) == "Bettrdata", "456214 -> Bettrdata")
ok(metrics_client_for("PRECISE_LEADS", 145916) == "OSC", "145916 -> OSC")

# Smartlead returns ids as strings on some endpoints.
ok(metrics_client_for("PRECISE_LEADS", "12256") == "Melior", "string client_id works")

# 381 of PL's campaigns have no client_id at all: they are the agency's own.
ok(metrics_client_for("PRECISE_LEADS", None) == "Precise Leads",
   "no client_id -> the agency itself")
ok(metrics_client_for("PRECISE_LEADS", 999999) == "Precise Leads",
   "unknown client_id -> the agency, never a guess")

# Account-name spellings vary by platform and OS env-var casing.
ok(metrics_client_for("Precise Leads", 12256) == "Melior", "human spelling")
ok(metrics_client_for("precise_leads", 12256) == "Melior", "lowercase + underscore")

# --- every other account IS the client; client_id must not reassign it ---
ok(metrics_client_for("BETTRDATA", None) == "BETTRDATA", "BETTRDATA unchanged")
ok(metrics_client_for("BETTRDATA", 12256) == "BETTRDATA",
   "a stray client_id on a direct account never renames it")
ok(metrics_client_for("Belardi Wong", None) == "Belardi Wong", "Belardi Wong unchanged")
ok(metrics_client_for("DARLEAN", 456214) == "DARLEAN", "DARLEAN unchanged")

print("\nALL PASSED")
