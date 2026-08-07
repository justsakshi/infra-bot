"""Per-client Campaign Metrics tabs: grouping, totals, and column projection."""
from smartlead.campaign_metrics import COLUMNS, rows_with_totals


def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def _row(client, campaign, platform, leads):
    r = {c: "" for c in COLUMNS}
    r.update(client=client, campaign=campaign, platform=platform,
             status="IN_PROGRESS", total_leads=leads)
    return r


ROWS = [
    _row("DARLEAN", "Field Services V4", "Smartlead", 361),
    _row("DARLEAN", "Consulting firms", "Heyreach", 285),
    _row("BETTRDATA", "Bettrdata- Retail", "Expandi", 60),
    _row("BETTRDATA", "BD email campaign", "Smartlead", 200),
]


# Mirrors write_campaign_metrics_per_client's grouping without touching Sheets.
def group(rows):
    out = {}
    for r in rows:
        name = str(r.get("client", "")).strip()
        if not name:
            continue
        out.setdefault(name, []).append(r)
    return out


g = group(ROWS)
ok(sorted(g) == ["BETTRDATA", "DARLEAN"], f"one group per client (got {sorted(g)})")
ok(len(g["DARLEAN"]) == 2, "Darlean keeps its 2 campaigns")
ok(len(g["BETTRDATA"]) == 2, "BettrData keeps its 2 campaigns")

# A client's tab must not contain another client's campaigns — the whole point
# of splitting, and the failure that would be worst to ship.
for client, rows in g.items():
    ok(all(r["client"] == client for r in rows),
       f"{client} tab contains only {client} rows")

# Total rows from the shared tab carry no client and must not create a tab.
polluted = ROWS + [{**{c: "" for c in COLUMNS}, "campaign": "Total", "total_leads": 906}]
ok(sorted(group(polluted)) == ["BETTRDATA", "DARLEAN"],
   "a client-less Total row does not become its own tab")

# Each tab ends with exactly one subtotal, covering only that client.
d = rows_with_totals(g["DARLEAN"])
totals = [r for r in d if str(r.get("campaign", "")).startswith("Total")]
ok(len(totals) == 1, f"one total row per client tab (got {len(totals)})")
ok(totals[0]["total_leads"] == 646, f"Darlean subtotal 361+285==646 (got {totals[0]['total_leads']})")

b = rows_with_totals(g["BETTRDATA"])
b_totals = [r for r in b if str(r.get("campaign", "")).startswith("Total")]
ok(b_totals[0]["total_leads"] == 260, f"BettrData subtotal 60+200==260 (got {b_totals[0]['total_leads']})")

# The client column is dropped — every row on the tab is that client.
projected = [{c: r.get(c, "") for c in COLUMNS if c != "client"} for r in d]
ok("client" not in projected[0], "client column dropped from per-client tab")
ok("campaign" in projected[0] and "platform" in projected[0],
   "every other column survives")
ok(len(projected[0]) == len(COLUMNS) - 1,
   f"exactly one column removed (got {len(projected[0])} of {len(COLUMNS)})")

# All platforms for a client land on the same tab.
plats = {r["platform"] for r in g["BETTRDATA"]}
ok(plats == {"Expandi", "Smartlead"},
   f"BettrData tab spans both its platforms (got {plats})")


# --- per-client standalone spreadsheets ---
from smartlead.config import _parse_client_sheets  # noqa: E402

p = _parse_client_sheets("BETTRDATA:1AbC,DARLEAN:1XyZ")
ok(p == {"BETTRDATA": "1AbC", "DARLEAN": "1XyZ"}, f"parses CLIENT:id pairs (got {p})")
ok(_parse_client_sheets("bettrdata:1AbC") == {"BETTRDATA": "1AbC"},
   "client names are upper-cased to match row/client values")
ok(_parse_client_sheets(" BETTRDATA : 1AbC ") == {"BETTRDATA": "1AbC"},
   "surrounding whitespace is tolerated")
ok(_parse_client_sheets("") == {}, "empty config disables the feature")

# One malformed entry must not cost the other clients their sheets.
mixed = _parse_client_sheets("BETTRDATA:1AbC,garbage,:noclient,CLIENT:")
ok(mixed == {"BETTRDATA": "1AbC"},
   f"malformed entries are skipped, valid ones survive (got {mixed})")

# Grouping for standalone sheets upper-cases, so it matches config keys even
# when a row's client field is cased differently.
def group_upper(rows):
    out = {}
    for r in rows:
        n = str(r.get("client", "")).strip()
        if n:
            out.setdefault(n.upper(), []).append(r)
    return out

gu = group_upper(ROWS + [_row("bettrdata", "lowercase client", "Expandi", 5)])
ok(len(gu["BETTRDATA"]) == 3,
   f"rows match their config key regardless of case (got {len(gu['BETTRDATA'])})")

print("\nALL PASSED")
