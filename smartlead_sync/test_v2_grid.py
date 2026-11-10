"""The bot fills the team's test grid from Smart Delivery v2 (v2_grid.py). Manveen said yes on 10 Nov 2026:
the Precise Leads tab had stopped at 5 Oct and the Bettrdata tab at 28 Sep, because the grid was filled by hand."""
from datetime import date

import v2_grid
from smartlead.sheets import _parse_test_date


def v(email, client, night, verdict, g, m):
    return {"email": email, "domain": email.split("@")[1], "client": client, "night": night, "verdict": verdict,
            "google": {"inbox": g[0], "reported": g[1], "sent": 4}, "microsoft": {"inbox": m[0], "reported": m[1], "sent": 4}}


GRID = [["", "", "", "5 Oct 2026", "", "9 Nov 2026", ""],
        ["Domains ", "ESP", "", "G suite", "Outlook", "G suite", "Outlook"],
        ["good.com", "Google", "", "Inbox", "Inbox", "", ""],
        ["bad.com", "Google", "", "Inbox", "Inbox", "Spam", ""],
        ["half.com", "Outlook", "", "", "", "", ""]]


def test_the_label_is_the_night_marked_v2_and_the_reader_still_gets_its_date():
    assert v2_grid.label_for("2026-11-09") == "9 Nov 2026 v2"
    assert _parse_test_date("9 Nov 2026 v2") == "2026-11-09"
    assert _parse_test_date("9 Nov 2026") == "2026-11-09" and _parse_test_date("v2") == ""


def test_a_night_is_pooled_per_domain_and_provider():
    rows = [v("a@good.com", "Precise Leads", "2026-11-09", "PASS", (4, 4), (4, 4)),
            v("b@good.com", "Precise Leads", "2026-11-09", "WATCH", (3, 4), (4, 4)),
            v("a@bad.com", "Precise Leads", "2026-11-09", "FAIL", (0, 4), (4, 4)),
            v("x@half.com", "Precise Leads", "2026-11-09", "INCOMPLETE", (1, 2), (4, 4)),     # not judged: left out
            v("y@half.com", "Precise Leads", "2026-11-09", "PASS", (4, 4), (0, 0)),           # Microsoft seeds never answered
            v("z@other.com", "Bettrdata", "2026-11-09", "PASS", (4, 4), (4, 4)),
            v("q@unknown.com", "Somebody Else", "2026-11-09", "PASS", (4, 4), (4, 4))]        # no grid tab for that client
    got = v2_grid.night_results(rows)
    assert sorted(got) == [("Bettrdata", "2026-11-09"), ("Precise Leads", "2026-11-09")]
    pl = got[("Precise Leads", "2026-11-09")]
    assert pl["good.com"] == {"G Suite": {"inbox_pct": 87.5}, "Office365": {"inbox_pct": 100.0}}
    assert pl["bad.com"]["G Suite"] == {"inbox_pct": 0.0}
    assert pl["half.com"] == {"G Suite": {"inbox_pct": 100.0}}                                # no Outlook claim


def test_a_new_night_gets_one_column_pair_and_each_provider_its_own_cell():
    results = {"good.com": {"G Suite": {"inbox_pct": 87.5}, "Office365": {"inbox_pct": 100.0}},
               "bad.com": {"G Suite": {"inbox_pct": 0.0}, "Office365": {"inbox_pct": 100.0}},
               "half.com": {"G Suite": {"inbox_pct": 100.0}},
               "gone.com": {"G Suite": {"inbox_pct": 100.0}}}
    plan = v2_grid.plan_tab(GRID, results, "9 Nov 2026 v2")
    assert plan["new_columns"] == (7, 8)                                   # appended after the last column, once
    assert plan["writes"] == [(2, 7, "Inbox"), (2, 8, "Inbox"), (3, 7, "spam"), (3, 8, "Inbox"), (4, 7, "Inbox")]
    assert plan["skipped"] == {"gone.com": "domain not in sheet"}          # reported, never added as a row


def test_a_hand_typed_cell_is_never_overwritten():
    """The team typed 'Spam' for bad.com under 9 Nov 2026 by hand. A v2 column of the same day sits beside it,
    and a second run over a column that already holds values changes nothing."""
    filled = [r + ["", ""] for r in GRID]
    filled[0][7], filled[1][7], filled[1][8] = "9 Nov 2026 v2", "G suite", "Outlook"
    filled[3][7] = "Inbox"                                                 # someone corrected the bot's cell
    results = {"bad.com": {"G Suite": {"inbox_pct": 0.0}, "Office365": {"inbox_pct": 100.0}}}
    plan = v2_grid.plan_tab(filled, results, "9 Nov 2026 v2")
    assert plan["new_columns"] is None and plan["writes"] == [(3, 8, "Inbox")]   # only the empty cell
    assert GRID[3][5] == "Spam"                                            # the hand column is another column


class FakeSheet:
    def __init__(self, rows):
        self.rows, self.col_count, self.updates, self.added = [list(r) for r in rows], len(rows[0]), [], 0

    def get_all_values(self):
        return [list(r) for r in self.rows]

    def add_cols(self, n):
        self.added += n
        self.col_count += n

    def batch_update(self, cells):
        self.updates.append(cells)


def test_sync_reads_each_tab_once_writes_once_and_a_dry_run_writes_nothing():
    rows = [v("a@good.com", "Precise Leads", "2026-11-09", "PASS", (4, 4), (4, 4)),
            v("a@bad.com", "Precise Leads", "2026-11-09", "FAIL", (0, 4), (4, 4))]
    sheet = FakeSheet(GRID)
    out = v2_grid.sync(apply=False, today=date(2026, 11, 10), verdict_rows=rows, open_tab=lambda tab: sheet)
    assert out == [{"tab": "Precise Leads", "night": "2026-11-09", "label": "9 Nov 2026 v2", "domains": 2,
                    "written": 4, "skipped": {}, "applied": False, "error": ""}]
    assert sheet.updates == []
    out = v2_grid.sync(apply=True, today=date(2026, 11, 10), verdict_rows=rows, open_tab=lambda tab: sheet)
    assert out[0]["applied"] and sheet.added == 2 and len(sheet.updates) == 1
    ranges = {c["range"]: c["values"][0][0] for c in sheet.updates[0]}
    assert ranges == {"H1": "9 Nov 2026 v2", "H2": "G suite", "I2": "Outlook",
                      "H3": "Inbox", "I3": "Inbox", "H4": "spam", "I4": "Inbox"}


def test_sync_never_raises():
    def boom(tab):
        raise RuntimeError("quota exceeded")
    rows = [v("a@good.com", "Precise Leads", "2026-11-09", "PASS", (4, 4), (4, 4))]
    out = v2_grid.sync(apply=True, today=date(2026, 11, 10), verdict_rows=rows, open_tab=boom)
    assert out[0]["error"] == "quota exceeded" and out[0]["applied"] is False
    assert v2_grid.sync(apply=True, verdict_rows=[], open_tab=boom) == []
