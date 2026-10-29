"""BettrData tracker dashboard: which cells get written, and which never do. No network."""
from smartlead.client_tracker import plan

HEAD = ["Campaign", "Channel", "Segment", "Status", "Launched", "Target Titles", "CTA", "# Steps",
        "Leads", "Sent / Initiated", "Open %", "Connected", "Replies", "Positive",
        "Connect %", "Reply %", "Last Change", "Change Date"]


def _line(name, ch, status, leads, sent, open_, conn, rep, pos, f_op=True):
    row = [name, ch, "Seg", status, "9/9/2026", "titles", "cta", "", leads, sent, open_, conn,
           rep, pos, "", "", "", ""]
    return row


def _sheet(lines, formula_rows=None):
    """Rate formulas on every row unless `formula_rows` names the rows that have them."""
    values = [HEAD] + lines
    formulas = [list(r) for r in values]
    for i in (range(2, len(values) + 1) if formula_rows is None else formula_rows):
        formulas[i - 1][14] = f'=IFERROR(L{i}/J{i},"")'
        formulas[i - 1][15] = f'=IFERROR(M{i}/J{i},"")'
    return values, formulas


def _email(name, **kw):
    r = {"client": "BettrData", "channel": "Email", "campaign": name, "status": "ACTIVE",
         "total_leads": 1420, "unique_sent": 165, "unique_opens": 0, "life_replies": 3,
         "life_positive": 2, "launch_date": "2026-10-27"}
    r.update(kw)
    return r


def _li(name, **kw):
    r = {"client": "BettrData", "channel": "LinkedIn", "campaign": name, "status": "IN_PROGRESS",
         "total_leads": "-", "life_invites": 12, "life_accepted": 0, "life_replies": 0,
         "life_positive": 0, "launch_date": "2026-10-28"}
    r.update(kw)
    return r


def test_existing_row_gets_new_numbers_only_where_they_changed():
    values, formulas = _sheet([_line("Roundtable | Laurie", "Email", "Draft", "578", "", "", "",
                                     "", "")], formula_rows=[2])
    updates, new = plan(values, formulas, [_email("Roundtable  |  laurie")], "2026-10-29")
    got = {u["range"]: u["values"][0][0] for u in updates}
    assert got == {"D2": "Active", "I2": 1420, "J2": 165, "M2": 3, "N2": 2}   # O/P formulas exist
    assert new == []


def test_formulas_blocked_status_and_expandi_lead_count_are_left_alone():
    values, formulas = _sheet([_line("Agencies twain", "LinkedIn", "Blocked", "1146", "140", "",
                                     "23", "2", "")], formula_rows=[2])
    formulas[1][12] = "=SUM(X1:X2)"                     # Replies is a formula here
    updates, _ = plan(values, formulas, [_li("Agencies twain", life_invites=150,
                                             life_accepted=25, life_replies=5)], "2026-10-29")
    got = {u["range"]: u["values"][0][0] for u in updates}
    assert got == {"J2": 150, "L2": 25}   # no D (Blocked), I (Expandi), M (formula), N (LinkedIn positive)


def test_same_name_different_channel_are_different_rows():
    values, formulas = _sheet([_line("Retail", "Email", "Paused", "10", "10", "", "", "0", "0"),
                               _line("Retail", "LinkedIn", "Paused", "", "5", "", "1", "0", "0")])
    updates, _ = plan(values, formulas, [_li("Retail", life_invites=9)], "2026-10-29")
    assert {u["range"] for u in updates} == {"D3", "J3"}   # Connected 1 -> 0 is never written


def test_open_rate_only_when_opens_are_tracked():
    values, formulas = _sheet([_line("A", "Email", "Active", "200", "200", "52.50%", "", "1", ""),
                               _line("B", "Email", "Active", "868", "868", "", "", "7", "1")])
    updates, _ = plan(values, formulas, [_email("A", total_leads=200, unique_sent=200,
                                                unique_opens=110, life_replies=1, life_positive=0),
                                         _email("B", total_leads=868, unique_sent=868,
                                                unique_opens=0, life_replies=7, life_positive=1)],
                      "2026-10-29")
    got = {u["range"]: u["values"][0][0] for u in updates}
    assert got == {"K2": "55.00%", "N2": 0}


def test_new_campaign_appended_with_the_sheets_formulas_and_new_row_mark():
    values, formulas = _sheet([_line("Old", "Email", "Paused", "1", "1", "", "", "0", "0")])
    _, new = plan(values, formulas, [_li("Roundtable | Data providers | Aaron")], "2026-10-29")
    (row,) = new
    assert row[:5] == ["Roundtable | Data providers | Aaron", "LinkedIn", "", "Active", "10/28/2026"]
    assert row[8:14] == ["", 12, "", 0, 0, ""]         # no Expandi lead total, no LinkedIn positive
    assert row[14:] == ['=IFERROR(L3/J3,"")', '=IFERROR(M3/J3,"")', "New row", "10/29/2026"]


def test_counts_never_go_down_but_leads_and_status_follow_the_platform():
    values, formulas = _sheet([_line("Hard to Find", "Email", "Active", "97", "868", "", "", "1", "")])
    updates, _ = plan(values, formulas, [_email("Hard to Find", status="PAUSED", total_leads=90,
                                                unique_sent=867, life_replies=0, life_positive=0)],
                      "2026-10-29")
    got = {u["range"]: u["values"][0][0] for u in updates}
    assert got == {"D2": "Paused", "I2": 90, "N2": 0}   # J 868->867 and M 1->0 skipped


def test_missing_rate_formulas_are_filled_in():
    values, formulas = _sheet([_line("Roundtable | Aaron", "Email", "Active", "2376", "284", "",
                                     "", "0", "0")], formula_rows=())
    updates, _ = plan(values, formulas, [_email("Roundtable | Aaron", total_leads=2376,
                                                unique_sent=284, life_replies=0, life_positive=0)],
                      "2026-10-29")
    got = {u["range"]: u["values"][0][0] for u in updates}
    assert got == {"O2": '=IFERROR(L2/J2,"")', "P2": '=IFERROR(M2/J2,"")'}


def test_new_rows_go_above_the_totals_row_and_totals_extend():
    from smartlead.client_tracker import extend_totals, totals_row
    values, formulas = _sheet([_line("Old", "Email", "Paused", "1", "1", "", "", "0", "0"),
                               _line("Yellow = note", "", "", "", "", "", "", "", "")])
    formulas[2][8] = "=SUM(I2:I2)"                      # row 3 is the totals row
    assert totals_row(formulas) == 3
    _, new = plan(values, formulas, [_li("Roundtable | Aaron")], "2026-10-29")
    assert new[0][14] == '=IFERROR(L3/J3,"")'           # inserted at row 3, totals move to 4
    assert extend_totals("=SUM(I2:I68)", 68, 10) == "=SUM(I2:I78)"
    assert extend_totals("=AVERAGE(O2:O65)", 68, 10) == "=AVERAGE(O2:O65)"   # theirs, untouched


def test_campaign_rows_below_the_totals_are_found():
    from smartlead.client_tracker import stray_rows
    values, formulas = _sheet([_line("Old", "Email", "Paused", "1", "1", "", "", "0", "0"),
                               _line("Yellow = note", "", "", "", "", "", "", "", ""),
                               _line("Replies checked ...", "", "", "", "", "", "", "", ""),
                               _line("Roundtable | Aaron", "Email", "Draft", "804", "", "", "", "", "")])
    formulas[2][8] = "=SUM(I2:I2)"
    assert stray_rows(values, formulas) == [5]
