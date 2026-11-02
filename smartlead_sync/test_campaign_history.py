"""Campaign History: rows written per day. No network."""
from smartlead import campaign_history as ch


def _row(client="BettrData", channel="Email", platform="Smartlead", name="Campaign A",
         cid="1", status="ACTIVE", launch="2026-10-27", **life):
    base = {"life_sent": None, "life_bounced": None, "life_invites": None,
            "life_accepted": None, "life_replies": None, "life_positive": None,
            "life_not_interested": None, "life_wrong_person": None}
    base.update({f"life_{k}": v for k, v in life.items()})
    return {"client": client, "channel": channel, "platform": platform, "campaign": name,
            "campaign_id": cid, "status": status, "launch_date": launch,
            "total_leads": 100, **base}


def test_history_rows_skip_totals_and_mark_missing_figures():
    rows = [_row(sent=284, bounced=1, replies=5, positive=2, not_interested=1, wrong_person=0),
            {"client": "BettrData", "campaign": "Total — BettrData"},   # no channel
            _row(channel="LinkedIn", platform="Expandi", name="LinkedIn A", cid="9",
                 invites=12, accepted=0, replies=0, positive=0)]
    out = ch.history_rows(rows, "2026-10-29")
    assert len(out) == 2
    email = out[0]
    assert email[:3] == ["2026-10-29", "BettrData", "Email"]
    assert email[8:16] == [284, 1, "-", "-", 5, 2, 1, 0]    # no invites on email
    linkedin = out[1]
    assert linkedin[8:16] == ["-", "-", 12, 0, 0, 0, "-", "-"]   # a real 0 stays 0
    assert len(email) == len(ch.HEADER)


def test_parse_history_round_trip():
    values = [ch.HEADER] + [[str(c) for c in r] for r in
                            ch.history_rows([_row(sent=1234, replies=3)], "2026-10-29")]
    parsed = ch.parse_history(values)
    assert parsed[0]["sent"] == 1234 and parsed[0]["replies"] == 3
    assert parsed[0]["invites"] is None


def test_row_ranges_group_contiguous_rows():
    assert ch._row_ranges([10, 5, 6, 7]) == [(5, 7), (10, 10)]
    assert ch._row_ranges([]) == []


class _FakeWS:
    def __init__(self, values):
        self.values = values
        self.deleted, self.appended, self.frozen = [], [], False

    def col_values(self, n):
        return [r[0] for r in self.values]

    def row_values(self, n):
        return self.values[n - 1]

    def update(self, values, range_name):
        self.values = values + self.values[1:]

    def freeze(self, rows):
        self.frozen = True

    def delete_rows(self, start, end):
        self.deleted.append((start, end))
        del self.values[start - 1:end]

    def append_rows(self, rows, value_input_option):
        self.appended.extend(rows)
        self.values.extend(rows)


class _FakeWriter:
    def __init__(self, ws):
        self.ws = ws

    def _get_or_create_shared_tab(self, title, rows, cols):
        return self.ws


def test_rerun_on_the_same_day_replaces_that_days_rows():
    ws = _FakeWS([ch.HEADER, ["2026-10-28", "x"], ["2026-10-29", "old"], ["2026-10-29", "old"]])
    n = ch.write_history(_FakeWriter(ws), [["2026-10-29", "new"]], "2026-10-29", "Campaign History")
    assert n == 1 and ws.deleted == [(3, 4)]
    assert [r[1] for r in ws.values[1:]] == ["x", "new"]


def test_first_write_adds_the_header():
    ws = _FakeWS([])
    ch.write_history(_FakeWriter(ws), [["2026-10-29", "a"]], "2026-10-29", "Campaign History")
    assert ws.values[0] == ch.HEADER and ws.frozen


def test_a_changed_header_is_rewritten():
    old = ch.HEADER[:13] + ["Positive", "Campaign ID"]
    ws = _FakeWS([old, ["2026-10-29", "old"]])
    ch.write_history(_FakeWriter(ws), [["2026-10-29", "new"]], "2026-10-29", "Campaign History")
    assert ws.values[0] == ch.HEADER and [r[1] for r in ws.values[1:]] == ["new"]


def test_open_columns_are_written_for_email_and_blank_for_linkedin():
    email = _row(sent=284, replies=5)
    email.update(unique_sent=132, unique_opens=106, open_tracking="on", out_of_office=2)
    li = _row(channel="LinkedIn", platform="Expandi", name="LinkedIn A", cid="9", invites=3)
    out = ch.history_rows([email, li], "2026-10-30")
    assert out[0][-4:] == [132, 106, "on", 2]
    assert out[1][-4:] == ["-", "-", "", "-"]
    parsed = ch.parse_history([ch.HEADER] + [[str(c) for c in r] for r in out])
    assert parsed[0]["reached"] == 132 and parsed[0]["opened"] == 106
    assert parsed[0]["open_tracking"] == "on" and parsed[0]["campaign_id"] == "1"


