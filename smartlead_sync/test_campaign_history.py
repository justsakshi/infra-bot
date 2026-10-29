"""Campaign History: rows written per day, and the change between two days. No network."""
from smartlead import campaign_history as ch


def _row(client="BettrData", channel="Email", platform="Smartlead", name="Campaign A",
         cid="1", status="ACTIVE", launch="2026-10-27", **life):
    base = {"life_sent": None, "life_bounced": None, "life_invites": None,
            "life_accepted": None, "life_replies": None, "life_positive": None}
    base.update({f"life_{k}": v for k, v in life.items()})
    return {"client": client, "channel": channel, "platform": platform, "campaign": name,
            "campaign_id": cid, "status": status, "launch_date": launch,
            "total_leads": 100, **base}


def test_history_rows_skip_totals_and_mark_missing_figures():
    rows = [_row(sent=284, bounced=1, replies=5, positive=2),
            {"client": "BettrData", "campaign": "Total — BettrData"},   # no channel
            _row(channel="LinkedIn", platform="Expandi", name="LinkedIn A", cid="9",
                 invites=12, accepted=0, replies=0, positive=0)]
    out = ch.history_rows(rows, "2026-10-29")
    assert len(out) == 2
    email = out[0]
    assert email[:3] == ["2026-10-29", "BettrData", "Email"]
    assert email[8:14] == [284, 1, "-", "-", 5, 2]          # no invites on email
    linkedin = out[1]
    assert linkedin[8:14] == ["-", "-", 12, 0, 0, 0]        # a real 0 stays 0
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


def test_baseline_is_friday_for_a_monday_post():
    days = ["2026-10-29", "2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02"]
    assert ch.baseline_day(days, "2026-11-02") == "2026-10-30"      # Mon -> Fri


def test_baseline_is_yesterday_midweek_or_the_latest_before_it():
    assert ch.baseline_day(["2026-10-29", "2026-10-30"], "2026-10-30") == "2026-10-29"
    # yesterday missing: the latest earlier day
    assert ch.baseline_day(["2026-10-27", "2026-10-30"], "2026-10-30") == "2026-10-27"
    # history started on Sunday, Monday post: the earliest day there is
    assert ch.baseline_day(["2026-11-01", "2026-11-02"], "2026-11-02") == "2026-11-01"
    assert ch.baseline_day(["2026-10-29"], "2026-10-29") is None


def _hist(day, **kw):
    r = dict(zip(ch.FIELDS, [day, "BettrData", "Email", "Smartlead", "Campaign A", "ACTIVE",
                             "2026-10-20", 100, None, None, None, None, None, None, "1"]))
    r.update(kw)
    return r


def test_change_is_today_minus_base():
    h = [_hist("2026-10-29", sent=100, replies=2, positive=1, bounced=0),
         _hist("2026-10-30", sent=265, replies=5, positive=1, bounced=1)]
    (c,) = ch.campaign_changes(h, "2026-10-30", "2026-10-29")
    assert c["change"] == {"sent": 165, "replies": 3, "positive": 0, "bounced": 1,
                           "invites": None, "accepted": None}
    assert c["totals"]["sent"] == 265 and c["running"] is True


def test_new_campaign_starts_from_zero_old_unseen_one_is_unknown():
    h = [_hist("2026-10-30", campaign_id="2", campaign="New", launch_date="2026-10-29", sent=40),
         _hist("2026-10-30", campaign_id="3", campaign="Resumed", launch_date="2026-09-01", sent=900)]
    new, resumed = ch.campaign_changes(h, "2026-10-30", "2026-10-29")
    assert new["change"]["sent"] == 40
    assert resumed["change"] is None          # its 900 must not count as one day's sends


def test_a_counter_that_goes_down_shows_zero():
    h = [_hist("2026-10-29", replies=5), _hist("2026-10-30", replies=4)]
    (c,) = ch.campaign_changes(h, "2026-10-30", "2026-10-29")
    assert c["change"]["replies"] == 0


class _FakeWS:
    def __init__(self, values):
        self.values = values
        self.deleted, self.appended, self.frozen = [], [], False

    def col_values(self, n):
        return [r[0] for r in self.values]

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
