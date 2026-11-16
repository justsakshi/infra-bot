"""smartlead/v2_api.py: the Smart Delivery v2 API read into the CSV's rows. A fake HTTP get; no network."""
from datetime import date

import pytest

from smartlead import v2_api
from smartlead import v2_results as v2

G, M = "GOOGLE_WORKSPACE", "MICROSOFT_365"
SENT = "2026-11-13T19:22:52.037Z"


def email(mb, provider=G, placement="INBOX", sent=SENT, bounce=None):
    return {"senderEmail": mb, "provider": provider, "placement": placement, "sentAt": sent, "bounce": bounce,
            "auth": {"spf": "pass"}, "tab": None}


class Pages:
    """A fake get: each path answers with its items, ``size`` a page, cursors '1', '2', ..."""

    def __init__(self, items: dict, size=50):
        self.items, self.size, self.calls = items, size, []

    def __call__(self, path, params):
        self.calls.append((path, dict(params)))
        if path not in self.items:
            return 404, None
        got = self.items[path]
        if isinstance(got, (int, Exception)):
            if isinstance(got, Exception):
                raise got
            return got, None
        n = int(params.get("cursor") or 0)
        page = got[n * self.size:(n + 1) * self.size]
        return 200, {"items": page, "nextCursor": str(n + 1) if (n + 1) * self.size < len(got) else None}


# --- rows like the CSV -------------------------------------------------------------------

def test_placements_map_to_the_csv_landed_values():
    assert [v2_api.landed({"placement": p}) for p in ("INBOX", "SPAM", "TAB", "MISSING", "NOT_SENT", "BOUNCED")] == \
        ["Inbox", "Spam", "Inbox (tab)", "Never arrived", "NOT_SENT", "BOUNCED"]
    assert v2_api.landed({"placement": "MISSING", "bounce": {"type": "hard"}}) == "BOUNCED"
    assert v2_api.landed({"placement": "INBOX", "bounce": {"type": "x"}}) == "Inbox"      # it arrived: not a bounce
    assert v2_api.landed({"placement": "UNREADABLE"}) == "UNREADABLE"                    # unknown: never final
    assert "UNREADABLE" not in v2.FINAL and "Never arrived" not in v2.FINAL


def test_rows_are_what_parse_makes_of_the_same_csv():
    emails = [email("A@Example.com"), email("a@example.com", M, "SPAM"), email("b@example.com", G, "MISSING"),
              email("b@example.com", M, "NOT_SENT", sent=None), {"senderEmail": "", "placement": "INBOX"}]
    rows = v2_api.rows(emails)
    csv_text = ("Mailbox,Provider,Landed,Sent at,Send status\n"
                f"a@example.com,{G},Inbox,{SENT},SENT\n"
                f"a@example.com,{M},Spam,{SENT},SENT\n"
                f"b@example.com,{G},Never arrived,{SENT},SENT\n"
                f"b@example.com,{M},NOT_SENT,,SKIPPED\n")
    assert rows == v2.parse(csv_text)
    assert v2.parse(v2_api.to_csv(rows)) == rows and v2.is_v2_csv(v2_api.to_csv(rows))
    assert v2.night_of(rows) == "2026-11-14" and v2.pending_seeds(rows) == 1


def test_a_run_is_named_like_its_csv_download():
    assert v2_api.csv_name("pl weekly spam test v2 synced 2", 1) == "pl-weekly-spam-test-v2-synced-2-run-1.csv"
    assert v2_api.csv_name("Melior spam test v2 #2weekly", 1) == "melior-spam-test-v2-2weekly-run-1.csv"
    assert v2_api.csv_name("pl infra audit 1/3 (retest)", 3) == "pl-infra-audit-1-3-retest-run-3.csv"
    assert v2_api.run_key("PL-Weekly-run-1 (2).csv") == v2_api.run_key("pl-weekly-run-1.csv") == "pl-weekly-run-1"


def test_a_copy_check_run_is_known_by_its_name():
    name = v2_api.csv_name("PL copy check", 1)
    assert v2.is_copy_check_file(name, ["a@example.com"], {})
    assert not v2.is_copy_check_file(v2_api.csv_name("pl weekly spam test", 1), ["a@example.com"], {})


# --- reading the API ------------------------------------------------------------------------

def test_every_page_is_read():
    emails = [email(f"m{i}@example.com") for i in range(120)]
    get = Pages({"/runs/r1/emails": emails})
    assert len(v2_api.Client("KEY", get=get).emails("r1")) == 120
    assert [p.get("cursor") for _, p in get.calls] == [None, "1", "2"]
    assert all("api_key" not in p for _, p in get.calls)          # the client adds the key itself, never a fake


def test_a_cursor_that_repeats_stops_the_listing():
    calls = []

    def loop(path, params):
        calls.append(params)
        return 200, {"items": [{"id": "t"}], "nextCursor": "same"}
    assert v2_api.Client("KEY", get=loop).tests() == [{"id": "t"}, {"id": "t"}] and len(calls) == 2


def test_an_error_never_carries_the_key_or_the_url():
    secret = "https://sl-sd-api.smartlead.ai/x?api_key=SECRET123"
    with pytest.raises(v2_api.V2ApiError) as e:
        v2_api.Client("SECRET123", get=Pages({"/tests": RuntimeError(secret)})).tests()
    assert "SECRET" not in str(e.value) and "SECRET" not in repr(e.value) and e.value.__suppress_context__
    assert v2_api.describe(e.value) == "tests: RuntimeError"
    with pytest.raises(v2_api.V2ApiError) as e:
        v2_api.Client("SECRET123", get=Pages({"/tests": 401})).tests()
    assert e.value.status == 401 and v2_api.describe(e.value) == "tests (HTTP 401)"
    assert v2_api.describe(ValueError(secret)) == "ValueError"
    with pytest.raises(v2_api.V2ApiError):
        v2_api.Client("K", get=lambda p, q: (200, {"data": []})).tests()       # not the shape we know


def test_completed_runs_reads_finished_runs_since_a_date_and_skips_what_it_is_told():
    tests = [{"id": "t1", "name": "pl weekly", "status": "ACTIVE"}, {"id": "t2", "name": "broken", "status": "ACTIVE"},
             {"id": "t3", "name": "old one", "status": "STOPPED"}]
    get = Pages({"/tests": tests,
                 "/tests/t1/history": [{"runId": "r1", "runNo": 1, "completedAt": "2026-11-13T19:30:00.000Z"},
                                       {"runId": "r2", "runNo": 2, "completedAt": None},          # still running
                                       {"runId": "r3", "runNo": 3, "completedAt": "2026-11-20T19:30:00.000Z"}],
                 "/tests/t2/history": 500,
                 "/tests/t3/history": [{"runId": "r9", "runNo": 1, "completedAt": "2026-11-08T10:00:00.000Z"}],
                 "/runs/r1/emails": [email("a@example.com")], "/runs/r3/emails": 502})
    runs, errors = v2_api.completed_runs(v2_api.Client("K", get=get), date(2026, 11, 14))
    assert [(r["run_id"], r["name"], len(r["rows"])) for r in runs] == [("r1", "pl-weekly-run-1.csv", 1)]
    assert errors == ["'pl weekly' run 3: run emails (HTTP 502)", "'broken': test history (HTTP 500)"]
    assert ("/runs/r9/emails", {}) not in get.calls                # before the date: never fetched
    get.calls.clear()
    get.items.update({f"/tests/x{n}/history": 500 for n in range(5)})
    get.items["/tests"] = [{"id": f"x{n}", "name": f"t{n}"} for n in range(5)]
    runs, errors = v2_api.completed_runs(v2_api.Client("K", get=get), date(2026, 11, 14))
    assert len(errors) == 4 and errors[-1].startswith("stopped after 3 errors") and len(get.calls) == 4
    get.items["/tests"] = tests
    get.calls.clear()
    runs, _ = v2_api.completed_runs(v2_api.Client("K", get=get), date(2026, 11, 14), skip=lambda r: r["run_id"] == "r1")
    assert runs == [] and not any(p == "/runs/r1/emails" for p, _ in get.calls)


# --- rate limit (16 Nov 2026: HTTP 429 on the first live read) ----------------------------------

def test_a_rate_limited_call_waits_and_tries_again():
    calls, waits = [], []

    def get(path, params):
        calls.append(path)
        return (429, None) if len(calls) < 3 else (200, {"items": [{"id": "t1"}], "nextCursor": None})
    c = v2_api.Client("k", get=get, sleep=waits.append)
    assert c.tests() == [{"id": "t1"}] and len(calls) == 3
    assert waits == [v2_api.RATE_WAIT, 2 * v2_api.RATE_WAIT]


def test_a_call_still_rate_limited_after_the_retries_is_an_error():
    waits = []
    c = v2_api.Client("k", get=lambda path, params: (429, None), sleep=waits.append)
    with pytest.raises(v2_api.V2ApiError, match="429"):
        c.tests()
    assert len(waits) == v2_api.RATE_RETRIES
    other = []
    c = v2_api.Client("k", get=lambda path, params: (503, None), sleep=other.append)
    with pytest.raises(v2_api.V2ApiError, match="503"):
        c.tests()
    assert other == []                                                   # only a 429 is waited out



def test_waiting_out_429s_stops_at_the_run_budget():
    waits = []
    c = v2_api.Client("k", get=lambda path, params: (429, None), sleep=waits.append)
    for _ in range(20):
        with pytest.raises(v2_api.V2ApiError, match="429"):
            c.tests()
    assert sum(waits) <= v2_api.RATE_BUDGET and len(waits) < 40      # later calls fail at once


def test_a_429_waits_at_least_a_minute():
    # Smartlead support, 16 Nov 2026: wait a minute after a 429 before retrying
    assert v2_api.RATE_WAIT >= 60
    assert v2_api.RATE_WAIT * (2 ** v2_api.RATE_RETRIES - 1) <= v2_api.RATE_BUDGET   # one call's waits fit the budget


def test_one_tests_detail_comes_back_with_its_senders_and_campaign():
    def get(path, params):
        assert path == "/tests/t1"
        return 200, {"id": "t1", "campaignId": 7, "senders": [{"email": "A@x.com", "emailAccountId": 5}]}
    d = v2_api.Client("K", get=get).test("t1")
    assert d["campaignId"] == 7 and d["senders"][0]["emailAccountId"] == 5
    with pytest.raises(v2_api.V2ApiError):
        v2_api.Client("K", get=lambda p, q: (200, ["not", "a", "dict"])).test("t1")
