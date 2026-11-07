"""Renewal review inputs: Smartlead tags, the v2_verdicts and blacklist_checks readers (shared
Mongo contract, 7 Nov 2026), Zapmail bill notes. Fakes only, no network."""
from datetime import date
from types import SimpleNamespace

import placement_report as pr
import renewal_review as cli
from smartlead import renewal_review as rr

T = date(2026, 11, 9)


class FakeCol:
    def __init__(self, docs=(), fail=None):
        self.docs, self.fail, self.queries = list(docs), fail, []

    def find(self, query, fields=None):
        self.queries.append(query)
        if self.fail:
            raise self.fail
        return iter(self.docs)


def _use(monkeypatch, **cols):
    asked = []

    def collection(name):
        asked.append(name)
        return cols[name]
    monkeypatch.setattr(cli, "_collection", collection)
    return asked


def test_smartlead_facts_keep_the_tags_email_accounts_returns(monkeypatch):
    def get(url, key, **params):
        assert url.endswith("/email-accounts/")
        return [{"from_email": "A@x.com", "client_id": None, "is_smtp_success": True, "is_imap_success": True,
                 "warmup_details": {"warmup_reputation": "100%"},
                 "tags": [{"tag_name": "DO NOT USE - Google spam (Nov 2026)"}, {"name": "Melior"}, "plain", {}, None]},
                {"from_email": "b@x.com", "warmup_details": {}}]

    class NoTests:
        def json(self):
            return []
    monkeypatch.setattr(cli, "_get", get)
    monkeypatch.setattr(cli.httpx, "post", lambda *a, **k: NoTests())
    monkeypatch.setattr(cli, "discover_accounts", lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(pr, "attachments", lambda key, sending_days=None: {})
    errors = []
    facts = cli.smartlead_facts(errors)
    assert errors == []
    assert facts["a@x.com"]["tags"] == ["DO NOT USE - Google spam (Nov 2026)", "Melior", "plain"]
    assert facts["b@x.com"]["tags"] == []
    # the tag reaches the verdict: a person's DO NOT USE is DROP
    assert rr.judge_inbox("a@x.com", facts["a@x.com"], None, T, True, [], {})[0] == "RETIRE"


def _smartlead(monkeypatch, classic=None, campaigns=None, accounts=None):
    """Fake Smartlead: /email-accounts reads; the classic SmartDelivery list and the campaign
    lists raise ``classic`` / ``campaigns`` when given."""
    def get(url, key, **params):
        if url.endswith("/email-accounts/"):
            if accounts:
                raise accounts
            return [{"from_email": "a@x.com", "is_smtp_success": True, "is_imap_success": True,
                     "warmup_details": {"warmup_reputation": "100%"}, "tags": [{"tag_name": "Melior"}]}]
        raise AssertionError(url)

    class Tests:
        def json(self):
            return []

    def post(*a, **k):
        if classic:
            raise classic
        return Tests()

    def attachments(key, sending_days=None):
        if campaigns:
            raise campaigns
        return {"a@x.com": {"campaigns": ["C1"], "sending": ["C1"]}}
    monkeypatch.setattr(cli, "_get", get)
    monkeypatch.setattr(cli.httpx, "post", post)
    monkeypatch.setattr(cli, "discover_accounts", lambda: [SimpleNamespace(name="PRECISE_LEADS", api_key="k")])
    monkeypatch.setattr(pr, "attachments", attachments)


def test_a_classic_read_failure_is_its_own_note_only_error(monkeypatch):
    """Review R3: SmartDelivery classic tests failing is not "could not read Smartlead"."""
    _smartlead(monkeypatch, classic=RuntimeError("502 Bad Gateway"))
    errors = []
    facts = cli.smartlead_facts(errors)
    assert errors == ["classic tests PRECISE_LEADS: 502 Bad Gateway"]
    assert facts["a@x.com"]["tags"] == ["Melior"] and facts["a@x.com"]["campaigns"] == ["C1"]
    assert rr.decision_errors(errors) == []
    assert rr.read_warning(errors, 7, True) == (":warning: Could not read classic tests, so classic early "
                                                "warnings are missing (details in the thread)")


def test_a_campaign_list_failure_keeps_the_classic_tests(monkeypatch):
    _smartlead(monkeypatch, campaigns=RuntimeError("timed out"))
    errors = []
    facts = cli.smartlead_facts(errors)
    assert errors == ["campaign lists PRECISE_LEADS: timed out"] and facts["a@x.com"]["campaigns"] == []
    assert rr.decision_errors(errors) == []


def test_an_inbox_list_failure_is_still_a_smartlead_error(monkeypatch):
    _smartlead(monkeypatch, accounts=RuntimeError("429"), classic=AssertionError("not reached"))
    errors = []
    assert cli.smartlead_facts(errors) == {} and errors == ["Smartlead PRECISE_LEADS: 429"]
    assert rr.decision_errors(errors) == errors


def test_auto_tag_matches_the_v2_job():
    import v2_weekly
    assert rr.AUTO_DNU_TAG == v2_weekly.TAG


def test_v2_results_by_inbox_lowercased_and_bounded(monkeypatch):
    col = FakeCol([{"email": "A@X.com", "night": "2026-11-02", "verdict": "PASS"},
                   {"email": "a@x.com", "night": "2026-10-26", "verdict": "FAIL"},
                   {"night": "2026-11-02", "verdict": "FAIL"},                       # no email: skipped
                   None])
    _use(monkeypatch, v2_verdicts=col)
    errors = []
    out = cli.v2_results({"x.com"}, {"a@x.com"}, T, errors)
    assert errors == [] and list(out) == ["a@x.com"] and len(out["a@x.com"]) == 2
    q = col.queries[0]
    assert q["night"] == {"$gte": "2026-07-12"}                                       # 120 days back
    assert {"domain": {"$in": ["x.com"]}} in q["$or"] and {"email": {"$in": ["a@x.com"]}} in q["$or"]


def test_v2_results_unreadable_is_none_with_an_error(monkeypatch):
    _use(monkeypatch, v2_verdicts=FakeCol(fail=RuntimeError("server selection timed out")))
    errors = []
    assert cli.v2_results({"x.com"}, set(), T, errors) is None
    assert errors == ["v2 results (Mongo v2_verdicts): server selection timed out"]


def test_no_mongo_uri_is_a_read_error_not_an_empty_result(monkeypatch):
    monkeypatch.delenv("MONGO_URI", raising=False)
    errors = []
    assert cli.dbl_listings({"x.com"}, errors) is None and cli.v2_results({"x.com"}, set(), T, errors) is None
    assert errors == ["blocklist checks (Mongo blacklist_checks): MONGO_URI is not set",
                      "v2 results (Mongo v2_verdicts): MONGO_URI is not set"]


def test_no_inboxes_means_no_mongo_call(monkeypatch):
    asked = _use(monkeypatch)
    assert cli.v2_results(set(), set(), T, []) == {} and cli.dbl_listings(set(), []) == {}
    assert asked == []


def test_dbl_listings_newest_check_per_domain(monkeypatch):
    col = FakeCol([{"domain": "x.com", "date": "2026-10-26", "listed_on": ["Spamhaus DBL (127.0.1.2)"]},
                   {"domain": "X.com", "date": "2026-11-02", "listed_on": []},           # delisted since
                   {"domain": "y.com", "date": "2026-11-02", "listed_on": ["Spamhaus DBL (127.0.1.2)"],
                    "clients": ["PRECISE_LEADS"], "unknown": []},                         # extra fields are fine
                   {"domain": "z.com", "date": "2026-11-02", "listed_on": "bad shape"},
                   {"date": "2026-11-02"}])
    _use(monkeypatch, blacklist_checks=col)
    out = cli.dbl_listings({"x.com", "y.com", "z.com"}, [])
    assert out == {"x.com": {"date": "2026-11-02", "listed_on": [], "unknown_on": None},
                   "y.com": {"date": "2026-11-02", "listed_on": ["Spamhaus DBL (127.0.1.2)"], "unknown_on": None},
                   "z.com": {"date": None, "listed_on": [], "unknown_on": "2026-11-02"}}   # bad shape: never clean
    assert rr.is_dbl(out["y.com"]["listed_on"]) and not rr.is_dbl(out["x.com"]["listed_on"])
    assert not rr.is_dbl(["SURBL (ABUSE)", "URIBL (127.0.0.2)"]) and not rr.is_dbl("Spamhaus DBL")
    assert col.queries == [{"domain": {"$in": ["x.com", "y.com", "z.com"]}}]


PASS_9 = [{"email": "a@x.com", "night": "2026-11-09", "verdict": "PASS", "weekly": True}]
LISTED_26 = {"domain": "x.com", "date": "2026-10-26", "listed_on": ["Spamhaus DBL (127.0.1.2)"],
             "lookup_errors": [], "inconclusive_zones": []}


def _dbl_verdict(monkeypatch, *rows):
    _use(monkeypatch, blacklist_checks=FakeCol(rows))
    dbl = cli.dbl_listings({"x.com"}, [])["x.com"]
    facts = {"in_smartlead": True, "connected": True, "reputation": "100%", "tags": []}
    return rr.judge_inbox("a@x.com", facts, None, T, True, PASS_9, dbl)


def test_unread_dbl_in_the_newest_check_is_never_clean(monkeypatch):
    """Review R2 / audit BLOCKLIST-4: blacklist_monitor puts "Spamhaus DBL" in inconclusive_zones
    (control probe failed) or lookup_errors (lookup failed). That is unknown, not clean."""
    inconclusive_2 = {"domain": "x.com", "date": "2026-11-02", "listed_on": [], "lookup_errors": [],
                      "inconclusive_zones": ["Spamhaus DBL"]}
    assert _dbl_verdict(monkeypatch, LISTED_26, inconclusive_2) == (
        "CHECK", "Spamhaus DBL could not be read in the check of 2 Nov; the check of 26 Oct listed the domain")
    errored_2 = {**inconclusive_2, "lookup_errors": ["Spamhaus DBL"], "inconclusive_zones": []}
    assert _dbl_verdict(monkeypatch, errored_2, LISTED_26)[1].startswith("Spamhaus DBL could not be read in the "
                                                                          "check of 2 Nov; the check of 26 Oct")
    # no check ever read DBL: CHECK
    assert _dbl_verdict(monkeypatch, inconclusive_2) == (
        "CHECK", "Spamhaus DBL could not be read in the check of 2 Nov, and no earlier check read it")
    # the newest check that could read DBL found it clean: nothing stops a keep
    clean_26 = {**LISTED_26, "listed_on": []}
    assert _dbl_verdict(monkeypatch, clean_26, inconclusive_2) == ("KEEP", "passed v2 on 9 Nov")
    # only another list unread (SURBL): DBL was read, clean
    surbl_2 = {**inconclusive_2, "inconclusive_zones": ["SURBL"]}
    assert _dbl_verdict(monkeypatch, LISTED_26, surbl_2) == ("KEEP", "passed v2 on 9 Nov")
    # a listing seen even over an unsure path counts; rows saved before these fields existed still read
    doh_listed = {**inconclusive_2, "listed_on": ["Spamhaus DBL (127.0.1.2)"]}
    assert _dbl_verdict(monkeypatch, doh_listed) == ("CHECK", "domain on Spamhaus DBL (check of 2 Nov)")
    assert _dbl_verdict(monkeypatch, {"domain": "x.com", "date": "2026-11-02", "listed_on": []}) == \
        ("KEEP", "passed v2 on 9 Nov")


def test_dbl_status_is_pure_and_newest_first():
    assert rr.dbl_status([]) == {}
    rows = [{"date": "2026-11-02", "listed_on": [], "inconclusive_zones": ["Spamhaus DBL"]},
            {"date": "2026-10-19", "listed_on": []}, LISTED_26, None]
    assert rr.dbl_status(rows) == {"date": "2026-10-26", "listed_on": ["Spamhaus DBL (127.0.1.2)"],
                                   "unknown_on": "2026-11-02"}
    assert rr.dbl_reason({"date": "2026-11-02", "listed_on": [], "unknown_on": None}) == ""


def test_dbl_unreadable_is_none_with_an_error(monkeypatch):
    _use(monkeypatch, blacklist_checks=FakeCol(fail=OSError("refused")))
    errors = []
    assert cli.dbl_listings({"x.com"}, errors) is None
    assert errors == ["blocklist checks (Mongo blacklist_checks): refused"]


def test_targets_include_smartlead_inboxes_on_bill_domains():
    bills = [{"inboxes": ["Old@grow.com"]}, {"inboxes": [], "domains": ["Other.com"]}]
    facts = {"new@grow.com": {}, "x@other.com": {}, "y@unrelated.com": {}}
    domains, emails = cli._targets(bills, facts)
    assert domains == {"grow.com", "other.com"}
    assert emails == {"old@grow.com", "new@grow.com", "x@other.com"}


def test_zapmail_bills_name_the_source_and_keep_both_notes(monkeypatch):
    import smartlead.zapmail_fleet as zf
    soon = date.today().isoformat()

    async def billing(days, errors):
        errors.append("PRECISE_LEADS/GOOGLE: 502")
        return [{"bills_on": soon, "mailboxes": 12, "provider": "MICROSOFT", "kind": "pre-warmed", "price": 84,
                 "subscription_id": "s1", "account": "PRECISE_LEADS", "payment_failure": "card declined"}]
    monkeypatch.setattr(zf, "subscriptions_billing", billing)
    errors = []
    bills = cli.zapmail_bills(7, errors)
    assert errors == ["Zapmail PRECISE_LEADS/GOOGLE: 502"]
    assert bills[0]["inboxes"] == [] and bills[0]["label"] == "12 Outlook pre-warmed"
    assert bills[0]["note"].startswith("payment failed: card declined · pre-warmed slots:")
    r = rr.review_bill(bills[0], {}, T, v2={}, dbl={})
    assert r["reviewed"] is False and r["rows"] == []
