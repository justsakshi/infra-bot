"""billing_watch.py saves what it has told only after Slack accepted the post (audit 7 Nov 2026, A3):
a refused post leaves the state as it was, so the next run sends the same alert again. No network."""
import json
import sys

import pytest

import billing_watch as cli
from smartlead import notify


def z(bills_on, **kw):
    r = {"provider": "Zapmail", "id": "sub_1", "label": "3 Google inboxes (PRECISE_LEADS)", "bills_on": bills_on,
         "status": "ACTIVE", "payment_failure": None, "inboxes": 3, "clients": ["Precise Leads"], "price": 10.5,
         "domains": ["x.com"]}
    r.update(kw)
    return r


class Col:
    def __init__(self, doc=None):
        self.doc, self.writes, self.inserted = doc, 0, []

    def find_one(self, q):
        return self.doc

    def update_one(self, q, upd, upsert=False):
        self.writes += 1
        self.doc = {"rows": upd["$set"]["rows"]}

    def insert_many(self, docs):
        self.inserted += list(docs)


@pytest.fixture
def run(monkeypatch, capsys):
    def go(rows, state, hist, accepted):
        posts = []

        def post(token, channel, head, thread):
            posts.append((channel, head, thread))
            return "1.0" if accepted else None
        monkeypatch.setattr(cli, "collect", lambda errors: rows)
        monkeypatch.setattr(cli, "_col", lambda: state)
        monkeypatch.setattr(cli, "_history_col", lambda: hist)
        monkeypatch.setattr(notify, "post_with_thread", post)
        monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
        monkeypatch.delenv("RENEWAL_REVIEW_CHANNEL", raising=False)
        monkeypatch.delenv("ZAPMAIL_NOTIFY_CHANNEL", raising=False)
        monkeypatch.setattr(sys, "argv", ["billing_watch.py", "--json"])
        code = cli.main()
        return code, posts, json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return go


def test_a_refused_alert_keeps_the_old_state_and_is_sent_again(run):
    state, hist = Col({"rows": {"Zapmail:sub_1": z("2026-11-01")}}), Col()
    now = [z("2026-11-01", payment_failure="card declined")]
    code, posts, out = run(now, state, hist, accepted=False)
    assert code == 1 and posts and out["posted"] is False and out["saved"] is False
    assert state.writes == 0 and hist.inserted == []
    code, posts, out = run(now, state, hist, accepted=True)         # next run: the same alert again
    assert code == 0 and "payment failed: card declined" in "\n".join(posts[0][1])
    assert posts[0][0] == "C0AGVSUNEFP" and out["saved"] is True and state.writes == 1
    assert [h["event"] for h in hist.inserted] == ["payment", "not_renewed"]


def test_nothing_to_post_still_saves(run):
    state, hist = Col({"rows": {"Zapmail:sub_1": z("2026-11-01")}}), Col()
    code, posts, out = run([z("2026-12-01")], state, hist, accepted=False)     # renewed: quiet
    assert code == 0 and posts == [] and out["saved"] is True and state.writes == 1


# --- the daily digest (16 Nov 2026) ---------------------------------------------------------

class State:
    """The billing_watch state doc, keeping every field written ($set), like Mongo."""
    def __init__(self, doc=None):
        self.doc, self.writes = doc, 0

    def find_one(self, q):
        return self.doc

    def update_one(self, q, upd, upsert=False):
        self.writes += 1
        self.doc = {**(self.doc or {}), **upd["$set"]}

    def insert_many(self, docs):
        pass


@pytest.fixture
def digest_run(monkeypatch, capsys):
    from smartlead import digest as dg

    def go(rows, state, report_ok=True, errors=()):
        posts, reports = [], []
        monkeypatch.setenv("DIGEST_MODE", "on")
        monkeypatch.setattr(cli, "collect", lambda errs: errs.extend(errors) or rows)
        monkeypatch.setattr(cli, "_col", lambda: state)
        monkeypatch.setattr(cli, "_history_col", lambda: State())
        monkeypatch.setattr(notify, "post_with_thread", lambda *a: posts.append(a) or "1.0")
        monkeypatch.setattr(dg, "report", lambda src, asks, done, detail, db=None:
                            reports.append((src, asks, done, detail)) or report_ok)
        monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
        monkeypatch.setattr(sys, "argv", ["billing_watch.py", "--json"])
        code = cli.main()
        return code, posts, reports, json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return go


def test_with_the_digest_on_a_failed_payment_is_an_urgent_ask_until_it_is_paid(digest_run):
    state = State({"rows": {"Zapmail:sub_1": z("2026-11-01")}})
    failing = [z("2026-11-01", payment_failure="card declined", invoice_url="https://pay.x.com/inv_9")]
    code, posts, reports, out = digest_run(failing, state)
    assert code == 0 and posts == [] and out["digest"] is True and out["saved"] is True
    (src, asks, done, detail), = reports
    assert src == "billing-watch" and done == [] and "payment failed: card declined" in detail[0]
    # one bill, one urgent item: the failed payment (its "not renewed" twin stays in the state, not in the digest)
    assert [a.key for a in asks] == ["billing-watch:payment:Zapmail:sub_1:2026-11-01"]
    assert set(state.doc["open"]) == {"billing-watch:not_renewed:Zapmail:sub_1:2026-11-01",
                                      "billing-watch:payment:Zapmail:sub_1:2026-11-01"}
    pay = asks[0]
    assert pay.urgent and pay.kind == "ask"
    assert pay.text == "Payment failed (card declined): Zapmail, 3 Google inboxes (PRECISE_LEADS) for Precise Leads."
    # 17 Nov 2026: the bill date, amount, inboxes and domains travel with the ask
    assert pay.detail == ["Zapmail · 3 Google inboxes (PRECISE_LEADS) for Precise Leads: bill date 1 Nov, "
                          "$10.50, 3 inboxes, status ACTIVE", "Domains (1): x.com"]
    assert pay.step.startswith("Zapmail, Billing, pay the invoice (<https://pay.x.com/inv_9|open the invoice>)")
    # next run: no new event, but the bill is still unpaid, so the same asks are handed over again
    code, posts, reports, _ = digest_run(failing, state)
    assert [a.key for a in reports[0][1]] == [a.key for a in asks] and posts == []
    # paid and renewed: the asks clear
    code, posts, reports, _ = digest_run([z("2026-12-01")], state)
    assert reports[0][1] == [] and state.doc["open"] == {}


def test_a_new_subscription_is_urgent_and_a_scaledmail_bill_day_is_a_plain_ask_for_three_days(digest_run):
    from datetime import date, timedelta
    t = date.today()
    sm = {"provider": "ScaledMail", "id": "rec1", "label": "2 × Google", "bills_on": t.isoformat(), "status": "Active",
          "payment_failure": None, "inboxes": 2, "clients": [], "price": 7, "domains": [],
          "invoice_url": "https://app.scaledmail.com"}
    state = State({"rows": {"ScaledMail:rec1": {**sm, "bills_on": (t - timedelta(days=30)).isoformat()}}})
    new = z((t + timedelta(days=20)).isoformat(), id="sub_new")
    code, posts, reports, _ = digest_run([sm, new], state)
    asks = {a.key: a for a in reports[0][1]}
    assert asks["billing-watch:new:Zapmail:sub_new"].urgent
    assert asks["billing-watch:new:Zapmail:sub_new"].text.startswith("Bought outside the bot: Zapmail")
    day = asks[f"billing-watch:bill_day:ScaledMail:rec1:{t.isoformat()}"]
    assert not day.urgent and "3-D Secure" in day.step and posts == []
    for a in state.doc["open"].values():                 # four days on, nothing but time closes them
        a["opened"] = (t - timedelta(days=4)).isoformat()
    code, posts, reports, _ = digest_run([sm, new], state)
    assert reports[0][1] == []


def test_read_errors_are_broken_asks(digest_run):
    state = State({"rows": {"Zapmail:sub_1": z("2026-12-01")}})
    code, posts, reports, _ = digest_run([z("2026-12-01")], state, errors=["ScaledMail: 503"])
    (a,) = reports[0][1]
    assert a.kind == "broken" and a.key == "billing-watch:read:ScaledMail" and posts == [] and code == 0


def test_when_the_digest_cannot_be_written_the_old_alert_goes_out(digest_run):
    state = State({"rows": {"Zapmail:sub_1": z("2026-11-01")}})
    code, posts, reports, out = digest_run([z("2026-11-01", payment_failure="card declined")], state, report_ok=False)
    assert reports and len(posts) == 1 and "payment failed: card declined" in "\n".join(posts[0][2])
    assert code == 0 and out["posted"] is True and out["digest"] is False


def test_with_the_digest_off_it_posts_as_before_and_never_reports(run, monkeypatch):
    from smartlead import digest as dg
    monkeypatch.setattr(dg, "report", lambda *a, **k: pytest.fail("the digest is off"))
    state, hist = Col({"rows": {"Zapmail:sub_1": z("2026-11-01")}}), Col()
    code, posts, out = run([z("2026-11-01", payment_failure="card declined")], state, hist, accepted=True)
    assert code == 0 and len(posts) == 1 and out["posted"] is True and out["digest"] is False
