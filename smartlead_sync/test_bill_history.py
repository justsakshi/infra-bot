"""Bill history: every renewal / failed payment / cancellation is kept, so a
domain shows which bill it renewed on and for how much (2026-10-13). No network."""
import json
import sys
from datetime import date

from smartlead import billing_watch as bw

T = date(2026, 10, 13)


def z(bills_on, **kw):
    r = {"provider": "Zapmail", "id": "sub_21", "label": "21 Google inboxes (PRECISE_LEADS)", "bills_on": bills_on,
         "status": "ACTIVE", "payment_failure": None, "inboxes": 21, "clients": ["Bettrdata"], "price": 68.25,
         "domains": ["bettrdatasgroup.com", "dataingesthq.com"]}
    r.update(kw)
    return r


def test_renewal_is_recorded_with_amount_domains_and_next_bill():
    prev = {"Zapmail:sub_21": z("2026-10-08")}
    events = bw.compare(prev, [z("2026-11-08")], T)
    recs = bw.history_records(events, [z("2026-11-08")], T)
    assert recs == [{"at": "2026-10-13", "key": "Zapmail:sub_21", "provider": "Zapmail", "id": "sub_21",
                     "event": "renewed", "label": "21 Google inboxes (PRECISE_LEADS)", "price": 68.25,
                     "next_bill": "2026-11-08", "domains": ["bettrdatasgroup.com", "dataingesthq.com"],
                     "inboxes": 21, "clients": ["Bettrdata"], "detail": "renewed — next bill 2026-11-08"}]


def test_failed_payment_and_scaledmail_bill_day_are_recorded_too():
    prev = {"Zapmail:sub_21": z("2026-10-08")}
    sm_prev = {"provider": "ScaledMail", "id": "rec1", "label": "30 × Google", "bills_on": "2026-10-13", "status": "Active",
               "inboxes": 30, "clients": ["Melior"], "price": 105.0, "domains": ["meliorbuild.com"]}
    prev["ScaledMail:rec1"] = {**sm_prev}
    now = [z("2026-10-08", payment_failure="card declined"), sm_prev]
    events = bw.compare(prev, now, T)
    kinds = sorted(r["event"] for r in bw.history_records(events, now, T))
    assert kinds == ["bill_day", "not_renewed", "payment"]      # 8 Oct passed without moving: not renewed too


def test_history_lines_for_a_domain_newest_first():
    recs = [{"at": "2026-09-08", "provider": "Zapmail", "event": "renewed", "label": "21 Google inboxes (PRECISE_LEADS)",
             "price": 68.25, "next_bill": "2026-10-08", "domains": ["a.com"]},
            {"at": "2026-10-08", "provider": "Zapmail", "event": "payment", "label": "21 Google inboxes (PRECISE_LEADS)",
             "price": 68.25, "next_bill": "2026-10-08", "domains": ["a.com"], "detail": "payment failed: card declined"}]
    assert bw.history_lines(recs) == [
        "8 Oct · payment failed: card declined · Zapmail · 21 Google inboxes (PRECISE_LEADS) · $68.25",
        "8 Sep · renewed · Zapmail · 21 Google inboxes (PRECISE_LEADS) · $68.25 · next bill 8 Oct"]


class FakeCol:
    def __init__(self, doc=None): self.doc, self.inserted, self.found = doc, [], []
    def find_one(self, q): return self.doc
    def update_one(self, q, upd, upsert=False): self.doc = {"rows": upd["$set"]["rows"]}
    def insert_many(self, docs): self.inserted += [dict(d) for d in docs]
    def find(self, q, proj=None):
        return FakeCursor([d for d in self.found if q["domains"] in d["domains"]])


class FakeCursor(list):
    def sort(self, *a): return FakeCursor(sorted(self, key=lambda d: d["at"], reverse=True))
    def limit(self, n): return FakeCursor(self[:n])


def test_watch_run_appends_history(monkeypatch):
    import billing_watch as cli
    state = FakeCol({"rows": {"Zapmail:sub_21": z("2026-10-08")}})
    hist = FakeCol()
    monkeypatch.setattr(cli, "collect", lambda errors: [z("2026-11-08")])
    monkeypatch.setattr(cli, "_col", lambda: state)
    monkeypatch.setattr(cli, "_history_col", lambda: hist)
    monkeypatch.setattr(sys, "argv", ["billing_watch.py", "--json"])
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    assert cli.main() == 0
    assert [h["event"] for h in hist.inserted] == ["renewed"] and hist.inserted[0]["price"] == 68.25


def test_history_lookup_for_a_domain(monkeypatch, capsys):
    import billing_watch as cli
    hist = FakeCol()
    hist.found = [{"at": "2026-10-08", "provider": "Zapmail", "event": "renewed", "label": "21 Google inboxes",
                   "price": 68.25, "next_bill": "2026-11-08", "domains": ["a.com"]}]
    monkeypatch.setattr(cli, "_history_col", lambda: hist)
    monkeypatch.setattr(sys, "argv", ["billing_watch.py", "--history", "a.com", "--json"])
    assert cli.main() == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out == {"domain": "a.com", "lines": ["8 Oct · renewed · Zapmail · 21 Google inboxes · $68.25 · next bill 8 Nov"]}
