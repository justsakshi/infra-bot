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
