"""The renewal review says what it actually saw (t396, 24 Nov 2026).

The Daily Renewal Check of 24 Nov flagged three Google inboxes "don't renew: tagged DO NOT USE ... in Smartlead" and
its footer said the review "found it not delivering". The owner had to ask whether they were bad and whether last
week's test was checked: the flag carried the tag only. Now every reviewed inbox carries its evidence (newest weekly
test at its own provider, warm-up against the 90% good mark, warm-up spam, the All Inboxes answer), and the tracker's
flag reason says it in plain words, or says that there is none.

Fixtures are made-up example.com inboxes.  python -m pytest test_renewal_evidence.py -q"""
from datetime import date

from smartlead import renewal_review as rr

T = date(2026, 11, 24)
TAG = "DO NOT USE - pulled from roundtable by Manveen 16 Nov (t235)"
EMAIL = "ann@example.com"


def facts(**kw):
    base = {"in_smartlead": True, "connected": True, "reputation": "55%", "tags": [TAG], "type": "GMAIL",
            "tests": [], "campaigns": []}
    base.update(kw)
    return base


def v2(night="2026-11-21", verdict="FAIL", google=(0, 4), microsoft=(4, 4), weekly=True, kind="GMAIL"):
    return {"email": EMAIL, "domain": "example.com", "night": night, "verdict": verdict, "weekly": weekly,
            "type": kind, "google": {"inbox": google[0], "reported": google[1], "sent": google[1]},
            "microsoft": {"inbox": microsoft[0], "reported": microsoft[1], "sent": microsoft[1]}, "pulled_on": None}


DAILY = {"date": "2026-11-23", "rep": 55, "warm_sent_7d": 56, "warm_spam_7d": 25}
STATUS = {"busy_reason": "no_capacity,do_not_use", "provider": "Gmail", "group": "Recovering"}


# --- the pieces ----------------------------------------------------------------------------------------------------

def test_test_words_at_its_own_provider():
    assert rr.test_words([v2()], "GMAIL") == \
        "weekly test Sat 21 Nov: spam at Google 4 of 4 (Google seeds are a signal only for Google inboxes)"
    # an Outlook inbox: its Microsoft seeds only; the Google result is not its provider
    assert rr.test_words([v2(google=(4, 4), microsoft=(1, 4), kind="OUTLOOK")], "OUTLOOK") == \
        "weekly test Sat 21 Nov: spam at Microsoft 3 of 4"
    assert rr.test_words([v2(verdict="PASS", google=(0, 4), microsoft=(4, 4), kind="OUTLOOK")], "OUTLOOK") == \
        "weekly test Sat 21 Nov: inbox at Microsoft 4 of 4"
    # SMTP / unknown type: both providers
    assert rr.test_words([v2(google=(3, 4), microsoft=(4, 4), kind="")], "") == \
        "weekly test Sat 21 Nov: spam at Google 1 of 4, inbox at Microsoft 4 of 4"


def test_test_words_picks_the_newest_judged_result():
    older = v2(night="2026-11-14", verdict="PASS", google=(4, 4))
    one_seed = v2(night="2026-11-22", verdict="INCOMPLETE", google=(0, 1), weekly=False)
    assert rr.test_words([older, v2(), one_seed], "GMAIL").startswith("weekly test Sat 21 Nov:")
    assert rr.test_words([v2(night="2026-11-22", weekly=False, google=(4, 4))], "GMAIL") == \
        "one-off test Sun 22 Nov: inbox at Google 4 of 4 (Google seeds are a signal only for Google inboxes)"
    assert rr.test_words([one_seed], "GMAIL") == ""
    assert rr.test_words([], "GMAIL") == "" and rr.test_words(None, "GMAIL") == ""


def test_warmup_words():
    assert rr.warmup_words(facts(), DAILY) == \
        "warm-up 55%, under the 90% good mark; 25 of 56 warm-up emails in spam in 7 days"
    assert rr.warmup_words(facts(reputation="96%"), {"warm_sent_7d": 12, "warm_spam_7d": 3}) == \
        "warm-up 96%, at or above the 90% good mark"           # under 30 warm-up emails: too few to say
    assert rr.warmup_words(facts(reputation=None), DAILY).startswith("warm-up 55%")     # inbox_daily fills in
    assert rr.warmup_words(facts(warmup_by="Vendor"), DAILY) == "warm-up run by Vendor, not judged here"
    assert rr.warmup_words(facts(reputation=None), None) == ""


def test_all_inboxes_words_use_the_one_verdict():
    assert rr.all_inboxes_words(STATUS) == "All Inboxes: recovering"
    # no stored group: worked out from the Busy Reason by usable.team_group, no rule of its own
    assert rr.all_inboxes_words({"busy_reason": "failed_v2", "provider": "Outlook"}) == "All Inboxes: do not use"
    assert rr.all_inboxes_words({"busy_reason": "", "provider": "Outlook"}) == "All Inboxes: safe to send"
    assert rr.all_inboxes_words(None) == ""


# --- the row and the tracker flag ----------------------------------------------------------------------------------

def bill(*emails):
    return {"provider": "Zapmail", "label": "3 Google pre-warmed", "bills_on": "2026-11-27", "price": 14.25,
            "inboxes": list(emails) or [EMAIL]}


def test_tagged_drop_with_evidence():
    r = rr.review_bill(bill(), {EMAIL: facts()}, T, v2={EMAIL: [v2()]}, daily={EMAIL: DAILY},
                       status={EMAIL: STATUS})
    row = r["rows"][0]
    assert row["verdict"] == "RETIRE" and row["why"] == f'tagged "{TAG}" in Smartlead'      # the verdict is unchanged
    assert rr.flag_reason(row) == (
        f'tagged "{TAG}" in Smartlead. Seen: weekly test Sat 21 Nov: spam at Google 4 of 4 (Google seeds are a '
        "signal only for Google inboxes); warm-up 55%, under the 90% good mark; 25 of 56 warm-up emails in spam in "
        "7 days; All Inboxes: recovering")


def test_tagged_drop_without_evidence_says_so():
    r = rr.review_bill(bill(), {EMAIL: facts(reputation=None)}, T, v2={EMAIL: []})
    assert rr.flag_reason(r["rows"][0]) == f'tagged "{TAG}" in Smartlead. No test result; dropped because tagged.'


def test_no_test_but_other_evidence():
    r = rr.review_bill(bill(), {EMAIL: facts(reputation="96%")}, T, v2={EMAIL: []})
    assert rr.flag_reason(r["rows"][0]) == (f'tagged "{TAG}" in Smartlead. Seen: no test result; warm-up 96%, at or '
                                            "above the 90% good mark")


def test_two_weekly_fails_drop_carries_the_counts():
    f = facts(tags=[], type="OUTLOOK", reputation="93%")
    docs = [v2(night="2026-11-16", kind="OUTLOOK", microsoft=(0, 4)), v2(night="2026-11-23", kind="OUTLOOK",
                                                                        microsoft=(1, 4))]
    for d in docs:
        d["weekly"] = True
    row = rr.review_bill(bill(), {EMAIL: f}, T, v2={EMAIL: docs})["rows"][0]
    assert row["verdict"] == "RETIRE"
    assert rr.flag_reason(row).endswith(". Seen: weekly test Mon 23 Nov: spam at Microsoft 3 of 4; warm-up 93%, "
                                        "at or above the 90% good mark")


def test_unread_sources_add_nothing_and_change_no_verdict():
    """inbox_daily / inbox_status not read (None): the line says only what was read, and the verdict is the same."""
    a = rr.review_bill(bill(), {EMAIL: facts()}, T, v2={EMAIL: [v2()]})
    b = rr.review_bill(bill(), {EMAIL: facts()}, T, v2={EMAIL: [v2()]}, daily={EMAIL: DAILY}, status={EMAIL: STATUS})
    assert a["rows"][0]["verdict"] == b["rows"][0]["verdict"] and a["rows"][0]["why"] == b["rows"][0]["why"]
    assert "All Inboxes" not in rr.flag_reason(a["rows"][0]) and "warm-up emails" not in rr.flag_reason(a["rows"][0])


def test_evidence_reads_that_fail_are_logged_not_review_errors(monkeypatch, capsys):
    """No verdict, headline or digest ask depends on inbox_daily / inbox_status: a failed read only shortens the line."""
    import renewal_review as cli

    def boom(name):
        raise RuntimeError("no Mongo")
    monkeypatch.setattr(cli, "_collection", boom)
    assert cli.daily_readings({EMAIL}, T) == {} and cli.inbox_verdicts({EMAIL}) == {}
    out = capsys.readouterr().out
    assert "inbox_daily) not read: RuntimeError" in out and "inbox_status) not read: RuntimeError" in out


def test_flag_reason_is_capped_at_a_word():
    row = {"why": "x" * 50, "evidence": " ".join(["word"] * 200), "verdict": "CHECK"}
    out = rr.flag_reason(row)
    assert len(out) <= rr.FLAG_REASON_MAX and out.endswith("…") and " wor…" not in out


def test_tracker_gets_the_evidence(monkeypatch):
    import renewal_review as cli
    import smartlead.zapmail_asset_sync as zas

    class Assets:
        ops = []

        def bulk_write(self, ops, ordered):
            Assets.ops = ops
            return type("R", (), {"modified_count": len(ops)})()

    monkeypatch.setattr(zas, "_db", lambda: {"assets": Assets()})
    row = rr.review_bill(bill(), {EMAIL: facts()}, T, v2={EMAIL: [v2()]}, daily={EMAIL: DAILY},
                         status={EMAIL: STATUS})["rows"][0]
    assert cli.flag_tracker({"errors": [], "reviews": [{"rows": [row]}]}) == {"written": 1}
    reason = Assets.ops[0]._doc["$set"]["renewalFlagReason"]
    assert reason == rr.flag_reason(row) and "spam at Google 4 of 4" in reason
