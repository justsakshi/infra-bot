"""Billing watch + renewal review rules (no network).  python3 -m pytest test_billing_review.py -q"""
from datetime import date

from smartlead import billing_watch as bw
from smartlead import renewal_review as rr

T = date(2026, 10, 8)


def _z(id_, bills_on, status="ACTIVE", fail=None):
    return {"provider": "Zapmail", "id": id_, "label": "15 Outlook inboxes", "bills_on": bills_on,
            "status": status, "payment_failure": fail, "inboxes": 15, "clients": ["Bettrdata"]}


def _s(id_, bills_on, status="Active"):
    return {"provider": "ScaledMail", "id": id_, "label": "39 × Google", "bills_on": bills_on,
            "status": status, "payment_failure": None, "inboxes": 39, "clients": ["Precise Leads"]}


def state(*rows):
    return {bw.key_of(r): dict(r) for r in rows}


def ev(prev, now, today=T):
    return sorted((e["key"], e["event"]) for e in bw.compare(prev, now, today))


def test_first_run_is_silent():
    assert ev({}, [_z("a", "2026-10-08")]) == []


def test_renewed_vs_not_renewed():
    prev = state(_z("a", "2026-10-07"), _z("b", "2026-10-07"))
    assert ev(prev, [_z("a", "2026-11-07"), _z("b", "2026-10-07")]) == [("Zapmail:a", "renewed"), ("Zapmail:b", "not_renewed")]


def test_not_renewed_alerts_once():
    prev = state(_z("b", "2026-10-07"))
    events = bw.compare(prev, [_z("b", "2026-10-07")], T)
    saved = bw.remember(prev, [_z("b", "2026-10-07")], events, T)
    assert ev(saved, [_z("b", "2026-10-07")]) == []


def test_payment_failure_status_new_gone():
    prev = state(_z("a", "2026-10-08"), _z("gone", "2026-10-20"))
    now = [_z("a", "2026-10-08", fail="No payment method on file"), _z("c", "2026-11-01")]
    assert ev(prev, now) == [("Zapmail:a", "payment"), ("Zapmail:c", "new"), ("Zapmail:gone", "gone")]
    assert ev(state(_s("o", "2026-11-06")), [_s("o", "2026-11-06", status="Cancelled")]) == [("ScaledMail:o", "status")]


def test_scaledmail_bill_day_note_once():
    prev = state(_s("o", "2026-10-08"))
    e = bw.compare(prev, [_s("o", "2026-10-08")], T)
    assert [x["event"] for x in e] == ["bill_day"]
    saved = bw.remember(prev, [_s("o", "2026-10-08")], e, T)
    assert ev(saved, [_s("o", "2026-10-08")]) == []
    head, thread = bw.format_events(e)
    assert "check the card payment" in head[1] and "3-D Secure" in thread[0]


def _facts(pct=95.0, tested_on="2026-10-05", seeds=20, history=(), **kw):
    """Smartlead facts with a classic test list (newest first); pct -> spam count on ``seeds`` seeds."""
    tests = [{"date": tested_on, "seeds": seeds, "spam": round(seeds * (100 - pct) / 100), "by": {}}]
    tests += [{"date": d, "seeds": seeds, "spam": round(seeds * (100 - p) / 100), "by": {}} for d, p in history]
    base = {"in_smartlead": True, "connected": True, "reputation": "98%", "tests": tests, "campaigns": [], "tags": []}
    base.update(kw)
    return base


def _v2(*nights, email="a@x.com", pulled_on=None):
    """v2_verdicts rows (shared contract) for one inbox: ("2026-10-05", "PASS"), ..."""
    return [{"email": email, "domain": email.split("@")[1], "night": n, "verdict": v,
             "weekly": date.fromisoformat(n).weekday() == 0, "pulled_on": pulled_on, "history": False}
            for n, v in nights]


PASS_5 = ("2026-10-05", "PASS")       # T is Thu 8 Oct; 28 Sep and 5 Oct are the weekly (Monday) nights


def _judge(f, v2=(), dbl=None):
    """Verdict for a@x.com. v2=None means v2 could not be read; dbl None means not listed."""
    from smartlead import kill_rule as kr
    k = kr.judge_all({"a@x.com": f["tests"]}, T)["a@x.com"] if f else None
    return rr.judge_inbox("a@x.com", f, k, T, True, None if v2 is None else list(v2), dbl or {})[0]


DBL = {"date": "2026-10-05", "listed_on": ["Spamhaus DBL (127.0.1.2)"]}


def test_judge_inbox_rule_decision_4():
    # DROP only for two weekly v2 fails with no pass since, or a person's DO NOT USE tag
    assert _judge(_facts(), _v2(("2026-09-28", "FAIL"), ("2026-10-05", "FAIL"))) == "RETIRE"
    assert _judge(_facts(), _v2(("2026-09-28", "FAIL"), ("2026-10-01", "WATCH"), ("2026-10-05", "FAIL"))) == "RETIRE"
    assert _judge(_facts(tags=["DO NOT USE - Google spam (Nov 2026)"]), _v2(PASS_5)) == "RETIRE"
    assert _judge(_facts(tags=["do not use"]), None) == "RETIRE"            # a tag needs no v2 read
    # one fail is CHECK, never DROP; a pass in between resets the count
    assert _judge(_facts(), _v2(("2026-10-05", "FAIL"))) == "CHECK"
    assert _judge(_facts(), _v2(("2026-09-28", "FAIL"), ("2026-10-01", "PASS"), ("2026-10-05", "FAIL"))) == "CHECK"
    assert _judge(_facts(), _v2(("2026-09-30", "FAIL"), ("2026-10-02", "FAIL"))) == "CHECK"   # not weekly nights
    assert _judge(_facts(), _v2(("2026-09-28", "FAIL"), ("2026-10-05", "WATCH"))) == "CHECK"
    # the v2 job's own tag marks a pull after ONE fail: CHECK, not a person's DO NOT USE
    assert _judge(_facts(tags=["DO NOT USE - failed v2 (auto)"]), _v2(PASS_5)) == "CHECK"
    # a pull under 30 days old, or a DBL listing, is CHECK even after a pass
    assert _judge(_facts(), _v2(PASS_5, pulled_on="2026-09-20")) == "CHECK"
    assert _judge(_facts(), _v2(PASS_5, pulled_on="2026-09-01")) == "KEEP"   # 37 days ago
    assert _judge(_facts(), _v2(PASS_5), DBL) == "CHECK"
    assert _judge(_facts(), _v2(PASS_5), {"date": "2026-10-05", "listed_on": ["SURBL (ABUSE)"]}) == "KEEP"
    # no v2 result in the last 8 days: UNTESTED, never KEEP (classic results don't count)
    assert _judge(_facts(), _v2(("2026-09-28", "PASS"))) == "UNTESTED"      # 10 days old
    assert _judge(_facts(), _v2()) == "UNTESTED"
    assert _judge(_facts(), _v2(("2026-10-05", "INCOMPLETE"), ("2026-10-05", "NOT_SENT"))) == "UNTESTED"
    assert _judge(_facts(), _v2(("2026-10-05", "WATCH"))) == "CHECK"
    assert _judge(_facts(), _v2(PASS_5)) == "KEEP"
    # classic tests never decide: a dead classic result with a v2 pass is still KEEP
    assert _judge(_facts(pct=40.0), _v2(PASS_5)) == "KEEP"
    assert _judge(_facts(pct=60.0, history=[("2026-09-28", 65.0)]), _v2()) == "UNTESTED"
    # Smartlead problems stay CHECK; not in Smartlead is CHECK
    assert _judge(_facts(connected=False), _v2(PASS_5)) == "CHECK"
    assert _judge(_facts(reputation="55%"), _v2(PASS_5)) == "CHECK"
    assert _judge(None, _v2(PASS_5)) == "CHECK"


def test_old_weekly_fails_are_check_never_drop():
    """Review R1: two weekly fails only DROP when the newer one is in the last 8 days. An inbox
    that failed in August and dropped out of the test sheet may have been fixed or rested."""
    t = date(2026, 11, 10)                                     # a Tuesday
    judge = lambda rows, f=None: rr.judge_inbox("a@x.com", f or _facts(), None, t, True, rows, {})   # noqa: E731
    aug = _v2(("2026-08-03", "FAIL"), ("2026-08-10", "FAIL"))
    assert judge(aug) == ("CHECK", "failed v2 on the weekly tests of 3 Aug and 10 Aug, no v2 result since")
    # the newer fail 8 days old still drops; 9 days old is CHECK
    assert judge(_v2(("2026-10-26", "FAIL"), ("2026-11-02", "FAIL")))[0] == "RETIRE"
    assert rr.judge_inbox("a@x.com", _facts(), None, date(2026, 11, 11), True,
                          _v2(("2026-10-26", "FAIL"), ("2026-11-02", "FAIL")), {})[0] == "CHECK"
    # a later non-weekly result that is not a pass: still CHECK, and the reason stays true
    later = _v2(("2026-08-03", "FAIL"), ("2026-08-10", "FAIL"), ("2026-11-05", "WATCH"))
    assert judge(later) == ("CHECK", "failed v2 on the weekly tests of 3 Aug and 10 Aug, no pass since")
    older = _v2(("2026-08-03", "FAIL"), ("2026-08-10", "FAIL"), ("2026-08-12", "WATCH"))
    assert judge(older) == ("CHECK", "failed v2 on the weekly tests of 3 Aug and 10 Aug, no pass since; "
                                     "no v2 result in the last 8 days")
    # other reasons still join it; a person's tag still drops
    assert judge(aug, _facts(reputation="55%"))[1] == ("failed v2 on the weekly tests of 3 Aug and 10 Aug, "
                                                       "no v2 result since; warmup reputation 55%")
    assert judge(aug, _facts(tags=["do not use"]))[0] == "RETIRE"
    # on a bill: CHECK, no support message asking the provider to remove it
    r = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-11-12", "price": 3.0,
                        "inboxes": ["a@x.com"]}, {"a@x.com": _facts()}, t, v2={"a@x.com": aug}, dbl={})
    assert r["counts"]["CHECK"] == 1 and r["counts"]["RETIRE"] == 0 and r["support_message"] == ""


def test_unread_v2_or_blocklist_keeps_nothing():
    v, why = rr.judge_inbox("a@x.com", _facts(), None, T, True, None, {})
    assert (v, why) == ("CHECK", "v2 results could not be read, so not kept")
    v, why = rr.judge_inbox("a@x.com", _facts(), None, T, True, _v2(PASS_5), None)
    assert (v, why) == ("CHECK", "blocklist checks could not be read, so not kept")
    # the default for a caller that passes no v2 is "not read": never a silent KEEP
    assert rr.judge_inbox("a@x.com", _facts(), None, T)[0] == "CHECK"


def test_judge_inbox_reasons_are_plain_and_dated():
    why = lambda f, v2, dbl={}: rr.judge_inbox("a@x.com", f, None, T, True, v2, dbl)[1]   # noqa: E731
    assert why(_facts(), _v2(("2026-09-28", "FAIL"), ("2026-10-05", "FAIL"))) == \
        "failed v2 on the weekly tests of 28 Sep and 5 Oct"
    assert why(_facts(tags=["DO NOT USE - Google spam"]), []) == 'tagged "DO NOT USE - Google spam" in Smartlead'
    assert why(_facts(), _v2(("2026-10-05", "FAIL"))) == "failed v2 on 5 Oct, no pass since"
    assert why(_facts(), _v2(PASS_5), DBL) == "domain on Spamhaus DBL (check of 5 Oct)"
    assert why(_facts(), _v2(PASS_5, pulled_on="2026-09-20")) == \
        "taken out of campaigns on 20 Sep after v2, under 30 days ago"
    assert why(_facts(), _v2(("2026-09-21", "FAIL")), DBL) == \
        "failed v2 on 21 Sep, no pass since; domain on Spamhaus DBL (check of 5 Oct); no v2 result in the last 8 days"
    assert why(_facts(), []) == "no v2 result in the last 8 days"
    assert why(_facts(), _v2(PASS_5)) == "passed v2 on 5 Oct"


def test_malformed_v2_rows_are_skipped_not_fatal():
    rows = [{"email": "a@x.com", "night": None, "verdict": "FAIL"}, {"night": "garbage", "verdict": "FAIL"},
            {"night": "2026-10-05", "verdict": None}, {"night": "2026-10-05", "verdict": "pass", "weekly": "yes",
                                                      "pulled_on": "not a date"}]
    assert rr.judge_inbox("a@x.com", _facts(), None, T, True, rows, {}) == ("KEEP", "passed v2 on 5 Oct")


def test_review_bill_counts_savings_and_cost_per_passing():
    dnu = ["DO NOT USE - Google spam (Nov 2026)"]
    facts = {"a@x.com": _facts(tags=dnu), "b@x.com": _facts(tags=dnu), "c@x.com": _facts(tags=dnu),
             "d@y.com": _facts(), "e@y.com": _facts()}
    v2 = {"d@y.com": _v2(PASS_5, email="d@y.com")}
    r = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-10-10", "price": 13.0,
                        "inboxes": ["a@x.com", "b@x.com", "c@x.com", "d@y.com"]}, facts, T, v2=v2, dbl={})
    assert r["counts"] == {"KEEP": 1, "RETIRE": 3, "CHECK": 0, "UNTESTED": 0}
    assert r["retire_saves_monthly"] == 9.75 and r["passing"] == 1 and r["cost_per_passing"] == 13.0
    assert "saves $9.75/mo" in rr.format_review([r], 3)[0][1]
    assert all(row["why"].startswith("tagged") for row in r["rows"] if row["verdict"] == "RETIRE")
    unknown = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-10-10", "price": None,
                              "inboxes": ["d@y.com"]}, facts, T, v2=v2, dbl={})
    assert unknown["passing"] == 1 and unknown["cost_per_passing"] is None


def test_classic_kill_only_adds_a_watch_note():
    # x.com: 3 mailboxes at ~70% on 20 classic seeds each = a dead domain by the old kill rule
    facts = {"a@x.com": _facts(pct=70.0), "b@x.com": _facts(pct=70.0), "c@x.com": _facts(pct=70.0)}
    v2 = {e: _v2(PASS_5, email=e) for e in facts}
    r = rr.review_bill({"provider": "Zapmail", "label": "L", "bills_on": "2026-10-10", "price": 9.0,
                        "inboxes": list(facts)}, facts, T, v2=v2, dbl={})
    assert r["counts"]["KEEP"] == 3 and r["counts"]["RETIRE"] == 0
    assert all(row["watch"].startswith("classic test: whole domain:") for row in r["rows"])
    assert r["support_message"] == ""


def test_failed_payment_carries_the_invoice_link():
    prev = state(_z("a", "2026-10-08"))
    now = [dict(_z("a", "2026-10-08", fail="card declined"), invoice_url="https://invoice.stripe.com/i/x")]
    txt = "\n".join(bw.format_events(bw.compare(prev, now, T))[0])
    assert "<https://invoice.stripe.com/i/x|Pay / see invoice>" in txt


def test_unread_smartlead_is_never_a_retire():
    assert rr.judge_inbox("a@x.com", None, None, T, complete=False)[0] == "CHECK"
    r = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-10-08", "price": 7,
                        "inboxes": ["a@x.com", "b@x.com"]}, {}, T, complete=False, v2={}, dbl={})
    assert r["counts"]["RETIRE"] == 0 and r["retire_saves_monthly"] == 0


def test_support_message_names_bad_domains_and_keeps_good_ones():
    facts = {"a@bad.com": _facts(), "b@bad.com": _facts(), "c@good.com": _facts(),
             "d@tagged.com": _facts(tags=["DO NOT USE - Google spam (Nov 2026)"])}
    two = (("2026-09-28", "FAIL"), ("2026-10-05", "FAIL"))
    v2 = {"a@bad.com": _v2(*two, email="a@bad.com"), "b@bad.com": _v2(*two, email="b@bad.com"),
          "c@good.com": _v2(PASS_5, email="c@good.com")}
    r = rr.review_bill({"provider": "ScaledMail", "label": "39 × Google Inboxes", "bills_on": "2026-10-08", "price": 10.5,
                        "clients": ["Precise Leads"], "order_id": "rec3", "inboxes": list(facts)}, facts, T, v2=v2, dbl={})
    m = r["support_message"]
    assert "3 of its inboxes are not working for us" in m
    assert "bad.com (2 inboxes) — failed our inbox-placement tests of 28 Sep and 5 Oct" in m
    assert "tagged.com (1 inbox) — we no longer use them" in m and "not charge us" in m
    assert "should kill" not in m and "DO NOT USE" not in m
    assert "keep the rest" in m and "good.com" in m and "rec3" in m
    clean = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-10-08", "price": 3.5,
                            "inboxes": ["c@good.com"]}, facts, T, v2=v2, dbl={})
    assert clean["support_message"] == ""


def test_no_support_message_asks_to_remove_inboxes_not_in_smartlead():
    # 3 Nov: the review drafted "please remove" notes for 9 PL inboxes that were ready that day
    # (not connected yet) and 25 Melior inboxes that were live under new addresses.
    facts = {"c@good.com": _facts()}
    v2 = {"c@good.com": _v2(PASS_5, email="c@good.com")}
    bill = {"provider": "ScaledMail", "label": "30 × Google", "bills_on": "2026-11-08", "price": 10.5,
            "clients": ["Precise Leads"], "order_id": "rec3"}
    r = rr.review_bill({**bill, "inboxes": ["a@new.com", "b@new.com", "c@good.com"]}, facts, T, v2=v2, dbl={})
    assert r["support_message"] == ""
    assert {x["email"]: x["verdict"] for x in r["rows"]} == {"a@new.com": "CHECK", "b@new.com": "CHECK", "c@good.com": "KEEP"}
    assert r["retire_saves_monthly"] == 0 and r["unused_saves_monthly"] == 7.0

    both = {**facts, "d@bad.com": _facts(tags=["do not use"]), "e@bad.com": _facts(tags=["do not use"])}
    m = rr.review_bill({**bill, "inboxes": ["a@new.com", "d@bad.com", "e@bad.com", "c@good.com"]},
                       both, T, v2=v2, dbl={})["support_message"]
    assert "bad.com (2 inboxes)" in m and "new.com" not in m


def test_renamed_mailboxes_are_judged_on_what_smartlead_has():
    # 3 Nov: ScaledMail still listed one old address x25 while Smartlead held 25
    # re-created inboxes on that domain, so all 25 looked "never used".
    facts = {"ryan@grow.com": _facts(), "r.m@grow.com": _facts(), "c@good.com": _facts()}
    v2 = {e: _v2(PASS_5, email=e) for e in facts}
    r = rr.review_bill({"provider": "ScaledMail", "label": "L", "bills_on": "2026-11-06", "price": 6.0,
                        "inboxes": ["baker_ken@grow.com", "ken.b@grow.com", "c@good.com"]}, facts, T, v2=v2, dbl={})
    assert sorted(x["email"] for x in r["rows"]) == ["c@good.com", "r.m@grow.com", "ryan@grow.com"]
    assert r["counts"] == {"KEEP": 3, "RETIRE": 0, "CHECK": 0, "UNTESTED": 0} and r["unused_saves_monthly"] == 0
    assert r["support_message"] == ""


def test_tracker_flags_refuse_a_half_read_review():
    import renewal_review as cli
    out = cli.flag_tracker({"errors": ["Smartlead PRECISE_LEADS: 429"], "reviews": []})
    assert out["written"] == 0 and "not written" in out["skipped"]
    out = cli.flag_tracker({"errors": ["v2 results (Mongo v2_verdicts): timed out"], "reviews": []})
    assert out["written"] == 0 and "v2 results" in out["skipped"]
    out = cli.flag_tracker({"errors": ["classic tests PRECISE_LEADS: 502", "Zapmail X: 500"], "reviews": []})
    assert out["written"] == 0 and out["skipped"] == "not written: Zapmail X: 500"


def test_tracker_flags_ignore_note_only_read_errors(monkeypatch):
    """Review R3: classic tests and campaign lists never decide, so they don't stop the flags."""
    import renewal_review as cli
    import smartlead.zapmail_asset_sync as zas

    class Assets:
        def bulk_write(self, ops, ordered):
            return type("R", (), {"modified_count": len(ops)})()

    monkeypatch.setattr(zas, "_db", lambda: {"assets": Assets()})
    rows = [{"email": "a@x.com", "verdict": "KEEP", "why": "passed v2 on 5 Oct"}]
    errors = ["classic tests PRECISE_LEADS: 502", "campaign lists BETTRDATA: timed out"]
    assert rr.decision_errors(errors) == []
    assert cli.flag_tracker({"errors": errors, "reviews": [{"rows": rows}]}) == {"written": 1}


def test_tracker_flags_keep_their_shape(monkeypatch):
    """renewal_labels.js reads DROP / CHECK: untested is written as CHECK, keep clears the flag."""
    import renewal_review as cli
    import smartlead.zapmail_asset_sync as zas

    class Assets:
        ops = []

        def bulk_write(self, ops, ordered):
            Assets.ops = ops
            return type("R", (), {"modified_count": len(ops)})()

    monkeypatch.setattr(zas, "_db", lambda: {"assets": Assets()})
    rows = [{"email": "a@x.com", "verdict": "RETIRE", "why": "failed v2 on the weekly tests of 28 Sep and 5 Oct"},
            {"email": "b@x.com", "verdict": "UNTESTED", "why": "no v2 result in the last 8 days"},
            {"email": "c@x.com", "verdict": "CHECK", "why": "domain on Spamhaus DBL (check of 5 Oct)"},
            {"email": "d@x.com", "verdict": "KEEP", "why": "passed v2 on 5 Oct"}]
    assert cli.flag_tracker({"errors": [], "reviews": [{"rows": rows}]}) == {"written": 4}
    got = {op._filter["name"]: op._doc for op in Assets.ops}
    assert got["a@x.com"]["$set"]["renewalFlag"] == "DROP"
    assert got["b@x.com"]["$set"] == {"renewalFlag": "CHECK", "renewalFlagReason": "no v2 result in the last 8 days",
                                      "renewalFlagAt": got["b@x.com"]["$set"]["renewalFlagAt"]}
    assert got["c@x.com"]["$set"]["renewalFlag"] == "CHECK"
    assert set(got["d@x.com"]["$unset"]) == {"renewalFlag", "renewalFlagReason", "renewalFlagAt"}
