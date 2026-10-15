"""volume_review.py: live data -> per-domain rows. No network."""
from datetime import date

import volume_review as cli
from smartlead import volume_rule as vr

TODAY = date(2026, 10, 21)
PASS = [{"date": "2026-10-20", "seeds": 20, "spam": 0, "by": {}}]
FULL = [15, 15, 15, 14, 15]


def box(i, email, *, limit=15, type_="GMAIL", host=None, tags=("Bot-Setup",), warmup="2026-10-01T10:00:00Z", rep="100%"):
    return {"id": i, "from_email": email, "type": type_, "smtp_host": host, "message_per_day": limit, "client_id": None,
            "tags": [{"tag_name": t} for t in tags],
            "warmup_details": {"warmup_created_at": warmup, "warmup_reputation": rep}}


def facts(*emails, sending=True, tests=PASS, client="Melior"):
    return {e: {"tests": tests, "sending": ["Campaign"] if sending else [], "client": client} for e in emails}


def build(accounts, fx, health=None, daily=None, approved=None, verdicts=None, pilot=()):
    return cli.build_rows({"PRECISE_LEADS": accounts}, fx, health or {}, daily or {}, approved or {},
                          verdicts or {}, TODAY, set(pilot))


THREE = ["a@pl.com", "b@pl.com", "c@pl.com"]


def test_healthy_full_domain_is_raised_as_one_row():
    rows, _ = build([box(i, e) for i, e in enumerate(THREE)], facts(*THREE),
                    health={e: (100, 0) for e in THREE}, daily={e: FULL for e in THREE})
    assert len(rows) == 1
    r = rows[0]
    assert (r["domain"], r["action"], r["from"], r["to"], r["client"], r["pilot"]) == (
        "pl.com", vr.RAISE, [15, 15, 15], [17, 17, 17], "Melior", False)
    assert [b["email"] for b in r["inboxes"]] == THREE


def test_pilot_domains_are_judged_even_without_the_setup_tag():
    accts = [box(i, e, tags=(), limit=20) for i, e in enumerate(THREE)]
    rows, _ = build(accts, facts(*THREE), approved={e: (20, None) for e in THREE}, pilot={"pl.com"})
    assert rows[0]["pilot"] and rows[0]["action"] == vr.HOLD
    rows, _ = build(accts, facts(*THREE))
    assert rows == []


def test_star_puts_every_judged_domain_in_the_pilot():
    rows, _ = build([box(i, e) for i, e in enumerate(THREE)], facts(*THREE), pilot={"*"})
    assert rows[0]["pilot"]


def test_inboxes_not_reviewed_are_counted_with_why():
    accts = [box(1, "w@new.com", warmup="2026-10-15T10:00:00Z"), box(2, "n@pl.com"), box(3, "h@pl.com", limit=12)]
    rows, waiting = build(accts, {**facts("w@new.com"), **facts("n@pl.com", sending=False), **facts("h@pl.com")})
    assert rows == []
    assert waiting["warming"] == 1 and waiting["first_ready"] == "2026-10-29"
    assert waiting["no_campaign"] == 1
    assert waiting["by_hand"] == ["h@pl.com changed by hand to 12 (set 15)"]


def test_signals_are_pooled_per_domain():
    rows, _ = build([box(i, e) for i, e in enumerate(THREE)], facts(*THREE),
                    health={"a@pl.com": (100, 5), "b@pl.com": (100, 5), "c@pl.com": (100, 2)},   # 4% pooled
                    daily={e: FULL for e in THREE}, approved={e: (17, "2026-10-14") for e in THREE})
    assert rows == []                                           # limit 15 but approved 17: changed by hand
    accts = [box(i, e, limit=17) for i, e in enumerate(THREE)]
    rows, _ = build(accts, facts(*THREE), health={"a@pl.com": (100, 5), "b@pl.com": (100, 5), "c@pl.com": (100, 2)},
                    daily={e: FULL for e in THREE}, approved={e: (17, "2026-10-14") for e in THREE})
    assert rows[0]["action"] == vr.STEP_BACK and rows[0]["to"] == [15, 15, 15] and "4%" in rows[0]["why"]


def test_kill_list_and_low_smtp_reputation():
    rows, _ = build([box(i, e) for i, e in enumerate(THREE)], facts(*THREE), health={e: (100, 0) for e in THREE},
                    daily={e: FULL for e in THREE}, verdicts={"a@pl.com": "SHOULD KILL"})
    assert rows[0]["action"] == vr.HOLD and "kill list" in rows[0]["why"]      # at the start already
    s = box(9, "s@smtp.com", type_="SMTP", host="mail.x.com", limit=7, rep="45%")
    _, waiting = build([s], facts("s@smtp.com"))
    assert waiting["rotate_ip"] == ["smtp.com (45%)"]
    young = box(9, "s@smtp.com", type_="SMTP", host="mail.x.com", limit=7, rep="45%", warmup="2026-10-17T10:00:00Z")
    _, waiting = build([young], facts("s@smtp.com"))
    assert waiting["rotate_ip"] == []
