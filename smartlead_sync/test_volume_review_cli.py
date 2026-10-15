"""volume_review.py: live data -> review rows, and the Apply reply text. No network."""
from datetime import date

import volume_review as cli
from smartlead import volume_rule as vr

TODAY = date(2026, 11, 3)
PASS2 = [{"date": "2026-11-02", "seeds": 20, "spam": 0, "by": {}}, {"date": "2026-10-26", "seeds": 20, "spam": 0, "by": {}}]


def box(i, email, *, limit=15, type_="GMAIL", host=None, tags=("Bot-Setup",), warmup="2026-10-13T10:00:00Z", rep="100%"):
    return {"id": i, "from_email": email, "type": type_, "smtp_host": host, "message_per_day": limit, "client_id": None,
            "tags": [{"tag_name": t} for t in tags],
            "warmup_details": {"warmup_created_at": warmup, "warmup_reputation": rep}}


def facts(email, sending=True, tests=PASS2, client="Precise Leads"):
    return {email: {"tests": tests, "sending": ["Campaign"] if sending else [], "client": client}}


def build(accounts, fx, health=None, approved=None, verdicts=None):
    return cli.build_rows({"PRECISE_LEADS": accounts}, fx, health or {}, approved or {}, verdicts or {}, TODAY)


def test_healthy_ready_inbox_in_a_campaign_is_raised():
    rows, waiting = build([box(1, "a@pl.com")], facts("a@pl.com"), health={"a@pl.com": (120, 0)})
    assert [(r["email"], r["action"], r["from"], r["to"], r["account"], r["id"], r["client"]) for r in rows] == [
        ("a@pl.com", vr.RAISE, 15, 17, "PRECISE_LEADS", 1, "Precise Leads")]


def test_inboxes_not_reviewed_are_counted_with_why():
    accts = [box(1, "w@new.com", warmup="2026-10-25T10:00:00Z"), box(2, "n@pl.com"), box(3, "h@pl.com", limit=12),
             box(4, "old@pl.com", tags=())]
    fx = {**facts("w@new.com"), **facts("n@pl.com", sending=False), **facts("h@pl.com"), **facts("old@pl.com")}
    rows, waiting = build(accts, fx)
    assert rows == []
    assert waiting["warming"] == 1 and waiting["first_ready"] == "2026-11-08"
    assert waiting["no_campaign"] == 1
    assert waiting["by_hand"] == ["h@pl.com changed by hand to 12 (set 15)"]


def test_approved_limit_and_date_feed_the_decision():
    rows, _ = build([box(1, "a@pl.com", limit=17)], facts("a@pl.com"), health={"a@pl.com": (120, 0)},
                    approved={"a@pl.com": (17, "2026-10-30")})
    assert rows[0]["action"] == vr.HOLD and "changed 4 days ago" in rows[0]["why"]


def test_kill_list_inbox_is_skipped_and_low_smtp_reputation_asks_for_ip_rotation():
    accts = [box(1, "k@pl.com"), box(2, "s@smtp.com", type_="SMTP", host="mail.x.com", limit=7, rep="45%")]
    fx = {**facts("k@pl.com"), **facts("s@smtp.com")}
    rows, waiting = build(accts, fx, health={"k@pl.com": (100, 0), "s@smtp.com": (100, 0)},
                          verdicts={"k@pl.com": "SHOULD KILL"})
    assert waiting["skipped"] == ["k@pl.com: on the kill list; see Kill warnings"]
    assert waiting["rotate_ip"] == ["smtp.com (45%)"]
    assert [r["action"] for r in rows] == [vr.HOLD]          # SMTP at 45% < 70: one slip


def test_apply_text():
    assert cli.apply_text({"error": "No volume review saved for 2026-11-03."}).startswith(":x:")
    t = cli.apply_text({"client": "Precise Leads", "changed": ["a@pl.com 15 -> 17"], "skipped": ["b@pl.com: now 12, not 15"],
                        "failed": []})
    assert "Precise Leads: changed the daily limit on 1 inbox" in t and "a@pl.com 15 -> 17" in t
    assert "Skipped (changed since the review)" in t


def test_ip_rotation_waits_for_a_full_week_of_warmup():
    # Smartlead support 15 Oct: reputation is a 7-day window; 50-69% in the first days is normal.
    young = box(1, "s@smtp.com", type_="SMTP", host="mail.x.com", limit=7, rep="45%", warmup="2026-10-30T10:00:00Z")
    _, waiting = build([young], facts("s@smtp.com"))
    assert waiting["rotate_ip"] == []
